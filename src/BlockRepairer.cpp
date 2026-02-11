#include "BlockRepairer.h"

#include <cstring>
#include <fstream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <iostream>
#include <filesystem>

#include <city.h>

#include "compat/codec.h"
#include "compat/compression_defs.h"
#include "compat/default_serializer.h"

namespace PartRepair
{

namespace
{

/// Local VarUInt reader — mirrors FormatDetector::tryReadVarUInt but returns
/// a simple bool instead of std::optional.
bool readVarUIntLocal(const char * data, size_t available, uint64_t & value, size_t & bytes_consumed)
{
    value = 0;
    bytes_consumed = 0;

    uint64_t result = 0;
    size_t i = 0;
    unsigned shift = 0;

    while (i < available)
    {
        uint8_t byte = static_cast<uint8_t>(data[i]);
        result |= (static_cast<uint64_t>(byte & 0x7F)) << shift;
        ++i;

        if ((byte & 0x80) == 0)
        {
            value = result;
            bytes_consumed = i;
            return true;
        }

        shift += 7;
        if (shift >= 64)
            return false; // overflow
    }

    return false; // ran out of data
}

/// Local VarUInt writer — encodes value into base-128 VLQ with MSB=1
/// continuation bits, matching ClickHouse's writeVarUInt().
void writeVarUIntLocal(uint64_t value, std::vector<char> & out)
{
    while (true)
    {
        uint8_t byte = static_cast<uint8_t>(value & 0x7F);
        value >>= 7;
        if (value != 0)
            byte |= 0x80;
        out.push_back(static_cast<char>(byte));
        if (value == 0)
            break;
    }
}

/// Check if a type name is of the form Nullable(T) and, if so, extract T.
bool parseNullableType(const std::string & type_name, std::string & inner_type)
{
    static const std::string prefix = "Nullable(";
    if (type_name.rfind(prefix, 0) != 0)
        return false;

    if (type_name.size() <= prefix.size() || type_name.back() != ')')
        return false;

    inner_type = type_name.substr(prefix.size(), type_name.size() - prefix.size() - 1);
    return !inner_type.empty();
}

/// Serialize a constant (same value for every row) for simple types supported
/// by this tool: fixed-width numeric types and String/FixedString.
std::vector<char> serializeExplicitConstant(
    const std::string & type_name,
    const std::string & literal,
    size_t row_count)
{
    if (row_count == 0)
        return {};

    auto fail = [&](const std::string & reason) -> std::vector<char>
    {
        throw std::runtime_error(
            "Cannot cast --default-value '" + literal + "' to type '" + type_name + "': " + reason);
    };

    auto emit_fixed = [&](auto parsed_value) -> std::vector<char>
    {
        using T = decltype(parsed_value);
        const size_t width = sizeof(T);
        std::vector<char> data(row_count * width);
        for (size_t i = 0; i < row_count; ++i)
        {
            std::memcpy(data.data() + i * width, &parsed_value, width);
        }
        return data;
    };

    try
    {
        if (type_name == "UInt8")
            return emit_fixed(static_cast<uint8_t>(std::stoul(literal)));
        if (type_name == "Int8")
            return emit_fixed(static_cast<int8_t>(std::stol(literal)));
        if (type_name == "UInt16")
            return emit_fixed(static_cast<uint16_t>(std::stoul(literal)));
        if (type_name == "Int16")
            return emit_fixed(static_cast<int16_t>(std::stol(literal)));
        if (type_name == "UInt32")
            return emit_fixed(static_cast<uint32_t>(std::stoul(literal)));
        if (type_name == "Int32")
            return emit_fixed(static_cast<int32_t>(std::stol(literal)));
        if (type_name == "UInt64")
            return emit_fixed(static_cast<uint64_t>(std::stoull(literal)));
        if (type_name == "Int64")
            return emit_fixed(static_cast<int64_t>(std::stoll(literal)));
        if (type_name == "Float32")
            return emit_fixed(static_cast<float>(std::stof(literal)));
        if (type_name == "Float64")
            return emit_fixed(static_cast<double>(std::stod(literal)));
        if (type_name == "Bool")
        {
            bool v;
            if (literal == "0" || literal == "false" || literal == "False" || literal == "FALSE")
                v = false;
            else if (literal == "1" || literal == "true" || literal == "True" || literal == "TRUE")
                v = true;
            else
                fail("expected 0/1 or true/false");
            return emit_fixed(static_cast<uint8_t>(v ? 1 : 0));
        }

        if (type_name == "String" || type_name == "FixedString")
        {
            /// String/FixedString constant: serialize as [VarUInt(len)][bytes] per row.
            std::vector<char> buf;
            buf.reserve(row_count * (literal.size() + 8)); // heuristic
            for (size_t i = 0; i < row_count; ++i)
            {
                writeVarUIntLocal(static_cast<uint64_t>(literal.size()), buf);
                buf.insert(buf.end(), literal.begin(), literal.end());
            }
            return buf;
        }
    }
    catch (const std::exception & e)
    {
        fail(e.what());
    }

    fail("unsupported type for explicit default; only simple numeric and String types are supported");
    return {};
}

/// --- Primary-key boundary helpers ---

/// Extract first and last serialized value from decompressed block data.
/// Returns {first_value, last_value}; either may be empty if type is unsupported.
/// Note: primary-key columns cannot be Nullable (enforced at CLI level).
std::pair<std::vector<char>, std::vector<char>> extractFirstLastValue(
    const std::string & type_name,
    const char * data,
    size_t size)
{
    std::pair<std::vector<char>, std::vector<char>> result;
    if (data == nullptr || size == 0)
        return result;

    size_t value_width = PartRepair::getTypeWidth(type_name);

    if (value_width > 0)
    {
        if (size < value_width)
            return result;

        size_t row_count = size / value_width;
        if (row_count == 0)
            return result;

        // First value
        result.first.assign(data, data + value_width);
        // Last value
        size_t last_row_start = (row_count - 1) * value_width;
        result.second.assign(data + last_row_start, data + last_row_start + value_width);
        return result;
    }

    if (type_name == "String" || type_name == "FixedString")
    {
        // Variable-width: iterate rows to get first and last.
        size_t pos = 0;
        size_t first_start = 0;
        size_t first_end = 0;
        size_t last_start = 0;
        size_t last_end = 0;

        while (pos < size)
        {
            uint64_t len = 0;
            size_t varint_bytes = 0;
            if (!readVarUIntLocal(data + pos, size - pos, len, varint_bytes))
                break;
            size_t row_start = pos;
            pos += varint_bytes;
            if (len > size - pos)
                break;
            pos += static_cast<size_t>(len);
            last_start = row_start;
            last_end = pos;
            if (first_end == 0)
            {
                first_start = row_start;
                first_end = pos;
            }
        }

        if (first_end > first_start)
        {
            result.first.assign(data + first_start, data + first_end);
            result.second.assign(data + last_start, data + last_end);
        }
        return result;
    }

    return result;
}

/// Compare two serialized values (same type). Returns <0 if a < b, 0 if a == b, >0 if a > b.
/// Supports fixed-width numeric types and String.
/// Note: primary-key columns cannot be Nullable (enforced at CLI level).
int compareValues(
    const std::string & type_name,
    const std::vector<char> & a,
    const std::vector<char> & b)
{
    if (a.empty() && b.empty())
        return 0;
    if (a.empty())
        return -1;
    if (b.empty())
        return 1;

    size_t width = PartRepair::getTypeWidth(type_name);
    if (width > 0 && a.size() >= width && b.size() >= width)
    {
        // Fixed-width numeric: compare as little-endian
        if (type_name == "UInt8" || type_name == "UInt16" || type_name == "UInt32" || type_name == "UInt64" || type_name == "Bool")
        {
            for (size_t i = width; i > 0; --i)
            {
                unsigned char ua = static_cast<unsigned char>(a[i - 1]);
                unsigned char ub = static_cast<unsigned char>(b[i - 1]);
                if (ua != ub)
                    return (ua < ub) ? -1 : 1;
            }
            return 0;
        }
        if (type_name == "Int8" || type_name == "Int16" || type_name == "Int32" || type_name == "Int64")
        {
            int64_t va = 0, vb = 0;
            std::memcpy(&va, a.data(), std::min(sizeof(va), width));
            std::memcpy(&vb, b.data(), std::min(sizeof(vb), width));
            if (width == 1) { va = static_cast<int8_t>(va); vb = static_cast<int8_t>(vb); }
            else if (width == 2) { va = static_cast<int16_t>(va); vb = static_cast<int16_t>(vb); }
            else if (width == 4) { va = static_cast<int32_t>(va); vb = static_cast<int32_t>(vb); }
            if (va < vb) return -1;
            if (va > vb) return 1;
            return 0;
        }
        if (type_name == "Float32" || type_name == "Float64")
        {
            if (width == 4)
            {
                float fa = 0, fb = 0;
                std::memcpy(&fa, a.data(), 4);
                std::memcpy(&fb, b.data(), 4);
                if (fa < fb) return -1;
                if (fa > fb) return 1;
                return 0;
            }
            if (width == 8)
            {
                double da = 0, db = 0;
                std::memcpy(&da, a.data(), 8);
                std::memcpy(&db, b.data(), 8);
                if (da < db) return -1;
                if (da > db) return 1;
                return 0;
            }
        }
        // Fallback: lexicographic
        int c = std::memcmp(a.data(), b.data(), width);
        if (c != 0) return c;
        return 0;
    }

    // String: lexicographic
    size_t len = std::min(a.size(), b.size());
    int c = std::memcmp(a.data(), b.data(), len);
    if (c != 0) return c;
    if (a.size() < b.size()) return -1;
    if (a.size() > b.size()) return 1;
    return 0;
}

/// True if value is in [prev_last, next_first] (prev_last and/or next_first may be null = no bound).
bool valueInRange(
    const std::string & type_name,
    const std::vector<char> & value,
    const std::vector<char> * prev_last,
    const std::vector<char> * next_first)
{
    if (prev_last != nullptr && !prev_last->empty() && compareValues(type_name, value, *prev_last) < 0)
        return false;
    if (next_first != nullptr && !next_first->empty() && compareValues(type_name, value, *next_first) > 0)
        return false;
    return true;
}

/// Format a serialized value for logging (hex for long values, scalar for UInt64 if possible).
std::string formatValueForLog(const std::string & type_name, const std::vector<char> & value)
{
    if (value.empty())
        return "none";
    if (type_name == "UInt64" && value.size() >= 8)
    {
        uint64_t v = 0;
        std::memcpy(&v, value.data(), 8);
        return std::to_string(v);
    }
    if (PartRepair::getTypeWidth(type_name) > 0 && value.size() <= 16)
    {
        std::string hex;
        for (unsigned char b : value)
        {
            char buf[4];
            snprintf(buf, sizeof(buf), "%02x", b);
            hex += buf;
        }
        return "0x" + hex;
    }
    std::string hex;
    size_t n = std::min(value.size(), size_t(16));
    for (size_t i = 0; i < n; ++i)
    {
        char buf[4];
        snprintf(buf, sizeof(buf), "%02x", static_cast<unsigned char>(value[i]));
        hex += buf;
    }
    if (value.size() > 16)
        hex += "...";
    return "0x" + hex;
}

/// Repeat a serialized value (fixed-width or String VarUInt+bytes) row_count times.
std::vector<char> serializeValueRepeated(
    const std::vector<char> & value,
    size_t row_count,
    size_t value_width_if_fixed)
{
    if (value.empty() || row_count == 0)
        return {};
    if (value_width_if_fixed > 0)
    {
        if (value.size() != value_width_if_fixed)
            return {};
        std::vector<char> out(row_count * value_width_if_fixed);
        for (size_t i = 0; i < row_count; ++i)
            std::memcpy(out.data() + i * value_width_if_fixed, value.data(), value_width_if_fixed);
        return out;
    }
    // String: value is VarUInt(len)+bytes
    std::vector<char> out;
    out.reserve(value.size() * row_count);
    for (size_t i = 0; i < row_count; ++i)
        out.insert(out.end(), value.begin(), value.end());
    return out;
}

/// Diagnostic info returned by buildPartiallySortedBlock when a sort violation is found.
struct SortViolationInfo
{
    bool found = false;          /// True if a sort violation was detected.
    size_t violation_row = 0;    /// Row index where the violation occurred (the "right" row).
    size_t rows_in_data = 0;     /// Total number of parseable rows in the decompressed data.
    std::vector<char> left_value;   /// Value at row (violation_row - 1).
    std::vector<char> right_value;  /// Value at violation_row.
    std::vector<char> fill_value;   /// Value chosen to fill remaining rows.
};

/// Build a block from decompressed data by keeping the longest sorted prefix and filling
/// the rest with the last sorted row's value (or prev_block_last if that value is out of range).
/// Returns decompressed buffer of exactly row_count rows, or empty if not applicable.
/// Note: primary-key columns cannot be Nullable (enforced at CLI level).
std::vector<char> buildPartiallySortedBlock(
    const std::string & type_name,
    const char * data,
    size_t size,
    size_t row_count,
    const std::vector<char> * prev_block_last,
    const std::vector<char> * next_block_first,
    size_t & partial_rows_out,
    size_t & fill_rows_out,
    SortViolationInfo & violation_out)
{
    partial_rows_out = 0;
    fill_rows_out = 0;
    violation_out = {};
    if (data == nullptr || size == 0 || row_count == 0)
        return {};

    size_t value_width = PartRepair::getTypeWidth(type_name);

    if (value_width > 0)
    {
        if (size < value_width)
            return {};
        size_t rows_in_data = size / value_width;
        if (rows_in_data == 0)
            return {};

        // Find last index k such that rows 0..k are sorted.
        size_t k = 0;
        std::vector<char> prev(data, data + value_width);

        for (size_t r = 1; r < rows_in_data; ++r)
        {
            size_t row_start = r * value_width;
            std::vector<char> curr(data + row_start, data + row_start + value_width);
            if (compareValues(type_name, prev, curr) > 0)
            {
                violation_out.found = true;
                violation_out.violation_row = r;
                violation_out.rows_in_data = rows_in_data;
                violation_out.left_value = prev;
                violation_out.right_value = curr;
                break;
            }
            k = r;
            prev = std::move(curr);
        }

        size_t partial_rows = k + 1;
        if (partial_rows >= row_count)
        {
            // All requested rows are present and sorted; truncate to row_count
            partial_rows_out = row_count;
            fill_rows_out = 0;
            std::vector<char> out(row_count * value_width);
            std::memcpy(out.data(), data, out.size());
            return out;
        }

        std::vector<char> fill_value(data + k * value_width, data + k * value_width + value_width);
        if (prev_block_last != nullptr && !prev_block_last->empty() && !valueInRange(type_name, fill_value, prev_block_last, next_block_first))
            fill_value = *prev_block_last;
        violation_out.fill_value = fill_value;

        partial_rows_out = partial_rows;
        fill_rows_out = row_count - partial_rows;
        std::vector<char> out(row_count * value_width);
        std::memcpy(out.data(), data, partial_rows * value_width);
        for (size_t r = 0; r < fill_rows_out; ++r)
            std::memcpy(out.data() + (partial_rows + r) * value_width, fill_value.data(), value_width);
        return out;
    }

    if (type_name == "String" || type_name == "FixedString")
    {
        std::vector<std::pair<size_t, size_t>> row_bounds;
        size_t pos = 0;
        while (pos < size)
        {
            uint64_t len = 0;
            size_t varint_bytes = 0;
            if (!readVarUIntLocal(data + pos, size - pos, len, varint_bytes))
                break;
            size_t start = pos;
            pos += varint_bytes;
            if (len > size - pos)
                break;
            pos += static_cast<size_t>(len);
            row_bounds.emplace_back(start, pos);
        }
        if (row_bounds.empty())
            return {};

        // Find last index k such that rows 0..k are sorted.
        size_t k = 0;
        std::vector<char> prev(data + row_bounds[0].first, data + row_bounds[0].second);
        for (size_t r = 1; r < row_bounds.size(); ++r)
        {
            std::vector<char> curr(data + row_bounds[r].first, data + row_bounds[r].second);
            if (compareValues(type_name, prev, curr) > 0)
            {
                violation_out.found = true;
                violation_out.violation_row = r;
                violation_out.rows_in_data = row_bounds.size();
                violation_out.left_value = prev;
                violation_out.right_value = curr;
                break;
            }
            k = r;
            prev = std::move(curr);
        }

        size_t partial_rows = k + 1;
        if (partial_rows >= row_count)
        {
            partial_rows_out = row_count;
            fill_rows_out = 0;
            std::vector<char> out;
            for (size_t i = 0; i < row_count && i < row_bounds.size(); ++i)
                out.insert(out.end(), data + row_bounds[i].first, data + row_bounds[i].second);
            return out;
        }

        std::vector<char> fill_value(data + row_bounds[k].first, data + row_bounds[k].second);
        if (prev_block_last != nullptr && !prev_block_last->empty() && !valueInRange(type_name, fill_value, prev_block_last, next_block_first))
            fill_value = *prev_block_last;
        violation_out.fill_value = fill_value;

        partial_rows_out = partial_rows;
        fill_rows_out = row_count - partial_rows;
        std::vector<char> out;
        for (size_t i = 0; i < partial_rows; ++i)
            out.insert(out.end(), data + row_bounds[i].first, data + row_bounds[i].second);
        std::vector<char> fill = serializeValueRepeated(fill_value, fill_rows_out, 0);
        out.insert(out.end(), fill.begin(), fill.end());
        return out;
    }

    return {};
}

} // anonymous namespace

BlockRepairer::BlockRepairer(
    const std::string & column_type_name,
    Logger & logger,
    const std::string & input_bin_path,
    const std::string & default_value_literal,
    bool default_null,
    bool primary_key)
    : column_type_name_(column_type_name)
    , logger_(logger)
    , input_bin_path_(input_bin_path)
    , primary_key_(primary_key)
{
    if (default_null)
    {
        default_mode_ = DefaultMode::NullValue;
    }
    else if (!default_value_literal.empty())
    {
        default_mode_ = DefaultMode::ExplicitValue;
        default_value_literal_ = default_value_literal;
    }
    else
    {
        default_mode_ = DefaultMode::TypeDefault;
    }
}

size_t BlockRepairer::getTypeWidth() const
{
    return PartRepair::getTypeWidth(column_type_name_);
}

std::vector<char> BlockRepairer::serializeDefaultValues(size_t row_count) const
{
    if (row_count == 0)
        return {};

    // Handle Nullable(...) specially when using NULL or explicit defaults.
    std::string inner_type;
    bool is_nullable = parseNullableType(column_type_name_, inner_type);

    if (default_mode_ == DefaultMode::NullValue)
    {
        if (!is_nullable)
        {
            throw std::runtime_error(
                "--default-null was requested but column type '" + column_type_name_
                + "' is not Nullable(...).");
        }

        // Nullable(T) with all NULLs: null map of ones + nested defaults for T.
        std::vector<char> null_map(row_count, static_cast<char>(1));
        std::vector<char> nested = serializeDefaults(inner_type, row_count);

        std::vector<char> out;
        out.reserve(null_map.size() + nested.size());
        out.insert(out.end(), null_map.begin(), null_map.end());
        out.insert(out.end(), nested.begin(), nested.end());
        return out;
    }

    if (default_mode_ == DefaultMode::ExplicitValue)
    {
        if (is_nullable)
        {
            // Nullable(T) with explicit non-NULL constant: null map of zeros + constant T.
            std::vector<char> null_map(row_count, static_cast<char>(0));
            std::vector<char> nested = serializeExplicitConstant(
                inner_type,
                default_value_literal_,
                row_count);

            std::vector<char> out;
            out.reserve(null_map.size() + nested.size());
            out.insert(out.end(), null_map.begin(), null_map.end());
            out.insert(out.end(), nested.begin(), nested.end());
            return out;
        }

        // Non-nullable simple type: serialize constant directly.
        return serializeExplicitConstant(column_type_name_, default_value_literal_, row_count);
    }

    // DefaultMode::TypeDefault — delegate to generic default serializer.
    return serializeDefaults(column_type_name_, row_count);
}

size_t BlockRepairer::estimateRowCount(
    const BlockInfo & block,
    const std::vector<MarkEntry> & marks) const
{
    // First try: sum rows from marks pointing to this block's file offset
    if (!marks.empty())
    {
        size_t total_rows = 0;
        for (const auto & mark : marks)
        {
            if (mark.offset_in_compressed_file == block.file_offset)
                total_rows += mark.rows_count;
        }
        if (total_rows > 0)
            return total_rows;
    }

    // Fallback: estimate from decompressed_size and type width
    size_t width = getTypeWidth();
    if (width > 0 && block.decompressed_size > 0)
        return block.decompressed_size / width;

    // Last resort: for variable-width types, we can't estimate.
    // Use 0 rows (empty block).
    logger_.warn("Cannot estimate row count for block " + std::to_string(block.block_index)
        + " (no marks, variable-width type). Using 0 rows.");
    return 0;
}

std::vector<char> BlockRepairer::generateDefaultBlock(
    size_t row_count,
    uint8_t method_byte)
{
    /// 1. Serialize default values using the configured default mode.
    std::vector<char> serialized_data = serializeDefaultValues(row_count);
    uint32_t decompressed_size = static_cast<uint32_t>(serialized_data.size());

    /// 2. Compress using our codec wrapper.
    /// For unsupported method bytes, compressBlock() falls back to LZ4.
    std::vector<char> compressed_block = compressBlock(method_byte, serialized_data.data(), decompressed_size);
    uint32_t compressed_size = static_cast<uint32_t>(compressed_block.size());

    /// 3. Compute checksum over compressed data (method byte through end of payload).
    /// This matches CompressedWriteBuffer::nextImpl() behavior.
    auto checksum = CityHash_v1_0_2::CityHash128(compressed_block.data(), compressed_size);

    /// 4. Build on-disk block: [checksum][compressed data]
    std::vector<char> on_disk_block;
    on_disk_block.resize(sizeof(Checksum) + compressed_size);

    // Write checksum (little-endian uint64 pair)
    std::memcpy(on_disk_block.data(), &checksum.low64, sizeof(uint64_t));
    std::memcpy(on_disk_block.data() + sizeof(uint64_t), &checksum.high64, sizeof(uint64_t));

    // Write compressed data (header + payload)
    std::memcpy(on_disk_block.data() + sizeof(Checksum), compressed_block.data(), compressed_size);

    logger_.info("Generated replacement block: " + std::to_string(row_count) + " rows, "
        + std::to_string(decompressed_size) + " bytes decompressed, "
        + std::to_string(compressed_size) + " bytes compressed");

    return on_disk_block;
}

std::vector<char> BlockRepairer::buildRepairedDecompressedBuffer(
    const ScanResult & scan,
    size_t row_count,
    size_t & partial_rows_out,
    size_t & default_rows_out)
{
    partial_rows_out = 0;
    default_rows_out = 0;

    if (scan.partial_bytes == 0 || scan.partial_data.empty())
        return {};

    if (row_count == 0)
        return {};

    size_t width = getTypeWidth();
    if (width > 0)
    {
        // Fixed-width types: interpret partial_bytes as a whole number of rows.
        size_t max_full_rows_from_partial = scan.partial_bytes / width;
        if (max_full_rows_from_partial == 0)
            return {};

        size_t partial_rows = std::min(max_full_rows_from_partial, row_count);
        size_t default_rows = row_count - partial_rows;

        const size_t total_bytes = row_count * width;
        std::vector<char> decompressed(total_bytes);

        const size_t partial_bytes_to_copy = partial_rows * width;
        std::memcpy(decompressed.data(), scan.partial_data.data(), partial_bytes_to_copy);

        if (default_rows > 0)
        {
            auto defaults = serializeDefaultValues(default_rows);
            if (defaults.size() != default_rows * width)
            {
                logger_.warn("Default serializer for type '" + column_type_name_
                    + "' returned " + std::to_string(defaults.size())
                    + " bytes for " + std::to_string(default_rows)
                    + " rows (expected " + std::to_string(default_rows * width)
                    + "). Falling back to full-default replacement for this block.");
                return {};
            }

            std::memcpy(decompressed.data() + partial_bytes_to_copy, defaults.data(), defaults.size());
        }

        partial_rows_out = partial_rows;
        default_rows_out = default_rows;
        return decompressed;
    }

    // Variable-width types: currently only String/FixedString are supported
    // for partial-data repair. Other variable-width types still fall back to
    // full-default replacement.
    if (column_type_name_ != "String" && column_type_name_ != "FixedString")
    {
        logger_.warn("Cannot use partial data for block " + std::to_string(scan.block_index)
            + " because column type '" + column_type_name_ + "' is variable-width and not "
            "supported for partial repair. Falling back to default values for this block.");
        return {};
    }

    const char * data = scan.partial_data.data();
    size_t available = scan.partial_bytes;
    size_t pos = 0;

    std::vector<char> decompressed;
    decompressed.reserve(available + row_count); // heuristic

    size_t salvaged_rows = 0;

    while (pos < available && salvaged_rows < row_count)
    {
        uint64_t str_len = 0;
        size_t varint_bytes = 0;

        if (!readVarUIntLocal(data + pos, available - pos, str_len, varint_bytes))
        {
            // Truncated or invalid length prefix: cannot safely parse more rows.
            break;
        }

        size_t header_pos = pos;
        pos += varint_bytes;

        if (str_len <= available - pos)
        {
            // Full string is present: copy original VarUInt header + payload.
            decompressed.insert(
                decompressed.end(),
                data + header_pos,
                data + header_pos + varint_bytes + static_cast<size_t>(str_len));

            pos += static_cast<size_t>(str_len);
            ++salvaged_rows;
            continue;
        }

        // String payload is truncated: salvage a shorter string by updating the
        // length prefix to the actual number of bytes we have, and log details
        // so that this row can be located later if needed.
        size_t truncated_len = available - pos;
        if (truncated_len > 0)
        {
            const size_t row_index_in_block = salvaged_rows; // zero-based within this block

            std::string preview = hexDump(
                data + pos,
                truncated_len,
                /*max_bytes=*/64);

            logger_.warn("Using truncated String value during repair: block_index="
                + std::to_string(scan.block_index)
                + ", row_index_in_block=" + std::to_string(row_index_in_block)
                + ", truncated_length=" + std::to_string(truncated_len)
                + ", hex_preview=" + preview);

            writeVarUIntLocal(static_cast<uint64_t>(truncated_len), decompressed);
            decompressed.insert(
                decompressed.end(),
                data + pos,
                data + pos + truncated_len);
            ++salvaged_rows;
        }

        // After a truncated string, we cannot safely parse further rows.
        pos = available;
        break;
    }

    if (salvaged_rows == 0)
    {
        logger_.warn("Partial data for String block " + std::to_string(scan.block_index)
            + " could not be safely parsed into complete or truncated rows. "
            + "Falling back to default values for this block.");
        return {};
    }

    size_t default_rows = (row_count > salvaged_rows) ? (row_count - salvaged_rows) : 0;
    if (default_rows > 0)
    {
        // For variable-width String/FixedString, honor an explicit default value
        // when provided; otherwise fall back to the empty string.
        if (default_mode_ == DefaultMode::ExplicitValue)
        {
            // serializeExplicitConstant() for String/FixedString emits
            // [VarUInt(len)][bytes] per row, matching how we salvaged rows above.
            std::vector<char> defaults = serializeExplicitConstant(
                column_type_name_,
                default_value_literal_,
                default_rows);

            decompressed.insert(decompressed.end(), defaults.begin(), defaults.end());
        }
        else
        {
            // Default String value is empty string: VarUInt(0) = single 0x00 byte.
            decompressed.insert(decompressed.end(), default_rows, static_cast<char>(0));
        }
    }

    partial_rows_out = salvaged_rows;
    default_rows_out = default_rows;
    return decompressed;
}

std::vector<char> BlockRepairer::buildPrimaryKeyRepairedBuffer(
    const ScanResult & scan,
    size_t row_count,
    const std::vector<char> * prev_block_last,
    const std::vector<char> * next_block_first,
    size_t & partial_rows_out,
    size_t & default_rows_out,
    std::vector<char> & picked_value_out)
{
    partial_rows_out = 0;
    default_rows_out = 0;
    picked_value_out.clear();

    if (scan.partial_bytes == 0 || scan.partial_data.empty() || row_count == 0)
        return {};

    // Note: primary-key columns cannot be Nullable (enforced at CLI level).
    size_t value_width = PartRepair::getTypeWidth(column_type_name_);

    if (value_width > 0)
    {
        // Fixed-width: find largest k such that value at row k is in [prev_last, next_first].
        size_t max_partial_rows = scan.partial_bytes / value_width;
        if (max_partial_rows == 0)
            return {};

        std::vector<char> value_k(value_width);
        size_t k_pick = size_t(-1);
        for (size_t k = max_partial_rows; k > 0; --k)
        {
            size_t row_start = (k - 1) * value_width;
            value_k.assign(
                scan.partial_data.data() + row_start,
                scan.partial_data.data() + row_start + value_width);
            if (valueInRange(column_type_name_, value_k, prev_block_last, next_block_first))
            {
                k_pick = k - 1;
                break;
            }
        }
        if (k_pick == size_t(-1))
            return {};

        size_t keep_rows = k_pick + 1;
        size_t fill_rows = row_count - keep_rows;
        partial_rows_out = keep_rows;
        default_rows_out = fill_rows;
        picked_value_out = value_k;

        const size_t total_bytes = row_count * value_width;
        std::vector<char> decompressed(total_bytes);
        std::memcpy(decompressed.data(), scan.partial_data.data(), keep_rows * value_width);
        if (fill_rows > 0)
        {
            std::vector<char> fill = serializeValueRepeated(value_k, fill_rows, value_width);
            if (fill.size() != fill_rows * value_width)
                return {};
            std::memcpy(decompressed.data() + keep_rows * value_width, fill.data(), fill.size());
        }
        return decompressed;
    }

    // String: parse partial data into rows, find largest m such that last kept row value is in range.
    if (column_type_name_ != "String" && column_type_name_ != "FixedString")
        return {};

    const char * data = scan.partial_data.data();
    size_t available = scan.partial_bytes;
    size_t pos = 0;
    std::vector<std::pair<size_t, size_t>> row_bounds; // (start, end) byte offsets
    while (pos < available)
    {
        uint64_t str_len = 0;
        size_t varint_bytes = 0;
        if (!readVarUIntLocal(data + pos, available - pos, str_len, varint_bytes))
            break;
        size_t start = pos;
        pos += varint_bytes;
        if (str_len > available - pos)
            break;
        pos += static_cast<size_t>(str_len);
        row_bounds.emplace_back(start, pos);
    }
    if (row_bounds.empty())
        return {};

    size_t keep_rows = 0;
    std::vector<char> picked_value;
    for (size_t m = row_bounds.size(); m > 0; --m)
    {
        const auto & bounds = row_bounds[m - 1];
        picked_value.assign(data + bounds.first, data + bounds.second);
        if (valueInRange(column_type_name_, picked_value, prev_block_last, next_block_first))
        {
            keep_rows = m;
            break;
        }
    }
    if (keep_rows == 0)
        return {};

    size_t fill_rows = row_count - keep_rows;
    partial_rows_out = keep_rows;
    default_rows_out = fill_rows;
    picked_value_out = picked_value;

    std::vector<char> decompressed;
    for (size_t i = 0; i < keep_rows; ++i)
        decompressed.insert(decompressed.end(), data + row_bounds[i].first, data + row_bounds[i].second);
    std::vector<char> fill = serializeValueRepeated(picked_value, fill_rows, 0);
    decompressed.insert(decompressed.end(), fill.begin(), fill.end());
    return decompressed;
}

std::vector<char> BlockRepairer::generateBlockFromDecompressed(
    const std::vector<char> & decompressed,
    uint8_t method_byte)
{
    uint32_t decompressed_size = static_cast<uint32_t>(decompressed.size());

    /// Compress using our codec wrapper.
    /// For unsupported method bytes, compressBlock() falls back to LZ4.
    std::vector<char> compressed_block = compressBlock(
        method_byte,
        decompressed.data(),
        decompressed_size);
    uint32_t compressed_size = static_cast<uint32_t>(compressed_block.size());

    /// Compute checksum over compressed data (method byte through end of payload).
    auto checksum = CityHash_v1_0_2::CityHash128(compressed_block.data(), compressed_size);

    /// Build on-disk block: [checksum][compressed data]
    std::vector<char> on_disk_block;
    on_disk_block.resize(sizeof(Checksum) + compressed_size);

    // Write checksum (little-endian uint64 pair)
    std::memcpy(on_disk_block.data(), &checksum.low64, sizeof(uint64_t));
    std::memcpy(on_disk_block.data() + sizeof(uint64_t), &checksum.high64, sizeof(uint64_t));

    // Write compressed data (header + payload)
    std::memcpy(on_disk_block.data() + sizeof(Checksum), compressed_block.data(), compressed_size);

    logger_.info("Generated mixed replacement block: "
        + std::to_string(decompressed_size) + " bytes decompressed, "
        + std::to_string(compressed_size) + " bytes compressed");

    return on_disk_block;
}

void BlockRepairer::repair(
    const std::vector<BlockInfo> & blocks,
    const std::vector<ScanResult> & scan_results,
    const std::vector<MarkEntry> & marks,
    const std::string & mark_file_path,
    const std::string & output_bin_path,
    const std::string & output_mark_path)
{
    if (blocks.size() != scan_results.size())
        throw std::runtime_error("Block count mismatch between iterator and scanner results");

    logger_.info("Starting repair: " + std::to_string(blocks.size()) + " blocks, output: " + output_bin_path);

    /// When primary_key_ is set, build first/last value per healthy block (re-read and decompress).
    using BoundaryPair = std::pair<std::vector<char>, std::vector<char>>;
    std::vector<BoundaryPair> block_boundaries;
    if (primary_key_)
    {
        block_boundaries.resize(blocks.size());
        std::ifstream in(input_bin_path_, std::ios::binary);
        if (!in.is_open())
            throw std::runtime_error("Cannot open input file for boundary extraction: " + input_bin_path_);
        for (size_t idx = 0; idx < blocks.size(); ++idx)
        {
            if (scan_results[idx].health != BlockHealthStatus::HEALTHY)
                continue;
            const auto & block = blocks[idx];
            const std::streamsize on_disk = static_cast<std::streamsize>(sizeof(Checksum) + block.compressed_size);
            in.seekg(static_cast<std::streamoff>(block.file_offset), std::ios::beg);
            if (!in.good())
                continue;
            std::vector<char> buf(static_cast<size_t>(on_disk));
            in.read(buf.data(), on_disk);
            if (in.gcount() != on_disk)
                continue;
            std::vector<char> decompressed(block.decompressed_size);
            try
            {
                decompressBlock(
                    block.method_byte,
                    buf.data() + sizeof(Checksum),
                    block.compressed_size,
                    decompressed.data(),
                    block.decompressed_size);
                auto first_last = extractFirstLastValue(
                    column_type_name_,
                    decompressed.data(),
                    block.decompressed_size);
                block_boundaries[idx] = std::move(first_last);
            }
            catch (const std::exception &)
            {
                // Leave boundary empty for this block
            }
        }
        logger_.info("Primary-key: extracted boundaries for healthy blocks");
    }

    /// Build a map: old_block_file_offset -> new_block_file_offset (both in .bin byte offsets).
    /// We process blocks in order and advance current_offset by each block's output size,
    /// so replaced blocks (which may have different compressed sizes) automatically produce
    /// correct new offsets for all subsequent blocks. Used to rewrite mark file offsets.
    std::map<uint64_t, uint64_t> offset_remap;

    std::ofstream out_file(output_bin_path, std::ios::binary | std::ios::trunc);
    if (!out_file.is_open())
        throw std::runtime_error("Cannot create output file: " + output_bin_path);

    uint64_t current_offset = 0;
    size_t blocks_replaced = 0;
    const size_t total_blocks = blocks.size();

    for (size_t i = 0; i < blocks.size(); ++i)
    {
        const auto & block = blocks[i];
        const auto & scan = scan_results[i];

        offset_remap[static_cast<uint64_t>(block.file_offset)] = current_offset;

        if (scan.health == BlockHealthStatus::HEALTHY)
        {
            /// Checksum was wrong but decompression succeeded: when --primary-key is used,
            /// validate sort order of decompressed data (bad checksum may mean corrupted data
            /// that violates ORDER BY and would cause MergeTree errors). If the sort order is
            /// violated, keep the longest sorted prefix and fill remaining rows with the last
            /// sorted value (partial salvage).
            if (scan.checksum_was_invalid && !scan.decompressed_data.empty() && primary_key_)
            {
                size_t row_count = estimateRowCount(block, marks);
                const std::vector<char> * prev_block_last_ptr = nullptr;
                const std::vector<char> * next_block_first_ptr = nullptr;
                if (!block_boundaries.empty())
                {
                    for (size_t j = i; j > 0; --j)
                    {
                        if (!block_boundaries[j - 1].first.empty())
                        {
                            prev_block_last_ptr = &block_boundaries[j - 1].second;
                            break;
                        }
                    }
                    for (size_t j = i + 1; j < block_boundaries.size(); ++j)
                    {
                        if (!block_boundaries[j].first.empty())
                        {
                            next_block_first_ptr = &block_boundaries[j].first;
                            break;
                        }
                    }
                }
                size_t partial_rows = 0;
                size_t fill_rows = 0;
                SortViolationInfo violation;
                std::vector<char> mixed = buildPartiallySortedBlock(
                    column_type_name_,
                    scan.decompressed_data.data(),
                    scan.decompressed_data.size(),
                    row_count,
                    prev_block_last_ptr,
                    next_block_first_ptr,
                    partial_rows,
                    fill_rows,
                    violation);

                if (!mixed.empty() && fill_rows == 0)
                {
                    /// Data was fully sorted — use original decompressed data with a fresh checksum.
                    auto replacement = generateBlockFromDecompressed(
                        scan.decompressed_data,
                        block.method_byte);
                    out_file.write(replacement.data(), replacement.size());
                    current_offset += static_cast<uint64_t>(replacement.size());
                    ++blocks_replaced;
                    /// Boundaries from original extraction are still valid (data was sorted).
                    logger_.info("Regenerated block " + std::to_string(block.block_index)
                        + " @ old offset " + std::to_string(block.file_offset)
                        + " -> new offset " + std::to_string(offset_remap[static_cast<uint64_t>(block.file_offset)])
                        + " (checksum was invalid, data recovered, sort order OK)");
                }
                else
                {
                    /// Sort order violated — partial salvage.
                    if (violation.found)
                    {
                        std::string left_str = formatValueForLog(column_type_name_, violation.left_value);
                        std::string right_str = formatValueForLog(column_type_name_, violation.right_value);
                        std::string fill_str = !violation.fill_value.empty()
                            ? formatValueForLog(column_type_name_, violation.fill_value) : "none";
                        std::string msg = "Block " + std::to_string(block.block_index)
                            + ": sort order violated at row " + std::to_string(violation.violation_row)
                            + "/" + std::to_string(violation.rows_in_data)
                            + ", left: " + left_str
                            + ", right: " + right_str
                            + "; fill_value=" + fill_str;
                        logger_.info(msg);
                        std::cerr << "[primary-key] " << msg << std::endl;
                    }

                    std::vector<char> replacement;
                    if (!mixed.empty())
                    {
                        replacement = generateBlockFromDecompressed(mixed, block.method_byte);
                        /// Update boundaries: first value from the sorted prefix, last = fill_value.
                        auto new_bounds = extractFirstLastValue(
                            column_type_name_, mixed.data(), mixed.size());
                        if (!new_bounds.first.empty())
                            block_boundaries[i] = std::move(new_bounds);
                    }
                    if (replacement.empty() && row_count > 0 && prev_block_last_ptr && !prev_block_last_ptr->empty())
                    {
                        size_t fill_width = getTypeWidth();
                        if (fill_width == 0)
                            fill_width = (prev_block_last_ptr->size() <= 32) ? prev_block_last_ptr->size() : 0;
                        std::vector<char> decompressed = serializeValueRepeated(
                            *prev_block_last_ptr,
                            row_count,
                            fill_width);
                        if (!decompressed.empty())
                        {
                            replacement = generateBlockFromDecompressed(decompressed, block.method_byte);
                            /// Block filled with prev_block_last: first=last=prev_block_last.
                            block_boundaries[i] = {*prev_block_last_ptr, *prev_block_last_ptr};
                        }
                    }
                    if (replacement.empty())
                    {
                        replacement = generateDefaultBlock(row_count, block.method_byte);
                        /// Block filled with type default: clear boundaries so subsequent
                        /// blocks skip past this one and find a real value.
                        block_boundaries[i] = {{}, {}};
                    }
                    out_file.write(replacement.data(), replacement.size());
                    current_offset += static_cast<uint64_t>(replacement.size());
                    ++blocks_replaced;
                    std::string prev_str = prev_block_last_ptr && !prev_block_last_ptr->empty()
                        ? formatValueForLog(column_type_name_, *prev_block_last_ptr) : "none";
                    std::string next_str = next_block_first_ptr && !next_block_first_ptr->empty()
                        ? formatValueForLog(column_type_name_, *next_block_first_ptr) : "none";
                    logger_.info("Block " + std::to_string(block.block_index)
                        + " (checksum invalid, sort violated): partial_rows=" + std::to_string(partial_rows)
                        + " fill_rows=" + std::to_string(fill_rows)
                        + " boundaries=[prev_last=" + prev_str + ", next_first=" + next_str + "]");
                    std::cerr << "[primary-key] Block " << block.block_index << ": partial_rows="
                        << partial_rows << " fill_rows=" << fill_rows
                        << " [prev_last=" << prev_str << ", next_first=" << next_str << "]\n";
                }
            }
            else if (scan.checksum_was_invalid && !scan.decompressed_data.empty())
            {
                /// No --primary-key: just regenerate with correct checksum.
                auto replacement = generateBlockFromDecompressed(
                    scan.decompressed_data,
                    block.method_byte);
                out_file.write(replacement.data(), replacement.size());
                current_offset += static_cast<uint64_t>(replacement.size());
                ++blocks_replaced;
                logger_.info("Regenerated block " + std::to_string(block.block_index)
                    + " @ old offset " + std::to_string(block.file_offset)
                    + " -> new offset " + std::to_string(offset_remap[static_cast<uint64_t>(block.file_offset)])
                    + " (checksum was invalid, data recovered)");
            }
            else
            {
                /// Healthy block: copy verbatim from original file (checksum + compressed data)
                std::ifstream in(input_bin_path_, std::ios::binary);
                if (!in.is_open())
                    throw std::runtime_error("Cannot open input file for repair: " + input_bin_path_);

                const auto block_start =
                    static_cast<std::streamoff>(block.file_offset);
                const auto on_disk_size =
                    static_cast<std::streamsize>(sizeof(Checksum) + block.compressed_size);

                in.seekg(block_start, std::ios::beg);
                if (!in.good())
                    throw std::runtime_error("Failed to seek in input file during repair");

                std::vector<char> buffer(static_cast<size_t>(on_disk_size));
                in.read(buffer.data(), on_disk_size);
                std::streamsize got = in.gcount();
                if (got != on_disk_size)
                    throw std::runtime_error("Failed to read full block from input file during repair");

                out_file.write(buffer.data(), got);
                current_offset += static_cast<uint64_t>(got);
            }
        }
        else
        {
            /// Corrupted block: prefer using salvaged partial data when available.
            size_t row_count = estimateRowCount(block, marks);

            bool used_partial = false;
            size_t partial_rows = 0;
            size_t default_rows = 0;

            const std::vector<char> * prev_block_last_ptr = nullptr;
            const std::vector<char> * next_block_first_ptr = nullptr;
            if (primary_key_ && !block_boundaries.empty())
            {
                for (size_t j = i; j > 0; --j)
                {
                    if (!block_boundaries[j - 1].first.empty())
                    {
                        prev_block_last_ptr = &block_boundaries[j - 1].second;
                        break;
                    }
                }
                for (size_t j = i + 1; j < block_boundaries.size(); ++j)
                {
                    if (!block_boundaries[j].first.empty())
                    {
                        next_block_first_ptr = &block_boundaries[j].first;
                        break;
                    }
                }
            }

            if (primary_key_ && scan.health == BlockHealthStatus::DECOMPRESSION_FAILED
                && scan.partial_bytes > 0 && !scan.partial_data.empty() && row_count > 0)
            {
                std::vector<char> picked_value;
                auto pk_buf = buildPrimaryKeyRepairedBuffer(
                    scan,
                    row_count,
                    prev_block_last_ptr,
                    next_block_first_ptr,
                    partial_rows,
                    default_rows,
                    picked_value);
                if (!pk_buf.empty())
                {
                    auto replacement = generateBlockFromDecompressed(pk_buf, block.method_byte);
                    out_file.write(replacement.data(), replacement.size());
                    current_offset += static_cast<uint64_t>(replacement.size());
                    ++blocks_replaced;
                    used_partial = true;

                    /// Update boundaries so subsequent blocks see the effective last value.
                    if (!block_boundaries.empty())
                    {
                        auto new_bounds = extractFirstLastValue(
                            column_type_name_, pk_buf.data(), pk_buf.size());
                        if (!new_bounds.first.empty())
                            block_boundaries[i] = std::move(new_bounds);
                    }

                    std::string prev_str = prev_block_last_ptr && !prev_block_last_ptr->empty()
                        ? formatValueForLog(column_type_name_, *prev_block_last_ptr) : "none";
                    std::string next_str = next_block_first_ptr && !next_block_first_ptr->empty()
                        ? formatValueForLog(column_type_name_, *next_block_first_ptr) : "none";
                    std::string picked_str = formatValueForLog(column_type_name_, picked_value);
                    std::string msg = "Block " + std::to_string(block.block_index)
                        + ": picked_default=" + picked_str
                        + " boundaries=[prev_last=" + prev_str + ", next_first=" + next_str + "]"
                        + " partial_rows=" + std::to_string(partial_rows)
                        + " default_rows=" + std::to_string(default_rows);
                    logger_.info(msg);
                    std::cerr << "[primary-key] " << msg << std::endl;
                }
            }

            if (!used_partial
                && scan.health == BlockHealthStatus::DECOMPRESSION_FAILED
                && scan.partial_bytes > 0
                && !scan.partial_data.empty()
                && row_count > 0)
            {
                auto mixed_decompressed = buildRepairedDecompressedBuffer(
                    scan,
                    row_count,
                    partial_rows,
                    default_rows);

                if (!mixed_decompressed.empty())
                {
                    auto replacement = generateBlockFromDecompressed(
                        mixed_decompressed,
                        block.method_byte);

                    out_file.write(replacement.data(), replacement.size());
                    current_offset += static_cast<uint64_t>(replacement.size());
                    ++blocks_replaced;
                    used_partial = true;

                    logger_.info("Replaced block " + std::to_string(block.block_index)
                        + " @ old offset " + std::to_string(block.file_offset)
                        + " -> new offset " + std::to_string(offset_remap[static_cast<uint64_t>(block.file_offset)])
                        + " using salvaged data for " + std::to_string(partial_rows)
                        + " rows and default values for " + std::to_string(default_rows)
                        + " rows (partial_bytes=" + std::to_string(scan.partial_bytes) + ")");
                }
            }

            if (!used_partial)
            {
                std::vector<char> replacement;
                if (primary_key_ && row_count > 0 && prev_block_last_ptr && !prev_block_last_ptr->empty())
                {
                    size_t fill_width = getTypeWidth();
                    if (fill_width == 0)
                        fill_width = (prev_block_last_ptr->size() <= 32) ? prev_block_last_ptr->size() : 0;
                    std::vector<char> decompressed = serializeValueRepeated(
                        *prev_block_last_ptr,
                        row_count,
                        fill_width);
                    if (!decompressed.empty())
                    {
                        replacement = generateBlockFromDecompressed(decompressed, block.method_byte);
                        /// Update boundaries: block filled with prev_block_last.
                        if (!block_boundaries.empty())
                            block_boundaries[i] = {*prev_block_last_ptr, *prev_block_last_ptr};
                    }
                }
                if (replacement.empty())
                {
                    replacement = generateDefaultBlock(row_count, block.method_byte);
                    /// Type-default block: clear boundaries so subsequent blocks skip past.
                    if (!block_boundaries.empty())
                        block_boundaries[i] = {{}, {}};
                }

                out_file.write(replacement.data(), replacement.size());
                current_offset += static_cast<uint64_t>(replacement.size());
                ++blocks_replaced;

                std::string reason;
                if (primary_key_)
                {
                    std::string prev_str = prev_block_last_ptr && !prev_block_last_ptr->empty()
                        ? formatValueForLog(column_type_name_, *prev_block_last_ptr) : "none";
                    std::string next_str = next_block_first_ptr && !next_block_first_ptr->empty()
                        ? formatValueForLog(column_type_name_, *next_block_first_ptr) : "none";
                    reason = ", new block: picked_default=prev_last=" + prev_str
                        + " next_first=" + next_str;
                    std::cerr << "[primary-key] new block " << block.block_index << ": rows=" << row_count
                        << " boundaries=[prev_last=" << prev_str << ", next_first=" << next_str << "]" << std::endl;
                }
                else if (scan.partial_bytes > 0 && scan.health == BlockHealthStatus::DECOMPRESSION_FAILED)
                    reason = ", partial data could not be safely used";
                logger_.info("Replaced block " + std::to_string(block.block_index)
                    + " @ old offset " + std::to_string(block.file_offset)
                    + " -> new offset " + std::to_string(offset_remap[static_cast<uint64_t>(block.file_offset)])
                    + " (" + std::to_string(row_count) + " default rows" + reason + ")");
            }
        }

        // Lightweight progress indicator on stderr: update every 1000 blocks
        // to avoid excessive stderr chatter for very large parts.
        if ((i + 1) % 1000 == 0)
        {
            std::cerr << "\r[Repair] Writing block " << (i + 1)
                      << " / " << total_blocks << std::flush;
        }
    }

    if (total_blocks > 0)
        std::cerr << "\r[Repair] Writing block " << total_blocks
                  << " / " << total_blocks << std::endl;

    out_file.close();
    logger_.info("Wrote repaired .bin file: " + output_bin_path
        + " (" + std::to_string(blocks_replaced) + " blocks replaced)");

    /// --- Mark file regeneration ---
    if (!output_mark_path.empty() && !marks.empty())
    {
        // Determine the desired output mark format (.mrk2 vs .cmrk2) from the
        // original mark file path. The ClickHouse reference implementation
        // distinguishes compressed (.cmrk2) vs uncompressed (.mrk2) marks
        // purely by extension, so we mirror that here instead of relying on
        // the extension of output_mark_path (which typically ends with
        // ".repaired").
        bool original_marks_compressed = false;
        if (!mark_file_path.empty())
        {
            std::string ext = std::filesystem::path(mark_file_path).extension().string();
            if (ext == ".cmrk2")
                original_marks_compressed = true;
        }

        /// Remap each mark's block offset to the repaired .bin layout. offset_remap was
        /// built in write order (each key = original block start, value = new block start),
        /// so updated block sizes and all subsequent block offsets are already reflected.
        std::vector<MarkEntry> new_marks;
        new_marks.reserve(marks.size());

        for (const auto & mark : marks)
        {
            MarkEntry new_mark = mark;

            auto it = offset_remap.find(mark.offset_in_compressed_file);
            if (it != offset_remap.end())
            {
                new_mark.offset_in_compressed_file = it->second;
            }
            else
            {
                /// Mark points to an offset we don't know about.
                /// This could mean the mark file references a different block layout.
                /// Keep original offset and warn.
                logger_.warn("Mark entry references unknown block offset "
                    + std::to_string(mark.offset_in_compressed_file)
                    + ", keeping original offset");
            }

            /// offset_in_decompressed_block and rows_count are unchanged — they are
            /// relative to the decompressed block content, not file positions.

            new_marks.push_back(new_mark);
        }

        MarkFileHandler mark_handler(logger_);
        if (original_marks_compressed)
            mark_handler.writeCmrk2(output_mark_path, new_marks);
        else
            mark_handler.writeMrk2(output_mark_path, new_marks);

        logger_.info("Wrote repaired mark file: " + output_mark_path);
    }
    else if (output_mark_path.empty() && !marks.empty())
    {
        logger_.info("Skipping mark file regeneration (no output path specified)");
    }
}

} // namespace PartRepair
