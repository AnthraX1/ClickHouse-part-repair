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

} // anonymous namespace

BlockRepairer::BlockRepairer(
    const std::string & column_type_name,
    Logger & logger,
    const std::string & input_bin_path,
    const std::string & default_value_literal,
    bool default_null)
    : column_type_name_(column_type_name)
    , logger_(logger)
    , input_bin_path_(input_bin_path)
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
            /// Checksum was wrong but decompression succeeded: regenerate block so
            /// the repaired file has a correct checksum instead of copying the bad one.
            if (scan.checksum_was_invalid && !scan.decompressed_data.empty())
            {
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

            if (scan.health == BlockHealthStatus::DECOMPRESSION_FAILED
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
                auto replacement = generateDefaultBlock(row_count, block.method_byte);

                out_file.write(replacement.data(), replacement.size());
                current_offset += static_cast<uint64_t>(replacement.size());
                ++blocks_replaced;

                logger_.info("Replaced block " + std::to_string(block.block_index)
                    + " @ old offset " + std::to_string(block.file_offset)
                    + " -> new offset " + std::to_string(offset_remap[static_cast<uint64_t>(block.file_offset)])
                    + " (" + std::to_string(row_count) + " default rows"
                    + (scan.partial_bytes > 0 && scan.health == BlockHealthStatus::DECOMPRESSION_FAILED
                           ? ", partial data could not be safely used"
                           : "")
                    + ")");
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
