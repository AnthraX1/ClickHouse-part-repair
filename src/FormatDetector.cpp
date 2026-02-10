#include "FormatDetector.h"

#include <cstring>
#include <map>
#include <set>
#include <sstream>
#include <algorithm>

namespace PartRepair
{

FormatDetector::FormatDetector(Logger & logger) : logger_(logger) {}

std::optional<std::pair<uint64_t, size_t>> FormatDetector::tryReadVarUInt(const char * data, size_t available) const
{
    /// VarUInt: base-128 VLQ encoding. MSB=1 means continuation.
    uint64_t result = 0;
    size_t i = 0;
    unsigned shift = 0;
    while (i < available)
    {
        uint8_t byte = static_cast<uint8_t>(data[i]);
        result |= (static_cast<uint64_t>(byte & 0x7F)) << shift;
        ++i;
        if ((byte & 0x80) == 0)
            return std::make_pair(result, i);
        shift += 7;
        if (shift >= 64)
            return std::nullopt; // overflow
    }
    return std::nullopt; // ran out of data
}

bool FormatDetector::looksLikeStringData(const char * data, size_t size) const
{
    /// Try to parse as a sequence of (VarUInt length, then `length` bytes of string data).
    /// If we can parse to the end without errors, it's likely String.
    size_t pos = 0;
    size_t string_count = 0;
    while (pos < size)
    {
        auto varint = tryReadVarUInt(data + pos, size - pos);
        if (!varint.has_value())
            return false;

        auto [str_len, varint_bytes] = varint.value();
        pos += varint_bytes;

        // Sanity check: string length shouldn't be absurdly large
        if (str_len > size - pos)
            return false;

        pos += str_len;
        ++string_count;
    }

    return pos == size && string_count > 0;
}

size_t FormatDetector::sumRowsForBlock(size_t block_file_offset, const std::vector<MarkEntry> & marks) const
{
    size_t total_rows = 0;
    for (const auto & mark : marks)
    {
        if (mark.offset_in_compressed_file == block_file_offset)
            total_rows += mark.rows_count;
    }
    return total_rows;
}

bool FormatDetector::isConsistentFixedWidth(
    const std::vector<ScanResult> & healthy_blocks,
    size_t type_width,
    const std::vector<MarkEntry> * marks) const
{
    if (type_width == 0)
        return false;

    for (const auto & block : healthy_blocks)
    {
        if (block.health != BlockHealthStatus::HEALTHY || block.decompressed_data.empty())
            continue;

        size_t data_size = block.decompressed_data.size();

        // Decompressed size must be divisible by type_width
        if (data_size % type_width != 0)
            return false;

        // If marks available, check row count consistency
        if (marks && !marks->empty())
        {
            size_t rows_from_marks = sumRowsForBlock(block.file_offset, *marks);
            if (rows_from_marks > 0)
            {
                size_t expected_bytes = rows_from_marks * type_width;
                if (expected_bytes != data_size)
                    return false;
            }
        }
    }

    return true;
}

bool FormatDetector::isConsistentNullableFixedWidth(
    const std::vector<ScanResult> & scan_results,
    size_t nested_type_width,
    const std::vector<MarkEntry> * marks) const
{
    if (nested_type_width == 0)
        return false;

    bool saw_any = false;

    for (const auto & block : scan_results)
    {
        if (block.health != BlockHealthStatus::HEALTHY || block.decompressed_data.empty())
            continue;

        const size_t data_size = block.decompressed_data.size();
        if (data_size == 0)
            continue;

        size_t rows = 0;

        if (marks && !marks->empty())
        {
            rows = sumRowsForBlock(block.file_offset, *marks);
            if (rows == 0)
                continue;

            const size_t expected = rows * (nested_type_width + 1);
            if (data_size != expected)
                return false;
        }
        else
        {
            const size_t bytes_per_row = nested_type_width + 1;
            if (data_size % bytes_per_row != 0)
                return false;
            rows = data_size / bytes_per_row;
        }

        if (rows == 0)
            return false;

        // First `rows` bytes must look like a null map: values 0 or 1 only.
        const char * data = block.decompressed_data.data();
        for (size_t i = 0; i < rows; ++i)
        {
            unsigned char v = static_cast<unsigned char>(data[i]);
            if (v != 0 && v != 1)
                return false;
        }

        saw_any = true;
    }

    return saw_any;
}

bool FormatDetector::isConsistentNullableString(
    const std::vector<ScanResult> & scan_results,
    const std::vector<MarkEntry> * marks) const
{
    if (!marks || marks->empty())
        return false;

    bool saw_any = false;

    for (const auto & block : scan_results)
    {
        if (block.health != BlockHealthStatus::HEALTHY || block.decompressed_data.empty())
            continue;

        size_t rows = sumRowsForBlock(block.file_offset, *marks);
        if (rows == 0)
            continue;

        const size_t data_size = block.decompressed_data.size();
        if (data_size <= rows)
            return false;

        const char * data = block.decompressed_data.data();

        // Check null map.
        for (size_t i = 0; i < rows; ++i)
        {
            unsigned char v = static_cast<unsigned char>(data[i]);
            if (v != 0 && v != 1)
                return false;
        }

        const char * nested_data = data + rows;
        size_t nested_size = data_size - rows;

        if (!looksLikeStringData(nested_data, nested_size))
            return false;

        saw_any = true;
    }

    return saw_any;
}

std::string FormatDetector::detect(
    const std::vector<ScanResult> & scan_results,
    const std::vector<MarkEntry> * marks)
{
    // Collect healthy blocks with decompressed data
    std::vector<const ScanResult *> healthy;
    for (const auto & r : scan_results)
    {
        if (r.health == BlockHealthStatus::HEALTHY && !r.decompressed_data.empty())
            healthy.push_back(&r);
    }

    if (healthy.empty())
    {
        logger_.warn("FormatDetector: no healthy blocks with decompressed data available.");
        return "";
    }

    logger_.info("FormatDetector: analyzing " + std::to_string(healthy.size()) + " healthy blocks.");

    // Collect candidate type widths from all healthy blocks
    // If marks are available, use rows to compute exact type width
    std::map<size_t, size_t> width_votes; // type_width -> count of blocks that are consistent

    if (marks && !marks->empty())
    {
        for (const auto * block : healthy)
        {
            size_t rows = sumRowsForBlock(block->file_offset, *marks);
            if (rows > 0 && block->decompressed_data.size() > 0)
            {
                size_t width = block->decompressed_data.size() / rows;
                if (width * rows == block->decompressed_data.size() && width > 0 && width <= 32)
                    width_votes[width]++;
            }
        }
    }

    // Check common fixed-size types
    struct TypeCandidate
    {
        std::string name;
        size_t width;
    };
    static const std::vector<TypeCandidate> fixed_types = {
        {"UInt8", 1}, {"Int8", 1},
        {"UInt16", 2}, {"Int16", 2}, {"Date", 2},
        {"UInt32", 4}, {"Int32", 4}, {"Float32", 4}, {"DateTime", 4},
        {"UInt64", 8}, {"Int64", 8}, {"Float64", 8}, {"DateTime64", 8},
        {"UInt128", 16}, {"Int128", 16},
        {"UInt256", 32}, {"Int256", 32},
    };

    std::vector<std::string> candidates;

    for (const auto & ft : fixed_types)
    {
        const bool fixed_ok = isConsistentFixedWidth(scan_results, ft.width, marks);
        const bool nullable_ok = isConsistentNullableFixedWidth(scan_results, ft.width, marks);

        if (fixed_ok)
            candidates.push_back(ft.name);
        if (nullable_ok)
            candidates.push_back("Nullable(" + ft.name + ")");
    }

    // Check if data looks like String (VarUInt-prefixed)
    bool all_look_like_strings = true;
    for (const auto * block : healthy)
    {
        if (!looksLikeStringData(block->decompressed_data.data(), block->decompressed_data.size()))
        {
            all_look_like_strings = false;
            break;
        }
    }
    if (all_look_like_strings)
        candidates.push_back("String");

    // Nullable(String) detection using marks (requires row counts).
    if (isConsistentNullableString(scan_results, marks))
        candidates.push_back("Nullable(String)");

    if (candidates.empty())
    {
        logger_.warn("FormatDetector: could not detect column type. Please specify --format.");
        return "";
    }

    // If we have mark-based width votes, prefer the type matching the most-voted width
    if (!width_votes.empty())
    {
        auto best = std::max_element(width_votes.begin(), width_votes.end(),
            [](const auto & a, const auto & b) { return a.second < b.second; });

        size_t best_width = best->first;
        // Filter candidates to those matching best_width
        std::vector<std::string> filtered;
        for (const auto & c : candidates)
        {
            for (const auto & ft : fixed_types)
            {
                if (ft.name == c && ft.width == best_width)
                {
                    filtered.push_back(c);
                    break;
                }
            }
        }
        if (!filtered.empty())
            candidates = filtered;
    }

    // If multiple candidates remain, prefer unsigned types, then pick the first
    if (candidates.size() > 1)
    {
        logger_.warn("FormatDetector: multiple candidates: " + [&]{
            std::string s;
            for (size_t i = 0; i < candidates.size(); ++i)
            {
                if (i > 0) s += ", ";
                s += candidates[i];
            }
            return s;
        }() + ". Choosing first candidate. Use --format for precision.");
    }

    logger_.info("FormatDetector: detected type = " + candidates.front());
    return candidates.front();
}

} // namespace PartRepair
