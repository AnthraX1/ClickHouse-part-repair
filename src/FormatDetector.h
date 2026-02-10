#pragma once

#include "BlockScanner.h"
#include "MarkFileHandler.h"
#include "Utils.h"

#include <optional>
#include <string>
#include <vector>

namespace PartRepair
{

/// Attempts to detect the column data type from decompressed data of healthy blocks.
class FormatDetector
{
public:
    explicit FormatDetector(Logger & logger);

    /// Attempt to detect the column type from healthy blocks.
    /// If marks are available, uses row counts for stronger detection.
    /// Returns the type name (e.g., "UInt64", "String", "Nullable(UInt64)")
    /// or empty string if ambiguous.
    std::string detect(
        const std::vector<ScanResult> & scan_results,
        const std::vector<MarkEntry> * marks = nullptr);

private:
    Logger & logger_;

    /// Check if decompressed data is consistent with a fixed-size numeric type of given width.
    bool isConsistentFixedWidth(
        const std::vector<ScanResult> & healthy_blocks,
        size_t type_width,
        const std::vector<MarkEntry> * marks) const;

    /// Check if decompressed data is consistent with a Nullable(fixed-width)
    /// numeric type of given nested width. For Nullable(T), the on-disk layout
    /// is: [null_map: 1 byte per row][nested: T bytes per row].
    bool isConsistentNullableFixedWidth(
        const std::vector<ScanResult> & scan_results,
        size_t nested_type_width,
        const std::vector<MarkEntry> * marks) const;

    /// Check if decompressed data is consistent with Nullable(String):
    /// first N bytes are a null map (0/1), followed by a String column
    /// (VarUInt length + bytes per row). Requires marks to obtain N.
    bool isConsistentNullableString(
        const std::vector<ScanResult> & scan_results,
        const std::vector<MarkEntry> * marks) const;

    /// Check if decompressed data looks like VarUInt-prefixed strings.
    bool looksLikeStringData(const char * data, size_t size) const;

    /// Try to read a VarUInt from raw bytes. Returns (value, bytes_consumed) or nullopt on failure.
    std::optional<std::pair<uint64_t, size_t>> tryReadVarUInt(const char * data, size_t available) const;

    /// Sum rows from marks that point into a specific compressed block offset.
    size_t sumRowsForBlock(size_t block_file_offset, const std::vector<MarkEntry> & marks) const;
};

} // namespace PartRepair
