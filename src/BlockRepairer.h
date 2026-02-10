#pragma once

#include "BlockIterator.h"
#include "BlockScanner.h"
#include "MarkFileHandler.h"
#include "Utils.h"

#include <string>
#include <vector>

namespace PartRepair
{

/// Repairs a corrupted .bin file by replacing corrupted blocks with blocks
/// containing default values of the specified column type.
/// Also regenerates the associated mark file with updated offsets.
class BlockRepairer
{
public:
    BlockRepairer(
        const std::string & column_type_name,
        Logger & logger,
        const std::string & input_bin_path,
        const std::string & default_value_literal,
        bool default_null);

    /// Repair the .bin file.
    /// - blocks: original blocks from BlockIterator
    /// - scan_results: per-block scan results from BlockScanner
    /// - marks: original mark entries (may be empty if no mark file)
    /// - mark_file_path: original mark file path (for determining output format)
    /// - output_bin_path: path for the repaired .bin file
    /// - output_mark_path: path for the repaired mark file (empty to skip)
    void repair(
        const std::vector<BlockInfo> & blocks,
        const std::vector<ScanResult> & scan_results,
        const std::vector<MarkEntry> & marks,
        const std::string & mark_file_path,
        const std::string & output_bin_path,
        const std::string & output_mark_path);

private:
    enum class DefaultMode
    {
        TypeDefault,   /// Use the type's built-in default (existing behavior).
        ExplicitValue, /// Use a user-specified value from --default-value.
        NullValue      /// Use NULL for every repaired row (Nullable(...) only).
    };

    std::string column_type_name_;
    Logger & logger_;
    std::string input_bin_path_;
    DefaultMode default_mode_ = DefaultMode::TypeDefault;
    std::string default_value_literal_;

    /// Serialize `row_count` default values according to the configured
    /// default mode (type default / explicit value / NULL for Nullable).
    /// Returns a decompressed buffer in ClickHouse's native binary format.
    std::vector<char> serializeDefaultValues(size_t row_count) const;

    /// Generate a compressed block containing `row_count` default values
    /// of the configured column type, using the specified compression codec.
    /// Returns the full on-disk block bytes: [checksum][header][payload].
    std::vector<char> generateDefaultBlock(
        size_t row_count,
        uint8_t method_byte);

    /// Build a decompressed buffer for a corrupted block by combining salvaged
    /// data from ScanResult::partial_data with default values up to row_count.
    /// Returns an empty vector if partial data cannot be safely used (e.g.
    /// variable-width types or zero recovered rows).
    std::vector<char> buildRepairedDecompressedBuffer(
        const ScanResult & scan,
        size_t row_count,
        size_t & partial_rows_out,
        size_t & default_rows_out);

    /// Compress an arbitrary decompressed buffer into an on-disk block with
    /// checksum, mirroring generateDefaultBlock semantics.
    std::vector<char> generateBlockFromDecompressed(
        const std::vector<char> & decompressed,
        uint8_t method_byte);

    /// Determine how many rows a corrupted block should contain.
    /// Uses marks if available, otherwise estimates from decompressed_size and type width.
    size_t estimateRowCount(
        const BlockInfo & block,
        const std::vector<MarkEntry> & marks) const;

    /// Get the byte width of the column type, or 0 for variable-width types.
    size_t getTypeWidth() const;
};

} // namespace PartRepair
