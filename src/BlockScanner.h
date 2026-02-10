#pragma once

#include "BlockIterator.h"
#include "Utils.h"

#include <string>
#include <vector>

namespace PartRepair
{

/// Result of scanning/validating one block.
enum class BlockHealthStatus
{
    HEALTHY,                /// Checksum OK (or skipped), decompression OK
    CHECKSUM_MISMATCH,      /// Checksum failed, decompression not attempted
    DECOMPRESSION_FAILED,   /// Checksum OK/skipped, but decompression failed (partial data may be salvaged)
    HEADER_CORRUPT,         /// Block header itself is corrupt
    UNREADABLE              /// Block couldn't be read from file
};

struct ScanResult
{
    size_t block_index = 0;
    size_t file_offset = 0;
    uint32_t compressed_size = 0;
    uint32_t decompressed_size = 0;
    std::string codec_name;

    BlockHealthStatus health = BlockHealthStatus::HEALTHY;
    std::string detail;

    /// For healthy blocks: full decompressed data.
    std::vector<char> decompressed_data;

    /// For corrupted blocks: partial decompressed data salvaged before error.
    std::vector<char> partial_data;
    size_t partial_bytes = 0;

    /// File paths of salvaged outputs (populated during scan).
    std::string partial_data_file;
    std::string raw_block_file;
};

/// Scans all blocks in a .bin file, validates checksums, attempts decompression,
/// and salvages partial data from corrupted blocks.
class BlockScanner
{
public:
    BlockScanner(bool skip_checksum,
                 const std::string & output_dir,
                 Logger & logger,
                 const std::string & input_bin_path);

    /// Scan all blocks. Returns per-block results.
    std::vector<ScanResult> scan(const std::vector<BlockInfo> & blocks);

private:
    bool skip_checksum_;
    std::string output_dir_;
    Logger & logger_;
    std::string input_bin_path_;

    /// Validate checksum of a single block. Returns true if OK.
    bool validateChecksum(const BlockInfo & block);

    /// Attempt full decompression. On success, populates result.decompressed_data.
    /// On failure, attempts partial decompression and populates result.partial_data.
    void attemptDecompression(const BlockInfo & block, ScanResult & result);

    /// Write salvage files for a corrupted block.
    void writeSalvageFiles(const BlockInfo & block, ScanResult & result);
};

} // namespace PartRepair
