#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "compat/compression_defs.h"
#include <city.h> // CityHash_v1_0_2::uint128

namespace PartRepair
{

/// Checksum type — matches ClickHouse's definition in CompressedReadBufferBase.cpp
using Checksum = CityHash_v1_0_2::uint128;

/// Status of a single compressed block after parsing.
enum class BlockStatus
{
    OK,               /// Header parsed, raw data read successfully
    HEADER_READ_ERROR, /// Could not read checksum + header (truncated file, etc.)
    HEADER_CORRUPT,   /// Header readable but values nonsensical (bad method byte, absurd sizes)
    PAYLOAD_READ_ERROR /// Header OK but could not read full compressed payload
};

/// Represents one compressed block as read from a .bin file.
struct BlockInfo
{
    size_t block_index = 0;          /// 0-based sequential index
    size_t file_offset = 0;          /// Byte offset of checksum start in the .bin file

    /// Parsed header fields
    Checksum stored_checksum{};
    uint8_t method_byte = 0;
    uint32_t compressed_size = 0;    /// From header: includes BLOCK_HEADER_SIZE
    uint32_t decompressed_size = 0;  /// From header: expected decompressed size

    BlockStatus status = BlockStatus::OK;
    std::string error_message;

    /// Total on-disk size of this block: sizeof(Checksum) + compressed_size.
    size_t totalOnDiskSize() const { return sizeof(Checksum) + compressed_size; }

    /// Full codec description from method byte. Empty if method_byte is unknown.
    std::string codecName() const;

    /// Whether this is a known compression method byte.
    static bool isKnownMethodByte(uint8_t byte);
};

/// Iterates over compressed blocks in a .bin file sequentially.
/// Handles errors gracefully: corrupted/truncated blocks are recorded rather than throwing.
class BlockIterator
{
public:
    /// Open a .bin file for block iteration.
    /// Throws on file-not-found or permission errors.
    explicit BlockIterator(const std::string & bin_file_path);

    /// Read all blocks from the file. Returns the vector of BlockInfo.
    ///
    /// @param mark_offsets
    ///     Optional sorted list of known block start offsets in the .bin file
    ///     (typically derived from a mark file). When provided, the iterator
    ///     can continue past header errors by jumping to the next known offset.
    ///
    /// @param enable_bruteforce
    ///     When true and no mark offsets are available, the iterator will
    ///     attempt a best‑effort bruteforce search for plausible next block
    ///     headers after a corrupted header, using statistics from previously
    ///     seen healthy blocks.
    std::vector<BlockInfo> readAllBlocks(
        const std::vector<uint64_t> * mark_offsets = nullptr,
        bool enable_bruteforce = false);

    /// File size in bytes.
    size_t fileSize() const { return file_size_; }

private:
    std::string file_path_;
    size_t file_size_ = 0;

    /// Try to read one block starting at the given offset.
    /// Returns the block info with status indicating success or failure.
    BlockInfo readOneBlock(const char * mapped_data, size_t offset, size_t block_index);
};

} // namespace PartRepair
