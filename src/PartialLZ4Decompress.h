#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

namespace PartRepair
{

/// Result of a partial decompression attempt.
struct PartialDecompressResult
{
    bool success = false;       /// True if decompression completed fully without error
    size_t bytes_written = 0;   /// Number of valid bytes written to dest before success or failure
};

/// Attempt LZ4 decompression of a single block, tracking how many bytes
/// were successfully written to the output before any error.
/// This is a modified copy of ClickHouse's LZ4::decompressImpl that returns
/// partial progress instead of just bool.
///
/// Both source and dest buffers must have ADDITIONAL_BYTES_AT_END_OF_BUFFER (64)
/// bytes of padding after their logical ends.
PartialDecompressResult partialLZ4Decompress(
    const char * source, size_t source_size,
    char * dest, size_t dest_size);

/// Attempt ZSTD decompression in streaming mode, tracking partial output.
/// Uses ZSTD_decompressStream() which naturally reports bytes written before failure.
PartialDecompressResult partialZSTDDecompress(
    const char * source, size_t source_size,
    char * dest, size_t dest_size);

} // namespace PartRepair
