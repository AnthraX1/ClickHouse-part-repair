/// Thin compression/decompression wrapper dispatching on method byte
/// to direct LZ4/ZSTD library calls.
/// Replaces ClickHouse's CompressionCodecFactory.

#pragma once

#include <cstdint>
#include <stdexcept>
#include <vector>

namespace PartRepair
{

/// Decompress a ClickHouse compressed block.
/// @param method_byte  Compression method byte from the block header.
/// @param compressed   Pointer to the start of the compressed block (method_byte position).
/// @param compressed_size  Total compressed size including the 9-byte header.
/// @param dest         Output buffer, must be at least decompressed_size bytes.
/// @param decompressed_size  Expected decompressed size from the block header.
/// @throws std::runtime_error on decompression failure or unsupported codec.
void decompressBlock(
    uint8_t method_byte,
    const char * compressed,
    uint32_t compressed_size,
    char * dest,
    uint32_t decompressed_size);

/// Compress data into a ClickHouse compressed block format.
/// Returns the full compressed block bytes: [method_byte][compressed_size_le32][decompressed_size_le32][payload].
/// Does NOT include the 16-byte checksum prefix — caller must prepend that.
/// @param method_byte  Compression method to use (LZ4 or ZSTD).
/// @param data         Input data to compress.
/// @param data_size    Size of the input data.
/// @returns Vector containing the full compressed block (header + payload).
/// @throws std::runtime_error on compression failure or unsupported codec.
std::vector<char> compressBlock(
    uint8_t method_byte,
    const char * data,
    uint32_t data_size);

} // namespace PartRepair
