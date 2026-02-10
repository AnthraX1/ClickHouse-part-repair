/// Implementation of codec.h — dispatches to LZ4/ZSTD system libraries.

#include "codec.h"
#include "compression_defs.h"

#include <cstring>
#include <sstream>

#include <lz4.h>
#include <zstd.h>

namespace PartRepair
{

void decompressBlock(
    uint8_t method_byte,
    const char * compressed,
    uint32_t compressed_size,
    char * dest,
    uint32_t decompressed_size)
{
    if (compressed_size < BLOCK_HEADER_SIZE)
        throw std::runtime_error("Compressed block too small for header");

    const char * payload = compressed + BLOCK_HEADER_SIZE;
    const uint32_t payload_size = compressed_size - BLOCK_HEADER_SIZE;

    auto method = static_cast<CompressionMethod>(method_byte);

    switch (method)
    {
        case CompressionMethod::LZ4:
        {
            int result = LZ4_decompress_safe(payload, dest, static_cast<int>(payload_size), static_cast<int>(decompressed_size));
            if (result < 0)
                throw std::runtime_error("LZ4 decompression failed (error code: " + std::to_string(result) + ")");
            if (static_cast<uint32_t>(result) != decompressed_size)
                throw std::runtime_error("LZ4 decompression size mismatch: expected "
                    + std::to_string(decompressed_size) + ", got " + std::to_string(result));
            break;
        }

        case CompressionMethod::ZSTD:
        case CompressionMethod::ZSTD_QPL:
        {
            size_t result = ZSTD_decompress(dest, decompressed_size, payload, payload_size);
            if (ZSTD_isError(result))
                throw std::runtime_error("ZSTD decompression failed: " + std::string(ZSTD_getErrorName(result)));
            if (static_cast<uint32_t>(result) != decompressed_size)
                throw std::runtime_error("ZSTD decompression size mismatch: expected "
                    + std::to_string(decompressed_size) + ", got " + std::to_string(result));
            break;
        }

        case CompressionMethod::NONE:
        {
            if (payload_size != decompressed_size)
                throw std::runtime_error("NONE codec: payload size (" + std::to_string(payload_size)
                    + ") != decompressed size (" + std::to_string(decompressed_size) + ")");
            std::memcpy(dest, payload, decompressed_size);
            break;
        }

        default:
        {
            const char * name = compressionMethodName(method_byte);
            std::string method_str;
            if (name)
                method_str = name;
            else
            {
                std::ostringstream s;
                s << "0x" << std::hex << static_cast<int>(method_byte);
                method_str = s.str();
            }
            throw std::runtime_error("Unsupported compression codec: " + method_str
                + ". Only LZ4, ZSTD, and NONE are supported for decompression.");
        }
    }
}

std::vector<char> compressBlock(
    uint8_t method_byte,
    const char * data,
    uint32_t data_size)
{
    auto method = static_cast<CompressionMethod>(method_byte);

    // For unsupported codecs, fall back to LZ4 (ClickHouse can read any codec)
    if (method != CompressionMethod::LZ4 && method != CompressionMethod::ZSTD
        && method != CompressionMethod::ZSTD_QPL && method != CompressionMethod::NONE)
    {
        method = CompressionMethod::LZ4;
        method_byte = static_cast<uint8_t>(CompressionMethod::LZ4);
    }

    std::vector<char> result;

    if (method == CompressionMethod::NONE)
    {
        uint32_t compressed_size = BLOCK_HEADER_SIZE + data_size;
        result.resize(compressed_size);

        result[0] = static_cast<char>(method_byte);
        std::memcpy(result.data() + 1, &compressed_size, sizeof(uint32_t));
        std::memcpy(result.data() + 5, &data_size, sizeof(uint32_t));
        std::memcpy(result.data() + BLOCK_HEADER_SIZE, data, data_size);
    }
    else if (method == CompressionMethod::LZ4)
    {
        int max_compressed = LZ4_compressBound(static_cast<int>(data_size));
        result.resize(BLOCK_HEADER_SIZE + max_compressed);

        int compressed_payload = LZ4_compress_default(
            data, result.data() + BLOCK_HEADER_SIZE,
            static_cast<int>(data_size), max_compressed);
        if (compressed_payload <= 0)
            throw std::runtime_error("LZ4 compression failed");

        uint32_t compressed_size = BLOCK_HEADER_SIZE + static_cast<uint32_t>(compressed_payload);
        result.resize(compressed_size);

        result[0] = static_cast<char>(method_byte);
        std::memcpy(result.data() + 1, &compressed_size, sizeof(uint32_t));
        std::memcpy(result.data() + 5, &data_size, sizeof(uint32_t));
    }
    else // ZSTD or ZSTD_QPL
    {
        size_t max_compressed = ZSTD_compressBound(data_size);
        result.resize(BLOCK_HEADER_SIZE + max_compressed);

        size_t compressed_payload = ZSTD_compress(
            result.data() + BLOCK_HEADER_SIZE, max_compressed,
            data, data_size, 1 /* default level */);
        if (ZSTD_isError(compressed_payload))
            throw std::runtime_error("ZSTD compression failed: " + std::string(ZSTD_getErrorName(compressed_payload)));

        uint32_t compressed_size = BLOCK_HEADER_SIZE + static_cast<uint32_t>(compressed_payload);
        result.resize(compressed_size);

        result[0] = static_cast<char>(method_byte);
        std::memcpy(result.data() + 1, &compressed_size, sizeof(uint32_t));
        std::memcpy(result.data() + 5, &data_size, sizeof(uint32_t));
    }

    return result;
}

} // namespace PartRepair
