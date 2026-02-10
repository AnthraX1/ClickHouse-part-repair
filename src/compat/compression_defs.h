/// Standalone replacements for ClickHouse compression format constants.
/// These define the on-disk compressed block format which has been stable
/// since ClickHouse was open-sourced.
///
/// Block layout on disk:
///   [checksum: 16 bytes (CityHash128)]
///   [method_byte: 1 byte]
///   [compressed_size: 4 bytes LE (includes 9-byte header)]
///   [decompressed_size: 4 bytes LE]
///   [payload: compressed_size - 9 bytes]

#pragma once

#include <cstdint>
#include <cstring>

namespace PartRepair
{

/// Size of the compressed block header: method_byte(1) + compressed_size(4) + decompressed_size(4)
/// Matches ClickHouse's COMPRESSED_BLOCK_HEADER_SIZE.
static constexpr uint8_t BLOCK_HEADER_SIZE = 9;

/// Maximum allowed compressed block size (1 GiB).
/// Matches ClickHouse's DBMS_MAX_COMPRESSED_SIZE.
static constexpr uint32_t MAX_COMPRESSED_SIZE = 0x40000000;

/// Compression method byte values.
/// Matches ClickHouse's CompressionMethodByte enum.
/// New values are only ever appended; existing values never change.
enum class CompressionMethod : uint8_t
{
    Delta           = 0x01,
    NONE            = 0x02,
    T64             = 0x10,
    FPC             = 0x11,
    AES_128_GCM_SIV = 0x12,
    AES_256_GCM_SIV = 0x13,
    DoubleDelta     = 0x14,
    Gorilla         = 0x15,
    GCD             = 0x16,
    DeflateQpl      = 0x41,
    ZSTD_QPL        = 0x42,
    LZ4             = 0x82,
    ZSTD            = 0x90,
    Multiple        = 0x91,
};

/// Load a little-endian integer from a potentially unaligned address.
/// Replaces ClickHouse's unalignedLoadLittleEndian<T>() from <base/unaligned.h>.
/// Assumes little-endian host (x86-64 / ARM64-LE).
template <typename T>
inline T loadLittleEndian(const void * p)
{
    T val;
    std::memcpy(&val, p, sizeof(T));
    return val;
}

/// Check if a byte is a known compression method.
inline bool isKnownCompressionMethod(uint8_t byte)
{
    switch (static_cast<CompressionMethod>(byte))
    {
        case CompressionMethod::NONE:
        case CompressionMethod::LZ4:
        case CompressionMethod::ZSTD:
        case CompressionMethod::Multiple:
        case CompressionMethod::Delta:
        case CompressionMethod::T64:
        case CompressionMethod::DoubleDelta:
        case CompressionMethod::Gorilla:
        case CompressionMethod::FPC:
        case CompressionMethod::GCD:
        case CompressionMethod::AES_128_GCM_SIV:
        case CompressionMethod::AES_256_GCM_SIV:
        case CompressionMethod::DeflateQpl:
        case CompressionMethod::ZSTD_QPL:
            return true;
        default:
            return false;
    }
}

/// Return a human-readable name for a compression method byte.
inline const char * compressionMethodName(uint8_t byte)
{
    switch (static_cast<CompressionMethod>(byte))
    {
        case CompressionMethod::NONE: return "NONE";
        case CompressionMethod::LZ4: return "LZ4";
        case CompressionMethod::ZSTD: return "ZSTD";
        case CompressionMethod::Multiple: return "Multiple";
        case CompressionMethod::Delta: return "Delta";
        case CompressionMethod::T64: return "T64";
        case CompressionMethod::DoubleDelta: return "DoubleDelta";
        case CompressionMethod::Gorilla: return "Gorilla";
        case CompressionMethod::FPC: return "FPC";
        case CompressionMethod::GCD: return "GCD";
        case CompressionMethod::AES_128_GCM_SIV: return "AES_128_GCM_SIV";
        case CompressionMethod::AES_256_GCM_SIV: return "AES_256_GCM_SIV";
        case CompressionMethod::DeflateQpl: return "DeflateQpl";
        case CompressionMethod::ZSTD_QPL: return "ZSTD_QPL";
        default: return nullptr;
    }
}

} // namespace PartRepair
