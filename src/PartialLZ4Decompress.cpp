/// This file contains a modified copy of ClickHouse's LZ4 decompressImpl from
/// src/Compression/LZ4_decompress_faster.cpp, adapted to return the number of bytes
/// successfully written to the output buffer before any error occurs.
/// This enables partial data salvage from corrupted compressed blocks.
///
/// The ZSTD partial decompressor uses ZSTD_decompressStream() which naturally
/// reports output progress via ZSTD_outBuffer.pos.

#include "PartialLZ4Decompress.h"

#include <cstring>
#include <algorithm>
#include <zstd.h>

namespace PartRepair
{

namespace
{

/// Minimal reimplementation of helpers from ClickHouse's LZ4_decompress_faster.cpp.
/// We use copy_amount=8 only (the simplest and most portable variant).

inline uint16_t LZ4_readLE16(const void * mem_ptr)
{
    const uint8_t * p = reinterpret_cast<const uint8_t *>(mem_ptr);
    return static_cast<uint16_t>(p[0]) + (p[1] << 8);
}

/// Simple byte-by-byte copy that works correctly for overlapping regions.
inline void safeCopy(uint8_t * dest, const uint8_t * src, size_t count)
{
    for (size_t i = 0; i < count; ++i)
        dest[i] = src[i];
}

/// Partial LZ4 decompression with copy_amount=8.
/// Returns (success, bytes_written_to_output).
/// On failure, bytes_written gives the number of valid decompressed bytes.
PartialDecompressResult decompressImplPartial(
    const char * const source, char * const dest,
    size_t source_size, size_t dest_size)
{
    const uint8_t * ip = reinterpret_cast<const uint8_t *>(source);
    uint8_t * op = reinterpret_cast<uint8_t *>(dest);
    const uint8_t * const input_end = ip + source_size;
    uint8_t * const output_begin = op;
    uint8_t * const output_end = op + dest_size;

    /// Track last known-good output position. We update this after each successful
    /// token (literal copy + match copy) completes.
    uint8_t * last_good_op = op;

    while (true)
    {
        size_t length;

        auto continue_read_length = [&]() -> bool
        {
            unsigned s;
            do
            {
                if (ip >= input_end)
                    return false;
                s = *ip++;
                length += s;
            } while (s == 255 && ip < input_end);
            return true;
        };

        /// Get literal length.
        if (ip >= input_end)
            return {false, static_cast<size_t>(last_good_op - output_begin)};

        const unsigned token = *ip++;
        length = token >> 4;

        if (length == 0x0F)
        {
            if (ip >= input_end)
                return {false, static_cast<size_t>(last_good_op - output_begin)};
            if (!continue_read_length())
                return {false, static_cast<size_t>(last_good_op - output_begin)};
        }

        /// Copy literals.
        uint8_t * copy_end = op + length;

        if (copy_end > output_end)
            return {false, static_cast<size_t>(last_good_op - output_begin)};

        if (ip + length > input_end)
            return {false, static_cast<size_t>(last_good_op - output_begin)};

        std::memcpy(op, ip, length);

        if (copy_end == output_end)
            return {true, dest_size};

        ip += length;
        op = copy_end;

        /// Now handle match.

        if (ip + 2 > input_end)
            return {false, static_cast<size_t>(last_good_op - output_begin)};

        /// Get match offset.
        size_t offset = LZ4_readLE16(ip);
        ip += 2;
        uint8_t * match = op - offset;

        if (match < output_begin)
            return {false, static_cast<size_t>(last_good_op - output_begin)};

        /// Get match length.
        length = token & 0x0F;
        if (length == 0x0F)
        {
            if (ip >= input_end)
                return {false, static_cast<size_t>(last_good_op - output_begin)};
            if (!continue_read_length())
                return {false, static_cast<size_t>(last_good_op - output_begin)};
        }
        length += 4;

        /// Copy match within block. Match may overlap with output (replication).
        copy_end = op + length;

        if (copy_end > output_end)
            return {false, static_cast<size_t>(last_good_op - output_begin)};

        /// Use safe byte-by-byte copy to handle overlapping correctly.
        safeCopy(op, match, length);
        op = copy_end;

        /// Both literal and match completed successfully — update last_good_op.
        last_good_op = op;
    }
}

} // anonymous namespace


PartialDecompressResult partialLZ4Decompress(
    const char * source, size_t source_size,
    char * dest, size_t dest_size)
{
    if (source_size == 0 || dest_size == 0)
        return {true, 0};

    return decompressImplPartial(source, dest, source_size, dest_size);
}


PartialDecompressResult partialZSTDDecompress(
    const char * source, size_t source_size,
    char * dest, size_t dest_size)
{
    if (source_size == 0 || dest_size == 0)
        return {true, 0};

    ZSTD_DCtx * dctx = ZSTD_createDCtx();
    if (!dctx)
        return {false, 0};

    ZSTD_inBuffer in_buf{source, source_size, 0};
    ZSTD_outBuffer out_buf{dest, dest_size, 0};

    bool success = true;
    while (in_buf.pos < in_buf.size)
    {
        size_t ret = ZSTD_decompressStream(dctx, &out_buf, &in_buf);
        if (ZSTD_isError(ret))
        {
            success = false;
            break;
        }
        if (ret == 0)
            break; // Frame complete
    }

    size_t bytes_written = out_buf.pos;
    ZSTD_freeDCtx(dctx);

    return {success && (bytes_written == dest_size), bytes_written};
}

} // namespace PartRepair
