#include "BlockScanner.h"
#include "PartialLZ4Decompress.h"
#include "compat/codec.h"
#include "compat/compression_defs.h"

#include <fstream>
#include <sstream>
#include <filesystem>
#include <cstring>
#include <iostream>

#include <city.h>

namespace fs = std::filesystem;

namespace PartRepair
{

namespace
{

/// Read the compressed region (header + payload) for a block from the on-disk
/// .bin file. Returns an empty vector on I/O failure.
std::vector<char> readCompressedBlockFromFile(
    const std::string & input_bin_path,
    const BlockInfo & block)
{
    std::vector<char> buffer;

    if (block.compressed_size == 0)
        return buffer;

    std::ifstream in(input_bin_path, std::ios::binary);
    if (!in.is_open())
        return {};

    const auto payload_offset =
        static_cast<std::streamoff>(block.file_offset + sizeof(Checksum));

    in.seekg(payload_offset, std::ios::beg);
    if (!in.good())
        return {};

    buffer.resize(block.compressed_size);
    in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    std::streamsize got = in.gcount();

    if (got != static_cast<std::streamsize>(buffer.size()))
    {
        if (got <= 0)
            return {};
        buffer.resize(static_cast<size_t>(got));
    }

    return buffer;
}

/// Maximum number of healthy blocks for which we keep full decompressed data
/// in memory for format detection. Keep this small to bound memory usage.
static constexpr size_t MAX_HEALTHY_DECOMPRESSED_SAMPLES = 10;

} // anonymous namespace

BlockScanner::BlockScanner(bool skip_checksum,
                           const std::string & output_dir,
                           Logger & logger,
                           const std::string & input_bin_path)
    : skip_checksum_(skip_checksum)
    , output_dir_(output_dir)
    , logger_(logger)
    , input_bin_path_(input_bin_path)
{
    ensureDirectory(output_dir_);
}

bool BlockScanner::validateChecksum(const BlockInfo & block)
{
    // Checksum covers the compressed data region (method byte through end of
    // payload), which we read on demand from the .bin file.
    auto compressed = readCompressedBlockFromFile(input_bin_path_, block);
    if (compressed.size() != block.compressed_size)
        return false;

    auto calculated = CityHash_v1_0_2::CityHash128(compressed.data(), block.compressed_size);
    return calculated.low64 == block.stored_checksum.low64
        && calculated.high64 == block.stored_checksum.high64;
}

void BlockScanner::attemptDecompression(const BlockInfo & block, ScanResult & result)
{
    auto compressed = readCompressedBlockFromFile(input_bin_path_, block);
    if (compressed.size() < BLOCK_HEADER_SIZE)
    {
        result.health = BlockHealthStatus::UNREADABLE;
        result.detail = "Failed to read compressed payload from file for decompression";
        return;
    }

    // First, try standard decompression via our codec wrapper
    try
    {
        result.decompressed_data.resize(block.decompressed_size);
        decompressBlock(
            block.method_byte,
            compressed.data(),
            static_cast<uint32_t>(compressed.size()),
            result.decompressed_data.data(),
            block.decompressed_size);
        result.health = BlockHealthStatus::HEALTHY;
        return;
    }
    catch (const std::exception &)
    {
        // Standard decompression failed. Fall through to partial decompression.
    }

    // Standard decompression failed — attempt partial decompression to salvage data.
    result.health = BlockHealthStatus::DECOMPRESSION_FAILED;

    // Allocate output buffer
    result.partial_data.resize(block.decompressed_size, 0);
    PartialDecompressResult partial_result;

    auto method = static_cast<CompressionMethod>(block.method_byte);
    const char * payload = compressed.data() + BLOCK_HEADER_SIZE;
    uint32_t payload_size = static_cast<uint32_t>(compressed.size()) - BLOCK_HEADER_SIZE;

    if (method == CompressionMethod::LZ4)
    {
        partial_result = partialLZ4Decompress(
            payload, payload_size,
            result.partial_data.data(), block.decompressed_size);
    }
    else if (method == CompressionMethod::ZSTD || method == CompressionMethod::ZSTD_QPL)
    {
        partial_result = partialZSTDDecompress(
            payload, payload_size,
            result.partial_data.data(), block.decompressed_size);
    }
    else
    {
        // For other codecs (Delta, Gorilla, etc.) we can't easily do partial decompression.
        // The data in compressed_data is still available in the raw block file.
        partial_result = {false, 0};
    }

    result.partial_bytes = partial_result.bytes_written;

    if (partial_result.success)
    {
        // Actually succeeded with partial decompressor — the standard codec may have been stricter.
        result.health = BlockHealthStatus::HEALTHY;
        result.decompressed_data = std::move(result.partial_data);
        result.partial_data.clear();
        result.partial_bytes = 0;
    }
    else
    {
        // Trim partial_data to actual written bytes
        result.partial_data.resize(partial_result.bytes_written);
    }
}

void BlockScanner::writeSalvageFiles(const BlockInfo & block, ScanResult & result)
{
    // Write raw compressed block (checksum + header + payload) for forensic analysis
    {
        std::string raw_path = output_dir_ + "/block_" + std::to_string(block.block_index) + "_raw.bin";
        std::ofstream raw_file(raw_path, std::ios::binary);
        if (raw_file.is_open())
        {
            // Write stored checksum first
            raw_file.write(reinterpret_cast<const char *>(&block.stored_checksum), sizeof(Checksum));
            // Then the compressed data (header + payload), read on demand from disk.
            auto compressed = readCompressedBlockFromFile(input_bin_path_, block);
            if (!compressed.empty())
            {
                raw_file.write(compressed.data(), static_cast<std::streamsize>(compressed.size()));
                result.raw_block_file = raw_path;
            }
        }
    }

    // Write partial decompressed data if any
    if (!result.partial_data.empty())
    {
        std::string partial_path = output_dir_ + "/block_" + std::to_string(block.block_index) + "_partial.bin";
        std::ofstream partial_file(partial_path, std::ios::binary);
        if (partial_file.is_open())
        {
            partial_file.write(result.partial_data.data(), result.partial_data.size());
            result.partial_data_file = partial_path;
        }
    }
}

std::vector<ScanResult> BlockScanner::scan(const std::vector<BlockInfo> & blocks)
{
    std::vector<ScanResult> results;
    results.reserve(blocks.size());

    size_t healthy_count = 0;
    size_t corrupt_count = 0;

    const size_t total_blocks = blocks.size();
    size_t processed = 0;
    size_t healthy_with_decompressed_data = 0;

    for (const auto & block : blocks)
    {
        ScanResult result;
        result.block_index = block.block_index;
        result.file_offset = block.file_offset;
        result.compressed_size = block.compressed_size;
        result.decompressed_size = block.decompressed_size;
        result.codec_name = block.codecName();

        // Handle blocks that couldn't be read at all
        if (block.status == BlockStatus::HEADER_READ_ERROR || block.status == BlockStatus::HEADER_CORRUPT)
        {
            result.health = BlockHealthStatus::HEADER_CORRUPT;
            result.detail = block.error_message;
            ++corrupt_count;

            logger_.error("Block " + std::to_string(block.block_index)
                + " @ offset " + std::to_string(block.file_offset)
                + ": HEADER CORRUPT — " + block.error_message);

            results.push_back(std::move(result));
            continue;
        }

        if (block.status == BlockStatus::PAYLOAD_READ_ERROR)
        {
            result.health = BlockHealthStatus::UNREADABLE;
            result.detail = block.error_message;
            ++corrupt_count;

            logger_.error("Block " + std::to_string(block.block_index)
                + " @ offset " + std::to_string(block.file_offset)
                + ": UNREADABLE — " + block.error_message);

            // Still write raw salvage
            writeSalvageFiles(block, result);
            results.push_back(std::move(result));
            continue;
        }

        // Print lightweight progress to stderr without logging OK blocks per-line.
        ++processed;
        std::cerr << "\rScanning block " << block.block_index << " / " << total_blocks << std::flush;

        // Block read OK — check checksum
        std::string checksum_status;
        if (!skip_checksum_)
        {
            if (!validateChecksum(block))
            {
                result.health = BlockHealthStatus::CHECKSUM_MISMATCH;
                result.detail = "Checksum mismatch";
                checksum_status = "MISMATCH";
                ++corrupt_count;

                logger_.warn("Block " + std::to_string(block.block_index)
                    + " @ offset " + std::to_string(block.file_offset)
                    + " [" + result.codec_name + "]"
                    + ": CHECKSUM MISMATCH"
                    + " (compressed=" + std::to_string(block.compressed_size)
                    + ", decompressed=" + std::to_string(block.decompressed_size) + ")");

                // Still attempt decompression to salvage data
                attemptDecompression(block, result);
                writeSalvageFiles(block, result);
                results.push_back(std::move(result));
                continue;
            }
            checksum_status = "OK";
        }
        else
        {
            checksum_status = "SKIPPED";
        }

        // Attempt decompression
        attemptDecompression(block, result);

        if (result.health == BlockHealthStatus::HEALTHY)
        {
            // Do not log OK blocks individually; just bump the counter.
            ++healthy_count;

            // We only need decompressed bytes for a small sample of healthy
            // blocks (for format detection heuristics). For all further
            // healthy blocks, immediately release the decompressed buffer to
            // keep memory usage bounded.
            if (healthy_with_decompressed_data < MAX_HEALTHY_DECOMPRESSED_SAMPLES)
            {
                ++healthy_with_decompressed_data;
            }
            else
            {
                std::vector<char>().swap(result.decompressed_data);
            }
        }
        else
        {
            ++corrupt_count;
            result.detail = "Decompression failed, salvaged " + std::to_string(result.partial_bytes)
                + " of " + std::to_string(block.decompressed_size) + " bytes";

            logger_.error("Block " + std::to_string(block.block_index)
                + " @ offset " + std::to_string(block.file_offset)
                + " [" + result.codec_name + "]"
                + ": DECOMPRESSION FAILED"
                + " (checksum=" + checksum_status
                + ", salvaged " + std::to_string(result.partial_bytes)
                + "/" + std::to_string(block.decompressed_size) + " bytes)");

            writeSalvageFiles(block, result);
        }

        results.push_back(std::move(result));
    }

    // Finish the progress line.
    if (total_blocks > 0)
        std::cerr << "\rScanning block " << (total_blocks - 1) << " / " << total_blocks << std::endl;

    logger_.info("Scan complete: " + std::to_string(blocks.size()) + " blocks total, "
        + std::to_string(healthy_count) + " healthy, "
        + std::to_string(corrupt_count) + " corrupted");

    return results;
}

} // namespace PartRepair
