#include "BlockIterator.h"

#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdexcept>
#include <sstream>
#include <algorithm>
#include <iostream>

namespace PartRepair
{

std::string BlockInfo::codecName() const
{
    const char * name = compressionMethodName(method_byte);
    if (name)
        return name;
    std::ostringstream s;
    s << "UNKNOWN(0x" << std::hex << static_cast<int>(method_byte) << ")";
    return s.str();
}

bool BlockInfo::isKnownMethodByte(uint8_t byte)
{
    return isKnownCompressionMethod(byte);
}

BlockIterator::BlockIterator(const std::string & bin_file_path)
    : file_path_(bin_file_path)
{
    struct stat st;
    if (stat(file_path_.c_str(), &st) != 0)
        throw std::runtime_error("Cannot stat file: " + file_path_);
    file_size_ = static_cast<size_t>(st.st_size);
    if (file_size_ == 0)
        throw std::runtime_error("File is empty: " + file_path_);
}

BlockInfo BlockIterator::readOneBlock(const char * mapped_data, size_t offset, size_t block_index)
{
    BlockInfo info;
    info.block_index = block_index;
    info.file_offset = offset;

    constexpr size_t checksum_size = sizeof(Checksum);
    constexpr uint8_t header_size = BLOCK_HEADER_SIZE;
    constexpr size_t min_block_size = checksum_size + header_size;

    // Check if we can read checksum + header
    if (offset + min_block_size > file_size_)
    {
        info.status = BlockStatus::HEADER_READ_ERROR;
        info.error_message = "Not enough bytes for checksum + header (need "
            + std::to_string(min_block_size) + ", have " + std::to_string(file_size_ - offset) + ")";
        return info;
    }

    // Read checksum (two little-endian uint64)
    const char * block_start = mapped_data + offset;
    info.stored_checksum.low64 = loadLittleEndian<uint64_t>(block_start);
    info.stored_checksum.high64 = loadLittleEndian<uint64_t>(block_start + sizeof(uint64_t));

    // Read header fields
    const char * header_ptr = block_start + checksum_size;
    info.method_byte = static_cast<uint8_t>(header_ptr[0]);

    // Validate method byte
    if (!BlockInfo::isKnownMethodByte(info.method_byte))
    {
        info.status = BlockStatus::HEADER_CORRUPT;
        info.error_message = "Unknown compression method byte: 0x"
            + [&]{ std::ostringstream s; s << std::hex << static_cast<int>(info.method_byte); return s.str(); }();
        return info;
    }

    // Read sizes from header (compressed_size at offset 1, decompressed_size at offset 5)
    info.compressed_size = loadLittleEndian<uint32_t>(header_ptr + 1);
    info.decompressed_size = loadLittleEndian<uint32_t>(header_ptr + 5);

    // Validate sizes
    if (info.compressed_size < header_size)
    {
        info.status = BlockStatus::HEADER_CORRUPT;
        info.error_message = "compressed_size (" + std::to_string(info.compressed_size)
            + ") is less than header size (" + std::to_string(header_size) + ")";
        return info;
    }

    if (info.compressed_size > MAX_COMPRESSED_SIZE)
    {
        info.status = BlockStatus::HEADER_CORRUPT;
        info.error_message = "compressed_size (" + std::to_string(info.compressed_size)
            + ") exceeds MAX_COMPRESSED_SIZE (" + std::to_string(MAX_COMPRESSED_SIZE) + ")";
        return info;
    }

    if (info.decompressed_size == 0)
    {
        info.status = BlockStatus::HEADER_CORRUPT;
        info.error_message = "decompressed_size is 0";
        return info;
    }

    // Check if full compressed payload is available in file
    if (offset + checksum_size + info.compressed_size > file_size_)
    {
        info.status = BlockStatus::PAYLOAD_READ_ERROR;
        info.error_message = "File truncated: need " + std::to_string(checksum_size + info.compressed_size)
            + " bytes from offset " + std::to_string(offset) + ", but file is only " + std::to_string(file_size_) + " bytes";
        return info;
    }

    info.status = BlockStatus::OK;
    return info;
}

std::vector<BlockInfo> BlockIterator::readAllBlocks(
    const std::vector<uint64_t> * mark_offsets,
    bool enable_bruteforce)
{
    std::vector<BlockInfo> blocks;

    int fd = open(file_path_.c_str(), O_RDONLY);
    if (fd == -1)
        throw std::runtime_error("Cannot open file: " + file_path_);

    char * mapped = static_cast<char *>(mmap(nullptr, file_size_, PROT_READ, MAP_PRIVATE, fd, 0));
    if (mapped == MAP_FAILED)
    {
        close(fd);
        throw std::runtime_error("Cannot mmap file: " + file_path_);
    }

    size_t offset = 0;
    size_t block_index = 0;

    // Signature statistics from healthy blocks, used for bruteforce search.
    bool have_signature = false;
    uint8_t dominant_method = 0;
    uint32_t min_compressed_size = 0;
    uint32_t max_compressed_size = 0;
    uint32_t min_decompressed_size = 0;
    uint32_t max_decompressed_size = 0;

    // Aggregate bruteforce statistics (for debugging and user feedback).
    size_t bruteforce_calls = 0;
    size_t bruteforce_total_candidates = 0;        // header/size matches before checksum
    size_t bruteforce_total_checksum_checks = 0;   // checksum computations
    size_t bruteforce_total_bytes_scanned = 0;

    auto update_signature = [&](const BlockInfo & block)
    {
        if (!have_signature)
        {
            have_signature = true;
            dominant_method = block.method_byte;
            min_compressed_size = max_compressed_size = block.compressed_size;
            min_decompressed_size = max_decompressed_size = block.decompressed_size;
            return;
        }

        // Only track stats for the dominant method byte.
        if (block.method_byte != dominant_method)
            return;

        min_compressed_size = std::min(min_compressed_size, block.compressed_size);
        max_compressed_size = std::max(max_compressed_size, block.compressed_size);
        min_decompressed_size = std::min(min_decompressed_size, block.decompressed_size);
        max_decompressed_size = std::max(max_decompressed_size, block.decompressed_size);
    };

    auto find_next_mark_offset = [&](size_t current_offset) -> size_t
    {
        if (!mark_offsets || mark_offsets->empty())
            return file_size_;

        auto it = std::upper_bound(mark_offsets->begin(), mark_offsets->end(), current_offset);
        if (it == mark_offsets->end())
            return file_size_;
        return *it;
    };

    auto bruteforce_find_next_block = [&](size_t start_offset, BlockInfo & out_block) -> bool
    {
        if (!have_signature || !enable_bruteforce)
            return false;

        ++bruteforce_calls;

        constexpr size_t checksum_size = sizeof(Checksum);
        constexpr size_t header_size = BLOCK_HEADER_SIZE;
        const size_t min_block_size = checksum_size + header_size;

        // Expand observed ranges slightly to tolerate variation.
        auto expand_min = [](uint32_t v) { return v / 2; };
        auto expand_max = [](uint32_t v) { return v + v / 2; }; // * 1.5

        uint32_t min_comp = expand_min(min_compressed_size);
        uint32_t max_comp = expand_max(max_compressed_size);
        uint32_t min_decomp = expand_min(min_decompressed_size);
        uint32_t max_decomp = expand_max(max_decompressed_size);

        size_t base_offset = start_offset;
        size_t search_offset = start_offset;

        // Print an initial message about bruteforce search.
        std::cerr << "[bruteforce] Starting search after corrupted header at offset "
                  << base_offset << " (dominant_method=0x"
                  << std::hex << static_cast<int>(dominant_method) << std::dec << ")\n";

        // Report progress roughly every 16 KiB of scanned data to avoid being too noisy
        constexpr size_t REPORT_INTERVAL_BYTES = 16 * 1024; // 16 KiB
        size_t next_report_at = REPORT_INTERVAL_BYTES;
        size_t local_candidates = 0;
        size_t local_checksum_checks = 0;

        while (search_offset + min_block_size <= file_size_)
        {
            const char * block_start = mapped + search_offset;
            const char * header_ptr = block_start + checksum_size;

            size_t scanned_bytes = search_offset - base_offset;
            if (scanned_bytes >= next_report_at)
            {
                std::cerr << "[bruteforce] Scanned "
                          << (scanned_bytes / 1024) << " KiB since offset "
                          << base_offset
                          << " (candidates=" << local_candidates
                          << ", checksum_checks=" << local_checksum_checks << ")\n";
                next_report_at += REPORT_INTERVAL_BYTES;
            }

            uint8_t method = static_cast<uint8_t>(header_ptr[0]);
            uint32_t compressed_size = loadLittleEndian<uint32_t>(header_ptr + 1);
            uint32_t decompressed_size = loadLittleEndian<uint32_t>(header_ptr + 5);

            // Quick filters on method and sizes
            if (!BlockInfo::isKnownMethodByte(method) || method != dominant_method)
            {
                ++search_offset;
                continue;
            }

            if (compressed_size < header_size || compressed_size > MAX_COMPRESSED_SIZE)
            {
                ++search_offset;
                continue;
            }

            if (decompressed_size == 0)
            {
                ++search_offset;
                continue;
            }

            if (compressed_size < min_comp || compressed_size > max_comp
                || decompressed_size < min_decomp || decompressed_size > max_decomp)
            {
                ++search_offset;
                continue;
            }

            // At this point, header and size checks passed: this is a candidate
            // block before checksum verification.
            ++local_candidates;
            ++bruteforce_total_candidates;

            if (search_offset + checksum_size + compressed_size > file_size_)
            {
                // Not enough bytes for full block, stop search.
                bruteforce_total_bytes_scanned += search_offset - base_offset;
                std::cerr << "[bruteforce] Stopped search at offset " << search_offset
                          << " (ran out of file, scanned "
                          << ((search_offset - base_offset) / 1024) << " KiB, "
                          << "candidates=" << local_candidates
                          << ", checksum_checks=" << local_checksum_checks << ")\n";
                return false;
            }

            // Verify checksum over [header + payload].
            Checksum stored;
            stored.low64 = loadLittleEndian<uint64_t>(block_start);
            stored.high64 = loadLittleEndian<uint64_t>(block_start + sizeof(uint64_t));

            ++local_checksum_checks;
            ++bruteforce_total_checksum_checks;

            auto computed = CityHash_v1_0_2::CityHash128(
                reinterpret_cast<const char *>(header_ptr),
                compressed_size);

            if (stored.low64 != computed.low64 || stored.high64 != computed.high64)
            {
                ++search_offset;
                continue;
            }

            // Build a proper BlockInfo using normal parsing.
            BlockInfo candidate = readOneBlock(mapped, search_offset, block_index + 1);
            if (candidate.status != BlockStatus::OK)
            {
                ++search_offset;
                continue;
            }

            bruteforce_total_bytes_scanned += search_offset - base_offset;

            std::cerr << "[bruteforce] Found candidate block at offset " << search_offset
                      << " after scanning "
                      << ((search_offset - base_offset) / 1024) << " KiB "
                      << "(candidates=" << local_candidates
                      << ", checksum_checks=" << local_checksum_checks << ")\n";

            out_block = std::move(candidate);
            return true;
        }

        bruteforce_total_bytes_scanned += search_offset - base_offset;

        std::cerr << "[bruteforce] No valid block found after scanning "
                  << ((search_offset - base_offset) / 1024) << " KiB "
                  << "from offset " << base_offset
                  << " (candidates=" << local_candidates
                  << ", checksum_checks=" << local_checksum_checks << ")\n";

        return false;
    };

    while (offset < file_size_)
    {
        BlockInfo block = readOneBlock(mapped, offset, block_index);

        if (block.status == BlockStatus::OK)
        {
            update_signature(block);
            blocks.push_back(std::move(block));
            offset += blocks.back().totalOnDiskSize();
            ++block_index;

            // Phase 1 progress: print every 1000 parsed blocks (including errors).
            if (block_index % 1000 == 0)
                std::cerr << "\r[Phase 1] Parsed " << block_index << " blocks" << std::flush;

            continue;
        }

        if (block.status == BlockStatus::PAYLOAD_READ_ERROR)
        {
            // Payload truncated — record the block and stop. We can't safely continue.
            blocks.push_back(std::move(block));
            offset = file_size_;
            ++block_index;
            break;
        }

        // HEADER_READ_ERROR or HEADER_CORRUPT.
        blocks.push_back(std::move(block));
        ++block_index;

        if (block_index % 1000 == 0)
            std::cerr << "\r[Phase 1] Parsed " << block_index << " blocks" << std::flush;

        // First, if we have mark offsets, try jumping to the next known block start.
        size_t next_mark_offset = find_next_mark_offset(offset);
        if (next_mark_offset < file_size_)
        {
            offset = next_mark_offset;
            continue;
        }

        // No marks (or no later mark). Optionally try bruteforce if enabled.
        BlockInfo recovered;
        if (enable_bruteforce && bruteforce_find_next_block(offset + 1, recovered))
        {
            // recovered.block_index was set using block_index+1 when created; keep it.
            blocks.push_back(std::move(recovered));
            offset = blocks.back().file_offset + blocks.back().totalOnDiskSize();
            ++block_index;
            continue;
        }

        // Nothing more we can do — stop iteration.
        offset = file_size_;
        break;
    }

    if (block_index > 0)
        std::cerr << "\r[Phase 1] Parsed " << block_index << " blocks total" << std::endl;

    munmap(mapped, file_size_);
    close(fd);

    return blocks;
}

} // namespace PartRepair
