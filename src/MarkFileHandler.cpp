#include "MarkFileHandler.h"

#include <cstring>
#include <fstream>
#include <stdexcept>
#include <filesystem>

#include <city.h>

#include "compat/codec.h"
#include "compat/compression_defs.h"

namespace fs = std::filesystem;

namespace PartRepair
{

MarkFileHandler::MarkFileHandler(Logger & logger) : logger_(logger) {}

std::vector<MarkEntry> MarkFileHandler::readMrk2(const std::string & path)
{
    /// .mrk2 on-disk format (adaptive, wide): for each mark:
    ///   writeBinaryLittleEndian(offset_in_compressed_file)   — 8 bytes
    ///   writeBinaryLittleEndian(offset_in_decompressed_block) — 8 bytes
    ///   writeBinaryLittleEndian(rows_count)                   — 8 bytes
    /// Total: 24 bytes per mark

    std::vector<MarkEntry> marks;

    std::ifstream file(path, std::ios::binary);
    if (!file.is_open())
        throw std::runtime_error("Cannot open mark file: " + path);

    size_t file_size = fs::file_size(path);
    constexpr size_t mark_size = sizeof(uint64_t) * 3; // 24 bytes

    if (file_size % mark_size != 0)
    {
        logger_.warn("Mark file size (" + std::to_string(file_size) + ") is not a multiple of mark entry size ("
            + std::to_string(mark_size) + "). File may be corrupted.");
    }

    size_t num_marks = file_size / mark_size;
    marks.reserve(num_marks);

    for (size_t i = 0; i < num_marks; ++i)
    {
        MarkEntry entry;
        file.read(reinterpret_cast<char *>(&entry.offset_in_compressed_file), sizeof(uint64_t));
        file.read(reinterpret_cast<char *>(&entry.offset_in_decompressed_block), sizeof(uint64_t));
        file.read(reinterpret_cast<char *>(&entry.rows_count), sizeof(uint64_t));

        if (!file.good())
        {
            logger_.warn("Mark file read error at mark " + std::to_string(i));
            break;
        }

        marks.push_back(entry);
    }

    logger_.info("Read " + std::to_string(marks.size()) + " marks from " + path);
    return marks;
}

std::vector<MarkEntry> MarkFileHandler::readCmrk2(const std::string & path)
{
    /// .cmrk2: same data as .mrk2 but wrapped in ClickHouse compressed block format.
    /// We read the compressed blocks ourselves and decompress, then parse the mark entries
    /// from the decompressed data.

    std::vector<MarkEntry> marks;

    try
    {
        // Read the entire file
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file.is_open())
            throw std::runtime_error("Cannot open compressed mark file: " + path);

        size_t file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        std::vector<char> file_data(file_size);
        file.read(file_data.data(), file_size);
        file.close();

        // Parse and decompress all compressed blocks, concatenating decompressed data
        std::vector<char> decompressed_all;
        size_t offset = 0;

        while (offset < file_size)
        {
            constexpr size_t checksum_size = sizeof(Checksum);
            constexpr size_t min_block = checksum_size + BLOCK_HEADER_SIZE;

            if (offset + min_block > file_size)
                break;

            const char * block_start = file_data.data() + offset;
            const char * header_ptr = block_start + checksum_size;

            uint32_t compressed_size = loadLittleEndian<uint32_t>(header_ptr + 1);
            uint32_t decompressed_size = loadLittleEndian<uint32_t>(header_ptr + 5);
            uint8_t method_byte = static_cast<uint8_t>(header_ptr[0]);

            if (compressed_size < BLOCK_HEADER_SIZE || offset + checksum_size + compressed_size > file_size)
                break;

            size_t prev_size = decompressed_all.size();
            decompressed_all.resize(prev_size + decompressed_size);

            decompressBlock(method_byte, header_ptr, compressed_size,
                            decompressed_all.data() + prev_size, decompressed_size);

            offset += checksum_size + compressed_size;
        }

        // Parse mark entries from decompressed data
        constexpr size_t mark_size = sizeof(uint64_t) * 3;
        size_t num_marks = decompressed_all.size() / mark_size;

        for (size_t i = 0; i < num_marks; ++i)
        {
            MarkEntry entry;
            const char * ptr = decompressed_all.data() + i * mark_size;
            entry.offset_in_compressed_file = loadLittleEndian<uint64_t>(ptr);
            entry.offset_in_decompressed_block = loadLittleEndian<uint64_t>(ptr + sizeof(uint64_t));
            entry.rows_count = loadLittleEndian<uint64_t>(ptr + 2 * sizeof(uint64_t));
            marks.push_back(entry);
        }
    }
    catch (const std::exception & e)
    {
        logger_.warn("Error reading compressed mark file: " + std::string(e.what()));
    }

    logger_.info("Read " + std::to_string(marks.size()) + " marks from " + path);
    return marks;
}

void MarkFileHandler::writeMrk2(const std::string & path, const std::vector<MarkEntry> & marks)
{
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    if (!file.is_open())
        throw std::runtime_error("Cannot create mark file: " + path);

    for (const auto & mark : marks)
    {
        file.write(reinterpret_cast<const char *>(&mark.offset_in_compressed_file), sizeof(uint64_t));
        file.write(reinterpret_cast<const char *>(&mark.offset_in_decompressed_block), sizeof(uint64_t));
        file.write(reinterpret_cast<const char *>(&mark.rows_count), sizeof(uint64_t));
    }

    logger_.info("Wrote " + std::to_string(marks.size()) + " marks to " + path);
}

void MarkFileHandler::writeCmrk2(const std::string & path, const std::vector<MarkEntry> & marks)
{
    /// Serialize all mark entries into a flat buffer, then compress as a single
    /// ClickHouse compressed block and write to file.

    try
    {
        // Serialize marks to buffer
        constexpr size_t mark_size = sizeof(uint64_t) * 3;
        std::vector<char> data(marks.size() * mark_size);

        for (size_t i = 0; i < marks.size(); ++i)
        {
            char * ptr = data.data() + i * mark_size;
            std::memcpy(ptr, &marks[i].offset_in_compressed_file, sizeof(uint64_t));
            std::memcpy(ptr + sizeof(uint64_t), &marks[i].offset_in_decompressed_block, sizeof(uint64_t));
            std::memcpy(ptr + 2 * sizeof(uint64_t), &marks[i].rows_count, sizeof(uint64_t));
        }

        // Compress as LZ4 block (default codec)
        auto compressed = compressBlock(
            static_cast<uint8_t>(CompressionMethod::LZ4),
            data.data(),
            static_cast<uint32_t>(data.size()));

        // Compute checksum
        auto checksum = CityHash_v1_0_2::CityHash128(compressed.data(), compressed.size());

        // Write to file: [checksum][compressed block]
        std::ofstream file(path, std::ios::binary | std::ios::trunc);
        if (!file.is_open())
            throw std::runtime_error("Cannot create compressed mark file: " + path);

        file.write(reinterpret_cast<const char *>(&checksum.low64), sizeof(uint64_t));
        file.write(reinterpret_cast<const char *>(&checksum.high64), sizeof(uint64_t));
        file.write(compressed.data(), compressed.size());
        file.close();
    }
    catch (const std::exception & e)
    {
        throw std::runtime_error("Error writing compressed mark file: " + std::string(e.what()));
    }

    logger_.info("Wrote " + std::to_string(marks.size()) + " marks to " + path);
}

std::vector<MarkEntry> MarkFileHandler::readMarks(const std::string & path)
{
    std::string ext = fs::path(path).extension().string();
    if (ext == ".cmrk2")
        return readCmrk2(path);
    else if (ext == ".mrk2")
        return readMrk2(path);
    else
    {
        logger_.warn("Unknown mark file extension: " + ext + ", trying as .mrk2");
        return readMrk2(path);
    }
}

void MarkFileHandler::writeMarks(const std::string & path, const std::vector<MarkEntry> & marks)
{
    std::string ext = fs::path(path).extension().string();
    if (ext == ".cmrk2")
        writeCmrk2(path, marks);
    else if (ext == ".mrk2")
        writeMrk2(path, marks);
    else
    {
        logger_.warn("Unknown mark file extension: " + ext + ", writing as .mrk2");
        writeMrk2(path, marks);
    }
}

std::map<uint64_t, std::vector<const MarkEntry *>> MarkFileHandler::buildBlockToMarksMap(
    const std::vector<MarkEntry> & marks) const
{
    std::map<uint64_t, std::vector<const MarkEntry *>> result;
    for (const auto & mark : marks)
        result[mark.offset_in_compressed_file].push_back(&mark);
    return result;
}

size_t MarkFileHandler::sumRowsForBlock(uint64_t block_offset, const std::vector<MarkEntry> & marks) const
{
    size_t total = 0;
    for (const auto & mark : marks)
    {
        if (mark.offset_in_compressed_file == block_offset)
            total += mark.rows_count;
    }
    return total;
}

} // namespace PartRepair
