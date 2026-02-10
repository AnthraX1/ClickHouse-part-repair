#pragma once

#include "BlockIterator.h"
#include "Utils.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

/// Reference: MarkInCompressedFile from src/Formats/MarkInCompressedFile.h
/// On-disk .mrk2 format: writeBinaryLittleEndian for each field
/// (src/Storages/MergeTree/MergeTreeDataPartWriterWide.cpp:385-398)
///
/// On-disk .mrk2 read: sizeof(MarkInCompressedFile) + readBinaryLittleEndian(granularity)
/// (src/Storages/MergeTree/MergeTreeMarksLoader.cpp:184-197)

namespace PartRepair
{

/// Represents one mark entry as stored on disk in .mrk2 / .cmrk2.
struct MarkEntry
{
    uint64_t offset_in_compressed_file = 0;   /// Byte offset to start of compressed block in .bin
    uint64_t offset_in_decompressed_block = 0; /// Byte offset within the decompressed block
    uint64_t rows_count = 0;                   /// Number of rows in this granule (adaptive only)
};

/// Reads and writes .mrk2 and .cmrk2 mark files.
class MarkFileHandler
{
public:
    explicit MarkFileHandler(Logger & logger);

    /// Read marks from a .mrk2 file (uncompressed, adaptive, wide).
    /// Each entry is: 2 * sizeof(uint64_t) for MarkInCompressedFile + sizeof(uint64_t) for rows_count.
    std::vector<MarkEntry> readMrk2(const std::string & path);

    /// Read marks from a .cmrk2 file (compressed, adaptive, wide).
    /// Same on-disk format as .mrk2 but wrapped in ClickHouse compressed blocks.
    std::vector<MarkEntry> readCmrk2(const std::string & path);

    /// Write marks to a .mrk2 file (uncompressed).
    void writeMrk2(const std::string & path, const std::vector<MarkEntry> & marks);

    /// Write marks to a .cmrk2 file (compressed).
    void writeCmrk2(const std::string & path, const std::vector<MarkEntry> & marks);

    /// Auto-detect format from file extension and read.
    std::vector<MarkEntry> readMarks(const std::string & path);

    /// Auto-detect format from file extension and write.
    void writeMarks(const std::string & path, const std::vector<MarkEntry> & marks);

    /// Build a mapping: compressed_block_file_offset -> list of marks that reference it.
    /// Useful for determining how many rows are in each compressed block.
    std::map<uint64_t, std::vector<const MarkEntry *>> buildBlockToMarksMap(
        const std::vector<MarkEntry> & marks) const;

    /// Sum rows from all marks pointing to a specific compressed block offset.
    size_t sumRowsForBlock(uint64_t block_offset, const std::vector<MarkEntry> & marks) const;

private:
    Logger & logger_;
};

} // namespace PartRepair
