#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <filesystem>

namespace PartRepair
{

/// Simple logger that writes to both stderr and an optional log file.
class Logger
{
public:
    explicit Logger(const std::string & log_path = "");
    ~Logger();

    void info(const std::string & msg);
    void warn(const std::string & msg);
    void error(const std::string & msg);

    /// Write raw line (no prefix) to log only.
    void logOnly(const std::string & msg);

private:
    std::ofstream log_file_;
    bool has_file_ = false;
};

/// Format a byte count as human-readable string.
std::string humanReadableSize(size_t bytes);

/// Return hex dump of first N bytes.
std::string hexDump(const char * data, size_t size, size_t max_bytes = 64);

/// Ensure output directory exists, creating it if needed.
void ensureDirectory(const std::string & path);

} // namespace PartRepair
