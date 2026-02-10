#include "Utils.h"
#include <iomanip>
#include <sstream>
#include <filesystem>

namespace fs = std::filesystem;

namespace PartRepair
{

Logger::Logger(const std::string & log_path)
{
    if (!log_path.empty())
    {
        ensureDirectory(fs::path(log_path).parent_path().string());
        log_file_.open(log_path, std::ios::out | std::ios::trunc);
        if (log_file_.is_open())
            has_file_ = true;
        else
            std::cerr << "[WARN] Could not open log file: " << log_path << std::endl;
    }
}

Logger::~Logger()
{
    if (has_file_)
        log_file_.close();
}

void Logger::info(const std::string & msg)
{
    std::cerr << "[INFO] " << msg << std::endl;
    if (has_file_)
        log_file_ << "[INFO] " << msg << std::endl;
}

void Logger::warn(const std::string & msg)
{
    std::cerr << "[WARN] " << msg << std::endl;
    if (has_file_)
        log_file_ << "[WARN] " << msg << std::endl;
}

void Logger::error(const std::string & msg)
{
    std::cerr << "[ERROR] " << msg << std::endl;
    if (has_file_)
        log_file_ << "[ERROR] " << msg << std::endl;
}

void Logger::logOnly(const std::string & msg)
{
    if (has_file_)
        log_file_ << msg << std::endl;
}

std::string humanReadableSize(size_t bytes)
{
    std::ostringstream oss;
    if (bytes < 1024)
        oss << bytes << " B";
    else if (bytes < 1024 * 1024)
        oss << std::fixed << std::setprecision(1) << (static_cast<double>(bytes) / 1024.0) << " KiB";
    else if (bytes < 1024ULL * 1024 * 1024)
        oss << std::fixed << std::setprecision(1) << (static_cast<double>(bytes) / (1024.0 * 1024.0)) << " MiB";
    else
        oss << std::fixed << std::setprecision(2) << (static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0)) << " GiB";
    return oss.str();
}

std::string hexDump(const char * data, size_t size, size_t max_bytes)
{
    std::ostringstream oss;
    size_t limit = std::min(size, max_bytes);
    for (size_t i = 0; i < limit; ++i)
    {
        oss << std::hex << std::setw(2) << std::setfill('0')
            << (static_cast<unsigned>(data[i]) & 0xFF);
        if (i + 1 < limit)
            oss << ' ';
    }
    if (limit < size)
        oss << " ... (" << (size - limit) << " more bytes)";
    return oss.str();
}

void ensureDirectory(const std::string & path)
{
    if (!path.empty() && !fs::exists(path))
        fs::create_directories(path);
}

} // namespace PartRepair
