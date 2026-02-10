/// Implementation of default_serializer.h

#include "default_serializer.h"

#include <cstring>
#include <stdexcept>
#include <map>

namespace PartRepair
{

size_t getTypeWidth(const std::string & type_name)
{
    /// Map common fixed-size types to their byte widths.
    /// Variable-size types (String, etc.) return 0.
    static const std::map<std::string, size_t> widths = {
        {"UInt8", 1}, {"Int8", 1}, {"Bool", 1},
        {"UInt16", 2}, {"Int16", 2}, {"Date", 2},
        {"UInt32", 4}, {"Int32", 4}, {"Float32", 4}, {"DateTime", 4}, {"IPv4", 4},
        {"UInt64", 8}, {"Int64", 8}, {"Float64", 8}, {"DateTime64", 8},
        {"UInt128", 16}, {"Int128", 16}, {"UUID", 16}, {"IPv6", 16},
        {"UInt256", 32}, {"Int256", 32},
    };

    auto it = widths.find(type_name);
    if (it != widths.end())
        return it->second;
    return 0;
}

std::vector<char> serializeDefaults(const std::string & type_name, size_t row_count)
{
    if (row_count == 0)
        return {};

    size_t width = getTypeWidth(type_name);
    if (width > 0)
    {
        /// Fixed-width type: default value is all zeros.
        /// This matches ClickHouse's serializeBinaryBulk() for default-constructed columns.
        return std::vector<char>(row_count * width, 0);
    }

    if (type_name == "String" || type_name == "FixedString")
    {
        /// String default = "" = VarUInt(0) per row = one 0x00 byte each.
        /// FixedString(N) default would need N, but we treat it as String for simplicity.
        return std::vector<char>(row_count, 0);
    }

    throw std::runtime_error(
        "Unsupported type for default serialization: " + type_name + ". "
        "Only simple fixed-width types and String are supported. "
        "For complex types (Nullable, Array, Tuple, Map), consider using a simpler --format.");
}

} // namespace PartRepair
