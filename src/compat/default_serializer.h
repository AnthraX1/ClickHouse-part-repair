/// Generates serialized default column values without depending on ClickHouse's
/// DataType/Column/Serialization framework.
///
/// For all common fixed-width types, the default value is 0 — so serialization
/// is just N * type_width bytes of zeros.
/// For String, each default is "" serialized as VarUInt(0) = one 0x00 byte.

#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace PartRepair
{

/// Get the byte width of a ClickHouse column type, or 0 for variable-width types.
size_t getTypeWidth(const std::string & type_name);

/// Serialize `row_count` default values for the given column type.
/// @param type_name   ClickHouse type name (e.g. "UInt64", "String", "DateTime").
/// @param row_count   Number of default rows to generate.
/// @returns The serialized binary data matching ClickHouse's native binary format.
/// @throws std::runtime_error if the type is not supported.
std::vector<char> serializeDefaults(const std::string & type_name, size_t row_count);

} // namespace PartRepair
