cmake_minimum_required(VERSION 3.16)
project(clickhouse-part-repair LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# --- System dependencies ---
find_package(PkgConfig REQUIRED)
pkg_check_modules(LZ4 REQUIRED IMPORTED_TARGET liblz4)
pkg_check_modules(ZSTD REQUIRED IMPORTED_TARGET libzstd)
find_package(Boost REQUIRED COMPONENTS program_options)

# --- Vendored CityHash 1.0.2 ---
add_subdirectory(vendor/cityhash102)

# --- Our tool ---
set(SOURCES
    src/main.cpp
    src/BlockIterator.cpp
    src/BlockScanner.cpp
    src/BlockRepairer.cpp
    src/MarkFileHandler.cpp
    src/FormatDetector.cpp
    src/PartialLZ4Decompress.cpp
    src/Utils.cpp
    src/compat/codec.cpp
    src/compat/default_serializer.cpp
)

add_executable(clickhouse-part-repair ${SOURCES})

target_include_directories(clickhouse-part-repair PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(clickhouse-part-repair PRIVATE
    cityhash102
    PkgConfig::LZ4
    PkgConfig::ZSTD
    Boost::program_options
)
