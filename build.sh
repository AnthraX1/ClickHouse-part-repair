#!/bin/bash
# Build script for clickhouse-part-repair
#
# Usage: ./build.sh
#
# Prerequisites (Debian/Ubuntu):
#   sudo apt install cmake g++ liblz4-dev libzstd-dev libboost-program-options-dev

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Building clickhouse-part-repair..."
echo ""

mkdir -p "${SCRIPT_DIR}/build"
cd "${SCRIPT_DIR}/build"

cmake .. -DCMAKE_BUILD_TYPE=Release

cmake --build . --target clickhouse-part-repair -- -j"$(nproc)"

echo ""
echo "Build complete: ${SCRIPT_DIR}/build/clickhouse-part-repair"
