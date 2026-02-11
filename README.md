# clickhouse-part-repair

> ⚠️ **Disclaimer**  
> This tool directly manipulates low-level MergeTree part files and is intended as a last-resort recovery utility. It can **permanently discard bytes**, reconstruct rows using heuristic defaults, and regenerate mark files in ways that may not exactly match the original layout. Always work on copies: **back up the entire part folder before running the tool**, and keep those backups even after attaching the repaired part to a table. If a repair attempt makes things worse or reveals unexpected data shifts, you will only be able to recover by restoring from these original corrupted files.  
>  
> This software is provided **"as is"**, without warranty of any kind, express or implied. The author and contributors are **not liable for any data loss, downtime, or other damages** arising from the use or misuse of this tool. Use it at your own risk.

A tool to scan, diagnose, and repair corrupted ClickHouse MergeTree `.bin` column files.

## Features

- **Block-level scanning**: Iterates over compressed blocks, validates checksums, attempts decompression
- **Partial data salvage**: Extracts successfully decompressed bytes from corrupted LZ4/ZSTD blocks
- **Format auto-detection**: Heuristically detects column type from healthy blocks and mark files
- **Bruteforce header recovery**: When no mark file is available, scans for plausible block headers using codec stats and checksums
- **Repair mode**: Replaces corrupted blocks with default-value blocks, regenerates mark files

## Prerequisites

- CMake 3.16+
- C++20 compatible compiler (GCC 10+, Clang 12+)
- System libraries:
  - `liblz4-dev`
  - `libzstd-dev`
  - `libboost-program-options-dev`

On Debian/Ubuntu:

```bash
sudo apt install cmake g++ liblz4-dev libzstd-dev libboost-program-options-dev
```

On Fedora/RHEL:

```bash
sudo dnf install cmake gcc-c++ lz4-devel libzstd-devel boost-devel
```

No ClickHouse source tree is needed. CityHash 1.0.2 (used for checksum computation) is vendored in the repository.

## Build

```bash
./build.sh

# Or manual cmake:
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --target clickhouse-part-repair -- -j$(nproc)
```

## Usage

### Important

- **Specifying the mark file is highly recommended and often necessary.** If you have a working mark file (`.mrk2` or `.cmrk2`), you must use it. The mark file provides row counts and block boundaries; without it, the tool cannot reliably infer row counts for variable-width types (e.g. `String`) and may produce wrong or empty replacement blocks, leading to further data corruption or loss. Use `--bruteforce` without a mark file only when the mark file is missing or unusable.
- **Work on a detached part or with the database stopped only.** Do not run the tool on a part that is currently attached to a table or while ClickHouse is using the part directory.
- **Do not set `--output-dir` to a path inside the part folder.** If the output directory (e.g. `repair_output`) is created inside the part directory, ClickHouse will treat it as an invalid file when attaching the part and attachment will fail. Use a directory outside the part folder, then copy the repaired files into the part folder.
- After copying repaired files into the part, **set ownership and permissions** on the copied files to match the rest of the part (typically the `clickhouse` user and the same permissions as other files in that part).

### Finding the corrupted column and preparing a copy

When ClickHouse reports a corrupted column file, the error message and server logs usually include the full path to the `.bin` file (and sometimes the part name), for example:

- `Cannot read compressed block (corrupted data) from file /var/lib/clickhouse/data/db/table/partition/202401_1_1_0/column.bin`

Once you have identified the exact part and column file:

1. **Detach the part or stop the server first.** Use `ALTER TABLE ... DETACH PART '...'` or stop `clickhouse-server` so nothing is reading or writing the part directory.
2. **Make a byte-for-byte copy of the suspected files** into a safe working directory using `dd` (or an equivalent tool). Because `conv=sync,noerror` pads incomplete blocks to the block size, the copy can be larger than the original; get the exact original size first and truncate the copy to match:

   ```bash
   # Get the exact original file size (in bytes)
   ORIG_SIZE=$(stat -c %s /var/lib/clickhouse/data/db/table/partition/part/column.bin)

   # Copy the corrupted column out of the part directory (pad on read errors; output may be larger)
   dd if=/var/lib/clickhouse/data/db/table/partition/part/column.bin of=new_dir/column.bin bs=4M conv=sync,noerror

   # Drop any trailing NUL padding so the copy matches the original size
   truncate -s "$ORIG_SIZE" new_dir/column.bin
   ```

   Copy the corresponding mark file(s) (e.g. `column.mrk2` or `column.cmrk2`) into `new_dir` as well.

3. **Run `clickhouse-part-repair` only on the copied files.** Do not point the tool directly at the live part directory; always work against the copied `.bin` (and `.mrk2` / `.cmrk2`), then copy the repaired output back into the detached part when you are satisfied with the result.

### Scan and diagnose

```bash
# Basic scan (validates checksums + decompression, using mark file):
./clickhouse-part-repair column.bin column.mrk2

# Basic scan without a mark file (best-effort bruteforce header recovery, slower):
./clickhouse-part-repair --bruteforce column.bin

# Scan without checksum validation (for files with checksum issues, using mark file):
./clickhouse-part-repair --no-checksum column.bin column.mrk2

# Scan without checksum validation and without a mark file:
./clickhouse-part-repair --no-checksum --bruteforce column.bin

# Scan with custom output directory:
./clickhouse-part-repair --output-dir /tmp/repair column.bin column.mrk2
```

At least one of a **mark file** (`column.mrk2` / `column.cmrk2`) or `--bruteforce` **must** be provided.
Without either, the tool will stop with an error once it encounters a corrupted header.

### Repair

```bash
# Auto-detect column type and repair:
./clickhouse-part-repair --repair column.bin column.mrk2

# Specify column type explicitly:
./clickhouse-part-repair --repair --format UInt64 column.bin column.mrk2

# Repair with all options:
./clickhouse-part-repair \
  --repair \
  --no-checksum \
  --format "String" \
  --output-dir ./repaired \
  column.bin column.cmrk2

# Repair using a custom default value (overrides type default):
./clickhouse-part-repair \
  --repair \
  --format UInt64 \
  --default-value 42 \
  column.bin column.mrk2

# Repair using NULL as the default (Nullable column only):
./clickhouse-part-repair \
  --repair \
  --format "Nullable(UInt64)" \
  --default-null \
  column.bin column.mrk2

# Repair without a mark file (last resort: requires an explicit total block count):
#   1. Run this tool on a healthy part with the same structure.
#   2. Read the "Found N blocks" line in the log.
#   3. Use that N as --total-blocks here.
./clickhouse-part-repair \
  --repair \
  --bruteforce \
  --total-blocks N \
  --format UInt64 \
  column.bin
```

### Sort key (ORDER BY) columns

MergeTree expects that for sort key columns every row within a block is sorted in ascending order, and the **last row of each block** is less than or equal to the **first row of the next block**. When a block is corrupted, the repaired block may violate these invariants and cause errors such as:

`Code: 49. DB::Exception: Sort order of blocks violated for column number 0, left: UInt64_..., right: UInt64_... (LOGICAL_ERROR)`

during merge or mutate. When repairing a column that is part of the table's `ORDER BY` (sort/primary key), use **`--primary-key`** so that repaired blocks preserve sort order.

**What `--primary-key` does:**

1. **Inter-block boundary checks.** The tool stores the first and last value of each healthy block. For each corrupted block it verifies that the last salvaged value lies between the previous block's last value and the next block's first value. If it does not, it backs off to an earlier salvaged row (or the previous block's last value) and uses that as the fill default.

2. **Intra-block sort validation.** When a block has an invalid checksum but decompresses successfully, the decompressed data may still be corrupted. The tool scans rows within the block to find the longest sorted prefix. If the sort order is violated at some row, only the rows before the violation are kept; the rest are filled with the last sorted value (or the previous block's last value if that is out of range). The violation details are printed, for example:

   ```
   [primary-key] Block 47968: sort order violated at row 3089/8192, left: 7380660709789378393, right: 4274438819; fill_value=7380660709789378393
   [primary-key] Block 47968: partial_rows=3089 fill_rows=5103 [prev_last=7380628022145972683, next_first=7380886550571433517]
   ```

3. **Boundary propagation.** After a block is repaired, the tool updates the tracked boundaries so that the next block sees the effective last value of the repaired block, not stale data from before the repair.

**`--format` is required** when using `--primary-key`. Nullable types cannot be used with `--primary-key` because ClickHouse does not allow Nullable columns in ORDER BY.

```bash
# Primary-key repair (requires --format):
./clickhouse-part-repair --repair --format UInt64 --primary-key column.bin column.mrk2
```

> **Warning: merging after sort key repair.**
> After a sort key column is repaired, the corrupted rows are replaced with fill values (typically the last known sorted value or the previous block's boundary value). These fill values may duplicate keys that exist in other parts of the table. When ClickHouse runs background merges, rows sharing the same sort key across parts are merged together. For `ReplacingMergeTree`, `CollapsingMergeTree`, and similar engines this means the repaired rows may **replace or collapse real rows from other parts**.
>
> Before attaching the repaired part, query the table to back up rows whose keys overlap with the fill range:
>
> ```sql
> -- Identify the fill value from the repair log, then back up affected rows:
> SELECT * FROM my_table WHERE sort_key_column = <fill_value> INTO OUTFILE 'backup.tsv';
> ```
>
> Keep this backup so you can restore the original rows if the merge discards them.

### Supported types for repair

Fixed-width: `UInt8`, `Int8`, `Bool`, `UInt16`, `Int16`, `Date`, `UInt32`, `Int32`, `Float32`, `DateTime`, `IPv4`, `UInt64`, `Int64`, `Float64`, `DateTime64`, `UInt128`, `Int128`, `UUID`, `IPv6`, `UInt256`, `Int256`

Variable-width: `String`

Nullable variants such as `Nullable(UInt64)` and `Nullable(String)` are also supported; you can combine them
with `--default-null` or `--default-value` to control how repaired rows are filled.

### Output

The tool produces:

- `repair.log` — Detailed per-block scan results (path configurable via `--log`, default `<output-dir>/repair.log`)
- `block_N_raw.bin` — Raw compressed data of corrupted block N (for forensic analysis)
- `block_N_partial.bin` — Partially decompressed data salvaged from corrupted block N
- `column.bin.repaired` — Repaired `.bin` file (repair mode only)
- `column.mrk2.repaired` — Regenerated mark file (repair mode only)

### After repair

```bash
# Replace the original files (use an output-dir outside the part folder):
cp ./repair_output/column.bin.repaired /path/to/part/column.bin
cp ./repair_output/column.mrk2.repaired /path/to/part/column.mrk2

# Set ownership and permissions to match the part (e.g. clickhouse user):
chown clickhouse:clickhouse /path/to/part/column.bin /path/to/part/column.mrk2
chmod --reference=/path/to/part/other_column.bin /path/to/part/column.bin
chmod --reference=/path/to/part/other_column.mrk2 /path/to/part/column.mrk2

# Remove checksums.txt from the part folder so ClickHouse regenerates it:
rm /path/to/part/checksums.txt

# Then attach the part or run:
# CHECK TABLE ... SETTINGS check_query_single_value_result=0
```

## Exit codes

- **0**: Completed successfully and no corrupted blocks were detected (or `--help` was shown).
- **1**: Corrupted blocks were detected, or a recoverable/usage error occurred (missing marks, invalid options, etc.).
- **2**: Unexpected internal error (uncaught exception).

## How this tool works

Internally, the tool treats a MergeTree column file as a sequence of compressed blocks with a stable on-disk layout: a 16-byte CityHash128 checksum, a 1-byte compression method, two 4-byte integers for compressed and decompressed sizes, followed by the codec payload. It first memory-maps the `.bin` file and walks this structure, optionally guided by the `.mrk2` / `.cmrk2` mark file to recover from corrupted headers and to associate blocks with logical row counts. Each block is described by lightweight metadata (`BlockInfo`) that is then fed into later phases.

During scanning, the tool validates the stored checksum against a freshly computed CityHash128 over the compressed data (unless `--no-checksum` is set) and then attempts full decompression via the configured codec (LZ4, ZSTD, or NONE). If decompression fails but some output can still be salvaged, it runs a partial decompression path that records how many bytes were safely produced and writes both the raw on-disk block and any partial decompressed buffer to disk for forensic inspection. Scan results capture per-block health, codec, sizes, and salvage information, and are summarised in `repair.log`.

Format detection and repair build on these scan results. If `--format` is not provided, the tool inspects healthy blocks (and, when available, marks) to infer the ClickHouse type, using fixed-width heuristics for numeric types and VarUInt-prefixed validation for `String`. In repair mode, each corrupted block is replaced either by a block built purely from default values or, when safe, by a mixture of salvaged rows and defaults. The notion of "default" is configurable: by default it uses the type's built-in zero/empty value; `--default-value` lets you supply an explicit literal that is serialized according to the detected type (including the nested type of `Nullable(...)`); and `--default-null` fills `Nullable` columns with NULLs by writing a null map of ones plus nested defaults.

Row alignment is preserved by deriving the intended row count per block from the mark file whenever possible and then keeping `rows_count` and `offset_in_decompressed_block` unchanged when regenerating marks. Only the on-disk offsets are remapped to point at the new block positions. For variable-width types like `String` where no marks are available, the tool cannot reliably infer a row count, so it falls back to treating such blocks as empty while still maintaining the surrounding marks; this can discard data in that region, but it does not shift subsequent rows. When no mark file is provided at all, only the `.bin` file is repaired—mark regeneration is intentionally skipped because a correct `.mrk2` / `.cmrk2` cannot be reconstructed from the compressed stream alone.

### Sort key / primary key repair internals

ClickHouse MergeTree stores data sorted by the table's `ORDER BY` expression. Within each data part, every column's `.bin` file is a sequence of compressed blocks containing 8192 rows (the default `index_granularity`). For ORDER BY columns, ClickHouse enforces a strict invariant: rows are stored in ascending sort order both within each block and across consecutive blocks. During merges and mutations, `CheckSortedTransform` verifies this by comparing the last row of each chunk against the first row of the next. A violation triggers `LOGICAL_ERROR` code 49 and aborts the operation.

When `--primary-key` is used, the tool enforces this invariant during repair through three mechanisms:

**1. Boundary extraction.** Before the repair loop begins, the tool decompresses every healthy block and extracts its first and last value (using native ClickHouse binary format: raw fixed-width bytes for numeric types, or VarUInt-prefixed bytes for String). These boundary pairs are stored in a per-block array that acts as a lookup table for the repair phase.

**2. Sort-aware block repair.** For each block that needs repair, the tool selects a strategy based on the block's condition:

- **Checksum invalid, decompression succeeded:** The decompressed data exists but cannot be trusted. The tool scans row-by-row from the start to find the longest prefix where each row is >= the previous row (`compareValues`). If the entire block is sorted, it is used as-is with a regenerated checksum. If a sort violation is found at row N, rows 0 through N-1 are kept and the remaining rows are filled with the value at row N-1 (the last sorted value). If that value falls outside the inter-block boundary range `[prev_block_last, next_block_first]`, the previous block's last value is used instead.

- **Decompression failed, partial data available:** The tool parses as many complete rows as possible from the partial decompression output. It then walks backward from the last parsed row to find the largest row index whose value lies within `[prev_block_last, next_block_first]`. Rows up to that point are kept; the rest are filled with that row's value.

- **No usable data:** The block is filled entirely with the previous block's last value (to maintain sort continuity), or the type's default value if no previous boundary is available.

**3. Boundary propagation.** After each block is repaired, the tool updates the boundary array with the effective first and last values of the repaired block. This ensures that the next block's repair sees the actual post-repair boundaries rather than stale pre-repair values. Without this step, a sequence of consecutive corrupted blocks would all use the same (too-old) boundary and produce values that violate inter-block sort order among themselves.

## Codec support

Decompression and compression are supported for **LZ4**, **ZSTD**, and **NONE** codecs. These cover the vast majority of ClickHouse data in practice.

Pipeline codecs (Delta, DoubleDelta, Gorilla, T64, FPC, GCD) and encryption codecs (AES) are recognized during scanning but cannot be decompressed. Blocks using these codecs will be reported as "unsupported codec". In repair mode, corrupted blocks with unsupported codecs are replaced with LZ4-compressed default blocks (which ClickHouse can read without issues).
