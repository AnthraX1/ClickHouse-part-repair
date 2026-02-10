/// clickhouse-part-repair: Scan, diagnose, and repair corrupted ClickHouse MergeTree .bin column files.
///
/// Usage:
///   clickhouse-part-repair [options] <column.bin> [column.mrk2|column.cmrk2]
///
/// See --help for full option list.

#include "BlockIterator.h"
#include "BlockScanner.h"
#include "BlockRepairer.h"
#include "FormatDetector.h"
#include "MarkFileHandler.h"
#include "Utils.h"

#include <iostream>
#include <filesystem>
#include <string>

#include <boost/program_options.hpp>

namespace po = boost::program_options;
namespace fs = std::filesystem;

int main(int argc, char ** argv)
{
    try
    {
        po::options_description desc("clickhouse-part-repair — scan, diagnose, and repair corrupted ClickHouse .bin column files");
        desc.add_options()
            ("help,h", "Show this help message")
            ("input", po::value<std::string>()->required()->value_name("COLUMN.BIN"),
                "Path to the column .bin file")
            ("marks", po::value<std::string>()->default_value("")->value_name("COLUMN.MRK2"),
                "Path to the mark file (.mrk2 or .cmrk2). Optional but recommended for repair mode.")
            ("format", po::value<std::string>()->default_value("")->value_name("TYPE"),
                "Column type (e.g., UInt64, String, Nullable(Float64)). "
                "If omitted, auto-detect from healthy blocks.")
            ("no-checksum", po::bool_switch()->default_value(false),
                "Skip checksum validation, attempt raw decompression of all blocks")
            ("bruteforce", po::bool_switch()->default_value(false),
                "Attempt to recover blocks after a corrupted header by bruteforce-scanning for plausible block headers "
                "(slower; use when no mark file is available)")
            ("repair", po::bool_switch()->default_value(false),
                "Enable repair mode: create new repaired files in --output-dir without overwriting the originals "
                "(replace corrupted blocks with default values and regenerate the mark file if present)")
            ("default-value", po::value<std::string>()->default_value("")->value_name("VALUE"),
                "Override the default value used for repaired rows. "
                "The string VALUE is parsed according to --format / detected type. "
                "Mutually exclusive with --default-null. Repair mode only.")
            ("default-null", po::bool_switch()->default_value(false),
                "Use NULL as the default for repaired rows. "
                "Only valid for Nullable(...) column types and mutually exclusive with --default-value.")
            ("total-blocks", po::value<std::size_t>()->default_value(0)->value_name("N"),
                "Expected total number of blocks in the column file. "
                "Required with --repair when no mark file is provided. "
                "Without a .mrk2/.cmrk2 file, the true block count cannot be derived reliably from the .bin alone. "
                "You can obtain N by running this tool on a healthy column bin file within the part folder and reading the "
                "\"Found N blocks\" line.")
            ("output-dir", po::value<std::string>()->default_value("./repair_output")->value_name("DIR"),
                "Directory for salvaged data, logs, and repaired files")
            ("log", po::value<std::string>()->default_value("")->value_name("FILE"),
                "Path to detailed log file (default: <output-dir>/repair.log)")
        ;

        po::positional_options_description positional;
        positional.add("input", 1);
        positional.add("marks", 1);

        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(desc).positional(positional).run(), vm);

        if (vm.count("help") || argc == 1)
        {
            std::cout << "Usage: " << argv[0] << " [options] <column.bin> [column.mrk2|column.cmrk2]\n\n";
            std::cout << desc << "\n";
            std::cout << "Examples:\n";
            std::cout << "  # Scan and diagnose corruption:\n";
            std::cout << "  " << argv[0] << " dir_name.bin dir_name.mrk2\n\n";
            std::cout << "  # Scan without checksum validation:\n";
            std::cout << "  " << argv[0] << " --no-checksum dir_name.bin\n\n";
            std::cout << "  # Repair with explicit type:\n";
            std::cout << "  " << argv[0] << " --repair --format UInt64 dir_name.bin dir_name.mrk2\n\n";
            return 0;
        }

        po::notify(vm);

        std::string bin_path = vm["input"].as<std::string>();
        std::string mark_path = vm["marks"].as<std::string>();
        std::string format = vm["format"].as<std::string>();
        bool skip_checksum = vm["no-checksum"].as<bool>();
        bool bruteforce = vm["bruteforce"].as<bool>();
        bool repair_mode = vm["repair"].as<bool>();
        std::string default_value_literal = vm["default-value"].as<std::string>();
        bool default_null = vm["default-null"].as<bool>();
        std::size_t expected_total_blocks = vm["total-blocks"].as<std::size_t>();
        std::string output_dir = vm["output-dir"].as<std::string>();
        std::string log_path = vm["log"].as<std::string>();

        if (log_path.empty())
            log_path = output_dir + "/repair.log";

        // Validate input file exists
        if (!fs::exists(bin_path))
        {
            std::cerr << "Error: input file does not exist: " << bin_path << std::endl;
            return 1;
        }

        PartRepair::ensureDirectory(output_dir);
        PartRepair::Logger logger(log_path);

        logger.info("=== clickhouse-part-repair ===");
        logger.info("Input: " + bin_path);
        if (!mark_path.empty())
            logger.info("Marks: " + mark_path);
        logger.info("Skip checksum: " + std::string(skip_checksum ? "yes" : "no"));
        logger.info("Bruteforce mode: " + std::string(bruteforce ? "yes" : "no"));
        logger.info("Repair mode: " + std::string(repair_mode ? "yes" : "no"));
        if (!default_value_literal.empty())
            logger.info("Custom default value (--default-value): '" + default_value_literal + "'");
        if (default_null)
            logger.info("Use NULL for repaired rows (--default-null): yes");
        if (expected_total_blocks > 0)
            logger.info("Expected total blocks (from --total-blocks): " + std::to_string(expected_total_blocks));
        logger.info("Output dir: " + output_dir);
        logger.info("Log file: " + log_path);

        // require either a mark file (as an argument) or --bruteforce.
        if (mark_path.empty() && !bruteforce)
        {
            std::cerr << "Error: one of --marks or --bruteforce must be provided.\n"
                      << "With neither a mark file nor bruteforce, the tool cannot reliably continue past\n"
                      << "a corrupted header or determine missing blocks.\n";
            return 1;
        }

        if (repair_mode && mark_path.empty() && expected_total_blocks == 0)
        {
            std::cerr << "Error: --total-blocks is required when using --repair without a mark file.\n"
                      << "Without a .mrk2/.cmrk2 file, the true total block count cannot be recovered "
                      << "reliably from the .bin alone.\n"
                      << "You can obtain the correct block count by running this tool on another healthy part "
                      << "with the same structure and reading the \"Found N blocks\" line in the log.\n";
            return 1;
        }

        // --- Read marks if available (for iteration guidance and repair) ---
        std::vector<PartRepair::MarkEntry> marks;
        std::vector<uint64_t> mark_block_offsets;
        if (!mark_path.empty() && fs::exists(mark_path))
        {
            try
            {
                PartRepair::MarkFileHandler mark_handler(logger);
                marks = mark_handler.readMarks(mark_path);

                auto block_to_marks = mark_handler.buildBlockToMarksMap(marks);
                mark_block_offsets.reserve(block_to_marks.size());
                for (const auto & kv : block_to_marks)
                    mark_block_offsets.push_back(kv.first);

                if (!mark_block_offsets.empty())
                {
                    logger.info("Using mark file offsets to continue block iteration past corrupted headers when possible.");
                }
            }
            catch (const std::exception & e)
            {
                logger.warn("Failed to read mark file: " + std::string(e.what()));
                logger.warn("Continuing without marks.");
            }
        }
        else if (!mark_path.empty())
        {
            logger.warn("Mark file not found: " + mark_path);
        }

        if (mark_block_offsets.empty())
        {
            if (bruteforce)
            {
                logger.info("No mark offsets available; bruteforce mode will attempt to recover blocks after corrupted headers.");
            }
            else
            {
                logger.warn("No mark offsets available; if a header error occurs, blocks after the corrupted block "
                    "can only be recovered by re-running with --bruteforce (slower, best-effort).");
            }
        }

        // --- Phase 1: Block Iteration ---
        logger.info("");
        logger.info("--- Phase 1: Reading blocks ---");
        PartRepair::BlockIterator iterator(bin_path);
        logger.info("File size: " + PartRepair::humanReadableSize(iterator.fileSize()));

        const std::vector<uint64_t> * mark_offsets_ptr =
            mark_block_offsets.empty() ? nullptr : &mark_block_offsets;

        auto blocks = iterator.readAllBlocks(mark_offsets_ptr, bruteforce);
        logger.info("Found " + std::to_string(blocks.size()) + " blocks");

        if (expected_total_blocks > 0 && expected_total_blocks != blocks.size())
        {
            logger.warn("Expected total blocks: " + std::to_string(expected_total_blocks)
                + ", but discovered " + std::to_string(blocks.size())
                + " blocks from the file structure. This suggests missing or unrecoverable blocks "
                + "in the corrupted region.");
        }

        // --- Phase 2: Block Scanning ---
        logger.info("");
        logger.info("--- Phase 2: Scanning blocks ---");
        PartRepair::BlockScanner scanner(skip_checksum, output_dir, logger, bin_path);
        auto scan_results = scanner.scan(blocks);

        // Print summary
        size_t healthy_count = 0, corrupt_count = 0;
        for (const auto & r : scan_results)
        {
            if (r.health == PartRepair::BlockHealthStatus::HEALTHY)
                ++healthy_count;
            else
                ++corrupt_count;
        }

        logger.info("");
        logger.info("=== Scan Summary ===");
        logger.info("Total blocks: " + std::to_string(blocks.size()));
        logger.info("Healthy: " + std::to_string(healthy_count));
        logger.info("Corrupted: " + std::to_string(corrupt_count));

        if (corrupt_count == 0)
        {
            logger.info("No corruption detected. File appears healthy.");
            if (!repair_mode)
                return 0;
        }

        // --- Phase 3: Format Detection ---
        std::string detected_format = format;
        if (detected_format.empty())
        {
            logger.info("");
            logger.info("--- Phase 3: Format Detection ---");
            PartRepair::FormatDetector detector(logger);
            detected_format = detector.detect(scan_results, marks.empty() ? nullptr : &marks);

            // Once the format is determined, free any decompressed block data
            // retained for sampling. From this point onward, we only need
            // per-block metadata and (for corrupted blocks) partial salvage
            // buffers, which are stored separately in ScanResult::partial_data.
            for (auto & r : scan_results)
            {
                std::vector<char>().swap(r.decompressed_data);
            }

            if (detected_format.empty())
            {
                if (repair_mode)
                {
                    logger.error("Cannot auto-detect column type. Please specify --format for repair mode.");
                    return 1;
                }
                logger.warn("Could not auto-detect column type. Specify --format for repair.");
            }
            else
            {
                logger.info("Detected format: " + detected_format);
            }
        }
        else
        {
            logger.info("Using specified format: " + detected_format);
        }

        // Validate default-value/default-null options after the final format is known.
        if (!default_value_literal.empty() && default_null)
        {
            std::cerr << "Error: --default-value and --default-null are mutually exclusive.\n";
            return 1;
        }

        if (default_null)
        {
            // Require a Nullable(...) column type for --default-null.
            if (detected_format.rfind("Nullable(", 0) != 0 || detected_format.size() < 11 || detected_format.back() != ')')
            {
                std::cerr << "Error: --default-null requires a Nullable(...) column type. "
                          << "Current type: '" << detected_format << "'.\n";
                return 1;
            }
        }

        // --- Phase 4: Repair (if requested) ---
        if (repair_mode)
        {
            if (detected_format.empty())
            {
                logger.error("Repair mode requires a column type (--format or auto-detected).");
                return 1;
            }

            logger.info("");
            logger.info("--- Phase 4: Repair ---");

            std::string output_bin = output_dir + "/" + fs::path(bin_path).filename().string() + ".repaired";
            std::string output_mark;
            if (!mark_path.empty())
            {
                output_mark = output_dir + "/" + fs::path(mark_path).filename().string() + ".repaired";
            }

            PartRepair::BlockRepairer repairer(
                detected_format,
                logger,
                bin_path,
                default_value_literal,
                default_null);
            repairer.repair(blocks, scan_results, marks, mark_path, output_bin, output_mark);

            logger.info("");
            logger.info("=== Repair Complete ===");
            logger.info("Repaired .bin: " + output_bin);
            if (!output_mark.empty())
                logger.info("Repaired marks: " + output_mark);
            logger.info("");
            logger.info("To use the repaired files, replace the originals:");
            logger.info("  cp " + output_bin + " " + bin_path);
            if (!output_mark.empty())
                logger.info("  cp " + output_mark + " " + mark_path);
            logger.info("Then regenerate checksums.txt or use CHECK TABLE ... SETTINGS check_query_single_value_result=0");
        }

        // Print salvage file summary for corrupted blocks
        bool has_salvage = false;
        for (const auto & r : scan_results)
        {
            if (!r.raw_block_file.empty() || !r.partial_data_file.empty())
            {
                if (!has_salvage)
                {
                    logger.info("");
                    logger.info("=== Salvaged Files ===");
                    has_salvage = true;
                }

                std::string msg = "Block " + std::to_string(r.block_index) + ":";
                if (!r.raw_block_file.empty())
                    msg += " raw=" + r.raw_block_file;
                if (!r.partial_data_file.empty())
                    msg += " partial=" + r.partial_data_file
                        + " (" + std::to_string(r.partial_bytes) + "/" + std::to_string(r.decompressed_size) + " bytes)";
                logger.info(msg);
            }
        }

        logger.info("");
        logger.info("Done. See log file for details: " + log_path);
        return corrupt_count > 0 ? 1 : 0;
    }
    catch (const po::error & e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        std::cerr << "Use --help for usage information." << std::endl;
        return 1;
    }
    catch (const std::exception & e)
    {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 2;
    }
}
