// chunkdb_verify: offline, read-only integrity verification for a chunkdb
// data directory.
//
// Output is machine-usable: one finding per line in the form
//   VERIFY <level> <code> <path> [detail...]
// followed by a summary line
//   SUMMARY checked=<n> warnings=<n> errors=<n>
//
// Each table's geometry comes from its manifest (`tables/<name>/table.manifest`).
// A missing or damaged manifest is an error, and that table's chunk artifacts
// are then not checked.
//
// Paths and details are emitted as C-style quoted, escaped tokens so that
// spaces, newlines, and other control characters can never split or forge a
// field. A consumer can unambiguously parse each finding.
//
// Exit status: 0 = clean, 1 = findings (warnings or errors), 2 = fatal
// (invalid invocation or the data directory could not be inspected).

#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include "verify.hpp"

namespace {

void PrintUsage() {
    std::cout
        << "Usage: chunkdb_verify --data-dir <path> [options]\n"
        << "  --data-dir <path>          data directory to verify (required)\n"
        << "Every table is checked with the geometry recorded in its manifest.\n"
        << "Verification is read-only; it never modifies the data directory.\n";
}

}  // namespace

int main(int argc, char** argv) {
    std::filesystem::path data_dir;

    try {
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            auto require_value = [&](const char* name) -> std::string {
                if (i + 1 >= argc) {
                    throw std::invalid_argument(std::string("missing value for ") + name);
                }
                ++i;
                return argv[i];
            };
            if (arg == "--data-dir") {
                data_dir = require_value("--data-dir");
            } else if (arg == "--help" || arg == "-h") {
                PrintUsage();
                return 0;
            } else {
                throw std::invalid_argument("unknown argument: " + arg);
            }
        }
        if (data_dir.empty()) {
            throw std::invalid_argument("--data-dir is required");
        }
    } catch (const std::exception& e) {
        std::cerr << "chunkdb_verify: " << e.what() << "\n";
        PrintUsage();
        return 2;
    }

    chunkdb::VerifyCounters counters;
    try {
        std::error_code exists_ec;
        if (!std::filesystem::is_directory(data_dir, exists_ec) || exists_ec) {
            std::cerr << "chunkdb_verify: data directory not found: " << data_dir.string() << "\n";
            return 2;
        }
        counters = chunkdb::VerifyDataDirectory(data_dir, std::cout);
    } catch (const std::exception& e) {
        std::cerr << "chunkdb_verify: fatal: " << e.what() << "\n";
        return 2;
    }

    std::cout << "SUMMARY checked=" << counters.checked << " warnings=" << counters.warnings
              << " errors=" << counters.errors << "\n";
    return (counters.warnings + counters.errors) > 0 ? 1 : 0;
}
