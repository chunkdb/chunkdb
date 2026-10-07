// chunkdb_migrate: converts a data directory written by chunkdb 1.x (or by the
// unreleased storage format of the 2.0 development line) into a new 2.0 data
// directory. See docs/MIGRATING.md.
//
// Exit status: 0 = converted, 1 = refused or failed (nothing written to
// --to), 2 = invalid invocation.

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>

#include "chunkdb/logging.hpp"
#include "migrate.hpp"

namespace {

void PrintUsage() {
    std::cout
        << "Usage: chunkdb_migrate --from <old-dir> --to <new-dir> [options]\n"
        << "  --from <path>                  data directory of chunkdb 1.x (not changed)\n"
        << "  --to <path>                    new data directory; must not exist or be empty\n"
        << "Geometry the old server ran with (its defaults when omitted):\n"
        << "  --block-bits <n>               default 16\n"
        << "  --chunk-width <n>              default 16\n"
        << "  --chunk-height <n>             default 16\n"
        << "  --large-chunk-width <n>        default 8\n"
        << "  --large-chunk-height <n>       default 8\n"
        << "Options of the new default table (chunkdb_server's defaults when omitted):\n"
        << "  --durability <relaxed|fsync-wal|fsync-checkpoint>\n"
        << "  --checkpoint-updates <n>\n"
        << "  --checkpoint-wal-bytes <n>\n"
        << "  --wal-group-commit-updates <n>\n"
        << "  --checkpoint-compression <none|zrle>\n"
        << "  --accept-loss                  convert even when the old server dropped data\n"
        << "                                 (a damaged WAL or image), dropping the same data\n";
}

[[nodiscard]] std::uint64_t ParseNumber(const std::string& text, const char* name, std::uint64_t min) {
    std::size_t consumed = 0;
    std::uint64_t value = 0;
    try {
        value = std::stoull(text, &consumed, 10);
    } catch (const std::exception&) {
        consumed = 0;
    }
    if (consumed != text.size() || text.empty() || text[0] == '-' || value < min) {
        throw std::invalid_argument(std::string("invalid value for ") + name + ": " + text);
    }
    return value;
}

void PrintSummary(const chunkdb::migrate::MigrateSummary& s) {
    std::cout << "converted chunks=" << s.chunks << " absent=" << s.absent_chunks
              << " source_bytes=" << s.source_bytes << "\n";
    std::cout << "images v1=" << s.images_by_version[1] << " v2=" << s.images_by_version[2]
              << " v3=" << s.images_by_version[3] << " v4=" << s.images_by_version[4]
              << " v5=" << s.images_by_version[5] << "\n";
    std::cout << "wals v2=" << s.wals_v2 << " v3=" << s.wals_v3 << " v4=" << s.wals_v4
              << " headerless=" << s.wals_headerless << " mixed=" << s.wals_mixed
              << " torn_tails=" << s.torn_tails << "\n";
    std::cout << "intents rollback=" << s.rollback_intents << " committed=" << s.committed_intents
              << "\n";
    std::cout << "revisions persisted=" << s.persisted_revisions << " assigned=" << s.assigned_revisions
              << " source_clock_ceiling=" << s.source_clock_ceiling << "\n";
    std::cout << "canonicalized_chunks=" << s.canonicalized_chunks << "\n";
    for (const auto& note : s.notes) {
        std::cout << "note: " << note << "\n";
    }
    for (const auto& loss : s.losses) {
        std::cout << "dropped: " << loss << "\n";
    }
    std::cout << "verify checked=" << s.verify.checked << " warnings=" << s.verify.warnings
              << " errors=" << s.verify.errors << "\n";
}

}  // namespace

int main(int argc, char** argv) {
    chunkdb::migrate::MigrateOptions options;
    options.geometry = {
        .large_chunk_width_chunks = 8,
        .large_chunk_height_chunks = 8,
        .chunk_width_blocks = 16,
        .chunk_height_blocks = 16,
        .block_bits = 16,
    };
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            auto value = [&](const char* name) -> std::string {
                if (i + 1 >= argc) {
                    throw std::invalid_argument(std::string("missing value for ") + name);
                }
                return argv[++i];
            };
            auto u32 = [&](const char* name) {
                const auto parsed = ParseNumber(value(name), name, 1);
                if (parsed > 0xFFFFFFFFULL) {
                    throw std::invalid_argument(std::string("value too large for ") + name);
                }
                return static_cast<std::uint32_t>(parsed);
            };
            if (arg == "--from") {
                options.from = value("--from");
            } else if (arg == "--to") {
                options.to = value("--to");
            } else if (arg == "--block-bits") {
                options.geometry.block_bits = u32("--block-bits");
            } else if (arg == "--chunk-width") {
                options.geometry.chunk_width_blocks = u32("--chunk-width");
            } else if (arg == "--chunk-height") {
                options.geometry.chunk_height_blocks = u32("--chunk-height");
            } else if (arg == "--large-chunk-width") {
                options.geometry.large_chunk_width_chunks = u32("--large-chunk-width");
            } else if (arg == "--large-chunk-height") {
                options.geometry.large_chunk_height_chunks = u32("--large-chunk-height");
            } else if (arg == "--durability") {
                options.table_options.durability_mode = chunkdb::ParseDurabilityMode(value("--durability"));
            } else if (arg == "--checkpoint-updates") {
                options.table_options.checkpoint_update_interval =
                    ParseNumber(value("--checkpoint-updates"), "--checkpoint-updates", 1);
            } else if (arg == "--checkpoint-wal-bytes") {
                options.table_options.checkpoint_wal_bytes =
                    ParseNumber(value("--checkpoint-wal-bytes"), "--checkpoint-wal-bytes", 1);
            } else if (arg == "--wal-group-commit-updates") {
                options.table_options.wal_group_commit_updates =
                    ParseNumber(value("--wal-group-commit-updates"), "--wal-group-commit-updates", 1);
            } else if (arg == "--checkpoint-compression") {
                options.table_options.checkpoint_compression =
                    chunkdb::ParseCheckpointCompression(value("--checkpoint-compression"));
            } else if (arg == "--accept-loss") {
                options.accept_loss = true;
            } else if (arg == "--help" || arg == "-h") {
                PrintUsage();
                return 0;
            } else {
                throw std::invalid_argument("unknown argument: " + arg);
            }
        }
        if (options.from.empty() || options.to.empty()) {
            throw std::invalid_argument("--from and --to are required");
        }
    } catch (const std::exception& e) {
        std::cerr << "chunkdb_migrate: " << e.what() << "\n";
        PrintUsage();
        return 2;
    }

    // Engine logs (table creation, checkpoints) are noise here.
    chunkdb::SetLogLevel(chunkdb::LogLevel::kError);
    try {
        const auto summary = chunkdb::migrate::Migrate(options);
        PrintSummary(summary);
        std::cout << "chunkdb_migrate: converted " << options.from.string() << " into "
                  << options.to.string() << "\n";
        return 0;
    } catch (const chunkdb::migrate::MigrateRefused& e) {
        std::cerr << "chunkdb_migrate: " << e.what() << ":\n";
        for (const auto& problem : e.problems()) {
            std::cerr << "  " << problem << "\n";
        }
        std::cerr << "nothing was written to " << options.to.string() << "\n";
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "chunkdb_migrate: " << e.what() << "\nnothing was written to "
                  << options.to.string() << "\n";
        return 1;
    }
}
