// chunkdb_verify: offline, read-only integrity verification for a chunkdb
// data directory.
//
// Output is machine-usable: one finding per line in the form
//   VERIFY <level> <code> <path> [detail...]
// followed by a summary line
//   SUMMARY checked=<n> warnings=<n> errors=<n>
//
// The geometry comes from the store manifest (`chunkdb.manifest`). A missing
// or damaged manifest is an error, and chunk artifacts are then not checked.
//
// Paths and details are emitted as C-style quoted, escaped tokens so that
// spaces, newlines, and other control characters can never split or forge a
// field. A consumer can unambiguously parse each finding.
//
// Exit status: 0 = clean, 1 = findings (warnings or errors), 2 = fatal
// (invalid invocation or the data directory could not be inspected).

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/geometry.hpp"
#include "store_manifest.hpp"
#include "wal_replay.hpp"

namespace {

struct VerifyCounters {
    std::uint64_t checked = 0;
    std::uint64_t warnings = 0;
    std::uint64_t errors = 0;
};

// Emits `text` as a double-quoted token with C-style escapes, so a path or
// detail containing spaces, quotes, or control characters cannot break the
// one-finding-per-line, space-separated field format.
[[nodiscard]] std::string QuoteToken(const std::string& text) {
    std::string out;
    out.reserve(text.size() + 2);
    out.push_back('"');
    for (const unsigned char ch : text) {
        switch (ch) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (ch < 0x20 || ch == 0x7F) {
                    static const char* kHex = "0123456789abcdef";
                    out += "\\x";
                    out.push_back(kHex[ch >> 4]);
                    out.push_back(kHex[ch & 0x0F]);
                } else {
                    out.push_back(static_cast<char>(ch));
                }
        }
    }
    out.push_back('"');
    return out;
}

void Report(
    VerifyCounters* counters,
    bool is_error,
    const std::string& code,
    const std::filesystem::path& path,
    const std::string& detail) {
    if (is_error) {
        ++counters->errors;
    } else {
        ++counters->warnings;
    }
    std::cout << "VERIFY " << (is_error ? "error" : "warning") << " " << code << " "
              << QuoteToken(path.string());
    if (!detail.empty()) {
        std::cout << " " << QuoteToken(detail);
    }
    std::cout << "\n";
}

void ReportInfo(
    const std::string& code,
    const std::filesystem::path& path,
    const std::string& detail) {
    std::cout << "VERIFY info " << code << " " << QuoteToken(path.string());
    if (!detail.empty()) {
        std::cout << " " << QuoteToken(detail);
    }
    std::cout << "\n";
}

[[nodiscard]] bool ParseCoordSuffix(
    const std::string& stem,
    const std::string& prefix,
    std::int64_t* out_x,
    std::int64_t* out_y) {
    if (stem.rfind(prefix, 0) != 0) {
        return false;
    }
    const std::string rest = stem.substr(prefix.size());
    const std::size_t separator = rest.find('_');
    if (separator == std::string::npos) {
        return false;
    }
    return chunkdb::TryParseInt64(rest.substr(0, separator), out_x) &&
           chunkdb::TryParseInt64(rest.substr(separator + 1), out_y);
}

[[nodiscard]] bool IsTmpArtifactName(const std::string& name) {
    return name.find(".tmp.") != std::string::npos;
}

void PrintUsage() {
    std::cout
        << "Usage: chunkdb_verify --data-dir <path> [options]\n"
        << "  --data-dir <path>          data directory to verify (required)\n"
        << "The geometry is read from the store manifest in the data directory.\n"
        << "Verification is read-only; it never modifies the data directory.\n";
}

void VerifyChunkFile(
    const chunkdb::Geometry& geometry,
    const std::filesystem::path& path,
    const chunkdb::ChunkCoord& coord,
    std::vector<std::uint8_t>* payload_out,
    std::vector<std::uint8_t>* presence_out,
    bool* image_ok,
    VerifyCounters* counters) {
    *image_ok = false;
    try {
        const auto bytes = chunkdb::LoadFile(path);
        auto image = chunkdb::ParseChunkImage(bytes, geometry, coord);
        *payload_out = std::move(image.payload);
        *presence_out = std::move(image.presence_bitmap);
        *image_ok = true;
    } catch (const std::exception& e) {
        Report(counters, true, "chunk_image_invalid", path, e.what());
    }
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

    VerifyCounters counters;
    try {
        bool initialized_marker_present = false;
        bool has_storage_artifacts = false;

        std::error_code exists_ec;
        if (!std::filesystem::exists(data_dir, exists_ec) || exists_ec) {
            std::cerr << "chunkdb_verify: data directory not found: " << data_dir.string() << "\n";
            return 2;
        }

        // Chunk artifacts are interpreted with the geometry the store was
        // created with, which only the manifest records.
        const auto manifest_path = chunkdb::StoreManifestPath(data_dir);
        std::optional<chunkdb::Geometry> store_geometry;
        ++counters.checked;
        try {
            const auto manifest = chunkdb::ReadStoreManifest(data_dir);
            if (manifest.has_value()) {
                const auto unknown = chunkdb::UnknownFeatures(manifest->features);
                if (unknown.incompat != 0U) {
                    Report(
                        &counters,
                        true,
                        "manifest_unknown_features",
                        manifest_path,
                        "store uses features this build does not support (unknown " +
                            chunkdb::DescribeFeatures(unknown) +
                            "); chunk artifacts were not checked");
                } else {
                    if (unknown.ro_compat != 0U || unknown.compat != 0U) {
                        Report(
                            &counters,
                            false,
                            "manifest_unknown_features",
                            manifest_path,
                            "unknown " + chunkdb::DescribeFeatures(unknown) +
                                "; data owned by those features was not checked");
                    }
                    store_geometry.emplace(manifest->geometry);
                }
            } else {
                Report(
                    &counters,
                    true,
                    "manifest_missing",
                    manifest_path,
                    "not an initialized chunkdb data directory; chunk artifacts were not checked");
            }
        } catch (const std::exception& e) {
            Report(
                &counters,
                true,
                "manifest_invalid",
                manifest_path,
                std::string(e.what()) + "; chunk artifacts were not checked");
        }

        const auto version_path = data_dir / "chunkdb.version";
        std::error_code version_exists_ec;
        const bool version_present =
            std::filesystem::exists(version_path, version_exists_ec);
        if (version_exists_ec) {
            Report(
                &counters,
                true,
                "version_clock_uninspectable",
                version_path,
                version_exists_ec.message());
        } else if (version_present) {
            ++counters.checked;
            try {
                std::error_code size_ec;
                const auto size = std::filesystem::file_size(version_path, size_ec);
                if (size_ec || size != 16U) {
                    Report(
                        &counters,
                        true,
                        "version_clock_invalid",
                        version_path,
                        size_ec ? size_ec.message() : "expected exactly 16 bytes");
                } else {
                    const auto bytes = chunkdb::LoadFile(version_path);
                    std::uint64_t ceiling = 0;
                    if (!chunkdb::TryParseVersionClockRecord(bytes, &ceiling)) {
                        Report(
                            &counters,
                            true,
                            "version_clock_invalid",
                            version_path,
                            "expected checked CKVR/u64/CRC32 record");
                    }
                }
            } catch (const std::exception& e) {
                Report(
                    &counters,
                    true,
                    "version_clock_unreadable",
                    version_path,
                    e.what());
            }
        }

        const auto snapshot_path = data_dir / "chunkdb.snapshot";
        std::error_code snapshot_exists_ec;
        if (std::filesystem::exists(
                snapshot_path, snapshot_exists_ec)) {
            ++counters.checked;
            try {
                std::uint64_t generation = 0;
                if (!chunkdb::TryParseSnapshotGenerationRecord(
                        chunkdb::LoadFile(snapshot_path),
                        &generation)) {
                    Report(
                        &counters,
                        true,
                        "snapshot_generation_invalid",
                        snapshot_path,
                        "expected 16-byte CKSG/u64/CRC32 record");
                } else if ((generation & 1U) != 0U) {
                    Report(
                        &counters,
                        true,
                        "snapshot_generation_recovery_required",
                        snapshot_path,
                        "odd generation=" +
                            std::to_string(generation) +
                            " means either a live writer inside (or"
                            " lingering on) a transition bracket, or a"
                            " crashed writer whose state requires"
                            " read-write recovery");
                }
            } catch (const std::exception& e) {
                Report(
                    &counters,
                    true,
                    "snapshot_generation_unreadable",
                    snapshot_path,
                    e.what());
            }
        } else if (snapshot_exists_ec) {
            Report(
                &counters,
                true,
                "snapshot_generation_uninspectable",
                snapshot_path,
                snapshot_exists_ec.message());
        }

        const auto initialized_path = data_dir / ".chunkdb.initialized";
        std::error_code initialized_exists_ec;
        if (std::filesystem::exists(initialized_path, initialized_exists_ec)) {
            initialized_marker_present = true;
            ++counters.checked;
            try {
                if (!chunkdb::IsValidInitializedStoreMarker(
                        chunkdb::LoadFile(initialized_path))) {
                    Report(
                        &counters,
                        true,
                        "initialized_marker_invalid",
                        initialized_path,
                        "expected 16-byte CKID/u64(1)/CRC32 record");
                }
            } catch (const std::exception& e) {
                Report(
                    &counters,
                    true,
                    "initialized_marker_unreadable",
                    initialized_path,
                    e.what());
            }
        } else if (initialized_exists_ec) {
            Report(
                &counters,
                true,
                "store_marker_uninspectable",
                initialized_path,
                initialized_exists_ec.message());
        }

        for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
            const auto name = entry.path().filename().string();

            // Process-lock artifacts and OS metadata are not storage state.
            if (name.rfind(".chunkdb", 0) == 0 || name.rfind(".", 0) == 0) {
                continue;
            }

            if (entry.is_regular_file()) {
                if (IsTmpArtifactName(name)) {
                    Report(&counters, false, "tmp_artifact", entry.path(), "");
                    continue;
                }
                if (name == "chunkdb.lock" || name == "chunkdb.lock.meta" ||
                    name.rfind("chunkdb.", 0) == 0) {
                    continue;
                }
                const auto ext = entry.path().extension();
                if (ext == ".chk" || ext == ".wal" || ext == ".rgn" || ext == ".rollback") {
                    // Chunk data outside any chunk directory is never read.
                    Report(&counters, false, "unexpected_file", entry.path(), "");
                } else {
                    ReportInfo("foreign_entry", entry.path(), "not chunkdb data; ignored");
                }
                continue;
            }

            if (!entry.is_directory()) {
                continue;
            }

            std::int64_t large_x = 0;
            std::int64_t large_y = 0;
            if (!ParseCoordSuffix(name, "L_", &large_x, &large_y)) {
                ReportInfo("foreign_entry", entry.path(), "not chunkdb data; ignored");
                continue;
            }
            has_storage_artifacts = true;
            if (!store_geometry.has_value()) {
                continue;
            }
            const chunkdb::Geometry& geometry = *store_geometry;

            for (const auto& file : std::filesystem::directory_iterator(entry.path())) {
                const auto file_name = file.path().filename().string();
                if (!file.is_regular_file()) {
                    Report(&counters, false, "unexpected_entry", file.path(), "");
                    continue;
                }
                if (IsTmpArtifactName(file_name)) {
                    Report(&counters, false, "tmp_artifact", file.path(), "");
                    continue;
                }
                const auto ext = file.path().extension();
                std::int64_t chunk_x = 0;
                std::int64_t chunk_y = 0;
                const bool coord_ok =
                    ParseCoordSuffix(file.path().stem().string(), "C_", &chunk_x, &chunk_y);

                if (ext == ".chk") {
                    ++counters.checked;
                    if (!coord_ok) {
                        Report(&counters, true, "chunk_name_invalid", file.path(), "");
                        continue;
                    }
                    const chunkdb::ChunkCoord coord{chunk_x, chunk_y};
                    const auto expected_large = geometry.ChunkToLarge(coord);
                    if (expected_large.x != large_x || expected_large.y != large_y) {
                        Report(
                            &counters, true, "chunk_misplaced", file.path(),
                            "expected_dir=L_" + std::to_string(expected_large.x) + "_" +
                                std::to_string(expected_large.y));
                        continue;
                    }
                    std::vector<std::uint8_t> payload;
                    std::vector<std::uint8_t> presence;
                    bool image_ok = false;
                    VerifyChunkFile(
                        geometry, file.path(), coord, &payload, &presence, &image_ok,
                        &counters);
                } else if (ext == ".wal") {
                    ++counters.checked;
                    if (!coord_ok) {
                        Report(&counters, true, "wal_name_invalid", file.path(), "");
                        continue;
                    }
                    const chunkdb::ChunkCoord coord{chunk_x, chunk_y};
                    const auto expected_large = geometry.ChunkToLarge(coord);
                    if (expected_large.x != large_x || expected_large.y != large_y) {
                        Report(
                            &counters, true, "wal_misplaced", file.path(),
                            "expected_dir=L_" + std::to_string(expected_large.x) + "_" +
                                std::to_string(expected_large.y));
                        continue;
                    }
                    try {
                        const auto wal_bytes = chunkdb::LoadFile(file.path());
                        std::vector<std::uint8_t> payload(geometry.ChunkPayloadBytes(), 0U);
                        std::vector<std::uint8_t> presence(
                            (geometry.ChunkBlockCount() + 7U) / 8U, 0U);
                        // Seed replay from the checkpoint image when present.
                        const auto image_path =
                            chunkdb::ChunkDataPath(data_dir, geometry, coord);
                        if (std::filesystem::exists(image_path)) {
                            try {
                                const auto image_bytes = chunkdb::LoadFile(image_path);
                                auto image =
                                    chunkdb::ParseChunkImage(image_bytes, geometry, coord);
                                payload = std::move(image.payload);
                                presence = std::move(image.presence_bitmap);
                            } catch (...) {
                                // Reported separately when the .chk file is
                                // visited; replay from an empty base here.
                            }
                        }
                        const auto replay = chunkdb::ReplayWal(
                            wal_bytes, geometry, coord, &payload, &presence);
                        if (!replay.replayable) {
                            Report(
                                &counters, true, "wal_not_replayable", file.path(),
                                replay.stop_reason);
                        } else if (replay.tail_truncated_or_corrupt) {
                            Report(
                                &counters, false, "wal_tail_truncated", file.path(),
                                "applied_records=" + std::to_string(replay.applied_records) +
                                    " reason=" + replay.stop_reason);
                        }
                    } catch (const std::exception& e) {
                        Report(&counters, true, "wal_unreadable", file.path(), e.what());
                    }
                } else if (ext == ".rollback") {
                    // Conditional intents live in `.chunkdb.intents/`;
                    // startup recovery no longer scans chunk directories
                    // for them, so an intent here would never be repaired.
                    ++counters.checked;
                    Report(
                        &counters,
                        true,
                        "conditional_intent_misplaced",
                        file.path(),
                        "expected under " +
                            chunkdb::ConditionalIntentDirectory(data_dir).string());
                } else {
                    Report(&counters, false, "unexpected_file", file.path(), "");
                }
            }
        }
        // Conditional-intent artifacts live in one dedicated shallow
        // directory; validate each record and surface pending recovery work.
        const auto intent_dir = chunkdb::ConditionalIntentDirectory(data_dir);
        std::error_code intent_dir_ec;
        if (std::filesystem::exists(intent_dir, intent_dir_ec) && !intent_dir_ec) {
            for (const auto& file : std::filesystem::directory_iterator(intent_dir)) {
                const auto file_name = file.path().filename().string();
                if (!file.is_regular_file()) {
                    Report(&counters, false, "unexpected_entry", file.path(), "");
                    continue;
                }
                if (IsTmpArtifactName(file_name)) {
                    Report(&counters, false, "tmp_artifact", file.path(), "");
                    continue;
                }
                if (file.path().extension() != ".rollback") {
                    Report(&counters, false, "unexpected_file", file.path(), "");
                    continue;
                }
                ++counters.checked;
                try {
                    const auto intent_bytes = chunkdb::LoadFile(file.path());
                    std::uint64_t committed_wal_size = 0;
                    chunkdb::ConditionalIntentState intent_state =
                        chunkdb::ConditionalIntentState::kRollback;
                    if (!chunkdb::TryParseConditionalIntent(
                            intent_bytes, &intent_state, &committed_wal_size)) {
                        Report(
                            &counters,
                            true,
                            "conditional_rollback_invalid",
                            file.path(),
                            "expected 16-byte CKRB-or-CKRC/u64/CRC32 record");
                        continue;
                    }
                    try {
                        (void)chunkdb::WalPathForConditionalIntent(
                            data_dir, file.path());
                    } catch (const std::exception& name_error) {
                        Report(
                            &counters,
                            true,
                            "conditional_rollback_invalid",
                            file.path(),
                            name_error.what());
                        continue;
                    }
                    Report(
                        &counters,
                        false,
                        intent_state ==
                                chunkdb::ConditionalIntentState::kRollback
                            ? "conditional_rollback_pending"
                            : "conditional_commit_cleanup_pending",
                        file.path(),
                        intent_state ==
                                chunkdb::ConditionalIntentState::kRollback
                            ? "startup will restore WAL boundary=" +
                                  std::to_string(committed_wal_size)
                            : "startup will preserve committed WAL and "
                              "remove marker; prior boundary=" +
                                  std::to_string(committed_wal_size));
                } catch (const std::exception& e) {
                    Report(
                        &counters,
                        true,
                        "conditional_rollback_unreadable",
                        file.path(),
                        e.what());
                }
            }
        }

        if (!version_present && !version_exists_ec && initialized_marker_present) {
            Report(
                &counters,
                true,
                "version_clock_missing",
                version_path,
                "initialized store cannot safely issue deterministic chunk versions");
        } else if (
            !version_present && !version_exists_ec && store_geometry.has_value() &&
            !initialized_marker_present && has_storage_artifacts) {
            Report(
                &counters,
                false,
                "version_bookkeeping_missing",
                version_path,
                "chunk data without version-clock bookkeeping; read-write startup "
                "will start a new clock");
        }
    } catch (const std::exception& e) {
        std::cerr << "chunkdb_verify: fatal: " << e.what() << "\n";
        return 2;
    }

    std::cout << "SUMMARY checked=" << counters.checked << " warnings=" << counters.warnings
              << " errors=" << counters.errors << "\n";
    return (counters.warnings + counters.errors) > 0 ? 1 : 0;
}
