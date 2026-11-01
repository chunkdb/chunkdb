// Offline, read-only integrity verification of a chunkdb data directory, as
// chunkdb_verify runs it (see verify.hpp for the output format).

#include "verify.hpp"
#include "backup.hpp"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <map>
#include <ostream>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/geometry.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feature_flags.hpp"
#include "feature_activation_records.hpp"
#include "feed_archive.hpp"
#include "feed_slot_records.hpp"
#include "migrations_records.hpp"
#include "store_manifest.hpp"
#include "txn_history.hpp"
#include "wal_replay.hpp"
#include "populated_index_format.hpp"
#include "chunk_coordinate_index.hpp"
#include "archive_base_format.hpp"
#include "txn_intent_format.hpp"
#include "shared_file_input.hpp"

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace {

// The public counters plus the stream findings go to.
struct VerifyCounters : chunkdb::VerifyCounters {
    std::ostream* out = nullptr;
    bool features = false;
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
    std::ostream& out = *counters->out;
    out << "VERIFY " << (is_error ? "error" : "warning") << " " << code << " "
        << QuoteToken(path.string());
    if (!detail.empty()) {
        out << " " << QuoteToken(detail);
    }
    out << "\n";
}

void ReportInfo(
    VerifyCounters* counters,
    const std::string& code,
    const std::filesystem::path& path,
    const std::string& detail) {
    std::ostream& out = *counters->out;
    out << "VERIFY info " << code << " " << QuoteToken(path.string());
    if (!detail.empty()) {
        out << " " << QuoteToken(detail);
    }
    out << "\n";
}

void ReportFeatures(VerifyCounters* counters, const std::filesystem::path& path, const chunkdb::FeatureFlags& flags) {
    if (!counters->features) return;
    std::string names;
    for (const auto& feature : chunkdb::kFeatureRegistry) {
        if ((flags.incompat & feature.incompat) == 0U) continue;
        if (!names.empty()) names += ',';
        names += feature.name;
    }
    ReportInfo(counters, "features", path, chunkdb::DescribeFeatures(flags) + " enabled=[" + names + "]");
}

[[nodiscard]] bool IsTmpArtifactName(const std::string& name) {
    return name.find(".tmp.") != std::string::npos;
}

struct CheckedIndex {
    struct Artifact { std::uint64_t revision, time; bool populated; };
    chunkdb::IndexInventoryHeader inventory;
    std::map<chunkdb::ChunkCoord, chunkdb::ChunkIndexRecord> retained;
    std::map<chunkdb::ChunkCoord, chunkdb::ChunkIndexRecord> unpublished;
    std::map<chunkdb::ChunkCoord, Artifact> artifacts;
    std::map<chunkdb::ChunkCoord, std::uint64_t> wal_cuts;
};

void RequirePlain(const std::filesystem::path& path, bool directory = false) {
    std::error_code error;
    const auto status = std::filesystem::symlink_status(path, error);
    if (error || (directory ? !std::filesystem::is_directory(status) : !std::filesystem::is_regular_file(status)))
        throw std::runtime_error("missing or non-plain checked path " + path.string());
#ifdef _WIN32
    const auto attributes = GetFileAttributesW(path.c_str());
    if (attributes == INVALID_FILE_ATTRIBUTES || (attributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0U)
        throw std::runtime_error("checked path is a reparse point " + path.string());
#endif
}

std::vector<std::uint8_t> ReadSmallRecord(const std::filesystem::path& path, std::size_t limit) {
    RequirePlain(path);
    chunkdb::SharedFileInput input(path);
    if (!input) throw std::runtime_error("cannot open checked record " + path.string());
    std::vector<std::uint8_t> bytes(limit + 1U);
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    const auto count = input.gcount();
    if (input.bad() || count > static_cast<std::streamsize>(limit))
        throw std::runtime_error("checked record exceeds its format bound " + path.string());
    bytes.resize(static_cast<std::size_t>(count));
    return bytes;
}

std::uint64_t Measure(std::istream& input, const std::filesystem::path& path) {
    input.seekg(0, std::ios::end);
    const auto size = input.tellg();
    if (size < 0) throw std::runtime_error("cannot measure " + path.string());
    input.seekg(0);
    if (!input) throw std::runtime_error("cannot rewind " + path.string());
    return static_cast<std::uint64_t>(size);
}

bool MatchesReplacement(const std::filesystem::path& path, const chunkdb::IndexInventoryEntry& entry) {
    std::error_code error;
    const auto status = std::filesystem::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory || (!error && !std::filesystem::exists(status))) return false;
    if (error || !std::filesystem::is_regular_file(status))
        throw std::runtime_error("invalid compaction replacement " + path.string());
    chunkdb::SharedFileInput input(path);
    if (!input) throw std::runtime_error("cannot open " + path.string());
    const auto bytes = Measure(input, path);
    return bytes == entry.replacement_bytes && chunkdb::IndexFileDigest(input, bytes) == entry.replacement_sha256;
}

CheckedIndex VerifyIndex(const std::filesystem::path& root, const chunkdb::Geometry& geometry,
    const chunkdb::StoreManifest& manifest, VerifyCounters* counters) {
    CheckedIndex result;
    std::map<chunkdb::LargeChunkCoord, chunkdb::IndexInventoryEntry> entries;
    const auto path = root / chunkdb::kChunkIndexFileName;
    ++counters->checked;
    RequirePlain(path);
    chunkdb::SharedFileInput inventory(path);
    if (!inventory) throw std::runtime_error("missing or unreadable coordinate inventory");
    result.inventory = chunkdb::ReadIndexInventory(inventory, Measure(inventory, path), geometry,
        manifest.store_id, [&](const auto& entry) { entries.emplace(entry.coord, entry); });
    const auto read_log = [&](const std::filesystem::path& log_path, const chunkdb::LargeChunkCoord& large,
        std::optional<std::uint64_t> cut, bool listed) {
        ++counters->checked;
        RequirePlain(log_path);
        chunkdb::SharedFileInput input(log_path);
        if (!input) throw std::runtime_error("missing or unreadable coordinate log " + log_path.string());
        const auto size = Measure(input, log_path);
        const auto checked = chunkdb::ReadChunkIndexLog(input, size, geometry, manifest.store_id, large, cut);
        if (size > checked.valid_bytes)
            Report(counters, false, "index_log_unpublished_suffix", log_path,
                "published_bytes=" + std::to_string(checked.valid_bytes));
        for (const auto& [coord, record] : checked.latest) {
            (void)coord;
            (void)geometry.LayoutAt(record.schema_version);
        }
        for (const auto& [coord, boundary] : checked.empty_boundaries) {
            if (boundary.kind == chunkdb::ChunkIndexKind::kForget &&
                (!listed || boundary.revision > result.inventory.retained_deletion_floor))
                throw std::runtime_error("coordinate forget record has no matching inventory floor");
            (listed ? result.retained : result.unpublished).emplace(coord, boundary);
        }
    };
    for (const auto& [large, entry] : entries) {
        auto selected = chunkdb::LargeChunkDirectory(root, large, geometry.dims()) / chunkdb::kChunkIndexFileName;
        RequirePlain(selected.parent_path(), true);
        auto cut = entry.committed_bytes;
        if (result.inventory.compaction_pending) {
            if (!MatchesReplacement(selected, entry)) {
                selected += ".compact";
                if (!MatchesReplacement(selected, entry))
                    throw std::runtime_error("missing or corrupt decided compaction replacement " + selected.string());
            }
            cut = entry.replacement_bytes;
        }
        read_log(selected, large, cut, true);
    }
    for (const auto& directory : std::filesystem::directory_iterator(root)) {
        chunkdb::LargeChunkCoord large{};
        if (!chunkdb::ParseLargeChunkName(directory.path().filename().string(), geometry.dims(), &large)) continue;
        RequirePlain(directory.path(), true);
        if (entries.contains(large)) continue;
        const auto log = directory.path() / chunkdb::kChunkIndexFileName;
        if (!std::filesystem::exists(log)) continue;
        read_log(log, large, {}, false);
        Report(counters, false, "index_log_unlisted", log, "requires writer admission before authorizing collection");
    }
    if (result.inventory.compaction_pending)
        Report(counters, false, "index_compaction_pending", path, "requires writer recovery; no files were changed");
    return result;
}

std::map<chunkdb::ChunkCoord, std::uint64_t> VisibilityCuts(const std::filesystem::path& root,
    const chunkdb::Geometry& geometry, const chunkdb::StoreManifest& manifest) {
    std::map<chunkdb::ChunkCoord, std::uint64_t> cuts;
    const auto directory = chunkdb::ConditionalIntentDirectory(root);
    if (!std::filesystem::exists(directory)) return cuts;
    const auto admit_cut = [&](const chunkdb::ChunkCoord& coord, std::uint64_t cut) {
        const auto path = chunkdb::ChunkWalPath(root, geometry, coord);
        if (cut != 0U) {
            RequirePlain(path);
            if (std::filesystem::file_size(path) < cut) throw std::runtime_error("rollback WAL is shorter than its committed cut");
        }
        const auto [at, inserted] = cuts.emplace(coord, cut);
        if (!inserted) at->second = std::min(at->second, cut);
    };
    for (const auto& file : std::filesystem::directory_iterator(directory)) {
        const auto name = file.path().filename().string();
        if (IsTmpArtifactName(name) || file.path().extension() != ".rollback") continue;
        RequirePlain(file.path());
        const bool transaction = chunkdb::IsTxnIntentArtifactName(name);
        const auto limit = transaction ? 40U + chunkdb::kMaxTxnWrittenChunks * (8U * geometry.dims() + 8U) : 16U;
        const auto bytes = ReadSmallRecord(file.path(), limit);
        if (transaction) {
            chunkdb::TxnIntent intent;
            if (!chunkdb::IsTxnIntentFileName(name) || !chunkdb::TryParseTxnIntent(bytes, &intent, geometry.dims(), manifest.store_id) ||
                file.path().filename() != chunkdb::TxnIntentPath(root, intent.version).filename())
                throw std::runtime_error("malformed transaction visibility record");
            if (intent.state == chunkdb::TxnIntentState::kRollback)
                for (const auto& member : intent.entries) admit_cut(member.coord, member.wal_boundary);
        } else {
            chunkdb::ConditionalIntentState state;
            std::uint64_t cut;
            chunkdb::ChunkCoord coord;
            if (!chunkdb::TryParseConditionalIntent(bytes, &state, &cut) ||
                !chunkdb::ParseConditionalIntentFileName(name, geometry, &coord))
                throw std::runtime_error("malformed conditional visibility record");
            if (state == chunkdb::ConditionalIntentState::kRollback) admit_cut(coord, cut);
        }
    }
    return cuts;
}

void ApplyBoundary(const chunkdb::Geometry& geometry, const chunkdb::ChunkIndexRecord& boundary,
    std::uint64_t& revision, std::uint64_t& time, std::uint64_t& schema,
    std::vector<std::uint8_t>& payload, std::vector<std::uint8_t>& presence,
    chunkdb::ChunkVars& vars, std::shared_ptr<const chunkdb::ChunkColumnState>& columns) {
    if (revision == boundary.revision && revision != 0U && time != boundary.commit_time_ms)
        throw std::runtime_error("retained boundary commit time differs from equal-version artifact");
    (void)chunkdb::ApplyRetainedEmptyBase(geometry, boundary, revision, time, schema,
        payload, presence, vars, columns);
}


void VerifyChunkArtifacts(const std::filesystem::path& root, const chunkdb::Geometry& geometry,
    const chunkdb::StoreManifest& manifest, const chunkdb::ChunkCoord& coord,
    CheckedIndex* index, VerifyCounters* counters) {
    const auto image_path = chunkdb::ChunkDataPath(root, geometry, coord);
    const auto wal_path = chunkdb::ChunkWalPath(root, geometry, coord);
    std::vector<std::uint8_t> payload(geometry.ChunkPayloadBytes(), 0U);
    std::vector<std::uint8_t> presence(chunkdb::ChunkPresenceBitmapBytes(geometry), 0U);
    chunkdb::ChunkVars vars;
    std::shared_ptr<const chunkdb::ChunkColumnState> columns;
    std::uint64_t revision = 0U, time = 0U, schema = 0U;
    if (std::filesystem::exists(image_path)) {
        ++counters->checked;
        try {
            auto image = chunkdb::ParseChunkImage(chunkdb::LoadChunkImageFile(image_path, geometry,
                coord, manifest.store_id, manifest.features), geometry, coord, manifest.store_id, manifest.features);
            payload = std::move(image.payload); presence = std::move(image.presence_bitmap);
            vars = std::move(image.vars); columns = std::move(image.chunk_columns);
            revision = image.revision; time = image.commit_time_ms; schema = image.schema_version;
        } catch (const std::exception& error) {
            Report(counters, true, "chunk_image_invalid", image_path, error.what());
            return; // An invalid image grants no empty replay base.
        }
    }
    if (index == nullptr) return; // Retained-base admission failed independently.
    try {
        if (const auto boundary = index->retained.find(coord); boundary != index->retained.end())
            ApplyBoundary(geometry, boundary->second, revision, time, schema, payload, presence, vars, columns);
        if (std::filesystem::exists(wal_path)) {
            ++counters->checked;
            const auto cut = index->wal_cuts.find(coord);
            std::vector<std::uint8_t> wal;
            if (cut != index->wal_cuts.end()) {
                if (cut->second > wal.max_size() || cut->second > static_cast<std::uint64_t>(std::numeric_limits<std::streamsize>::max()))
                    throw std::runtime_error("rollback WAL cut exceeds addressable bytes");
                RequirePlain(wal_path);
                chunkdb::SharedFileInput input(wal_path);
                if (!input || cut->second > Measure(input, wal_path)) throw std::runtime_error("rollback WAL shortened before verification");
                wal.resize(static_cast<std::size_t>(cut->second));
                input.read(reinterpret_cast<char*>(wal.data()), static_cast<std::streamsize>(wal.size()));
                if (input.gcount() != static_cast<std::streamsize>(wal.size()) || input.bad())
                    throw std::runtime_error("rollback WAL prefix read failed");
            } else wal = chunkdb::LoadFile(wal_path);
            if (!wal.empty() || cut == index->wal_cuts.end()) {
                const auto replay = chunkdb::ReplayWal(wal, geometry, coord, manifest.store_id, manifest.features,
                    revision, schema, &payload, &presence, &vars, nullptr, &columns);
                if (replay.torn_creation) {
                    Report(counters, cut != index->wal_cuts.end(), cut == index->wal_cuts.end() ? "wal_torn_creation" : "wal_damaged",
                        wal_path, "interrupted creation holds no mutation");
                    if (cut != index->wal_cuts.end()) return;
                } else if (!replay.replayable) {
                    Report(counters, true, "wal_not_replayable", wal_path, replay.stop_reason);
                    return;
                } else if (replay.tail_truncated_or_corrupt) {
                    Report(counters, !replay.stopped_at_crash_tail || cut != index->wal_cuts.end(),
                        replay.stopped_at_crash_tail && cut == index->wal_cuts.end() ? "wal_tail_truncated" : "wal_damaged", wal_path,
                        replay.stop_reason + " at byte " + std::to_string(replay.valid_end));
                    if (!replay.stopped_at_crash_tail || cut != index->wal_cuts.end()) return;
                }
                if (const auto boundary = index->retained.find(coord); boundary != index->retained.end()) {
                    for (auto at = chunkdb::ChunkWalHeaderBytes(geometry.dims()); at < replay.valid_end;) {
                        if (chunkdb::ReadLe64(wal, at + 4U) == boundary->second.revision &&
                            chunkdb::ReadLe64(wal, at + 12U) != boundary->second.commit_time_ms)
                            throw std::runtime_error("retained boundary commit time differs from equal-version WAL frame");
                        at += chunkdb::kWalFrameFixedHeaderSize + chunkdb::ReadLe16(wal, at + 22U) +
                            chunkdb::kWalFrameHeaderCrcSize + chunkdb::ReadLe32(wal, at + 28U) + chunkdb::kWalFrameTrailerSize;
                    }
                }
                if (!replay.vars_problem.empty()) {
                    Report(counters, true, "wal_vars_inconsistent", wal_path, replay.vars_problem);
                    return;
                }
                if (replay.applied_frames != 0U) { revision = replay.revision; time = replay.commit_time_ms; }
                schema = geometry.layout().schema().version;
            }
        }
        chunkdb::BringChunkColumnsToSchema(geometry, schema, geometry.chunk_schema().version, &columns);
        if (index->unpublished.contains(coord)) index->artifacts.emplace(coord, CheckedIndex::Artifact{
            revision, time, chunkdb::ChunkPopulated(geometry, presence, columns.get())});
    } catch (const std::exception& error) {
        Report(counters, true, "chunk_state_invalid", wal_path, error.what());
    }
}

template <typename T>
[[nodiscard]] bool ParseNumber(std::string_view text, T* value) {
    if (text.empty()) return false;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), *value);
    return error == std::errc{} && end == text.data() + text.size();
}

struct ArchiveBase {
    chunkdb::ArchiveBaseDescriptor descriptor;
    std::optional<chunkdb::ChunkStateImage> image;
};

ArchiveBase ReadArchiveBase(const std::filesystem::path& directory, const chunkdb::Geometry& geometry,
    const chunkdb::StoreManifest& manifest, chunkdb::ChunkCoord coord, std::uint64_t first) {
    const auto path = directory / chunkdb::ArchiveBaseFileName(coord, geometry.dims(), first);
    const auto size = chunkdb::ArchiveBaseDescriptorBytes(geometry.dims());
    const auto bytes = ReadSmallRecord(path, size);
    if (bytes.size() != size)
        throw std::runtime_error("archive base descriptor has wrong length");
    ArchiveBase result{.descriptor = chunkdb::ParseArchiveBaseDescriptor(bytes, geometry.dims(), manifest.store_id,
        coord, first, chunkdb::kMaxChunkImageFileBytes), .image = {}};
    (void)geometry.LayoutAt(result.descriptor.base_schema);
    const auto linked = directory / (chunkdb::ChunkFileStem(coord, geometry.dims()) + "." + std::to_string(first) + ".chk");
    if (result.descriptor.kind == chunkdb::ArchiveBaseKind::kImage) {
        const auto raw = chunkdb::LoadChunkImageFile(linked, geometry, coord, manifest.store_id, manifest.features);
        auto image = chunkdb::ParseChunkImage(raw, geometry, coord, manifest.store_id, manifest.features);
        chunkdb::ValidatePinnedArchiveBaseImage(result.descriptor, {.dims = geometry.dims(), .epoch = manifest.store_id,
            .coord = coord, .revision = image.revision, .commit_time_ms = image.commit_time_ms,
            .schema_version = image.schema_version}, raw, chunkdb::kMaxChunkImageFileBytes);
        result.image = std::move(image);
    } else if (std::filesystem::exists(linked)) {
        throw std::runtime_error("non-image archive base has an unexpected pinned image");
    }
    return result;
}

void VerifyFeedArtifacts(
    const std::filesystem::path& root,
    const chunkdb::Geometry& geometry,
    const chunkdb::StoreManifest& manifest,
    VerifyCounters* counters) {
    const bool enabled = (manifest.features.incompat & chunkdb::kFeatureFeedSlots) != 0U;
    const auto records_path = root / chunkdb::kFeedSlotsFileName;
    try {
        const auto records = chunkdb::ReadFeedSlotRecords(root, manifest.store_id);
        if (records) {
            ++counters->checked;
            if (!enabled) Report(counters, true, "feed_slots_without_feature", records_path, "feed slots feature is absent");
            if (records->filter_format && (manifest.features.incompat & chunkdb::kFeatureSlotFilters) == 0U)
                Report(counters, true, "slot_filters_without_feature", records_path, "slot filters feature is absent");
        }
    } catch (const std::exception& error) {
        ++counters->checked;
        Report(counters, true, "feed_slots_invalid", records_path, error.what());
    }
    const auto directory = root / chunkdb::kFeedArchiveDirName;
    if (!std::filesystem::exists(directory)) return;
    if (!enabled) Report(counters, true, "feed_archives_without_feature", directory, "feed slots feature is absent");
    if (!std::filesystem::is_directory(directory)) {
        Report(counters, true, "feed_archive_directory_invalid", directory, "expected a directory");
        return;
    }
    bool has_archive = false;
    for (const auto& file : std::filesystem::directory_iterator(directory)) {
        const auto name = file.path().filename().string();
        if (!file.is_regular_file()) {
            Report(counters, false, "unexpected_entry", file.path(), "");
            continue;
        }
        if (IsTmpArtifactName(name)) {
            Report(counters, false, "tmp_artifact", file.path(), "");
            continue;
        }
        ++counters->checked;
        chunkdb::ChunkCoord coord{};
        std::uint64_t first = 0U, last = 0U;
        const auto dot = name.find('.');
        const auto extension = file.path().extension().string();
        const auto range = dot == std::string::npos ? std::string_view{} :
            std::string_view(name).substr(dot + 1U, name.size() - dot - 1U - extension.size());
        const auto dash = range.find('-');
        const bool base = extension == ".chk";
        const bool descriptor = extension == ".base";
        if (dot == std::string::npos ||
            !chunkdb::ParseChunkFileStem(name.substr(0U, dot), geometry.dims(), &coord) ||
            ((base || descriptor) ? !ParseNumber(range, &first) :
                extension != ".wal" || dash == std::string_view::npos ||
                !ParseNumber(range.substr(0U, dash), &first) ||
                !ParseNumber(range.substr(dash + 1U), &last) || last < first) || first == 0U ||
            range != ((base || descriptor) ? std::to_string(first) : std::to_string(first) + "-" + std::to_string(last))) {
            Report(counters, true, "feed_archive_name_invalid", file.path(), "");
            continue;
        }
        try {
            if (descriptor) {
                (void)ReadArchiveBase(directory, geometry, manifest, coord, first);
                continue;
            }
            if (base) {
                const auto image = chunkdb::ParseChunkImage(chunkdb::LoadChunkImageFile(file.path(), geometry,
                    coord, manifest.store_id, manifest.features), geometry,
                    coord, manifest.store_id, manifest.features);
                if (image.revision >= first) throw std::runtime_error("base revision overlaps the archive WAL");
                const auto descriptor_path = directory / chunkdb::ArchiveBaseFileName(coord, geometry.dims(), first);
                if (std::filesystem::exists(descriptor_path)) {
                    (void)ReadArchiveBase(directory, geometry, manifest, coord, first);
                } else {
                    const auto current = chunkdb::ChunkDataPath(root, geometry, coord);
                    if (!std::filesystem::exists(current) ||
                        chunkdb::LoadChunkImageFile(current, geometry, coord, manifest.store_id, manifest.features) !=
                            chunkdb::LoadChunkImageFile(file.path(), geometry, coord, manifest.store_id, manifest.features))
                        throw std::runtime_error("staged archive image has no matching original live image");
                }
                // A base without an archive is valid after a crash before the
                // WAL rename, or during deletion of an acknowledged segment.
                continue;
            }
            has_archive = true;
            auto payload = std::vector<std::uint8_t>(geometry.ChunkPayloadBytes(), 0U);
            auto presence = std::vector<std::uint8_t>(chunkdb::ChunkPresenceBitmapBytes(geometry), 0U);
            chunkdb::ChunkVars vars;
            std::shared_ptr<const chunkdb::ChunkColumnState> chunk_columns;
            auto binding = ReadArchiveBase(directory, geometry, manifest, coord, first);
            const auto base_revision = binding.descriptor.base_revision;
            const auto base_schema = binding.descriptor.base_schema;
            if (binding.image) {
                auto image = std::move(*binding.image);
                payload = std::move(image.payload);
                presence = std::move(image.presence_bitmap);
                vars = std::move(image.vars);
                chunk_columns = std::move(image.chunk_columns);
            } else {
                payload.assign(geometry.LayoutAt(base_schema).payload_bytes(), 0U);
                if (base_revision != 0U) chunk_columns = chunkdb::DefaultChunkColumnsAt(geometry, base_schema);
            }
            const auto bytes = chunkdb::LoadFile(file.path());
            const auto replay = chunkdb::ReplayWal(bytes, geometry, coord, manifest.store_id,
                manifest.features, base_revision, base_schema, &payload, &presence, &vars, nullptr, &chunk_columns);
            if (!replay.replayable || replay.tail_truncated_or_corrupt || !replay.vars_problem.empty())
                throw std::runtime_error("archive WAL is damaged: " + replay.stop_reason + " " + replay.vars_problem);
            const auto header_size = chunkdb::ChunkWalHeaderBytes(geometry.dims());
            if (replay.applied_frames == 0U || bytes.size() < header_size + 12U ||
                chunkdb::ReadLe64(bytes, header_size + 4U) != first || replay.revision != last)
                throw std::runtime_error("archive revision range disagrees with its name");
        } catch (const std::exception& error) {
            Report(counters, true, base || descriptor ? "feed_archive_base_invalid" : "feed_archive_wal_invalid", file.path(), error.what());
        }
    }
    if (has_archive) {
        try {
            auto reader = chunkdb::FeedArchiveAccess::Create(root, geometry, manifest.store_id,
                {manifest.store_id, 0U}, std::numeric_limits<std::uint64_t>::max(), {}, manifest.features);
            while (reader.Next()) {}
        } catch (const std::exception& error) {
            Report(counters, true, "feed_archive_history_invalid", directory, error.what());
        }
    }
}

// Verifies one table directory (a store): its manifest, bookkeeping, chunk
// images, WALs and conditional intents.
void VerifyTable(const std::filesystem::path& data_dir, VerifyCounters* counters) {
    bool initialized_marker_present = false;
    bool has_storage_artifacts = false;

    // Chunk artifacts are interpreted with the geometry the store was
    // created with, which only the manifest records.
    const auto manifest_path = chunkdb::StoreManifestPath(data_dir);
    std::optional<chunkdb::Geometry> store_geometry;
    std::optional<chunkdb::StoreManifest> store_manifest;
    ++counters->checked;
    try {
        const auto manifest = chunkdb::ReadStoreManifest(data_dir);
        if (manifest.has_value()) {
            ReportFeatures(counters, manifest_path, manifest->features);
            if ((manifest->features.incompat & chunkdb::kFeatureActivation) != 0U ||
                (manifest->features.incompat & chunkdb::kFeatureMigrations) != 0U)
                Report(counters, true, "table_directory_feature", manifest_path, "directory-only feature on table");
            const auto unknown = chunkdb::UnknownFeatures(manifest->features);
            if (unknown.incompat != 0U) {
                Report(
                    counters,
                    true,
                    "manifest_unknown_features",
                    manifest_path,
                    "store uses features this build does not support (unknown " +
                        chunkdb::DescribeFeatures(unknown) +
                        "); chunk artifacts were not checked");
            } else {
                if (unknown.ro_compat != 0U || unknown.compat != 0U) {
                    Report(
                        counters,
                        false,
                        "manifest_unknown_features",
                        manifest_path,
                        "unknown " + chunkdb::DescribeFeatures(unknown) +
                            "; data owned by those features was not checked");
                }
                store_geometry.emplace(manifest->geometry, manifest->schema, *manifest->chunk_schema);
                store_manifest = manifest;
            }
        } else {
            Report(
                counters,
                true,
                "manifest_missing",
                manifest_path,
                "table without a manifest; its chunk artifacts were not checked");
        }
    } catch (const std::exception& e) {
        Report(
            counters,
            true,
            "manifest_invalid",
            manifest_path,
            std::string(e.what()) + "; chunk artifacts were not checked");
    }

    std::optional<CheckedIndex> index;
    if (store_geometry && store_manifest) {
        try { index = VerifyIndex(data_dir, *store_geometry, *store_manifest, counters); }
        catch (const std::exception& error) {
            Report(counters, true, "coordinate_index_invalid", data_dir / chunkdb::kChunkIndexFileName, error.what());
        }
        if (index) {
            try { index->wal_cuts = VisibilityCuts(data_dir, *store_geometry, *store_manifest); }
            catch (const std::exception& error) {
                Report(counters, true, "rollback_visibility_invalid", chunkdb::ConditionalIntentDirectory(data_dir), error.what());
                index.reset();
            }
        }
        VerifyFeedArtifacts(data_dir, *store_geometry, *store_manifest, counters);
    }

    const auto version_path = data_dir / "chunkdb.version";
    std::error_code version_exists_ec;
    const bool version_present =
        std::filesystem::exists(version_path, version_exists_ec);
    if (version_exists_ec) {
        Report(
            counters,
            true,
            "version_clock_uninspectable",
            version_path,
            version_exists_ec.message());
    } else if (version_present) {
        ++counters->checked;
        try {
            std::error_code size_ec;
            const auto size = std::filesystem::file_size(version_path, size_ec);
            if (size_ec || size != 16U) {
                Report(
                    counters,
                    true,
                    "version_clock_invalid",
                    version_path,
                    size_ec ? size_ec.message() : "expected exactly 16 bytes");
            } else {
                const auto bytes = chunkdb::LoadFile(version_path);
                std::uint64_t ceiling = 0;
                if (!chunkdb::TryParseVersionClockRecord(bytes, &ceiling)) {
                    Report(
                        counters,
                        true,
                        "version_clock_invalid",
                        version_path,
                        "expected checked CKVR/u64/CRC32 record");
                }
            }
        } catch (const std::exception& e) {
            Report(
                counters,
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
        ++counters->checked;
        try {
            std::uint64_t generation = 0;
            if (!chunkdb::TryParseSnapshotGenerationRecord(
                    chunkdb::LoadFile(snapshot_path),
                    &generation)) {
                Report(
                    counters,
                    true,
                    "snapshot_generation_invalid",
                    snapshot_path,
                    "expected 16-byte CKSG/u64/CRC32 record");
            } else if ((generation & 1U) != 0U) {
                // The fence is odd for the whole writer session, so every
                // crash leaves it odd: the next writer open runs recovery.
                // That is pending work, not damage; artifact checks below
                // still report any real damage as errors.
                Report(
                    counters,
                    false,
                    "snapshot_generation_recovery_required",
                    snapshot_path,
                    "odd generation=" +
                        std::to_string(generation) +
                        " names an open writer session or a crashed writer;"
                        " the next read-write open runs recovery");
            }
        } catch (const std::exception& e) {
            Report(
                counters,
                true,
                "snapshot_generation_unreadable",
                snapshot_path,
                e.what());
        }
    } else if (snapshot_exists_ec) {
        Report(
            counters,
            true,
            "snapshot_generation_uninspectable",
            snapshot_path,
            snapshot_exists_ec.message());
    }

    const auto initialized_path = data_dir / ".chunkdb.initialized";
    std::error_code initialized_exists_ec;
    if (std::filesystem::exists(initialized_path, initialized_exists_ec)) {
        initialized_marker_present = true;
        ++counters->checked;
        try {
            if (!chunkdb::IsValidInitializedStoreMarker(
                    chunkdb::LoadFile(initialized_path))) {
                Report(
                    counters,
                    true,
                    "initialized_marker_invalid",
                    initialized_path,
                    "expected 16-byte CKID/u64(1)/CRC32 record");
            }
        } catch (const std::exception& e) {
            Report(
                counters,
                true,
                "initialized_marker_unreadable",
                initialized_path,
                e.what());
        }
    } else if (initialized_exists_ec) {
        Report(
            counters,
            true,
            "store_marker_uninspectable",
            initialized_path,
            initialized_exists_ec.message());
    }

    std::set<chunkdb::ChunkCoord> checked_coords;
    for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
        const auto name = entry.path().filename().string();

        // Process-lock artifacts and OS metadata are not storage state.
        if (name.rfind(".chunkdb", 0) == 0 || name.rfind(".", 0) == 0) {
            continue;
        }

        if (entry.is_regular_file()) {
            if (IsTmpArtifactName(name)) {
                Report(counters, false, "tmp_artifact", entry.path(), "");
                continue;
            }
            if (name == chunkdb::kStoreManifestFileName || name.rfind("chunkdb.", 0) == 0) {
                continue;
            }
            const auto ext = entry.path().extension();
            if (ext == ".chk" || ext == ".wal" || ext == ".rgn" || ext == ".rollback") {
                // Chunk data outside any chunk directory is never read.
                Report(counters, name.starts_with("C_"), "unexpected_file", entry.path(), "");
            } else {
                ReportInfo(counters, "foreign_entry", entry.path(), "not chunkdb data; ignored");
            }
            continue;
        }

        if (!entry.is_directory()) {
            if (name.starts_with("L_")) Report(counters, true, "large_chunk_path_invalid", entry.path(), "expected a plain directory");
            continue;
        }

        if (!store_geometry) {
            has_storage_artifacts = has_storage_artifacts || name.starts_with("L_");
            continue;
        }
        chunkdb::LargeChunkCoord large{};
        if (!chunkdb::ParseLargeChunkName(name, store_geometry->dims(), &large)) {
            if (name.starts_with("L_")) {
                Report(counters, true, "large_chunk_name_invalid", entry.path(), "noncanonical or wrong-dimensional coordinate");
                continue;
            }
            ReportInfo(counters, "foreign_entry", entry.path(), "not chunkdb data; ignored");
            continue;
        }
        has_storage_artifacts = true;
        try { RequirePlain(entry.path(), true); }
        catch (const std::exception& error) {
            Report(counters, true, "large_chunk_path_invalid", entry.path(), error.what());
            continue;
        }
        const chunkdb::Geometry& geometry = *store_geometry;

        for (const auto& file : std::filesystem::directory_iterator(entry.path())) {
            const auto file_name = file.path().filename().string();
            if (!file.is_regular_file()) {
                Report(counters, file_name.starts_with("C_"), "unexpected_entry", file.path(), "");
                continue;
            }
            if (IsTmpArtifactName(file_name)) {
                Report(counters, false, "tmp_artifact", file.path(), "");
                continue;
            }
            const auto ext = file.path().extension();
            chunkdb::ChunkCoord coord{};
            const bool coord_ok =
                chunkdb::ParseChunkFileStem(file.path().stem().string(), geometry.dims(), &coord);

            if (ext == ".chk" || ext == ".wal") {
                try { RequirePlain(file.path()); }
                catch (const std::exception& error) {
                    Report(counters, true, "chunk_path_invalid", file.path(), error.what());
                    continue;
                }
                if (!coord_ok) {
                    ++counters->checked;
                    Report(counters, true, ext == ".chk" ? "chunk_name_invalid" : "wal_name_invalid", file.path(), "noncanonical or wrong-dimensional coordinate");
                    continue;
                }
                const auto expected = geometry.ChunkToLarge(coord);
                if (expected != large) {
                    ++counters->checked;
                    Report(counters, true, ext == ".chk" ? "chunk_misplaced" : "wal_misplaced", file.path(),
                        "expected_dir=" + chunkdb::LargeChunkDirectory({}, expected, geometry.dims()).string());
                    continue;
                }
                if (checked_coords.insert(coord).second)
                    VerifyChunkArtifacts(data_dir, geometry, *store_manifest, coord, index ? &*index : nullptr, counters);
            } else if (file_name == chunkdb::kChunkIndexFileName || file_name == std::string(chunkdb::kChunkIndexFileName) + ".compact") {
                if (file_name.ends_with(".compact") && index && !index->inventory.compaction_pending)
                    Report(counters, false, "index_compaction_staging_unpublished", file.path(), "no pending inventory decision");
            } else if (ext == ".rollback") {
                // Conditional intents live in `.chunkdb.intents/`;
                // startup recovery no longer scans chunk directories
                // for them, so an intent here would never be repaired.
                ++counters->checked;
                Report(
                    counters,
                    true,
                    "conditional_intent_misplaced",
                    file.path(),
                    "expected under " +
                        chunkdb::ConditionalIntentDirectory(data_dir).string());
            } else {
                Report(counters, false, "unexpected_file", file.path(), "");
            }
        }
    }
    if (index && store_geometry) for (const auto& [coord, boundary] : index->unpublished) {
        const auto artifact = index->artifacts.find(coord);
        bool valid = artifact != index->artifacts.end() && artifact->second.revision >= boundary.revision;
        if (valid && artifact->second.revision == boundary.revision) {
            valid = artifact->second.time == boundary.commit_time_ms && !artifact->second.populated;
        }
        if (!valid) Report(counters, true, "index_unpublished_boundary_without_artifact",
            chunkdb::LargeChunkDirectory(data_dir, store_geometry->ChunkToLarge(coord), store_geometry->dims()) /
                chunkdb::kChunkIndexFileName, "unlisted empty boundary has no matching committed artifact");
    }
    // Conditional-intent artifacts live in one dedicated shallow
    // directory; validate each record and surface pending recovery work.
    const auto intent_dir = chunkdb::ConditionalIntentDirectory(data_dir);
    std::error_code intent_dir_ec;
    if (std::filesystem::exists(intent_dir, intent_dir_ec) && !intent_dir_ec) {
        for (const auto& file : std::filesystem::directory_iterator(intent_dir)) {
            const auto file_name = file.path().filename().string();
            if (!file.is_regular_file()) {
                Report(counters, false, "unexpected_entry", file.path(), "");
                continue;
            }
            if (IsTmpArtifactName(file_name)) {
                Report(counters, false, "tmp_artifact", file.path(), "");
                continue;
            }
            if (file.path().extension() != ".rollback") {
                Report(counters, false, "unexpected_file", file.path(), "");
                continue;
            }
            if (chunkdb::IsTxnIntentArtifactName(file_name)) {
                // A checked transaction intent the next writer start
                // resolves.
                ++counters->checked;
                if (!store_manifest) {
                    Report(counters, true, "txn_intent_uncheckable", file.path(), "table manifest was not admitted");
                    continue;
                }
                try {
                    chunkdb::TxnIntent intent;
                    if (!chunkdb::IsTxnIntentFileName(file_name) ||
                        !chunkdb::TryParseTxnIntent(ReadSmallRecord(file.path(),
                            40U + chunkdb::kMaxTxnWrittenChunks * (8U * store_manifest->geometry.dims + 8U)), &intent,
                            store_manifest->geometry.dims, store_manifest->store_id) ||
                        file_name != chunkdb::TxnIntentPath(data_dir, intent.version).filename().string()) {
                        Report(
                            counters,
                            true,
                            "txn_intent_invalid",
                            file.path(),
                            "expected a checked transaction intent named txn-<version>.rollback");
                        continue;
                    }
                    Report(
                        counters,
                        false,
                        intent.state == chunkdb::TxnIntentState::kRollback ? "txn_rollback_pending"
                                                                           : "txn_commit_cleanup_pending",
                        file.path(),
                        intent.state == chunkdb::TxnIntentState::kRollback
                            ? "startup will truncate " + std::to_string(intent.entries.size()) +
                                  " WALs to their boundaries"
                            : "startup will keep the committed frames of " + std::to_string(intent.entries.size()) +
                                  " chunks and remove the marker");
                } catch (const std::exception& e) {
                    Report(counters, true, "txn_intent_unreadable", file.path(), e.what());
                }
                continue;
            }
            ++counters->checked;
            try {
                const auto intent_bytes = ReadSmallRecord(file.path(), 16U);
                std::uint64_t committed_wal_size = 0;
                chunkdb::ConditionalIntentState intent_state =
                    chunkdb::ConditionalIntentState::kRollback;
                if (!chunkdb::TryParseConditionalIntent(
                        intent_bytes, &intent_state, &committed_wal_size)) {
                    Report(
                        counters,
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
                        counters,
                        true,
                        "conditional_rollback_invalid",
                        file.path(),
                        name_error.what());
                    continue;
                }
                Report(
                    counters,
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
                    counters,
                    true,
                    "conditional_rollback_unreadable",
                    file.path(),
                    e.what());
            }
        }
    }

    if (!version_present && !version_exists_ec && initialized_marker_present) {
        Report(
            counters,
            true,
            "version_clock_missing",
            version_path,
            "initialized store cannot safely issue deterministic chunk versions");
    } else if (
        !version_present && !version_exists_ec && store_geometry.has_value() &&
        !initialized_marker_present && has_storage_artifacts) {
        Report(
            counters,
            false,
            "version_bookkeeping_missing",
            version_path,
            "chunk data without version-clock bookkeeping; read-write startup "
            "will start a new clock");
    }
}

// Verifies a data directory: its manifest, the leftovers of interrupted table
// operations, and every table under `tables/`.
void VerifyDataDirectoryImpl(const std::filesystem::path& data_dir, VerifyCounters* counters) {
    const auto manifest_path = chunkdb::DataDirManifestPath(data_dir);
    bool check_tables = false;
    ++counters->checked;
    try {
        const auto manifest = chunkdb::ReadDataDirManifest(data_dir);
        if (!manifest.has_value()) {
            Report(
                counters, true, "data_dir_manifest_missing", manifest_path,
                "not an initialized chunkdb data directory; tables were not checked");
        } else {
            ReportFeatures(counters, manifest_path, manifest->features);
            const auto unknown = chunkdb::UnknownFeatures(manifest->features);
            if (unknown.incompat != 0U) {
                Report(
                    counters, true, "data_dir_manifest_unknown_features", manifest_path,
                    "data directory uses features this build does not support (unknown " +
                        chunkdb::DescribeFeatures(unknown) + "); tables were not checked");
            } else {
                if (unknown.ro_compat != 0U || unknown.compat != 0U) {
                    Report(
                        counters, false, "data_dir_manifest_unknown_features", manifest_path,
                        "unknown " + chunkdb::DescribeFeatures(unknown) +
                            "; data owned by those features was not checked");
                }
                check_tables = true;
            }
        }
    } catch (const std::exception& e) {
        Report(
            counters, true, "data_dir_manifest_invalid", manifest_path,
            std::string(e.what()) + "; tables were not checked");
    }

    if (check_tables) {
        ++counters->checked;
        try {
            (void)chunkdb::ReadMigrationRecords(data_dir);
        } catch (const std::exception& error) {
            Report(counters, true, "migration_records_invalid", data_dir / chunkdb::kMigrationsFileName, error.what());
        }
        ++counters->checked;
        try {
            if (const auto journal = chunkdb::ReadMigrationJournal(data_dir)) {
                chunkdb::ValidateMigrationJournal(data_dir, *journal);
                Report(counters, false, "migration_recovery_pending", data_dir / chunkdb::kMigrationPendingFileName,
                       "migration '" + journal->record.name + "' requires a writer start to finish publication");
            }
        } catch (const std::exception& error) {
            Report(counters, true, "migration_pending_invalid", data_dir / chunkdb::kMigrationPendingFileName, error.what());
        }
    }

    std::uint32_t pending_feature = 0U;
    ++counters->checked;
    try {
        if (const auto journal = chunkdb::ReadFeatureActivationJournal(data_dir)) {
            chunkdb::ValidateFeatureActivationJournal(data_dir, *journal);
            pending_feature = journal->feature;
            Report(counters, false, "feature_recovery_pending", data_dir / chunkdb::kFeaturePendingFileName,
                "activation decision requires a read-write start to finish publication");
        }
    } catch (const std::exception& error) {
        Report(counters, true, "feature_pending_invalid", data_dir / chunkdb::kFeaturePendingFileName, error.what());
    }

    for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
        const auto name = entry.path().filename().string();
        if (name == chunkdb::kDataDirManifestFileName || name == chunkdb::kBackupMarkerName || name == "tables" ||
            name == chunkdb::kMigrationsFileName || name == chunkdb::kMigrationPendingFileName || name == chunkdb::kFeaturePendingFileName ||
            name.rfind(".chunkdb.lock", 0) == 0) {
            continue;
        }
        if (name == chunkdb::kUsersFileName) {
            ++counters->checked;
            try {
                if (!std::filesystem::is_regular_file(entry.symlink_status()))
                    throw std::runtime_error("users record is not a regular file");
                (void)chunkdb::DecodeUsers(chunkdb::LoadFile(entry.path()));
            } catch (const std::exception& error) {
                Report(counters, true, "users_invalid", entry.path(), error.what());
            }
            continue;
        }
        if (name == ".chunkdb.staging" || name == ".chunkdb.dropped" || name == chunkdb::kBackupStagingName) {
            const bool staging = name == ".chunkdb.staging";
            const bool backup = name == chunkdb::kBackupStagingName;
            for (const auto& leftover : std::filesystem::directory_iterator(entry.path())) {
                Report(
                    counters, false,
                    backup ? "interrupted_backup" : staging ? "interrupted_table_create" : "interrupted_table_drop",
                    leftover.path(),
                    backup ? "backup staging remains; the next backup or writer start removes recognized copies owned by this data directory; inspect unrecognized entries"
                            : staging ? "a table creation was interrupted; the next writer start removes it"
                            : "a table drop was interrupted; the next writer start removes it");
            }
            continue;
        }
        if (IsTmpArtifactName(name)) {
            Report(counters, false, "tmp_artifact", entry.path(), "");
            continue;
        }
        if (name.rfind(".", 0) == 0) {
            continue;  // OS metadata
        }
        if (chunkdb::IsStoreEntryName(name)) {
            // Store state belongs inside a table; chunkdb never reads it here.
            Report(counters, false, "unexpected_entry", entry.path(), "not inside a table");
            continue;
        }
        ReportInfo(counters, "foreign_entry", entry.path(), "not chunkdb data; ignored");
    }

    const auto tables_dir = data_dir / "tables";
    if (!check_tables || !std::filesystem::is_directory(tables_dir)) {
        return;
    }
    std::vector<std::filesystem::path> tables;
    for (const auto& entry : std::filesystem::directory_iterator(tables_dir)) {
        const auto name = entry.path().filename().string();
        if (!chunkdb::IsValidTableName(name) || !entry.is_directory()) {
            ReportInfo(counters, "foreign_entry", entry.path(), "not a table; ignored");
            continue;
        }
        tables.push_back(entry.path());
    }
    std::sort(tables.begin(), tables.end());
    for (const auto& table : tables) {
        VerifyTable(table, counters);
        if (check_tables) {
            try {
                const auto root = chunkdb::ReadDataDirManifest(data_dir);
                const auto manifest = chunkdb::ReadStoreManifest(table);
                if (root && manifest) {
                    const auto missing = chunkdb::TableScopedFeatures(root->features).incompat &
                        ~manifest->features.incompat & ~pending_feature;
                    if (missing != 0U) Report(counters, true, "feature_owner_missing", chunkdb::StoreManifestPath(table),
                        "directory enables missing table-owned bits " + chunkdb::DescribeFeatures({.incompat = missing}));
                }
            } catch (const std::exception& error) {
                Report(counters, true, "feature_ownership_invalid", table, error.what());
            }
        }
    }
}

}  // namespace


chunkdb::VerifyCounters chunkdb::VerifyDataDirectory(
    const std::filesystem::path& data_dir,
    std::ostream& out, bool features) {
    ::VerifyCounters counters;
    counters.out = &out;
    counters.features = features;
    const auto present = [](const auto& path) {
        return std::filesystem::symlink_status(path).type() != std::filesystem::file_type::not_found;
    };
    if (present(data_dir / kBackupIncompleteName) || present(data_dir / kRestoreIncompleteName)) {
        Report(&counters, true, "backup_incomplete", data_dir, "copy publication has not completed");
        return counters;
    }
    if (present(data_dir / kBackupMarkerName)) {
        ++counters.checked;
        try { ValidateBackupInventory(data_dir, ReadBackupRecord(data_dir)); }
        catch (const std::exception& error) {
            Report(&counters, true, "backup_invalid", data_dir / kBackupMarkerName, error.what());
            return counters;
        }
    }
    VerifyDataDirectoryImpl(data_dir, &counters);
    return counters;
}
