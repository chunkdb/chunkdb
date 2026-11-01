#include "chunk_coordinate_index.hpp"

#include <algorithm>
#include <array>
#include <fstream>
#include <set>
#include <stdexcept>

#include "checkpoint.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/crc32.hpp"
#include "snapshot_generation.hpp"
#include "store_manifest.hpp"
#include "wal_replay.hpp"

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace chunkdb {

namespace {
std::uint64_t FileBytes(std::ifstream& input, const std::filesystem::path& path) {
    input.seekg(0, std::ios::end);
    const auto bytes = input.tellg();
    if (bytes < 0) throw std::runtime_error("failed to measure coordinate artifact " + path.string());
    input.seekg(0);
    if (!input) throw std::runtime_error("failed to rewind coordinate artifact " + path.string());
    return static_cast<std::uint64_t>(bytes);
}
std::vector<std::uint8_t> Prefix(std::ifstream& input, std::size_t count) {
    std::vector<std::uint8_t> bytes(count);
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(count));
    if (input.gcount() != static_cast<std::streamsize>(count))
        throw std::runtime_error("coordinate artifact header is truncated");
    return bytes;
}
void RequirePlainPath(const std::filesystem::path& path, bool directory) {
    const auto status = std::filesystem::symlink_status(path);
    if (directory ? !std::filesystem::is_directory(status) : !std::filesystem::is_regular_file(status))
        throw std::runtime_error("coordinate artifact is not an owned plain path: " + path.string());
#ifdef _WIN32
    const auto attributes = GetFileAttributesW(path.c_str());
    if (attributes == INVALID_FILE_ATTRIBUTES || (attributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0U)
        throw std::runtime_error("coordinate artifact is a reparse point: " + path.string());
#endif
}
}

std::vector<ChunkCoord> ChunkStore::AdmitCoordinateArtifacts(ChunkCoordinateIndex& index) {
    std::set<ChunkCoord> coordinates;
    for (const auto& directory : std::filesystem::directory_iterator(data_dir_)) {
        const auto name = directory.path().filename().string();
        if (!name.starts_with("L_")) continue;
        LargeChunkCoord large;
        if (!ParseLargeChunkName(name, geometry_.dims(), &large))
            throw std::runtime_error("coordinate directory has noncanonical dimensions or spelling: " + name);
        RequirePlainPath(directory.path(), true);
        for (const auto& file : std::filesystem::directory_iterator(directory.path())) {
            const auto path = file.path();
            const auto file_name = path.filename().string();
            if (file_name == kChunkIndexFileName) {
                RequirePlainPath(path, false);
                index.AdmitUnlistedLog(large);
                continue;
            }
            if (path.extension() != ".wal" && path.extension() != ".chk") continue;
            ChunkCoord coord;
            if (!ParseChunkFilePath(path.lexically_relative(data_dir_), geometry_, &coord))
                throw std::runtime_error("coordinate artifact has noncanonical placement: " + path.string());
            RequirePlainPath(path, false);
            std::ifstream input(path, std::ios::binary);
            if (!input) {
                if (access_mode_ == AccessMode::kReadOnly && !std::filesystem::exists(path)) continue;
                throw std::runtime_error("failed to admit coordinate artifact " + path.string());
            }
            const auto size = FileBytes(input, path);
            if (path.extension() == ".wal") {
                const auto header_size = ChunkWalHeaderBytes(geometry_.dims());
                const auto header = Prefix(input, static_cast<std::size_t>(std::min<std::uint64_t>(size, header_size)));
                if (header.size() >= 10U && ReadLe16(header, 8U) == 1U) throw EarlierDevelopmentFormatError();
                const bool complete = header.size() == header_size &&
                    ReadLe32(header, header_size - 4U) == Crc32(header.data(), header_size - 4U);
                if (!complete && IsInterruptedWalCreation(header, coord, store_id_, geometry_.dims())) {
                    std::array<char, 65536> suffix;
                    while (input) {
                        input.read(suffix.data(), static_cast<std::streamsize>(suffix.size()));
                        if (std::any_of(suffix.begin(), suffix.begin() + input.gcount(), [](char byte) { return byte != 0; }))
                            throw std::runtime_error("interrupted WAL creation has a nonzero suffix");
                    }
                    if (!input.eof()) throw std::runtime_error("failed to read interrupted WAL creation");
                } else {
                    ValidateWalHeader(header, coord, store_id_, features_, geometry_.dims());
                }
            } else {
                if (size < 12U) throw std::runtime_error("coordinate image header is truncated");
                const auto first = Prefix(input, 12U);
                if (ReadLe16(first, 8U) == 1U) throw EarlierDevelopmentFormatError();
                const auto count = ReadLe16(first, 10U);
                if (count > kImageMaxSections) throw std::runtime_error("coordinate image has too many sections");
                const auto header_size = ChunkImageHeaderBytes(geometry_.dims(), count);
                input.seekg(0);
                const auto header = Prefix(input, header_size);
                (void)ParseChunkImageHeader(header, size,
                    {.dims = geometry_.dims(), .epoch = store_id_, .coord = coord, .admitted_features = features_},
                    {.max_file_bytes = kMaxChunkImageFileBytes, .max_section_stored_bytes = kMaxChunkImageFileBytes,
                        .max_section_raw_bytes = kMaxChunkImageRawBytes, .max_total_raw_bytes = kMaxChunkImageRawBytes});
            }
            coordinates.insert(coord);
        }
    }
    return {coordinates.begin(), coordinates.end()};
}

void ChunkStore::PrepareCoordinateIndex() {
    coordinate_index_ = std::make_shared<ChunkCoordinateIndex>(data_dir_, geometry_, store_id_,
        access_mode_ == AccessMode::kReadOnly);
    coordinate_startup_candidates_ = AdmitCoordinateArtifacts(*coordinate_index_);
}

void ChunkStore::InitializeCoordinateIndex() {
    if (access_mode_ == AccessMode::kReadOnly) {
        read_only_retained_index_ = coordinate_index_;
        std::vector<ChunkCoord>{}.swap(coordinate_startup_candidates_);
        return;
    }
    for (const auto& coord : coordinate_startup_candidates_) {
        auto loaded = LoadChunkPayload(coord, false, coordinate_index_);
        BringChunkColumnsToSchema(geometry_, loaded.schema_version, geometry_.chunk_schema().version, &loaded.chunk_columns);
        BringToCurrentSchema(geometry_, loaded.schema_version, loaded.presence_bitmap, &loaded.payload, &loaded.vars);
        coordinate_index_->ObserveRecovered(coord, ChunkState{.version = loaded.revision,
            .payload = std::move(loaded.payload), .presence_bitmap = std::move(loaded.presence_bitmap),
            .vars = std::move(loaded.vars), .written = loaded.revision != 0U,
            .commit_time_ms = loaded.commit_time_ms, .chunk_columns = std::move(loaded.chunk_columns)});
    }
    coordinate_index_->CompleteRecovery();
    if (const auto highest = coordinate_index_->maximum_recovered_revision(); highest != 0U) RaiseVersionClockAbove(highest);
    coordinate_index_complete_ = true;
    std::vector<ChunkCoord>{}.swap(coordinate_startup_candidates_);
}

std::shared_ptr<ChunkCoordinateIndex> ChunkStore::RetainedIndexForSnapshot() {
    const auto path = data_dir_ / kChunkIndexFileName;
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("read-only table is missing its coordinate inventory");
    const auto header = ReadIndexInventoryHeader(input, FileBytes(input, path), geometry_, store_id_);
    if (header.compaction_pending) throw std::runtime_error("coordinate compaction requires writer recovery");
    {
        std::lock_guard lock(coordinate_index_refresh_mutex_);
        if (read_only_retained_index_ && read_only_retained_index_->inventory_generation() == header.generation)
            return read_only_retained_index_;
    }
    auto candidate = std::make_shared<ChunkCoordinateIndex>(data_dir_, geometry_, store_id_, true);
    if (candidate->inventory_generation() != header.generation)
        throw std::runtime_error("coordinate inventory changed during snapshot observation");
    return candidate;
}

std::optional<ChunkIndexRecord> ChunkStore::RetainedEmptyBoundaryForArchive(const ChunkCoord& coord) {
    const auto boundary = coordinate_index_->EmptyBoundary(coord);
    if (boundary && boundary->kind == ChunkIndexKind::kDeleted) return boundary;
    return std::nullopt;
}

std::shared_ptr<ChunkCoordinateIndex> ChunkStore::CoordinateIndexForScan() {
    if (access_mode_ != AccessMode::kReadOnly) return coordinate_index_;
    std::lock_guard refresh(coordinate_index_refresh_mutex_);
    const auto before = ReadSnapshotGenerationForScan(snapshot_generation_path_, snapshot_generation_record_seen_);
    if ((before.second & 1U) != 0U) throw std::runtime_error("read-only coordinate scan is unstable during writer publication");
    if (coordinate_index_complete_ && coordinate_index_generation_ == before) return coordinate_index_;
    auto candidate = std::make_shared<ChunkCoordinateIndex>(data_dir_, geometry_, store_id_, true);
    const auto coordinates = AdmitCoordinateArtifacts(*candidate);
    for (const auto& coord : coordinates) {
        auto loaded = LoadChunkPayload(coord, false, candidate);
        BringChunkColumnsToSchema(geometry_, loaded.schema_version, geometry_.chunk_schema().version, &loaded.chunk_columns);
        BringToCurrentSchema(geometry_, loaded.schema_version, loaded.presence_bitmap, &loaded.payload, &loaded.vars);
        candidate->ObserveRecovered(coord, ChunkState{.version = loaded.revision,
            .payload = std::move(loaded.payload), .presence_bitmap = std::move(loaded.presence_bitmap),
            .vars = std::move(loaded.vars), .written = loaded.revision != 0U,
            .commit_time_ms = loaded.commit_time_ms, .chunk_columns = std::move(loaded.chunk_columns)});
    }
    const auto after = ReadSnapshotGenerationForScan(snapshot_generation_path_, snapshot_generation_record_seen_);
    if (before != after || (after.second & 1U) != 0U)
        throw std::runtime_error("read-only coordinate index changed during collection");
    coordinate_index_ = candidate;
    read_only_retained_index_ = candidate;
    coordinate_index_generation_ = before;
    coordinate_index_complete_ = true;
    return candidate;
}

}  // namespace chunkdb
