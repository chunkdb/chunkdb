#include "history_store.hpp"

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <string>
#include <system_error>

#include "checkpoint.hpp"
#include "chunkdb/file_layout.hpp"
#include "wal_replay.hpp"

namespace chunkdb::history {

namespace {

constexpr std::string_view kSegmentSuffix = ".hseg";
constexpr std::string_view kTempMarker = ".hseg.tmp.";

[[nodiscard]] std::string ChunkFilePrefix(const ChunkCoord& chunk) {
    return "C_" + std::to_string(chunk.x) + "_" + std::to_string(chunk.y) + ".";
}

[[nodiscard]] bool AllZero(const std::vector<std::uint8_t>& bytes, std::size_t from) noexcept {
    return std::all_of(bytes.begin() + static_cast<std::ptrdiff_t>(from), bytes.end(), [](std::uint8_t byte) {
        return byte == 0U;
    });
}

[[nodiscard]] bool AnyPresent(const Geometry& geometry, const ChunkState& state) noexcept {
    return std::any_of(
        state.state.begin() + static_cast<std::ptrdiff_t>(geometry.ChunkPayloadBytes()), state.state.end(),
        [](std::uint8_t byte) { return byte != 0U; });
}

[[noreturn]] void Damaged(const std::filesystem::path& path, const std::string& problem) {
    throw HistoryDamagedError("history of " + path.string() + " is damaged: " + problem);
}

// The records EncodeRecords wrote, at `offset` in their segment.
void IndexRecords(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& records,
    std::size_t offset,
    std::vector<RecordRef>* out) {
    for (std::size_t at = 0; at < records.size();) {
        const auto record = ReadRecord(geometry, records.data() + at, records.size() - at, false);
        if (record.status != RecordStatus::kOk) {
            throw std::logic_error("history records just encoded do not read back");
        }
        out->push_back(RecordRef{.offset = offset + at, .summary = record.summary});
        at += record.summary.size;
    }
}

void CrashAtFailpoint(const char* name) {
    if (ConsumeFailpointEnv(name)) {
        std::_Exit(86);
    }
}

// Collects the mutations replay applies above a revision.
class Collector final : public WalReplayObserver {
  public:
    Collector(const Geometry& geometry, std::uint64_t history_start, std::uint64_t after_revision, Derivation* out)
        : differ_(geometry), history_start_(history_start), after_revision_(after_revision), out_(out) {}

    void BeforeFrame(
        const WalFrameInfo& frame,
        const std::vector<std::uint8_t>& state,
        const ChunkExtra& extra) override {
        recording_ = frame.revision >= history_start_ && frame.revision > after_revision_;
        if (!recording_) {
            return;
        }
        if (out_->mutations.empty()) {
            out_->base = ChunkState{.state = state, .extra = extra};
            out_->base_revision = revision_;
            out_->base_time_ms = time_ms_;
        }
        differ_.Capture(state, extra, *frame.touched_blocks);
    }

    void AfterFrame(
        const WalFrameInfo& frame,
        const std::vector<std::uint8_t>& state,
        const ChunkExtra& extra) override {
        if (recording_) {
            auto changes = differ_.Changes(state, extra);
            if (!changes.empty()) {
                out_->mutations.push_back(Mutation{
                    .revision = frame.revision,
                    .time_ms = frame.commit_time_ms,
                    .tag = std::vector<std::uint8_t>(frame.tag.begin(), frame.tag.end()),
                    .changes = std::move(changes),
                });
            }
        }
        revision_ = frame.revision;
        time_ms_ = frame.commit_time_ms;
    }

    void Start(std::uint64_t revision, std::uint64_t time_ms) noexcept {
        revision_ = revision;
        time_ms_ = time_ms;
    }

  private:
    BlockDiffer differ_;
    std::uint64_t history_start_;
    std::uint64_t after_revision_;
    Derivation* out_;
    bool recording_ = false;
    std::uint64_t revision_ = 0;
    std::uint64_t time_ms_ = 0;
};

}  // namespace

std::uint64_t ChunkHistory::last_revision() const noexcept {
    return segments.empty() ? 0U : segments.back().last_revision;
}

std::uint64_t ChunkHistory::last_time_ms() const noexcept {
    return segments.empty() ? 0U : segments.back().last_time_ms;
}

std::uint64_t ChunkHistory::trimmed_before() const noexcept {
    return !segments.empty() && segments.front().cut ? segments.front().base_revision : 0U;
}

Derivation DeriveHistory(
    const Geometry& geometry,
    const ChunkCoord& chunk,
    const StoreId& store_id,
    const FeatureFlags& store_features,
    const std::vector<std::uint8_t>* image,
    const std::vector<std::uint8_t>* wal,
    std::uint64_t history_start,
    std::uint64_t after_revision,
    bool allow_crash_tail,
    std::uint64_t whole_bytes) {
    const std::filesystem::path path =
        "chunk (" + std::to_string(chunk.x) + "," + std::to_string(chunk.y) + ")";
    ChunkStateImage start{
        .payload = std::vector<std::uint8_t>(geometry.ChunkPayloadBytes(), 0U),
        .presence_bitmap = std::vector<std::uint8_t>(ChunkPresenceBitmapBytes(geometry), 0U),
    };
    if (image != nullptr) {
        try {
            start = ParseChunkImage(*image, geometry, chunk, store_id, store_features);
        } catch (const std::exception& e) {
            Damaged(path, std::string("its image cannot be read: ") + e.what());
        }
    }
    Derivation out;
    out.image_revision = start.revision;
    Collector collector(geometry, history_start, after_revision, &out);
    collector.Start(start.revision, start.commit_time_ms);
    if (wal != nullptr) {
        const auto result = ReplayWal(
            *wal, geometry, chunk, store_id, store_features, start.revision, &start.payload,
            &start.presence_bitmap, &start.extra, &collector);
        if (!result.replayable && !result.torn_creation) {
            Damaged(path, "its WAL cannot be replayed: " + result.stop_reason);
        }
        if (result.tail_truncated_or_corrupt &&
            !(allow_crash_tail && result.stopped_at_crash_tail && result.valid_end >= whole_bytes)) {
            Damaged(path, "its WAL stops before its end: " + result.stop_reason);
        }
        if (!result.extra_problem.empty()) {
            Damaged(path, "its WAL leaves invalid extra data: " + result.extra_problem);
        }
        if (result.applied_frames > 0U) {
            start.revision = result.revision;
            start.commit_time_ms = result.commit_time_ms;
        }
    }
    out.final_state = ChunkState{
        .state = BuildChunkStateBytes(geometry, start.payload, start.presence_bitmap),
        .extra = std::move(start.extra),
    };
    out.final_revision = start.revision;
    out.final_time_ms = start.commit_time_ms;
    if (out.mutations.empty()) {
        out.base = out.final_state;
        out.base_revision = out.final_revision;
        out.base_time_ms = out.final_time_ms;
    }
    return out;
}

HistoryFiles::HistoryFiles(
    std::filesystem::path data_dir,
    const Geometry& geometry,
    const StoreId& store_id,
    std::uint64_t history_start)
    : data_dir_(std::move(data_dir)),
      dir_(data_dir_ / std::string(kHistoryDirName)),
      geometry_(geometry),
      store_id_(store_id),
      history_start_(history_start) {}

std::filesystem::path HistoryFiles::ChunkDirectory(const ChunkCoord& chunk) const {
    return LargeChunkDirectory(dir_, geometry_.ChunkToLarge(chunk));
}

std::filesystem::path HistoryFiles::SegmentPath(const ChunkCoord& chunk, std::uint64_t base_revision) const {
    return ChunkDirectory(chunk) /
           (ChunkFilePrefix(chunk) + std::to_string(base_revision) + std::string(kSegmentSuffix));
}

ChunkHistory HistoryFiles::Load(const ChunkCoord& chunk, bool writable) const {
    ChunkHistory history;
    const auto dir = ChunkDirectory(chunk);
    std::error_code ec;
    if (!std::filesystem::exists(dir, ec)) {
        if (ec) {
            throw std::runtime_error("cannot inspect " + dir.string() + ": " + ec.message());
        }
        return history;
    }
    const std::string prefix = ChunkFilePrefix(chunk);
    std::vector<std::filesystem::path> temps;
    for (std::filesystem::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec)) {
        const std::string name = it->path().filename().string();
        if (name.rfind(prefix, 0) != 0) {
            continue;
        }
        const std::string rest = name.substr(prefix.size());
        if (rest.find(kTempMarker) != std::string::npos) {
            temps.push_back(it->path());
            continue;
        }
        if (rest.size() <= kSegmentSuffix.size() ||
            rest.compare(rest.size() - kSegmentSuffix.size(), kSegmentSuffix.size(), kSegmentSuffix) != 0) {
            continue;
        }
        const std::string number = rest.substr(0, rest.size() - kSegmentSuffix.size());
        std::uint64_t base = 0;
        if (!TryParseUint64(number, &base) || std::to_string(base) != number) {
            Damaged(it->path(), "the segment name does not hold a revision");
        }
        history.segments.push_back(SegmentInfo{.path = it->path(), .base_revision = base});
    }
    if (ec) {
        throw std::runtime_error("cannot list " + dir.string() + ": " + ec.message());
    }
    std::sort(history.segments.begin(), history.segments.end(), [](const SegmentInfo& lhs, const SegmentInfo& rhs) {
        return lhs.base_revision < rhs.base_revision;
    });

    // Headers and records; a torn tail can only end the newest segment.
    std::size_t torn_at = 0;
    for (std::size_t i = 0; i < history.segments.size(); ++i) {
        auto& segment = history.segments[i];
        const bool newest = i + 1 == history.segments.size();
        std::vector<std::uint8_t> bytes;
        try {
            bytes = LoadFile(segment.path);
        } catch (const std::exception& e) {
            throw std::runtime_error("cannot read " + segment.path.string() + ": " + e.what());
        }
        SegmentHeader header;
        try {
            header = ReadSegmentHeader(geometry_, bytes, store_id_, chunk, &segment.header_size);
        } catch (const std::exception& e) {
            Damaged(segment.path, e.what());
        }
        if (header.base_revision != segment.base_revision) {
            Damaged(segment.path, "its header starts at another revision than its name");
        }
        if (header.history_start != history_start_) {
            Damaged(segment.path, "it belongs to history that started at another revision");
        }
        segment.base_time_ms = header.base_time_ms;
        segment.keyframe = header.keyframe.has_value();
        segment.cut = header.cut;
        segment.first = header.first;
        std::uint64_t revision = segment.base_revision;
        std::uint64_t time_ms = segment.base_time_ms;
        std::size_t at = segment.header_size;
        while (at < bytes.size()) {
            const auto record = ReadRecord(geometry_, bytes.data() + at, bytes.size() - at, false);
            if (record.status != RecordStatus::kOk) {
                if (newest && (record.status == RecordStatus::kTruncated || AllZero(bytes, at))) {
                    torn_at = at;
                    break;
                }
                Damaged(segment.path, record.status == RecordStatus::kTruncated ? "a record is cut short"
                                                                                : record.problem);
            }
            if (record.summary.first_revision <= revision || record.summary.first_time_ms < time_ms) {
                Damaged(segment.path, "its records do not follow each other");
            }
            if (segment.first_revision == 0U) {
                segment.first_revision = record.summary.first_revision;
                segment.first_time_ms = record.summary.first_time_ms;
            }
            segment.records.push_back(RecordRef{.offset = at, .summary = record.summary});
            revision = record.summary.last_revision;
            time_ms = record.summary.last_time_ms;
            at += record.summary.size;
        }
        segment.size = at;
        segment.last_revision = revision;
        segment.last_time_ms = time_ms;
        if (segment.first_revision == 0U) {
            Damaged(segment.path, "it holds no record");
        }
    }

    // A trim publishes its cut before it removes the older segments.
    std::size_t cut = 0;
    for (std::size_t i = 0; i < history.segments.size(); ++i) {
        if (history.segments[i].cut) {
            cut = i;
        }
    }
    const std::vector<SegmentInfo> garbage(history.segments.begin(), history.segments.begin() + static_cast<std::ptrdiff_t>(cut));
    history.segments.erase(history.segments.begin(), history.segments.begin() + static_cast<std::ptrdiff_t>(cut));
    for (std::size_t i = 0; i < history.segments.size(); ++i) {
        const auto& segment = history.segments[i];
        if (i == 0) {
            if (!segment.first && !segment.cut) {
                Damaged(segment.path, "the chunk's history does not start with it");
            }
            continue;
        }
        const auto& previous = history.segments[i - 1];
        if (segment.first || segment.base_revision != previous.last_revision ||
            segment.base_time_ms != previous.last_time_ms) {
            Damaged(segment.path, "it does not start where the segment before it ends");
        }
    }
    for (const auto& segment : history.segments) {
        if (segment.keyframe) {
            history.bytes_since_keyframe = 0;
        }
        history.bytes_since_keyframe += segment.size - segment.header_size;
    }

    if (writable) {
        bool removed = false;
        for (const auto& temp : temps) {
            std::filesystem::remove(temp, ec);
            if (ec) {
                throw std::runtime_error("cannot remove " + temp.string() + ": " + ec.message());
            }
            removed = true;
        }
        for (const auto& segment : garbage) {
            std::filesystem::remove(segment.path, ec);
            if (ec) {
                throw std::runtime_error("cannot remove " + segment.path.string() + ": " + ec.message());
            }
            removed = true;
        }
        if (removed) {
            SyncDirectoryPath(dir);
        }
        if (torn_at != 0U) {
            const auto& newest = history.segments.back();
            std::filesystem::resize_file(newest.path, newest.size, ec);
            if (ec) {
                throw std::runtime_error("cannot trim " + newest.path.string() + ": " + ec.message());
            }
            SyncFilePath(newest.path);
        }
    }
    return history;
}

void HistoryFiles::Append(
    const ChunkCoord& chunk,
    ChunkHistory* history,
    std::span<const Mutation> mutations,
    const ChunkState& base,
    std::uint64_t base_revision,
    std::uint64_t base_time_ms) const {
    if (mutations.empty()) {
        return;
    }
    if (mutations.front().revision <= history->last_revision()) {
        throw std::logic_error("history append does not follow the chunk's history");
    }
    std::vector<std::uint8_t> records;
    EncodeRecords(geometry_, mutations, &records);

    if (history->segments.empty() ||
        history->segments.back().size - history->segments.back().header_size >= kSegmentTargetRecordBytes) {
        const bool first = history->segments.empty();
        SegmentHeader header{
            .store_id = store_id_,
            .chunk = chunk,
            .history_start = history_start_,
            .base_revision = first ? base_revision : history->last_revision(),
            .base_time_ms = first ? base_time_ms : history->last_time_ms(),
            .first = first,
        };
        std::vector<std::uint8_t> bytes;
        if (first) {
            if (AnyPresent(geometry_, base)) {
                header.keyframe = base;
            }
            bytes = EncodeSegmentHeader(geometry_, header);
        } else {
            header.keyframe = base;
            bytes = EncodeSegmentHeader(geometry_, header);
            if (history->bytes_since_keyframe < kKeyframeRatio * (bytes.size() - kSegmentHeaderSize)) {
                header.keyframe.reset();
                bytes = EncodeSegmentHeader(geometry_, header);
            }
        }
        const std::size_t header_size = bytes.size();
        bytes.insert(bytes.end(), records.begin(), records.end());
        const auto path = SegmentPath(chunk, header.base_revision);
        if (!PublishNewFile(
                path, bytes, "CHUNKDB_FAILPOINT_CRASH_HISTORY_SEGMENT_BEFORE_PUBLISH_ONCE",
                "CHUNKDB_FAILPOINT_CRASH_HISTORY_SEGMENT_AFTER_PUBLISH_ONCE")) {
            throw HistoryDamagedError(
                "history segment " + path.string() + " exists but is not part of the chunk's history");
        }
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_HISTORY_AFTER_SEGMENT_PUBLISH_FAIL_ONCE")) {
            throw std::runtime_error("injected failure after publishing history segment " + path.string());
        }
        std::vector<RecordRef> refs;
        IndexRecords(geometry_, records, header_size, &refs);
        history->segments.push_back(SegmentInfo{
            .path = path,
            .base_revision = header.base_revision,
            .base_time_ms = header.base_time_ms,
            .keyframe = header.keyframe.has_value(),
            .first = first,
            .header_size = header_size,
            .size = bytes.size(),
            .first_revision = mutations.front().revision,
            .last_revision = mutations.back().revision,
            .first_time_ms = mutations.front().time_ms,
            .last_time_ms = mutations.back().time_ms,
            .records = std::move(refs),
        });
        history->bytes_since_keyframe =
            (header.keyframe.has_value() ? 0U : history->bytes_since_keyframe) + records.size();
        return;
    }

    auto& segment = history->segments.back();
    std::error_code ec;
    // A failed append makes the caller load the history again, which trims
    // what it left; any other difference was not written by this store.
    const auto file_size = std::filesystem::file_size(segment.path, ec);
    if (ec) {
        throw std::runtime_error("cannot inspect " + segment.path.string() + ": " + ec.message());
    }
    if (file_size != segment.size) {
        Damaged(segment.path, "it changed outside this store");
    }
    {
        std::ofstream out(segment.path, std::ios::binary | std::ios::app);
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_HISTORY_APPEND_FAIL_ONCE")) {
            out.write(reinterpret_cast<const char*>(records.data()), static_cast<std::streamsize>(records.size() / 2U));
            out.flush();
            throw std::runtime_error("injected history append failure: " + segment.path.string());
        }
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_CRASH_HISTORY_PARTIAL_APPEND_ONCE")) {
            out.write(reinterpret_cast<const char*>(records.data()), static_cast<std::streamsize>(records.size() / 2U));
            out.flush();
            std::_Exit(86);
        }
        out.write(reinterpret_cast<const char*>(records.data()), static_cast<std::streamsize>(records.size()));
        out.flush();
        if (!out.good()) {
            throw std::runtime_error("cannot append to " + segment.path.string());
        }
    }
    SyncFilePath(segment.path);
    CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_HISTORY_AFTER_APPEND_ONCE");
    IndexRecords(geometry_, records, segment.size, &segment.records);
    segment.size += records.size();
    segment.last_revision = mutations.back().revision;
    segment.last_time_ms = mutations.back().time_ms;
    history->bytes_since_keyframe += records.size();
}

HistoryFiles::SegmentContents HistoryFiles::ReadSegment(const ChunkCoord& chunk, const SegmentInfo& segment) const {
    SegmentContents contents;
    try {
        contents.bytes = LoadFile(segment.path);
    } catch (const std::exception& e) {
        throw HistoryDamagedError("cannot read " + segment.path.string() + ": " + e.what());
    }
    if (contents.bytes.size() < segment.size) {
        Damaged(segment.path, "it is shorter than its records");
    }
    contents.bytes.resize(segment.size);
    std::size_t header_size = 0;
    try {
        contents.header = ReadSegmentHeader(geometry_, contents.bytes, store_id_, chunk, &header_size);
    } catch (const std::exception& e) {
        Damaged(segment.path, e.what());
    }
    if (header_size != segment.header_size) {
        Damaged(segment.path, "its header changed");
    }
    return contents;
}

}  // namespace chunkdb::history
