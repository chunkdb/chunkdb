#include "feed_slots.hpp"

#include <algorithm>
#include <limits>
#include <fstream>
#include <map>
#include <regex>
#include <cstring>

#include "change_feed.hpp"
#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feature_flags.hpp"
#include "feed_archive.hpp"
#include "wal_replay.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace chunkdb {
namespace {
struct Segment {
    std::filesystem::path wal;
    std::filesystem::path base;
    std::uint64_t last;
    std::uint64_t bytes;
    ChunkCoord coord;
    std::uint64_t first;
};
std::vector<Segment> Segments(const std::filesystem::path& root) {
    const auto directory = root / kFeedArchiveDirName;
    if (!std::filesystem::exists(directory)) return {};
    static const std::regex pattern(R"(^C_(-?[0-9]+)_(-?[0-9]+)\.([0-9]+)-([0-9]+)\.wal$)");
    std::vector<Segment> result;
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        std::smatch match;
        const auto name = entry.path().filename().string();
        if (!std::regex_match(name, match, pattern)) continue;
        const auto first = std::stoull(match[3].str()), last = std::stoull(match[4].str());
        if (first == 0U || last < first) throw std::runtime_error("invalid archived WAL range: " + name);
        const auto base = directory / ("C_" + match[1].str() + "_" + match[2].str() + "." + match[3].str() + ".chk");
        auto bytes = std::filesystem::file_size(entry.path());
        if (std::filesystem::exists(base)) {
            const auto base_bytes = std::filesystem::file_size(base);
            if (base_bytes > std::numeric_limits<std::uint64_t>::max() - bytes)
                throw std::runtime_error("feed archive size overflow");
            bytes += base_bytes;
        }
        result.push_back({entry.path(), base, last, bytes,
                          {std::stoll(match[1].str()), std::stoll(match[2].str())}, first});
    }
    return result;
}
std::shared_ptr<std::atomic<std::size_t>> ReaderPins(const StoreId& epoch) {
    // Reopened stores of this epoch must respect cursors of the previous store.
    static std::mutex mutex;
    static std::map<StoreId, std::weak_ptr<std::atomic<std::size_t>>> counters;
    std::lock_guard lock(mutex);
    for (auto it = counters.begin(); it != counters.end();)
        if (it->second.expired()) it = counters.erase(it); else ++it;
    auto& weak = counters[epoch];
    auto counter = weak.lock();
    if (!counter) weak = counter = std::make_shared<std::atomic<std::size_t>>(0U);
    return counter;
}
struct ReaderPin {
    explicit ReaderPin(std::shared_ptr<std::atomic<std::size_t>> value) : counter(std::move(value)) {
        counter->fetch_add(1U, std::memory_order_acq_rel);
    }
    ~ReaderPin() { counter->fetch_sub(1U, std::memory_order_acq_rel); }
    std::shared_ptr<std::atomic<std::size_t>> counter;
};
void CrashPoint(const char* name) {
    if (ConsumeFailpointEnv(name)) std::_Exit(86);
}
}  // namespace

std::optional<std::string_view> ChunkStore::SlotWriteUser() const noexcept {
    if (!feed_slots_active_.load(std::memory_order_acquire)) return std::nullopt;
    return CurrentWriteUser();
}

void FeedSlotTestAccess::Sync(Table& table) {
    auto lease = table.Acquire();
    if (!lease) throw TableNotFoundError("table was dropped");
    auto& store = lease->store();
    store.feed_slots_->Sync(store.feed_.load(std::memory_order_seq_cst)->CompletedWatermark());
}
void FeedSlotTestAccess::Retain(Table& table) {
    auto lease = table.Acquire();
    if (!lease) throw TableNotFoundError("table was dropped");
    lease->store().feed_slots_->Retain();
}
void FeedSlotTestAccess::SetHook(Table& table, FeedSlotTestHook* hook) {
    auto lease = table.Acquire();
    if (!lease) throw TableNotFoundError("table was dropped");
    lease->store().feed_slots_->hook_.store(hook, std::memory_order_release);
}

FeedSlots::FeedSlots(ChunkStore& store, std::size_t max_bytes, std::chrono::milliseconds interval)
    : store_(store), max_bytes_(max_bytes), interval_(interval),
      records_(ReadFeedSlotRecords(store.data_dir_, store.store_id_).value_or(FeedSlotRecords{store.store_id_, 0U, {}})),
      readers_(ReaderPins(store.store_id_)) {
    if (max_bytes_ == 0U || interval_.count() <= 0)
        throw std::invalid_argument("slot limits and sync interval must be positive");
    const bool enabled = std::any_of(records_.slots.begin(), records_.slots.end(), [](const auto& slot) { return !slot.lost; });
    if (!records_.slots.empty() && (store.features_.incompat & kFeatureFeedSlots) == 0U)
        throw std::runtime_error("chunkdb.slots requires the feed slots storage feature");
    if (enabled && store.allow_multiple_processes_)
        throw std::invalid_argument("feed slots require a single-process table");
    if (store.access_mode_ == AccessMode::kReadWrite)
        CleanupAtomicTmpArtifacts(store.data_dir_ / kFeedSlotsFileName);
    store.feed_slots_active_.store(enabled, std::memory_order_release);
    if (store.access_mode_ == AccessMode::kReadWrite) {
        RecoverAliases();
        Retain();
    }
}
FeedSlots::~FeedSlots() { Stop(); }
bool FeedSlots::active() const noexcept { return store_.feed_slots_active_.load(std::memory_order_acquire); }
bool FeedSlots::ArchiveRequired() const noexcept {
    return active() || readers_->load(std::memory_order_acquire) != 0U;
}
void FeedSlots::RecoverAliases() {
    for (const auto& segment : Segments(store_.data_dir_)) {
        const auto live = ChunkWalPath(store_.data_dir_, store_.geometry_, segment.coord);
        if (!std::filesystem::exists(live)) continue;
        if (!std::filesystem::equivalent(live, segment.wal)) {
            std::ifstream file(live, std::ios::binary);
            if (!file) throw std::runtime_error("cannot inspect live archive segment " + live.string());
            const auto live_bytes = LoadFile(live);
            if (live_bytes.size() >= kWalHeaderSize + kWalFrameFixedHeaderSize) {
                ValidateWalHeader(live_bytes, segment.coord, store_.store_id_, store_.features_);
                const auto at = kWalHeaderSize;
                const std::uint64_t header_size = kWalFrameFixedHeaderSize + static_cast<std::uint64_t>(ReadLe16(live_bytes, at + 22U)) + kWalFrameHeaderCrcSize;
                const std::uint64_t size = header_size + ReadLe32(live_bytes, at + 28U) + kWalFrameTrailerSize;
                // Trust a revision only after both checksums prove a complete
                // first frame. Incomplete/cut first appends remain under the
                // ordinary recovery parser's crash-tail policy.
                if (size > live_bytes.size() - at ||
                    std::memcmp(live_bytes.data() + at, kWalFrameMagic, kWalFrameMagicSize) != 0 ||
                    Crc32(live_bytes.data() + at + kWalFrameMagicSize, static_cast<std::size_t>(header_size) - kWalFrameMagicSize - kWalFrameHeaderCrcSize) !=
                        ReadLe32(live_bytes, at + static_cast<std::size_t>(header_size) - kWalFrameHeaderCrcSize) ||
                    Crc32(live_bytes.data() + at + static_cast<std::size_t>(header_size), ReadLe32(live_bytes, at + 28U)) !=
                        ReadLe32(live_bytes, at + static_cast<std::size_t>(size) - kWalFrameTrailerSize)) continue;
                const std::vector<std::uint8_t> frame(live_bytes.begin() + static_cast<std::ptrdiff_t>(at),
                    live_bytes.begin() + static_cast<std::ptrdiff_t>(at + size));
                if (InspectFeedFrame(frame, store_.geometry_, store_.features_).revision <= segment.last)
                    throw std::runtime_error("independent live WAL overlaps an archived segment");
            }
            continue;
        }
        // A power loss during the cross-directory rename may preserve both
        // hard-linked names. Validate the immutable archive's declared range
        // before dropping the live alias; an append must never modify it.
        const auto bytes = LoadFile(segment.wal);
        ValidateWalHeader(bytes, segment.coord, store_.store_id_, store_.features_);
        std::uint64_t first = 0U, last = 0U;
        for (std::size_t at = kWalHeaderSize; at < bytes.size();) {
            if (bytes.size() - at < kWalFrameFixedHeaderSize) throw std::runtime_error("partial aliased archive WAL");
            const std::uint64_t size = kWalFrameFixedHeaderSize + static_cast<std::uint64_t>(ReadLe16(bytes, at + 22U)) +
                kWalFrameHeaderCrcSize + ReadLe32(bytes, at + 28U) + kWalFrameTrailerSize;
            if (size > bytes.size() - at) throw std::runtime_error("partial aliased archive WAL");
            const std::vector<std::uint8_t> frame(bytes.begin() + static_cast<std::ptrdiff_t>(at),
                bytes.begin() + static_cast<std::ptrdiff_t>(at + size));
            const auto info = InspectFeedFrame(frame, store_.geometry_, store_.features_);
            if (info.revision <= last) throw std::runtime_error("aliased archive WAL revisions do not increase");
            if (first == 0U) first = info.revision;
            last = info.revision;
            at += static_cast<std::size_t>(size);
        }
        if (first != segment.first || last != segment.last)
            throw std::runtime_error("aliased archive WAL range disagrees with its name");
        ChunkStore::SnapshotGenerationWriteGuard generation(&store_);
        std::filesystem::remove(live);
        SyncDirectoryPath(live.parent_path());
        SyncDirectoryPath(segment.wal.parent_path());
        generation.Finish();
    }
}
void FeedSlots::Start(std::shared_ptr<ChangeFeed> feed) {
    if (!ArchiveRequired() || store_.access_mode_ == AccessMode::kReadOnly) return;
    Stop();
    feed_ = std::move(feed);
    {
        std::lock_guard lock(worker_mutex_);
        stop_ = false;
    }
    try { worker_ = std::thread([this] { Run(); }); }
    catch (const std::exception& error) {
        store_.PoisonDurability("cannot start feed slot worker: " + std::string(error.what()));
    }
}
void FeedSlots::Stop() {
    {
        std::lock_guard lock(worker_mutex_);
        stop_ = true;
    }
    worker_cv_.notify_all();
    if (worker_.joinable()) worker_.join();
    feed_.reset();
}
void FeedSlots::Run() {
    std::unique_lock lock(worker_mutex_);
    while (!worker_cv_.wait_for(lock, interval_, [this] { return stop_; })) {
        lock.unlock();
        try {
            if (active()) Sync(feed_->CompletedWatermark());
            Retain();
            if (!active() && !store_.feed_watchers_active_.load(std::memory_order_acquire)) {
                store_.feed_.store(nullptr, std::memory_order_seq_cst);
                if (auto* hook = hook_.load(std::memory_order_acquire))
                    hook->Run(FeedSlotTestHook::Point::kFeedDisabled, 0U);
            }
            if (!ArchiveRequired()) return;
        } catch (const std::exception& error) {
            store_.PoisonDurability("feed slot sync failed: " + std::string(error.what()));
            LogMessage(LogLevel::kError, LogComponent::kStore, "feed slot durability frozen", {{"error", error.what()}});
            return;
        }
        lock.lock();
    }
}
void FeedSlots::Persist(FeedSlotRecords next) {
    bool published = false;
    try { WriteFeedSlotRecords(store_.data_dir_, next, &published); }
    catch (const std::exception& error) {
        if (published) {
            // The replacement exists, but its durability is ambiguous. Preserve
            // all history and refuse further writes until recovery.
            store_.feed_slots_active_.store(true, std::memory_order_release);
            store_.PoisonDurability("feed slot record publication failed: " + std::string(error.what()));
        }
        throw;
    }
    records_ = std::move(next);
    store_.feed_slots_active_.store(
        std::any_of(records_.slots.begin(), records_.slots.end(), [](const auto& slot) { return !slot.lost; }),
        std::memory_order_release);
}
FeedSlot FeedSlots::Create(std::string_view name, std::uint64_t completed) {
    RequireValidFeedSlotName(name);
    Sync(completed);
    std::lock_guard lock(mutex_);
    auto next = records_;
    if (std::any_of(next.slots.begin(), next.slots.end(), [&](const auto& slot) { return slot.name == name; }))
        throw std::invalid_argument("feed slot already exists: " + std::string(name));
    next.slots.push_back({std::string(name), completed, false});
    Persist(std::move(next));
    return {std::string(name), {store_.store_id_, completed}, records_.durable_watermark, 0U};
}
void FeedSlots::Drop(std::string_view name) {
    RequireValidFeedSlotName(name);
    std::lock_guard lock(mutex_);
    auto next = records_;
    const auto it = std::find_if(next.slots.begin(), next.slots.end(), [&](const auto& slot) { return slot.name == name; });
    if (it == next.slots.end()) throw FeedSlotNotFoundError("unknown feed slot: " + std::string(name));
    next.slots.erase(it);
    Persist(std::move(next));
}
void FeedSlots::Advance(std::string_view name, FeedPosition position) {
    RequireValidFeedSlotName(name);
    store_.ThrowIfDurabilityPoisoned();
    std::lock_guard lock(mutex_);
    auto next = records_;
    auto it = std::find_if(next.slots.begin(), next.slots.end(), [&](const auto& slot) { return slot.name == name; });
    if (it == next.slots.end()) throw FeedSlotNotFoundError("unknown feed slot: " + std::string(name));
    if (it->lost) throw FeedSlotLostError("feed slot exceeded its retention limit: " + std::string(name));
    if (position.epoch != next.epoch || position.revision < it->written || position.revision > next.durable_watermark)
        throw std::invalid_argument("feed slot position has wrong epoch, decreases or exceeds durable watermark");
    if (position.revision == it->written) return;
    it->written = position.revision;
    Persist(std::move(next));
}
std::uint64_t FeedSlots::RetainedBytes(std::uint64_t written) const {
    std::uint64_t bytes = 0U;
    const auto segments = Segments(store_.data_dir_);
    for (const auto& segment : segments) {
        if (segment.last <= written) continue;
        if (segment.bytes > std::numeric_limits<std::uint64_t>::max() - bytes)
            return std::numeric_limits<std::uint64_t>::max();
        bytes += segment.bytes;
    }
    const auto directory = store_.data_dir_ / kFeedArchiveDirName;
    if (std::filesystem::exists(directory)) {
        static const std::regex base_name(R"(^C_(-?[0-9]+)_(-?[0-9]+)\.([0-9]+)\.chk$)");
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            std::smatch match;
            const auto name = entry.path().filename().string();
            if (!std::regex_match(name, match, base_name) ||
                std::any_of(segments.begin(), segments.end(), [&](const auto& segment) { return segment.base == entry.path(); })) continue;
            const ChunkCoord coord{std::stoll(match[1].str()), std::stoll(match[2].str())};
            if (!std::filesystem::exists(ChunkWalPath(store_.data_dir_, store_.geometry_, coord))) continue;
            const auto size = std::filesystem::file_size(entry.path());
            if (size > std::numeric_limits<std::uint64_t>::max() - bytes) return std::numeric_limits<std::uint64_t>::max();
            bytes += size;
        }
    }
    return bytes;
}
std::vector<FeedSlot> FeedSlots::List() const {
    std::lock_guard publish_lock(store_.checkpoint_publish_mutex_);
    std::lock_guard lock(mutex_);
    std::vector<FeedSlot> result;
    for (const auto& slot : records_.slots) {
        if (slot.lost) continue;
        result.push_back({slot.name, {records_.epoch, slot.written}, records_.durable_watermark, RetainedBytes(slot.written)});
    }
    return result;
}
FeedArchiveReader FeedSlots::Reader(FeedPosition after) {
    std::lock_guard publish_lock(store_.checkpoint_publish_mutex_);
    std::lock_guard lock(mutex_);
    if ((store_.features_.incompat & kFeatureFeedSlots) == 0U)
        throw std::invalid_argument("archive reader requires an initialized feed slot table");
    if (after.epoch != records_.epoch || after.revision > records_.durable_watermark)
        throw std::invalid_argument("archive position has wrong epoch or exceeds durable watermark");
    auto earliest = records_.durable_watermark;
    for (const auto& slot : records_.slots) if (!slot.lost) earliest = std::min(earliest, slot.written);
    if (after.revision < earliest)
        throw FeedArchiveExpiredError("archive position precedes every retained slot position");
    auto pin = std::make_shared<ReaderPin>(readers_);
    return FeedArchiveAccess::Create(store_.data_dir_, store_.geometry_, records_.epoch, after,
                                    records_.durable_watermark, std::move(pin), store_.features_);
}
void FeedSlots::Sync(std::uint64_t completed) {
    try { SyncImpl(completed); }
    catch (const std::exception& error) {
        store_.PoisonDurability("feed slot synchronization failed: " + std::string(error.what()));
        throw;
    }
}
void FeedSlots::SyncImpl(std::uint64_t completed) {
    store_.ThrowIfDurabilityPoisoned();
    std::unique_lock barrier_lock(store_.wal_barrier_mutex_);
    if (auto* hook = hook_.load(std::memory_order_acquire)) hook->Run(FeedSlotTestHook::Point::kBeforeFlush, completed);
    std::vector<std::shared_ptr<ChunkStore::LargeChunk>> large_chunks;
    {
        std::lock_guard lock(store_.large_chunks_mutex_);
        for (const auto& [_, large] : store_.large_chunks_) large_chunks.push_back(large);
    }
    for (const auto& large : large_chunks) {
        std::vector<std::pair<ChunkCoord, std::shared_ptr<ChunkStore::RegularChunk>>> chunks;
        {
            std::lock_guard lock(large->mutex);
            for (const auto& item : large->chunks) chunks.push_back(item);
        }
        for (const auto& [coord, chunk] : chunks) {
            std::unique_lock lock(chunk->mutex);
            store_.FlushWalBatch(coord, chunk, false);
        }
    }
    // Fsync is outside chunk locks. Publication is serialized so none of the
    // flushed paths can be replaced by an unsynced checkpoint during the pass.
    std::lock_guard publish_lock(store_.checkpoint_publish_mutex_);
    if (auto* hook = hook_.load(std::memory_order_acquire)) hook->Run(FeedSlotTestHook::Point::kBeforeSync, completed);
    std::vector<std::filesystem::path> dirs{store_.data_dir_};
    for (const auto& directory : std::filesystem::directory_iterator(store_.data_dir_)) {
        const auto name = directory.path().filename().string();
        if (!directory.is_directory() || (name.rfind("L_", 0U) != 0U && name != kFeedArchiveDirName)) continue;
        dirs.push_back(directory.path());
        for (const auto& entry : std::filesystem::directory_iterator(directory.path()))
            if (entry.is_regular_file() && (entry.path().extension() == ".wal" || entry.path().extension() == ".chk"))
                SyncFilePath(entry.path());
    }
    for (auto it = dirs.rbegin(); it != dirs.rend(); ++it) SyncDirectoryPath(*it);
    store_.ThrowIfDurabilityPoisoned();
    if (auto* hook = hook_.load(std::memory_order_acquire)) hook->Run(FeedSlotTestHook::Point::kBeforePersist, completed);
    std::lock_guard lock(mutex_);
    if (completed <= records_.durable_watermark) return;
    auto next = records_;
    next.durable_watermark = completed;
    if (next.slots.empty()) records_ = std::move(next); else Persist(std::move(next));
}
void FeedSlots::Retain() {
    store_.ThrowIfDurabilityPoisoned();
    std::lock_guard publish_lock(store_.checkpoint_publish_mutex_);
    std::lock_guard lock(mutex_);
    auto next = records_;
    std::vector<std::string> lost;
    for (auto& slot : next.slots) {
        if (!slot.lost && RetainedBytes(slot.written) > max_bytes_) {
            slot.lost = true;
            lost.push_back(slot.name);
        }
    }
    if (!lost.empty()) Persist(std::move(next));
    for (const auto& name : lost)
        LogMessage(LogLevel::kWarn, LogComponent::kStore, "feed slot lost: retention limit exceeded", {{"slot", name}});
    if (readers_->load(std::memory_order_acquire) != 0U) return;
    std::uint64_t minimum = std::numeric_limits<std::uint64_t>::max();
    for (const auto& slot : records_.slots) if (!slot.lost) minimum = std::min(minimum, slot.written);
    bool removed = false;
    for (const auto& segment : Segments(store_.data_dir_)) {
        if (segment.last > minimum) continue;
        std::filesystem::remove(segment.wal);
        std::filesystem::remove(segment.base);
        removed = true;
    }
    const auto directory = store_.data_dir_ / kFeedArchiveDirName;
    if (std::filesystem::exists(directory)) {
        static const std::regex base_name(R"(^C_(-?[0-9]+)_(-?[0-9]+)\.([0-9]+)\.chk$)");
        const auto retained = Segments(store_.data_dir_);
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            std::smatch match;
            const auto name = entry.path().filename().string();
            if (!std::regex_match(name, match, base_name)) continue;
            const auto first = std::stoull(match[3].str());
            if (first > minimum || std::any_of(retained.begin(), retained.end(), [&](const auto& segment) { return segment.base == entry.path(); })) continue;
            const ChunkCoord coord{std::stoll(match[1].str()), std::stoll(match[2].str())};
            // A linked base can precede its WAL archive after a crash. Keep it
            // while any live WAL for this chunk might still require it.
            if (std::filesystem::exists(ChunkWalPath(store_.data_dir_, store_.geometry_, coord))) continue;
            std::filesystem::remove(entry.path());
            removed = true;
        }
    }
    if (removed) SyncDirectoryPath(store_.data_dir_ / kFeedArchiveDirName);
    if (auto* hook = hook_.load(std::memory_order_acquire)) hook->Run(FeedSlotTestHook::Point::kAfterRetention, minimum);
}
std::filesystem::path FeedSlots::PrepareArchive(ChunkCoord coord) {
    const auto wal_path = ChunkWalPath(store_.data_dir_, store_.geometry_, coord);
    if (!std::filesystem::exists(wal_path)) return {};
    const auto wal = LoadFile(wal_path);
    ValidateWalHeader(wal, coord, store_.store_id_, store_.features_);
    std::uint64_t first = 0U, last = 0U;
    for (std::size_t at = kWalHeaderSize; at < wal.size();) {
        if (wal.size() - at < kWalFrameFixedHeaderSize) throw std::runtime_error("partial WAL cannot be archived");
        const std::uint64_t size = kWalFrameFixedHeaderSize + static_cast<std::uint64_t>(ReadLe16(wal, at + 22U)) +
                                   kWalFrameHeaderCrcSize + ReadLe32(wal, at + 28U) + kWalFrameTrailerSize;
        if (size > wal.size() - at) throw std::runtime_error("partial WAL cannot be archived");
        const std::vector<std::uint8_t> frame(wal.begin() + static_cast<std::ptrdiff_t>(at),
            wal.begin() + static_cast<std::ptrdiff_t>(at + size));
        const auto info = InspectFeedFrame(frame, store_.geometry_, store_.features_);
        if (info.revision <= last) throw std::runtime_error("archive WAL revisions do not increase");
        if (first == 0U) first = info.revision;
        last = info.revision;
        at += static_cast<std::size_t>(size);
    }
    if (first == 0U) return {};
    const auto directory = store_.data_dir_ / kFeedArchiveDirName;
    EnsureDirectoryPathExists(directory, true);
    const auto stem = "C_" + std::to_string(coord.x) + "_" + std::to_string(coord.y) + "." + std::to_string(first);
    const auto base = directory / (stem + ".chk");
    const auto image = ChunkDataPath(store_.data_dir_, store_.geometry_, coord);
    if (!std::filesystem::exists(base) && std::filesystem::exists(image)) {
        const auto state = ParseChunkImage(LoadFile(image), store_.geometry_, coord, store_.store_id_, store_.features_);
        // On retry a published image already contains this WAL. The original
        // base was absent, so this newer image must never become its base.
        if (state.revision < first) {
#ifdef _WIN32
            if (!CreateHardLinkW(base.c_str(), image.c_str(), nullptr))
                throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "archive base hard link");
#else
            if (::link(image.c_str(), base.c_str()) != 0)
                throw std::system_error(errno, std::generic_category(), "archive base hard link");
#endif
            SyncFilePath(base);
            SyncDirectoryPath(directory);
        }
    }
    SyncFilePath(wal_path);
    SyncDirectoryPath(wal_path.parent_path());
    CrashPoint("CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_BASE_LINK_ONCE");
    return directory / (stem + "-" + std::to_string(last) + ".wal");
}

}  // namespace chunkdb
