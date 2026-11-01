#include "feed_slots.hpp"

#include <algorithm>
#include <charconv>
#include <limits>
#include <map>

#include "change_feed.hpp"
#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feature_flags.hpp"
#include "feed_archive.hpp"
#include "wal_replay.hpp"
#include "chunk_coordinate_index.hpp"
#include "shared_file_input.hpp"

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
    std::filesystem::path descriptor;
};
bool Number(std::string_view text, std::uint64_t* value) {
    if (text.empty()) return false;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), *value);
    return error == std::errc{} && end == text.data() + text.size();
}
std::optional<std::pair<ChunkCoord, std::uint64_t>> BaseName(std::string_view name, std::uint8_t dims) {
    if (name.ends_with(".base")) {
        ChunkCoord coord;
        std::uint64_t first = 0U;
        if (!ParseArchiveBaseFileName(name, dims, &coord, &first)) return std::nullopt;
        return std::make_pair(coord, first);
    }
    if (!name.ends_with(".chk")) return std::nullopt;
    name.remove_suffix(4U);
    const auto dot = name.find('.');
    ChunkCoord coord;
    std::uint64_t first = 0U;
    if (dot == std::string_view::npos || !ParseChunkFileStem(name.substr(0U, dot), dims, &coord) ||
        !Number(name.substr(dot + 1U), &first) || first == 0U) return std::nullopt;
    return std::make_pair(coord, first);
}
std::vector<Segment> Segments(const std::filesystem::path& root, std::uint8_t dims) {
    const auto directory = root / kFeedArchiveDirName;
    if (!std::filesystem::exists(directory)) return {};
    std::vector<Segment> result;
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        auto name = entry.path().filename().string();
        if (!name.ends_with(".wal")) continue;
        name.resize(name.size() - 4U);
        const auto dot = name.find('.'), dash = name.find('-', dot);
        ChunkCoord coord;
        std::uint64_t first = 0U, last = 0U;
        if (dot == std::string::npos || dash == std::string::npos ||
            !ParseChunkFileStem(std::string_view(name).substr(0U, dot), dims, &coord) ||
            !Number(std::string_view(name).substr(dot + 1U, dash - dot - 1U), &first) ||
            !Number(std::string_view(name).substr(dash + 1U), &last)) continue;
        if (first == 0U || last < first) throw std::runtime_error("invalid archived WAL range: " + name);
        const auto base = directory / (ChunkFileStem(coord, dims) + "." + std::to_string(first) + ".chk");
        const auto descriptor = directory / ArchiveBaseFileName(coord, dims, first);
        auto bytes = std::filesystem::file_size(entry.path());
        for (const auto& artifact : {base, descriptor}) if (std::filesystem::exists(artifact)) {
            const auto base_bytes = std::filesystem::file_size(artifact);
            if (base_bytes > std::numeric_limits<std::uint64_t>::max() - bytes)
                throw std::runtime_error("feed archive size overflow");
            bytes += base_bytes;
        }
        result.push_back({entry.path(), base, last, bytes, coord, first, descriptor});
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

FeedArchiveReader FeedSlotTestAccess::CompletedPrefix(Table& table, std::string_view name, FeedPosition after) {
    auto lease = table.Acquire();
    if (!lease) throw TableNotFoundError("table was dropped");
    return lease->store().feed_slots_->ReaderCompletedPrefix(name, after);
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
void FeedSlotTestAccess::StageAck(Table& table, std::string_view name, FeedPosition position) {
    auto lease = table.Acquire();
    if (!lease) throw TableNotFoundError("table was dropped");
    lease->store().feed_slots_->StageAck(name, position);
}
bool FeedSlotTestAccess::FlushAcks(Table& table, bool force, std::chrono::steady_clock::time_point now) {
    auto lease = table.Acquire();
    if (!lease) throw TableNotFoundError("table was dropped");
    return lease->store().feed_slots_->FlushAcks(force, now);
}
void FeedSlotTestAccess::SetHook(Table& table, FeedSlotTestHook* hook) {
    if (hook == nullptr) {
        // Drain manual passes and join the worker before the caller destroys
        // a hook that a callback may already have loaded.
        auto store = table.BeginExclusive();
        if (!store) throw TableNotFoundError("table was dropped");
        store->feed_slots_->hook_.store(nullptr, std::memory_order_release);
        table.EndExclusive(std::move(store), table.options_);
        return;
    }
    auto lease = table.Acquire();
    if (!lease) throw TableNotFoundError("table was dropped");
    lease->store().feed_slots_->hook_.store(hook, std::memory_order_release);
}

FeedSlots::FeedSlots(ChunkStore& store, std::size_t max_bytes, std::chrono::milliseconds interval)
    : store_(store), max_bytes_(max_bytes), interval_(interval),
      records_(ReadFeedSlotRecords(store.data_dir_, store.store_id_, store.geometry_.dims()).value_or(
          FeedSlotRecords{store.store_id_, 0U, {}, false, store.geometry_.dims()})),
      readers_(ReaderPins(store.store_id_)) {
    if (max_bytes_ == 0U || interval_.count() <= 0)
        throw std::invalid_argument("slot limits and sync interval must be positive");
    const bool filtered = records_.filter_format || std::any_of(records_.slots.begin(), records_.slots.end(),
        [](const auto& slot) { return !slot.filters.empty() || slot.schema_filter_lost; });
    if (filtered && (store.features_.incompat & kFeatureSlotFilters) == 0U)
        throw std::runtime_error("chunkdb.slots filters require the slot filters storage feature");
    if (!records_.slots.empty() && (store.features_.incompat & kFeatureFeedSlots) == 0U)
        throw std::runtime_error("chunkdb.slots requires the feed slots storage feature");
    const bool originally_enabled = std::any_of(records_.slots.begin(), records_.slots.end(), [](const auto& slot) { return !slot.lost; });
    if (originally_enabled && store.allow_multiple_processes_)
        throw std::invalid_argument("feed slots require a single-process table");
    bool changed = false;
    for (auto& slot : records_.slots) {
        if (slot.lost || !slot.filters.column_ids) continue;
        const auto& columns = store.geometry_.layout().schema().columns;
        if (std::any_of(slot.filters.column_ids->begin(), slot.filters.column_ids->end(), [&](const auto id) {
            return std::none_of(columns.begin(), columns.end(), [&](const auto& column) { return column.id == id; });
        })) { slot.lost = true; slot.schema_filter_lost = true; changed = true; }
    }
    if (changed && store.access_mode_ == AccessMode::kReadWrite) WriteFeedSlotRecords(store.data_dir_, records_);
    const bool enabled = std::any_of(records_.slots.begin(), records_.slots.end(), [](const auto& slot) { return !slot.lost; });
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
void FeedSlots::ValidateRecoveryAliases(const ChunkStore& store) {
    auto segments = Segments(store.data_dir(), store.geometry().dims());
    std::sort(segments.begin(), segments.end(), [](const auto& first, const auto& second) {
        return first.coord < second.coord || (first.coord == second.coord && first.first < second.first);
    });
    for (std::size_t i = 0U; i < segments.size(); ++i) {
        const auto& segment = segments[i];
        SharedFileInput file(segment.wal);
        file.seekg(0, std::ios::end);
        const auto measured = file.tellg();
        if (!file || measured < 0) throw std::runtime_error("cannot measure aliased archive WAL");
        const auto info = InspectWalStream(file, static_cast<std::uint64_t>(measured), store.geometry(), segment.coord,
            store.store_id(), store.features());
        if (info.first != segment.first || info.last != segment.last)
            throw std::runtime_error("aliased archive WAL range disagrees with its name");
        if (i != 0U && segments[i - 1U].coord == segment.coord && segments[i - 1U].last >= segment.first)
            throw std::runtime_error("archived WAL ranges overlap");
    }
    for (std::size_t begin = 0U; begin < segments.size();) {
        auto end = begin + 1U;
        while (end < segments.size() && segments[end].coord == segments[begin].coord) ++end;
        const auto live = ChunkWalPath(store.data_dir(), store.geometry(), segments[begin].coord);
        if (std::filesystem::exists(live)) {
            bool alias = false;
            for (auto at = begin; at < end; ++at)
                if (std::filesystem::equivalent(live, segments[at].wal)) { alias = true; break; }
            if (!alias) {
                SharedFileInput file(live);
                file.seekg(0, std::ios::end);
                const auto measured = file.tellg();
                if (!file || measured < 0) throw std::runtime_error("cannot inspect live archive segment " + live.string());
                const auto first = InspectFirstWalFrameStream(file, static_cast<std::uint64_t>(measured), store.geometry(),
                    segments[begin].coord, store.store_id(), store.features());
                if (first && first->revision <= segments[end - 1U].last)
                    throw std::runtime_error("independent live WAL overlaps an archived segment");
            }
        }
        begin = end;
    }
}

void FeedSlots::RecoverAliases() {
    ValidateRecoveryAliases(store_);
    for (const auto& segment : Segments(store_.data_dir_, store_.geometry_.dims())) {
        const auto live = ChunkWalPath(store_.data_dir_, store_.geometry_, segment.coord);
        if (!std::filesystem::exists(live) || !std::filesystem::equivalent(live, segment.wal)) continue;
        // Complete admission precedes every removal, including when one live
        // name aliases an earlier member of a multi-segment archive.
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
    auto next_sync = std::chrono::steady_clock::now() + interval_;
    const auto tick = std::min(interval_, std::chrono::milliseconds(100));
    while (!worker_cv_.wait_for(lock, tick, [this] { return stop_; })) {
        lock.unlock();
        try {
            const bool sync_due = active() && std::chrono::steady_clock::now() >= next_sync;
            if (sync_due) {
                Sync(feed_->CompletedWatermark());
                next_sync = std::chrono::steady_clock::now() + interval_;
            }
            const bool acknowledged = FlushAcks(false);
            if (sync_due || acknowledged || !active()) Retain();
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
    next.filter_format = next.filter_format || std::any_of(next.slots.begin(), next.slots.end(),
        [](const auto& slot) { return !slot.filters.empty() || slot.schema_filter_lost; });
    bool published = false;
    try { WriteFeedSlotRecords(store_.data_dir_, next, &published); }
    catch (const std::exception& error) {
        if (published) {
            // The replacement exists, but its durability is ambiguous. Preserve
            // all history and refuse further writes until recovery.
            store_.feed_slots_active_.store(true, std::memory_order_release);
            store_.PoisonDurability("feed slot record publication failed: " + std::string(error.what()));
            throw WriteOutcomeUnknownError("feed slot record publication failed: " + std::string(error.what()),
                WriteOutcomeUnknownError::Stage::kSlotRecords);
        }
        throw;
    }
    records_ = std::move(next);
    store_.feed_slots_active_.store(
        std::any_of(records_.slots.begin(), records_.slots.end(), [](const auto& slot) { return !slot.lost; }),
        std::memory_order_release);
}
FeedSlot FeedSlots::Create(std::string_view name, std::uint64_t completed, bool if_not_exists, const FeedSlotFilters& filters) {
    RequireValidFeedSlotName(name);
    store_.ThrowIfDurabilityPoisoned();
    if (if_not_exists) {
        std::lock_guard lock(mutex_);
        const auto found = std::find_if(records_.slots.begin(), records_.slots.end(),
            [&](const auto& slot) { return slot.name == name; });
        if (found != records_.slots.end()) {
            if (found->filters != filters) throw FeedSlotFiltersError("feed slot filters differ from existing slot");
            return {found->name, {records_.epoch, found->written}, records_.durable_watermark, 0U, found->lost, found->filters, found->schema_filter_lost};
        }
    }
    Sync(completed);
    std::lock_guard lock(mutex_);
    auto next = records_;
    const auto found = std::find_if(next.slots.begin(), next.slots.end(), [&](const auto& slot) { return slot.name == name; });
    if (found != next.slots.end()) {
        if (if_not_exists) {
            if (found->filters != filters) throw FeedSlotFiltersError("feed slot filters differ from existing slot");
            return {found->name, {records_.epoch, found->written}, records_.durable_watermark, 0U, found->lost, found->filters, found->schema_filter_lost};
        }
        throw std::invalid_argument("feed slot already exists: " + std::string(name));
    }
    next.slots.push_back({std::string(name), completed, false, filters});
    Persist(std::move(next));
    return {std::string(name), {store_.store_id_, completed}, records_.durable_watermark, 0U, false, filters};
}
void FeedSlots::Drop(std::string_view name, bool if_exists) {
    RequireValidFeedSlotName(name);
    std::lock_guard lock(mutex_);
    auto next = records_;
    const auto it = std::find_if(next.slots.begin(), next.slots.end(), [&](const auto& slot) { return slot.name == name; });
    if (it == next.slots.end()) {
        if (if_exists) return;
        throw FeedSlotNotFoundError("unknown feed slot: " + std::string(name));
    }
    const auto pending = acknowledgements_->pending.find(std::string(name));
    next.slots.erase(it);
    Persist(std::move(next));
    if (pending != acknowledgements_->pending.end()) acknowledgements_->pending.erase(pending);
}
void FeedSlots::Advance(std::string_view name, FeedPosition position) {
    RequireValidFeedSlotName(name);
    store_.ThrowIfDurabilityPoisoned();
    std::lock_guard lock(mutex_);
    auto next = records_;
    auto it = std::find_if(next.slots.begin(), next.slots.end(), [&](const auto& slot) { return slot.name == name; });
    if (it == next.slots.end()) throw FeedSlotNotFoundError("unknown feed slot: " + std::string(name));
    if (it->lost) throw FeedSlotLostError(it->schema_filter_lost ? "feed slot projected column was dropped" : "feed slot exceeded its retention limit",
        it->schema_filter_lost ? "schema_filter" : "retention_limit");
    if (position.epoch != next.epoch || position.revision < it->written || position.revision > next.durable_watermark)
        throw std::invalid_argument("feed slot position has wrong epoch, decreases or exceeds durable watermark");
    if (position.revision == it->written) return;
    it->written = position.revision;
    Persist(std::move(next));
}
void FeedSlots::StageAck(std::string_view name, FeedPosition position) {
    store_.ThrowIfDurabilityPoisoned();
    std::lock_guard lock(mutex_);
    const auto slot = std::find_if(records_.slots.begin(), records_.slots.end(),
        [&](const auto& record) { return record.name == name; });
    if (slot == records_.slots.end()) throw FeedSlotNotFoundError("unknown feed slot: " + std::string(name));
    if (slot->lost) throw FeedSlotLostError(slot->schema_filter_lost ? "feed slot projected column was dropped" : "feed slot exceeded its retention limit",
            slot->schema_filter_lost ? "schema_filter" : "retention_limit");
    if (position.epoch != records_.epoch) throw std::invalid_argument("ACK belongs to another epoch");
    if (position.revision <= slot->written) return;
    auto& pending = acknowledgements_->pending[std::string(name)];
    pending = std::max(pending, position.revision);
}
bool FeedSlots::FlushAcks(bool force, std::chrono::steady_clock::time_point now) {
    store_.ThrowIfDurabilityPoisoned();
    std::lock_guard lock(mutex_);
    if (acknowledgements_->pending.empty()) return false;
    if (!force && now - acknowledgements_->flushed < std::chrono::milliseconds(100)) return false;
    auto next = records_;
    bool changed = false;
    for (auto& slot : next.slots) {
        const auto pending = acknowledgements_->pending.find(slot.name);
        if (slot.lost || pending == acknowledgements_->pending.end() || pending->second > next.durable_watermark) continue;
        if (pending->second > slot.written) { slot.written = pending->second; changed = true; }
    }
    if (changed) {
        Persist(std::move(next));
        acknowledgements_->flushed = std::max(now, std::chrono::steady_clock::now());
        if (auto* hook = hook_.load(std::memory_order_acquire)) hook->Run(FeedSlotTestHook::Point::kAfterAckPersist, records_.durable_watermark);
    }
    std::erase_if(acknowledgements_->pending, [&](const auto& pending) {
        const auto slot = std::find_if(records_.slots.begin(), records_.slots.end(),
            [&](const auto& record) { return record.name == pending.first; });
        return slot == records_.slots.end() || slot->lost || pending.second <= slot->written;
    });
    return changed;
}
std::vector<std::uint64_t> FeedSlots::RetainedBytes(const std::vector<std::uint64_t>& positions) const {
    constexpr auto kMax = std::numeric_limits<std::uint64_t>::max();
    const auto add = [](std::uint64_t bytes, std::uint64_t more) { return more > kMax - bytes ? kMax : bytes + more; };
    // One listing serves every slot; the publish and records locks keep it current.
    const auto segments = Segments(store_.data_dir_, store_.geometry_.dims());
    // A base linked for a live WAL that is not archived yet counts for every position.
    std::uint64_t bases = 0U;
    const auto directory = store_.data_dir_ / kFeedArchiveDirName;
    if (std::filesystem::exists(directory)) {
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            const auto base = BaseName(entry.path().filename().string(), store_.geometry_.dims());
            if (!base ||
                std::any_of(segments.begin(), segments.end(), [&](const auto& segment) {
                    return segment.base == entry.path() || segment.descriptor == entry.path(); })) continue;
            const auto coord = base->first;
            if (!std::filesystem::exists(ChunkWalPath(store_.data_dir_, store_.geometry_, coord))) continue;
            bases = add(bases, std::filesystem::file_size(entry.path()));
        }
    }
    std::vector<std::uint64_t> result;
    result.reserve(positions.size());
    for (const auto written : positions) {
        auto bytes = bases;
        for (const auto& segment : segments) if (segment.last > written) bytes = add(bytes, segment.bytes);
        result.push_back(bytes);
    }
    return result;
}
std::vector<FeedSlot> FeedSlots::List(bool include_lost) const {
    std::lock_guard publish_lock(store_.checkpoint_publish_mutex_);
    std::lock_guard lock(mutex_);
    std::vector<std::uint64_t> positions;
    for (const auto& slot : records_.slots) if (!slot.lost || include_lost) positions.push_back(slot.written);
    const auto retained = positions.empty() ? positions : RetainedBytes(positions);
    std::vector<FeedSlot> result;
    for (const auto& slot : records_.slots) {
        if (slot.lost && !include_lost) continue;
        result.push_back({slot.name, {records_.epoch, slot.written}, records_.durable_watermark, retained[result.size()], slot.lost, slot.filters, slot.schema_filter_lost});
    }
    return result;
}
FeedSlot FeedSlots::Get(std::string_view name) const {
    store_.ThrowIfDurabilityPoisoned();
    std::lock_guard lock(mutex_);
    const auto it = std::find_if(records_.slots.begin(), records_.slots.end(),
        [&](const auto& slot) { return slot.name == name; });
    if (it == records_.slots.end()) throw FeedSlotNotFoundError("unknown feed slot: " + std::string(name));
    return {it->name, {records_.epoch, it->written}, records_.durable_watermark, 0U, it->lost, it->filters, it->schema_filter_lost};
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
FeedArchiveReader FeedSlots::ReaderCompletedPrefix(std::string_view slot_name, FeedPosition after) {
    StoreId epoch;
    std::uint64_t through;
    std::vector<FeedWalPrefix> prefixes;
    std::shared_ptr<void> pin;
    {
        std::lock_guard publish_lock(store_.checkpoint_publish_mutex_);
        std::lock_guard lock(mutex_);
        const auto slot = std::find_if(records_.slots.begin(), records_.slots.end(),
            [&](const auto& record) { return record.name == slot_name; });
        if (slot == records_.slots.end()) throw FeedSlotNotFoundError("unknown feed slot: " + std::string(slot_name));
        if (slot->lost) throw FeedSlotLostError(slot->schema_filter_lost ? "feed slot projected column was dropped" : "feed slot exceeded its retention limit",
            slot->schema_filter_lost ? "schema_filter" : "retention_limit");
        if (after.epoch != records_.epoch || after.revision > records_.durable_watermark)
            throw std::invalid_argument("archive position has wrong epoch or exceeds durable watermark");
        auto earliest = records_.durable_watermark;
        for (const auto& slot : records_.slots) if (!slot.lost) earliest = std::min(earliest, slot.written);
        if (after.revision < earliest)
            throw FeedArchiveExpiredError("archive position precedes every retained slot position");
        epoch = records_.epoch;
        through = records_.durable_watermark;
        pin = std::make_shared<ReaderPin>(readers_);
    }
    prefix_index_.SeedMissing(store_.data_dir_, store_.geometry_, epoch, store_.features_,
        store_.checkpoint_publish_mutex_);
    {
        std::lock_guard publish_lock(store_.checkpoint_publish_mutex_);
        prefixes = prefix_index_.Capture(through);
        for (auto& prefix : prefixes)
            prefix.retained_empty = store_.RetainedEmptyBoundaryForArchive(prefix.coord);
    }
    std::erase_if(prefixes, [&](const auto& prefix) { return prefix.last <= after.revision; });
    // The caller holds a shared table lease, so geometry/store identity remain
    // stable while ordinary writers continue. Publication locks cover only the
    // in-memory snapshot and pin: listing/opening files never blocks producers.
    return FeedArchiveAccess::CreateCompletedPrefix(store_.data_dir_, store_.geometry_, epoch,
        after, through, std::move(pin), prefixes, store_.features_);
}
void FeedSlots::Sync(std::uint64_t completed) {
    bool advanced;
    try { advanced = SyncImpl(completed); }
    catch (const std::exception& error) {
        store_.PoisonDurability("feed slot synchronization failed: " + std::string(error.what()));
        throw;
    }
    // SyncImpl has released the WAL, checkpoint and records locks before
    // callbacks can wake readers of the newly persisted durable frontier.
    if (advanced && feed_) feed_->NotifyDurableWatermark();
}
bool FeedSlots::SyncImpl(std::uint64_t completed) {
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
    // The store tracks what it wrote without a sync, as for WALFLUSH, so a
    // pass syncs only that, with the WAL barrier's take: it includes what a
    // rollback boundary took out of the set and may still be syncing, so the
    // frontier never relies on a sync this pass does not wait for. A store's
    // first pass also syncs every WAL and image, which an earlier process may
    // have left unsynced.
    auto drained = store_.TakeUnsynced();
    try {
        if (!synced_) {
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
        }
        store_.SyncUnsynced(drained);
    } catch (const std::exception&) {
        store_.RestoreUnsynced(std::move(drained));
        throw;
    }
    synced_ = true;
    store_.ThrowIfDurabilityPoisoned();
    if (auto* hook = hook_.load(std::memory_order_acquire)) hook->Run(FeedSlotTestHook::Point::kBeforePersist, completed);
    std::lock_guard lock(mutex_);
    if (completed <= records_.durable_watermark) return false;
    auto next = records_;
    next.durable_watermark = completed;
    if (next.slots.empty()) records_ = std::move(next); else Persist(std::move(next));
    return true;
}
void FeedSlots::Retain() {
    store_.ThrowIfDurabilityPoisoned();
    std::lock_guard publish_lock(store_.checkpoint_publish_mutex_);
    std::lock_guard lock(mutex_);
    auto next = records_;
    std::vector<std::uint64_t> positions;
    for (const auto& slot : next.slots) if (!slot.lost) positions.push_back(slot.written);
    const auto retained = positions.empty() ? positions : RetainedBytes(positions);
    std::vector<std::string> lost;
    std::size_t live = 0U;
    for (auto& slot : next.slots) {
        if (!slot.lost && retained[live++] > max_bytes_) {
            slot.lost = true;
            lost.push_back(slot.name);
        }
    }
    if (!lost.empty()) {
        Persist(std::move(next));
        for (const auto& name : lost) acknowledgements_->pending.erase(name);
    }
    for (const auto& name : lost)
        LogMessage(LogLevel::kWarn, LogComponent::kStore, "feed slot lost: retention limit exceeded", {{"slot", name}});
    if (readers_->load(std::memory_order_acquire) != 0U) return;
    std::uint64_t minimum = std::numeric_limits<std::uint64_t>::max();
    for (const auto& slot : records_.slots) if (!slot.lost) minimum = std::min(minimum, slot.written);
    bool removed = false;
    for (const auto& segment : Segments(store_.data_dir_, store_.geometry_.dims())) {
        if (segment.last > minimum) continue;
        std::filesystem::remove(segment.wal);
        std::filesystem::remove(segment.base);
        std::filesystem::remove(segment.descriptor);
        removed = true;
    }
    const auto directory = store_.data_dir_ / kFeedArchiveDirName;
    if (std::filesystem::exists(directory)) {
        const auto retained = Segments(store_.data_dir_, store_.geometry_.dims());
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            const auto base = BaseName(entry.path().filename().string(), store_.geometry_.dims());
            if (!base) continue;
            const auto first = base->second;
            if (first > minimum || std::any_of(retained.begin(), retained.end(), [&](const auto& segment) {
                    return segment.base == entry.path() || segment.descriptor == entry.path(); })) continue;
            const auto coord = base->first;
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
    SharedFileInput wal(wal_path);
    wal.seekg(0, std::ios::end);
    const auto measured = wal.tellg();
    if (!wal || measured < 0) throw std::runtime_error("cannot measure archive WAL");
    const auto info = InspectWalStream(wal, static_cast<std::uint64_t>(measured), store_.geometry_, coord,
        store_.store_id_, store_.features_);
    const auto [first, last, first_schema] = info;
    if (first == 0U) return {};
    const auto directory = store_.data_dir_ / kFeedArchiveDirName;
    if (!archive_dir_prepared_.load(std::memory_order_acquire)) {
        // An archive directory this store has not prepared itself may have
        // been made by a preparation that failed before its parent was
        // synced, here or in an earlier process: its entry is durable only
        // once that sync is done. A failed preparation is not recorded.
        if (!EnsureDirectoryPathExists(directory, true)) {
            SyncParentDirectory(directory);
        }
        archive_dir_prepared_.store(true, std::memory_order_release);
    }
    const auto stem = ChunkFileStem(coord, store_.geometry_.dims()) + "." + std::to_string(first);
    const auto base = directory / (stem + ".chk");
    const auto descriptor_path = directory / ArchiveBaseFileName(coord, store_.geometry_.dims(), first);
    const auto image = ChunkDataPath(store_.data_dir_, store_.geometry_, coord);
    if (const auto existing = ReadFeedArchiveBaseDescriptor(descriptor_path, store_.geometry_, store_.store_id_, coord, first)) {
        if (existing->base_schema > first_schema) throw std::runtime_error("archive base schema exceeds its first frame");
        if (existing->kind == ArchiveBaseKind::kImage) {
            (void)ReadPinnedFeedArchiveBase(base, store_.geometry_, store_.store_id_, coord, store_.features_, *existing);
            SyncFilePath(base);
        }
        else if (std::filesystem::exists(base)) throw std::runtime_error("non-image archive base has an unexpected pinned image");
        // A failed atomic rename can leave a visible descriptor whose
        // directory sync is still owed. Repay it before replacing its base.
        SyncFilePath(descriptor_path);
        SyncDirectoryPath(directory);
    } else {
        ArchiveBaseDescriptor descriptor{.dims = store_.geometry_.dims(), .epoch = store_.store_id_,
            .coord = coord, .first = first, .base_schema = first_schema};
        std::optional<ChunkStateImage> original;
        std::vector<std::uint8_t> original_bytes;
        bool covered = false;
        const bool pinned = std::filesystem::exists(base);
        const auto source = pinned ? base : image;
        if (std::filesystem::exists(source)) {
            auto bytes = LoadChunkImageFile(source, store_.geometry_, coord, store_.store_id_, store_.features_);
            auto state = ParseChunkImage(bytes, store_.geometry_, coord, store_.store_id_, store_.features_);
            if (state.revision < first) { original = std::move(state); original_bytes = std::move(bytes); }
            else covered = true;
        }
        if (pinned) {
            if (!original || !std::filesystem::exists(image))
                throw std::runtime_error("staged archive image has no original live image");
            const auto current = LoadChunkImageFile(image, store_.geometry_, coord, store_.store_id_, store_.features_);
            if (current != original_bytes)
                throw std::runtime_error("staged archive image differs from the original live image");
        }
        const auto retained = store_.RetainedEmptyBoundaryForArchive(coord);
        if (retained && retained->revision >= first) covered = true;
        if (retained && retained->revision < first && (!original || retained->revision >= original->revision)) {
            if (pinned) throw std::runtime_error("staged archive image precedes its retained empty base");
            descriptor.kind = ArchiveBaseKind::kRetainedEmpty;
            descriptor.base_revision = retained->revision;
            descriptor.base_time_ms = retained->commit_time_ms;
            descriptor.base_schema = retained->schema_version;
        } else if (original) {
            descriptor.kind = ArchiveBaseKind::kImage;
            descriptor.base_revision = original->revision;
            descriptor.base_time_ms = original->commit_time_ms;
            descriptor.base_schema = original->schema_version;
            descriptor.image_length = original_bytes.size();
            descriptor.image_sha256 = crypto::Sha256Of(original_bytes);
            if (!pinned) {
#ifdef _WIN32
                if (!CreateHardLinkW(base.c_str(), image.c_str(), nullptr))
                    throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "archive base hard link");
#else
                if (::link(image.c_str(), base.c_str()) != 0)
                    throw std::system_error(errno, std::generic_category(), "archive base hard link");
#endif
            }
            (void)ReadPinnedFeedArchiveBase(base, store_.geometry_, store_.store_id_, coord, store_.features_, descriptor);
            SyncFilePath(base);
            SyncDirectoryPath(directory);
        } else if (covered || pinned) {
            throw std::runtime_error("original archive base is missing after its WAL was covered");
        }
        if (descriptor.base_schema > first_schema) throw std::runtime_error("archive base schema exceeds its first frame");
        AtomicWrite(descriptor_path, SerializeArchiveBaseDescriptor(descriptor, kMaxChunkImageFileBytes), true, true,
            nullptr, "CHUNKDB_FAILPOINT_FEED_BASE_AFTER_RENAME_ONCE");
    }
    SyncFilePath(wal_path);
    SyncDirectoryPath(wal_path.parent_path());
    CrashPoint("CHUNKDB_FAILPOINT_CRASH_FEED_AFTER_BASE_LINK_ONCE");
    return directory / (stem + "-" + std::to_string(last) + ".wal");
}

}  // namespace chunkdb
