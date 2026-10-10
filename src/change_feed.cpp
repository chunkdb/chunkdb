#include "change_feed.hpp"

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "chunkdb/table_catalog.hpp"
#include "chunk_store_internal.hpp"
#include "wal_replay.hpp"
#include "feed_protocol.hpp"

namespace chunkdb {
namespace {
std::atomic<std::uint64_t> g_feed_id{0};
thread_local std::string_view g_write_user;

std::size_t BlockValuesBytes(const FeedBlockChange& block) {
    std::size_t bytes = 0U;
    for (const auto* values : {&block.before, &block.after}) {
        if (!values->has_value()) continue;
        bytes += (*values)->capacity() * sizeof(ColumnValue);
        for (const auto& value : **values) {
            bytes += std::visit([](const auto& v) -> std::size_t {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, std::string>) return v.capacity();
                else if constexpr (std::is_same_v<T, BitsValue>) return v.digits.capacity();
                else if constexpr (std::is_same_v<T, BytesValue>) return v.bytes.capacity();
                else return 0U;
            }, value);
        }
    }
    return bytes;
}
std::size_t EntryBytes(const FeedEntry& entry) {
    std::size_t bytes = (entry.protocol_frame ? entry.protocol_frame->capacity() + sizeof(std::string) + 2U * sizeof(void*) : 0U) + sizeof(FeedEntry) + 2U * sizeof(void*) + (entry.user ? entry.user->capacity() : 0U);
    bytes += entry.blocks.capacity() * sizeof(FeedBlockChange);
    for (const auto& block : entry.blocks) bytes += BlockValuesBytes(block);
    bytes += entry.columns.capacity() * sizeof(Column);
    for (const auto& column : entry.columns) bytes += column.name.capacity() + column.default_value.capacity();
    return bytes;
}

bool SameValues(const std::optional<std::vector<ColumnValue>>& a,
                const std::optional<std::vector<ColumnValue>>& b) {
    if (a.has_value() != b.has_value()) return false;
    if (!a) return true;
    return SameFeedValues(*a, *b);
}

std::optional<std::int64_t> BlockCoordinate(std::int64_t chunk, std::uint32_t width, std::size_t local) {
    const auto factor = static_cast<std::uint64_t>(width);
    const auto offset = static_cast<std::uint64_t>(local);
    if (chunk >= 0) {
        const auto limit = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) - offset;
        if (static_cast<std::uint64_t>(chunk) > limit / factor)
            return std::nullopt;
        return static_cast<std::int64_t>(static_cast<std::uint64_t>(chunk) * factor + offset);
    }
    // The first block of the chunk containing INT64_MIN can lie below it
    // for a non-power-of-two width; add the local offset before converting.
    const auto magnitude = 0ULL - static_cast<std::uint64_t>(chunk);
    const auto limit = (1ULL << 63U) + offset;
    if (magnitude > limit / factor)
        return std::nullopt;
    return std::bit_cast<std::int64_t>(std::uint64_t{0} - (magnitude * factor - offset));
}

}  // namespace

ScopedWriteUser::ScopedWriteUser(std::string_view user) noexcept : previous_(g_write_user) { g_write_user = user; }
ScopedWriteUser::~ScopedWriteUser() { g_write_user = previous_; }
std::string_view CurrentWriteUser() noexcept { return g_write_user; }

ChangeFeed::Producer::~Producer() {
    while (allocated != nullptr) {
        auto* next_allocated = allocated->allocated_next;
        delete allocated;
        allocated = next_allocated;
    }
}
ChangeFeed::RawWrite* ChangeFeed::Producer::Take(ChangeFeed& feed) {
    // No waiting: a sender reclaiming capacity means the buffer is full.
    // Only this thread pops normally; the sender can pop under this handshake.
    if (reclaiming.exchange(true, std::memory_order_acq_rel)) return nullptr;
    struct ReclaimOwnership {
        std::atomic<bool>& flag;
        ~ReclaimOwnership() { flag.store(false, std::memory_order_release); }
    } ownership{reclaiming};
    auto* write = recycled.load(std::memory_order_acquire);
    while (write != nullptr && !recycled.compare_exchange_weak(write, write->next, std::memory_order_acq_rel)) {}
    if (write == nullptr) {
        if (!feed.Reserve(sizeof(RawWrite))) return nullptr;
        try {
            write = new RawWrite;
        } catch (const std::bad_alloc&) {
            feed.Release(sizeof(RawWrite));
            throw;
        }
        write->allocated_next = allocated;
        allocated = write;
    }
    write->next = nullptr;
    write->used = 0;
    return write;
}
void ChangeFeed::Producer::Recycle(RawWrite* write) noexcept {
    write->next = recycled.load(std::memory_order_relaxed);
    while (!recycled.compare_exchange_weak(write->next, write, std::memory_order_release, std::memory_order_relaxed)) {}
}
void ChangeFeed::DiscardBuffers(RawWrite& write) noexcept {
    std::vector<RawFrame>().swap(write.frames);
    std::vector<std::uint8_t>().swap(write.user);
    Release(write.bytes);
    write.bytes = 0U;
    write.used = 0U;
}
void ChangeFeed::Recycle(Producer& producer, RawWrite* write) noexcept {
    producer.Recycle(write);
}
bool ChangeFeed::ReclaimSpare(std::size_t needed) noexcept {
    bool freed = false;
    for (auto* producer = producers_.load(std::memory_order_seq_cst); producer != nullptr; producer = producer->next) {
        if (producer->reclaiming.exchange(true, std::memory_order_acq_rel)) continue;
        auto* cached = producer->recycled.exchange(nullptr, std::memory_order_acquire);
        while (cached != nullptr) {
            auto* next = cached->next;
            const auto used = bytes_.load(std::memory_order_relaxed);
            if (needed > budget_ || used > budget_ - needed) {
                // Take and this reclamation own the same non-waiting flag.
                // Only they edit the allocation list; this node is detached
                // from the idle stack and cannot be acquired by its writer.
                auto** link = &producer->allocated;
                while (*link != cached) link = &(*link)->allocated_next;
                *link = cached->allocated_next;
                DiscardBuffers(*cached);
                Release(sizeof(RawWrite));
                delete cached;
                freed = true;
            } else producer->Recycle(cached);
            cached = next;
        }
        producer->reclaiming.store(false, std::memory_order_release);
    }
    return freed;
}
void ChangeFeed::Producer::Publish(RawWrite* write) noexcept {
    write->next = incoming.load(std::memory_order_relaxed);
    while (!incoming.compare_exchange_weak(write->next, write, std::memory_order_release, std::memory_order_relaxed)) {}
}
void ChangeFeed::Producer::Collect() noexcept {
    auto* newest = incoming.exchange(nullptr, std::memory_order_acquire);
    RawWrite* oldest = nullptr;
    // Detaching the writer's stack and reversing it preserves its revision order.
    while (newest != nullptr) {
        auto* next = newest->next;
        newest->next = oldest;
        oldest = newest;
        newest = next;
    }
    if (tail != nullptr) tail->next = oldest;
    else pending = oldest;
    while (oldest != nullptr) {
        tail = oldest;
        oldest = oldest->next;
    }
}

ChangeFeed::ChangeFeed(StoreId epoch, std::size_t budget)
    : epoch_(epoch), id_(g_feed_id.fetch_add(1) + 1U), budget_(budget) {
    if (budget == 0U) throw std::invalid_argument("feed buffer bytes must be positive");
}
ChangeFeed::~ChangeFeed() {
    Pause();
    auto* producer = producers_.load();
    while (producer != nullptr) {
        auto* next = producer->next;
        delete producer;
        producer = next;
    }
}
ChangeFeed::Producer& ChangeFeed::ThreadProducer() {
    struct Local { std::uint64_t id; Producer* producer; std::weak_ptr<ChangeFeed> owner; };
    thread_local std::vector<Local> locals;
    for (const auto& local : locals) if (local.id == id_) return *local.producer;
    std::erase_if(locals, [](const Local& local) { return local.owner.expired(); });
    auto producer = std::make_unique<Producer>();
    // Allocate the TLS entry before registration, so a failed allocation
    // cannot leave an unreachable producer in the table.
    locals.push_back({id_, producer.get(), weak_from_this()});
    producer->next = producers_.load(std::memory_order_seq_cst);
    while (!producers_.compare_exchange_weak(producer->next, producer.get(), std::memory_order_seq_cst)) {}
    return *producer.release();
}
bool ChangeFeed::Reserve(std::size_t bytes) noexcept {
    auto used = bytes_.load(std::memory_order_relaxed);
    do {
        if (bytes > budget_ || used > budget_ - bytes) return false;
    } while (!bytes_.compare_exchange_weak(used, used + bytes, std::memory_order_relaxed));
    return true;
}
void ChangeFeed::Release(std::size_t bytes) noexcept { bytes_.fetch_sub(bytes, std::memory_order_relaxed); }
void ChangeFeed::Lost(std::uint64_t revision) noexcept {
    auto until = lost_until_.load(std::memory_order_seq_cst);
    while (until < revision && !lost_until_.compare_exchange_weak(until, revision, std::memory_order_seq_cst)) {}
    Wake();
}
void ChangeFeed::Wake() noexcept {
    if (!wake_.exchange(true, std::memory_order_seq_cst)) wake_.notify_one();
}
void ChangeFeed::RunHook(FeedTestHook::Point point, std::uint64_t version) const {
    if (auto* hook = hook_.load(std::memory_order_acquire)) hook->Run(point, version);
}
std::uint64_t ChangeFeed::Watermark() const {
    if (clock_ == nullptr) return watermark_;
    // Clock MUST precede the registry and every slot load. A new producer
    // missed by this scan cannot take a version below the sampled clock.
    auto lower = clock_->load(std::memory_order_seq_cst);
    RunHook(FeedTestHook::Point::kAfterClock, lower);
    for (auto* p = producers_.load(std::memory_order_seq_cst); p != nullptr; p = p->next) {
        const auto bound = p->bound.load(std::memory_order_seq_cst);
        if (bound != 0U) lower = std::min(lower, bound);
    }
    return lower - 1U;
}
std::uint64_t ChangeFeed::CompletedWatermark() const {
    std::lock_guard lock(mutex_);
    if (error_) std::rethrow_exception(error_);
    return Watermark();
}
void ChangeFeed::Fail(std::exception_ptr error) {
    Pause();
    std::lock_guard lock(mutex_);
    error_ = std::move(error);
    closed_ = true;
    cv_.notify_all();
    Notify();
}
void ChangeFeed::Resume(ChunkStore& store) {
    try {
        ResumeImpl(store);
    } catch (const std::bad_alloc&) {
        Fail(std::current_exception());
    } catch (const std::logic_error&) {
        Fail(std::current_exception());
    } catch (const std::runtime_error&) {
        Fail(std::current_exception());
    }
}
void ChangeFeed::ResumeImpl(ChunkStore& store) {
    std::lock_guard lock(mutex_);
    if (closed_) return;
    const auto clock = store.version_clock_.load(std::memory_order_seq_cst);
    ceiling_ = store.version_clock_ceiling_.load(std::memory_order_acquire);
    if (!geometry_) {
        floor_ = clock - 1U;
        watermark_ = floor_;
    } else if (geometry_->layout().schema().version != store.geometry().layout().schema().version) {
        auto schema = std::make_shared<FeedEntry>();
        schema->kind = FeedEntry::Kind::kSchema;
        schema->position = {epoch_, clock};
        schema->schema_version = store.geometry().layout().schema().version;
        schema->columns = store.geometry().layout().schema().columns;
        schema->protocol_frame = std::make_shared<const std::string>(EncodeFeedEntry(*schema));
        const auto bytes = EntryBytes(*schema);
        // Reserve its revision before serving any lease of the reopened store.
        const auto revision = store.NextChunkVersion();
        if (revision != clock) throw std::logic_error("reopened feed clock changed without a lease");
        Append(std::move(schema), bytes);
        watermark_ = revision;
    }
    geometry_ = store.geometry();
    features_ = store.features();
    clock_ = &store.version_clock_;
    ceiling_clock_ = &store.version_clock_ceiling_;
    stopping_.store(false, std::memory_order_release);
    sender_ = std::thread([this] { Send(); });
    store.feed_.store(this, std::memory_order_seq_cst);
}
void ChangeFeed::Pause() {
    if (sender_.joinable()) {
        stopping_.store(true, std::memory_order_release);
        Wake();
        sender_.join();
    }
    std::lock_guard lock(mutex_);
    if (clock_ != nullptr) watermark_ = Watermark();
    if (ceiling_clock_ != nullptr) ceiling_ = ceiling_clock_->load(std::memory_order_acquire);
    clock_ = nullptr;
    ceiling_clock_ = nullptr;
}
void ChangeFeed::End() {
    Pause();
    std::lock_guard lock(mutex_);
    closed_ = true;
    ClearRing();
    // End runs after the exclusive lease drained and the sender joined.
    auto* producer = producers_.exchange(nullptr, std::memory_order_seq_cst);
    while (producer != nullptr) {
        auto* next = producer->next;
        delete producer;
        producer = next;
    }
    bytes_.store(0U, std::memory_order_relaxed);
    cv_.notify_all();
    Notify();
}
void ChangeFeed::ClearRing() {
    for (const auto& entry : ring_) Release(entry.bytes);
    ring_.clear();
}
void ChangeFeed::Append(std::shared_ptr<const FeedEntry> entry, std::size_t bytes, RawWrite* consumed, bool notify) {
    // Sender/exclusive control only, with mutex_. Writers touch only bytes_.
    while (!Reserve(bytes)) {
        if (ring_.empty()) {
            if (consumed != nullptr && consumed->bytes != 0U) {
                DiscardBuffers(*consumed);
                continue;
            }
            if (ReclaimSpare(bytes)) continue;
            Lost(entry->position.revision);
            return;
        }
        floor_ = ring_.front().entry->position.revision;
        Release(ring_.front().bytes);
        ring_.pop_front();
    }
    try {
        ring_.push_back({std::move(entry), bytes});
    } catch (const std::bad_alloc&) {
        Release(bytes);
        throw;
    }
    if (notify) { cv_.notify_all(); Notify(); }
}
std::shared_ptr<const FeedEntry> ChangeFeed::Decode(const RawWrite& write) const {
    auto entry = std::make_shared<FeedEntry>();
    entry->position = {epoch_, write.revision};
    if (!write.user.empty()) entry->user = std::string(write.user.begin(), write.user.end());
    const auto& geometry = *geometry_;
    const auto base_bytes = EntryBytes(*entry);
    std::size_t value_bytes = 0U;
    for (std::size_t f = 0; f < write.used; ++f) {
        const auto& raw = write.frames[f];
        auto& before = before_scratch_;
        auto& after = after_scratch_;
        const auto& captured_layout = geometry.layout();
        if (!raw.block) {
            before.payload = raw.payload;
            before.presence_bitmap = raw.presence;
        } else {
            before.payload.assign(captured_layout.payload_bytes(), 0U);
            before.presence_bitmap.assign((geometry.ChunkBlockCount() + 7U) / 8U, 0U);
            const auto b = *raw.block;
            before.presence_bitmap[b / 8U] = raw.presence[0];
            std::size_t at = 0U;
            for (const auto& fixed : captured_layout.fixed_columns()) {
                const auto offset = fixed.values + b * fixed.width / 8U;
                const auto count = (b * fixed.width % 8U + fixed.width + 7U) / 8U;
                std::copy_n(raw.payload.data() + at, count, before.payload.data() + offset);
                at += count;
                if (fixed.validity != ChunkLayout::kNoValidity)
                    before.payload[fixed.validity + b / 8U] = raw.payload[at++];
            }
        }
        before.vars = ChunkVars::Decode(raw.vars.data(), raw.vars.size(), geometry.ChunkBlockCount());
        after = before;
        const auto frame = ReplayFeedFrame(raw.wal, geometry, &after, features_);
        if (frame.revision != write.revision || (f != 0U &&
            (frame.schema_version != entry->schema_version || frame.commit_time_ms != entry->commit_time_ms))) {
            throw std::logic_error("frames of a feed change disagree");
        }
        entry->schema_version = frame.schema_version;
        entry->commit_time_ms = frame.commit_time_ms;
        const auto& layout = geometry.LayoutAt(frame.schema_version);
        const bool same_vars = before.vars.Encode() == after.vars.Encode();
        // Full fixed-width chunks often repeat a row. Keep owned value vectors,
        // but decode that row only once; raw equality also preserves float bits.
        const bool memo_rows = !raw.block && layout.fixed_columns().size() == layout.schema().columns.size() &&
            std::all_of(layout.fixed_columns().begin(), layout.fixed_columns().end(),
                [](const auto& column) { return column.width % 8U == 0U; });
        struct RowMemo { bool valid = false; std::size_t block = 0U; std::size_t entry = 0U; };
        RowMemo before_memo, after_memo;
        const auto values = [&](const ChunkState& state, std::size_t block, const RowMemo& memo, bool was) {
            bool same = memo_rows && memo.valid;
            if (same) for (const auto& fixed : layout.fixed_columns()) {
                const auto bytes = fixed.width / 8U;
                if (std::memcmp(state.payload.data() + fixed.values + block * bytes,
                                state.payload.data() + fixed.values + memo.block * bytes, bytes) != 0 ||
                    (fixed.validity != ChunkLayout::kNoValidity &&
                     ((state.payload[fixed.validity + block / 8U] >> (block % 8U)) & 1U) !=
                     ((state.payload[fixed.validity + memo.block / 8U] >> (memo.block % 8U)) & 1U))) {
                    same = false; break;
                }
            }
            if (same) {
                const auto& previous = entry->blocks[memo.entry];
                return was ? *previous.before : *previous.after;
            }
            return DecodeBlockColumns(layout, state.payload, state.vars, block, &value_scratch_);
        };
        const auto first = raw.block.value_or(0U);
        const auto end = raw.block ? first + 1U : geometry.ChunkBlockCount();
        for (std::size_t b = first; b < end; ++b) {
            const auto mask = 1U << (b % 8U);
            const bool was = (before.presence_bitmap[b / 8U] & mask) != 0U;
            const bool now = (after.presence_bitmap[b / 8U] & mask) != 0U;
            if (!was && !now) continue;
            bool same = was == now && same_vars;
            if (same) for (const auto& fixed : layout.fixed_columns()) {
                const auto bit = b * fixed.width;
                const auto offset = fixed.values + bit / 8U;
                const auto count = (bit % 8U + fixed.width + 7U) / 8U;
                if (std::memcmp(before.payload.data() + offset, after.payload.data() + offset, count) != 0 ||
                    (fixed.validity != ChunkLayout::kNoValidity &&
                     ((before.payload[fixed.validity + b / 8U] ^ after.payload[fixed.validity + b / 8U]) & mask) != 0U)) {
                    same = false; break;
                }
            }
            if (same) continue;
            std::optional<std::vector<ColumnValue>> values_before;
            std::optional<std::vector<ColumnValue>> values_after;
            if ((before.presence_bitmap[b / 8U] & (1U << (b % 8U))) != 0U)
                values_before = values(before, b, before_memo, true);
            if ((after.presence_bitmap[b / 8U] & (1U << (b % 8U))) != 0U)
                values_after = values(after, b, after_memo, false);
            if (SameValues(values_before, values_after)) continue;
            const auto width = geometry.config().chunk_width_blocks;
            FeedBlockChange block{raw.coord,
                BlockCoordinate(raw.coord.x, width, b % width),
                BlockCoordinate(raw.coord.y, geometry.config().chunk_height_blocks, b / width),
                std::move(values_before), std::move(values_after),
                static_cast<std::uint32_t>(b % width), static_cast<std::uint32_t>(b / width)};
            value_bytes += BlockValuesBytes(block);
            if (base_bytes + value_bytes + (entry->blocks.size() + 1U) * sizeof(FeedBlockChange) > budget_) return nullptr;
            entry->blocks.push_back(std::move(block));
            if (memo_rows) {
                const auto index = entry->blocks.size() - 1U;
                if (was) before_memo = {true, b, index};
                if (now) after_memo = {true, b, index};
            }
            if (base_bytes + value_bytes + entry->blocks.capacity() * sizeof(FeedBlockChange) > budget_) return nullptr;
        }
    }
    entry->protocol_frame = std::make_shared<const std::string>(EncodeFeedEntry(*entry));
    return entry;
}
void ChangeFeed::Merge(std::uint64_t watermark) {
    {
        std::lock_guard lock(mutex_);
        watermark_ = std::max(watermark_, watermark);
    }
    auto* producers = producers_.load(std::memory_order_seq_cst);
    for (auto* p = producers; p != nullptr; p = p->next) p->Collect();
    auto lost = lost_until_.load(std::memory_order_seq_cst);
    const bool reset = lost != 0U && watermark >= lost;
    if (lost != 0U && !reset) return;
    std::size_t queued_since_notify = 0U;
    for (;;) {
        Producer* selected = nullptr;
        for (auto* p = producers; p != nullptr; p = p->next) {
            if (p->pending != nullptr && p->pending->revision <= watermark &&
                (selected == nullptr || p->pending->revision < selected->pending->revision)) selected = p;
        }
        if (selected == nullptr) break;
        auto* write = selected->pending;
        selected->pending = write->next;
        if (selected->pending == nullptr) selected->tail = nullptr;
        bool notify_batch = false;
        try {
            if (!reset) {
                auto entry = Decode(*write);
                if (!entry) Lost(write->revision);
                else {
                    const auto bytes = EntryBytes(*entry);
                    std::lock_guard lock(mutex_);
                    if (!ring_.empty() && entry->position.revision <= ring_.back().entry->position.revision) {
                        throw std::logic_error("feed queue revisions are out of order");
                    }
                    Append(std::move(entry), bytes, write, false);
                    // A reader can start its next write as soon as mutex_ is
                    // released. Return this capacity before publishing it.
                    Recycle(*selected, write);
                    write = nullptr;
                    // Ring publication is immediate, but waking readers for
                    // every frame makes the I/O thread park between frames.
                    // Bound notifications by the same 64-entry batch as I/O,
                    // and always notify the remainder at the end of this scan.
                    if (++queued_since_notify == 64U) {
                        notify_batch = true;
                        queued_since_notify = 0U;
                    }
                }
            }
        } catch (const std::bad_alloc&) {
            Lost(write->revision);
        }
        if (write != nullptr) Recycle(*selected, write);
        if (notify_batch) {
            std::lock_guard lock(mutex_);
            cv_.notify_all();
            Notify();
        }
    }
    std::lock_guard lock(mutex_);
    if (reset) {
        // Drain the discarded prefix before readers can resume at its end.
        // Idle headers and buffers must not keep a small feed full forever.
        ClearRing();
        (void)ReclaimSpare(budget_);
        floor_ = watermark;
        ++reset_;
        lost_until_.compare_exchange_strong(lost, 0U, std::memory_order_seq_cst);
    }
    cv_.notify_all();
    Notify();
}
void ChangeFeed::Send() {
    try {
        for (;;) {
            // Consume the signal before sampling the clock and queues. Writes
            // published earlier are visible to this scan; later writes leave
            // the signal set, so waiting cannot miss them. Only the first
            // writer of a batch needs to wake a sleeping sender.
            wake_.exchange(false, std::memory_order_seq_cst);
            const auto watermark = Watermark();
            RunHook(FeedTestHook::Point::kBeforeMerge, watermark);
            Merge(watermark);
            if (stopping_.load(std::memory_order_acquire)) {
                // Exclusive lease: no new producer or write. Drain once more
                // in case the stop arrived after the preceding clock sample.
                Merge(Watermark());
                return;
            }
            wake_.wait(false, std::memory_order_acquire);
        }
    } catch (const std::logic_error&) {
        std::lock_guard lock(mutex_);
        error_ = std::current_exception();
        closed_ = true;
        cv_.notify_all();
        Notify();
    } catch (const std::runtime_error&) {
        std::lock_guard lock(mutex_);
        error_ = std::current_exception();
        closed_ = true;
        cv_.notify_all();
        Notify();
    }
}

void ChangeFeed::NotifyDurableWatermark() {
    std::lock_guard lock(mutex_);
    Notify();
}

void ChangeFeed::Notify() {
    // Publication and registration hold mutex_. Callbacks only signal I/O.
    std::erase_if(notifications_, [](const auto& callback) { return callback.expired(); });
    for (const auto& weak : notifications_) if (auto callback = weak.lock()) (*callback)();
}
std::size_t FeedSubscription::buffer_bytes() const noexcept { return feed_->budget(); }
FeedPosition FeedSubscription::Resync() {
    std::lock_guard lock(feed_->mutex_);
    position_ = {feed_->epoch_, std::max(feed_->watermark_, feed_->Watermark())};
    reset_ = feed_->reset_;
    invalid_ = false;
    return position_;
}

FeedSubscription::FeedSubscription(std::shared_ptr<ChangeFeed> feed, std::weak_ptr<Table> table, const FeedOptions& options)
    : feed_(std::move(feed)), table_(std::move(table)), area_(options.area) {}
FeedSubscription::~FeedSubscription() {
    if (auto table = table_.lock()) table->ReleaseFeed(feed_);
}
std::shared_ptr<const FeedEntry> FeedSubscription::Next(std::chrono::milliseconds timeout) {
    return feed_->Next(*this, timeout);
}
std::unique_ptr<FeedSubscription> ChangeFeed::Subscribe(std::weak_ptr<Table> table, const FeedOptions& options) {
    auto result = std::unique_ptr<FeedSubscription>(new FeedSubscription(shared_from_this(), {}, options));
    std::lock_guard lock(mutex_);
    if (error_) std::rethrow_exception(error_);
    result->position_ = options.after.value_or(FeedPosition{epoch_, Watermark()});
    result->reset_ = reset_;
    result->invalid_ = (options.after.has_value() && !subscribed_) || result->position_.epoch != epoch_ || result->position_.revision >= ceiling_clock_->load(std::memory_order_acquire) ||
        result->position_.revision < floor_ || result->position_.revision > Watermark();
    // Revisions have gaps: a supplied position must name a retained entry or
    // the feed's initial/eviction boundary, rather than guess a missing change.
    if (options.after && result->position_.revision != floor_ &&
        std::none_of(ring_.begin(), ring_.end(), [&](const Retained& r) {
            return r.entry->position == result->position_;
        })) result->invalid_ = true;
    if (options.notify) {
        result->notify_ = std::make_shared<const std::function<void()>>(options.notify);
        notifications_.push_back(result->notify_);
    }
    subscribed_ = true;
    result->table_ = std::move(table);
    return result;
}
std::shared_ptr<const FeedEntry> ChangeFeed::Next(FeedSubscription& sub, std::chrono::milliseconds timeout) {
    if (sub.ended_) return nullptr;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        std::unique_lock lock(mutex_);
        const auto ready = [&] {
            return error_ || closed_ || sub.invalid_ || sub.reset_ != reset_ || sub.position_.revision < floor_ ||
                (lost_until_.load(std::memory_order_seq_cst) == 0U && !ring_.empty() &&
                 ring_.back().entry->position.revision > sub.position_.revision);
        };
        if (!ready() && !cv_.wait_until(lock, deadline, ready)) return nullptr;
        if (error_) std::rethrow_exception(error_);
        if (closed_) {
            auto entry = std::make_shared<FeedEntry>();
            entry->kind = FeedEntry::Kind::kEnd;
            entry->position = {epoch_, watermark_};
            sub.position_ = entry->position;
            sub.ended_ = true;
            return entry;
        }
        if (sub.invalid_ || sub.reset_ != reset_ || sub.position_.revision < floor_) {
            auto entry = std::make_shared<FeedEntry>();
            entry->kind = FeedEntry::Kind::kResync;
            entry->position = {epoch_, std::max(watermark_, Watermark())};
            sub.position_ = entry->position;
            sub.reset_ = reset_;
            sub.invalid_ = false;
            return entry;
        }
        if (lost_until_.load(std::memory_order_seq_cst) != 0U) continue;
        const auto next = std::upper_bound(ring_.begin(), ring_.end(), sub.position_.revision,
            [](std::uint64_t revision, const Retained& retained) {
                return revision < retained.entry->position.revision;
            });
        if (next == ring_.end()) return nullptr;
        auto entry = next->entry;
        // Take only an immutable handle under the ring lock. Filtering and
        // copying a large transaction belongs to this reader alone.
        lock.unlock();
        if (entry->kind != FeedEntry::Kind::kChange || !sub.area_) {
            sub.position_ = entry->position;
            return entry;
        }
        const auto inside = [&](const FeedBlockChange& block) {
            return block.chunk.x >= sub.area_->first.x && block.chunk.x <= sub.area_->last.x &&
                   block.chunk.y >= sub.area_->first.y && block.chunk.y <= sub.area_->last.y;
        };
        if (entry->blocks.empty()) {
            sub.position_ = entry->position;
            continue;
        }
        if (std::all_of(entry->blocks.begin(), entry->blocks.end(), inside)) {
            sub.position_ = entry->position;
            return entry;
        }
        auto filtered = std::make_shared<FeedEntry>(*entry);
        std::erase_if(filtered->blocks, [&](const FeedBlockChange& block) { return !inside(block); });
        sub.position_ = entry->position;
        if (!filtered->blocks.empty()) {
            filtered->protocol_frame = std::make_shared<const std::string>(EncodeFeedEntry(*filtered));
            return filtered;
        }
    }
}

void ChunkStore::RegisterWriteCompletion(WriteCompletion& completion) {
    std::lock_guard lock(write_completion_mutex_);
    completion.bound = version_clock_.load(std::memory_order_seq_cst);
    completion.next = write_completions_;
    if (write_completions_ != nullptr) write_completions_->previous = &completion;
    write_completions_ = &completion;
}
void ChunkStore::UnregisterWriteCompletion(WriteCompletion& completion) noexcept {
    {
        std::lock_guard lock(write_completion_mutex_);
        if (completion.previous != nullptr) completion.previous->next = completion.next;
        else write_completions_ = completion.next;
        if (completion.next != nullptr) completion.next->previous = completion.previous;
    }
    write_completion_cv_.notify_all();
}

void FeedWriteGuard::Start(ChangeFeed& feed, std::atomic<std::uint64_t>& clock) {
    auto& producer = feed.ThreadProducer();
    if (producer.context.active || producer.bound.load(std::memory_order_seq_cst) != 0U) {
        throw std::logic_error("nested write to a feed on one thread");
    }
    // Take can allocate. Publish the slot only after it succeeds: a throwing
    // constructor has no destructor to clear a slot.
    auto* write = producer.Take(feed);
    state_ = &producer.context;
    *state_ = {&feed, &producer, write, 0, false, true};
    struct ConstructionCleanup {
        FeedWriteGuard& guard;
        bool armed = true;
        ~ConstructionCleanup() { if (armed) guard.Finish(); }
    } cleanup{*this};
    const auto lower = clock.load(std::memory_order_seq_cst);
    state_->feed->RunHook(FeedTestHook::Point::kBeforeSlot, lower);
    state_->producer->bound.store(lower, std::memory_order_seq_cst);
    if (state_->write == nullptr) state_->dropped = true;
    else (void)CopyBuffer(state_->write->user, {reinterpret_cast<const std::uint8_t*>(g_write_user.data()), g_write_user.size()});
    cleanup.armed = false;
}

bool FeedWriteGuard::Charge(std::size_t bytes) noexcept {
    if (state_->feed->Reserve(bytes)) {
        state_->write->bytes += bytes;
        return true;
    }
    state_->dropped = true;
    return false;
}
bool FeedWriteGuard::ResizeBuffer(std::vector<std::uint8_t>& target, std::size_t size) {
    if (size > target.capacity()) {
        const auto growth = size - target.capacity();
        if (!Charge(growth)) return false;
        GrowthReservation reservation{*this, growth};
        target.reserve(size);
        reservation.allocated = true;
        if (target.capacity() > size && !Charge(target.capacity() - size)) {
            state_->feed->DiscardBuffers(*state_->write);
            return false;
        }
    }
    target.resize(size);

    return true;
}
bool FeedWriteGuard::CopyBuffer(std::vector<std::uint8_t>& target, std::span<const std::uint8_t> source) {
    if (!ResizeBuffer(target, source.size())) return false;
    if (!source.empty()) std::copy(source.begin(), source.end(), target.begin());
    return true;
}
ChangeFeed::RawFrame* FeedWriteGuard::NewFrame(ChunkCoord coord) {
    if (state_->write->used == state_->write->frames.size()) {
        if (state_->write->frames.size() == state_->write->frames.capacity()) {
            const auto count = state_->write->used + 1U;
            const auto growth = sizeof(ChangeFeed::RawFrame) * (count - state_->write->frames.capacity());
            if (!Charge(growth)) return nullptr;
            GrowthReservation reservation{*this, growth};
            state_->write->frames.reserve(count);
            reservation.allocated = true;
            if (state_->write->frames.capacity() > count &&
                !Charge(sizeof(ChangeFeed::RawFrame) * (state_->write->frames.capacity() - count))) {
                state_->feed->DiscardBuffers(*state_->write);
                return nullptr;
            }
        }
        state_->write->frames.emplace_back();
    }
    auto& frame = state_->write->frames[state_->write->used++];
    frame.coord = coord;
    return &frame;
}
void FeedWriteGuard::CopyBefore(ChunkCoord coord, const std::vector<std::uint8_t>& payload,
                               const std::vector<std::uint8_t>& presence, const std::vector<std::uint8_t>& vars) {
    auto* captured = NewFrame(coord);
    if (!captured) return;
    auto& frame = *captured;
    frame.block.reset();
    if (!CopyBuffer(frame.payload, payload)) return;
    if (!CopyBuffer(frame.presence, presence)) return;
    (void)CopyBuffer(frame.vars, vars);
}
void FeedWriteGuard::CopyBlock(ChunkCoord coord, const std::vector<std::uint8_t>& payload,
                              const std::vector<std::uint8_t>& presence, const ChunkVars& vars,
                              const ChunkLayout& layout, std::size_t block) {
    auto* captured = NewFrame(coord);
    if (!captured) return;
    auto& frame = *captured;
    frame.block = block;
    std::size_t row_bytes = 0U;
    for (const auto& fixed : layout.fixed_columns())
        row_bytes += (block * fixed.width % 8U + fixed.width + 7U) / 8U +
                     (fixed.validity != ChunkLayout::kNoValidity ? 1U : 0U);
    if (!ResizeBuffer(frame.payload, row_bytes) || !ResizeBuffer(frame.presence, 1U)) return;
    frame.presence[0] = presence[block / 8U] & (1U << (block % 8U));
    std::size_t at = 0U;
    for (const auto& fixed : layout.fixed_columns()) {
        const auto offset = fixed.values + block * fixed.width / 8U;
        const auto count = (block * fixed.width % 8U + fixed.width + 7U) / 8U;
        std::copy_n(payload.data() + offset, count, frame.payload.data() + at);
        at += count;
        if (fixed.validity != ChunkLayout::kNoValidity)
            frame.payload[at++] = payload[fixed.validity + block / 8U] & (1U << (block % 8U));
    }
    std::size_t var_bytes = 0U;
    for (const auto entry : vars) if (entry.key.block_index == block) var_bytes += kVarEntryHeaderBytes + entry.value.size();
    if (!ResizeBuffer(frame.vars, var_bytes)) return;
    at = 0U;
    // Iteration preserves encoded key order, even for schemas with reordered ids.
    for (const auto entry : vars) if (entry.key.block_index == block) {
        const auto count = kVarEntryHeaderBytes + entry.value.size();
        std::copy_n(entry.value.data() - kVarEntryHeaderBytes, count, frame.vars.data() + at);
        at += count;
    }
}

void FeedWriteGuard::CopyFrame(const std::vector<std::uint8_t>& batch, std::size_t frame_bytes) {
    auto& frame = state_->write->frames[state_->write->used - 1U];
    (void)CopyBuffer(frame.wal, {batch.data() + batch.size() - frame_bytes, frame_bytes});
}

void FeedWriteGuard::Publish() noexcept {
    if (state_->dropped) state_->feed->Lost(state_->revision);
    else {
        state_->write->revision = state_->revision;
        state_->producer->Publish(state_->write);
        state_->write = nullptr;
    }
}
void FeedWriteGuard::Finish() noexcept {
    if (state_->write != nullptr) {
        state_->feed->Recycle(*state_->producer, state_->write);
        state_->write = nullptr;
    }
    state_->producer->bound.store(0U, std::memory_order_seq_cst);
    state_->active = false;
    // A true SC sample follows this producer's cleared SC bound. Either it
    // precedes the sender's SC clear (and its next watermark includes us),
    // or another writer has already signalled the next scan. Avoid a shared
    // read-modify-write when the sender is already scheduled.
    if (!state_->feed->wake_.load(std::memory_order_seq_cst)) state_->feed->Wake();
}

void FeedTestAccess::SetHook(Table& table, FeedTestHook* hook) {
    std::lock_guard lock(table.mutex_);
    table.feed_->hook_.store(hook, std::memory_order_release);
    table.feed_->Wake();
}
std::uint64_t FeedTestAccess::Watermark(Table& table) {
    std::lock_guard table_lock(table.mutex_);
    std::lock_guard feed_lock(table.feed_->mutex_);
    return table.feed_->Watermark();
}
std::size_t FeedTestAccess::BufferedBytes(Table& table) {
    std::lock_guard lock(table.mutex_);
    return table.feed_->bytes_.load();
}
}  // namespace chunkdb
