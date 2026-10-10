#pragma once

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>

#include "change_feed.hpp"
#include "chunkdb/bit_codec.hpp"
#include "chunkdb/table_catalog.hpp"
#include "txn_test_utils.hpp"

namespace chunkdb::feed_test {
using namespace std::chrono_literals;

inline CatalogConfig Config(const std::filesystem::path& dir) {
    auto store = txn_test::Config(dir, DurabilityMode::kRelaxed);
    store.checkpoint_update_interval = 1'000'000;
    store.checkpoint_wal_bytes = 1ULL << 30U;
    return CatalogConfigFromStoreConfig(store);
}
inline std::shared_ptr<const FeedEntry> Next(FeedSubscription& sub) {
    auto entry = sub.Next(10s);
    assert(entry && "feed did not deliver an entry");
    return entry;
}
inline std::string Bits(std::uint32_t value) {
    std::string result(32, '0');
    for (std::size_t i = 0; i < 32U; ++i) if ((value & (1U << i)) != 0U) result[i] = '1';
    return result;
}
inline std::optional<std::vector<ColumnValue>> Block(const ChunkState& state, std::size_t b) {
    if ((state.presence_bitmap[b / 8U] & (1U << (b % 8U))) == 0U) return std::nullopt;
    return std::vector<ColumnValue>{BitsValue{BitCodec::ExtractBits(state.payload, b * 32U, 32U)}};
}
inline std::vector<FeedBlockChange> Diff(ChunkCoord coord, const ChunkState& before, const ChunkState& after) {
    std::vector<FeedBlockChange> changes;
    for (std::size_t b = 0; b < 16U; ++b) {
        auto old_value = Block(before, b);
        auto new_value = Block(after, b);
        if (old_value != new_value) changes.push_back({coord, coord.x * 4 + static_cast<std::int64_t>(b % 4),
            coord.y * 4 + static_cast<std::int64_t>(b / 4), std::move(old_value), std::move(new_value), static_cast<std::uint32_t>(b % 4), static_cast<std::uint32_t>(b / 4)});
    }
    return changes;
}
inline void EqualBlocks(const std::vector<FeedBlockChange>& expected, const std::vector<FeedBlockChange>& actual) {
    assert(actual.size() == expected.size());
    for (const auto& want : expected) {
        const auto found = std::find_if(actual.begin(), actual.end(), [&](const auto& block) {
            return block.chunk == want.chunk && block.local_x == want.local_x && block.local_y == want.local_y;
        });
        assert(found != actual.end());
        assert(found->x == want.x && found->y == want.y && found->before == want.before && found->after == want.after);
    }
}

// A deterministic barrier in a production hook, with no sleeps or polling.
class Pause : public FeedTestHook {
  public:
    explicit Pause(Point point, bool fail = false) : point_(point), fail_(fail) {}
    void Run(Point point, std::uint64_t revision) override {
        if (point != point_) return;
        std::unique_lock lock(mutex_);
        if (entered_) return;
        entered_ = true;
        revision_ = revision;
        cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
        if (fail_) throw std::runtime_error("injected feed writer failure after its version");
    }
    std::uint64_t Wait() {
        std::unique_lock lock(mutex_);
        assert(cv_.wait_for(lock, 10s, [&] { return entered_; }));
        return revision_;
    }
    void Release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }
  private:
    Point point_;
    bool fail_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_ = false;
    bool released_ = false;
    std::uint64_t revision_ = 0;
};
}  // namespace chunkdb::feed_test
