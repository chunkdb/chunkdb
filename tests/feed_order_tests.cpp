#include <array>
#include <barrier>
#include <cassert>
#include <map>
#include <thread>

#include "feed_test_utils.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::feed_test;
using chunkdb::test::ScopedTempDir;

void ConcurrentModel() {
    ScopedTempDir dir("chunkdb-feed-order");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto feed = table->SubscribeFeed();
    auto area = table->SubscribeFeed({.area = FeedArea{{0, 0}, {0, 3}}});
    constexpr std::size_t writers = 6;
    constexpr std::size_t rounds = 48;
    struct Expected { std::uint64_t version; std::vector<FeedBlockChange> blocks; std::string user; };
    std::array<std::vector<Expected>, writers> expected;
    std::barrier start(static_cast<std::ptrdiff_t>(writers));
    std::vector<std::thread> threads;
    for (std::size_t t = 0; t < writers; ++t) {
        threads.emplace_back([&, t] {
            const auto user = "writer-" + std::to_string(t);
            ScopedWriteUser identity(user);
            auto lease = table->Acquire();
            auto& store = lease->store();
            const ChunkCoord a{static_cast<std::int64_t>(t) * 2, static_cast<std::int64_t>(t % 4)};
            const ChunkCoord b{a.x + 1, a.y};
            (void)store.ReadChunkState(a.x, a.y);
            (void)store.ReadChunkState(b.x, b.y);
            start.arrive_and_wait();
            for (std::size_t i = 0; i < rounds; ++i) {
                const auto old_a = store.ReadChunkState(a.x, a.y);
                const auto old_b = store.ReadChunkState(b.x, b.y);
                const auto value = static_cast<std::uint32_t>(i + 1);
                if (i % 8 == 0) store.SetBlockBits(a.x * 4, a.y * 4, Bits(value));
                else if (i % 8 == 1) {
                    auto state = old_a;
                    txn_test::SetCounter(&state, value);
                    (void)store.SetChunkStateBytes(a.x, a.y, state.payload, state.presence_bitmap);
                } else if (i % 8 == 2) {
                    auto state = old_a;
                    txn_test::SetCounter(&state, value);
                    assert(store.WriteChunkState(a.x, a.y, std::move(state), old_a.version).ok);
                } else if (i % 8 == 3) {
                    auto snapshot = store.BeginTxnSnapshot(txn_test::kTxnDuration);
                    auto sa = store.ReadChunkStateAt(*snapshot, a.x, a.y);
                    auto sb = store.ReadChunkStateAt(*snapshot, b.x, b.y);
                    txn_test::SetCounter(&sa, value);
                    txn_test::SetCounter(&sb, value + 1000U);
                    (void)store.CommitTransaction(*snapshot, {}, {{a, std::move(sa)}, {b, std::move(sb)}});
                } else if (i % 8 == 4) {
                    assert(store.ApplyChunkBatch(a.x, a.y, true, old_a.version,
                        {{true, a.x * 4, a.y * 4, Bits(value)}, {true, a.x * 4 + 1, a.y * 4, Bits(value + 100)}}).ok);
                } else if (i % 8 == 5) {
                    auto state = old_a;
                    txn_test::SetCounter(&state, value);
                    assert(store.CasChunkStateBytes(a.x, a.y, old_a.version, state.payload, state.presence_bitmap).ok);
                } else if (i % 8 == 6) store.SetBlock(a.x * 4, a.y * 4, {{"bits", BitsValue{Bits(value)}}});
                else {
                    assert(store.UnsetBlock(a.x * 4, a.y * 4, old_a.version).ok);
                }
                auto after_a = store.ReadChunkState(a.x, a.y);
                auto after_b = store.ReadChunkState(b.x, b.y);
                auto blocks = Diff(a, old_a, after_a);
                const auto blocks_b = Diff(b, old_b, after_b);
                blocks.insert(blocks.end(), blocks_b.begin(), blocks_b.end());
                expected[t].push_back({after_a.version, std::move(blocks), user});
                // Rejected CAS and a no-op must take no visible revision.
                assert(!store.CasChunkStateBytes(a.x, a.y, old_a.version, old_a.payload, old_a.presence_bitmap).ok);
                assert(store.SetChunkStateBytes(a.x, a.y, after_a.payload, after_a.presence_bitmap) == after_a.version);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    std::map<std::uint64_t, Expected> model;
    std::size_t inside_count = 0;
    for (const auto& list : expected) for (const auto& change : list) {
        assert(model.emplace(change.version, change).second);
        if (std::any_of(change.blocks.begin(), change.blocks.end(), [](const auto& b) { return b.chunk.x == 0; })) ++inside_count;
    }
    auto previous = feed->position().revision;
    for (const auto& [revision, expected_change] : model) {
        const auto event = Next(*feed);
        assert(event->kind == FeedEntry::Kind::kChange && event->position.epoch == table->store_id());
        assert(event->position.revision == revision && revision > previous);
        assert(event->commit_time_ms != 0U && event->schema_version == 1U);
        assert(event->user == expected_change.user);
        EqualBlocks(expected_change.blocks, event->blocks);
        previous = revision;
    }
    for (std::size_t i = 0; i < inside_count; ++i) {
        const auto event = Next(*area);
        const auto& all = model.at(event->position.revision).blocks;
        std::vector<FeedBlockChange> inside;
        for (const auto& b : all) if (b.chunk.x == 0) inside.push_back(b);
        EqualBlocks(inside, event->blocks);
    }
    assert(!feed->Next() && !area->Next());
}

void FailedRollback() {
    ScopedTempDir dir("chunkdb-feed-vars");
    TableCatalog catalog(Config(dir.path()));
    auto table = catalog.Create("rollback", {1, 1, 2, 2, 32}, catalog.default_options());
    auto feed = table->SubscribeFeed();
    auto lease = table->Acquire();
    auto& store = lease->store();
    const auto before = store.ReadChunkState(0, 0);
    {
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_INTENT_PUBLISH_ONCE", "1");
        auto state = before;
        txn_test::SetCounter(&state, 55);
        assert(txn_test::Throws([&] { (void)store.WriteChunkState(0, 0, state, before.version); }));
    }
    const auto same = store.ReadChunkState(0, 0);
    assert(same.payload == before.payload && same.version == before.version);
    store.SetBlockBits(0, 0, Bits(9));
    auto event = Next(*feed);
    assert(event->kind == FeedEntry::Kind::kChange && event->blocks.size() == 1U && !event->user);
    assert(event->position.revision == store.GetChunkVersion(0, 0));
    assert(!event->blocks[0].before && event->blocks[0].after == std::vector<ColumnValue>{BitsValue{Bits(9)}});
    assert(!feed->Next());
}
}  // namespace
int main() {
    ConcurrentModel();
    FailedRollback();
}
