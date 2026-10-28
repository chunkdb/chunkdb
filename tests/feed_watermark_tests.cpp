#include <cassert>
#include <thread>

#include "feed_test_utils.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::feed_test;
using chunkdb::test::ScopedTempDir;

void NestedGuardCannotReplaceWriterContext() {
    ScopedTempDir dir("chunkdb-feed-nested-guard");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto feed = table->SubscribeFeed();
    struct Nested : FeedTestHook {
        ChunkStore* store = nullptr;
        bool rejected = false;
        void Run(Point point, std::uint64_t) override {
            if (point != Point::kBeforeSlot) return;
            try {
                FeedWriteGuard nested(*store);
                assert(false && "nested guard replaced the pending writer");
            } catch (const std::logic_error&) { rejected = true; }
        }
    } hook;
    FeedTestAccess::SetHook(*table, &hook);
    std::uint64_t revision;
    {
        auto lease = table->Acquire();
        hook.store = &lease->store();
        hook.store->SetBlockBits(0, 0, Bits(1));
        revision = hook.store->GetChunkVersion(0, 0);
    }
    assert(hook.rejected);
    const auto entry = Next(*feed);
    assert(entry->kind == FeedEntry::Kind::kChange && entry->position.revision == revision);
    assert(entry->blocks[0].after == std::vector<ColumnValue>{BitsValue{Bits(1)}});
    FeedTestAccess::SetHook(*table, nullptr);
    table->StopFeed();
}

void UnwatchedCompletedFrontier(bool fail) {
    ScopedTempDir dir("chunkdb-unwatched-frontier");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    assert(!FeedTestAccess::Capturing(*table));
    {
        auto lease = table->Acquire();
        (void)lease->store().ReadChunkState(0, 0);
        (void)lease->store().ReadChunkState(1, 0);
    }
    Pause pause(FeedTestHook::Point::kAfterVersion, fail);
    FeedTestAccess::SetWriteHook(*table, &pause);
    bool failed = false;
    std::thread pending([&] {
        auto lease = table->Acquire();
        try { lease->store().SetBlockBits(0, 0, Bits(1)); }
        catch (const std::runtime_error&) { failed = true; }
    });
    const auto reserved = pause.Wait();
    std::uint64_t committed = 0;
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(4, 0, Bits(2));
        committed = lease->store().GetChunkVersion(1, 0);
        assert(committed > reserved && *lease->store().CompletedRevision() < reserved);
    }
    pause.Release();
    pending.join();
    assert(failed == fail);
    FeedTestAccess::SetWriteHook(*table, nullptr);
    {
        auto lease = table->Acquire();
        assert(*lease->store().CompletedRevision() >= committed);
    }
    assert(!FeedTestAccess::Capturing(*table));
}

void HoldWriter(unsigned path, bool fail) {
    ScopedTempDir dir("chunkdb-feed-watermark");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    {
        auto lease = table->Acquire();
        (void)lease->store().ReadChunkState(0, 0);
        (void)lease->store().ReadChunkState(1, 0);
    }
    auto feed = table->SubscribeFeed();
    Pause pause(FeedTestHook::Point::kAfterVersion, fail);
    FeedTestAccess::SetHook(*table, &pause);
    bool failed = false;
    std::thread low([&] {
        auto lease = table->Acquire();
        auto& store = lease->store();
        try {
            const auto state = store.ReadChunkState(0, 0);
            auto next = state;
            txn_test::SetCounter(&next, 1);
            if (path == 0) store.SetBlockBits(0, 0, Bits(1));
            else if (path == 1) store.SetBlock(0, 0, {{"bits", BitsValue{Bits(1)}}});
            else if (path == 2) (void)store.SetChunkStateBytes(0, 0, next.payload, next.presence_bitmap);
            else if (path == 3) (void)store.WriteChunkState(0, 0, next, state.version);
            else if (path == 4) {
                auto snapshot = store.BeginTxnSnapshot(txn_test::kTxnDuration);
                (void)store.CommitTransaction(*snapshot, {}, {{{0, 0}, std::move(next)}});
            } else if (path == 5) (void)store.ApplyChunkBatch(0, 0, false, 0, {{true, 0, 0, Bits(1)}});
            else (void)store.CasChunkStateBytes(0, 0, state.version, next.payload, next.presence_bitmap);
        } catch (const std::runtime_error&) { failed = true; }
    });
    const auto reserved = pause.Wait();
    std::uint64_t fast_revision = 0;
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(4, 0, Bits(2));
        fast_revision = lease->store().GetChunkVersion(1, 0);
    }
    assert(fast_revision > reserved);
    assert(FeedTestAccess::Watermark(*table) < reserved);
    {
        auto lease = table->Acquire();
        assert(*lease->store().CompletedRevision() < reserved);
    }
    assert(!feed->Next(50ms) && "sender passed an unfinished writer");
    pause.Release();
    low.join();
    assert(failed == fail);
    {
        auto lease = table->Acquire();
        assert(*lease->store().CompletedRevision() >= fast_revision);
    }
    FeedTestAccess::SetHook(*table, nullptr);
    if (!fail) assert(Next(*feed)->position.revision == reserved);
    assert(Next(*feed)->position.revision == fast_revision);
    assert(!feed->Next());
    table->StopFeed();
}

void ProgressWaitsForSenderAndReader() {
    ScopedTempDir dir("chunkdb-feed-drained-progress");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto feed = table->SubscribeFeed();
    struct Schedule : FeedTestHook {
        Pause writer{Point::kAfterVersion};
        Pause sender{Point::kBeforeMergeDrain};
        std::atomic<std::uint64_t> target{UINT64_MAX};
        void Run(Point point, std::uint64_t revision) override {
            if (point == Point::kAfterVersion) writer.Run(point, revision);
            if (point == Point::kBeforeMergeDrain && revision >= target.load()) sender.Run(point, revision);
        }
    } schedule;
    FeedTestAccess::SetHook(*table, &schedule);
    std::thread pending([&] {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, Bits(1));
    });
    const auto revision = schedule.writer.Wait();
    schedule.target.store(revision);
    schedule.writer.Release();
    pending.join();
    (void)schedule.sender.Wait();
    // The completed clock and early Merge watermark already include this
    // revision; the sender has not put its mutation in the ring yet.
    const auto before = feed->Progress();
    assert(before && before->revision < revision);
    assert(!feed->Next());
    schedule.sender.Release();
    // Signal delivery proves admission before asserting the reader barrier.
    auto other = table->SubscribeFeed({.after = *before});
    assert(Next(*other)->position.revision == revision);
    assert(feed->Progress()->revision < revision);
    assert(Next(*feed)->position.revision == revision);
    assert(feed->Progress()->revision >= revision);
    FeedTestAccess::SetHook(*table, nullptr);
    table->StopFeed();
}

void ResumeAtReservationGap() {
    ScopedTempDir dir("chunkdb-feed-resume-gap");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto feed = table->SubscribeFeed();
    std::uint64_t first = 0, gap = 0, later = 0;
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, Bits(1));
        first = lease->store().GetChunkVersion(0, 0);
    }
    assert(Next(*feed)->position.revision == first); // Ring admission signal.
    {
        auto lease = table->Acquire();
        gap = lease->store().GetChunkVersion(1, 0); // Cold cache reservation, no mutation.
        assert(!lease->store().GetBlockWithVersion(4, 0).version);
        lease->store().SetBlockBits(0, 0, Bits(2));
        later = lease->store().GetChunkVersion(0, 0);
    }
    assert(first < gap && gap < later);
    assert(Next(*feed)->position.revision == later); // The entire prefix is retained.
    const auto epoch = feed->position().epoch;
    auto resumed = table->SubscribeFeed({.after = FeedPosition{epoch, gap}});
    const auto entry = Next(*resumed);
    assert(entry->kind == FeedEntry::Kind::kChange && entry->position.revision == later);
    assert(!resumed->Next());
    table->StopFeed();
}

void QueueOrder() {
    ScopedTempDir dir("chunkdb-feed-queue-order");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto feed = table->SubscribeFeed();
    Pause pause(FeedTestHook::Point::kBeforeMerge);
    FeedTestAccess::SetHook(*table, &pause);
    (void)pause.Wait();
    std::uint64_t first = 0;
    std::uint64_t second = 0;
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, Bits(1));
        first = lease->store().GetChunkVersion(0, 0);
        lease->store().SetBlockBits(0, 0, Bits(2));
        second = lease->store().GetChunkVersion(0, 0);
    }
    pause.Release();
    assert(Next(*feed)->position.revision == first);
    const auto event = Next(*feed);
    assert(event->position.revision == second);
    assert(event->blocks[0].before == std::vector<ColumnValue>{BitsValue{Bits(1)}});
    FeedTestAccess::SetHook(*table, nullptr);
    table->StopFeed();
}

void QueuedOverflow() {
    ScopedTempDir dir("chunkdb-feed-overflow");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto slow = table->SubscribeFeed({.buffer_bytes = 1024U});
    auto other = table->SubscribeFeed();
    Pause pause(FeedTestHook::Point::kBeforeMerge);
    FeedTestAccess::SetHook(*table, &pause);
    (void)pause.Wait();
    std::uint64_t last = 0;
    {
        auto lease = table->Acquire();
        for (std::uint32_t i = 1; i < 100; ++i) {
            lease->store().SetBlockBits(0, 0, Bits(i));
            assert(FeedTestAccess::BufferedBytes(*table) <= 1024U);
        }
        last = lease->store().GetChunkVersion(0, 0);
    }
    // The sender was held throughout: writers completed without its help.
    pause.Release();
    for (auto* subscription : {slow.get(), other.get()}) {
        auto event = Next(*subscription);
        assert(event->kind == FeedEntry::Kind::kResync && event->position.revision == last);
    }
    FeedTestAccess::SetHook(*table, nullptr);
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, Bits(100));
    }
    assert(Next(*slow)->kind == FeedEntry::Kind::kChange);
    assert(Next(*other)->kind == FeedEntry::Kind::kChange);
    table->StopFeed();
}
void OverflowWaitsForWriter(bool fail) {
    ScopedTempDir dir("chunkdb-feed-overflow-watermark");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto feed = table->SubscribeFeed({.buffer_bytes = 1024U});
    Pause pause(FeedTestHook::Point::kAfterVersion, fail);
    FeedTestAccess::SetHook(*table, &pause);
    std::thread low([&] {
        auto lease = table->Acquire();
        try { lease->store().SetBlockBits(0, 0, Bits(1)); }
        catch (const std::runtime_error&) { assert(fail); }
    });
    const auto reserved = pause.Wait();
    std::uint64_t last = 0;
    {
        auto lease = table->Acquire();
        for (std::uint32_t i = 1; i <= 30; ++i) lease->store().SetBlockBits(4, 0, Bits(i));
        last = lease->store().GetChunkVersion(1, 0);
    }
    assert(last > reserved && !feed->Next(50ms));
    pause.Release();
    low.join();
    auto resync = Next(*feed);
    assert(resync->kind == FeedEntry::Kind::kResync && resync->position.revision == last);
    FeedTestAccess::SetHook(*table, nullptr);
    table->StopFeed();
}

void ConservativeSlotDoesNotRetreatResync() {
    ScopedTempDir dir("chunkdb-feed-conservative-slot");
    TableCatalog catalog(Config(dir.path()));
    (void)feed_test::CreateDefault(catalog);
    auto table = catalog.Find("default");
    auto active = table->SubscribeFeed({.buffer_bytes = 4096U});
    auto lagging = table->SubscribeFeed();
    struct TwoPauses : FeedTestHook {
        Pause before{Point::kBeforeSlot};
        Pause after{Point::kAfterVersion};
        std::mutex mutex;
        std::thread::id writer;
        void Run(Point point, std::uint64_t revision) override {
            {
                std::lock_guard lock(mutex);
                if (writer == std::thread::id{} && point == Point::kBeforeSlot) writer = std::this_thread::get_id();
                if (writer != std::this_thread::get_id()) return;
            }
            before.Run(point, revision);
            after.Run(point, revision);
        }
    } pause;
    FeedTestAccess::SetHook(*table, &pause);
    std::thread late([&] {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, Bits(1));
    });
    const auto bound = pause.before.Wait();
    for (std::uint32_t i = 1; i < 25; ++i) {
        {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(4, 0, Bits(i));
        }
        assert(Next(*active)->kind == FeedEntry::Kind::kChange);
    }
    const auto completed = active->position();
    assert(completed.revision > bound);
    pause.before.Release();
    const auto reserved = pause.after.Wait();
    assert(reserved > completed.revision && FeedTestAccess::Watermark(*table) < bound);
    const auto progress = active->Progress();
    assert(progress && progress->revision >= completed.revision);
    auto resync = Next(*lagging);
    assert(resync->kind == FeedEntry::Kind::kResync && resync->position.revision >= completed.revision);
    pause.after.Release();
    late.join();
    assert(Next(*lagging)->position.revision == reserved);
    FeedTestAccess::SetHook(*table, nullptr);
    table->StopFeed();
}

}  // namespace
int main() {
    NestedGuardCannotReplaceWriterContext();
    UnwatchedCompletedFrontier(false);
    UnwatchedCompletedFrontier(true);
    for (unsigned path = 0; path < 7; ++path) {
        HoldWriter(path, false);
        HoldWriter(path, true);
    }
    ProgressWaitsForSenderAndReader();
    ResumeAtReservationGap();
    QueueOrder();
    QueuedOverflow();
    OverflowWaitsForWriter(false);
    OverflowWaitsForWriter(true);
    ConservativeSlotDoesNotRetreatResync();
}
