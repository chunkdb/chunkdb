// Heap held by one SET CHUNK: the statement holds a small constant number of
// chunk-sized buffers at once, and a transaction's limits refuse a chunk form
// before it is decoded (docs/TRANSACTIONS.md). The global allocation
// functions of this binary count live heap bytes and their high-water mark.

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "test_utils.hpp"

// Not on MinGW: there the C++ runtime DLL allocates through its own
// operator new, which this binary's replacement does not reach.
#if !defined(__MINGW32__)
#define CHUNKDB_COUNTS_HEAP 1
#if defined(__APPLE__)
// From <malloc/malloc.h>, whose Mach headers GCC does not parse; not
// const, as GCC would take the block for one whose bytes are read.
extern "C" std::size_t malloc_size(void* pointer);
#else
#include <malloc.h>
#endif
#endif

#if defined(CHUNKDB_COUNTS_HEAP)
namespace {

// Live bytes as the C allocator sizes its blocks: the block size is read
// back on release, so no header changes the alignment malloc gives.
std::atomic<std::size_t> g_live_bytes{0};
std::atomic<std::size_t> g_peak_bytes{0};

std::size_t BlockBytes(void* block) noexcept {
#if defined(__APPLE__)
    return malloc_size(block);
#elif defined(_WIN32)
    return _msize(block);
#else
    return malloc_usable_size(block);
#endif
}

void* CountedAllocate(std::size_t size) {
    void* block = std::malloc(size == 0U ? 1U : size);
    if (block == nullptr) {
        throw std::bad_alloc();
    }
    // operator new must return memory aligned for any ordinary type.
    if (reinterpret_cast<std::uintptr_t>(block) % __STDCPP_DEFAULT_NEW_ALIGNMENT__ != 0U) {
        std::fprintf(stderr, "malloc returned a block below the default new alignment\n");
        std::abort();
    }
    const std::size_t bytes = BlockBytes(block);
    const std::size_t live = g_live_bytes.fetch_add(bytes) + bytes;
    std::size_t peak = g_peak_bytes.load();
    while (live > peak && !g_peak_bytes.compare_exchange_weak(peak, live)) {
    }
    return block;
}

void CountedRelease(void* block) noexcept {
    if (block == nullptr) {
        return;
    }
    g_live_bytes.fetch_sub(BlockBytes(block));
    std::free(block);
}

}  // namespace

void* operator new(std::size_t size) { return CountedAllocate(size); }
void* operator new[](std::size_t size) { return CountedAllocate(size); }
void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return CountedAllocate(size);
    } catch (const std::bad_alloc&) {
        return nullptr;
    }
}
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return CountedAllocate(size);
    } catch (const std::bad_alloc&) {
        return nullptr;
    }
}
void operator delete(void* pointer) noexcept { CountedRelease(pointer); }
void operator delete[](void* pointer) noexcept { CountedRelease(pointer); }
void operator delete(void* pointer, std::size_t) noexcept { CountedRelease(pointer); }
void operator delete[](void* pointer, std::size_t) noexcept { CountedRelease(pointer); }
void operator delete(void* pointer, const std::nothrow_t&) noexcept { CountedRelease(pointer); }
void operator delete[](void* pointer, const std::nothrow_t&) noexcept { CountedRelease(pointer); }

namespace {

using chunkdb::CommandEngine;
using Parameters = std::vector<std::optional<std::string>>;

// Live heap bytes above those when the window opened, at their highest.
class HeapWindow {
  public:
    HeapWindow() : base_(g_live_bytes.load()) { g_peak_bytes.store(base_); }
    [[nodiscard]] std::size_t PeakGrowth() const { return g_peak_bytes.load() - base_; }

  private:
    std::size_t base_;
};

void ExpectReplyStarts(const std::string& reply, const std::string& prefix) {
    if (reply.rfind(prefix, 0) != 0) {
        std::fprintf(stderr, "expected a reply starting '%s', got '%s'\n", prefix.c_str(), reply.c_str());
        assert(false);
    }
}

void ExpectGrowthBelow(const char* statement, std::size_t growth, std::size_t bound) {
    if (growth >= bound) {
        std::fprintf(stderr, "%s: the heap grew by %zu bytes, expected less than %zu\n", statement, growth, bound);
        assert(false);
    }
}

// 256 x 256 blocks of 256 bits: a 2 MiB payload, past the default 1 MiB
// checkpoint threshold, so one SET CHUNK also checkpoints the chunk inline.
struct Fixture {
    chunkdb::test::ScopedTempDir dir{"chunkdb-chunk-write-memory"};
    std::shared_ptr<chunkdb::TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    chunkdb::SessionState a;
    chunkdb::SessionState b;
    // A chunk form with every block present.
    std::string form;

    explicit Fixture(chunkdb::EngineConfig config = {}) {
        chunkdb::CatalogConfig catalog_config;
        catalog_config.data_dir = dir.path();
        catalog = std::make_shared<chunkdb::TableCatalog>(catalog_config);
        config.require_auth = false;
        config.server_version = "test";
        engine = std::make_unique<CommandEngine>(config, catalog);
        for (auto* session : {&a, &b}) {
            ExpectReplyStarts(engine->Execute(*session, "HELLO 3\r\n"), "%");
        }
        ExpectReplyStarts(Run(a, "CREATE TABLE big (b bits(256)) CHUNK 256 x 256"), "+OK");
        // The form of chunk (1,0), with every byte after its header set.
        ExpectReplyStarts(Run(a, "SET BLOCK 256 0 IN big b = b'" + std::string(256, '1') + "'"), ":");
        const std::string reply = Run(a, "GET CHUNK 1 0 FROM big");
        const auto body = reply.find("\r\n") + 2U;
        form = reply.substr(body, reply.size() - body - 2U);
        std::fill(form.begin() + 16, form.end(), '\xff');
    }

    std::string Run(chunkdb::SessionState& session, const std::string& line, const Parameters& parameters = {}) {
        return engine->Execute(session, line + "\r\n", parameters);
    }

    // The bytes of the chunk state the form sends.
    [[nodiscard]] std::size_t StateBytes() const { return form.size() - 16U; }
};

// One SET CHUNK of a new chunk holds the decoded state, the empty state it
// replaces and its WAL frame at once; the replaced state is gone before the
// checkpoint image is built. The request frame is the caller's.
void TestSetChunkHoldsThreeStates() {
    Fixture f;
    const Parameters parameters{f.form};
    const HeapWindow window;
    ExpectReplyStarts(f.Run(f.a, "SET CHUNK 0 0 IN big $1", parameters), ":");
    ExpectGrowthBelow("SET CHUNK of a new chunk", window.PeakGrowth(), 3U * f.StateBytes() + f.StateBytes() / 4U);
}

// A SET CHUNK, with or without IF VERSION, while a transaction is open
// moves the state it replaces into the transaction history instead of
// copying it.
void TestSetChunkKeepsWithoutCopy(bool conditional) {
    Fixture f;
    const Parameters parameters{f.form};
    ExpectReplyStarts(f.Run(f.a, "SET CHUNK 0 0 IN big $1", parameters), ":");
    const std::string version = f.Run(f.a, "GET CHUNK 0 0 FROM big");
    std::uint64_t chunk_version = 0;
    const auto body = version.find("\r\n") + 2U;
    for (std::size_t i = 0; i < 8U; ++i) {
        chunk_version |= static_cast<std::uint64_t>(static_cast<unsigned char>(version[body + i])) << (8U * i);
    }
    // An open transaction that read the chunk needs the state it had.
    ExpectReplyStarts(f.Run(f.b, "BEGIN"), "+OK");
    const std::string old_block = f.Run(f.b, "GET BLOCK 0 0 FROM big");
    ExpectReplyStarts(old_block, "*");
    // Every block present with a NULL value: the payload after the header
    // and presence is cleared.
    std::string next = f.form;
    std::fill(next.begin() + 16 + 256 * 256 / 8, next.end(), '\0');
    const Parameters next_parameters{next};
    const HeapWindow window;
    const std::string condition = conditional ? " IF VERSION " + std::to_string(chunk_version) : "";
    ExpectReplyStarts(f.Run(f.a, "SET CHUNK 0 0 IN big $1" + condition, next_parameters), ":");
    // The decoded state and one more chunk-sized buffer, the WAL frame or the
    // image; the kept state is the one the chunk held, not a copy.
    ExpectGrowthBelow("SET CHUNK with a kept state", window.PeakGrowth(), 2U * f.StateBytes() + f.StateBytes() / 4U);
    assert(f.Run(f.a, "GET BLOCK 0 0 FROM big") != old_block);
    assert(f.Run(f.b, "GET BLOCK 0 0 FROM big") == old_block);
    ExpectReplyStarts(f.Run(f.b, "ROLLBACK"), "+OK");
}

// A SET CHUNK in a transaction past --txn-max-bytes is refused before its
// form is decoded or the chunk is read.
void TestTxnLimitRefusesBeforeDecoding() {
    Fixture f(chunkdb::EngineConfig{.txn_max_bytes = 1024U * 1024U});
    const Parameters parameters{f.form};
    ExpectReplyStarts(f.Run(f.a, "BEGIN"), "+OK");
    const HeapWindow window;
    ExpectReplyStarts(
        f.Run(f.a, "SET CHUNK 0 0 IN big $1", parameters),
        "-ERR INVALID_ARGUMENT reason=invalid_argument the transaction's changes would take");
    ExpectGrowthBelow("SET CHUNK past --txn-max-bytes", window.PeakGrowth(), f.StateBytes() / 4U);
    ExpectReplyStarts(f.Run(f.a, "ROLLBACK"), "+OK");
}

}  // namespace
#endif

int main() {
#if !defined(CHUNKDB_COUNTS_HEAP)
    std::fprintf(stderr, "skipped: this platform's C++ runtime allocates outside the counted operator new\n");
    return 77;
#else
    TestSetChunkHoldsThreeStates();
    TestSetChunkKeepsWithoutCopy(false);
    TestSetChunkKeepsWithoutCopy(true);
    TestTxnLimitRefusesBeforeDecoding();
    return 0;
#endif
}
