// Block history (#45): the table options and the feature flag, where history
// starts, tags from the protocol to the WAL, and the history checkpoints
// write: every mutation, across restarts, eviction, failures and crashes.

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <utility>
#include <stdexcept>
#include <string>
#include <vector>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feature_flags.hpp"
#include "history_store.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"

namespace {

using Bytes = std::vector<std::uint8_t>;
using chunkdb::test::ScopedTempDir;

const chunkdb::GeometryConfig kGeometry{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 8,
    .chunk_height_blocks = 8,
    .block_bits = 4,
};

void SetEnvVar(const char* key, const char* value) {
#ifdef _WIN32
    const int rc = _putenv_s(key, value);
#else
    const int rc = setenv(key, value, 1);
#endif
    assert(rc == 0);
    (void)rc;
}

bool Contains(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

template <typename Exception = std::exception, typename Fn>
void ExpectThrow(Fn fn, const std::string& part) {
    try {
        fn();
    } catch (const Exception& e) {
        if (!Contains(e.what(), part)) {
            std::fprintf(stderr, "expected '%s', got '%s'\n", part.c_str(), e.what());
            assert(false);
        }
        return;
    }
    std::fprintf(stderr, "expected an exception with '%s'\n", part.c_str());
    assert(false);
}

Bytes ReadFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

chunkdb::StoreConfig Config(const std::filesystem::path& dir) {
    chunkdb::StoreConfig config;
    config.geometry = kGeometry;
    config.data_dir = dir;
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    return config;
}

// The TAG entry of every frame of a WAL file, in order (empty for a frame
// without one), read by the layout of docs/STORAGE_FORMAT.md Section 4.1.
std::vector<Bytes> WalFrameTags(const std::filesystem::path& wal_path) {
    const Bytes wal = ReadFile(wal_path);
    std::vector<Bytes> tags;
    std::size_t at = chunkdb::kWalHeaderSize;
    while (at < wal.size()) {
        assert(wal.size() - at >= chunkdb::kWalFrameFixedHeaderSize);
        assert(std::equal(
            chunkdb::kWalFrameMagic, chunkdb::kWalFrameMagic + chunkdb::kWalFrameMagicSize,
            wal.begin() + static_cast<std::ptrdiff_t>(at)));
        const std::uint16_t tlv_size = chunkdb::ReadLe16(wal, at + 22U);
        const std::uint32_t body_size = chunkdb::ReadLe32(wal, at + 28U);
        Bytes tag;
        for (std::size_t tlv = at + chunkdb::kWalFrameFixedHeaderSize;
             tlv < at + chunkdb::kWalFrameFixedHeaderSize + tlv_size;) {
            const std::uint16_t type = chunkdb::ReadLe16(wal, tlv);
            const std::uint16_t length = chunkdb::ReadLe16(wal, tlv + 2U);
            if (type == chunkdb::kWalTlvTag) {
                tag.assign(
                    wal.begin() + static_cast<std::ptrdiff_t>(tlv + 4U),
                    wal.begin() + static_cast<std::ptrdiff_t>(tlv + 4U + length));
            }
            tlv += 4U + length;
        }
        tags.push_back(std::move(tag));
        at += chunkdb::kWalFrameFixedHeaderSize + tlv_size + chunkdb::kWalFrameHeaderCrcSize +
              body_size + chunkdb::kWalFrameTrailerSize;
    }
    assert(at == wal.size());
    return tags;
}

chunkdb::TableOptions HistoryOptions() {
    chunkdb::TableOptions options;
    options.history = true;
    options.history_start = 7;
    return options;
}

void TestManifestOptions() {
    using chunkdb::DecodeTableOptions;
    using chunkdb::EncodeTableOptions;

    // Without history nothing of it is written; with history only the start
    // and the limits that differ from their defaults.
    const chunkdb::TableOptions plain;
    assert(DecodeTableOptions(EncodeTableOptions(plain)).history == false);
    assert(!chunkdb::HasHistory(chunkdb::TableFeatures(plain)));
    auto options = HistoryOptions();
    const auto minimal = EncodeTableOptions(options);
    assert(minimal.size() == EncodeTableOptions(plain).size() + 12U);
    assert(chunkdb::HasHistory(chunkdb::TableFeatures(options)));
    options.history_max_age_ms = 86'400'000;
    options.history_max_chunk_bytes = 1U << 20U;
    options.history_max_tag_bytes = 255;
    const auto decoded = DecodeTableOptions(EncodeTableOptions(options));
    assert(decoded.history && decoded.history_start == 7);
    assert(decoded.history_max_age_ms == 86'400'000);
    assert(decoded.history_max_chunk_bytes == (1U << 20U));
    assert(decoded.history_max_tag_bytes == 255);
    const auto defaults = DecodeTableOptions(minimal);
    assert(defaults.history_max_age_ms == 0 && defaults.history_max_chunk_bytes == 0);
    assert(defaults.history_max_tag_bytes == chunkdb::kDefaultHistoryMaxTagBytes);

    const auto with_option = [](Bytes options_area, std::uint16_t type, std::uint64_t value) {
        chunkdb::WriteLe16(options_area, type);
        chunkdb::WriteLe16(options_area, 8U);
        chunkdb::WriteLe64(options_area, value);
        return options_area;
    };
    ExpectThrow([&] { (void)DecodeTableOptions(with_option(minimal, chunkdb::kOptionHistoryStart, 9)); },
                "appears twice");
    ExpectThrow(
        [&] {
            (void)DecodeTableOptions(
                with_option(EncodeTableOptions(plain), chunkdb::kOptionHistoryMaxAgeMs, 5));
        },
        "history options without history_start");
    ExpectThrow(
        [&] { (void)DecodeTableOptions(with_option(minimal, chunkdb::kOptionHistoryMaxTagBytes, 256)); },
        "has value 256");
    ExpectThrow(
        [&] { (void)DecodeTableOptions(with_option(minimal, chunkdb::kOptionHistoryMaxChunkBytes, 0)); },
        "has value 0");

    // The feature and history_start go together.
    chunkdb::StoreManifest manifest{
        .features = chunkdb::TableFeatures(HistoryOptions()),
        .geometry = kGeometry,
        .store_id = chunkdb::NewStoreId(),
        .options = minimal,
    };
    assert(chunkdb::ParseStoreManifest(chunkdb::SerializeStoreManifest(manifest)).options == minimal);
    auto without_feature = manifest;
    without_feature.features = {};
    ExpectThrow(
        [&] { (void)chunkdb::ParseStoreManifest(chunkdb::SerializeStoreManifest(without_feature)); },
        "history options without the history feature");
    auto without_start = manifest;
    without_start.options = EncodeTableOptions(plain);
    ExpectThrow(
        [&] { (void)chunkdb::ParseStoreManifest(chunkdb::SerializeStoreManifest(without_start)); },
        "history feature without history_start");
    assert(chunkdb::UnknownFeatures(manifest.features).ro_compat == 0U);
}

void TestStoreOptions() {
    ScopedTempDir dir("chunkdb-history-store");
    // History options other than `history` need it.
    for (const auto& change : std::vector<void (*)(chunkdb::StoreConfig*)>{
             [](chunkdb::StoreConfig* c) { c->history_max_age_ms = 1; },
             [](chunkdb::StoreConfig* c) { c->history_max_chunk_bytes = 1; },
             [](chunkdb::StoreConfig* c) { c->history_max_tag_bytes = 8; },
         }) {
        auto config = Config(dir.path() / "refused");
        change(&config);
        ExpectThrow<std::invalid_argument>(
            [&] { chunkdb::ChunkStore store(config); }, "apply only to a table with history");
    }
    {
        auto config = Config(dir.path() / "refused");
        config.history_start = 3;
        ExpectThrow<std::invalid_argument>(
            [&] { chunkdb::ChunkStore store(config); }, "history_start applies only");
        config.history = true;
        config.history_start = 0;
        config.history_max_tag_bytes = 256;
        ExpectThrow<std::invalid_argument>(
            [&] { chunkdb::ChunkStore store(config); }, "between 1 and 255");
        config.history_max_tag_bytes = 0;
        ExpectThrow<std::invalid_argument>(
            [&] { chunkdb::ChunkStore store(config); }, "between 1 and 255");
    }
    assert(!std::filesystem::exists(dir.path() / "refused"));

    // A new store's history starts at its clock's first token.
    auto config = Config(dir.path() / "store");
    config.history = true;
    config.initial_version_floor = 40;
    config.history_max_tag_bytes = 4;
    {
        chunkdb::ChunkStore store(config);
        assert(store.history() && store.history_start() == 40);
        store.SetBlockBits(0, 0, "0001");
        assert(store.GetChunkVersion(0, 0) >= 40);
    }
    const auto manifest = chunkdb::ReadStoreManifest(config.data_dir);
    assert(manifest.has_value() && chunkdb::HasHistory(manifest->features));
    assert(chunkdb::DecodeTableOptions(manifest->options).history_start == 40);

    // An existing store opens with history exactly when it has it, and
    // keeps the start it recorded.
    {
        auto without = config;
        without.history = false;
        without.history_max_tag_bytes = chunkdb::kDefaultHistoryMaxTagBytes;
        ExpectThrow<std::invalid_argument>(
            [&] { chunkdb::ChunkStore store(without); }, "has history; open it with history on");
        auto other_start = config;
        other_start.history_start = 41;
        ExpectThrow<std::invalid_argument>(
            [&] { chunkdb::ChunkStore store(other_start); }, "has history from revision 40, not 41");
        {
            chunkdb::ChunkStore store(config);
            assert(store.history_start() == 40);
        }
        auto same_start = config;
        same_start.history_start = 40;
        same_start.initial_version_floor = 0;
        chunkdb::ChunkStore store(same_start);
        assert(store.history_start() == 40);
    }
    {
        auto plain = Config(dir.path() / "plain");
        { chunkdb::ChunkStore store(plain); }
        plain.history = true;
        ExpectThrow<std::invalid_argument>(
            [&] { chunkdb::ChunkStore store(plain); }, "has no history; enable it on the table");
    }
}

void TestStoreTags() {
    ScopedTempDir dir("chunkdb-history-tags");
    const Bytes tag{0xAB, 0x01};
    const Bytes long_tag(5, 0x11);
    {
        // Without history a tag is refused before anything changes.
        chunkdb::ChunkStore plain(Config(dir.path() / "plain"));
        ExpectThrow<std::invalid_argument>(
            [&] { plain.SetBlockBits(0, 0, "0001", tag); }, "TAG needs a table with history");
        assert(!plain.ReadBlockBits(0, 0).has_value());
        plain.SetBlockBits(0, 0, "0001");
    }

    auto config = Config(dir.path() / "store");
    config.history = true;
    config.history_max_tag_bytes = 4;
    config.extra_max_block_bits = 16;
    chunkdb::ChunkStore store(config);
    const auto payload = Bytes(chunkdb::Geometry(kGeometry).ChunkPayloadBytes(), 0x22);
    const auto presence = chunkdb::FullPresenceBitmap(chunkdb::Geometry(kGeometry));
    const auto refused = [&](auto write) {
        ExpectThrow<std::invalid_argument>(write, "exceeds history_max_tag_bytes (4)");
    };
    refused([&] { store.SetBlockBits(0, 0, "0001", long_tag); });
    refused([&] { store.UnsetBlock(0, 0, long_tag); });
    refused([&] { (void)store.SetChunkStateBytes(1, 0, payload, presence, long_tag); });
    refused([&] { (void)store.CasChunkStateBytes(1, 0, 1, payload, presence, long_tag); });
    refused([&] { (void)store.PutBlockExtra(0, 0, chunkdb::ExtraValue{.bit_length = 8, .bytes = {1}}, long_tag); });
    refused([&] { (void)store.DeleteBlockExtra(0, 0, long_tag); });
    refused([&] {
        (void)store.ApplyChunkBatch(
            0, 0, false, 0, {chunkdb::ChunkBatchOp{.set = true, .x = 0, .y = 0, .bits = "0001"}},
            long_tag);
    });
    assert(!std::filesystem::exists(chunkdb::ChunkWalPath(config.data_dir, store.geometry(), {0, 0})));

    // Each write puts its tag on its own frame; untagged frames have none.
    const Bytes tag2{0x02};
    store.SetBlockBits(0, 0, "0001", tag);
    store.SetBlockBits(1, 0, "0010");
    (void)store.PutBlockExtra(1, 0, chunkdb::ExtraValue{.bit_length = 8, .bytes = {5}}, tag2);
    (void)store.DeleteBlockExtra(1, 0, tag);
    store.UnsetBlock(1, 0, tag2);
    (void)store.ApplyChunkBatch(
        0, 0, false, 0, {chunkdb::ChunkBatchOp{.set = true, .x = 2, .y = 0, .bits = "0011"}}, tag);
    (void)store.CasChunkStateBytes(0, 0, store.GetChunkVersion(0, 0), payload, presence, tag2);
    (void)store.SetChunkStateBytes(0, 0, Bytes(payload.size(), 0x33), presence, tag);
    // A write that changes nothing has no frame, tag or not.
    store.SetBlockBits(0, 0, store.GetBlockBits(0, 0), tag2);
    const auto tags =
        WalFrameTags(chunkdb::ChunkWalPath(config.data_dir, store.geometry(), {0, 0}));
    assert((tags == std::vector<Bytes>{tag, {}, tag2, tag, tag2, tag, tag2, tag}));
}

void TestCatalogHistoryStart() {
    ScopedTempDir dir("chunkdb-history-catalog");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    config.default_geometry = kGeometry;
    std::uint64_t enabled_start = 0;
    std::uint64_t last_plain_version = 0;
    {
        chunkdb::TableCatalog catalog(config);
        // A new table's history covers it from its first revision.
        auto created_options = chunkdb::TableOptions{};
        created_options.history = true;
        created_options.history_start = 999;  // not the caller's to set
        const auto created = catalog.Create("created", kGeometry, created_options);
        assert(created->Info().options.history_start == 1);
        {
            auto lease = created->Acquire();
            assert(lease->store().history() && lease->store().history_start() == 1);
        }

        // Enabling history on a table with data starts it above every
        // revision issued so far.
        const auto table = catalog.Find("default");
        {
            auto lease = table->Acquire();
            lease->store().SetBlockBits(0, 0, "0001");
            lease->store().SetBlockBits(0, 0, "0010");
            last_plain_version = lease->store().GetChunkVersion(0, 0);
        }
        ExpectThrow<std::invalid_argument>(
            [&] {
                chunkdb::TableOptionsUpdate update;
                update.history_max_tag_bytes = 8;
                catalog.SetOptions("default", update);
            },
            "apply only to a table with history");
        chunkdb::TableOptionsUpdate enable;
        enable.history = true;
        enable.history_max_tag_bytes = 8;
        catalog.SetOptions("default", enable);
        const auto info = table->Info().options;
        assert(info.history && info.history_max_tag_bytes == 8);
        enabled_start = info.history_start;
        assert(enabled_start > last_plain_version);
        {
            auto lease = table->Acquire();
            assert(lease->store().history_start() == enabled_start);
            assert(lease->store().GetChunkVersion(0, 0) == last_plain_version);
            lease->store().SetBlockBits(0, 0, "0011", Bytes{0x01});
            assert(lease->store().GetChunkVersion(0, 0) >= enabled_start);
        }
        // Other options change; history_start stays and history stays on.
        chunkdb::TableOptionsUpdate limits;
        limits.history_max_age_ms = 60'000;
        catalog.SetOptions("default", limits);
        assert(table->Info().options.history_start == enabled_start);
        assert(table->Info().options.history_max_age_ms == 60'000);
        chunkdb::TableOptionsUpdate disable;
        disable.history = false;
        ExpectThrow<std::invalid_argument>(
            [&] { catalog.SetOptions("default", disable); }, "history cannot be disabled");
    }
    // The start survives a restart.
    chunkdb::TableCatalog reopened(config);
    const auto info = reopened.Find("default")->Info().options;
    assert(info.history && info.history_start == enabled_start && info.history_max_age_ms == 60'000);
}

void TestEnableNotDurable() {
    // A manifest enabling history that may not be durable keeps the table
    // unserved until a restart reads the durable one.
    ScopedTempDir dir("chunkdb-history-enable");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    config.default_geometry = kGeometry;
    {
        chunkdb::TableCatalog catalog(config);
        SetEnvVar("CHUNKDB_FAILPOINT_TABLESET_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
        chunkdb::TableOptionsUpdate enable;
        enable.history = true;
        ExpectThrow<std::runtime_error>([&] { catalog.SetOptions("default", enable); }, "injected");
        assert(catalog.Find("default") == nullptr);
    }
    chunkdb::TableCatalog reopened(config);
    assert(reopened.Find("default")->Info().options.history);
}

std::string BulkBody(const std::string& reply) {
    assert(!reply.empty() && reply[0] == '$');
    const auto header_end = reply.find("\r\n");
    const auto length = std::stoull(reply.substr(1, header_end - 1));
    return reply.substr(header_end + 2, length);
}

void TestProtocol() {
    ScopedTempDir dir("chunkdb-history-protocol");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    config.default_geometry = kGeometry;
    config.default_options.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
    chunkdb::EngineConfig engine_config;
    engine_config.require_auth = false;
    chunkdb::CommandEngine engine(engine_config, catalog);
    chunkdb::SessionState session;
    const auto hello = BulkBody(engine.Execute(session, "HELLO 2\n"));
    assert(Contains(hello, "max_tag_bytes=255\n"));
    assert(Contains(
        hello,
        "history=off\nhistory_start=0\nhistory_max_age_ms=0\nhistory_max_chunk_bytes=0\n"
        "history_max_tag_bytes=0\n"));

    // Options: on/off, limits only with history, history_start not settable.
    const std::string table = "TABLECREATE h block_bits 4 chunk_width_blocks 8 chunk_height_blocks 8 "
                              "large_chunk_width_chunks 2 large_chunk_height_chunks 2 ";
    assert(Contains(engine.Execute(session, table + "history yes\n"), "history must be on or off"));
    assert(Contains(
        engine.Execute(session, table + "history_max_age_ms 5\n"), "apply only to a table with history"));
    assert(Contains(engine.Execute(session, table + "history on history_start 5\n"), "cannot be given"));
    assert(Contains(
        engine.Execute(session, table + "history on history_max_tag_bytes 256\n"), "between 1 and 255"));
    assert(engine.Execute(
               session, table + "history ON history_max_age_ms 0 history_max_chunk_bytes 4096 "
                                "history_max_tag_bytes 3 durability_mode fsync-wal extra_max_block_bits 8\n") ==
           "+OK\r\n");
    const auto info = BulkBody(engine.Execute(session, "TABLEINFO h\n"));
    assert(Contains(
        info,
        "history=on\nhistory_start=1\nhistory_max_age_ms=0\nhistory_max_chunk_bytes=4096\n"
        "history_max_tag_bytes=3\n"));
    assert(Contains(engine.Execute(session, "TABLESET h history off\n"), "cannot be disabled"));

    // TAG on every write command of a table without history is refused;
    // payloads are read and dropped.
    const auto send = [&](const std::string& line, const std::string& payload) {
        const auto plan = engine.PlanPayload(session, line);
        if (plan.plan == chunkdb::CommandEngine::PayloadPlan::kDiscard) {
            assert(plan.bytes == payload.size());
            return engine.ExecuteDiscarded(session, line, plan.reject_response);
        }
        assert(plan.plan == chunkdb::CommandEngine::PayloadPlan::kRead && plan.bytes == payload.size());
        return engine.Execute(session, line, payload);
    };
    const std::string chunk(32, '\x11');
    const std::string no_history = "-ERR INVALID_ARGUMENT TAG needs a table with history";
    assert(engine.Execute(session, "SET 0 0 0001 TAG ab\n").rfind(no_history, 0) == 0);
    assert(engine.Execute(session, "UNSET 0 0 TAG ab\n").rfind(no_history, 0) == 0);
    assert(engine.Execute(session, "MSET 0 0 0001 1 0 0001 TAG ab\n").rfind(no_history, 0) == 0);
    assert(engine.Execute(session, "CHUNKBATCH 0 0 TAG ab SET 0 0 0001\n").rfind(no_history, 0) == 0);
    assert(Contains(send("CHUNKPUT 0 0 TAG ab 32\n", chunk), "table 'default' has none"));
    assert(engine.Execute(session, "GET 0 0\n") == "$-1\r\n");

    // Malformed tags.
    for (const std::string& line : std::vector<std::string>{
             "SET 0 0 0001 TAG\n", "SET 0 0 0001 TAG abc\n", "SET 0 0 0001 TAG xz\n",
          "SET 0 0 0001 TAG " + std::string(512, 'a') + "\n", "CHUNKBATCH 0 0 TAG\n"}) {
        const auto reply = engine.Execute(session, line);
        assert(reply.rfind("-ERR INVALID_ARGUMENT", 0) == 0);
    }
    assert(Contains(engine.Execute(session, "SET 0 0 0001 TAG xz\n"), "pairs of hex digits"));

    // On a history table every write command takes a tag up to the limit.
    assert(BulkBody(engine.Execute(session, "USE h\n")).rfind("table=h\n", 0) == 0);
    assert(engine.Execute(session, "SET 0 0 0001 TAG 0A0b0C\n") == "+OK\r\n");
    assert(Contains(engine.Execute(session, "SET 0 0 0010 TAG 01020304\n"), "exceeds history_max_tag_bytes (3)"));
    assert(engine.Execute(session, "MSET 0 0 0010 1 0 0001 TAG 01\n") == "+OK\r\n");
    assert(engine.Execute(session, "UNSET 1 0 TAG 02\n") == "+OK\r\n");
    assert(engine.Execute(session, "CHUNKBATCH 0 0 TAG 03 SET 2 0 0001\n")[0] == '$');
    assert(send("XPUT 0 0 8 TAG 04 1\n", "\x05")== "+OK\r\n");
    assert(Contains(send("XPUT 0 0 8 TAG 01020304 1\n", "\x05"), "exceeds history_max_tag_bytes (3)"));
    assert(engine.Execute(session, "XDEL 0 0 TAG 05\n") == "+OK\r\n");
    const auto version = BulkBody(engine.Execute(session, "CHUNKVER 0 0\n"));
    assert(send("CHUNKPUT 0 0 STATE IF " + version + " TAG 06 40\n", chunk + std::string(8, '\xff'))[0] == '$');
    assert(send("CHUNKPUT 0 0 TAG 07 32\n", std::string(32, '\x12'))[0] == '$');
    assert(Contains(send("CHUNKPUT 0 0 TAG 01020304 32\n", chunk), "exceeds history_max_tag_bytes (3)"));

    const auto wal = chunkdb::ChunkWalPath(dir.path() / "tables" / "h", chunkdb::Geometry(kGeometry), {0, 0});
    const auto tags = WalFrameTags(wal);
    assert((tags == std::vector<Bytes>{
                        {0x0A, 0x0B, 0x0C}, {0x01}, {0x01}, {0x02}, {0x03}, {0x04}, {0x05}, {0x06}, {0x07}}));
}


// --- The checkpoint writes history ---

namespace history = chunkdb::history;

std::uint64_t g_random = 0x9E3779B97F4A7C15ULL;
std::uint64_t Next(std::uint64_t bound) {
    g_random ^= g_random << 13U;
    g_random ^= g_random >> 7U;
    g_random ^= g_random << 17U;
    return g_random % bound;
}

std::string RandomBits(std::size_t count) {
    std::string bits(count, '0');
    for (auto& bit : bits) {
        bit = Next(2) == 0U ? '0' : '1';
    }
    return bits;
}

Bytes RandomBytes(std::size_t count) {
    Bytes bytes(count);
    for (auto& byte : bytes) {
        byte = static_cast<std::uint8_t>(Next(256));
    }
    return bytes;
}

chunkdb::ExtraValue RandomExtra() {
    const auto bits = static_cast<std::uint32_t>(1 + Next(24));
    auto bytes = RandomBytes((bits + 7U) / 8U);
    if (bits % 8U != 0U) {
        bytes.back() &= static_cast<std::uint8_t>(0xFFU >> (8U - bits % 8U));
    }
    return chunkdb::ExtraValue{.bit_length = bits, .bytes = std::move(bytes)};
}

// The chunk's state as history describes it.
history::ChunkState StoreState(chunkdb::ChunkStore& store, const chunkdb::ChunkCoord& chunk) {
    const auto bytes = store.GetChunkStateExtraBytes(chunk.x, chunk.y);
    const std::size_t state_size = chunkdb::ChunkStateBytes(store.geometry());
    history::ChunkState state{.state = Bytes(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(state_size))};
    state.extra = chunkdb::ChunkExtra::Decode(
        bytes.data() + state_size, bytes.size() - state_size, store.geometry().ChunkBlockCount(),
        chunkdb::ExtraPadding::kReject);
    return state;
}

struct HistoryOnDisk {
    history::ChunkHistory chunk;
    // Where the oldest segment starts.
    history::ChunkState base;
    std::vector<history::Mutation> mutations;
    // The state after the last mutation.
    history::ChunkState end;
};

// Reads the chunk's history as a reader does and checks it: every change is a
// real one, segments follow each other, keyframes hold the replayed state.
HistoryOnDisk ReadHistory(const chunkdb::ChunkStore& store, const chunkdb::ChunkCoord& chunk) {
    const history::HistoryFiles files(store.data_dir(), store.geometry(), store.store_id(), store.history_start());
    HistoryOnDisk out;
    out.chunk = files.Load(chunk, /*writable=*/false);
    out.base = history::EmptyChunkState(store.geometry());
    history::ChunkState state = out.base;
    for (std::size_t i = 0; i < out.chunk.segments.size(); ++i) {
        const auto contents = files.ReadSegment(chunk, out.chunk.segments[i]);
        if (contents.header.keyframe.has_value()) {
            if (i == 0) {
                out.base = *contents.header.keyframe;
                state = out.base;
            } else {
                assert(*contents.header.keyframe == state);
            }
        }
        assert(contents.header.first == (i == 0));
        for (std::size_t at = out.chunk.segments[i].header_size; at < contents.bytes.size();) {
            const auto record = history::ReadRecord(
                store.geometry(), contents.bytes.data() + at, contents.bytes.size() - at, true);
            assert(record.status == history::RecordStatus::kOk);
            for (const auto& mutation : record.mutations) {
                history::ApplyMutation(store.geometry(), mutation, &state, /*reject_unchanged=*/true);
                assert(out.mutations.empty() || out.mutations.back().revision < mutation.revision);
                assert(out.mutations.empty() || out.mutations.back().time_ms <= mutation.time_ms);
                out.mutations.push_back(mutation);
            }
            at += record.summary.size;
        }
    }
    out.end = std::move(state);
    return out;
}

std::vector<Bytes> Tags(const std::vector<history::Mutation>& mutations) {
    std::vector<Bytes> tags;
    for (const auto& mutation : mutations) {
        tags.push_back(mutation.tag);
    }
    return tags;
}

chunkdb::StoreConfig HistoryConfig(const std::filesystem::path& dir) {
    auto config = Config(dir);
    config.history = true;
    config.extra_max_block_bits = 24;
    config.checkpoint_update_interval = 100000;
    return config;
}

// One random write of any kind to `chunk`; false when the store refused it.
bool RandomWrite(chunkdb::ChunkStore& store, const chunkdb::ChunkCoord& chunk, const Bytes& tag) {
    const auto& geometry = store.geometry();
    const std::int64_t x = chunk.x * 8 + static_cast<std::int64_t>(Next(8));
    const std::int64_t y = chunk.y * 8 + static_cast<std::int64_t>(Next(8));
    const auto payload = [&] { return RandomBytes(geometry.ChunkPayloadBytes()); };
    const auto presence = [&] { return RandomBytes(chunkdb::ChunkPresenceBitmapBytes(geometry)); };
    try {
        switch (Next(9)) {
            case 0:
            case 1:
                store.SetBlockBits(x, y, RandomBits(4), tag);
                break;
            case 2:
                store.UnsetBlock(x, y, tag);
                break;
            case 3:
                (void)store.SetChunkStateBytes(chunk.x, chunk.y, payload(), presence(), tag);
                break;
            case 4:
                (void)store.CasChunkStateBytes(
                    chunk.x, chunk.y, store.GetChunkVersion(chunk.x, chunk.y), payload(), presence(), tag);
                break;
            case 5:
                (void)store.PutBlockExtra(x, y, RandomExtra(), tag);
                break;
            case 6:
                (void)store.DeleteBlockExtra(x, y, tag);
                break;
            case 7: {
                std::vector<chunkdb::ChunkBatchOp> ops;
                for (std::uint64_t n = 1 + Next(4); n > 0; --n) {
                    const std::int64_t bx = chunk.x * 8 + static_cast<std::int64_t>(Next(8));
                    const std::int64_t by = chunk.y * 8 + static_cast<std::int64_t>(Next(8));
                    switch (Next(4)) {
                        case 0:
                            ops.push_back({.set = true, .x = bx, .y = by, .bits = RandomBits(4)});
                            break;
                        case 1:
                            ops.push_back({.set = false, .x = bx, .y = by});
                            break;
                        case 2:
                            ops.push_back({.x = bx, .y = by, .kind = chunkdb::ChunkBatchOpKind::kExtraPut, .extra = RandomExtra()});
                            break;
                        default:
                            ops.push_back({.x = bx, .y = by, .kind = chunkdb::ChunkBatchOpKind::kExtraDel});
                            break;
                    }
                }
                (void)store.ApplyChunkBatch(chunk.x, chunk.y, Next(2) == 0U, store.GetChunkVersion(chunk.x, chunk.y), ops, tag);
                break;
            }
            default: {
                const auto state = presence();
                chunkdb::ChunkExtra extra;
                for (std::uint32_t block = 0; block < geometry.ChunkBlockCount(); ++block) {
                    if (((state[block / 8U] >> (block % 8U)) & 1U) != 0U && Next(4) == 0U) {
                        extra.Assign(block, RandomExtra());
                    }
                }
                (void)store.SetChunkStateBytes(chunk.x, chunk.y, payload(), state, extra, tag);
                break;
            }
        }
    } catch (const std::invalid_argument&) {
        return false;
    }
    return true;
}

// Random writes of every kind, tagged or not, with checkpoints inline and
// explicit: history holds exactly the mutations that changed each chunk,
// with their revisions and tags, and replays to the chunk's state.
void TestCheckpointRecordsEveryMutation() {
    for (const auto mode : {chunkdb::DurabilityMode::kRelaxed, chunkdb::DurabilityMode::kFsyncWal,
                            chunkdb::DurabilityMode::kFsyncCheckpoint}) {
        ScopedTempDir dir("chunkdb-history-model");
        auto config = HistoryConfig(dir.path());
        config.durability_mode = mode;
        config.checkpoint_update_interval = 5;
        chunkdb::ChunkStore store(config);
        const std::vector<chunkdb::ChunkCoord> chunks = {{0, 0}, {1, 0}, {-1, 2}};
        std::vector<std::vector<history::Mutation>> expected(chunks.size());
        const std::uint64_t started = chunkdb::UnixMillisNow();
        for (int i = 0; i < 400; ++i) {
            const std::size_t c = Next(chunks.size());
            const auto chunk = chunks[c];
            const auto before = StoreState(store, chunk);
            const auto version_before = store.GetChunkVersion(chunk.x, chunk.y);
            const Bytes tag = i % 3 == 0 ? Bytes{} : Bytes{static_cast<std::uint8_t>(i), static_cast<std::uint8_t>(i >> 8)};
            (void)RandomWrite(store, chunk, tag);
            const auto after = StoreState(store, chunk);
            const auto version_after = store.GetChunkVersion(chunk.x, chunk.y);
            if (after == before) {
                assert(version_after == version_before);
                continue;
            }
            assert(version_after > version_before);
            expected[c].push_back(history::Mutation{
                .revision = version_after,
                .tag = tag,
                .changes = history::DiffBlocks(store.geometry(), before, after, nullptr),
            });
            if (Next(40) == 0U) {
                store.CheckpointForTests(chunk.x, chunk.y);
            }
        }
        for (std::size_t c = 0; c < chunks.size(); ++c) {
            store.CheckpointForTests(chunks[c].x, chunks[c].y);
            const auto on_disk = ReadHistory(store, chunks[c]);
            assert(on_disk.mutations.size() == expected[c].size());
            for (std::size_t m = 0; m < expected[c].size(); ++m) {
                auto actual = on_disk.mutations[m];
                assert(actual.time_ms >= started && actual.time_ms <= chunkdb::UnixMillisNow());
                actual.time_ms = 0;
                assert(actual == expected[c][m]);
            }
            assert(on_disk.end == StoreState(store, chunks[c]));
            assert(on_disk.base == history::EmptyChunkState(store.geometry()));
        }
        assert(store.RuntimeStats().checkpoints > 20U);
    }
}

// A write that only replaces extra data logs no span: the blocks that lose
// their values are still events.
void TestExtraOnlyReplace() {
    ScopedTempDir dir("chunkdb-history-extra-replace");
    chunkdb::ChunkStore store(HistoryConfig(dir.path()));
    store.SetBlockBits(0, 0, "0001");
    store.SetBlockBits(1, 0, "0010");
    (void)store.PutBlockExtra(0, 0, chunkdb::ExtraValue{.bit_length = 3, .bytes = {0x05}});
    (void)store.PutBlockExtra(1, 0, chunkdb::ExtraValue{.bit_length = 3, .bytes = {0x06}});
    const auto before = StoreState(store, {0, 0});
    const auto& geometry = store.geometry();
    const auto state = store.GetChunkStateBytes(0, 0);
    const Bytes payload(state.begin(), state.begin() + static_cast<std::ptrdiff_t>(geometry.ChunkPayloadBytes()));
    const Bytes presence(state.begin() + static_cast<std::ptrdiff_t>(geometry.ChunkPayloadBytes()), state.end());
    chunkdb::ChunkExtra extra;
    extra.Assign(1, chunkdb::ExtraValue{.bit_length = 3, .bytes = {0x06}});
    (void)store.SetChunkStateBytes(0, 0, payload, presence, extra, Bytes{0x0E});
    store.CheckpointForTests(0, 0);
    const auto on_disk = ReadHistory(store, {0, 0});
    assert(on_disk.mutations.back().tag == Bytes{0x0E});
    assert(on_disk.mutations.back().changes == history::DiffBlocks(geometry, before, StoreState(store, {0, 0}), nullptr));
    assert(on_disk.mutations.back().changes.size() == 1U &&
           on_disk.mutations.back().changes[0].extra_change == history::ExtraChangeKind::kRemoved);
}

// Mutations made before a restart or an eviction reach history at the next
// checkpoint.
void TestHistoryAcrossRestartAndEviction() {
    ScopedTempDir dir("chunkdb-history-restart");
    auto config = HistoryConfig(dir.path());
    config.max_loaded_chunks = 1;
    std::vector<Bytes> tags;
    const auto tagged = [&](int i) {
        tags.push_back(Bytes{static_cast<std::uint8_t>(i), static_cast<std::uint8_t>(i >> 8)});
        return tags.back();
    };
    {
        chunkdb::ChunkStore store(config);
        for (int i = 0; i < 20; ++i) {
            store.SetBlockBits(i % 8, 0, (i / 8) % 2 == 0 ? "0101" : "1010", tagged(i));
            store.SetBlockBits(8, 0, i % 2 == 0 ? "0001" : "0010");  // evicts chunk (0,0)
        }
    }
    chunkdb::ChunkStore store(config);
    for (int i = 20; i < 40; ++i) {
        store.SetBlockBits(i % 8, 1, (i / 8) % 2 == 0 ? "0110" : "1001", tagged(i));
    }
    store.CheckpointForTests(0, 0);
    auto on_disk = ReadHistory(store, {0, 0});
    assert(Tags(on_disk.mutations) == tags);
    assert(on_disk.end == StoreState(store, {0, 0}));
    assert(ReadHistory(store, {1, 0}).mutations.size() == 0U);
    store.CheckpointForTests(1, 0);
    assert(ReadHistory(store, {1, 0}).mutations.size() == 20U);

}

// Segments roll over once their records pass kSegmentTargetRecordBytes, and
// a new segment gets a keyframe only once the records since the last one
// take kKeyframeRatio times its size.
void TestSegmentsAndKeyframes() {
    ScopedTempDir dir("chunkdb-history-segments");
    auto config = HistoryConfig(dir.path());
    config.geometry = chunkdb::GeometryConfig{
        .large_chunk_width_chunks = 2,
        .large_chunk_height_chunks = 2,
        .chunk_width_blocks = 64,
        .chunk_height_blocks = 64,
        .block_bits = 32,
    };
    config.durability_mode = chunkdb::DurabilityMode::kRelaxed;
    config.checkpoint_update_interval = 512;
    chunkdb::ChunkStore store(config);
    const auto& geometry = store.geometry();
    // A keyframe of this state takes about 16 KiB: one for every 128 KiB of
    // records, two segments.
    (void)store.SetChunkStateBytes(
        0, 0, RandomBytes(geometry.ChunkPayloadBytes()), chunkdb::FullPresenceBitmap(geometry));
    for (int i = 0; i < 30000; ++i) {
        store.SetBlockBits(static_cast<std::int64_t>(Next(64)), static_cast<std::int64_t>(Next(64)), RandomBits(32));
    }
    store.CheckpointForTests(0, 0);
    const auto on_disk = ReadHistory(store, {0, 0});
    assert(on_disk.end == StoreState(store, {0, 0}));
    const auto& segments = on_disk.chunk.segments;
    assert(segments.size() >= 3U);
    // The chunk was empty where its history starts: no keyframe.
    assert(segments.front().first && !segments.front().keyframe);
    std::uint64_t since_keyframe = 0;
    for (std::size_t i = 0; i < segments.size(); ++i) {
        const std::size_t records = segments[i].size - segments[i].header_size;
        if (i > 0) {
            assert(segments[i - 1].size - segments[i - 1].header_size >= history::kSegmentTargetRecordBytes);
            const std::size_t keyframe = segments[i].header_size - history::kSegmentHeaderSize;
            assert(segments[i].keyframe == (since_keyframe >= history::kKeyframeRatio * keyframe) ||
                   !segments[i].keyframe);
            assert(segments[i].keyframe || since_keyframe < history::kKeyframeRatio * 16000U);
        }
        since_keyframe = (segments[i].keyframe ? 0U : since_keyframe) + records;
    }
    const auto keyframes = std::count_if(segments.begin(), segments.end(), [](const auto& s) { return s.keyframe; });
    assert(keyframes >= 1 && static_cast<std::size_t>(keyframes) + 1U < segments.size());

    // A segment missing from the chain is damage.
    std::filesystem::remove(segments[1].path);
    const history::HistoryFiles files(store.data_dir(), store.geometry(), store.store_id(), store.history_start());
    ExpectThrow<history::HistoryDamagedError>(
        [&] { (void)files.Load({0, 0}, false); }, "does not start where the segment before it ends");
}

// History enabled on a table with data starts from that data: the first
// segment's keyframe holds it, and every event is at or above history_start.
void TestEnableOnExistingData() {
    ScopedTempDir dir("chunkdb-history-enable-data");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    config.default_geometry = kGeometry;
    config.default_options.extra_max_block_bits = 16;
    chunkdb::TableCatalog catalog(config);
    const auto table = catalog.Find("default");
    history::ChunkState at_enable;
    {
        auto lease = table->Acquire();
        auto& store = lease->store();
        store.SetBlockBits(0, 0, "0011");
        store.SetBlockBits(1, 0, "0111");
        (void)store.PutBlockExtra(1, 0, chunkdb::ExtraValue{.bit_length = 9, .bytes = {0xFF, 0x01}});
        store.CheckpointForTests(0, 0);
        store.SetBlockBits(2, 0, "1111");  // still in the WAL at enable
        at_enable = StoreState(store, {0, 0});
    }
    chunkdb::TableOptionsUpdate enable;
    enable.history = true;
    catalog.SetOptions("default", enable);
    auto lease = table->Acquire();
    auto& store = lease->store();
    const std::uint64_t start = store.history_start();
    store.UnsetBlock(0, 0, Bytes{0x01});
    store.SetBlockBits(3, 0, "1000", Bytes{0x02});
    store.CheckpointForTests(0, 0);
    const auto on_disk = ReadHistory(store, {0, 0});
    assert(on_disk.base == at_enable);
    assert(on_disk.chunk.segments.front().keyframe && on_disk.chunk.segments.front().first);
    assert(on_disk.chunk.segments.front().base_revision < start);
    assert((Tags(on_disk.mutations) == std::vector<Bytes>{{0x01}, {0x02}}));
    assert(on_disk.mutations.front().revision >= start);
    assert(on_disk.end == StoreState(store, {0, 0}));
}

std::vector<std::string> FileNames(const std::filesystem::path& dir) {
    std::vector<std::string> names;
    if (std::filesystem::exists(dir)) {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(dir)) {
            names.push_back(entry.path().filename().string());
        }
    }
    return names;
}

// A failed append, damaged history and files that do not replay to the
// chunk's state stop the checkpoint; the WAL keeps every write.
void TestCheckpointFailures() {
    {
        ScopedTempDir dir("chunkdb-history-append-fail");
        chunkdb::ChunkStore store(HistoryConfig(dir.path()));
        std::vector<Bytes> tags;
        for (int i = 0; i < 6; ++i) {
            tags.push_back(Bytes{static_cast<std::uint8_t>(i)});
            store.SetBlockBits(i, 0, "0001", tags.back());
        }
        store.CheckpointForTests(0, 0);
        for (int i = 6; i < 12; ++i) {
            tags.push_back(Bytes{static_cast<std::uint8_t>(i)});
            store.SetBlockBits(i % 8, 1, "0011", tags.back());
        }
        SetEnvVar("CHUNKDB_FAILPOINT_HISTORY_APPEND_FAIL_ONCE", "1");
        ExpectThrow<std::runtime_error>([&] { store.CheckpointForTests(0, 0); }, "injected history append failure");
        assert(std::filesystem::exists(chunkdb::ChunkWalPath(dir.path(), store.geometry(), {0, 0})));
        tags.push_back(Bytes{0x40});
        store.SetBlockBits(7, 7, "1111", tags.back());
        store.CheckpointForTests(0, 0);
        const auto on_disk = ReadHistory(store, {0, 0});
        assert(Tags(on_disk.mutations) == tags);
        assert(on_disk.end == StoreState(store, {0, 0}));
    }
    {
        // A failure after a new segment is published: the next checkpoint
        // reads it back instead of publishing it again.
        ScopedTempDir dir("chunkdb-history-publish-fail");
        chunkdb::ChunkStore store(HistoryConfig(dir.path()));
        store.SetBlockBits(0, 0, "0001", Bytes{0x01});
        SetEnvVar("CHUNKDB_FAILPOINT_HISTORY_AFTER_SEGMENT_PUBLISH_FAIL_ONCE", "1");
        ExpectThrow<std::runtime_error>([&] { store.CheckpointForTests(0, 0); }, "injected failure after publishing");
        store.SetBlockBits(1, 0, "0001", Bytes{0x02});
        store.CheckpointForTests(0, 0);
        assert((Tags(ReadHistory(store, {0, 0}).mutations) == std::vector<Bytes>{{0x01}, {0x02}}));
    }
    {
        // Bytes the store did not write after the newest segment's records.
        ScopedTempDir dir("chunkdb-history-foreign-bytes");
        chunkdb::ChunkStore store(HistoryConfig(dir.path()));
        store.SetBlockBits(0, 0, "0001");
        store.CheckpointForTests(0, 0);
        const auto segment = ReadHistory(store, {0, 0}).chunk.segments.front().path;
        std::ofstream(segment, std::ios::binary | std::ios::app).put('\x07');
        store.SetBlockBits(1, 0, "0001");
        ExpectThrow<history::HistoryDamagedError>(
            [&] { store.CheckpointForTests(0, 0); }, "changed outside this store");
    }
    {
        // A damaged segment fails the chunk's history closed.
        ScopedTempDir dir("chunkdb-history-damaged");
        auto config = HistoryConfig(dir.path());
        std::filesystem::path segment;
        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "0001");
            store.CheckpointForTests(0, 0);
            segment = ReadHistory(store, {0, 0}).chunk.segments.front().path;
            store.SetBlockBits(1, 0, "0001");
        }
        auto bytes = ReadFile(segment);
        bytes[bytes.size() - 3] ^= 0x10U;
        std::ofstream(segment, std::ios::binary | std::ios::trunc)
            .write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        chunkdb::ChunkStore store(config);
        ExpectThrow<chunkdb::history::HistoryDamagedError>([&] { store.CheckpointForTests(0, 0); }, "is damaged");
        assert(store.GetBlockBits(1, 0) == "0001");
        assert(std::filesystem::exists(chunkdb::ChunkWalPath(dir.path(), store.geometry(), {0, 0})));
    }
    {
        // An image that holds mutations its history lacks.
        ScopedTempDir dir("chunkdb-history-lost");
        auto config = HistoryConfig(dir.path());
        {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "0001");
            store.CheckpointForTests(0, 0);
        }
        std::filesystem::remove_all(dir.path() / "history");
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(1, 0, "0001");
        ExpectThrow<chunkdb::history::HistoryDamagedError>(
            [&] { store.CheckpointForTests(0, 0); }, "but its image holds revision");
    }
    {
        // Files that no longer replay to the chunk's state: fail closed.
        ScopedTempDir dir("chunkdb-history-mismatch");
        chunkdb::ChunkStore store(HistoryConfig(dir.path()));
        store.SetBlockBits(0, 0, "0001");
        store.CheckpointForTests(0, 0);
        store.SetBlockBits(1, 0, "0001");
        std::filesystem::remove(chunkdb::ChunkWalPath(dir.path(), store.geometry(), {0, 0}));
        ExpectThrow<std::runtime_error>([&] { store.CheckpointForTests(0, 0); }, "do not replay to the state");
        ExpectThrow<std::runtime_error>([&] { store.SetBlockBits(2, 0, "0001"); }, "");
        assert(ReadHistory(store, {0, 0}).mutations.size() == 1U);
    }
}

// Crash points around the history append; a child process dies at each.
int RunCrashChild(const std::filesystem::path& dir, const std::string& action) {
    chunkdb::ChunkStore store(HistoryConfig(dir));
    const int first_batch = action == "segment" ? 10 : 20;
    for (int i = 0; i < first_batch; ++i) {
        if (action != "segment" && i == 10) {
            store.CheckpointForTests(0, 0);
        }
        store.SetBlockBits(i % 8, i / 8, i % 2 == 0 ? "0110" : "1001", Bytes{static_cast<std::uint8_t>(i)});
    }
    store.CheckpointForTests(0, 0);
    return 0;
}

void TestCrashBoundaries(const std::string& executable) {
    for (const auto& [action, failpoint] : std::vector<std::pair<std::string, const char*>>{
             {"segment", "CHUNKDB_FAILPOINT_CRASH_HISTORY_SEGMENT_BEFORE_PUBLISH_ONCE"},
             {"segment", "CHUNKDB_FAILPOINT_CRASH_HISTORY_SEGMENT_AFTER_PUBLISH_ONCE"},
             {"append", "CHUNKDB_FAILPOINT_CRASH_HISTORY_AFTER_APPEND_ONCE"},
             {"append", "CHUNKDB_FAILPOINT_CRASH_HISTORY_PARTIAL_APPEND_ONCE"},
         }) {
        ScopedTempDir dir("chunkdb-history-crash");
        std::string command = "\"" + executable + "\" --crash-child \"" + dir.path().string() + "\" " + action;
#ifdef _WIN32
        command = "\"" + command + "\"";
#endif
        SetEnvVar(failpoint, "1");
        const int status = std::system(command.c_str());
        SetEnvVar(failpoint, "");
#ifdef _WIN32
        assert(status == 86);
#else
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 86);
#endif
        const int written = action == "segment" ? 10 : 20;
        chunkdb::ChunkStore store(HistoryConfig(dir.path()));
        for (int i = 0; i < written; ++i) {
            assert(store.GetBlockBits(i % 8, i / 8) == (i % 2 == 0 ? "0110" : "1001"));
        }
        std::vector<Bytes> tags;
        for (int i = 0; i < written + 3; ++i) {
            tags.push_back(Bytes{static_cast<std::uint8_t>(i)});
        }
        for (int i = written; i < written + 3; ++i) {
            store.SetBlockBits(i % 8, 7, "1111", tags[static_cast<std::size_t>(i)]);
        }
        store.CheckpointForTests(0, 0);
        const auto on_disk = ReadHistory(store, {0, 0});
        assert(Tags(on_disk.mutations) == tags);
        assert(on_disk.end == StoreState(store, {0, 0}));
        for (const auto& name : FileNames(dir.path() / "history")) {
            assert(name.find(".tmp.") == std::string::npos);
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::string(argv[1]) == "--crash-child") {
        return RunCrashChild(argv[2], argv[3]);
    }
    chunkdb::SetLogLevel(chunkdb::LogLevel::kError);
    TestManifestOptions();
    TestStoreOptions();
    TestStoreTags();
    TestCatalogHistoryStart();
    TestEnableNotDurable();
    TestProtocol();
    TestCheckpointRecordsEveryMutation();
    TestExtraOnlyReplace();
    TestHistoryAcrossRestartAndEviction();
    TestSegmentsAndKeyframes();
    TestEnableOnExistingData();
    TestCheckpointFailures();
    TestCrashBoundaries(argv[0]);
    std::puts("history tests passed");
    return 0;
}
