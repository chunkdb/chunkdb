// Block history (#45): the table options and the feature flag, where history
// starts, tags from the protocol to the WAL.

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feature_flags.hpp"
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

}  // namespace

int main() {
    chunkdb::SetLogLevel(chunkdb::LogLevel::kError);
    TestManifestOptions();
    TestStoreOptions();
    TestStoreTags();
    TestCatalogHistoryStart();
    TestEnableNotDurable();
    TestProtocol();
    std::puts("history tests passed");
    return 0;
}
