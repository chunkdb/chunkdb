// Text and bytes columns (#61): the VARS container and codec, the image
// section, the WAL records and their replay checks, typed block values with
// NULL, empty values, defaults and limits, durability across restart and
// checkpoint, rollback, whole-chunk writes, and offline verification.

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/chunk_vars.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/table_catalog.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "verify.hpp"
#include "wal_replay.hpp"
#include "wal_writer.hpp"

namespace {

using Bytes = std::vector<std::uint8_t>;
using chunkdb::BytesValue;
using chunkdb::ChunkVars;
using chunkdb::Column;
using chunkdb::ColumnKind;
using chunkdb::ColumnType;
using chunkdb::ColumnValue;
using chunkdb::TableSchema;
using chunkdb::VarChange;
using chunkdb::VarKey;
using chunkdb::test::ScopedTempDir;

void SetEnvVar(const char* key, const char* value) {
#ifdef _WIN32
    const int rc = _putenv_s(key, value);
#else
    const int rc = setenv(key, value, 1);
#endif
    assert(rc == 0);
    (void)rc;
}

void UnsetEnvVar(const char* key) {
#ifdef _WIN32
    const int rc = _putenv_s(key, "");
#else
    const int rc = unsetenv(key);
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

void Le16(Bytes* out, std::uint16_t v) {
    for (int i = 0; i < 2; ++i) out->push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}
void Le32(Bytes* out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out->push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}
void Le64(Bytes* out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) out->push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}

Bytes ReadFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void WriteFile(const std::filesystem::path& path, const Bytes& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(out);
}

Bytes B(const std::string& text) {
    return Bytes(text.begin(), text.end());
}

// One encoded VARS entry, any content.
Bytes Entry(std::uint32_t column_id, std::uint32_t block_index, const Bytes& value) {
    Bytes out;
    Le32(&out, column_id);
    Le32(&out, block_index);
    Le32(&out, static_cast<std::uint32_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
    return out;
}

Bytes Concat(std::initializer_list<Bytes> parts) {
    Bytes out;
    for (const auto& part : parts) {
        out.insert(out.end(), part.begin(), part.end());
    }
    return out;
}

Column MakeColumn(std::uint32_t id, std::string name, ColumnKind kind, std::uint32_t size) {
    Column column;
    column.id = id;
    column.name = std::move(name);
    column.type = ColumnType{.kind = kind, .size = size};
    return column;
}

// id u4 REQUIRED, sign text(8) NULL, blob bytes(6), note text(5) DEFAULT 'hi'.
TableSchema Schema() {
    auto id = MakeColumn(1, "id", ColumnKind::kUnsigned, 4);
    id.required = true;
    auto sign = MakeColumn(2, "sign", ColumnKind::kText, 8);
    sign.nullable = true;
    auto note = MakeColumn(4, "note", ColumnKind::kText, 5);
    note.has_default = true;
    note.default_value = B("hi");
    return TableSchema{
        .version = 1,
        .next_column_id = 5,
        .columns = {id, sign, MakeColumn(3, "blob", ColumnKind::kBytes, 6), note},
    };
}

constexpr chunkdb::GeometryConfig kGeometry{
    .large_chunk_width_chunks = 2,
    .large_chunk_height_chunks = 2,
    .chunk_width_blocks = 4,
    .chunk_height_blocks = 4,
    .block_bits = 4,
};
const chunkdb::Geometry kSmall(kGeometry, Schema());
constexpr std::size_t kPayloadBytes = 8;
const chunkdb::StoreId kStoreId = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
const chunkdb::ChunkCoord kCoord{-3, 7};

// --- container and codec ---------------------------------------------------

void TestContainerAgainstModel() {
    std::uint64_t state = 0x9E3779B97F4A7C15ULL;
    const auto next = [&state](std::uint64_t bound) {
        state ^= state << 13U;
        state ^= state >> 7U;
        state ^= state << 17U;
        return state % bound;
    };
    const auto random_key = [&] {
        return VarKey{.column_id = 1U + static_cast<std::uint32_t>(next(3)), .block_index = static_cast<std::uint32_t>(next(24))};
    };
    const auto random_value = [&](std::size_t seed) {
        Bytes value(next(40));
        for (std::size_t i = 0; i < value.size(); ++i) {
            value[i] = static_cast<std::uint8_t>(seed + i * 37U);
        }
        return value;
    };
    const auto check = [](const ChunkVars& vars, const std::map<std::uint64_t, Bytes>& model) {
        assert(vars.size() == model.size());
        std::size_t size = 0;
        std::size_t i = 0;
        for (const auto& [packed, value] : model) {
            const VarKey key{.column_id = static_cast<std::uint32_t>(packed >> 32U),
                             .block_index = static_cast<std::uint32_t>(packed)};
            const auto found = vars.Find(key);
            assert(found.has_value() && Bytes(found->begin(), found->end()) == value);
            const auto entry = vars.entry(i++);
            assert(entry.key == key && Bytes(entry.value.begin(), entry.value.end()) == value);
            size += chunkdb::kVarEntryHeaderBytes + value.size();
        }
        assert(vars.encoded_size() == size);
        assert(ChunkVars::Decode(vars.Encode().data(), vars.encoded_size(), 24) == vars);
    };
    ChunkVars vars;
    std::map<std::uint64_t, Bytes> model;
    for (int step = 0; step < 4000; ++step) {
        const auto key = random_key();
        if (next(3) == 0U) {
            assert(vars.Remove(key) == (model.erase(key.packed()) == 1U));
            assert(!vars.Find(key).has_value());
        } else {
            const auto value = random_value(static_cast<std::size_t>(step));
            vars.Assign(key, value);
            model[key.packed()] = value;
        }
        check(vars, model);
        if (step % 50 == 0) {
            // A random update and its undo restore the exact encoding.
            const ChunkVars before = vars;
            chunkdb::VarUpdate update;
            for (std::uint32_t column = 1; column <= 3; ++column) {
                for (std::uint32_t block = 0; block < 24; block += 1 + static_cast<std::uint32_t>(next(6))) {
                    const VarKey key{.column_id = column, .block_index = block};
                    const auto it = model.find(key.packed());
                    if (it != model.end() && next(2) == 0U) {
                        update.changes.push_back(VarChange{.key = key, .value = std::nullopt});
                    } else if (next(2) == 0U) {
                        auto value = random_value(block + static_cast<std::size_t>(step));
                        if (it == model.end() || it->second != value) {
                            update.changes.push_back(VarChange{.key = key, .value = std::move(value)});
                        }
                    }
                }
            }
            const auto expected_size = chunkdb::VarsSizeAfter(vars, update);
            auto applied = update;
            auto undo = chunkdb::ApplyVarUpdate(&vars, std::move(applied));
            assert(vars.encoded_size() == expected_size);
            for (const auto& change : update.changes) {
                const auto found = vars.Find(change.key);
                assert(found.has_value() == change.value.has_value());
                if (change.value.has_value()) {
                    assert(Bytes(found->begin(), found->end()) == *change.value);
                }
            }
            chunkdb::UndoVarUpdate(&vars, std::move(undo));
            assert(vars == before);
            check(vars, model);
        }
    }
    // A change that changes nothing, or changes out of order, are writer bugs.
    ChunkVars small;
    small.Assign({.column_id = 2, .block_index = 1}, B("x"));
    ExpectThrow<std::logic_error>(
        [&] {
            (void)chunkdb::ApplyVarUpdate(
                &small, chunkdb::VarUpdate{.changes = {VarChange{.key = {2, 1}, .value = B("x")}}, .replace = {}});
        },
        "does not change anything");
    ExpectThrow<std::logic_error>(
        [&] {
            (void)chunkdb::ApplyVarUpdate(
                &small, chunkdb::VarUpdate{
                            .changes = {VarChange{.key = {3, 0}, .value = B("a")}, VarChange{.key = {2, 5}, .value = B("b")}},
                            .replace = {}});
        },
        "not in strictly ascending key order");
}

void TestDecodeErrors() {
    const auto decode = [](const Bytes& bytes) { return ChunkVars::Decode(bytes.data(), bytes.size(), 16); };
    assert(decode(Concat({Entry(1, 3, B("a")), Entry(1, 4, {}), Entry(2, 0, B("bc"))})).size() == 3U);
    ExpectThrow<std::invalid_argument>([&] { (void)decode(Bytes{1, 0, 0}); }, "header extends past the section");
    ExpectThrow<std::invalid_argument>(
        [&] {
            auto cut = Entry(1, 3, B("abc"));
            cut.pop_back();
            (void)decode(cut);
        },
        "extends past the section");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)decode(Concat({Entry(2, 0, B("a")), Entry(1, 5, B("b"))})); }, "not strictly ascending");
    ExpectThrow<std::invalid_argument>(
        [&] { (void)decode(Concat({Entry(1, 5, B("a")), Entry(1, 5, B("b"))})); }, "not strictly ascending");
    ExpectThrow<std::invalid_argument>([&] { (void)decode(Entry(1, 16, B("a"))); }, "the chunk has 16 blocks");
    ExpectThrow<std::invalid_argument>([] { chunkdb::RequireValidVarLimit(12); }, "var_max_chunk_bytes must be between 13");
    ExpectThrow<std::invalid_argument>(
        [] { chunkdb::RequireValidVarLimit(chunkdb::kVarMaxChunkBytesLimit + 1U); }, "var_max_chunk_bytes must be between");
}

// The rules a stored VARS section must follow for its table's columns.
void TestValidityRules() {
    const auto& layout = kSmall.layout();
    const Bytes presence = {0x0F, 0x00};  // blocks 0..3 present
    const auto check = [&](const Bytes& section) {
        const auto vars = ChunkVars::Decode(section.data(), section.size(), 16);
        layout.RequireValidVars(vars, presence);
    };
    check(Concat({Entry(2, 0, {}), Entry(2, 1, B("Ünï")), Entry(3, 2, Bytes{0, 255}), Entry(4, 3, B("hey"))}));
    ExpectThrow<std::invalid_argument>([&] { check(Entry(1, 0, B("x"))); }, "no text or bytes column has this id");
    ExpectThrow<std::invalid_argument>([&] { check(Entry(9, 0, B("x"))); }, "no text or bytes column has this id");
    ExpectThrow<std::invalid_argument>([&] { check(Entry(2, 4, B("x"))); }, "a value of an absent block");
    ExpectThrow<std::invalid_argument>([&] { check(Entry(2, 0, B("123456789"))); }, "longer than text(8)");
    ExpectThrow<std::invalid_argument>([&] { check(Entry(3, 0, {})); }, "empty value stored in a column that cannot be NULL");
    ExpectThrow<std::invalid_argument>([&] { check(Entry(2, 0, Bytes{0xC0, 0x80})); }, "text that is not UTF-8");
}

// --- image -----------------------------------------------------------------

void TestImageSection() {
    ChunkVars vars;
    vars.Assign({.column_id = 2, .block_index = 1}, B("sign"));
    vars.Assign({.column_id = 3, .block_index = 0}, Bytes{1, 2, 3});
    const Bytes payload(kPayloadBytes, 0x11U);
    const Bytes presence = {0x03, 0x00};
    for (const auto compression : {chunkdb::CheckpointCompression::kNone, chunkdb::CheckpointCompression::kZrle}) {
        const auto bytes =
            chunkdb::SerializeChunkImage(kSmall, kCoord, payload, presence, compression, 42, 1000, kStoreId, &vars);
        const auto image = chunkdb::ParseChunkImage(bytes, kSmall, kCoord, kStoreId, {});
        assert(image.vars == vars && image.payload == payload && image.presence_bitmap == presence);
        // Without values there is no VARS section.
        const ChunkVars none;
        const auto plain =
            chunkdb::SerializeChunkImage(kSmall, kCoord, payload, presence, compression, 42, 1000, kStoreId, &none);
        assert(plain.size() < bytes.size());
        assert(chunkdb::ParseChunkImage(plain, kSmall, kCoord, kStoreId, {}).vars.empty());
    }
    // A section that breaks the table's rules is damage.
    const auto with_section = [&](const Bytes& section) {
        auto decoded = ChunkVars::Decode(section.data(), section.size(), 16);
        return chunkdb::SerializeChunkImage(
            kSmall, kCoord, payload, presence, chunkdb::CheckpointCompression::kNone, 42, 1000, kStoreId, &decoded);
    };
    ExpectThrow<std::runtime_error>(
        [&] { (void)chunkdb::ParseChunkImage(with_section(Entry(2, 5, B("x"))), kSmall, kCoord, kStoreId, {}); },
        "chunk image VARS section is damaged: column 2 block 5: a value of an absent block");
    auto damaged = with_section(Entry(2, 1, B("x")));
    damaged.back() ^= 0x01U;
    ExpectThrow<std::runtime_error>(
        [&] { (void)chunkdb::ParseChunkImage(damaged, kSmall, kCoord, kStoreId, {}); }, "section 3 checksum mismatch");
}

// --- WAL -------------------------------------------------------------------

using Record = std::pair<std::uint8_t, Bytes>;

Bytes SpanBody(std::uint32_t offset, const Bytes& bytes) {
    Bytes out;
    Le32(&out, offset);
    out.insert(out.end(), bytes.begin(), bytes.end());
    return out;
}

Bytes DelBody(std::uint32_t column_id, std::uint32_t block_index) {
    Bytes out;
    Le32(&out, column_id);
    Le32(&out, block_index);
    return out;
}

// A frame with any records, so the writer's ordering rules can be broken.
Bytes RawFrame(std::uint64_t revision, const std::vector<Record>& records) {
    Bytes body;
    for (const auto& [type, record] : records) {
        body.push_back(type);
        Le32(&body, static_cast<std::uint32_t>(record.size()));
        body.insert(body.end(), record.begin(), record.end());
    }
    Bytes frame = {'F', 'R', 'M', '2'};
    Le64(&frame, revision);
    Le64(&frame, 1000U + revision);
    Le16(&frame, 0);
    Le16(&frame, 0);
    Le32(&frame, static_cast<std::uint32_t>(records.size()));
    Le32(&frame, static_cast<std::uint32_t>(body.size()));
    Le32(&frame, chunkdb::Crc32(frame.data() + 4, frame.size() - 4));
    frame.insert(frame.end(), body.begin(), body.end());
    Le32(&frame, chunkdb::Crc32(body));
    return frame;
}

struct Replayed {
    chunkdb::WalReplayResult result;
    Bytes payload;
    Bytes presence;
    ChunkVars vars;
};

Replayed Replay(const Bytes& wal, const chunkdb::Geometry& geometry = kSmall) {
    Replayed out;
    out.payload.assign(geometry.ChunkPayloadBytes(), 0U);
    out.presence.assign(2, 0U);
    out.result = chunkdb::ReplayWal(wal, geometry, kCoord, kStoreId, {}, 0, &out.payload, &out.presence, &out.vars);
    return out;
}

Bytes Wal(const std::vector<Bytes>& frames) {
    auto wal = chunkdb::BuildWalHeader(kCoord, kStoreId, {});
    for (const auto& frame : frames) {
        wal.insert(wal.end(), frame.begin(), frame.end());
    }
    return wal;
}

void TestWalRecords() {
    ChunkVars replacement;
    replacement.Assign({.column_id = 3, .block_index = 1}, B("z"));
    // Blocks 0 and 1 become present with values; then a value changes and
    // another goes; then all of them are replaced.
    const auto f1 = RawFrame(
        1,
        {{chunkdb::kWalRecordSpan, SpanBody(kPayloadBytes, {0x03, 0x00})},
         {chunkdb::kWalRecordVarPut, Entry(2, 0, B("ab"))},
         {chunkdb::kWalRecordVarPut, Entry(2, 1, {})},
         {chunkdb::kWalRecordVarPut, Entry(4, 0, B("hi"))}});
    const auto f2 =
        RawFrame(2, {{chunkdb::kWalRecordVarDel, DelBody(2, 0)}, {chunkdb::kWalRecordVarPut, Entry(2, 1, B("cd"))}});
    const auto f3 = RawFrame(3, {{chunkdb::kWalRecordVarReplace, replacement.Encode()}});
    const auto good = Wal({f1, f2, f3});
    {
        const auto r = Replay(good);
        assert(r.result.replayable && !r.result.tail_truncated_or_corrupt && r.result.vars_problem.empty());
        assert(r.result.applied_frames == 3U && r.vars == replacement);
        const auto two = Replay(Wal({f1, f2}));
        assert(two.vars.size() == 2U && two.vars.Find({2, 1}).has_value() && !two.vars.Find({2, 0}).has_value());
    }

    // The writer produces the same records.
    {
        Bytes batch;
        chunkdb::WalFrameBuilder frame(&batch);
        frame.AppendSpan(kPayloadBytes, Bytes{0x03, 0x00}.data(), 2);
        frame.AppendVarPut({2, 0}, B("ab"));
        frame.AppendVarPut({2, 1}, {});
        frame.AppendVarPut({4, 0}, B("hi"));
        (void)frame.Finish(1, 1001);
        assert(batch == f1);
        Bytes replace_batch;
        chunkdb::WalFrameBuilder replace_frame(&replace_batch);
        replace_frame.AppendVarReplace(replacement);
        (void)replace_frame.Finish(3, 1003);
        assert(replace_batch == f3);
        Bytes scratch;
        chunkdb::WalFrameBuilder unordered(&scratch);
        unordered.AppendVarDel({3, 4});
        ExpectThrow<std::logic_error>([&] { unordered.AppendVarPut({2, 9}, B("x")); }, "out of order");
        ExpectThrow<std::logic_error>([&] { unordered.AppendVarReplace(replacement); }, "out of order");
    }

    // Each malformed frame stops replay with nothing of it applied.
    const auto expect_stop = [&](const Bytes& bad, const std::string& reason) {
        auto wal = good;
        wal.insert(wal.end(), bad.begin(), bad.end());
        const auto r = Replay(wal);
        if (r.result.stop_reason != reason) {
            std::fprintf(stderr, "expected %s, got %s\n", reason.c_str(), r.result.stop_reason.c_str());
            assert(false);
        }
        assert(r.result.tail_truncated_or_corrupt && !r.result.stopped_at_crash_tail);
        assert(r.result.applied_frames == 3U && r.vars == replacement);
    };
    expect_stop(
        RawFrame(4, {{chunkdb::kWalRecordVarPut, Entry(3, 1, B("a"))}, {chunkdb::kWalRecordVarPut, Entry(2, 1, B("a"))}}),
        "record_vars_order");
    expect_stop(
        RawFrame(4, {{chunkdb::kWalRecordVarPut, Entry(2, 1, B("a"))}, {chunkdb::kWalRecordVarDel, DelBody(2, 1)}}),
        "record_vars_order");
    expect_stop(
        RawFrame(4, {{chunkdb::kWalRecordVarReplace, {}}, {chunkdb::kWalRecordVarDel, DelBody(2, 1)}}),
        "record_vars_order");
    expect_stop(RawFrame(4, {{chunkdb::kWalRecordVarDel, Bytes{1, 0, 0}}}), "record_vars_invalid");
    {
        auto put = Entry(2, 1, B("abc"));
        put.pop_back();
        expect_stop(RawFrame(4, {{chunkdb::kWalRecordVarPut, put}}), "record_vars_invalid");
    }
    expect_stop(RawFrame(4, {{chunkdb::kWalRecordVarDel, DelBody(2, 16)}}), "record_out_of_range");
    expect_stop(
        RawFrame(4, {{chunkdb::kWalRecordVarReplace, Concat({Entry(3, 1, B("a")), Entry(2, 0, B("a"))})}}),
        "record_vars_invalid");

    // The rules hold for the state replay ends in, not per frame.
    {
        auto wal = good;
        const auto bad = RawFrame(4, {{chunkdb::kWalRecordVarPut, Entry(2, 7, B("a"))}});
        wal.insert(wal.end(), bad.begin(), bad.end());
        const auto r = Replay(wal);
        assert(!r.result.tail_truncated_or_corrupt && Contains(r.result.vars_problem, "a value of an absent block"));
    }

    // A table without text or bytes columns has no value records.
    const chunkdb::Geometry bits(kGeometry);
    const auto r = Replay(Wal({RawFrame(1, {{chunkdb::kWalRecordVarDel, DelBody(2, 0)}})}), bits);
    assert(r.result.stop_reason == "record_vars_without_columns");
}

// --- store -----------------------------------------------------------------

chunkdb::StoreConfig Config(
    const std::filesystem::path& dir,
    std::size_t checkpoint_update_interval = 256,
    std::size_t var_max_chunk_bytes = 4096) {
    chunkdb::StoreConfig config;
    config.geometry = kGeometry;
    config.schema = Schema();
    config.data_dir = dir;
    config.checkpoint_update_interval = checkpoint_update_interval;
    config.var_max_chunk_bytes = var_max_chunk_bytes;
    return config;
}

std::vector<ColumnValue> Row(std::uint64_t id, ColumnValue sign, Bytes blob, std::string note) {
    return {id, std::move(sign), BytesValue{.bytes = std::move(blob)}, std::move(note)};
}

void ExpectRow(chunkdb::ChunkStore& store, std::int64_t x, std::int64_t y, const std::optional<std::vector<ColumnValue>>& want) {
    if (store.GetBlock(x, y) != want) {
        std::fprintf(stderr, "block (%lld,%lld) does not hold the expected values\n", static_cast<long long>(x),
                     static_cast<long long>(y));
        assert(false);
    }
}

void TestBlockValues() {
    ScopedTempDir dir("chunkdb-vars-values");
    chunkdb::ChunkStore store(Config(dir.path(), 256, 64));
    // A new block: sign NULL, blob empty, note its default.
    store.SetBlock(0, 0, {{"id", std::uint64_t{1}}});
    ExpectRow(store, 0, 0, Row(1, std::monostate{}, {}, "hi"));
    store.SetBlock(0, 0, {{"sign", std::string("hello")}, {"blob", BytesValue{.bytes = {0, 1, 2}}}, {"note", std::string()}});
    ExpectRow(store, 0, 0, Row(1, std::string("hello"), {0, 1, 2}, ""));
    // An empty text in a NULL column is not NULL.
    store.SetBlock(1, 0, {{"id", std::uint64_t{2}}, {"sign", std::string()}});
    ExpectRow(store, 1, 0, Row(2, std::string(), {}, "hi"));
    store.SetBlock(1, 0, {{"sign", std::monostate{}}});
    ExpectRow(store, 1, 0, Row(2, std::monostate{}, {}, "hi"));

    // Refused values change nothing.
    const auto version = store.GetChunkVersion(0, 0);
    ExpectThrow<std::invalid_argument>([&] { store.SetBlock(0, 0, {{"sign", std::string("123456789")}}); }, "too long");
    ExpectThrow<std::invalid_argument>(
        [&] { store.SetBlock(0, 0, {{"sign", std::string("\xC0\x80")}}); }, "the text is not UTF-8");
    ExpectThrow<std::invalid_argument>([&] { store.SetBlock(0, 0, {{"blob", std::monostate{}}}); }, "cannot be NULL");
    ExpectThrow<std::invalid_argument>(
        [&] { store.SetBlock(0, 0, {{"blob", std::string("text")}}); }, "column blob is bytes(6), not text");
    ExpectThrow<std::invalid_argument>(
        [&] { store.SetBlock(0, 0, {{"sign", std::string("ok")}, {"id", std::uint64_t{16}}}); }, "out of range");
    ExpectRow(store, 0, 0, Row(1, std::string("hello"), {0, 1, 2}, ""));
    assert(store.GetChunkVersion(0, 0) == version);
    // Writing the values a block has is not a write.
    store.SetBlock(0, 0, {{"sign", std::string("hello")}, {"note", std::string()}});
    assert(store.GetChunkVersion(0, 0) == version);

    // The chunk's values are bounded by var_max_chunk_bytes (64): each entry
    // takes 12 bytes plus its value.
    store.SetBlock(2, 0, {{"id", std::uint64_t{3}}});
    ExpectThrow<std::invalid_argument>(
        [&] { store.SetBlock(3, 0, {{"id", std::uint64_t{4}}, {"sign", std::string("abcdefgh")}}); },
        "more than var_max_chunk_bytes (64)");
    assert(store.GetBlock(3, 0) == std::nullopt);

    // UNSET removes a block with its values; created again it starts over.
    store.UnsetBlock(0, 0);
    assert(store.GetBlock(0, 0) == std::nullopt);
    store.SetBlock(0, 0, {{"id", std::uint64_t{5}}});
    ExpectRow(store, 0, 0, Row(5, std::monostate{}, {}, "hi"));
}

// Whole-chunk writes keep the values of blocks that stay present and drop
// the others.
void TestWholeChunkWrites() {
    ScopedTempDir dir("chunkdb-vars-chunk");
    {
        chunkdb::ChunkStore store(Config(dir.path()));
        store.SetBlock(0, 0, {{"id", std::uint64_t{1}}, {"sign", std::string("a")}});
        store.SetBlock(1, 0, {{"id", std::uint64_t{2}}, {"sign", std::string("b")}});
        store.SetBlock(2, 0, {{"id", std::uint64_t{3}}, {"sign", std::string("c")}});
        auto payload = store.GetChunkPayloadBytes(0, 0);
        (void)store.SetChunkStateBytes(0, 0, payload, Bytes{0x05, 0x00});
        ExpectRow(store, 0, 0, Row(1, std::string("a"), {}, "hi"));
        assert(store.GetBlock(1, 0) == std::nullopt);
        // A block that a whole-chunk write makes present has no text or bytes
        // values: such writes carry fixed-width bytes only.
        (void)store.SetChunkStateBytes(0, 0, payload, Bytes{0x07, 0x00});
        ExpectRow(store, 1, 0, Row(2, std::monostate{}, {}, ""));
        // The conditional form drops values the same way.
        const auto version = store.GetChunkVersion(0, 0);
        assert(store.CasChunkStateBytes(0, 0, version, payload, Bytes{0x03, 0x00}).ok);
        assert(store.GetBlock(2, 0) == std::nullopt);
    }
    // The dropped values stay dropped after replaying the WAL.
    auto config = Config(dir.path());
    config.schema.reset();
    config.geometry_fields = 0;
    chunkdb::ChunkStore store(config);
    ExpectRow(store, 0, 0, Row(1, std::string("a"), {}, "hi"));
    ExpectRow(store, 1, 0, Row(2, std::monostate{}, {}, ""));
    assert(store.GetBlock(2, 0) == std::nullopt);
}

// A lowered var_max_chunk_bytes refuses writes that grow a chunk over it, not
// ones that shrink it.
void TestLoweredLimit() {
    ScopedTempDir dir("chunkdb-vars-limit");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    chunkdb::TableCatalog catalog(config);
    (void)catalog.Create("world", kGeometry, chunkdb::TableOptions{}, Schema());
    {
        auto lease = *catalog.Find("world")->Acquire();
        lease.store().SetBlock(0, 0, {{"id", std::uint64_t{1}}, {"sign", std::string("abcdefgh")}});
        lease.store().SetBlock(1, 0, {{"id", std::uint64_t{2}}});
    }
    // 20 + 14 + 14 bytes stored; the limit drops below that.
    chunkdb::TableOptionsUpdate update;
    update.var_max_chunk_bytes = 40;
    catalog.SetOptions("world", update);
    auto lease = *catalog.Find("world")->Acquire();
    ExpectThrow<std::invalid_argument>(
        [&] { lease.store().SetBlock(2, 0, {{"id", std::uint64_t{3}}}); }, "more than var_max_chunk_bytes (40)");
    lease.store().SetBlock(0, 0, {{"sign", std::string("ab")}});
    ExpectRow(lease.store(), 0, 0, Row(1, std::string("ab"), {}, "hi"));
}

void TestValuesSurviveRestart() {
    for (const std::size_t interval : {std::size_t{1}, std::size_t{100000}}) {
        ScopedTempDir dir("chunkdb-vars-restart");
        {
            chunkdb::ChunkStore store(Config(dir.path(), interval));
            for (std::int64_t x = 0; x < 6; ++x) {
                store.SetBlock(x, 0, {{"id", static_cast<std::uint64_t>(x)}, {"sign", std::string(static_cast<std::size_t>(x), 'q')}});
            }
            store.SetBlock(2, 0, {{"blob", BytesValue{.bytes = {9}}}, {"note", std::string("bye")}});
            store.SetBlock(3, 0, {{"note", std::string()}});
            store.UnsetBlock(4, 0);
        }
        auto config = Config(dir.path(), interval);
        config.schema.reset();
        config.geometry_fields = 0;
        chunkdb::ChunkStore store(config);
        for (std::int64_t x = 0; x < 6; ++x) {
            if (x == 4) {
                assert(store.GetBlock(x, 0) == std::nullopt);
                continue;
            }
            ExpectRow(
                store, x, 0,
                Row(static_cast<std::uint64_t>(x), std::string(static_cast<std::size_t>(x), 'q'),
                    x == 2 ? Bytes{9} : Bytes{}, x == 2 ? "bye" : x == 3 ? "" : "hi"));
        }
    }
}

void TestFailedWriteRollsBack() {
    ScopedTempDir dir("chunkdb-vars-rollback");
    auto config = Config(dir.path());
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    chunkdb::ChunkStore store(config);
    store.SetBlock(0, 0, {{"id", std::uint64_t{7}}, {"sign", std::string("keep")}});
    const auto before = store.GetBlock(0, 0);
    const auto version = store.GetChunkVersion(0, 0);
    const std::vector<std::function<void()>> writes = {
        [&] { store.SetBlock(0, 0, {{"sign", std::string("gone")}, {"blob", BytesValue{.bytes = {1}}}}); },
        [&] { store.SetBlock(0, 0, {{"sign", std::monostate{}}}); },
        [&] { store.SetBlock(0, 0, {{"sign", std::string("longer!")}}); },
        [&] { store.UnsetBlock(0, 0); },
        [&] { (void)store.SetChunkStateBytes(0, 0, store.GetChunkPayloadBytes(0, 0), Bytes{0x00, 0x00}); },
        [&] {
            (void)store.CasChunkStateBytes(
                0, 0, store.GetChunkVersion(0, 0), store.GetChunkPayloadBytes(0, 0), Bytes{0x00, 0x00});
        },
    };
    for (const auto& write : writes) {
        SetEnvVar("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
        bool threw = false;
        try {
            write();
        } catch (const std::exception&) {
            threw = true;
        }
        UnsetEnvVar("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE");
        assert(threw);
        assert(store.GetBlock(0, 0) == before);
        assert(store.GetChunkVersion(0, 0) == version);
    }
}

void TestVerifyFindsDamagedValues() {
    ScopedTempDir dir("chunkdb-vars-verify");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    {
        chunkdb::TableCatalog catalog(config);
        chunkdb::TableOptions options;
        options.checkpoint_update_interval = 1;
        auto lease = *catalog.Create("world", kGeometry, options, Schema())->Acquire();
        lease.store().SetBlock(0, 0, {{"id", std::uint64_t{1}}, {"sign", std::string("a")}});
        lease.store().SetBlock(1, 0, {{"id", std::uint64_t{2}}});
    }
    const auto run_verify = [&] {
        std::ostringstream out;
        const auto counters = chunkdb::VerifyDataDirectory(dir.path(), out);
        return std::make_pair(counters, out.str());
    };
    {
        const auto [counters, out] = run_verify();
        if (counters.errors != 0U || counters.warnings != 0U) {
            std::fprintf(stderr, "%s", out.c_str());
            assert(false);
        }
    }
    const auto table_dir = dir.path() / "tables" / "world";
    const auto manifest = chunkdb::ReadStoreManifest(table_dir);
    const auto image_path = chunkdb::ChunkDataPath(table_dir, kSmall, {0, 0});
    const auto image = chunkdb::ParseChunkImage(ReadFile(image_path), kSmall, {0, 0}, manifest->store_id, manifest->features);
    assert(image.vars.size() == 3U);
    // A value of a block the image has absent.
    auto damaged = image.vars;
    damaged.Assign({.column_id = 2, .block_index = 9}, B("x"));
    WriteFile(
        image_path,
        chunkdb::SerializeChunkImage(
            kSmall, {0, 0}, image.payload, image.presence_bitmap, chunkdb::CheckpointCompression::kNone,
            image.revision, image.commit_time_ms, manifest->store_id, &damaged));
    const auto [counters, out] = run_verify();
    assert(counters.errors != 0U);
    if (!Contains(out, "a value of an absent block")) {
        std::fprintf(stderr, "%s", out.c_str());
        assert(false);
    }
}

}  // namespace

int main() {
    TestContainerAgainstModel();
    TestDecodeErrors();
    TestValidityRules();
    TestImageSection();
    TestWalRecords();
    TestBlockValues();
    TestWholeChunkWrites();
    TestLoweredLimit();
    TestValuesSurviveRestart();
    TestFailedWriteRollsBack();
    TestVerifyFindsDamagedValues();
    return 0;
}
