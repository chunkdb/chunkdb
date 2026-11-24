// Follower tables and APPLY (docs/design/REPLICATION_APPLY.md): a primary
// engine and a follower engine in separate data directories; the primary's
// commits are sent as WATCH t REPLICATION pushes built from its chunk forms.
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/table_catalog.hpp"
#include "replication_push.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;
using Parameters = std::vector<std::optional<std::string>>;

constexpr const char* kColumns = "(n u8, s text(8) NULL) CHUNK 2 x 2";

struct Server {
    test::ScopedTempDir dir;
    std::shared_ptr<TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    std::unique_ptr<SessionState> session;

    explicit Server(const std::string& prefix) : dir(prefix) { Open(); }

    void Open() {
        CatalogConfig config;
        config.data_dir = dir.path();
        catalog = std::make_shared<TableCatalog>(config);
        engine = std::make_unique<CommandEngine>(EngineConfig{.require_auth = false}, catalog);
        session = std::make_unique<SessionState>();
        assert(engine->Execute(*session, "HELLO 3\r\n").front() == '%');
    }

    void Restart() {
        session.reset();
        engine.reset();
        catalog.reset();
        Open();
    }

    std::string Run(const std::string& line, const Parameters& parameters = {}) {
        return engine->Execute(*session, line + "\r\n", parameters);
    }
    std::string Apply(const std::string& push) { return Run("APPLY $1", Parameters{push}); }
};

bool StartsWith(std::string_view text, std::string_view prefix) { return text.substr(0, prefix.size()) == prefix; }

void ExpectError(const std::string& reply, std::string_view prefix) {
    if (!StartsWith(reply, "-ERR " + std::string(prefix))) {
        std::fprintf(stderr, "expected -ERR %.*s, got %s\n", static_cast<int>(prefix.size()), prefix.data(), reply.c_str());
        assert(false);
    }
}

std::string DescribeField(const std::string& describe, std::string_view key) {
    const std::string marker = "$" + std::to_string(key.size()) + "\r\n" + std::string(key) + "\r\n";
    const auto at = describe.find(marker);
    assert(at != std::string::npos);
    const auto value = at + marker.size();
    const auto end = describe.find("\r\n", value);
    if (describe[value] == '$') {
        const auto start = end + 2U;
        return describe.substr(start, std::stoull(describe.substr(value + 1U, end - value - 1U)));
    }
    return describe.substr(value, end - value);
}

// The form of a GET CHUNK reply, and the fields of its header.
std::string Form(const std::string& reply) {
    assert(reply.front() == '$');
    const auto end = reply.find("\r\n");
    return reply.substr(end + 2U, std::stoull(reply.substr(1U, end - 1U)));
}
std::uint64_t Le64(const std::string& bytes, std::size_t at) {
    std::uint64_t value = 0U;
    for (std::size_t i = 0; i < 8U; ++i) value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(bytes[at + i])) << (8U * i);
    return value;
}
std::uint64_t FormVersion(const std::string& form) { return Le64(form, 8U); }
std::uint64_t FormSchema(const std::string& form) { return Le64(form, 16U); }
std::uint64_t FormTime(const std::string& form) { return Le64(form, 24U); }

std::string Bulk(std::string_view text) {
    std::string out;
    Protocol::AppendBulk(out, text);
    return out;
}
std::string Int(std::int64_t value) { return ":" + std::to_string(value) + "\r\n"; }
std::string Null() { return "_\r\n"; }
std::string U64Parameter(std::uint64_t value) {
    std::string bytes(8U, '\0');
    for (std::size_t i = 0; i < 8U; ++i) bytes[i] = static_cast<char>((value >> (8U * i)) & 0xFFU);
    return bytes;
}

struct Chunk {
    std::int64_t cx = 0;
    std::int64_t cy = 0;
    std::uint64_t version = 0;
    std::string data;  // kind and data
};
Chunk FormChunk(std::int64_t cx, std::int64_t cy, std::uint64_t version, const std::optional<std::string>& form) {
    return {cx, cy, version, Bulk("form") + (form ? Bulk(*form) : Null())};
}
struct Row {
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::optional<std::vector<std::optional<std::string>>> after;
};
Chunk RowsChunk(std::int64_t cx, std::int64_t cy, std::uint64_t version, const std::vector<Row>& rows) {
    std::string data = Bulk("rows") + "*2\r\n*" + std::to_string(rows.size()) + "\r\n";
    for (const auto& row : rows) {
        data += "*3\r\n" + Int(row.x) + Int(row.y);
        if (!row.after) { data += Null(); continue; }
        data += "*" + std::to_string(row.after->size()) + "\r\n";
        for (const auto& value : *row.after) data += value ? Bulk(*value) : Null();
    }
    return {cx, cy, version, data + Null()};
}

std::string CommitPush(const std::string& epoch, std::uint64_t revision, std::uint64_t time,
    std::optional<std::string> user, std::uint64_t schema, const std::vector<Chunk>& chunks) {
    std::string push = ">9\r\n" + Bulk("replication") + Bulk(epoch) + Int(static_cast<std::int64_t>(revision)) +
        Int(static_cast<std::int64_t>(time)) + (user ? Bulk(*user) : Null()) + Int(static_cast<std::int64_t>(schema)) +
        "*" + std::to_string(chunks.size()) + "\r\n";
    for (const auto& chunk : chunks)
        push += "*5\r\n" + Int(chunk.cx) + Int(chunk.cy) + Int(static_cast<std::int64_t>(chunk.version)) + chunk.data;
    return push + Null() + Null();
}

std::string ProgressPush(const std::string& epoch, std::uint64_t revision) {
    return ">3\r\n" + Bulk("progress") + Bulk(epoch) + Int(static_cast<std::int64_t>(revision));
}

// A primary with table t and a follower with table f of t's epoch.
struct Pair {
    Server primary{"chunkdb-apply-primary"};
    Server follower{"chunkdb-apply-follower"};
    std::string epoch;
    Pair() {
        assert(primary.Run(std::string("CREATE TABLE t ") + kColumns) == "+OK\r\n");
        epoch = DescribeField(primary.Run("DESCRIBE t"), "epoch");
        assert(epoch.size() == 32U);
        assert(follower.Run(std::string("CREATE TABLE f ") + kColumns + " WITH follower = '" + epoch + "'") == "+OK\r\n");
    }
    // Applies the primary's current form of each chunk as one commit.
    std::string ApplyForms(const std::vector<std::pair<std::int64_t, std::int64_t>>& coords) {
        std::vector<Chunk> chunks;
        std::uint64_t version = 0U, time = 0U, schema = 0U;
        for (const auto& [cx, cy] : coords) {
            const auto form = Form(primary.Run("GET CHUNK " + std::to_string(cx) + " " + std::to_string(cy) + " FROM t"));
            version = FormVersion(form);
            time = FormTime(form);
            schema = FormSchema(form);
            chunks.push_back(FormChunk(cx, cy, version, form));
        }
        return follower.Apply(CommitPush(epoch, version, time, std::nullopt, schema, chunks));
    }
    void ExpectSameChunk(std::int64_t cx, std::int64_t cy) {
        const std::string get = "GET CHUNK " + std::to_string(cx) + " " + std::to_string(cy) + " FROM ";
        assert(primary.Run(get + "t") == follower.Run(get + "f"));
    }
    void ExpectSameRevision() {
        assert(DescribeField(primary.Run("DESCRIBE t"), "revision") == DescribeField(follower.Run("DESCRIBE f"), "revision"));
    }
};

void TestFollowerTable() {
    Pair pair;
    auto& follower = pair.follower;
    const auto describe = follower.Run("DESCRIBE f");
    assert(DescribeField(describe, "epoch") == pair.epoch);
    assert(DescribeField(describe, "revision") == ":0");
    assert(describe.find("$8\r\nfollower\r\n#t\r\n") != std::string::npos);
    assert(pair.primary.Run("DESCRIBE t").find("follower") == std::string::npos);

    ExpectError(follower.Run("SET BLOCK 0 0 IN f n = 1"), "INVALID_ARGUMENT reason=follower ");
    ExpectError(follower.Run("SET BLOCK 0 0 IN f n = n + 1 IF n = 0"), "INVALID_ARGUMENT reason=follower ");
    ExpectError(follower.Run("DELETE BLOCK 0 0 FROM f"), "INVALID_ARGUMENT reason=follower ");
    assert(pair.primary.Run("SET BLOCK 0 0 IN t n = 1").front() == ':');
    const auto form = Form(pair.primary.Run("GET CHUNK 0 0 FROM t"));
    ExpectError(follower.Run("SET CHUNK 0 0 IN f $1", Parameters{form}), "INVALID_ARGUMENT reason=follower ");
    assert(follower.Run("BEGIN") == "+OK\r\n");
    ExpectError(follower.Run("SET BLOCK 0 0 IN f n = 1"), "INVALID_ARGUMENT reason=follower ");
    ExpectError(follower.Run("DELETE BLOCK 0 0 FROM f"), "INVALID_ARGUMENT reason=follower ");
    assert(follower.Run("ROLLBACK") == "+OK\r\n");
    ExpectError(follower.Run("ALTER TABLE f ADD COLUMN extra u8 NULL"), "INVALID_ARGUMENT reason=follower ");
    ExpectError(follower.Run("ALTER TABLE f ADD CHECK (n < 9)"), "INVALID_ARGUMENT reason=follower ");
    ExpectError(follower.Run("COMPACT TOMBSTONES f BEFORE 1"), "INVALID_ARGUMENT reason=follower ");
    ExpectError(follower.Run("CREATE SLOT 's' ON f"), "INVALID_ARGUMENT reason=follower ");
    ExpectError(follower.Run("MIGRATE 'm1' ALTER TABLE f ADD COLUMN extra u8 NULL"), "INVALID_ARGUMENT reason=follower ");
    // Local options and reads stay.
    assert(follower.Run("ALTER TABLE f SET checkpoint_updates = 100") == "+OK\r\n");
    assert(follower.Run("GET CHUNK 0 0 FROM f") == "_\r\n");
    assert(follower.Run("DESCRIBE f").find("$8\r\nfollower\r\n#t\r\n") != std::string::npos);

    // One table of an epoch per data directory, and a valid epoch.
    ExpectError(follower.Run(std::string("CREATE TABLE g ") + kColumns + " WITH follower = '" + pair.epoch + "'"),
        "INVALID_ARGUMENT reason=epoch_in_use ");
    ExpectError(follower.Run(std::string("CREATE TABLE g ") + kColumns + " WITH follower = 'abc'"), "INVALID_ARGUMENT ");
    ExpectError(follower.Run(std::string("CREATE TABLE g ") + kColumns + " WITH follower = '" +
        std::string(32U, '0') + "'"), "INVALID_ARGUMENT ");
    ExpectError(follower.Run("ALTER TABLE f SET follower = '" + pair.epoch + "'"), "INVALID_ARGUMENT ");
    ExpectError(pair.primary.Run(std::string("CREATE TABLE g ") + kColumns + " WITH follower = '" + pair.epoch + "'"),
        "INVALID_ARGUMENT reason=epoch_in_use ");
    // The follower opens as a follower again.
    follower.Restart();
    ExpectError(follower.Run("SET BLOCK 0 0 IN f n = 1"), "INVALID_ARGUMENT reason=follower ");
    assert(DescribeField(follower.Run("DESCRIBE f"), "epoch") == pair.epoch);
}

void TestApplyForms() {
    Pair pair;
    assert(pair.primary.Run("SET BLOCK 0 0 IN t n = 1, s = 'a'").front() == ':');
    assert(pair.ApplyForms({{0, 0}}) == "+applied\r\n");
    pair.ExpectSameChunk(0, 0);
    pair.ExpectSameRevision();
    // A push the follower holds is skipped.
    assert(pair.ApplyForms({{0, 0}}) == "+skipped\r\n");

    // A transaction over two chunks commits at one revision.
    assert(pair.primary.Run("BEGIN") == "+OK\r\n");
    assert(pair.primary.Run("SET BLOCK 1 1 IN t n = 2") == "_\r\n");
    assert(pair.primary.Run("SET BLOCK 2 0 IN t n = 3, s = 'bc'") == "_\r\n");
    assert(!StartsWith(pair.primary.Run("COMMIT"), "-ERR"));
    assert(pair.ApplyForms({{0, 0}, {1, 0}}) == "+applied\r\n");
    pair.ExpectSameChunk(0, 0);
    pair.ExpectSameChunk(1, 0);
    pair.ExpectSameRevision();

    // A commit that leaves a chunk empty: its form, and null for a deletion.
    assert(pair.primary.Run("DELETE BLOCK 2 0 FROM t").front() == ':');
    assert(pair.ApplyForms({{1, 0}}) == "+applied\r\n");
    pair.ExpectSameChunk(1, 0);
    assert(pair.primary.Run("SET BLOCK 3 1 IN t n = 4").front() == ':');
    assert(pair.ApplyForms({{1, 0}}) == "+applied\r\n");
    assert(pair.primary.Run("DELETE BLOCK 3 1 FROM t").front() == ':');
    const auto empty = Form(pair.primary.Run("GET CHUNK 1 0 FROM t"));
    assert(pair.follower.Apply(CommitPush(pair.epoch, FormVersion(empty), FormTime(empty), std::nullopt, FormSchema(empty),
        {FormChunk(1, 0, FormVersion(empty), std::nullopt)})) == "+applied\r\n");
    pair.ExpectSameChunk(1, 0);
    pair.ExpectSameRevision();
}

void TestApplyRows() {
    Pair pair;
    // SET BLOCK: the full row, each value in its parameter encoding.
    assert(pair.primary.Run("SET BLOCK 1 1 IN t n = 7, s = 'ab'").front() == ':');
    auto form = Form(pair.primary.Run("GET CHUNK 0 0 FROM t"));
    assert(pair.follower.Apply(CommitPush(pair.epoch, FormVersion(form), FormTime(form), std::string("writer"), FormSchema(form),
        {RowsChunk(0, 0, FormVersion(form), {Row{1, 1, std::vector<std::optional<std::string>>{U64Parameter(7), "ab"}}})})) ==
        "+applied\r\n");
    pair.ExpectSameChunk(0, 0);
    pair.ExpectSameRevision();
    // A NULL value, then a deletion.
    assert(pair.primary.Run("SET BLOCK 0 1 IN t n = 5").front() == ':');
    form = Form(pair.primary.Run("GET CHUNK 0 0 FROM t"));
    assert(pair.follower.Apply(CommitPush(pair.epoch, FormVersion(form), FormTime(form), std::nullopt, FormSchema(form),
        {RowsChunk(0, 0, FormVersion(form), {Row{0, 1, std::vector<std::optional<std::string>>{U64Parameter(5), std::nullopt}}})})) ==
        "+applied\r\n");
    pair.ExpectSameChunk(0, 0);
    assert(pair.primary.Run("DELETE BLOCK 1 1 FROM t").front() == ':');
    form = Form(pair.primary.Run("GET CHUNK 0 0 FROM t"));
    assert(pair.follower.Apply(CommitPush(pair.epoch, FormVersion(form), FormTime(form), std::nullopt, FormSchema(form),
        {RowsChunk(0, 0, FormVersion(form), {Row{1, 1, std::nullopt}})})) == "+applied\r\n");
    pair.ExpectSameChunk(0, 0);
    pair.ExpectSameRevision();
    // A row of the wrong length or a block of another chunk changes nothing.
    assert(pair.primary.Run("SET BLOCK 0 0 IN t n = 9").front() == ':');
    form = Form(pair.primary.Run("GET CHUNK 0 0 FROM t"));
    const auto version = FormVersion(form);
    ExpectError(pair.follower.Apply(CommitPush(pair.epoch, version, FormTime(form), std::nullopt, FormSchema(form),
        {RowsChunk(0, 0, version, {Row{0, 0, std::vector<std::optional<std::string>>{U64Parameter(9)}}})})),
        "INVALID_ARGUMENT reason=apply ");
    ExpectError(pair.follower.Apply(CommitPush(pair.epoch, version, FormTime(form), std::nullopt, FormSchema(form),
        {RowsChunk(0, 0, version, {Row{2, 0, std::vector<std::optional<std::string>>{U64Parameter(9), std::nullopt}}})})),
        "INVALID_ARGUMENT reason=apply ");
    assert(DescribeField(pair.follower.Run("DESCRIBE f"), "revision") != ":" + std::to_string(version));
}

void TestApplyRefusals() {
    Pair pair;
    assert(pair.primary.Run("SET BLOCK 0 0 IN t n = 1").front() == ':');
    const auto form = Form(pair.primary.Run("GET CHUNK 0 0 FROM t"));
    const auto version = FormVersion(form), time = FormTime(form), schema = FormSchema(form);
    auto& follower = pair.follower;
    ExpectError(follower.Apply(CommitPush(std::string(32U, 'a'), version, time, std::nullopt, schema,
        {FormChunk(0, 0, version, form)})), "NO_TABLE ");
    ExpectError(follower.Apply(CommitPush(pair.epoch, version + 1U, time, std::nullopt, schema,
        {FormChunk(0, 0, version, form)})), "INVALID_ARGUMENT reason=apply ");
    ExpectError(follower.Apply(CommitPush(pair.epoch, version + 1U, time, std::nullopt, schema,
        {FormChunk(0, 0, version + 1U, form)})), "INVALID_ARGUMENT reason=apply ");
    ExpectError(follower.Apply(CommitPush(pair.epoch, version, time, std::nullopt, schema + 1U,
        {FormChunk(0, 0, version, form)})), "SCHEMA_MISMATCH ");
    ExpectError(follower.Apply("not a push"), "INVALID_ARGUMENT ");
    ExpectError(follower.Apply(ProgressPush(pair.epoch, version) + "_\r\n"), "INVALID_ARGUMENT ");
    ExpectError(follower.Apply(">5\r\n" + Bulk("resync") + Bulk(pair.epoch) + Int(1) + Null() + Null()), "INVALID_ARGUMENT ");
    ExpectError(follower.Apply(">2\r\n" + Bulk("schema") + "%0\r\n"), "INVALID_ARGUMENT ");
    ExpectError(follower.Run("APPLY $1", Parameters{std::nullopt}), "INVALID_ARGUMENT ");
    ExpectError(follower.Run("APPLY 1"), "SYNTAX ");
    // The primary's own table is not a follower.
    ExpectError(pair.primary.Apply(CommitPush(pair.epoch, version + 1U, time, std::nullopt, schema,
        {FormChunk(0, 0, version + 1U, form)})), "INVALID_ARGUMENT reason=not_follower ");
    assert(follower.Run("BEGIN") == "+OK\r\n");
    ExpectError(follower.Apply(ProgressPush(pair.epoch, version)), "INVALID_ARGUMENT ");
    assert(follower.Run("ROLLBACK") == "+OK\r\n");
    // Nothing was applied.
    assert(follower.Run("GET CHUNK 0 0 FROM f") == "_\r\n");
    assert(DescribeField(follower.Run("DESCRIBE f"), "revision") == ":0");
    assert(pair.ApplyForms({{0, 0}}) == "+applied\r\n");
    pair.ExpectSameChunk(0, 0);
}

// Progress raises the revision in memory; a restart keeps the last applied
// commit, and the next commit applies at its own revision.
void TestProgressAndRestart() {
    Pair pair;
    assert(pair.primary.Run("SET BLOCK 0 0 IN t n = 1").front() == ':');
    assert(pair.ApplyForms({{0, 0}}) == "+applied\r\n");
    const auto applied = DescribeField(pair.follower.Run("DESCRIBE f"), "revision");
    const auto last = std::stoull(applied.substr(1U));
    assert(pair.follower.Apply(ProgressPush(pair.epoch, last + 1000U)) == "+applied\r\n");
    assert(DescribeField(pair.follower.Run("DESCRIBE f"), "revision") == ":" + std::to_string(last + 1000U));
    assert(pair.follower.Apply(ProgressPush(pair.epoch, last + 10U)) == "+skipped\r\n");
    pair.follower.Restart();
    assert(DescribeField(pair.follower.Run("DESCRIBE f"), "revision") == applied);
    pair.ExpectSameChunk(0, 0);
    assert(pair.ApplyForms({{0, 0}}) == "+skipped\r\n");
    assert(pair.primary.Run("SET BLOCK 1 0 IN t n = 2").front() == ':');
    assert(pair.ApplyForms({{0, 0}}) == "+applied\r\n");
    pair.ExpectSameChunk(0, 0);
    pair.ExpectSameRevision();
    pair.follower.Restart();
    pair.ExpectSameChunk(0, 0);
    pair.ExpectSameRevision();
}

void TestDecoder() {
    const auto value = DecodeReplicationValue(">2\r\n$4\r\nkind\r\n*3\r\n:-5\r\n(18446744073709551615\r\n_\r\n");
    assert(value.type == '>' && value.items.size() == 2U && value.items[0].text == "kind");
    assert(value.items[1].items[0].integer == -5 && value.items[1].items[0].negative);
    assert(value.items[1].items[1].unsigned_value == UINT64_MAX);
    for (const std::string bad : {"", "*1\r\n", "$5\r\nab\r\n", "*99999999999\r\n", ":x\r\n", "_\r\nmore", "%1\r\n",
             ":99999999999999999999\r\n", "$-1\r\n", "*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n_\r\n"}) {
        bool refused = false;
        try {
            (void)DecodeReplicationValue(bad);
        } catch (const std::invalid_argument&) {
            refused = true;
        }
        assert(refused);
    }
    assert(ParseEpochHex(std::string(32U, 'F')).has_value());
    assert(!ParseEpochHex(std::string(31U, 'f')).has_value());
    assert(!ParseEpochHex(std::string(32U, 'g')).has_value());
}

// Option type 14 is moved out of the options area and written back.
void TestManifestFollower() {
    GeometryConfig geometry;
    geometry.block_bits = 8U;
    StoreManifest manifest{.geometry = geometry, .store_id = NewStoreId(), .options = EncodeTableOptions(TableOptions{}),
        .schema = SingleBitsColumnSchema(8)};
    manifest.follower = 42U;
    const auto parsed = ParseStoreManifest(SerializeStoreManifest(manifest));
    assert(parsed.follower == std::optional<std::uint64_t>(42U));
    assert(parsed.options == manifest.options);
    manifest.follower.reset();
    assert(!ParseStoreManifest(SerializeStoreManifest(manifest)).follower.has_value());
    const auto option = [](std::uint16_t length) {
        std::vector<std::uint8_t> bytes{14U, 0U, static_cast<std::uint8_t>(length), 0U};
        bytes.resize(bytes.size() + length, 0U);
        return bytes;
    };
    for (const auto& [extra, reason] : std::vector<std::pair<std::vector<std::uint8_t>, std::string>>{
             {option(4U), "follower option has invalid length"}, {[&] {
                 auto twice = option(8U);
                 const auto again = option(8U);
                 twice.insert(twice.end(), again.begin(), again.end());
                 return twice;
             }(), "follower option appears twice"}}) {
        auto damaged = manifest;
        damaged.options.insert(damaged.options.end(), extra.begin(), extra.end());
        const auto bytes = SerializeStoreManifest(damaged);
        bool refused = false;
        try {
            (void)ParseStoreManifest(bytes);
        } catch (const std::runtime_error& error) {
            refused = std::string(error.what()).find(reason) != std::string::npos;
        }
        assert(refused);
    }
}

}  // namespace

int main() {
    TestManifestFollower();
    TestDecoder();
    TestFollowerTable();
    TestApplyForms();
    TestApplyRows();
    TestApplyRefusals();
    TestProgressAndRestart();
    return 0;
}
