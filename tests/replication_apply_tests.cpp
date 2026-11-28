// Follower tables and APPLY (docs/design/REPLICATION_APPLY.md): a primary
// engine and a follower engine in separate data directories; the primary's
// commits are sent as WATCH t REPLICATION pushes built from its chunk forms.
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <thread>
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

#ifndef _WIN32
#include <sys/wait.h>
#endif

namespace {
using namespace chunkdb;
using Parameters = std::vector<std::optional<std::string>>;

constexpr const char* kColumns = "(n u8, s text(8) NULL) CHUNK 2 x 2";

class ScopedEnv {
  public:
    ScopedEnv(const char* key, const char* value) : key_(key) {
#ifdef _WIN32
        _putenv_s(key_, value);
#else
        setenv(key_, value, 1);
#endif
    }
    ~ScopedEnv() {
#ifdef _WIN32
        _putenv_s(key_, "");
#else
        unsetenv(key_);
#endif
    }
    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;
  private:
    const char* key_;
};

std::unique_ptr<SessionState> Greeted(CommandEngine& engine) {
    auto session = std::make_unique<SessionState>();
    assert(engine.Execute(*session, "HELLO 3\r\n").front() == '%');
    return session;
}

struct Server {
    test::ScopedTempDir dir;
    std::filesystem::path backups;
    std::size_t written_chunk_limit = kMaxTxnWrittenChunks;
    std::shared_ptr<TableCatalog> catalog;
    std::unique_ptr<CommandEngine> engine;
    std::unique_ptr<SessionState> session;

    explicit Server(const std::string& prefix, std::filesystem::path backup_dir = {},
                    std::size_t chunk_limit = kMaxTxnWrittenChunks)
        : dir(prefix), backups(std::move(backup_dir)), written_chunk_limit(chunk_limit) { Open(); }

    void Open() {
        CatalogConfig config;
        config.data_dir = dir.path();
        config.query_limits.txn_max_written_chunks = written_chunk_limit;
        catalog = std::make_shared<TableCatalog>(config);
        engine = std::make_unique<CommandEngine>(EngineConfig{.require_auth = false, .backup_dir = backups}, catalog);
        session = Greeted(*engine);
    }

    void Close() {
        session.reset();
        engine.reset();
        catalog.reset();
    }

    void Restart() {
        Close();
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
    Server primary;
    Server follower;
    std::string epoch;
    explicit Pair(const std::filesystem::path& backups = {}, std::size_t follower_chunk_limit = kMaxTxnWrittenChunks)
        : primary("chunkdb-apply-primary"), follower("chunkdb-apply-follower", backups, follower_chunk_limit) {
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

void TestFencedFollowerReservesEpoch() {
    Server server("chunkdb-apply-fenced");
    const auto option = " WITH follower = '" + std::string(32U, 'a') + "'";
    {
        ScopedEnv sync("CHUNKDB_FAILPOINT_TABLE_CREATE_SYNC_FAIL_ONCE", "1");
        ScopedEnv undo("CHUNKDB_FAILPOINT_TABLE_CREATE_UNDO_FAIL_ONCE", "1");
        ExpectError(server.Run(std::string("CREATE TABLE f ") + kColumns + option), "INTERNAL ");
    }
    assert(server.catalog->Fence("f"));
    const auto duplicate = std::string("CREATE TABLE g ") + kColumns + option;
    ExpectError(server.Run(duplicate), "INVALID_ARGUMENT reason=epoch_in_use ");
    assert(server.Run("RECOVER TABLE f") == "+OK\r\n");
    ExpectError(server.Run(duplicate), "INVALID_ARGUMENT reason=epoch_in_use ");
    assert(server.Run("DROP TABLE f") == "+OK\r\n");
    assert(server.Run(duplicate) == "+OK\r\n");
}

void TestDuplicateEpochRefusesStartup() {
    Pair pair;
    Server copy("chunkdb-apply-duplicate");
    assert(copy.Run(std::string("CREATE TABLE g ") + kColumns + " WITH follower = '" + pair.epoch + "'") == "+OK\r\n");
    copy.Close();
    pair.follower.Close();
    std::filesystem::rename(copy.dir.path() / "tables" / "g", pair.follower.dir.path() / "tables" / "g");
    bool refused = false;
    try {
        pair.follower.Open();
    } catch (const std::runtime_error& error) {
        refused = std::string_view(error.what()).starts_with("duplicate table epoch ");
    }
    assert(refused);
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
    assert(DecodeReplicationValue("*3\r\n_\r\n_\r\n_\r\n", 8U, 4U).items.size() == 3U);
    bool over = false;
    try {
        (void)DecodeReplicationValue("*3\r\n_\r\n_\r\n_\r\n", 8U, 3U);
    } catch (const std::invalid_argument&) {
        over = true;
    }
    assert(over);
    assert(ParseEpochHex(std::string(32U, 'F')).has_value());
    assert(!ParseEpochHex(std::string(31U, 'f')).has_value());
    assert(!ParseEpochHex(std::string(32U, 'g')).has_value());
}

// The follower flag is fixed when the table opens: DDL that a reopen (ALTER
// TABLE SET) holds up is still refused, and DESCRIBE keeps `follower`.
void TestDdlRacingReopen() {
    Pair pair;
    auto& engine = *pair.follower.engine;
    std::atomic<bool> done{false};
    std::thread reopen([&] {
        auto session = Greeted(engine);
        for (int i = 0; i < 40; ++i)
            assert(engine.Execute(*session, "ALTER TABLE f SET checkpoint_updates = " + std::to_string(100 + i) + "\r\n") ==
                "+OK\r\n");
        done = true;
    });
    std::thread describe([&] {
        auto session = Greeted(engine);
        while (!done) assert(engine.Execute(*session, "DESCRIBE f\r\n").find("$8\r\nfollower\r\n#t\r\n") != std::string::npos);
    });
    auto session = Greeted(engine);
    for (int i = 0; !done || i < 10; ++i) {
        for (const char* line : {"ALTER TABLE f ADD COLUMN extra u8 NULL\r\n", "ALTER TABLE f ADD CHECK (n < 9)\r\n",
                 "ALTER TABLE f ADD CHUNK COLUMN meta u8 NULL\r\n", "ALTER TABLE f RENAME COLUMN n TO m\r\n",
                 "CREATE SLOT 's' ON f\r\n", "COMPACT TOMBSTONES f BEFORE 1\r\n",
                 "MIGRATE 'race' ALTER TABLE f ADD COLUMN extra u8 NULL\r\n"})
            ExpectError(engine.Execute(*session, line), "INVALID_ARGUMENT reason=follower ");
    }
    reopen.join();
    describe.join();
    assert(DescribeField(pair.follower.Run("DESCRIBE f"), "version") == ":1");
}

// A follower's checkpoint keeps an emptied chunk instead of collecting it.
void TestFollowerCheckpoint() {
    Pair pair;
    assert(pair.follower.Run("ALTER TABLE f SET checkpoint_updates = 1") == "+OK\r\n");
    assert(pair.primary.Run("SET BLOCK 2 0 IN t n = 3").front() == ':');
    assert(pair.ApplyForms({{1, 0}}) == "+applied\r\n");
    assert(pair.primary.Run("DELETE BLOCK 2 0 FROM t").front() == ':');
    assert(pair.ApplyForms({{1, 0}}) == "+applied\r\n");
    pair.ExpectSameChunk(1, 0);
    assert(pair.follower.Run("SHOW TABLE STATS f").find("$10\r\ntombstones\r\n:0\r\n") != std::string::npos);
    pair.follower.Restart();
    pair.ExpectSameChunk(1, 0);
    pair.ExpectSameRevision();
    assert(pair.follower.Run("SHOW TABLE STATS f").find("$10\r\ntombstones\r\n:0\r\n") != std::string::npos);
}

// The primary's commit of two chunks, as one push.
std::string TwoChunkCommit(Pair& pair) {
    assert(pair.primary.Run("BEGIN") == "+OK\r\n");
    assert(pair.primary.Run("SET BLOCK 0 0 IN t n = 4") == "_\r\n");
    assert(pair.primary.Run("SET BLOCK 2 0 IN t n = 5, s = 'x'") == "_\r\n");
    assert(!StartsWith(pair.primary.Run("COMMIT"), "-ERR"));
    std::vector<Chunk> chunks;
    std::string first;
    for (const auto& [cx, cy] : std::vector<std::pair<std::int64_t, std::int64_t>>{{0, 0}, {1, 0}}) {
        const auto form = Form(pair.primary.Run("GET CHUNK " + std::to_string(cx) + " " + std::to_string(cy) + " FROM t"));
        if (first.empty()) first = form;
        chunks.push_back(FormChunk(cx, cy, FormVersion(form), form));
    }
    return CommitPush(pair.epoch, FormVersion(first), FormTime(first), std::nullopt, FormSchema(first), chunks);
}

// The follower after a failed APPLY of TwoChunkCommit: both chunks or
// neither, and sending the push again completes it.
void ExpectWholeOrNothing(Pair& pair, const std::string& push, bool committed) {
    auto& follower = pair.follower;
    if (committed) {
        pair.ExpectSameChunk(0, 0);
        pair.ExpectSameChunk(1, 0);
        pair.ExpectSameRevision();
        assert(follower.Apply(push) == "+skipped\r\n");
    } else {
        assert(follower.Run("GET CHUNK 0 0 FROM f") == "_\r\n");
        assert(follower.Run("GET CHUNK 1 0 FROM f") == "_\r\n");
        assert(DescribeField(follower.Run("DESCRIBE f"), "revision") == ":0");
        assert(follower.Apply(push) == "+applied\r\n");
    }
    pair.ExpectSameChunk(0, 0);
    pair.ExpectSameChunk(1, 0);
    pair.ExpectSameRevision();
}

void TestApplyRollbackCanRetry() {
    Pair pair;
    const auto push = TwoChunkCommit(pair);
    {
        ScopedEnv fail("CHUNKDB_FAILPOINT_TXN_APPEND_FAIL_AT", "1");
        ExpectError(pair.follower.Apply(push), "INTERNAL ");
    }
    ExpectWholeOrNothing(pair, push, false);
}

// A commit record that cannot be made durable answers OUTCOME_UNKNOWN; after
// a restart the push is skipped, since the commit stands.
void TestOutcomeUnknownResend() {
    Pair pair;
    const auto push = TwoChunkCommit(pair);
    {
        ScopedEnv rename("CHUNKDB_FAILPOINT_TXN_COMMIT_INTENT_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
        ScopedEnv sync("CHUNKDB_FAILPOINT_TXN_COMMIT_INTENT_COMPLETION_SYNC_FAIL_ONCE", "1");
        ExpectError(pair.follower.Apply(push), "OUTCOME_UNKNOWN ");
    }
    pair.follower.Restart();
    ExpectWholeOrNothing(pair, push, true);
}

constexpr int kCrashExit = 86;
constexpr std::size_t kCrashFollowerChunkLimit = 1U;

int RunChild(const std::string& executable, const std::vector<std::string>& args) {
    std::string command = "\"" + executable + "\"";
    for (const auto& arg : args) command += " \"" + arg + "\"";
#ifdef _WIN32
    command = "\"" + command + "\"";
    return std::system(command.c_str());
#else
    const int status = std::system(command.c_str());
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
#endif
}

// Child: APPLY of the push in `push_file` into the catalog at `dir` with a
// crash failpoint armed; returns only when it did not crash.
int RunApplyCrashChild(const std::filesystem::path& dir, const std::filesystem::path& push_file,
    const char* failpoint, const char* value) {
    std::ifstream input(push_file, std::ios::binary);
    const std::string push{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
    CatalogConfig config;
    config.data_dir = dir;
    config.query_limits.txn_max_written_chunks = kCrashFollowerChunkLimit;
    auto catalog = std::make_shared<TableCatalog>(config);
    CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
    auto session = Greeted(engine);
    const ScopedEnv armed(failpoint, value);
    const Parameters parameters{push};
    (void)engine.Execute(*session, "APPLY $1\r\n", parameters);
    return 3;
}

// A process that crashes at each step of a two-chunk APPLY leaves the
// follower with the whole commit or none of it.
void TestCrashDuringApply(const std::string& executable, DurabilityMode mode) {
    struct Case {
        const char* failpoint;
        const char* value;
        bool committed;
    };
    for (const auto& item : std::vector<Case>{
             {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_INTENT_PUBLISH_ONCE", "1", false},
             {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "0", false},
             {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "1", false},
             {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_FRAMES", "2", false},
             {"CHUNKDB_FAILPOINT_CRASH_TXN_BEFORE_COMMIT_PUBLISH_ONCE", "1", false},
             {"CHUNKDB_FAILPOINT_CRASH_TXN_AFTER_COMMIT_PUBLISH_ONCE", "1", true},
             {"CHUNKDB_FAILPOINT_CRASH_TXN_BEFORE_INTENT_UNLINK_ONCE", "1", true}}) {
        Pair pair({}, kCrashFollowerChunkLimit);
        assert(pair.follower.Run(std::string("ALTER TABLE f SET durability_mode = '") +
            DurabilityModeName(mode) + "'") == "+OK\r\n");
        const auto push = TwoChunkCommit(pair);
        test::ScopedTempDir files("chunkdb-apply-push");
        const auto push_file = files.path() / "push";
        {
            std::ofstream output(push_file, std::ios::binary);
            output.write(push.data(), static_cast<std::streamsize>(push.size()));
        }
        pair.follower.Close();
        const int code = RunChild(executable,
            {"--apply-crash-child", pair.follower.dir.path().string(), push_file.string(), item.failpoint, item.value});
        if (code != kCrashExit) {
            std::fprintf(stderr, "%s child for %s=%s exited with %d\n", DurabilityModeName(mode), item.failpoint, item.value, code);
            assert(false);
        }
        pair.follower.Open();
        ExpectWholeOrNothing(pair, push, item.committed);
        pair.follower.Restart();
        pair.ExpectSameChunk(0, 0);
        pair.ExpectSameRevision();
    }
}

// A backup of a follower restores as a writable table with a new epoch whose
// lineage is the primary's epoch at the cut.
void TestRestoreFollowerWritable() {
    test::ScopedTempDir backups("chunkdb-apply-backups");
    Pair pair(backups.path());
    assert(pair.primary.Run("SET BLOCK 0 0 IN t n = 6").front() == ':');
    assert(pair.ApplyForms({{0, 0}}) == "+applied\r\n");
    auto& follower = pair.follower;
    assert(follower.Run("BACKUP TO 'b1'").front() == '%');
    assert(follower.Run("RESTORE TABLE f FROM 'b1' AS g").front() == '%');
    const auto describe = follower.Run("DESCRIBE g");
    assert(describe.find("follower") == std::string::npos);
    assert(DescribeField(describe, "epoch") != pair.epoch);
    const auto cut = std::stoull(DescribeField(describe, "revision").substr(1U));
    assert(follower.Run("GET CHUNK 0 0 FROM g") == pair.primary.Run("GET CHUNK 0 0 FROM t"));
    const auto written = follower.Run("SET BLOCK 1 1 IN g n = 7");
    assert(written.front() == ':' && std::stoull(written.substr(1U)) > cut);
    // The follower itself stays a follower.
    ExpectError(follower.Run("SET BLOCK 1 1 IN f n = 7"), "INVALID_ARGUMENT reason=follower ");
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

int main(int argc, char** argv) {
    if (argc == 6 && std::string(argv[1]) == "--apply-crash-child")
        return RunApplyCrashChild(argv[2], argv[3], argv[4], argv[5]);
    assert(argc >= 1);
    if (argc == 2 && std::string_view(argv[1]) == "--crash") {
        for (const auto mode : {DurabilityMode::kRelaxed, DurabilityMode::kFsyncWal, DurabilityMode::kFsyncCheckpoint})
            TestCrashDuringApply(argv[0], mode);
        return 0;
    }
    TestManifestFollower();
    TestDecoder();
    TestFollowerTable();
    TestFencedFollowerReservesEpoch();
    TestDuplicateEpochRefusesStartup();
    TestApplyForms();
    TestApplyRows();
    TestApplyRefusals();
    TestProgressAndRestart();
    TestDdlRacingReopen();
    TestFollowerCheckpoint();
    TestApplyRollbackCanRetry();
    TestOutcomeUnknownResend();
    TestRestoreFollowerWritable();
    return 0;
}
