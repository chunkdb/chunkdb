#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "chunkdb/area_selector.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/change_feed.hpp"
#include "sectioned_chunk_form.hpp"
#include "users.hpp"

namespace chunkdb::expr {
struct Expression;
struct WriteExpressions;
}  // namespace chunkdb::expr

// CQL statements (docs/CQL.md): parsed from one request line, not yet
// executed.
namespace chunkdb::cql {

// A statement that does not follow the grammar; `what()` says where.
class ParseError : public std::invalid_argument {
  public:
    using std::invalid_argument::invalid_argument;
};

class DuplicateHelloOptionError : public std::invalid_argument {
  public:
    explicit DuplicateHelloOptionError(std::string option)
        : std::invalid_argument("duplicate HELLO option"), option_(std::move(option)) {}
    [[nodiscard]] const std::string& option() const noexcept { return option_; }
  private:
    std::string option_;
};

class UnknownCommandError : public ParseError {
  public:
    using ParseError::ParseError;
};

// An integer literal: its magnitude and sign, so both u64 and i64 values
// fit.
struct Integer {
    bool negative = false;
    std::uint64_t magnitude = 0;

    friend bool operator==(const Integer&, const Integer&) = default;
};

struct Null {
    friend bool operator==(const Null&, const Null&) = default;
};
struct Text {
    std::string value;
    friend bool operator==(const Text&, const Text&) = default;
};
struct Bytes {
    std::vector<std::uint8_t> value;
    friend bool operator==(const Bytes&, const Bytes&) = default;
};
struct Bits {
    // '0' and '1', the first the lowest bit.
    std::string digits;
    friend bool operator==(const Bits&, const Bits&) = default;
};
// The most parameters one statement takes.
inline constexpr std::size_t kMaxParameters = 65535;

// `$n`: the n-th value sent after the statement, 1-based.
struct Parameter {
    std::size_t index = 0;
    friend bool operator==(const Parameter&, const Parameter&) = default;
};

using Literal = std::variant<Null, Integer, double, bool, Text, Bytes, Bits, Parameter>;

struct Assignment {
    std::string column;
    Literal value;
    friend bool operator==(const Assignment&, const Assignment&) = default;
};

struct GetBlock {
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::string table;
    std::vector<std::string> columns;  // empty: every column
    bool with_version = false;
    bool with_schema = false;
    std::int64_t z = 0;
    std::uint8_t dims = 2;
};
// RETURNING [OLD | NEW] (* | column, ...) of a block write (docs/DESIGN-2.0.md
// E4): the block before the write (OLD) or after it (NEW).
struct Returning {
    bool new_values = false;
    // In the order given; empty for *, every column in schema order.
    std::vector<std::string> columns;
};
struct SetBlock {
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::string table;
    std::vector<Assignment> values;
    std::optional<std::uint64_t> if_version;
    std::int64_t z = 0;
    std::uint8_t dims = 2;
    // IF <expr> and values that are more than a single value
    // (docs/design/EXPRESSIONS_DESIGN.md); null for a statement without them.
    std::shared_ptr<const expr::WriteExpressions> expressions;
    // RETURNING; null without it.
    std::shared_ptr<const Returning> returning;
};
struct DeleteBlock {
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::string table;
    std::optional<std::uint64_t> if_version;
    std::int64_t z = 0;
    std::uint8_t dims = 2;
    // IF <expr>; null without it.
    std::shared_ptr<const expr::WriteExpressions> expressions;
    // RETURNING (OLD only); null without it.
    std::shared_ptr<const Returning> returning;
};
struct GetChunk {
    std::int64_t chunk_x = 0;
    std::int64_t chunk_y = 0;
    std::string table;
    std::vector<std::string> columns;
    std::int64_t chunk_z = 0;
    std::uint8_t dims = 2;
};
struct SetChunk {
    std::int64_t chunk_x = 0;
    std::int64_t chunk_y = 0;
    std::string table;
    Parameter state;
    std::optional<std::uint64_t> if_version;
    std::int64_t chunk_z = 0;
    std::uint8_t dims = 2;
    std::optional<std::vector<Assignment>> metadata;
    std::vector<std::string> columns;
    bool if_absent = false;
};
// A chunk range or ball; dims selects whether z0 and z1 participate.
struct GetArea {
    bool around = false;
    std::int64_t x0 = 0;
    std::int64_t y0 = 0;
    std::int64_t x1 = 0;
    std::int64_t y1 = 0;
    std::int64_t radius = 0;
    std::string table;
    std::vector<std::string> columns;
    bool with_schema = false;
    std::int64_t z0 = 0;
    std::int64_t z1 = 0;
    std::uint8_t dims = 2;
    std::optional<std::uint64_t> limit;
    std::optional<ChunkCoord> after;
    // SCALE k USING reduction: one cell per k blocks on every active axis.
    std::optional<ChunkFormScale> scale;
};

// One function of AGGREGATE; `column` is empty for COUNT(*).
struct AggregateFunction {
    enum class Kind : std::uint8_t { kCount, kSum, kMin, kMax, kAvg };
    Kind kind = Kind::kCount;
    std::string column;
    friend bool operator==(const AggregateFunction&, const AggregateFunction&) = default;
};
// AGGREGATE over a chunk range or ball; CHUNK c is the range from c to c.
struct Aggregate {
    bool around = false;
    std::int64_t x0 = 0;
    std::int64_t y0 = 0;
    std::int64_t x1 = 0;
    std::int64_t y1 = 0;
    std::int64_t radius = 0;
    std::string table;
    std::vector<AggregateFunction> functions;
    bool by_chunk = false;
    std::int64_t z0 = 0;
    std::int64_t z1 = 0;
    std::uint8_t dims = 2;
    // WHERE (expr.hpp): only the present blocks for which it is TRUE.
    std::shared_ptr<const expr::Expression> where;
};

// CHECK [name] (expr): the condition's text as written, trimmed.
struct CheckDefinition {
    std::optional<std::string> name;
    std::string expression;
};
struct ColumnDefinition {
    std::string name;
    ColumnType type;
    bool nullable = false;
    bool required = false;
    std::optional<Literal> default_value;
    // Block columns of CREATE TABLE only.
    std::vector<CheckDefinition> checks{};
};
struct Option {
    std::string name;
    Literal value;
};
struct HelloRequest {
    std::uint64_t protocol = 0U;
    std::optional<std::string> user;
    std::vector<Option> options;
    std::size_t parameters = 0U;
};
struct CreateTable {
    std::string table;
    std::vector<ColumnDefinition> columns;
    std::uint32_t chunk_width = 16;
    std::uint32_t chunk_height = 16;
    std::optional<std::pair<std::uint32_t, std::uint32_t>> large;
    std::vector<Option> options;
    bool if_not_exists = false;
    std::uint32_t chunk_depth = 1;
    std::uint32_t large_depth = 1;
    std::uint8_t dims = 2;
    std::vector<ColumnDefinition> chunk_columns;
    // CHECK clauses after the columns.
    std::vector<CheckDefinition> checks{};
};
struct AddColumn {
    ColumnDefinition column;
    bool if_not_exists = false;
    bool chunk_column = false;
};
struct DropColumn {
    std::string column;
    bool if_exists = false;
    bool chunk_column = false;
};
struct RenameColumn {
    std::string column;
    std::string new_name;
    bool chunk_column = false;
};
struct AlterColumnType {
    std::string column;
    ColumnType type;
    // Without USING: widen, or narrow after checking every value.
    std::optional<Conversion> conversion;
};
struct SetOption {
    Option option;
};
// ALTER COLUMN c SET DEFAULT v | DROP DEFAULT | SET NULL | DROP NULL |
// SET REQUIRED | DROP REQUIRED.
struct AlterColumnAttribute {
    enum class Change : std::uint8_t { kSetDefault, kDropDefault, kSetNull, kDropNull, kSetRequired, kDropRequired };
    std::string column;
    Change change = Change::kSetDefault;
    // kSetDefault only.
    std::optional<Literal> value{};
};
// RENAME TO: the table's new name.
struct RenameTable {
    std::string new_name;
};
// ADD CHECK [name] (expr), DROP CHECK [IF EXISTS] name.
struct AddCheck {
    CheckDefinition check;
};
struct DropCheck {
    std::string name;
    bool if_exists = false;
};
struct AlterTable {
    std::string table;
    std::variant<AddColumn, DropColumn, RenameColumn, AlterColumnType, SetOption, AlterColumnAttribute, RenameTable,
        AddCheck, DropCheck>
        change;
};
struct DropTable {
    std::string table;
    bool if_exists = false;
};
struct ShowTables {};
// Runs the recovery a restart runs, for one fenced table.
struct RecoverTable {
    std::string table;
};
struct CreateSlot {
    std::string name;
    std::string table;
    bool if_not_exists = false;
    // Empty for the whole table; AREA ..., AREA ... gives several.
    std::vector<FeedArea> areas{};
    std::optional<std::vector<std::string>> columns{};
    bool no_before = false;
    bool chunks = false;
    std::optional<std::string> owner{};
};
struct DropSlot {
    std::string name;
    std::string table;
    bool if_exists = false;
};
struct ShowSlots {
    std::optional<std::string> table;
    // Only slots whose names start with it.
    std::optional<std::string> prefix{};
};
struct Describe {
    std::string table;
};
struct FlushWal {};
struct Backup {
    // A path selects BACKUP TO; no path selects BACKUP STREAM.
    std::optional<std::string> path{};
};
// Restores table `table` of a completed backup as a new table `name`
// (docs/design/RESTORE_TABLE.md); without AS, `name` is `table`.
struct RestoreTable {
    std::string table;
    std::string path;
    std::string name;
};
// Exchanges the names of two tables atomically.
struct SwapTables {
    std::string first;
    std::string second;
};
struct ShowMetrics {};
struct ShowStatus {};
struct ShowConnections {};
struct Cancel { std::uint64_t id = 0U; };
struct KillConnection { std::uint64_t id = 0U; };
struct EnableFeature { std::string name; };
// Loads the TLS certificate and key again from the configured files.
struct ReloadTls {};
// Answers +PONG: for health checks.
struct Ping {};
struct Watch {
    // WATCH CATALOG: the catalog, not a table; every other field is empty.
    bool catalog = false;
    std::string table;
    std::optional<std::vector<std::string>> columns{};
    bool no_before = false;
    bool chunks = false;
    std::optional<std::string> slot{};
    // Empty for the whole table; AREA ..., AREA ... gives several.
    std::vector<FeedArea> areas{};
    std::optional<FeedPosition> after{};
    std::optional<std::uint64_t> ack_timeout_ms{};
    bool takeover = false;
};
struct Unwatch {};
struct Ack {
    std::uint64_t revision = 0;
};
// User statements (docs/design/USERS_DESIGN.md). A verifier is a parameter or a
// text literal; never a password.
struct CreateUser {
    std::string user;
    Literal verifier;
    bool manages_users = false;
    bool requires_channel_binding = false;
    bool if_not_exists = false;
};
// Exactly one change is set: the verifier, the added verifier,
// drop_old_verifier, manages_users, the limits or requires_channel_binding.
struct AlterUser {
    std::string user;
    std::optional<Literal> verifier;
    // ADD VERIFIER: a second verifier the user logs in with besides its own.
    std::optional<Literal> added_verifier;
    // DROP OLD VERIFIER: the added verifier becomes the only one.
    bool drop_old_verifier = false;
    std::optional<bool> manages_users;
    std::optional<std::uint64_t> max_connections{};
    std::optional<std::uint64_t> statements_per_sec{};
    std::optional<bool> requires_channel_binding;
};
struct DropUser {
    std::string user;
    bool if_exists = false;
};
// GRANT, or REVOKE when `revoke`.
struct GrantRight {
    bool revoke = false;
    // Table rights are hierarchical. A list grants its highest right or
    // revokes its lowest right, so the registry publishes the whole list
    // once. Absent when the list names server rights only.
    std::optional<Right> right;
    // ServerRight bits; the parser accepts them ON * only.
    std::uint8_t server_rights = 0U;
    // A table name, a prefix grant `p*`, or kEveryTable.
    std::string table;
    std::string user;
};
struct ShowUsers {};
// Transactions (docs/design/TRANSACTIONS_DESIGN.md).
// BEGIN READ ONLY: reads only, without a read set (docs/reference/TRANSACTION_STATEMENTS.md).
struct Begin {
    bool read_only = false;
};
struct Commit {};
struct Rollback {};
// The populated chunks of table, ordered by every active coordinate axis.
struct ScanChunks {
    std::string table;
    std::optional<std::pair<std::int64_t, std::int64_t>> after;
    std::optional<std::uint64_t> limit;
    std::int64_t after_z = 0;
    std::uint8_t dims = 2;
    std::optional<std::uint64_t> since;
    bool with_data = false;
    std::vector<std::string> columns;
    // WHERE (expr.hpp): only the present blocks for which it is TRUE.
    std::shared_ptr<const expr::Expression> where;
    // AREA a TO b or AREA AROUND c RADIUS r: only the chunks inside it.
    bool has_area = false;
    ChunkAreaSelector area{};
};

using MigrationStatement = std::variant<CreateTable, AlterTable, DropTable, GrantRight, CreateSlot, DropSlot>;
struct Migrate { std::string name; std::string text; MigrationStatement statement; };
struct ShowMigrations {};
struct CompactTombstones {
    std::string table;
    std::uint64_t before = 0U;
};
struct ShowTableStats { std::string table; };

using Statement = std::variant<
    GetBlock,
    SetBlock,
    DeleteBlock,
    GetChunk,
    SetChunk,
    GetArea,
    Aggregate,
    CreateTable,
    AlterTable,
    DropTable,
    ShowTables,
    RecoverTable,
    Describe,
    FlushWal,
    Backup,
    RestoreTable,
    SwapTables,
    ShowMetrics,
    ShowStatus,
    ShowConnections,
    Cancel,
    KillConnection,
    EnableFeature,
    ReloadTls,
    Ping,
    ScanChunks,
    CreateUser,
    AlterUser,
    DropUser,
    GrantRight,
    ShowUsers,
    Begin,
    Commit,
    Rollback,
    Watch,
    Unwatch,
    CreateSlot,
    DropSlot,
    ShowSlots,
    Ack,
    Migrate,
    ShowMigrations,
    CompactTombstones,
    ShowTableStats>;

struct Parsed {
    Statement statement;
    // The parameter frames that follow the line: $1 to $parameters, each used.
    std::size_t parameters = 0;
};

// Whether `line` has a `$` outside quotes, so frames may follow it; a line
// that does not parse still tells the server whether to expect them.
[[nodiscard]] bool MayHaveParameters(std::string_view line) noexcept;
// Counts token occurrences, not their indices, even in invalid statements.
[[nodiscard]] std::size_t CountParameterTokens(std::string_view line) noexcept;

// Parses one statement (docs/CQL.md). Throws ParseError naming the
// first token that does not fit, with its 1-based column.
[[nodiscard]] Parsed Parse(std::string_view line);
// How AGGREGATE names `function` in its reply and errors: `count(*)`,
// `sum(a)`, `min(a)`, `max(a)` or `avg(a)`.
[[nodiscard]] std::string AggregateFunctionName(const AggregateFunction& function);
[[nodiscard]] HelloRequest ParseHello(std::string_view line);

}  // namespace chunkdb::cql
