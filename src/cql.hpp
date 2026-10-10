#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "chunkdb/schema.hpp"
#include "chunkdb/change_feed.hpp"
#include "users.hpp"

// CQL statements (docs/CQL.md): parsed from one request line, not yet
// executed.
namespace chunkdb::cql {

// A statement that does not follow the grammar; `what()` says where.
class ParseError : public std::invalid_argument {
  public:
    using std::invalid_argument::invalid_argument;
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
};
struct SetBlock {
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::string table;
    std::vector<Assignment> values;
    std::optional<std::uint64_t> if_version;
};
struct DeleteBlock {
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::string table;
    std::optional<std::uint64_t> if_version;
};
struct GetChunk {
    std::int64_t chunk_x = 0;
    std::int64_t chunk_y = 0;
    std::string table;
    std::vector<std::string> columns;
};
struct SetChunk {
    std::int64_t chunk_x = 0;
    std::int64_t chunk_y = 0;
    std::string table;
    Parameter state;
    std::optional<std::uint64_t> if_version;
};
// Blocks from (x0, y0) to (x1, y1), or within `radius` of (x0, y0).
struct GetArea {
    bool around = false;
    std::int64_t x0 = 0;
    std::int64_t y0 = 0;
    std::int64_t x1 = 0;
    std::int64_t y1 = 0;
    std::int64_t radius = 0;
    std::string table;
    std::vector<std::string> columns;
};

struct ColumnDefinition {
    std::string name;
    ColumnType type;
    bool nullable = false;
    bool required = false;
    std::optional<Literal> default_value;
};
struct Option {
    std::string name;
    Literal value;
};
struct CreateTable {
    std::string table;
    std::vector<ColumnDefinition> columns;
    std::uint32_t chunk_width = 0;
    std::uint32_t chunk_height = 0;
    std::optional<std::pair<std::uint32_t, std::uint32_t>> large;
    std::vector<Option> options;
    bool if_not_exists = false;
};
struct AddColumn {
    ColumnDefinition column;
    bool if_not_exists = false;
};
struct DropColumn {
    std::string column;
    bool if_exists = false;
};
struct RenameColumn {
    std::string column;
    std::string new_name;
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
struct AlterTable {
    std::string table;
    std::variant<AddColumn, DropColumn, RenameColumn, AlterColumnType, SetOption> change;
};
struct DropTable {
    std::string table;
    bool if_exists = false;
};
struct ShowTables {};
struct CreateSlot {
    std::string name;
    std::string table;
    bool if_not_exists = false;
};
struct DropSlot {
    std::string name;
    std::string table;
    bool if_exists = false;
};
struct ShowSlots {
    std::optional<std::string> table;
};
struct Describe {
    std::string table;
};
struct FlushWal {};
struct Backup {
    std::string path;
};
struct ShowMetrics {};
// Answers +PONG: for health checks.
struct Ping {};
struct Watch {
    std::string table;
    std::optional<std::string> slot{};
    std::optional<FeedArea> area{};
    std::optional<FeedPosition> after{};
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
    bool if_not_exists = false;
};
struct AlterUser {
    std::string user;
    std::optional<Literal> verifier;
    std::optional<bool> manages_users;
};
struct DropUser {
    std::string user;
    bool if_exists = false;
};
// GRANT, or REVOKE when `revoke`.
struct GrantRight {
    bool revoke = false;
    Right right = Right::kRead;
    // A table name, or kEveryTable.
    std::string table;
    std::string user;
};
struct ShowUsers {};
// Transactions (docs/design/TRANSACTIONS_DESIGN.md).
struct Begin {};
struct Commit {};
struct Rollback {};
// The populated chunks of `table` after (`after_x`, `after_y`) in scan
// order, at most `limit` of them.
struct ScanChunks {
    std::string table;
    std::optional<std::pair<std::int64_t, std::int64_t>> after;
    std::optional<std::uint64_t> limit;
};

using MigrationStatement = std::variant<CreateTable, AlterTable, DropTable, GrantRight, CreateSlot, DropSlot>;
struct Migrate { std::string name; std::string text; MigrationStatement statement; };
struct ShowMigrations {};

using Statement = std::variant<
    GetBlock,
    SetBlock,
    DeleteBlock,
    GetChunk,
    SetChunk,
    GetArea,
    CreateTable,
    AlterTable,
    DropTable,
    ShowTables,
    Describe,
    FlushWal,
    Backup,
    ShowMetrics,
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
    ShowMigrations>;

struct Parsed {
    Statement statement;
    // The parameter frames that follow the line: $1 to $parameters, each used.
    std::size_t parameters = 0;
};

// Whether `line` has a `$` outside quotes, so frames may follow it; a line
// that does not parse still tells the server whether to expect them.
[[nodiscard]] bool MayHaveParameters(std::string_view line) noexcept;

// Parses one statement (docs/CQL.md). Throws ParseError naming the
// first token that does not fit, with its 1-based column.
[[nodiscard]] Parsed Parse(std::string_view line);

}  // namespace chunkdb::cql
