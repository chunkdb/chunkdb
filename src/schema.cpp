#include "chunkdb/schema.hpp"

#include <algorithm>
#include <cmath>
#include <optional>
#include <utility>
#include <bit>
#include <limits>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

#include "chunk_store_internal.hpp"
#include "expr.hpp"

namespace chunkdb {

namespace {

constexpr std::uint8_t kFlagNullable = 0x1U;
constexpr std::uint8_t kFlagRequired = 0x2U;
constexpr std::uint8_t kFlagHasDefault = 0x4U;
constexpr std::uint8_t kKnownFlags = kFlagNullable | kFlagRequired | kFlagHasDefault;

[[nodiscard]] bool IsKnownKind(std::uint8_t kind) noexcept {
    return kind >= static_cast<std::uint8_t>(ColumnKind::kUnsigned) &&
           kind <= static_cast<std::uint8_t>(ColumnKind::kBytes);
}

[[nodiscard]] bool IsValidName(std::string_view name) noexcept {
    if (name.empty() || name.size() > kMaxColumnNameBytes) {
        return false;
    }
    const auto lower_or_underscore = [](char c) { return (c >= 'a' && c <= 'z') || c == '_'; };
    if (!lower_or_underscore(name.front())) {
        return false;
    }
    for (const char c : name) {
        if (!lower_or_underscore(c) && !(c >= '0' && c <= '9')) {
            return false;
        }
    }
    return true;
}

// The parsed condition of `check`; a ParseError becomes
// std::invalid_argument naming the check.
[[nodiscard]] expr::Condition ParseCheck(const CheckConstraint& check) {
    try {
        return expr::ParseCondition(check.expression);
    } catch (const cql::ParseError& error) {
        throw std::invalid_argument("CHECK " + check.name + ": " + error.what());
    }
}

[[nodiscard]] bool ReadsColumn(const CheckConstraint& check, std::string_view column) {
    const auto names = CheckColumnNames(check);
    return std::find(names.begin(), names.end(), column) != names.end();
}

// Throws std::invalid_argument for a CHECK list `schema` cannot have.
void ValidateChecks(const TableSchema& schema, ColumnDomain domain) {
    if (schema.checks.empty()) return;
    if (domain != ColumnDomain::kBlock) throw std::invalid_argument("chunk columns cannot have CHECK constraints");
    if (schema.checks.size() > kMaxChecksPerTable)
        throw std::invalid_argument("a table has at most " + std::to_string(kMaxChecksPerTable) + " CHECK constraints");
    std::unordered_set<std::string> names;
    for (const auto& check : schema.checks) {
        if (!names.insert(check.name).second) throw std::invalid_argument("CHECK " + check.name + " appears twice");
        ValidateCheck(check, schema.columns);
    }
}

}  // namespace

void ValidateCheck(const CheckConstraint& check, const std::vector<Column>& columns) {
    if (!IsValidName(check.name)) {
        throw std::invalid_argument(
            "CHECK name '" + check.name + "' must be 1 to " + std::to_string(kMaxColumnNameBytes) +
            " characters: a-z, 0-9 and _, not starting with a digit");
    }
    if (check.expression.size() > kMaxCheckExpressionBytes) {
        throw std::invalid_argument(
            "CHECK " + check.name + " has more than " + std::to_string(kMaxCheckExpressionBytes) + " bytes");
    }
    const auto condition = ParseCheck(check);
    if (condition.parameters != 0U) throw std::invalid_argument("CHECK " + check.name + " cannot use parameters");
    for (const auto& node : condition.expression.nodes) {
        const char* refused = node.op == expr::Op::kChunkColumn ? "chunk columns"
                            : node.op == expr::Op::kNow        ? "NOW()"
                            : node.op == expr::Op::kExists     ? "EXISTS"
                                                               : nullptr;
        if (refused != nullptr)
            throw std::invalid_argument("CHECK " + check.name + " reads only the block's columns and position, not " + refused);
    }
    try {
        (void)expr::PlanCondition(condition.expression, expr::ColumnList(columns));
    } catch (const std::invalid_argument& error) {
        throw std::invalid_argument("CHECK " + check.name + ": " + error.what());
    } catch (const std::out_of_range& error) {
        throw std::invalid_argument("CHECK " + check.name + ": " + error.what());
    }
}

std::vector<std::string> CheckColumnNames(const CheckConstraint& check) {
    std::vector<std::string> names;
    for (const auto& node : ParseCheck(check).expression.nodes) {
        if (node.op == expr::Op::kColumn && std::find(names.begin(), names.end(), node.name) == names.end())
            names.push_back(node.name);
    }
    return names;
}

std::string NewCheckName(const TableSchema& schema, const std::string& expression, std::string_view column) {
    auto columns = column.empty() ? CheckColumnNames(CheckConstraint{.name = "check", .expression = expression})
                                  : std::vector<std::string>{std::string(column)};
    constexpr std::string_view kSuffix = "_check";
    // Room for the suffix and a number of up to three digits.
    const std::size_t room = kMaxColumnNameBytes - kSuffix.size() - 4U;
    const std::string base = columns.empty() ? std::string("check") : columns.front().substr(0, room) + std::string(kSuffix);
    for (std::size_t n = 1U;; ++n) {
        const std::string name = n == 1U ? base : base + "_" + std::to_string(n);
        if (!HasCheck(schema, name)) return name;
    }
}

bool HasCheck(const TableSchema& schema, std::string_view name) noexcept {
    return std::any_of(
        schema.checks.begin(), schema.checks.end(), [name](const CheckConstraint& check) { return check.name == name; });
}

void ValidateColumnType(const Column& column) {
    const auto& type = column.type;
    const auto bad = [&](const std::string& rule) {
        throw std::invalid_argument("column " + column.name + ": " + rule);
    };
    switch (type.kind) {
        case ColumnKind::kUnsigned:
            if (type.size < 1U || type.size > 64U) {
                bad("uN takes N from 1 to 64");
            }
            return;
        case ColumnKind::kSigned:
            if (type.size < 2U || type.size > 64U) {
                bad("iN takes N from 2 to 64");
            }
            return;
        case ColumnKind::kBool:
            if (type.size != 1U) {
                bad("bool is 1 bit");
            }
            return;
        case ColumnKind::kFloat32:
            if (type.size != 32U) {
                bad("f32 is 32 bits");
            }
            return;
        case ColumnKind::kFloat64:
            if (type.size != 64U) {
                bad("f64 is 64 bits");
            }
            return;
        case ColumnKind::kBits:
            if (type.size < 1U || type.size > kMaxFixedBitsPerBlock) {
                bad("bits(N) takes N from 1 to " + std::to_string(kMaxFixedBitsPerBlock));
            }
            return;
        case ColumnKind::kText:
        case ColumnKind::kBytes:
            if (type.size < 1U || type.size > kMaxVariableValueBytes) {
                bad(std::string(type.kind == ColumnKind::kText ? "text(max)" : "bytes(max)") +
                    " takes max from 1 to " + std::to_string(kMaxVariableValueBytes));
            }
            return;
    }
    bad("unknown type");
}

namespace {

void ValidateDefault(const Column& column) {
    if (!column.has_default) {
        if (!column.default_value.empty()) {
            throw std::invalid_argument("column " + column.name + ": a default value without DEFAULT");
        }
        return;
    }
    const auto& value = column.default_value;
    if (IsFixedWidth(column.type.kind)) {
        const std::uint32_t bits = FixedWidthBits(column.type);
        if (value.size() != (bits + 7U) / 8U) {
            throw std::invalid_argument(
                "column " + column.name + ": a default of " + ColumnTypeName(column.type) + " takes " +
                std::to_string((bits + 7U) / 8U) + " bytes");
        }
        if (bits % 8U != 0U && (value.back() >> (bits % 8U)) != 0U) {
            throw std::invalid_argument("column " + column.name + ": the default has bits past the type's width");
        }
        return;
    }
    if (value.size() > column.type.size) {
        throw std::invalid_argument(
            "column " + column.name + ": the default is longer than " + ColumnTypeName(column.type));
    }
    if (column.type.kind == ColumnKind::kText && !IsUtf8(value)) {
        throw std::invalid_argument("column " + column.name + ": the default is not UTF-8");
    }
}

class Reader {
  public:
    Reader(const std::uint8_t* data, std::size_t size) : data_(data), size_(size) {}

    [[nodiscard]] std::uint64_t Le(std::size_t bytes) {
        Need(bytes);
        std::uint64_t value = 0;
        for (std::size_t i = 0; i < bytes; ++i) {
            value |= static_cast<std::uint64_t>(data_[at_ + i]) << (8U * i);
        }
        at_ += bytes;
        return value;
    }
    [[nodiscard]] std::vector<std::uint8_t> Bytes(std::size_t count) {
        Need(count);
        std::vector<std::uint8_t> out(data_ + at_, data_ + at_ + count);
        at_ += count;
        return out;
    }
    [[nodiscard]] bool AtEnd() const noexcept { return at_ == size_; }

  private:
    void Need(std::size_t bytes) const {
        if (size_ - at_ < bytes) {
            throw std::runtime_error("schema area is truncated");
        }
    }

    const std::uint8_t* data_;
    std::size_t size_;
    std::size_t at_ = 0;
};

}  // namespace

bool IsUtf8(std::span<const std::uint8_t> bytes) noexcept {
    std::size_t i = 0;
    while (i < bytes.size()) {
        const std::uint8_t lead = bytes[i];
        std::size_t length = 0;
        std::uint32_t code = 0;
        if (lead < 0x80U) {
            ++i;
            continue;
        }
        if (lead >= 0xC2U && lead <= 0xDFU) {
            length = 2;
            code = lead & 0x1FU;
        } else if (lead >= 0xE0U && lead <= 0xEFU) {
            length = 3;
            code = lead & 0x0FU;
        } else if (lead >= 0xF0U && lead <= 0xF4U) {
            length = 4;
            code = lead & 0x07U;
        } else {
            return false;
        }
        if (bytes.size() - i < length) {
            return false;
        }
        for (std::size_t k = 1; k < length; ++k) {
            const std::uint8_t next = bytes[i + k];
            if ((next & 0xC0U) != 0x80U) {
                return false;
            }
            code = (code << 6U) | (next & 0x3FU);
        }
        if ((length == 3 && (code < 0x800U || (code >= 0xD800U && code <= 0xDFFFU))) ||
            (length == 4 && (code < 0x10000U || code > 0x10FFFFU))) {
            return false;
        }
        i += length;
    }
    return true;
}

bool IsFixedWidth(ColumnKind kind) noexcept {
    return kind != ColumnKind::kText && kind != ColumnKind::kBytes;
}

std::uint32_t FixedWidthBits(const ColumnType& type) noexcept {
    return IsFixedWidth(type.kind) ? type.size : 0U;
}

std::string ColumnTypeName(const ColumnType& type) {
    const std::string size = std::to_string(type.size);
    switch (type.kind) {
        case ColumnKind::kUnsigned:
            return "u" + size;
        case ColumnKind::kSigned:
            return "i" + size;
        case ColumnKind::kBool:
            return "bool";
        case ColumnKind::kFloat32:
            return "f32";
        case ColumnKind::kFloat64:
            return "f64";
        case ColumnKind::kBits:
            return "bits(" + size + ")";
        case ColumnKind::kText:
            return "text(" + size + ")";
        case ColumnKind::kBytes:
            return "bytes(" + size + ")";
    }
    return "unknown";
}

TableSchema SingleBitsColumnSchema(std::uint32_t block_bits) {
    return TableSchema{
        .version = 1,
        .next_column_id = 2,
        .columns = {Column{
            .id = 1,
            .name = "bits",
            .type = ColumnType{.kind = ColumnKind::kBits, .size = block_bits},
            .nullable = false,
            .required = false,
            .has_default = false,
            .default_value = {},
        }},
    };
}

std::uint32_t FixedBitsPerBlock(const TableSchema& schema) noexcept {
    std::uint64_t total = 0;
    for (const auto& column : schema.columns) {
        total += FixedWidthBits(column.type);
    }
    return total > 0xFFFFFFFFU ? 0xFFFFFFFFU : static_cast<std::uint32_t>(total);
}

namespace {

// The rules for the columns of one version.
void ValidateColumns(const TableSchema& schema, ColumnDomain domain) {
    if (schema.columns.empty() && domain == ColumnDomain::kBlock) {
        throw std::invalid_argument("a table needs at least one column");
    }
    if (schema.next_column_id == 0)
        throw std::invalid_argument("schema next column id must be nonzero");
    if (schema.columns.size() > kMaxColumnsPerTable) {
        throw std::invalid_argument(
            "a table has at most " + std::to_string(kMaxColumnsPerTable) + " columns");
    }
    std::unordered_set<std::uint32_t> ids;
    std::unordered_set<std::string> names;
    for (const auto& column : schema.columns) {
        if (!IsValidName(column.name)) {
            throw std::invalid_argument(
                "column name '" + column.name + "' must be 1 to " + std::to_string(kMaxColumnNameBytes) +
                " characters: a-z, 0-9 and _, not starting with a digit");
        }
        if (!names.insert(column.name).second) {
            throw std::invalid_argument("column " + column.name + " appears twice");
        }
        if (column.id == 0U || column.id >= schema.next_column_id || !ids.insert(column.id).second) {
            throw std::invalid_argument("column " + column.name + " has an invalid id");
        }
        ValidateColumnType(column);
        if (column.nullable && column.required) {
            throw std::invalid_argument("column " + column.name + " cannot be both NULL and REQUIRED");
        }
        ValidateDefault(column);
    }
    std::uint64_t fixed_bits = 0;
    for (const auto& column : schema.columns) {
        fixed_bits += FixedWidthBits(column.type);
    }
    if (fixed_bits > kMaxFixedBitsPerBlock) {
        throw std::invalid_argument(
            "the fixed-width columns of a block take " + std::to_string(fixed_bits) + " bits, at most " +
            std::to_string(kMaxFixedBitsPerBlock));
    }
}

// Turns `schema` into the version before it by undoing its last step.
// Throws std::invalid_argument when the step does not match the columns.
void UndoLastStep(TableSchema* schema) {
    const SchemaStep step = std::move(schema->history.back());
    schema->history.pop_back();
    // A narrowing in progress belongs to the current version.
    schema->pending.reset();
    auto& columns = schema->columns;
    auto& checks = schema->checks;
    for (auto change = step.changes.rbegin(); change != step.changes.rend(); ++change) {
        const std::size_t at = change->position;
        const auto mismatch = [&] {
            return std::invalid_argument(
                "schema history: version " + std::to_string(step.version) + " does not match its columns");
        };
        switch (change->kind) {
            case SchemaChange::Kind::kAddColumn:
                if (at >= columns.size() || columns[at] != change->column) {
                    throw mismatch();
                }
                columns.erase(columns.begin() + static_cast<std::ptrdiff_t>(at));
                // Adding is the only change that takes an id.
                schema->next_column_id = change->column.id;
                break;
            case SchemaChange::Kind::kDropColumn:
                if (at > columns.size()) {
                    throw mismatch();
                }
                columns.insert(columns.begin() + static_cast<std::ptrdiff_t>(at), change->column);
                break;
            case SchemaChange::Kind::kRenameColumn:
                if (at >= columns.size() || columns[at] != change->column) {
                    throw mismatch();
                }
                columns[at].name = change->old_name;
                // The checks named the column by its new name.
                for (auto& check : checks) {
                    try {
                        check.expression = expr::RenameColumnIn(check.expression, change->column.name, change->old_name);
                    } catch (const cql::ParseError&) {
                        throw mismatch();
                    }
                }
                break;
            case SchemaChange::Kind::kChangeType:
                if (at >= columns.size() || columns[at] != change->column || change->previous.id != change->column.id) {
                    throw mismatch();
                }
                columns[at] = change->previous;
                break;
            case SchemaChange::Kind::kChangeAttributes:
                if (at >= columns.size() || columns[at] != change->column || change->previous.id != change->column.id ||
                    change->previous.name != change->column.name || change->previous.type != change->column.type) {
                    throw mismatch();
                }
                columns[at] = change->previous;
                break;
            case SchemaChange::Kind::kAddCheck:
                // Compared by name: a later rename may have rewritten a
                // column name of the text.
                if (at >= checks.size() || checks[at].name != change->check.name) {
                    throw mismatch();
                }
                checks.erase(checks.begin() + static_cast<std::ptrdiff_t>(at));
                break;
            case SchemaChange::Kind::kDropCheck:
                if (at > checks.size()) {
                    throw mismatch();
                }
                checks.insert(checks.begin() + static_cast<std::ptrdiff_t>(at), change->check);
                break;
            case SchemaChange::Kind::kOtherDomain:
                break;
            default:
                throw std::invalid_argument("unknown schema change");
        }
    }
    schema->version = step.version - 1U;
}

}  // namespace

namespace {

// The rules for a pending narrowing (defined with the type rules below).
void ValidatePending(const TableSchema& schema);

}  // namespace

namespace {

bool IsOtherDomainStep(const SchemaStep& step) noexcept {
    return step.changes.size() == 1U && step.changes.front().kind == SchemaChange::Kind::kOtherDomain;
}

void ValidateSchema(const TableSchema& schema, ColumnDomain domain) {
    if (domain != ColumnDomain::kBlock && domain != ColumnDomain::kChunk)
        throw std::invalid_argument("unknown column domain");
    if (schema.version == 0U) {
        throw std::invalid_argument("schema version must be at least 1");
    }
    ValidateColumns(schema, domain);
    ValidateChecks(schema, domain);
    if (schema.version - 1U > kMaxSchemaVersions) {
        throw std::invalid_argument("a table has at most " + std::to_string(kMaxSchemaVersions + 1U) + " schema versions");
    }
    if (schema.history.size() != schema.version - 1U) {
        throw std::invalid_argument(
            "schema history has " + std::to_string(schema.history.size()) + " steps for version " +
            std::to_string(schema.version));
    }
    // Every earlier version is a valid schema too.
    TableSchema earlier = schema;
    while (!earlier.history.empty()) {
        const auto& step = earlier.history.back();
        if (step.version != earlier.version || step.changes.empty()) {
            throw std::invalid_argument("schema history step for version " + std::to_string(step.version) + " is malformed");
        }
        for (const auto& change : step.changes) {
            if (change.kind < SchemaChange::Kind::kAddColumn || change.kind > SchemaChange::Kind::kDropCheck)
                throw std::invalid_argument("unknown schema change");
            // Chunk columns have no ALTER COLUMN attribute changes or checks.
            if (change.kind == SchemaChange::Kind::kChangeAttributes && domain != ColumnDomain::kBlock)
                throw std::invalid_argument("chunk columns cannot record an attribute change");
            const bool check_change =
                change.kind == SchemaChange::Kind::kAddCheck || change.kind == SchemaChange::Kind::kDropCheck;
            if (check_change && (domain != ColumnDomain::kBlock || change.column != Column{}))
                throw std::invalid_argument("schema CHECK change is malformed");
            if (!check_change && change.check != CheckConstraint{})
                throw std::invalid_argument("schema change carries a CHECK it does not make");
            if (change.kind != SchemaChange::Kind::kOtherDomain) continue;
            if (!IsOtherDomainStep(step) || change.position != 0 || change.column != Column{} ||
                !change.old_name.empty() || change.previous != Column{} || change.conversion != Conversion::kExact)
                throw std::invalid_argument("schema other-domain marker is malformed");
        }
        UndoLastStep(&earlier);
        ValidateColumns(earlier, domain);
        ValidateChecks(earlier, domain);
    }
    ValidatePending(schema);
}

}  // namespace

void ValidateTableSchema(const TableSchema& schema) { ValidateSchema(schema, ColumnDomain::kBlock); }
void ValidateChunkSchema(const TableSchema& schema) { ValidateSchema(schema, ColumnDomain::kChunk); }

void ValidateSchemaDomains(const TableSchema& blocks, const TableSchema& chunks) {
    ValidateTableSchema(blocks);
    ValidateChunkSchema(chunks);
    if (blocks.version != chunks.version)
        throw std::invalid_argument("block and chunk schema revisions differ");
    for (std::size_t i = 0; i < blocks.history.size(); ++i) {
        if (IsOtherDomainStep(blocks.history[i]) == IsOtherDomainStep(chunks.history[i]))
            throw std::invalid_argument("schema revision must change exactly one column domain");
    }
}

TableSchema EmptyChunkSchemaFor(const TableSchema& blocks) {
    TableSchema chunks;
    chunks.version = blocks.version;
    chunks.history.reserve(blocks.history.size());
    for (const auto& step : blocks.history)
        chunks.history.push_back(SchemaStep{.version = step.version,
            .changes = {SchemaChange{.kind = SchemaChange::Kind::kOtherDomain}}});
    ValidateSchemaDomains(blocks, chunks);
    return chunks;
}

TableSchema SchemaAtVersion(const TableSchema& schema, std::uint64_t version) {
    if (version == 0U || version > schema.version) {
        throw std::invalid_argument(
            "the table has no schema version " + std::to_string(version) + " (current " +
            std::to_string(schema.version) + ")");
    }
    TableSchema earlier = schema;
    while (earlier.version > version) {
        UndoLastStep(&earlier);
    }
    return earlier;
}

namespace {

[[nodiscard]] std::size_t ColumnIndex(const TableSchema& schema, std::string_view name) {
    for (std::size_t i = 0; i < schema.columns.size(); ++i) {
        if (schema.columns[i].name == name) {
            return i;
        }
    }
    throw std::invalid_argument("the table has no column " + std::string(name));
}

[[nodiscard]] TableSchema NextVersion(
    const TableSchema& schema, SchemaChange change, TableSchema next, ColumnDomain domain = ColumnDomain::kBlock) {
    if (schema.pending.has_value()) {
        throw std::invalid_argument("a narrowing of this table is in progress");
    }
    next.version = schema.version + 1U;
    next.history.push_back(SchemaStep{.version = next.version, .changes = {std::move(change)}});
    ValidateSchema(next, domain);
    return next;
}

}  // namespace

namespace {

TableSchema AddDomainColumn(
    const TableSchema& schema, Column column, bool allow_required_without_default, ColumnDomain domain) {
    if (column.required && !column.has_default && !allow_required_without_default) {
        throw std::invalid_argument(
            "column " + column.name + ": a REQUIRED column added to a table needs a DEFAULT for the blocks it has");
    }
    TableSchema next = schema;
    column.id = next.next_column_id++;
    next.columns.push_back(column);
    // Built before the call: arguments may be evaluated in any order, and
    // `next` is moved into it.
    SchemaChange change{
        .kind = SchemaChange::Kind::kAddColumn,
        .position = static_cast<std::uint32_t>(next.columns.size() - 1U),
        .column = std::move(column),
        .old_name = {},
    };
    return NextVersion(schema, std::move(change), std::move(next), domain);
}

TableSchema DropDomainColumn(const TableSchema& schema, std::string_view name, ColumnDomain domain) {
    const std::size_t at = ColumnIndex(schema, name);
    for (const auto& check : schema.checks) {
        if (ReadsColumn(check, name))
            throw std::invalid_argument("column " + std::string(name) + " is read by CHECK " + check.name + "; drop the check first");
    }
    TableSchema next = schema;
    Column dropped = next.columns[at];
    next.columns.erase(next.columns.begin() + static_cast<std::ptrdiff_t>(at));
    if (domain == ColumnDomain::kBlock && FixedBitsPerBlock(next) == 0U) {
        throw std::invalid_argument("column " + dropped.name + " is the last fixed-width column; a table needs one");
    }
    SchemaChange change{
        .kind = SchemaChange::Kind::kDropColumn,
        .position = static_cast<std::uint32_t>(at),
        .column = std::move(dropped),
        .old_name = {},
    };
    return NextVersion(schema, std::move(change), std::move(next), domain);
}

TableSchema RenameDomainColumn(
    const TableSchema& schema, std::string_view name, std::string new_name, ColumnDomain domain) {
    const std::size_t at = ColumnIndex(schema, name);
    TableSchema next = schema;
    next.columns[at].name = std::move(new_name);
    // Checks read the column by its new name.
    for (auto& check : next.checks) check.expression = expr::RenameColumnIn(check.expression, name, next.columns[at].name);
    SchemaChange change{
        .kind = SchemaChange::Kind::kRenameColumn,
        .position = static_cast<std::uint32_t>(at),
        .column = next.columns[at],
        .old_name = std::string(name),
    };
    return NextVersion(schema, std::move(change), std::move(next), domain);
}

}  // namespace

TableSchema AddColumn(const TableSchema& schema, Column column, bool allow_required_without_default) {
    return AddDomainColumn(schema, std::move(column), allow_required_without_default, ColumnDomain::kBlock);
}
TableSchema DropColumn(const TableSchema& schema, std::string_view name) {
    return DropDomainColumn(schema, name, ColumnDomain::kBlock);
}
TableSchema RenameColumn(const TableSchema& schema, std::string_view name, std::string new_name) {
    return RenameDomainColumn(schema, name, std::move(new_name), ColumnDomain::kBlock);
}
TableSchema AddChunkColumn(const TableSchema& schema, Column column, bool allow_required_without_default) {
    return AddDomainColumn(schema, std::move(column), allow_required_without_default, ColumnDomain::kChunk);
}
TableSchema DropChunkColumn(const TableSchema& schema, std::string_view name) {
    return DropDomainColumn(schema, name, ColumnDomain::kChunk);
}
TableSchema RenameChunkColumn(const TableSchema& schema, std::string_view name, std::string new_name) {
    return RenameDomainColumn(schema, name, std::move(new_name), ColumnDomain::kChunk);
}
TableSchema AdvanceOtherDomain(const TableSchema& schema, ColumnDomain domain) {
    return NextVersion(schema, SchemaChange{.kind = SchemaChange::Kind::kOtherDomain}, schema, domain);
}

std::string UnsupportedSchemaReason(const TableSchema& schema) {
    if (FixedBitsPerBlock(schema) == 0U) {
        return "a table needs at least one fixed-width column";
    }
    return {};
}

namespace {

[[nodiscard]] std::string ValueTypeName(const ColumnValue& value) {
    switch (value.index()) {
        case 0:
            return "NULL";
        case 1:
            return "an unsigned integer";
        case 2:
            return "a signed integer";
        case 3:
            return "a bool";
        case 4:
            return "an f32";
        case 5:
            return "an f64";
        case 6:
            return "bits(" + std::to_string(std::get<BitsValue>(value).digits.size()) + ")";
        case 7:
            return "text";
        default:
            return "bytes";
    }
}

template <typename T>
[[nodiscard]] const T& ValueOf(const Column& column, const ColumnValue& value) {
    const T* typed = std::get_if<T>(&value);
    if (typed == nullptr) {
        throw std::invalid_argument(
            "column " + column.name + " is " + ColumnTypeName(column.type) + ", not " + ValueTypeName(value));
    }
    return *typed;
}

}  // namespace

std::vector<std::uint8_t> EncodeColumnValue(const Column& column, const ColumnValue& value) {
    // The type bounds the allocation.
    ValidateColumnType(column);
    std::vector<std::uint8_t> bytes((std::size_t{FixedWidthBits(column.type)} + 7U) / 8U, 0U);
    EncodeColumnValue(column, value, bytes.data());
    return bytes;
}

void EncodeColumnValue(const Column& column, const ColumnValue& value, std::uint8_t* bytes) {
    const std::uint32_t width = FixedWidthBits(column.type);
    if (width == 0U) {
        throw std::invalid_argument("column " + column.name + ": text and bytes columns are not supported yet");
    }
    if (std::holds_alternative<std::monostate>(value)) {
        throw std::invalid_argument("column " + column.name + " cannot be NULL");
    }
    std::fill_n(bytes, (std::size_t{width} + 7U) / 8U, std::uint8_t{0});
    if (column.type.kind == ColumnKind::kBits) {
        const auto& digits = ValueOf<BitsValue>(column, value).digits;
        if (digits.size() != width) {
            throw std::invalid_argument(
                "column " + column.name + " is " + ColumnTypeName(column.type) + ", not " + ValueTypeName(value));
        }
        for (std::size_t i = 0; i < digits.size(); ++i) {
            if (digits[i] != '0' && digits[i] != '1') {
                throw std::invalid_argument("column " + column.name + ": a bits value holds only 0 and 1");
            }
            if (digits[i] == '1') {
                bytes[i / 8U] |= static_cast<std::uint8_t>(1U << (i % 8U));
            }
        }
        return;
    }
    std::uint64_t raw = 0;
    switch (column.type.kind) {
        case ColumnKind::kUnsigned: {
            raw = ValueOf<std::uint64_t>(column, value);
            if (width < 64U && (raw >> width) != 0U) {
                throw std::invalid_argument(
                    "column " + column.name + " is " + ColumnTypeName(column.type) + ": " + std::to_string(raw) +
                    " is out of range 0.." + std::to_string((std::uint64_t{1} << width) - 1U));
            }
            break;
        }
        case ColumnKind::kSigned: {
            const std::int64_t signed_value = ValueOf<std::int64_t>(column, value);
            if (width < 64U) {
                const std::int64_t max = (std::int64_t{1} << (width - 1U)) - 1;
                const std::int64_t min = -max - 1;
                if (signed_value < min || signed_value > max) {
                    throw std::invalid_argument(
                        "column " + column.name + " is " + ColumnTypeName(column.type) + ": " +
                        std::to_string(signed_value) + " is out of range " + std::to_string(min) + ".." +
                        std::to_string(max));
                }
            }
            raw = static_cast<std::uint64_t>(signed_value);
            break;
        }
        case ColumnKind::kBool:
            raw = ValueOf<bool>(column, value) ? 1U : 0U;
            break;
        case ColumnKind::kFloat32:
            raw = std::bit_cast<std::uint32_t>(ValueOf<float>(column, value));
            break;
        case ColumnKind::kFloat64:
            raw = std::bit_cast<std::uint64_t>(ValueOf<double>(column, value));
            break;
        default:
            break;
    }
    if (width < 64U) {
        raw &= (std::uint64_t{1} << width) - 1U;
    }
    // An integer, bool or float takes at most 8 bytes.
    const std::size_t raw_bytes = std::min<std::size_t>((std::size_t{width} + 7U) / 8U, sizeof raw);
    for (std::size_t i = 0; i < raw_bytes; ++i) {
        bytes[i] = static_cast<std::uint8_t>(raw >> (8U * i));
    }
}

ColumnValue DecodeColumnValue(const Column& column, const std::uint8_t* bytes) {
    const std::uint32_t width = FixedWidthBits(column.type);
    if (column.type.kind == ColumnKind::kBits) {
        BitsValue bits;
        bits.digits.resize(width);
        for (std::size_t i = 0; i < width; ++i) {
            bits.digits[i] = ((bytes[i / 8U] >> (i % 8U)) & 1U) != 0U ? '1' : '0';
        }
        return bits;
    }
    std::uint64_t raw = 0;
    const std::size_t raw_bytes = std::min<std::size_t>((std::size_t{width} + 7U) / 8U, sizeof raw);
    for (std::size_t i = 0; i < raw_bytes; ++i) {
        raw |= static_cast<std::uint64_t>(bytes[i]) << (8U * i);
    }
    switch (column.type.kind) {
        case ColumnKind::kUnsigned:
            return raw;
        case ColumnKind::kSigned:
            if (width < 64U && ((raw >> (width - 1U)) & 1U) != 0U) {
                raw |= ~((std::uint64_t{1} << width) - 1U);
            }
            return static_cast<std::int64_t>(raw);
        case ColumnKind::kBool:
            return raw != 0U;
        case ColumnKind::kFloat32:
            return std::bit_cast<float>(static_cast<std::uint32_t>(raw));
        case ColumnKind::kFloat64:
            return std::bit_cast<double>(raw);
        default:
            throw std::invalid_argument("column " + column.name + ": text and bytes columns are not supported yet");
    }
}

namespace {

void EncodeColumn(std::vector<std::uint8_t>& out, const Column& column) {
    WriteLe32(out, column.id);
    out.push_back(static_cast<std::uint8_t>(column.type.kind));
    WriteLe32(out, column.type.size);
    out.push_back(static_cast<std::uint8_t>(
        (column.nullable ? kFlagNullable : 0U) | (column.required ? kFlagRequired : 0U) |
        (column.has_default ? kFlagHasDefault : 0U)));
    out.push_back(static_cast<std::uint8_t>(column.name.size()));
    out.insert(out.end(), column.name.begin(), column.name.end());
    if (column.has_default) {
        WriteLe32(out, static_cast<std::uint32_t>(column.default_value.size()));
        out.insert(out.end(), column.default_value.begin(), column.default_value.end());
    }
}

[[nodiscard]] Column DecodeColumn(Reader& in) {
    Column column;
    column.id = static_cast<std::uint32_t>(in.Le(4));
    const auto kind = static_cast<std::uint8_t>(in.Le(1));
    if (!IsKnownKind(kind)) {
        throw std::runtime_error("unknown column type " + std::to_string(kind));
    }
    column.type = ColumnType{.kind = static_cast<ColumnKind>(kind), .size = static_cast<std::uint32_t>(in.Le(4))};
    const auto flags = static_cast<std::uint8_t>(in.Le(1));
    if ((flags & ~kKnownFlags) != 0U) {
        throw std::runtime_error("unknown column flags " + std::to_string(flags));
    }
    column.nullable = (flags & kFlagNullable) != 0U;
    column.required = (flags & kFlagRequired) != 0U;
    column.has_default = (flags & kFlagHasDefault) != 0U;
    const auto name_length = static_cast<std::size_t>(in.Le(1));
    const auto name = in.Bytes(name_length);
    column.name.assign(name.begin(), name.end());
    if (column.has_default) {
        const auto length = static_cast<std::size_t>(in.Le(4));
        if (length > kMaxVariableValueBytes) {
            throw std::runtime_error("column default of " + std::to_string(length) + " bytes");
        }
        column.default_value = in.Bytes(length);
    }
    return column;
}

[[nodiscard]] bool IsKnownChange(std::uint8_t kind) noexcept {
    return kind >= static_cast<std::uint8_t>(SchemaChange::Kind::kAddColumn) &&
           kind <= static_cast<std::uint8_t>(SchemaChange::Kind::kDropCheck);
}

// A check: name length `u8`, name, expression length `u32`, expression.
void EncodeCheck(std::vector<std::uint8_t>& out, const CheckConstraint& check) {
    out.push_back(static_cast<std::uint8_t>(check.name.size()));
    out.insert(out.end(), check.name.begin(), check.name.end());
    WriteLe32(out, static_cast<std::uint32_t>(check.expression.size()));
    out.insert(out.end(), check.expression.begin(), check.expression.end());
}

[[nodiscard]] CheckConstraint DecodeCheck(Reader& in) {
    CheckConstraint check;
    const auto name = in.Bytes(static_cast<std::size_t>(in.Le(1)));
    check.name.assign(name.begin(), name.end());
    const auto length = static_cast<std::size_t>(in.Le(4));
    if (length > kMaxCheckExpressionBytes) {
        throw std::runtime_error("CHECK expression of " + std::to_string(length) + " bytes");
    }
    const auto expression = in.Bytes(length);
    check.expression.assign(expression.begin(), expression.end());
    return check;
}

// Tags of the sections after the history.
constexpr std::uint8_t kTagNarrowing = 1U;
constexpr std::uint8_t kTagDropNull = 2U;
constexpr std::uint8_t kTagAddCheck = 3U;
constexpr std::uint8_t kTagChecks = 4U;

}  // namespace

namespace {

std::vector<std::uint8_t> EncodeSchema(const TableSchema& schema, ColumnDomain domain) {
    ValidateSchema(schema, domain);
    std::vector<std::uint8_t> out;
    WriteLe64(out, schema.version);
    WriteLe32(out, schema.next_column_id);
    WriteLe32(out, static_cast<std::uint32_t>(schema.columns.size()));
    for (const auto& column : schema.columns) {
        EncodeColumn(out, column);
    }
    for (const auto& step : schema.history) {
        WriteLe32(out, static_cast<std::uint32_t>(step.changes.size()));
        for (const auto& change : step.changes) {
            out.push_back(static_cast<std::uint8_t>(change.kind));
            if (change.kind == SchemaChange::Kind::kOtherDomain) continue;
            WriteLe32(out, change.position);
            if (change.kind == SchemaChange::Kind::kAddCheck || change.kind == SchemaChange::Kind::kDropCheck) {
                EncodeCheck(out, change.check);
                continue;
            }
            EncodeColumn(out, change.column);
            if (change.kind == SchemaChange::Kind::kRenameColumn) {
                out.push_back(static_cast<std::uint8_t>(change.old_name.size()));
                out.insert(out.end(), change.old_name.begin(), change.old_name.end());
            }
            if (change.kind == SchemaChange::Kind::kChangeType) {
                EncodeColumn(out, change.previous);
                out.push_back(static_cast<std::uint8_t>(change.conversion));
            }
            if (change.kind == SchemaChange::Kind::kChangeAttributes) {
                EncodeColumn(out, change.previous);
            }
        }
    }
    if (!schema.checks.empty()) {
        out.push_back(kTagChecks);
        WriteLe32(out, static_cast<std::uint32_t>(schema.checks.size()));
        for (const auto& check : schema.checks) EncodeCheck(out, check);
    }
    if (schema.pending.has_value() && schema.pending->check.has_value()) {
        out.push_back(kTagAddCheck);
        EncodeCheck(out, *schema.pending->check);
    } else if (schema.pending.has_value()) {
        out.push_back(schema.pending->not_null ? kTagDropNull : kTagNarrowing);
        WriteLe32(out, schema.pending->column_id);
        out.push_back(static_cast<std::uint8_t>(schema.pending->type.kind));
        WriteLe32(out, schema.pending->type.size);
    }
    return out;
}

TableSchema DecodeSchema(const std::uint8_t* data, std::size_t size, ColumnDomain domain) {
    Reader in(data, size);
    TableSchema schema;
    schema.version = in.Le(8);
    schema.next_column_id = static_cast<std::uint32_t>(in.Le(4));
    const auto count = static_cast<std::uint32_t>(in.Le(4));
    if (count > kMaxColumnsPerTable) {
        throw std::runtime_error("schema area declares " + std::to_string(count) + " columns");
    }
    schema.columns.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        schema.columns.push_back(DecodeColumn(in));
    }
    if (schema.version == 0U || schema.version - 1U > kMaxSchemaVersions) {
        throw std::runtime_error("schema version " + std::to_string(schema.version) + " is out of range");
    }
    // One step per version above 1, each creating the next version.
    for (std::uint64_t version = 2; version <= schema.version; ++version) {
        SchemaStep step{.version = version, .changes = {}};
        const auto changes = static_cast<std::uint32_t>(in.Le(4));
        if (changes == 0U || changes > kMaxColumnsPerTable * 2U) {
            throw std::runtime_error("schema version " + std::to_string(version) + " has " + std::to_string(changes) + " changes");
        }
        for (std::uint32_t i = 0; i < changes; ++i) {
            const auto kind = static_cast<std::uint8_t>(in.Le(1));
            if (!IsKnownChange(kind)) {
                throw std::runtime_error("unknown schema change " + std::to_string(kind));
            }
            SchemaChange change;
            change.kind = static_cast<SchemaChange::Kind>(kind);
            if (change.kind == SchemaChange::Kind::kOtherDomain) {
                step.changes.push_back(std::move(change));
                continue;
            }
            change.position = static_cast<std::uint32_t>(in.Le(4));
            if (change.kind == SchemaChange::Kind::kAddCheck || change.kind == SchemaChange::Kind::kDropCheck) {
                change.check = DecodeCheck(in);
                step.changes.push_back(std::move(change));
                continue;
            }
            change.column = DecodeColumn(in);
            if (change.kind == SchemaChange::Kind::kRenameColumn) {
                const auto name = in.Bytes(static_cast<std::size_t>(in.Le(1)));
                change.old_name.assign(name.begin(), name.end());
            }
            if (change.kind == SchemaChange::Kind::kChangeType) {
                change.previous = DecodeColumn(in);
                const auto conversion = static_cast<std::uint8_t>(in.Le(1));
                if (conversion > static_cast<std::uint8_t>(Conversion::kTruncate)) {
                    throw std::runtime_error("unknown type conversion " + std::to_string(conversion));
                }
                change.conversion = static_cast<Conversion>(conversion);
            }
            if (change.kind == SchemaChange::Kind::kChangeAttributes) {
                change.previous = DecodeColumn(in);
            }
            step.changes.push_back(std::move(change));
        }
        schema.history.push_back(std::move(step));
    }
    // The checks, then a narrowing, DROP NULL check or ADD CHECK in progress.
    // kNoTag: the area ends.
    constexpr std::uint64_t kNoTag = 256U;
    std::uint64_t tag = in.AtEnd() ? kNoTag : in.Le(1);
    if (tag == kTagChecks) {
        const auto checks = static_cast<std::uint32_t>(in.Le(4));
        if (checks == 0U || checks > kMaxChecksPerTable) {
            throw std::runtime_error("schema area declares " + std::to_string(checks) + " CHECK constraints");
        }
        for (std::uint32_t i = 0; i < checks; ++i) schema.checks.push_back(DecodeCheck(in));
        tag = in.AtEnd() ? kNoTag : in.Le(1);
    }
    if (tag == kTagAddCheck) {
        schema.pending = PendingNarrowing{.check = DecodeCheck(in)};
    } else if (tag != kNoTag) {
        if (tag != kTagNarrowing && tag != kTagDropNull) {
            throw std::runtime_error("schema area has trailing bytes");
        }
        PendingNarrowing pending;
        pending.not_null = tag == kTagDropNull;
        pending.column_id = static_cast<std::uint32_t>(in.Le(4));
        const auto kind = static_cast<std::uint8_t>(in.Le(1));
        if (!IsKnownKind(kind)) {
            throw std::runtime_error("unknown column type " + std::to_string(kind));
        }
        pending.type = ColumnType{.kind = static_cast<ColumnKind>(kind), .size = static_cast<std::uint32_t>(in.Le(4))};
        schema.pending = pending;
    }
    if (!in.AtEnd()) {
        throw std::runtime_error("schema area has trailing bytes");
    }
    ValidateSchema(schema, domain);
    return schema;
}

}  // namespace

std::vector<std::uint8_t> EncodeTableSchema(const TableSchema& schema) {
    return EncodeSchema(schema, ColumnDomain::kBlock);
}
std::vector<std::uint8_t> EncodeChunkSchema(const TableSchema& schema) {
    return EncodeSchema(schema, ColumnDomain::kChunk);
}
TableSchema DecodeTableSchema(const std::uint8_t* data, std::size_t size) {
    return DecodeSchema(data, size, ColumnDomain::kBlock);
}
TableSchema DecodeChunkSchema(const std::uint8_t* data, std::size_t size) {
    return DecodeSchema(data, size, ColumnDomain::kChunk);
}

std::vector<std::uint8_t> EncodeVarValue(const Column& column, const ColumnValue& value) {
    if (std::holds_alternative<std::monostate>(value)) {
        throw std::invalid_argument("column " + column.name + " cannot be NULL");
    }
    std::vector<std::uint8_t> bytes;
    if (column.type.kind == ColumnKind::kText) {
        const auto& text = ValueOf<std::string>(column, value);
        bytes.assign(text.begin(), text.end());
        if (!IsUtf8(bytes)) {
            throw std::invalid_argument("column " + column.name + ": the text is not UTF-8");
        }
    } else if (column.type.kind == ColumnKind::kBytes) {
        bytes = ValueOf<BytesValue>(column, value).bytes;
    } else {
        throw std::invalid_argument("column " + column.name + " is " + ColumnTypeName(column.type) + ", not text or bytes");
    }
    if (bytes.size() > column.type.size) {
        throw std::invalid_argument(
            "column " + column.name + " is " + ColumnTypeName(column.type) + ": " + std::to_string(bytes.size()) +
            " bytes are too long");
    }
    return bytes;
}

ColumnValue DecodeVarValue(const Column& column, std::span<const std::uint8_t> bytes) {
    if (column.type.kind == ColumnKind::kText) {
        return std::string(bytes.begin(), bytes.end());
    }
    return BytesValue{.bytes = std::vector<std::uint8_t>(bytes.begin(), bytes.end())};
}

namespace {

enum class Family { kInteger, kFloat, kBool, kBits, kText, kBytes };

[[nodiscard]] Family FamilyOf(ColumnKind kind) noexcept {
    switch (kind) {
        case ColumnKind::kUnsigned:
        case ColumnKind::kSigned:
            return Family::kInteger;
        case ColumnKind::kFloat32:
        case ColumnKind::kFloat64:
            return Family::kFloat;
        case ColumnKind::kBool:
            return Family::kBool;
        case ColumnKind::kBits:
            return Family::kBits;
        case ColumnKind::kText:
            return Family::kText;
        case ColumnKind::kBytes:
            return Family::kBytes;
    }
    return Family::kBool;
}

// Whether `to` holds every value of `from` (same family).
[[nodiscard]] bool Widens(const ColumnType& from, const ColumnType& to) noexcept {
    if (from.kind == ColumnKind::kUnsigned && to.kind == ColumnKind::kSigned) {
        return to.size > from.size;
    }
    if (from.kind == ColumnKind::kSigned && to.kind == ColumnKind::kUnsigned) {
        return false;
    }
    if (from.kind == ColumnKind::kFloat32 || from.kind == ColumnKind::kFloat64) {
        return to.kind == ColumnKind::kFloat64 || from.kind == ColumnKind::kFloat32;
    }
    return to.size >= from.size;
}

using Wide = __int128;

[[nodiscard]] Wide IntegerOf(const ColumnValue& value) {
    if (const auto* u = std::get_if<std::uint64_t>(&value)) {
        return static_cast<Wide>(*u);
    }
    return static_cast<Wide>(std::get<std::int64_t>(value));
}

[[nodiscard]] std::pair<Wide, Wide> RangeOf(const ColumnType& type) noexcept {
    if (type.kind == ColumnKind::kUnsigned) {
        return {0, (static_cast<Wide>(1) << type.size) - 1};
    }
    const Wide half = static_cast<Wide>(1) << (type.size - 1U);
    return {-half, half - 1};
}

[[nodiscard]] ColumnValue IntegerValue(const ColumnType& type, Wide number) {
    if (type.kind == ColumnKind::kUnsigned) {
        return static_cast<std::uint64_t>(number);
    }
    return static_cast<std::int64_t>(number);
}

[[nodiscard]] double FloatOf(const ColumnValue& value) {
    if (const auto* f = std::get_if<float>(&value)) {
        return static_cast<double>(*f);
    }
    return std::get<double>(value);
}

[[nodiscard]] ColumnValue FloatValue(const ColumnType& type, double number) {
    if (type.kind == ColumnKind::kFloat32) {
        return static_cast<float>(number);
    }
    return number;
}

[[nodiscard]] std::vector<std::uint8_t> BytesOf(const ColumnValue& value) {
    if (const auto* text = std::get_if<std::string>(&value)) {
        return {text->begin(), text->end()};
    }
    return std::get<BytesValue>(value).bytes;
}

[[nodiscard]] ColumnValue VarValue(const ColumnType& type, std::vector<std::uint8_t> bytes) {
    if (type.kind == ColumnKind::kText) {
        return std::string(bytes.begin(), bytes.end());
    }
    return BytesValue{.bytes = std::move(bytes)};
}

// `value` as a value of type `to`, or std::nullopt when it does not fit.
[[nodiscard]] std::optional<ColumnValue> Fit(const ColumnType& to, const ColumnValue& value) {
    switch (FamilyOf(to.kind)) {
        case Family::kInteger: {
            const Wide number = IntegerOf(value);
            const auto [low, high] = RangeOf(to);
            if (number < low || number > high) {
                return std::nullopt;
            }
            return IntegerValue(to, number);
        }
        case Family::kFloat: {
            const double number = FloatOf(value);
            if (to.kind == ColumnKind::kFloat32 && std::isfinite(number) &&
                std::fabs(number) > static_cast<double>(std::numeric_limits<float>::max())) {
                return std::nullopt;
            }
            return FloatValue(to, number);
        }
        case Family::kBool:
            return value;
        case Family::kBits: {
            auto digits = std::get<BitsValue>(value).digits;
            if (digits.size() <= to.size) {
                digits.resize(to.size, '0');
                return BitsValue{.digits = std::move(digits)};
            }
            if (digits.find('1', to.size) != std::string::npos) {
                return std::nullopt;
            }
            digits.resize(to.size);
            return BitsValue{.digits = std::move(digits)};
        }
        case Family::kText:
        case Family::kBytes: {
            auto bytes = BytesOf(value);
            if (bytes.size() > to.size) {
                return std::nullopt;
            }
            return VarValue(to, std::move(bytes));
        }
    }
    return std::nullopt;
}

// What a column takes for a value that does not fit under kDefault.
[[nodiscard]] ColumnValue FallbackOf(const Column& column) {
    if (column.has_default) {
        return IsFixedWidth(column.type.kind) ? DecodeColumnValue(column, column.default_value.data())
                                              : DecodeVarValue(column, column.default_value);
    }
    if (column.nullable) {
        return std::monostate{};
    }
    switch (FamilyOf(column.type.kind)) {
        case Family::kInteger:
            return IntegerValue(column.type, 0);
        case Family::kFloat:
            return FloatValue(column.type, 0.0);
        case Family::kBool:
            return false;
        case Family::kBits:
            return BitsValue{.digits = std::string(column.type.size, '0')};
        case Family::kText:
        case Family::kBytes:
            return VarValue(column.type, {});
    }
    return std::monostate{};
}

[[nodiscard]] ColumnValue Clamp(const ColumnType& to, const ColumnValue& value) {
    if (FamilyOf(to.kind) == Family::kInteger) {
        const auto [low, high] = RangeOf(to);
        return IntegerValue(to, std::clamp(IntegerOf(value), low, high));
    }
    const double limit = static_cast<double>(std::numeric_limits<float>::max());
    return FloatValue(to, std::clamp(FloatOf(value), -limit, limit));
}

[[nodiscard]] ColumnValue Truncate(const ColumnType& to, const ColumnValue& value) {
    if (to.kind == ColumnKind::kBits) {
        auto digits = std::get<BitsValue>(value).digits;
        digits.resize(to.size);
        return BitsValue{.digits = std::move(digits)};
    }
    auto bytes = BytesOf(value);
    std::size_t keep = to.size;
    if (to.kind == ColumnKind::kText) {
        // Back to the start of a character.
        while (keep > 0 && (bytes[keep] & 0xC0U) == 0x80U) {
            --keep;
        }
    }
    bytes.resize(keep);
    return VarValue(to, std::move(bytes));
}

[[nodiscard]] bool TakesConversion(Family family, Conversion conversion) noexcept {
    switch (conversion) {
        case Conversion::kExact:
        case Conversion::kDefault:
            return true;
        case Conversion::kClamp:
            return family == Family::kInteger || family == Family::kFloat;
        case Conversion::kTruncate:
            return family == Family::kText || family == Family::kBytes || family == Family::kBits;
    }
    return false;
}

}  // namespace

bool HoldsEveryValue(const ColumnType& from, const ColumnType& to) noexcept {
    return FamilyOf(from.kind) == FamilyOf(to.kind) && Widens(from, to);
}

ColumnValue ConvertValue(const Column& from, const Column& to, Conversion conversion, const ColumnValue& value) {
    (void)from;
    if (std::holds_alternative<std::monostate>(value)) {
        return value;
    }
    if (auto fitted = Fit(to.type, value); fitted.has_value()) {
        return std::move(*fitted);
    }
    switch (conversion) {
        case Conversion::kExact:
            throw std::logic_error("column " + to.name + ": a value does not fit " + ColumnTypeName(to.type));
        case Conversion::kClamp:
            return Clamp(to.type, value);
        case Conversion::kDefault:
            return FallbackOf(to);
        case Conversion::kTruncate:
            return Truncate(to.type, value);
    }
    throw std::logic_error("unknown type conversion");
}

namespace {

// The next version with column `at` of `type`; the default follows by
// `conversion`. The caller has checked the change is allowed.
// Throws std::invalid_argument when column `at` cannot take `type` by
// `conversion` under the table's checks: a conversion that may change the
// values a check reads, or a check that does not plan with the new type.
void RequireChecksTakeType(const TableSchema& schema, std::size_t at, const ColumnType& type, Conversion conversion) {
    if (schema.checks.empty()) return;
    auto columns = schema.columns;
    columns[at].type = type;
    const auto& name = columns[at].name;
    for (const auto& check : schema.checks) {
        if (!ReadsColumn(check, name)) continue;
        if (conversion != Conversion::kExact)
            throw std::invalid_argument(
                "column " + name + " is read by CHECK " + check.name + ", which converted values could fail; drop the check first");
        try {
            ValidateCheck(check, columns);
        } catch (const std::invalid_argument& error) {
            throw std::invalid_argument(
                "column " + name + " cannot become " + ColumnTypeName(type) + ": " + error.what() + "; drop the check first");
        }
    }
}

[[nodiscard]] TableSchema ChangeTypeAt(const TableSchema& schema, std::size_t at, ColumnType type, Conversion conversion) {
    const Column& before = schema.columns[at];
    RequireChecksTakeType(schema, at, type, conversion);
    TableSchema next = schema;
    Column& after = next.columns[at];
    after.type = type;
    // Before the default is fitted to the type and encoded.
    ValidateColumnType(after);
    if (before.has_default) {
        // The default follows the values, and must fit as they do.
        const ColumnValue value = IsFixedWidth(before.type.kind) ? DecodeColumnValue(before, before.default_value.data())
                                                                 : DecodeVarValue(before, before.default_value);
        auto fitted = Fit(type, value);
        if (!fitted.has_value()) {
            if (conversion == Conversion::kDefault || conversion == Conversion::kExact) {
                throw std::invalid_argument(
                    "column " + before.name + ": its DEFAULT does not fit " + ColumnTypeName(type) + "; change it first");
            }
            fitted = conversion == Conversion::kClamp ? Clamp(type, value) : Truncate(type, value);
        }
        after.default_value =
            IsFixedWidth(type.kind) ? EncodeColumnValue(after, *fitted) : EncodeVarValue(after, *fitted);
    }
    // Built before the call: `after` lives in `next`, which is moved.
    SchemaChange change{
        .kind = SchemaChange::Kind::kChangeType,
        .position = static_cast<std::uint32_t>(at),
        .column = after,
        .old_name = {},
        .previous = before,
        .conversion = conversion,
    };
    return NextVersion(schema, std::move(change), std::move(next));
}

}  // namespace

TableSchema ChangeColumnType(
    const TableSchema& schema,
    std::string_view name,
    ColumnType type,
    Conversion conversion) {
    const std::size_t at = ColumnIndex(schema, name);
    const Column& before = schema.columns[at];
    const Family family = FamilyOf(before.type.kind);
    if (FamilyOf(type.kind) != family) {
        throw std::invalid_argument(
            "column " + before.name + " cannot change from " + ColumnTypeName(before.type) + " to " +
            ColumnTypeName(type) + ": add a column of the new type, copy the values and drop this one");
    }
    if (before.type == type) {
        throw std::invalid_argument("column " + before.name + " is already " + ColumnTypeName(type));
    }
    if (conversion == Conversion::kExact && !Widens(before.type, type)) {
        throw std::invalid_argument(
            "column " + before.name + ": " + ColumnTypeName(type) + " does not hold every " + ColumnTypeName(before.type) +
            " value; say what happens to those that do not fit (CLAMP, DEFAULT or TRUNCATE)");
    }
    if (!TakesConversion(family, conversion)) {
        throw std::invalid_argument(
            "column " + before.name + ": " + ColumnTypeName(before.type) + " values cannot be " +
            (conversion == Conversion::kClamp ? "clamped" : "truncated"));
    }
    return ChangeTypeAt(schema, at, type, conversion);
}

namespace {

void ValidatePending(const TableSchema& schema) {
    if (!schema.pending.has_value()) {
        return;
    }
    const auto& pending = *schema.pending;
    if (pending.check.has_value()) {
        if (pending.column_id != 0U || pending.type != ColumnType{} || pending.not_null)
            throw std::invalid_argument("the CHECK in progress also names a column");
        if (HasCheck(schema, pending.check->name))
            throw std::invalid_argument("the table already has a CHECK named " + pending.check->name);
        if (schema.checks.size() >= kMaxChecksPerTable)
            throw std::invalid_argument("a table has at most " + std::to_string(kMaxChecksPerTable) + " CHECK constraints");
        ValidateCheck(*pending.check, schema.columns);
        return;
    }
    const auto column = std::find_if(schema.columns.begin(), schema.columns.end(), [&](const Column& c) {
        return c.id == pending.column_id;
    });
    if (column == schema.columns.end()) {
        throw std::invalid_argument("the narrowing in progress names no column of the table");
    }
    if (pending.not_null) {
        if (!column->nullable || pending.type != column->type) {
            throw std::invalid_argument("column " + column->name + " has no NULL to drop");
        }
        return;
    }
    if (FamilyOf(pending.type.kind) != FamilyOf(column->type.kind) || pending.type == column->type ||
        Widens(column->type, pending.type)) {
        throw std::invalid_argument(
            "column " + column->name + " cannot be narrowed from " + ColumnTypeName(column->type) + " to " +
            ColumnTypeName(pending.type));
    }
    Column narrowed = *column;
    narrowed.type = pending.type;
    narrowed.has_default = false;
    narrowed.default_value.clear();
    ValidateColumnType(narrowed);
    if (column->has_default) {
        const ColumnValue value = IsFixedWidth(column->type.kind) ? DecodeColumnValue(*column, column->default_value.data())
                                                                  : DecodeVarValue(*column, column->default_value);
        if (!Fit(pending.type, value).has_value()) {
            throw std::invalid_argument(
                "column " + column->name + ": its DEFAULT does not fit " + ColumnTypeName(pending.type) + "; change it first");
        }
    }
}

}  // namespace

std::string ColumnTypeRange(const ColumnType& type) {
    const auto prefix = ColumnTypeName(type) + " holds ";
    switch (type.kind) {
        case ColumnKind::kUnsigned: {
            const auto high = type.size == 64U ? std::numeric_limits<std::uint64_t>::max() : (std::uint64_t{1} << type.size) - 1U;
            return prefix + "0.." + std::to_string(high);
        }
        case ColumnKind::kSigned: {
            const auto low = type.size == 64U ? std::numeric_limits<std::int64_t>::min() : -(std::int64_t{1} << (type.size - 1U));
            const auto high = type.size == 64U ? std::numeric_limits<std::int64_t>::max() : (std::int64_t{1} << (type.size - 1U)) - 1;
            return prefix + std::to_string(low) + ".." + std::to_string(high);
        }
        case ColumnKind::kFloat32:
        case ColumnKind::kFloat64: {
            const double high = type.kind == ColumnKind::kFloat32 ? static_cast<double>(std::numeric_limits<float>::max()) : std::numeric_limits<double>::max();
            std::ostringstream range;
            range << std::setprecision(std::numeric_limits<double>::max_digits10) << -high << ".." << high;
            return prefix + "finite values in " + range.str() + " (also NaN and infinities)";
        }
        case ColumnKind::kText: return prefix + "0.." + std::to_string(type.size) + " UTF-8 bytes";
        case ColumnKind::kBytes: return prefix + "0.." + std::to_string(type.size) + " bytes";
        case ColumnKind::kBits: return prefix + std::to_string(type.size) + " bits; discarded bits must be zero";
        case ColumnKind::kBool: return prefix + "false or true";
    }
    throw std::invalid_argument("unknown column type");
}

bool ValueFits(const ColumnType& type, const ColumnValue& value) {
    return std::holds_alternative<std::monostate>(value) || Fit(type, value).has_value();
}

TableSchema WithPendingNarrowing(const TableSchema& schema, std::string_view name, ColumnType type) {
    if (schema.pending.has_value()) {
        throw std::invalid_argument("another narrowing of this table is in progress");
    }
    const std::size_t at = ColumnIndex(schema, name);
    RequireChecksTakeType(schema, at, type, Conversion::kExact);
    TableSchema next = schema;
    next.pending = PendingNarrowing{.column_id = schema.columns[at].id, .type = type};
    ValidateTableSchema(next);
    return next;
}

TableSchema NarrowColumnType(const TableSchema& schema, std::string_view name, ColumnType type) {
    const std::size_t at = ColumnIndex(schema, name);
    if (!schema.pending.has_value() || schema.pending->not_null || schema.pending->column_id != schema.columns[at].id ||
        schema.pending->type != type) {
        throw std::invalid_argument("column " + std::string(name) + " has no narrowing to " + ColumnTypeName(type) + " in progress");
    }
    return ChangeTypeAt(WithoutPendingNarrowing(schema), at, type, Conversion::kExact);
}

TableSchema WithoutPendingNarrowing(const TableSchema& schema) {
    TableSchema next = schema;
    next.pending.reset();
    return next;
}

namespace {

// The next version with column `at` given `after`'s attributes; the caller
// has checked the change.
[[nodiscard]] TableSchema ChangeAttributesAt(const TableSchema& schema, std::size_t at, const Column& after) {
    TableSchema next = schema;
    Column& column = next.columns[at];
    column.nullable = after.nullable;
    column.required = after.required;
    column.has_default = after.has_default;
    column.default_value = after.has_default ? after.default_value : std::vector<std::uint8_t>{};
    SchemaChange change{
        .kind = SchemaChange::Kind::kChangeAttributes,
        .position = static_cast<std::uint32_t>(at),
        .column = column,
        .old_name = {},
        .previous = schema.columns[at],
        .conversion = Conversion::kExact,
    };
    return NextVersion(schema, std::move(change), std::move(next));
}

}  // namespace

TableSchema ChangeColumnAttributes(const TableSchema& schema, std::string_view name, const Column& attributes) {
    const std::size_t at = ColumnIndex(schema, name);
    const Column& before = schema.columns[at];
    if (before.nullable == attributes.nullable && before.required == attributes.required &&
        before.has_default == attributes.has_default &&
        (!attributes.has_default || before.default_value == attributes.default_value)) {
        throw std::invalid_argument("column " + before.name + " already has these attributes");
    }
    if (before.nullable && !attributes.nullable) {
        throw std::invalid_argument("column " + before.name + ": DROP NULL checks every stored value first");
    }
    if (attributes.nullable && attributes.required) {
        throw std::invalid_argument(
            "column " + before.name + " cannot be both NULL and REQUIRED; " +
            (before.required ? "DROP REQUIRED first" : "DROP NULL first"));
    }
    return ChangeAttributesAt(schema, at, attributes);
}

TableSchema WithPendingNotNull(const TableSchema& schema, std::string_view name) {
    if (schema.pending.has_value()) {
        throw std::invalid_argument("another narrowing of this table is in progress");
    }
    const Column& column = schema.columns[ColumnIndex(schema, name)];
    if (!column.nullable) {
        throw std::invalid_argument("column " + column.name + " is not NULL");
    }
    TableSchema next = schema;
    next.pending = PendingNarrowing{.column_id = column.id, .type = column.type, .not_null = true};
    ValidateTableSchema(next);
    return next;
}

TableSchema DropColumnNull(const TableSchema& schema, std::string_view name) {
    const std::size_t at = ColumnIndex(schema, name);
    if (!schema.pending.has_value() || !schema.pending->not_null || schema.pending->column_id != schema.columns[at].id) {
        throw std::invalid_argument("column " + std::string(name) + " has no DROP NULL check in progress");
    }
    Column after = schema.columns[at];
    after.nullable = false;
    return ChangeAttributesAt(WithoutPendingNarrowing(schema), at, after);
}

TableSchema WithPendingCheck(const TableSchema& schema, CheckConstraint check) {
    if (schema.pending.has_value()) {
        throw std::invalid_argument("another narrowing or CHECK of this table is in progress");
    }
    TableSchema next = schema;
    next.pending = PendingNarrowing{.column_id = 0U, .type = {}, .not_null = false, .check = std::move(check)};
    ValidateTableSchema(next);
    return next;
}

TableSchema AddPendingCheck(const TableSchema& schema) {
    if (!schema.pending.has_value() || !schema.pending->check.has_value()) {
        throw std::invalid_argument("the table has no CHECK in progress");
    }
    TableSchema next = WithoutPendingNarrowing(schema);
    next.checks.push_back(*schema.pending->check);
    SchemaChange change{
        .kind = SchemaChange::Kind::kAddCheck,
        .position = static_cast<std::uint32_t>(next.checks.size() - 1U),
        .check = next.checks.back(),
    };
    return NextVersion(WithoutPendingNarrowing(schema), std::move(change), std::move(next));
}

TableSchema DropCheck(const TableSchema& schema, std::string_view name) {
    const auto at = std::find_if(
        schema.checks.begin(), schema.checks.end(), [name](const CheckConstraint& check) { return check.name == name; });
    if (at == schema.checks.end()) {
        throw std::invalid_argument("the table has no CHECK " + std::string(name));
    }
    const auto position = static_cast<std::size_t>(at - schema.checks.begin());
    TableSchema next = schema;
    next.checks.erase(next.checks.begin() + static_cast<std::ptrdiff_t>(position));
    SchemaChange change{
        .kind = SchemaChange::Kind::kDropCheck,
        .position = static_cast<std::uint32_t>(position),
        .check = *at,
    };
    return NextVersion(schema, std::move(change), std::move(next));
}

}  // namespace chunkdb
