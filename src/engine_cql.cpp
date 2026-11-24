// Protocol 3: CQL statements over the typed column API, with typed replies
// (docs/CQL.md).

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <variant>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/protocol.hpp"
#include "aggregate.hpp"
#include "block_checks.hpp"
#include "block_filter.hpp"
#include "chunkdb/schema.hpp"
#include "chunk_store_internal.hpp"
#include "chunk_form.hpp"
#include "chunk_scale.hpp"
#include "typed_chunk_form.hpp"
#include "area_cursor.hpp"
#include "sectioned_chunk_form.hpp"
#include "chunkdb/chunk_columns.hpp"
#include "cql.hpp"
#include "expr.hpp"
#include "feature_flags.hpp"
#include "backup.hpp"
#include "slot_watch.hpp"
#include "feed_protocol.hpp"
#include "store_manifest.hpp"
#include "table_options_text.hpp"
#include "user_registry.hpp"
#include "migrations.hpp"
#include "reply_stream.hpp"
#include "statement_control.hpp"

namespace chunkdb {

namespace {

template <typename... Fn>
struct Overloaded : Fn... {
    using Fn::operator()...;
};
template <typename... Fn>
Overloaded(Fn...) -> Overloaded<Fn...>;

// The most bytes a parameter of `type` holds: uN and iN 8, little-endian;
// bool 1; f32 4; f64 8; bits(N) (N + 7) / 8, the lowest bit first; text and
// bytes their maximum.
[[nodiscard]] std::size_t ParameterBytes(const ColumnType& type) {
    switch (type.kind) {
        case ColumnKind::kUnsigned:
        case ColumnKind::kSigned:
        case ColumnKind::kFloat64:
            return 8;
        case ColumnKind::kBool:
            return 1;
        case ColumnKind::kFloat32:
            return 4;
        case ColumnKind::kBits:
            return (static_cast<std::size_t>(type.size) + 7U) / 8U;
        case ColumnKind::kText:
        case ColumnKind::kBytes:
            return type.size;
    }
    throw std::logic_error("unknown column kind");
}

// The most bytes the parameter frames of one statement take together. Each
// frame is already bounded by the column it fills, but a statement may name
// one column many times (SET BLOCK ... b=$1, b=$2, ...), which the store
// only refuses once every frame has been read; without a sum, a request line
// of a few kilobytes makes the server hold tens of gigabytes. The bound is
// what the values of one block can ever store -- a chunk's whole
// variable-length budget, which var_max_chunk_bytes never exceeds, plus the
// fixed-width columns a table may have -- so a statement that could succeed
// is never refused.
constexpr std::size_t kMaxStatementParameterBytes =
    kVarMaxChunkBytesLimit + static_cast<std::size_t>(kMaxColumnsPerTable) * 8U;

[[nodiscard]] std::uint64_t LoadLittleEndian(std::string_view bytes) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(bytes[i])) << (8U * i);
    }
    return value;
}

[[nodiscard]] std::string Describe(const Column& column) {
    // An expression's parameter has no column of its own.
    if (column.name.empty()) return "a " + ColumnTypeName(column.type) + " value";
    return "column " + column.name + " (" + ColumnTypeName(column.type) + ")";
}

// The value parameter $`index` sent for `column`.
[[nodiscard]] ColumnValue ParameterValue(
    const Column& column,
    std::size_t index,
    const std::optional<std::string>& frame) {
    if (!frame.has_value()) {
        return std::monostate{};
    }
    const std::string& bytes = *frame;
    const auto require_size = [&](std::size_t size) {
        if (bytes.size() != size) {
            throw std::invalid_argument(
                "$" + std::to_string(index) + " for " + Describe(column) + " must be " + std::to_string(size) +
                " bytes, got " + std::to_string(bytes.size()));
        }
    };
    switch (column.type.kind) {
        case ColumnKind::kUnsigned:
            require_size(8);
            return LoadLittleEndian(bytes);
        case ColumnKind::kSigned:
            require_size(8);
            return static_cast<std::int64_t>(LoadLittleEndian(bytes));
        case ColumnKind::kBool: {
            require_size(1);
            const auto byte = static_cast<std::uint8_t>(bytes[0]);
            if (byte > 1U) {
                throw std::invalid_argument("$" + std::to_string(index) + " for " + Describe(column) + " must be 0 or 1");
            }
            return byte == 1U;
        }
        case ColumnKind::kFloat32:
            require_size(4);
            return std::bit_cast<float>(static_cast<std::uint32_t>(LoadLittleEndian(bytes)));
        case ColumnKind::kFloat64:
            require_size(8);
            return std::bit_cast<double>(LoadLittleEndian(bytes));
        case ColumnKind::kBits: {
            require_size(ParameterBytes(column.type));
            const std::uint32_t tail = column.type.size % 8U;
            if (tail != 0U && (static_cast<std::uint8_t>(bytes.back()) >> tail) != 0U) {
                throw std::invalid_argument(
                    "$" + std::to_string(index) + " for " + Describe(column) + " has bits past its width");
            }
            return DecodeColumnValue(column, reinterpret_cast<const std::uint8_t*>(bytes.data()));
        }
        case ColumnKind::kText:
            return bytes;
        case ColumnKind::kBytes:
            return BytesValue{.bytes = std::vector<std::uint8_t>(bytes.begin(), bytes.end())};
    }
    throw std::logic_error("unknown column kind");
}

// A read's WHERE planned against `geometry`, with its parameters decoded as
// the types they take and NOW() the time the statement starts.
[[nodiscard]] BlockFilter WhereFilter(
    const Geometry& geometry,
    const expr::Expression& where,
    std::span<const std::optional<std::string>> parameters) {
    BlockFilter filter(geometry, where);
    std::vector<ColumnValue> values(parameters.size());
    for (const auto& parameter : filter.parameters()) {
        // Named as every expression parameter is (Describe).
        const Column column{.name = {}, .type = parameter.type, .nullable = true, .default_value = {}};
        values.at(parameter.index - 1U) = ParameterValue(column, parameter.index, parameters[parameter.index - 1U]);
    }
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    filter.Bind(std::move(values), static_cast<std::uint64_t>(std::max<std::int64_t>(now, 0)));
    return filter;
}

// The value a literal of the statement gives `column`. Whether it is in
// the column's range is checked where the store encodes it.
[[nodiscard]] ColumnValue LiteralValue(const Column& column, const cql::Literal& literal) {
    const ColumnKind kind = column.type.kind;
    const auto mismatch = [&column](std::string_view what) {
        return std::invalid_argument(Describe(column) + " does not take " + std::string(what));
    };
    return std::visit(
        Overloaded{
            [](const cql::Null&) -> ColumnValue { return std::monostate{}; },
            [&](const cql::Integer& value) -> ColumnValue {
                switch (kind) {
                    case ColumnKind::kUnsigned:
                        if (value.negative) {
                            throw std::invalid_argument(Describe(column) + " does not hold a negative value");
                        }
                        return value.magnitude;
                    case ColumnKind::kSigned: {
                        const std::uint64_t limit =
                            static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) + (value.negative ? 1U : 0U);
                        if (value.magnitude > limit) {
                            throw std::invalid_argument(Describe(column) + " does not hold this value");
                        }
                        return value.negative ? static_cast<std::int64_t>(0U - value.magnitude)
                                              : static_cast<std::int64_t>(value.magnitude);
                    }
                    case ColumnKind::kFloat32:
                    case ColumnKind::kFloat64: {
                        const double number = value.negative ? -static_cast<double>(value.magnitude)
                                                             : static_cast<double>(value.magnitude);
                        if (kind == ColumnKind::kFloat64) {
                            return number;
                        }
                        return static_cast<float>(number);
                    }
                    default:
                        throw mismatch("an integer");
                }
            },
            [&](double value) -> ColumnValue {
                if (kind == ColumnKind::kFloat64) {
                    return value;
                }
                if (kind == ColumnKind::kFloat32) {
                    if (std::isfinite(value) && std::abs(value) > static_cast<double>(std::numeric_limits<float>::max())) {
                        throw std::invalid_argument(Describe(column) + " does not hold this value");
                    }
                    return static_cast<float>(value);
                }
                throw mismatch("a number with a fraction");
            },
            [&](bool value) -> ColumnValue {
                if (kind != ColumnKind::kBool) {
                    throw mismatch("TRUE or FALSE");
                }
                return value;
            },
            [&](const cql::Text& value) -> ColumnValue {
                if (kind != ColumnKind::kText) {
                    throw mismatch("text");
                }
                return value.value;
            },
            [&](const cql::Bytes& value) -> ColumnValue {
                if (kind != ColumnKind::kBytes) {
                    throw mismatch("bytes");
                }
                return BytesValue{.bytes = value.value};
            },
            [&](const cql::Bits& value) -> ColumnValue {
                if (kind != ColumnKind::kBits) {
                    throw mismatch("bits");
                }
                return BitsValue{.digits = value.digits};
            },
            [](const cql::Parameter&) -> ColumnValue {
                throw std::logic_error("a parameter is not a literal");
            },
        },
        literal);
}

void AppendValue(std::string& out, const Column&, const ColumnValue& value) {
    Protocol::AppendValue(out, value);
}

std::size_t ValueReplyBytes(const ColumnValue& value) {
    std::optional<std::size_t> bulk;
    if (const auto* text = std::get_if<std::string>(&value)) bulk = text->size();
    else if (const auto* bytes = std::get_if<BytesValue>(&value)) bulk = bytes->bytes.size();
    else if (const auto* bits = std::get_if<BitsValue>(&value)) bulk = (bits->digits.size() + 7U) / 8U;
    if (bulk) {
        if (*bulk > kMaxChunkRangeResponseBytes) return kMaxChunkRangeResponseBytes + 1U;
        return *bulk + std::to_string(*bulk).size() + 5U;
    }
    std::string scalar;
    Protocol::AppendValue(scalar, value);
    return scalar.size();
}

[[nodiscard]] std::size_t RequireColumn(const ChunkLayout& layout, std::string_view name) {
    const std::size_t index = layout.FindColumn(name);
    if (index == std::string_view::npos) {
        throw std::invalid_argument("the table has no column " + std::string(name));
    }
    return index;
}

void RequireDimensions(const Geometry& geometry, std::uint8_t dims) {
    if (geometry.dims() != dims)
        throw std::invalid_argument("coordinate dimensions differ from the table: the table has " +
                                    std::to_string(geometry.dims()) + " and the statement " + std::to_string(dims) +
                                    "; give one coordinate per table dimension (DESCRIBE shows dims)");
}

ChunkCoord BlockCoord(const Geometry& geometry, std::int64_t x, std::int64_t y, std::int64_t z) {
    return geometry.dims() == 3U ? geometry.BlockToChunk(x, y, z) : geometry.BlockToChunk(x, y);
}

void AppendChunkState(ReplyParts& reply, const Geometry& geometry, const ChunkState& state,
    std::span<const std::size_t> columns, bool private_state) {
    reply.AppendBulk(EncodeSectionedChunkState(geometry, state, kVarMaxChunkBytesLimit, columns, private_state));
}

// A large reply becomes a continuation when the caller writes one.
[[nodiscard]] std::string FinishReply(ReplyParts reply, std::unique_ptr<ReplyContinuation>* continuation) {
    if (continuation == nullptr) return std::move(reply).Join();
    *continuation = std::move(reply).Continuation();
    return std::string{};
}

std::string AreaReplyPrefix(bool with_schema, std::uint64_t schema, bool paged, std::size_t count) {
    std::string prefix;
    if (with_schema) {
        Protocol::AppendArrayHeader(prefix, 2U);
        Protocol::AppendInteger(prefix, schema);
    }
    if (paged) {
        Protocol::AppendMapHeader(prefix, 3U);
        Protocol::AppendBulk(prefix, "chunks");
    }
    Protocol::AppendArrayHeader(prefix, count);
    return prefix;
}

// The cells `selector` requests, at most `bound`, which is positive.
[[nodiscard]] std::size_t AreaCellBound(const ChunkAreaSelector& selector, std::size_t bound) {
    const auto extent = [bound](std::int64_t first, std::int64_t last) {
        if (last < first) return bound;
        const auto delta = static_cast<std::uint64_t>(last) - static_cast<std::uint64_t>(first);
        return delta >= bound ? bound : static_cast<std::size_t>(delta + 1U);
    };
    // Each axis multiplies the count, which never exceeds the bound.
    const auto times = [bound](std::size_t count, std::size_t axis) {
        return axis > bound / count ? bound : count * axis;
    };
    auto count = bound;
    if (!selector.around) {
        count = times(extent(selector.first.x, selector.last.x), extent(selector.first.y, selector.last.y));
        if (selector.dims == 3U) count = times(count, extent(selector.first.z, selector.last.z));
    } else if (selector.radius >= 0 && static_cast<std::uint64_t>(selector.radius) < bound / 2U) {
        const auto diameter = static_cast<std::size_t>(selector.radius) * 2U + 1U;
        count = times(diameter, diameter);
        if (selector.dims == 3U) count = times(count, diameter);
    }
    return count;
}

std::string AreaReplySuffix(bool paged, bool more, const ChunkCoord& coord, std::uint8_t dims) {
    std::string suffix;
    if (!paged) return suffix;
    Protocol::AppendBulk(suffix, "more");
    Protocol::AppendBoolean(suffix, more);
    Protocol::AppendBulk(suffix, "after");
    if (!more) Protocol::AppendNull(suffix);
    else {
        Protocol::AppendArrayHeader(suffix, dims);
        Protocol::AppendInteger(suffix, coord.x);
        Protocol::AppendInteger(suffix, coord.y);
        if (dims == 3U) Protocol::AppendInteger(suffix, coord.z);
    }
    return suffix;
}

// Bound admitted wire storage independently of a lowered write quota; the
// locked mutation checks whether the resulting stored state may grow.
[[nodiscard]] std::size_t ChunkFormBytes(const Geometry& geometry, bool projected) {
    std::size_t metadata = 4U + 16U * geometry.chunk_schema().columns.size();
    for (const auto& column : geometry.chunk_schema().columns)
        if (IsFixedWidth(column.type.kind)) metadata += (FixedWidthBits(column.type) + 7U) / 8U;
    return 36U + 6U * 12U + 4U + 24U * geometry.layout().schema().columns.size() +
        ChunkPresenceBitmapBytes(geometry) + geometry.ChunkPayloadBytes() + metadata +
        kVarMaxChunkBytesLimit * (projected ? 2U : 1U);
}

// The most one encoded chunk form of the table takes in a reply, with the
// variable byte bound only where a block or chunk column is variable.
[[nodiscard]] std::size_t ChunkReplyFormBytes(const Geometry& geometry) {
    const auto& chunk_columns = geometry.chunk_schema().columns;
    const bool variable = geometry.layout().has_var_columns() ||
        std::any_of(chunk_columns.begin(), chunk_columns.end(),
            [](const Column& column) { return !IsFixedWidth(column.type.kind); });
    return ChunkFormBytes(geometry, false) - (variable ? 0U : kVarMaxChunkBytesLimit);
}

// Retained state of `count` chunk forms, each with `framing` bytes, within
// the response byte limit `budget`.
[[nodiscard]] std::size_t ChunkReplyStateBytes(const Geometry& geometry, std::size_t count, std::size_t framing,
    std::size_t budget = kMaxChunkRangeResponseBytes) {
    const auto chunk_bytes = std::min<std::size_t>(budget, framing + ChunkReplyFormBytes(geometry));
    return count > budget / chunk_bytes ? budget : count * chunk_bytes;
}

// Schema indexes of `names`, or empty for every column.
[[nodiscard]] std::vector<std::size_t> ColumnsOf(const ChunkLayout& layout, const std::vector<std::string>& names);

// The statement without the line's CR LF or LF.
[[nodiscard]] std::string_view StatementOf(std::string_view line) noexcept {
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
        line.remove_suffix(1);
    }
    return line;
}

std::vector<std::size_t> ColumnsOf(const ChunkLayout& layout, const std::vector<std::string>& names) {
    std::vector<std::size_t> columns;
    columns.reserve(names.size());
    for (const auto& name : names) {
        columns.push_back(RequireColumn(layout, name));
    }
    return columns;
}

template<class Statement>
FeedSlotFilters SlotFilters(const std::shared_ptr<Table>& table, const Statement& statement) {
    std::optional<FeedArea> area;
    if constexpr (requires { statement.areas; }) {
        for (const auto& each : statement.areas) RequireDimensions(table->geometry(), each.dims);
    } else area = statement.area;
    if (area) RequireDimensions(table->geometry(), area->dims);
    FeedSlotFilters filters{.area = area, .no_before = statement.no_before, .chunks = statement.chunks};
    if (statement.columns) filters.column_ids = table->ResolveFeedColumns(*statement.columns);
    return filters;
}

// OWNER, else the user running CREATE SLOT; none without authentication.
std::optional<std::string> SlotOwner(const cql::CreateSlot& create, bool require_auth, const std::string& user) {
    if (create.owner) return create.owner;
    return require_auth ? std::optional<std::string>(user) : std::nullopt;
}

// Gives `column`, of a valid type, the DEFAULT `literal`; NULL leaves a NULL
// column without one.
void SetDefault(Column& column, const cql::Literal& literal) {
    const ColumnValue value = LiteralValue(column, literal);
    if (std::holds_alternative<std::monostate>(value)) {
        // A NULL column without a DEFAULT takes NULL.
        if (!column.nullable) {
            throw std::invalid_argument(Describe(column) + " cannot be NULL, so its DEFAULT cannot be NULL");
        }
        column.has_default = false;
        column.default_value.clear();
        return;
    }
    column.has_default = true;
    column.default_value =
        IsFixedWidth(column.type.kind) ? EncodeColumnValue(column, value) : EncodeVarValue(column, value);
}

// A column of CREATE TABLE or ADD COLUMN; `id` is replaced by AddColumn.
[[nodiscard]] Column ColumnFrom(const cql::ColumnDefinition& definition, std::uint32_t id) {
    Column column;
    column.id = id;
    column.name = definition.name;
    column.type = definition.type;
    column.nullable = definition.nullable;
    column.required = definition.required;
    if (!definition.default_value.has_value()) {
        return column;
    }
    // Before the default is read and encoded for the type.
    ValidateColumnType(column);
    SetDefault(column, *definition.default_value);
    return column;
}

// The check `definition` makes in `schema`; an unnamed one takes a name
// from `column` (a column's own CHECK) or from the first column it reads.
[[nodiscard]] CheckConstraint CheckFrom(
    const cql::CheckDefinition& definition, const TableSchema& schema, std::string_view column = {}) {
    CheckConstraint check{.name = definition.name.value_or(std::string{}), .expression = definition.expression};
    if (!definition.name.has_value()) check.name = NewCheckName(schema, definition.expression, column);
    else if (HasCheck(schema, check.name)) throw std::invalid_argument("the table already has a CHECK named " + check.name);
    return check;
}

// The NULL, REQUIRED and DEFAULT column `attribute.column` of `schema` takes
// after ALTER COLUMN (chunkdb::ChangeColumnAttributes checks the result).
[[nodiscard]] Column AttributesFrom(const TableSchema& schema, const cql::AlterColumnAttribute& attribute) {
    using Change = cql::AlterColumnAttribute::Change;
    const auto at = std::find_if(schema.columns.begin(), schema.columns.end(),
        [&](const Column& column) { return column.name == attribute.column; });
    if (at == schema.columns.end()) throw std::invalid_argument("the table has no column " + attribute.column);
    Column column = *at;
    switch (attribute.change) {
        case Change::kSetDefault: SetDefault(column, *attribute.value); break;
        case Change::kDropDefault:
            if (!column.has_default) throw std::invalid_argument(Describe(column) + " has no DEFAULT");
            column.has_default = false;
            column.default_value.clear();
            break;
        case Change::kSetNull: column.nullable = true; break;
        case Change::kDropNull: column.nullable = false; break;
        case Change::kSetRequired: column.required = true; break;
        case Change::kDropRequired: column.required = false; break;
    }
    return column;
}

// WITH and SET options, by the names TABLEINFO prints; each at most once.
[[nodiscard]] TableOptionsUpdate OptionsFrom(const std::vector<cql::Option>& options) {
    TableOptionsUpdate update;
    std::vector<std::string_view> seen;
    for (const auto& option : options) {
        if (std::find(seen.begin(), seen.end(), option.name) != seen.end()) {
            throw std::invalid_argument("option " + option.name + " is given twice");
        }
        seen.push_back(option.name);
        std::string text;
        if (const auto* integer = std::get_if<cql::Integer>(&option.value); integer != nullptr && !integer->negative) {
            text = std::to_string(integer->magnitude);
        } else if (const auto* word = std::get_if<cql::Text>(&option.value); word != nullptr) {
            text = word->value;
        } else {
            throw std::invalid_argument("option " + option.name + " takes a number or a quoted value");
        }
        ApplyTableOption(&update, option.name, text);
    }
    return update;
}

struct FileUsage {
    std::uint64_t files = 0U;
    std::uint64_t bytes = 0U;
};

// SHOW TABLE STATS: the regular files under table `table`'s directory and
// their sizes, listed without a lease, so that no statement on the table
// waits for it. A file or directory removed during the walk is not counted,
// and a table directory removed before it is a dropped table; any other
// error fails the statement. It stops when the statement is stopped.
[[nodiscard]] FileUsage DirectoryUsage(const std::filesystem::path& root, const std::string& table) {
    namespace fs = std::filesystem;
    const auto gone = [](const std::error_code& error) { return error == std::errc::no_such_file_or_directory; };
    FileUsage usage;
    std::vector<fs::path> pending{root};
    while (!pending.empty()) {
        const fs::path dir = std::move(pending.back());
        pending.pop_back();
        std::error_code error;
        fs::directory_iterator it(dir, error);
        if (error && dir == root && gone(error)) throw TableNotFoundError("table '" + table + "' does not exist");
        if (error && !gone(error)) throw fs::filesystem_error("cannot list table files", dir, error);
        for (const fs::directory_iterator end{}; !error && it != end; it.increment(error)) {
            CheckStatementCancellation();
            const auto status = it->symlink_status(error);
            if (!error && fs::is_directory(status)) pending.push_back(it->path());
            if (!error && fs::is_regular_file(status)) {
                const auto size = fs::file_size(it->path(), error);
                if (!error) {
                    ++usage.files;
                    usage.bytes += size;
                }
            }
            if (error && !gone(error)) throw fs::filesystem_error("cannot read table files", it->path(), error);
            error.clear();
        }
        if (error && !gone(error)) throw fs::filesystem_error("cannot list table files", dir, error);
    }
    return usage;
}

// The value a column's encoded DEFAULT holds.
// DESCRIBE: a map of the table's name, schema version, columns, chunk and
// large-chunk sizes, options, epoch and completed revision, and why it is
// fenced when it is. A fenced table has no `store` and a null revision.
[[nodiscard]] std::string DescribeReply(const TableInfo& info, const ChunkStore* store, const std::string* fenced) {
    std::string reply;
    const auto key = [&reply](std::string_view name) { Protocol::AppendBulk(reply, name); };
    const auto sides = [&reply, &info](std::uint64_t first, std::uint64_t second, std::uint64_t third) {
        Protocol::AppendArrayHeader(reply, info.geometry.dims);
        Protocol::AppendInteger(reply, first);
        Protocol::AppendInteger(reply, second);
        if (info.geometry.dims == 3U) Protocol::AppendInteger(reply, third);
    };
    const auto& checks = info.schema.checks;
    Protocol::AppendMapHeader(reply, 10U + (fenced != nullptr ? 1U : 0U) + (checks.empty() ? 0U : 1U));
    key("table");
    Protocol::AppendBulk(reply, info.name);
    key("version");
    Protocol::AppendInteger(reply, info.schema.version);
    key("columns");
    Protocol::AppendColumns(reply, info.schema.columns);
    key("chunk");
    sides(info.geometry.chunk_width_blocks, info.geometry.chunk_height_blocks, info.geometry.chunk_depth_blocks);
    key("large");
    sides(info.geometry.large_chunk_width_chunks, info.geometry.large_chunk_height_chunks,
        info.geometry.large_chunk_depth_chunks);
    key("options");
    Protocol::AppendMapHeader(reply, 8);
    key("durability_mode");
    Protocol::AppendBulk(reply, DurabilityModeName(info.options.durability_mode));
    key("checkpoint_updates");
    Protocol::AppendInteger(reply, static_cast<std::uint64_t>(info.options.checkpoint_update_interval));
    key("checkpoint_wal_bytes");
    Protocol::AppendInteger(reply, static_cast<std::uint64_t>(info.options.checkpoint_wal_bytes));
    key("wal_group_commit_updates");
    Protocol::AppendInteger(reply, static_cast<std::uint64_t>(info.options.wal_group_commit_updates));
    key("checkpoint_compression");
    Protocol::AppendBulk(reply, CheckpointCompressionName(info.options.checkpoint_compression));
    key("var_max_chunk_bytes");
    Protocol::AppendInteger(reply, static_cast<std::uint64_t>(info.options.var_max_chunk_bytes));
    key("feed_buffer_bytes");
    Protocol::AppendInteger(reply, static_cast<std::uint64_t>(info.feed_buffer_bytes));
    key("slot_max_bytes");
    Protocol::AppendInteger(reply, static_cast<std::uint64_t>(info.slot_max_bytes));
    key("epoch");
    Protocol::AppendBulk(reply, StoreIdHex(store != nullptr ? store->store_id() : info.store_id));
    key("revision");
    if (const auto revision = store != nullptr ? store->CompletedRevision() : std::nullopt)
        Protocol::AppendInteger(reply, *revision);
    else Protocol::AppendNull(reply);
    key("dims");
    Protocol::AppendInteger(reply, static_cast<std::uint64_t>(info.geometry.dims));
    key("chunk_columns");
    Protocol::AppendColumns(reply, info.chunk_schema.columns);
    if (!checks.empty()) {
        key("checks");
        Protocol::AppendArrayHeader(reply, checks.size());
        for (const auto& check : checks) {
            Protocol::AppendMapHeader(reply, 2);
            key("name");
            Protocol::AppendBulk(reply, check.name);
            key("expression");
            Protocol::AppendBulk(reply, check.expression);
        }
    }
    if (fenced != nullptr) {
        key("fenced");
        Protocol::AppendBulk(reply, *fenced);
    }
    return reply;
}

[[nodiscard]] const char* RightName(Right right) noexcept {
    switch (right) {
        case Right::kRead:
            return "READ";
        case Right::kWrite:
            return "WRITE";
        case Right::kAdmin:
            return "ADMIN";
    }
    return "?";
}

// The most bytes of a verifier, sent as a parameter or a text literal.
constexpr std::size_t kMaxVerifierBytes = 512;

// The verifier of CREATE USER or ALTER USER: a parameter frame or a text
// literal, in the SCRAM-SHA-256$... form.
[[nodiscard]] scram::Verifier VerifierFrom(
    const cql::Literal& value,
    std::span<const std::optional<std::string>> parameters) {
    if (const auto* parameter = std::get_if<cql::Parameter>(&value); parameter != nullptr) {
        const auto& frame = parameters[parameter->index - 1U];
        if (!frame.has_value()) {
            throw std::invalid_argument("the verifier cannot be NULL");
        }
        return scram::ParseVerifier(*frame);
    }
    const auto& text = std::get<cql::Text>(value).value;
    if (text.size() > kMaxVerifierBytes) {
        throw std::invalid_argument("a verifier has at most " + std::to_string(kMaxVerifierBytes) + " bytes");
    }
    return scram::ParseVerifier(text);
}

[[nodiscard]] std::string VersionReply(const ChunkMutationResult& result) {
    if (!result.ok) {
        return Protocol::Error("VERSION_MISMATCH", "", {{"current", std::to_string(result.version)}});
    }
    std::string reply;
    Protocol::AppendInteger(reply, result.version);
    return reply;
}

std::string NullReply() {
    std::string reply;
    Protocol::AppendNull(reply);
    return reply;
}

// IF VERSION is refused inside a transaction: COMMIT checks every chunk the
// transaction touched.
void RequireNoVersionInTxn(const SessionState& session, const std::optional<std::uint64_t>& if_version) {
    if (session.transaction != nullptr && if_version.has_value()) {
        throw std::invalid_argument(
            "IF VERSION is not used inside a transaction: COMMIT checks every chunk the transaction touched");
    }
}

// The expressions of one SET BLOCK or DELETE BLOCK, planned against the
// table's columns as they are.
struct BlockWritePlan {
    std::optional<expr::ConditionPlan> condition;
    std::optional<expr::AssignmentsPlan> assignments;
    // The block columns they read (schema indexes, ascending).
    std::vector<std::size_t> block_slots;
    bool reads_now = false;
    bool reads_chunk_columns = false;
    // Whether the statement depends on the chunk as it is: a condition, or
    // a value that reads a block or chunk column.
    bool reads_chunk = false;
};

[[nodiscard]] BlockWritePlan PlanBlockWrite(const expr::WriteExpressions& expressions, const Geometry& geometry) {
    const expr::ColumnSpans schema(geometry.layout().schema().columns, geometry.chunk_schema().columns);
    BlockWritePlan plan;
    if (expressions.condition) plan.condition = expr::PlanCondition(*expressions.condition, schema);
    if (!expressions.assignments.empty()) plan.assignments = expr::PlanAssignments(expressions.assignments, schema);
    const auto reads = [&plan](expr::Op op) {
        return (plan.condition && plan.condition->Reads(op)) || (plan.assignments && plan.assignments->Reads(op));
    };
    plan.reads_now = reads(expr::Op::kNow);
    plan.reads_chunk_columns = reads(expr::Op::kChunkColumn);
    if (plan.condition) plan.block_slots = plan.condition->BlockSlots();
    if (plan.assignments) {
        const auto slots = plan.assignments->BlockSlots();
        plan.block_slots.insert(plan.block_slots.end(), slots.begin(), slots.end());
        std::sort(plan.block_slots.begin(), plan.block_slots.end());
        plan.block_slots.erase(std::unique(plan.block_slots.begin(), plan.block_slots.end()), plan.block_slots.end());
    }
    plan.reads_chunk = plan.condition || !plan.block_slots.empty() || plan.reads_chunk_columns;
    return plan;
}

// Calls `use` with each parameter the expressions take and its type.
template <class Use>
void ForEachExpressionParameter(const BlockWritePlan& plan, Use&& use) {
    if (plan.condition)
        for (const auto& parameter : plan.condition->parameters()) use(parameter);
    if (plan.assignments)
        for (const auto& parameter : plan.assignments->parameters()) use(parameter);
}

// The expressions' parameters, $n at index n - 1, each decoded as the type
// it takes.
[[nodiscard]] std::vector<ColumnValue> ExpressionParameters(
    const BlockWritePlan& plan, std::span<const std::optional<std::string>> frames) {
    std::vector<ColumnValue> values(frames.size());
    ForEachExpressionParameter(plan, [&](const expr::ParameterType& parameter) {
        Column column;
        column.type = parameter.type;
        values[parameter.index - 1U] = ParameterValue(column, parameter.index, frames[parameter.index - 1U]);
    });
    return values;
}

[[nodiscard]] bool SessionControl(const cql::Statement& statement) {
    return std::holds_alternative<cql::ShowConnections>(statement) ||
           std::holds_alternative<cql::Cancel>(statement) || std::holds_alternative<cql::KillConnection>(statement);
}

[[nodiscard]] bool AllowedInTransaction(const cql::Statement& statement) {
    if (SessionControl(statement)) return true;
    return std::holds_alternative<cql::GetBlock>(statement) || std::holds_alternative<cql::SetBlock>(statement) ||
           std::holds_alternative<cql::DeleteBlock>(statement) || std::holds_alternative<cql::GetChunk>(statement) ||
           std::holds_alternative<cql::SetChunk>(statement) || std::holds_alternative<cql::GetArea>(statement) ||
           std::holds_alternative<cql::Aggregate>(statement) ||
           std::holds_alternative<cql::Describe>(statement) || std::holds_alternative<cql::Ping>(statement) ||
           std::holds_alternative<cql::Begin>(statement) ||
           std::holds_alternative<cql::Commit>(statement) || std::holds_alternative<cql::Rollback>(statement);
}

// What a server that opened its data directory read-only runs: reads and
// listings. Every other statement would change data, schema, users, rights or
// slots, or needs a writer (WATCH, BEGIN, FLUSH WAL, BACKUP).
[[nodiscard]] bool AvailableReadOnly(const cql::Statement& statement) {
    if (SessionControl(statement)) return true;
    return std::holds_alternative<cql::GetBlock>(statement) || std::holds_alternative<cql::GetChunk>(statement) ||
           std::holds_alternative<cql::GetArea>(statement) || std::holds_alternative<cql::ScanChunks>(statement) ||
           std::holds_alternative<cql::Aggregate>(statement) ||
           std::holds_alternative<cql::ShowTableStats>(statement) ||
           std::holds_alternative<cql::ShowTables>(statement) || std::holds_alternative<cql::Describe>(statement) ||
           std::holds_alternative<cql::ShowUsers>(statement) || std::holds_alternative<cql::ShowSlots>(statement) ||
           std::holds_alternative<cql::ShowMigrations>(statement) || std::holds_alternative<cql::ShowMetrics>(statement) ||
           std::holds_alternative<cql::ShowStatus>(statement) ||
           std::holds_alternative<cql::ReloadTls>(statement) || std::holds_alternative<cql::Ping>(statement) ||
           std::holds_alternative<cql::Commit>(statement) || std::holds_alternative<cql::Rollback>(statement) ||
           std::holds_alternative<cql::Unwatch>(statement) || std::holds_alternative<cql::Ack>(statement);
}

[[nodiscard]] std::size_t StateBytes(const ChunkState& state) {
    return state.payload.size() + state.presence_bitmap.size() + state.vars.encoded_size() +
        (state.chunk_columns ? state.chunk_columns->bytes.size() : 0U);
}

// The fewest StateBytes of the state `form` decodes to: the chunk's payload
// and presence, and for a full form its values and metadata row as well. A
// projected form's values merge with the chunk's, so they are not counted.
[[nodiscard]] std::size_t DecodedStateBytesAtLeast(
    const Geometry& geometry, const ChunkFormSections& form, bool projected) {
    std::size_t bytes = geometry.ChunkPayloadBytes() + ChunkPresenceBitmapBytes(geometry);
    if (projected) return bytes;
    for (const auto& section : form.sections) {
        if (section.kind == static_cast<std::uint16_t>(ChunkFormSectionKind::kVariableValues) ||
            (section.kind == static_cast<std::uint16_t>(ChunkFormSectionKind::kChunkColumns) &&
                !geometry.chunk_schema().columns.empty()))
            bytes += section.body.size();
    }
    return bytes;
}

}  // namespace

const User* CommandEngine::CurrentUser(SessionState& session) const {
    if (!config_.require_auth) {
        return nullptr;
    }
    // One atomic read while nothing changed; a changed or dropped user is
    // read again. Another incarnation is another user: one dropped and
    // created again under the same name gives this session nothing.
    const std::uint64_t generation = config_.users->Generation();
    if (session.user_generation != generation) {
        // The ticket names the incarnation this session logged in as.
        auto found = config_.users->FindForConnection(session.user, session.user_ticket);
        session.user_rights = found.has_value() ? std::make_shared<const User>(std::move(*found)) : nullptr;
        session.user_generation = generation;
    }
    return session.user_rights.get();
}

std::optional<Right> CommandEngine::RightOnTable(SessionState& session, const std::string& table) const {
    if (!config_.require_auth) {
        return Right::kAdmin;
    }
    const User* user = CurrentUser(session);
    if (user == nullptr) {
        return std::nullopt;
    }
    return RightOn(*user, table);
}

namespace {

// Fills what the logs say about `statement`: its verb, its audit event (for
// logins, schema, users, rights, backups, migrations and slots), and the
// names it acts on. Values, verifiers and parameters are never copied.
void DescribeStatement(const cql::Statement& statement, StatementLogInfo& info) {
    const auto table_change = [&info](const auto& change, const char* verb, const char* event) {
        info.verb = verb;
        info.audit_event = event;
        info.table = change.table;
    };
    const auto grant = [&info](const cql::GrantRight& grant) {
        info.verb = grant.revoke ? "REVOKE" : "GRANT";
        info.audit_event = grant.revoke ? "revoke" : "grant";
        info.table = grant.table;
        info.target_user = grant.user;
    };
    const auto slot = [&info](const auto& change, const char* verb, const char* event) {
        info.verb = verb;
        info.audit_event = event;
        info.table = change.table;
        info.slot = change.name;
    };
    std::visit(
        Overloaded{
            [&](const cql::GetBlock& get) { info.verb = "GET BLOCK"; info.table = get.table; info.chunks = 1U; },
            [&](const cql::SetBlock& set) { info.verb = "SET BLOCK"; info.table = set.table; info.chunks = 1U; },
            [&](const cql::DeleteBlock& del) { info.verb = "DELETE BLOCK"; info.table = del.table; info.chunks = 1U; },
            [&](const cql::GetChunk& get) { info.verb = "GET CHUNK"; info.table = get.table; info.chunks = 1U; },
            [&](const cql::SetChunk& set) { info.verb = "SET CHUNK"; info.table = set.table; info.chunks = 1U; },
            [&](const cql::GetArea& get) { info.verb = "GET AREA"; info.table = get.table; },
            [&](const cql::Aggregate& aggregate) { info.verb = "AGGREGATE"; info.table = aggregate.table; },
            [&](const cql::ScanChunks& scan) { info.verb = "SCAN CHUNKS"; info.table = scan.table; },
            [&](const cql::CreateTable& create) { table_change(create, "CREATE TABLE", "create_table"); },
            [&](const cql::AlterTable& alter) {
                table_change(alter, "ALTER TABLE", "alter_table");
                if (const auto* rename = std::get_if<cql::RenameTable>(&alter.change)) info.table += "," + rename->new_name;
            },
            [&](const cql::DropTable& drop) { table_change(drop, "DROP TABLE", "drop_table"); },
            [&](const cql::RecoverTable& recover) { table_change(recover, "RECOVER TABLE", "recover_table"); },
            [&](const cql::CompactTombstones& compact) { table_change(compact, "COMPACT TOMBSTONES", "compact_tombstones"); },
            [&](const cql::ShowTableStats& stats) { info.verb = "SHOW TABLE STATS"; info.table = stats.table; },
            [&](const cql::ShowTables&) { info.verb = "SHOW TABLES"; },
            [&](const cql::Describe& describe) { info.verb = "DESCRIBE"; info.table = describe.table; },
            [&](const cql::FlushWal&) { info.verb = "FLUSH WAL"; },
            [&](const cql::Backup&) { info.verb = "BACKUP"; info.audit_event = "backup"; },
            [&](const cql::RestoreTable& restore) {
                info.verb = "RESTORE TABLE";
                info.audit_event = "restore_table";
                info.table = restore.name;
            },
            [&](const cql::SwapTables& swap) {
                info.verb = "SWAP TABLES";
                info.audit_event = "swap_tables";
                info.table = swap.first + "," + swap.second;
            },
            [&](const cql::ShowMetrics&) { info.verb = "SHOW METRICS"; },
            [&](const cql::ReloadTls&) { info.verb = "RELOAD TLS"; info.audit_event = "reload_tls"; },
            [&](const cql::ShowStatus&) { info.verb = "SHOW STATUS"; },
            [&](const cql::ShowConnections&) { info.verb = "SHOW CONNECTIONS"; },
            [&](const cql::Cancel& cancel) {
                info.verb = "CANCEL"; info.audit_event = "cancel"; info.target_connection = cancel.id;
            },
            [&](const cql::KillConnection& kill) {
                info.verb = "KILL CONNECTION"; info.audit_event = "kill_connection"; info.target_connection = kill.id;
            },
            [&](const cql::EnableFeature&) { info.verb = "ENABLE FEATURE"; info.audit_event = "enable_feature"; },
            [&](const cql::Ping&) { info.verb = "PING"; },
            [&](const cql::CreateUser& create) {
                info.verb = "CREATE USER";
                info.audit_event = "create_user";
                info.target_user = create.user;
            },
            [&](const cql::AlterUser& alter) {
                info.verb = "ALTER USER";
                info.audit_event = "alter_user";
                info.target_user = alter.user;
                // The same order as the statement runs its forms.
                info.change = alter.verifier ? "verifier"
                    : alter.added_verifier ? "add_verifier"
                    : alter.drop_old_verifier ? "drop_old_verifier"
                    : alter.max_connections || alter.statements_per_sec ? "limit"
                    : alter.requires_channel_binding ? "requires_channel_binding"
                    : "manages_users";
            },
            [&](const cql::DropUser& drop) {
                info.verb = "DROP USER";
                info.audit_event = "drop_user";
                info.target_user = drop.user;
            },
            [&](const cql::GrantRight& right) { grant(right); },
            [&](const cql::ShowUsers&) { info.verb = "SHOW USERS"; },
            [&](const cql::Begin&) { info.verb = "BEGIN"; },
            [&](const cql::Commit&) { info.verb = "COMMIT"; },
            [&](const cql::Rollback&) { info.verb = "ROLLBACK"; },
            [&](const cql::Watch& watch) { info.verb = "WATCH"; info.table = watch.table; },
            [&](const cql::Unwatch&) { info.verb = "UNWATCH"; },
            [&](const cql::CreateSlot& create) { slot(create, "CREATE SLOT", "create_slot"); },
            [&](const cql::DropSlot& drop) { slot(drop, "DROP SLOT", "drop_slot"); },
            [&](const cql::ShowSlots& show) { info.verb = "SHOW SLOTS"; if (show.table) info.table = *show.table; },
            [&](const cql::Ack&) { info.verb = "ACK"; },
            [&](const cql::Migrate& migrate) {
                std::visit([&](const auto& inner) { DescribeStatement(cql::Statement(inner), info); }, migrate.statement);
                info.verb = "MIGRATE";
                info.audit_event = "migrate";
                info.migration = migrate.name;
            },
            [&](const cql::ShowMigrations&) { info.verb = "SHOW MIGRATIONS"; },
        },
        statement);
}

TableDefinition DefinitionFrom(const cql::CreateTable& create, const TableOptions& defaults) {
    if (create.columns.size() > kMaxColumnsPerTable) {
        throw std::invalid_argument("a table has at most " + std::to_string(kMaxColumnsPerTable) + " columns");
    }
    TableSchema schema{
        .version = 1,
        .next_column_id = static_cast<std::uint32_t>(create.columns.size() + 1U),
        .columns = {}, .history = {}, .pending = std::nullopt,
    };
    for (std::size_t i = 0; i < create.columns.size(); ++i)
        schema.columns.push_back(ColumnFrom(create.columns[i], static_cast<std::uint32_t>(i + 1U)));
    // Column CHECKs in column order, then the table's; ValidateTableSchema
    // checks each against the columns.
    for (const auto& column : create.columns)
        for (const auto& check : column.checks) schema.checks.push_back(CheckFrom(check, schema, column.name));
    for (const auto& check : create.checks) schema.checks.push_back(CheckFrom(check, schema));
    if (schema.checks.size() > kMaxChecksPerTable)
        throw std::invalid_argument("a table has at most " + std::to_string(kMaxChecksPerTable) + " CHECK constraints");
    GeometryConfig geometry;
    geometry.chunk_width_blocks = create.chunk_width;
    geometry.chunk_height_blocks = create.chunk_height;
    geometry.chunk_depth_blocks = create.chunk_depth;
    geometry.dims = create.dims;
    if (create.large) {
        geometry.large_chunk_width_chunks = create.large->first;
        geometry.large_chunk_height_chunks = create.large->second;
        geometry.large_chunk_depth_chunks = create.large_depth;
    }
    geometry.block_bits = FixedBitsPerBlock(schema);
    TableSchema chunk_schema{.version = 1U, .next_column_id = static_cast<std::uint32_t>(create.chunk_columns.size() + 1U),
        .columns = {}, .history = {}, .pending = std::nullopt};
    for (std::size_t i = 0U; i < create.chunk_columns.size(); ++i)
        chunk_schema.columns.push_back(ColumnFrom(create.chunk_columns[i], static_cast<std::uint32_t>(i + 1U)));
    return {geometry, OptionsFrom(create.options).ApplyTo(defaults), std::move(schema), std::move(chunk_schema)};
}

bool HasColumn(const TableSchema& schema, std::string_view name) {
    return std::any_of(schema.columns.begin(), schema.columns.end(),
                       [name](const Column& column) { return column.name == name; });
}

// ADMIN on `*`, from that grant alone.
[[nodiscard]] bool HasAdminOnEveryTable(const User* user) {
    if (user == nullptr) return false;
    const auto found = user->grants.find(kEveryTable);
    return found != user->grants.end() && found->second >= Right::kAdmin;
}

// A recorded migration belongs to what its statement changes: ADMIN on the
// table it names, or MANAGES USERS for GRANT and REVOKE. Only a caller with
// that right sees the record or learns whether its name skips or conflicts.
// A recorded statement that no longer parses needs ADMIN on `*`.
bool MayReplayMigration(bool require_auth, const User* user, const MigrationRecord& record) {
    if (!require_auth) return true;
    if (user == nullptr) return false;
    cql::Parsed parsed;
    try {
        parsed = cql::Parse(record.statement);
    } catch (const cql::ParseError&) {
        const auto right = RightOn(*user, std::string(kEveryTable));
        return right.has_value() && *right >= Right::kAdmin;
    }
    if (std::holds_alternative<cql::GrantRight>(parsed.statement)) return user->manages_users;
    const auto table = std::visit([](const auto& inner) -> std::string {
        using T = std::decay_t<decltype(inner)>;
        if constexpr (std::is_same_v<T, cql::CreateTable> || std::is_same_v<T, cql::AlterTable> || std::is_same_v<T, cql::DropTable> ||
                      std::is_same_v<T, cql::CreateSlot> || std::is_same_v<T, cql::DropSlot>)
            return inner.table;
        return {};
    }, parsed.statement);
    const auto right = table.empty() ? std::nullopt : RightOn(*user, table);
    return right.has_value() && *right >= Right::kAdmin;
}

}  // namespace

void CommandEngine::DescribeLine(std::string_view line, StatementLogInfo& info) {
    cql::Parsed parsed;
    try {
        parsed = cql::Parse(StatementOf(line));
    } catch (const cql::ParseError&) {
        return;  // As in ExecuteStatement: a statement that does not parse is not logged.
    }
    DescribeStatement(parsed.statement, info);
}

void CommandEngine::RequireRight(SessionState& session, const std::string& table, Right needed) const {
    const auto right = RightOnTable(session, table);
    if (!right.has_value()) {
        // As for a table that does not exist: names do not leak.
        throw TableNotFoundError("table '" + table + "' does not exist");
    }
    if (*right < needed) {
        throw PermissionDeniedError(std::string(RightName(needed)) + " on " + table);
    }
}

void CommandEngine::RequireRightOnEveryTable(SessionState& session, Right needed) const {
    if (!config_.require_auth) {
        return;
    }
    const User* user = CurrentUser(session);
    const auto found = user != nullptr ? user->grants.find(kEveryTable) : decltype(user->grants.end()){};
    if (user == nullptr || found == user->grants.end() || found->second < needed) {
        throw PermissionDeniedError(std::string(RightName(needed)) + " on *");
    }
}

bool CommandEngine::HasServerRight(SessionState& session, ServerRight right) const {
    if (!config_.require_auth) return true;
    const User* user = CurrentUser(session);
    return user != nullptr && chunkdb::HasServerRight(*user, right);
}

void CommandEngine::RequireMonitor(SessionState& session) const {
    if (!HasServerRight(session, ServerRight::kMonitor) && !HasAdminOnEveryTable(CurrentUser(session))) {
        throw PermissionDeniedError("MONITOR or ADMIN on *");
    }
}

void CommandEngine::RequireCreateRight(SessionState& session, const std::string& table) const {
    // Checked before the name is looked up: the reply does not tell whether it exists.
    if (const auto right = RightOnTable(session, table); !right.has_value() || *right < Right::kAdmin) {
        throw PermissionDeniedError("ADMIN on " + table);
    }
}

void CommandEngine::RequireManagesUsers(SessionState& session) const {
    if (config_.users == nullptr) {
        throw std::invalid_argument("this server runs without users (--auth none)");
    }
    if (!config_.require_auth) {
        return;
    }
    const User* user = CurrentUser(session);
    if (user == nullptr || !user->manages_users) {
        throw PermissionDeniedError("MANAGES USERS");
    }
}

Table::Lease CommandEngine::AcquireNamedTable(SessionState& session, std::string_view name) const {
    const auto not_found = [name] {
        return TableNotFoundError("table '" + std::string(name) + "' does not exist");
    };
    if (session.table == nullptr || session.table->name() != name) {
        session.table = catalog_->Require(name);
    }
    if (auto lease = session.table->Acquire(); lease.has_value()) {
        return std::move(*lease);
    }
    // Dropped, fenced or recovered since the last statement; a table of that
    // name may exist again.
    session.table = catalog_->Require(name);
    auto lease = session.table->Acquire();
    if (!lease.has_value()) {
        throw not_found();
    }
    return std::move(*lease);
}

CommandEngine::PayloadRequest CommandEngine::PlanParameters(SessionState& session, std::string_view line) const {
    PayloadRequest request;
    request.frame_count = cql::CountParameterTokens(line);
    if (request.frame_count == 0U) {
        return request;
    }
    // Invalid statements still have a lexical frame count. The connection
    // drains them under its global bound without retaining their payloads.
    const auto reject = [&request](std::string response) {
        request.plan = PayloadPlan::kDiscard;
        request.reject_response = std::move(response);
        return request;
    };
    // A session without its user drains the frames and closes, as Execute
    // answers it.
    if (std::string lost = LostUserReply(session); !lost.empty()) return reject(std::move(lost));
    cql::Parsed parsed;
    try {
        parsed = cql::Parse(StatementOf(line));
    } catch (const cql::UnknownCommandError& e) {
        return reject(Protocol::Error("UNKNOWN_COMMAND", e.what()));
    } catch (const cql::ParseError& e) {
        return reject(Protocol::Error("SYNTAX", e.what()));
    }
    if (parsed.parameters == 0) {
        return request;
    }
    // The table as it is now: the one the session last used may have been
    // dropped and created again with other columns or chunk sizes.
    // A table the user has no right on reads as one that does not exist.
    const auto find = [this, &session](const std::string& name) -> std::shared_ptr<Table> {
        if (!RightOnTable(session, name).has_value()) {
            return nullptr;
        }
        return catalog_->Find(name);
    };
    // A fenced table reads as fenced, not absent.
    const auto missing = [this, &session, &reject](const std::string& name) {
        if (RightOnTable(session, name).has_value()) {
            try {
                (void)catalog_->Require(name);
            } catch (const TableFencedError& fenced) {
                return reject(Protocol::Error("INTERNAL", fenced.what()));
            } catch (const TableNotFoundError&) {
                // Absent, as answered below.
            }
        }
        return reject(Protocol::Error("NO_TABLE", "table '" + name + "' does not exist; use SHOW TABLES to list accessible tables"));
    };
    if (std::holds_alternative<cql::CreateUser>(parsed.statement) ||
        std::holds_alternative<cql::AlterUser>(parsed.statement)) {
        request.parameter_limits = {kMaxVerifierBytes};
        request.plan = PayloadPlan::kParameters;
        return request;
    }
    if (const auto* chunk = std::get_if<cql::SetChunk>(&parsed.statement); chunk != nullptr) {
        const std::shared_ptr<Table> table = find(chunk->table);
        if (table == nullptr) {
            return missing(chunk->table);
        }
        const auto info = table->Info();
        if (chunk->metadata) {
            request.parameter_limits.assign(parsed.parameters, 0U);
            for (const auto& assignment : *chunk->metadata) {
                const auto* parameter = std::get_if<cql::Parameter>(&assignment.value);
                if (parameter == nullptr) continue;
                const auto at = std::find_if(info.chunk_schema.columns.begin(), info.chunk_schema.columns.end(),
                    [&](const auto& column) { return column.name == assignment.column; });
                if (at == info.chunk_schema.columns.end())
                    return reject(Protocol::Error("INVALID_ARGUMENT", "unknown chunk column " + assignment.column));
                request.parameter_limits[parameter->index - 1U] = ParameterBytes(at->type);
            }
            request.parameter_bytes_limit = kMaxStatementParameterBytes;
        } else request.parameter_limits = {ChunkFormBytes(table->geometry(), !chunk->columns.empty())};
        request.plan = PayloadPlan::kParameters;
        return request;
    }
    // A read's WHERE: each parameter is at most the size of the type it
    // takes from the table.
    const auto where_read = [&]() -> std::optional<std::pair<std::string, std::shared_ptr<const expr::Expression>>> {
        if (const auto* aggregate = std::get_if<cql::Aggregate>(&parsed.statement)) return std::pair{aggregate->table, aggregate->where};
        if (const auto* scan = std::get_if<cql::ScanChunks>(&parsed.statement)) return std::pair{scan->table, scan->where};
        return std::nullopt;
    }();
    if (where_read && where_read->second) {
        const std::shared_ptr<Table> table = find(where_read->first);
        if (table == nullptr) return missing(where_read->first);
        const Geometry geometry = table->geometry();
        try {
            const BlockFilter filter(geometry, *where_read->second);
            request.parameter_limits.assign(parsed.parameters, 0U);
            for (const auto& parameter : filter.parameters())
                request.parameter_limits.at(parameter.index - 1U) = ParameterBytes(parameter.type);
        } catch (const std::invalid_argument& error) {
            return reject(ErrorReply(error));
        } catch (const std::out_of_range& error) {
            return reject(ErrorReply(error));
        }
        request.parameter_bytes_limit = kMaxStatementParameterBytes;
        request.plan = PayloadPlan::kParameters;
        return request;
    }
    const auto* set = std::get_if<cql::SetBlock>(&parsed.statement);
    const auto* del = std::get_if<cql::DeleteBlock>(&parsed.statement);
    if (set == nullptr && (del == nullptr || !del->expressions)) {
        return reject(Protocol::Error("UNKNOWN_COMMAND", "this statement is not available yet"));
    }
    const std::string& table_name = set != nullptr ? set->table : del->table;
    const std::shared_ptr<Table> table = find(table_name);
    if (table == nullptr) {
        return missing(table_name);
    }
    // The statement checks the values again under its lease, so a column
    // that changes in between is not trusted.
    const Geometry geometry = table->geometry();
    const auto& layout = geometry.layout();
    request.parameter_limits.assign(parsed.parameters, 0);
    // An expression's parameter takes the type the plan gives it.
    if (const auto& expressions = set != nullptr ? set->expressions : del->expressions) {
        try {
            ForEachExpressionParameter(PlanBlockWrite(*expressions, geometry), [&](const expr::ParameterType& parameter) {
                request.parameter_limits[parameter.index - 1U] = ParameterBytes(parameter.type);
            });
            if (expressions->version_parameter != 0U) request.parameter_limits[expressions->version_parameter - 1U] = 8U;
        } catch (const std::invalid_argument& e) {
            return reject(ErrorReply(e));
        } catch (const std::out_of_range& e) {
            return reject(ErrorReply(e));
        }
    }
    for (const auto& assignment :
         set != nullptr ? std::span<const cql::Assignment>(set->values) : std::span<const cql::Assignment>{}) {
        const auto* parameter = std::get_if<cql::Parameter>(&assignment.value);
        if (parameter == nullptr) {
            continue;
        }
        const std::size_t index = layout.FindColumn(assignment.column);
        if (index == std::string_view::npos) {
            return reject(Protocol::Error("INVALID_ARGUMENT", "the table has no column " + assignment.column + "; use DESCRIBE " + table_name + " to check column names"));
        }
        request.parameter_limits[parameter->index - 1U] = ParameterBytes(layout.schema().columns[index].type);
    }
    // The connection checks actual declared frame lengths before reading
    // each payload, not the sum of column maxima: small values may fit even
    // when their columns allow a much larger total.
    request.parameter_bytes_limit = kMaxStatementParameterBytes;
    request.plan = PayloadPlan::kParameters;
    return request;
}

std::string CommandEngine::ExecuteStatement(
    SessionState& session,
    std::string_view line,
    std::span<const std::optional<std::string>> parameters,
    MetricsRegistry::CommandClass& command_class,
    StatementLogInfo* info,
    std::unique_ptr<ReplyContinuation>* continuation,
    const std::function<void(std::size_t)>* reserve_reply) {
    try {
        CheckStatementCancellation();
        cql::Parsed parsed;
        try {
            parsed = cql::Parse(StatementOf(line));
        } catch (const cql::UnknownCommandError& e) {
            return Protocol::Error("UNKNOWN_COMMAND", e.what());
        } catch (const cql::ParseError& e) {
            return Protocol::Error("SYNTAX", e.what());
        }
        // Described before the health check, so a statement refused because
        // migrations need recovery is still audited.
        if (info != nullptr) {
            DescribeStatement(parsed.statement, *info);
        }
        catalog_->migration_health()->Check();
        if (parameters.size() != parsed.parameters) {
            return Protocol::Error(
                "PROTOCOL",
                "the statement takes " + std::to_string(parsed.parameters) + " parameters, got " +
                    std::to_string(parameters.size()));
        }
        if (catalog_->access_mode() == AccessMode::kReadOnly && !AvailableReadOnly(parsed.statement)) {
            StatementLogInfo described;
            DescribeStatement(parsed.statement, described);
            return Protocol::Error(
                "INVALID_ARGUMENT",
                std::string(described.verb) + " is not available: the server opened its data directory read-only");
        }
        if (session.transaction != nullptr && session.transaction->aborted.has_value() && !SessionControl(parsed.statement)) {
            // Ended by a conflict: only COMMIT or ROLLBACK closes it.
            std::string reply = *session.transaction->aborted;
            if (std::holds_alternative<cql::Rollback>(parsed.statement)) {
                reply = Protocol::SimpleString("OK");
            }
            if (std::holds_alternative<cql::Rollback>(parsed.statement) ||
                std::holds_alternative<cql::Commit>(parsed.statement)) {
                EnterStatementDecision();
                session.transaction.reset();
            }
            return reply;
        }
        if (session.transaction != nullptr && !AllowedInTransaction(parsed.statement)) {
            return Protocol::Error(
                "INVALID_ARGUMENT",
                "inside a transaction only GET, SET and DELETE statements, AGGREGATE, DESCRIBE, PING, COMMIT and ROLLBACK run");
        }
        return std::visit(
            Overloaded{
                [&](const cql::ShowConnections&) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    return ListConnections(session);
                },
                [&](const cql::Cancel& cancel) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    return ControlConnection(session, cancel.id, false);
                },
                [&](const cql::KillConnection& kill) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    return ControlConnection(session, kill.id, true);
                },
                [&](const cql::Migrate& migration) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    MigrationRequest request;
                    std::stop_source cancelled;
                    const auto request_stop = [&] { cancelled.request_stop(); };
                    std::stop_callback shutdown(session.backup_cancelled, request_stop);
                    std::stop_callback statement(StatementStopToken(), request_stop);
                    request.cancelled = cancelled.get_token();
                    request.record = {migration.name, 0U, config_.require_auth ? session.user : std::string{}, migration.text};
                    std::visit(Overloaded{
                        [&](const cql::CreateTable& create) {
                            RequireCreateRight(session, create.table);
                            request.kind = MigrationRequest::Kind::kCreate;
                            request.table = create.table;
                            request.if_not_exists = create.if_not_exists;
                            request.definition = [&] { return DefinitionFrom(create, catalog_->default_options()); };
                        },
                        [&](const cql::AlterTable& alter) {
                            RequireRight(session, alter.table, Right::kAdmin);
                            request.kind = MigrationRequest::Kind::kAlter;
                            request.table = alter.table;
                            std::visit(Overloaded{
                                [&](const cql::AddColumn& add) {
                                    request.chunk_column = add.chunk_column;
                                    if (add.if_not_exists)
                                        request.columns_if_needed = [&add](const TableSchema& schema) { return !HasColumn(schema, add.column.name); };
                                    request.alter = [&add](StoreManifest& m) {
                                        if (add.chunk_column) *m.chunk_schema = AddChunkColumn(*m.chunk_schema,
                                            ColumnFrom(add.column, m.chunk_schema->next_column_id), true);
                                        else m.schema = AddColumn(m.schema, ColumnFrom(add.column, m.schema.next_column_id), true);
                                    };
                                },
                                [&](const cql::DropColumn& drop) {
                                    request.chunk_column = drop.chunk_column;
                                    if (drop.if_exists)
                                        request.columns_if_needed = [&drop](const TableSchema& schema) { return HasColumn(schema, drop.column); };
                                    request.alter = [&drop](StoreManifest& m) {
                                        if (drop.chunk_column) *m.chunk_schema = DropChunkColumn(*m.chunk_schema, drop.column);
                                        else m.schema = DropColumn(m.schema, drop.column);
                                    };
                                },
                                [&](const cql::RenameColumn& rename) {
                                    request.chunk_column = rename.chunk_column;
                                    request.alter = [&rename](StoreManifest& m) {
                                        if (rename.chunk_column) *m.chunk_schema = RenameChunkColumn(*m.chunk_schema, rename.column, rename.new_name);
                                        else m.schema = RenameColumn(m.schema, rename.column, rename.new_name);
                                    };
                                },
                                [&](const cql::AlterColumnType& change) {
                                    if (change.conversion) {
                                        request.alter = [&change](StoreManifest& m) { m.schema = ChangeColumnType(m.schema, change.column, change.type, *change.conversion); };
                                    } else request.narrowing = std::make_pair(change.column, change.type);
                                },
                                [&](const cql::SetOption& set) {
                                    request.alter = [&set](StoreManifest& m) {
                                        ReplaceTableOptions(&m, OptionsFrom({set.option}).ApplyTo(DecodeTableOptions(m.options)));
                                    };
                                },
                                [&](const cql::AlterColumnAttribute& attribute) {
                                    if (attribute.change == cql::AlterColumnAttribute::Change::kDropNull) {
                                        request.drop_null = attribute.column;
                                        return;
                                    }
                                    request.alter = [&attribute](StoreManifest& m) {
                                        m.schema = ChangeColumnAttributes(m.schema, attribute.column, AttributesFrom(m.schema, attribute));
                                    };
                                },
                                [&](const cql::RenameTable&) {
                                    // The parser refuses it inside MIGRATE.
                                    throw std::invalid_argument("MIGRATE cannot rename a table");
                                },
                                [&](const cql::AddCheck& add) {
                                    request.add_check = [&add](const TableSchema& schema) { return CheckFrom(add.check, schema); };
                                },
                                [&](const cql::DropCheck& drop) {
                                    if (drop.if_exists)
                                        request.columns_if_needed = [&drop](const TableSchema& schema) { return HasCheck(schema, drop.name); };
                                    request.alter = [&drop](StoreManifest& m) { m.schema = chunkdb::DropCheck(m.schema, drop.name); };
                                }
                            }, alter.change);
                        },
                        [&](const cql::DropTable& drop) {
                            RequireRight(session, drop.table, Right::kAdmin);
                            request.kind = MigrationRequest::Kind::kDrop; request.table = drop.table; request.if_exists = drop.if_exists;
                        },
                        [&](const cql::GrantRight& grant) {
                            RequireManagesUsers(session);
                            request.kind = MigrationRequest::Kind::kGrant;
                            request.user = grant.user; request.table = grant.table;
                            request.right = grant.right; request.server_rights = grant.server_rights; request.revoke = grant.revoke;
                        },
                        [&](const cql::CreateSlot& slot) {
                            RequireRight(session, slot.table, Right::kAdmin);
                            request.kind = MigrationRequest::Kind::kCreateSlot; request.table = slot.table; request.slot = slot.name; request.if_not_exists = slot.if_not_exists;
                            request.slot_filters = {.area = slot.area, .no_before = slot.no_before, .chunks = slot.chunks};
                            request.slot_columns = slot.columns;
                            request.slot_owner = SlotOwner(slot, config_.require_auth, session.user);
                            request.slot_owner_given = slot.owner.has_value();
                        },
                        [&](const cql::DropSlot& slot) {
                            RequireRight(session, slot.table, Right::kAdmin);
                            request.kind = MigrationRequest::Kind::kDropSlot; request.table = slot.table; request.slot = slot.name; request.if_exists = slot.if_exists;
                        }
                    }, migration.statement);
                    // The rights just checked, so the lookup reads no user state.
                    (void)CurrentUser(session);
                    request.require_recorded_access = [require_auth = config_.require_auth, user = session.user_rights](const MigrationRecord& record) {
                        if (!MayReplayMigration(require_auth, user.get(), record))
                            throw PermissionDeniedError("migration name '" + record.name + "' is recorded for a change outside your rights");
                    };
                    return Protocol::SimpleString(catalog_->Migrate(request, config_.users.get()) ? "applied" : "skipped");
                },
                [&](const cql::ShowMigrations&) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    const User* user = CurrentUser(session);
                    // Without MANAGES USERS or an ADMIN grant no record is within the
                    // caller's rights: the ledger is not read.
                    if (config_.require_auth && (user == nullptr || (!user->manages_users && !HasAnyGrant(*user, Right::kAdmin)))) {
                        std::string empty;
                        Protocol::AppendArrayHeader(empty, 0U);
                        return empty;
                    }
                    std::vector<MigrationRecord> records;
                    for (auto& record : catalog_->Migrations())
                        if (MayReplayMigration(config_.require_auth, user, record)) records.push_back(std::move(record));
                    // Who applied a record is user information: MANAGES USERS shows it.
                    const bool show_user = !config_.require_auth || user->manages_users;
                    std::string reply;
                    Protocol::AppendArrayHeader(reply, records.size());
                    for (const auto& record : records) {
                        Protocol::AppendMapHeader(reply, 4);
                        Protocol::AppendBulk(reply, "name"); Protocol::AppendBulk(reply, record.name);
                        Protocol::AppendBulk(reply, "applied_ms"); Protocol::AppendInteger(reply, record.applied_ms);
                        Protocol::AppendBulk(reply, "user");
                        if (show_user) Protocol::AppendBulk(reply, record.user);
                        else Protocol::AppendNull(reply);
                        Protocol::AppendBulk(reply, "statement"); Protocol::AppendBulk(reply, record.statement);
                    }
                    return reply;
                },
                [&](const cql::Watch& watch) {
                    RequireRight(session, watch.table, Right::kRead);
                    // The feed ends the watch once the right or user is gone,
                    // checking from the generation READ was confirmed at. Set
                    // before subscribing: no watch exists without its check.
                    session.watch_right = config_.require_auth ? std::make_shared<ReadRightCheck>(config_.users,
                        session.user, session.user_incarnation, watch.table, session.user_generation) : nullptr;
                    auto table = catalog_->Require(watch.table);
                    const auto filters = SlotFilters(table, watch);
                    FeedOptions options{
                        .after = watch.after,
                        .areas = watch.areas,
                        .column_ids = filters.column_ids,
                        .no_before = filters.no_before,
                        .chunks = filters.chunks,
                        .buffer_bytes = session.watch_options.buffer_bytes,
                        .progress_interval = session.watch_options.progress_interval,
                        .ack_timeout_ms = watch.ack_timeout_ms,
                        .notify = session.watch_options.notify,
                    };
                    if (watch.slot) {
                        const auto* user = config_.require_auth ? CurrentUser(session) : nullptr;
                        // A user dropped since the right check must not become an owner.
                        if (config_.require_auth && !user) throw UserDroppedError();
                        const bool admin_all = !config_.require_auth || (user && user->grants.contains(kEveryTable) &&
                            user->grants.at(kEveryTable) >= Right::kAdmin);
                        auto state = SlotWatch::Create(table, *watch.slot, options, watch.takeover,
                            config_.require_auth ? session.user : std::string{}, [&](std::string_view old_user) {
                                if (config_.require_auth && old_user != session.user && !admin_all)
                                    throw PermissionDeniedError("ADMIN on *");
                            });
                        if (session.register_slot_watch && !session.register_slot_watch(state))
                            return Protocol::Error("BUSY", "maximum slot catch-up workers reached", {{"reason", "slot_worker_limit"}, {"table", watch.table}});
                        // A slot without an owner takes the user of its first
                        // granted attach; one by ADMIN on * records nobody.
                        if (config_.require_auth && !admin_all && !state->owner()) state->BindOwner(session.user);
                        // A watch that ADMIN on * allows ends once that right is gone.
                        if (session.watch_right && state->owner() != session.user) session.watch_right->RequireEveryTableAdmin();
                        session.slot_watch = std::move(state);
                    } else session.watch = table->SubscribeFeed(options);
                    session.table = std::move(table);
                    const auto position = session.slot_watch ? session.slot_watch->position() : session.watch->position();
                    std::string reply;
                    Protocol::AppendMapHeader(reply, 2U);
                    Protocol::AppendBulk(reply, "epoch");
                    Protocol::AppendBulk(reply, StoreIdHex(position.epoch));
                    Protocol::AppendBulk(reply, "revision");
                    Protocol::AppendInteger(reply, position.revision);
                    return reply;
                },
                [&](const cql::Unwatch&) { return Protocol::Error("INVALID_ARGUMENT", "no watch is open"); },
                [&](const cql::Ack&) { return Protocol::Error("INVALID_ARGUMENT", "no slot watch is open"); },
                [&](const cql::CreateSlot& create) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRight(session, create.table, Right::kAdmin);
                    const auto table = catalog_->Require(create.table);
                    (void)table->CreateFeedSlot(create.name, create.if_not_exists, SlotFilters(table, create),
                        SlotOwner(create, config_.require_auth, session.user), create.owner.has_value());
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::DropSlot& drop) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRight(session, drop.table, Right::kAdmin);
                    const auto table = catalog_->Require(drop.table);
                    table->DropFeedSlot(drop.name, drop.if_exists);
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::ShowSlots& show) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    std::vector<std::pair<std::string, FeedSlot>> slots;
                    const auto append = [&](const std::string& name) {
                        const auto table = catalog_->Require(name);
                        if (auto* hook = hook_.load(std::memory_order_acquire))
                            hook->Run(CommandEngineTestHook::Point::kBeforeSlotTableList, name);
                        for (auto& slot : table->ListFeedSlots(true))
                            if (!show.prefix || slot.name.starts_with(*show.prefix)) slots.emplace_back(name, std::move(slot));
                    };
                    // MONITOR lists the slots of every table; owners stay hidden.
                    const bool monitor = HasServerRight(session, ServerRight::kMonitor);
                    if (show.table) {
                        if (!monitor) RequireRight(session, *show.table, Right::kRead);
                        append(*show.table);
                    } else {
                        const auto tables = catalog_->List();
                        if (auto* hook = hook_.load(std::memory_order_acquire))
                            hook->Run(CommandEngineTestHook::Point::kAfterSlotTablesListed, {});
                        for (const auto& info : tables) {
                            if (!monitor && !RightOnTable(session, info.name)) continue;
                            try {
                                append(info.name);
                            } catch (const TableNotFoundError&) {
                                // DROP may finish after List or Find. An unscoped
                                // listing omits that table; ON t stays strict.
                            } catch (const TableFencedError&) {
                                // So may a failure that fences it.
                            }
                        }
                    }
                    std::string reply;
                    // An owner is user information: the owner itself or MANAGES USERS sees it.
                    const auto* viewer = CurrentUser(session);
                    const bool show_owners = !config_.require_auth || (viewer != nullptr && viewer->manages_users);
                    Protocol::AppendArrayHeader(reply, slots.size());
                    for (const auto& [name, slot] : slots) {
                        Protocol::AppendMapHeader(reply, 12);
                        Protocol::AppendBulk(reply, "table");
                        Protocol::AppendBulk(reply, name);
                        Protocol::AppendBulk(reply, "name");
                        Protocol::AppendBulk(reply, slot.name);
                        Protocol::AppendBulk(reply, "epoch");
                        Protocol::AppendBulk(reply, StoreIdHex(slot.position.epoch));
                        Protocol::AppendBulk(reply, "acked");
                        Protocol::AppendInteger(reply, slot.position.revision);
                        // A slot delivers up to the durable revision its records hold.
                        Protocol::AppendBulk(reply, "revision");
                        Protocol::AppendInteger(reply, slot.durable_watermark);
                        Protocol::AppendBulk(reply, "lag_revisions");
                        Protocol::AppendInteger(reply, slot.durable_watermark > slot.position.revision ? slot.durable_watermark - slot.position.revision : 0U);
                        Protocol::AppendBulk(reply, "retained_bytes");
                        Protocol::AppendInteger(reply, slot.retained_bytes);
                        Protocol::AppendBulk(reply, "attached");
                        Protocol::AppendBoolean(reply, slot.attached);
                        Protocol::AppendBulk(reply, "lost");
                        reply += slot.lost ? "#t\r\n" : "#f\r\n";
                        Protocol::AppendBulk(reply, "lost_reason");
                        if (slot.lost) Protocol::AppendBulk(reply, slot.schema_filter_lost ? "schema_filter" : "retention_limit");
                        else Protocol::AppendNull(reply);
                        Protocol::AppendBulk(reply, "filters"); Protocol::AppendMapHeader(reply, 5U);
                        Protocol::AppendBulk(reply, "area");
                        if (slot.filters.area) {
                            const auto& area = *slot.filters.area;
                            Protocol::AppendArrayHeader(reply, static_cast<std::size_t>(area.dims) * 2U);
                            Protocol::AppendInteger(reply, area.first.x);
                            Protocol::AppendInteger(reply, area.first.y);
                            if (area.dims == 3U) Protocol::AppendInteger(reply, area.first.z);
                            Protocol::AppendInteger(reply, area.last.x);
                            Protocol::AppendInteger(reply, area.last.y);
                            if (area.dims == 3U) Protocol::AppendInteger(reply, area.last.z);
                        } else Protocol::AppendNull(reply);
                        Protocol::AppendBulk(reply, "column_ids");
                        if (slot.filters.column_ids) {
                            Protocol::AppendArrayHeader(reply, slot.filters.column_ids->size());
                            for (const auto id : *slot.filters.column_ids) Protocol::AppendInteger(reply, static_cast<std::uint64_t>(id));
                        } else Protocol::AppendNull(reply);
                        Protocol::AppendBulk(reply, "columns");
                        if (slot.filters.column_ids) {
                            Protocol::AppendArrayHeader(reply, slot.column_names.size());
                            for (const auto& name : slot.column_names) {
                                if (name) Protocol::AppendBulk(reply, *name); else Protocol::AppendNull(reply);
                            }
                        } else Protocol::AppendNull(reply);
                        Protocol::AppendBulk(reply, "no_before"); reply += slot.filters.no_before ? "#t\r\n" : "#f\r\n";
                        Protocol::AppendBulk(reply, "chunks"); reply += slot.filters.chunks ? "#t\r\n" : "#f\r\n";
                        Protocol::AppendBulk(reply, "owner");
                        if (slot.owner && (show_owners || (viewer != nullptr && *slot.owner == session.user))) Protocol::AppendBulk(reply, *slot.owner);
                        else Protocol::AppendNull(reply);
                    }
                    return reply;
                },
                [&](const cql::GetBlock& get) {
                    command_class = MetricsRegistry::CommandClass::kPointRead;
                    RequireRight(session, get.table, Right::kRead);
                    const auto lease = session.transaction != nullptr ? TxnLease(session, get.table)
                                                                      : AcquireNamedTable(session, get.table);
                    ChunkStore& store = lease.store();
                    RequireDimensions(store.geometry(), get.dims);
                    const auto& layout = store.geometry().layout();
                    std::vector<std::size_t> columns = ColumnsOf(layout, get.columns);
                    if (columns.empty()) {
                        columns.resize(layout.schema().columns.size());
                        for (std::size_t i = 0; i < columns.size(); ++i) {
                            columns[i] = i;
                        }
                    }
                    std::optional<std::vector<ColumnValue>> values;
                    if (reserve_reply) (*reserve_reply)(BlockReplyCaptureBound(layout, columns));
                    std::optional<std::uint64_t> version;
                    if (session.transaction != nullptr) {
                        auto& txn = *session.transaction;
                        const ChunkCoord coord = BlockCoord(store.geometry(), get.x, get.y, get.z);
                        TxnRead(txn, std::span<const ChunkCoord>(&coord, 1));
                        const auto own = txn.writes.find(coord);
                        if (own != txn.writes.end()) values = get.dims == 3U
                            ? store.BlockInState3D(own->second, get.x, get.y, get.z)
                            : store.BlockInState(own->second, get.x, get.y);
                        else if (get.with_version) {
                            auto read = get.dims == 3U
                                ? store.GetBlockWithVersion3DAt(*txn.snapshot, get.x, get.y, get.z)
                                : store.GetBlockWithVersionAt(*txn.snapshot, get.x, get.y);
                            values = std::move(read.values);
                            version = read.version;
                        } else values = get.dims == 3U ? store.GetBlock3DAt(*txn.snapshot, get.x, get.y, get.z)
                                                      : store.GetBlockAt(*txn.snapshot, get.x, get.y);
                    } else if (get.with_version) {
                        auto read = get.dims == 3U ? store.GetBlockWithVersion3D(get.x, get.y, get.z)
                                                  : store.GetBlockWithVersion(get.x, get.y);
                        values = std::move(read.values);
                        version = read.version;
                    } else values = get.dims == 3U ? store.GetBlock3D(get.x, get.y, get.z) : store.GetBlock(get.x, get.y);
                    std::string reply;
                    if (get.with_version || get.with_schema) {
                        Protocol::AppendArrayHeader(reply, 1U + (get.with_version ? 1U : 0U) + (get.with_schema ? 1U : 0U));
                        if (get.with_version) {
                            if (version) Protocol::AppendInteger(reply, *version);
                            else Protocol::AppendNull(reply);
                        }
                        if (get.with_schema) Protocol::AppendInteger(reply, layout.schema().version);
                    }
                    if (!values) Protocol::AppendNull(reply);
                    else {
                        Protocol::AppendArrayHeader(reply, columns.size());
                        auto response_bytes = reply.size();
                        for (const auto index : columns) {
                            CheckStatementCancellation();
                            const auto bytes = ValueReplyBytes((*values)[index]);
                            if (bytes > kMaxChunkRangeResponseBytes - response_bytes)
                                throw std::out_of_range("GET BLOCK response exceeds the response byte limit; request fewer columns");
                            response_bytes += bytes;
                        }
                        reply.reserve(response_bytes);
                        for (const std::size_t index : columns)
                            AppendValue(reply, layout.schema().columns[index], (*values)[index]);
                    }
                    return reply;
                },
                [&](const cql::SetBlock& set) {
                    if (set.expressions) return BlockWriteWithExpressions(session, &set, nullptr, parameters, command_class);
                    command_class = set.if_version.has_value() ? MetricsRegistry::CommandClass::kConditional
                                                               : MetricsRegistry::CommandClass::kPointWrite;
                    RequireRight(session, set.table, Right::kWrite);
                    RequireNoVersionInTxn(session, set.if_version);
                    const auto lease = session.transaction != nullptr ? TxnLease(session, set.table)
                                                                      : AcquireNamedTable(session, set.table);
                    ChunkStore& store = lease.store();
                    RequireDimensions(store.geometry(), set.dims);
                    const auto& layout = store.geometry().layout();
                    std::vector<ColumnAssignment> values;
                    values.reserve(set.values.size());
                    for (const auto& assignment : set.values) {
                        const Column& column = layout.schema().columns[RequireColumn(layout, assignment.column)];
                        const auto* parameter = std::get_if<cql::Parameter>(&assignment.value);
                        values.push_back(ColumnAssignment{
                            .column = assignment.column,
                            .value = parameter != nullptr
                                         ? ParameterValue(column, parameter->index, parameters[parameter->index - 1U])
                                         : LiteralValue(column, assignment.value),
                        });
                    }
                    if (session.transaction != nullptr) {
                        TxnWrite(
                            *session.transaction, store, BlockCoord(store.geometry(), set.x, set.y, set.z),
                            [&](ChunkState& state) {
                                if (set.dims == 3U) store.SetBlockInState3D(state, set.x, set.y, set.z, values);
                                else store.SetBlockInState(state, set.x, set.y, values);
                            });
                        return NullReply();
                    }
                    return VersionReply(set.dims == 3U ? store.SetBlock3D(set.x, set.y, set.z, values, set.if_version)
                                                       : store.SetBlock(set.x, set.y, values, set.if_version));
                },
                [&](const cql::DeleteBlock& del) {
                    if (del.expressions) return BlockWriteWithExpressions(session, nullptr, &del, parameters, command_class);
                    command_class = del.if_version.has_value() ? MetricsRegistry::CommandClass::kConditional
                                                               : MetricsRegistry::CommandClass::kPointWrite;
                    RequireRight(session, del.table, Right::kWrite);
                    RequireNoVersionInTxn(session, del.if_version);
                    if (session.transaction != nullptr) {
                        const auto lease = TxnLease(session, del.table);
                        ChunkStore& store = lease.store();
                        RequireDimensions(store.geometry(), del.dims);
                        TxnWrite(
                            *session.transaction, store, BlockCoord(store.geometry(), del.x, del.y, del.z),
                            [&](ChunkState& state) {
                                if (del.dims == 3U) store.UnsetBlockInState3D(state, del.x, del.y, del.z);
                                else store.UnsetBlockInState(state, del.x, del.y);
                            });
                        return NullReply();
                    }
                    const auto lease = AcquireNamedTable(session, del.table);
                    RequireDimensions(lease.store().geometry(), del.dims);
                    return VersionReply(del.dims == 3U
                        ? lease.store().UnsetBlock3D(del.x, del.y, del.z, del.if_version)
                        : lease.store().UnsetBlock(del.x, del.y, del.if_version));
                },
                [&](const cql::GetChunk& get) {
                    command_class = MetricsRegistry::CommandClass::kChunkRead;
                    RequireRight(session, get.table, Right::kRead);
                    const auto lease = session.transaction != nullptr ? TxnLease(session, get.table)
                                                                      : AcquireNamedTable(session, get.table);
                    ChunkStore& store = lease.store();
                    RequireDimensions(store.geometry(), get.dims);
                    const auto& layout = store.geometry().layout();
                    const auto columns = ColumnsOf(layout, get.columns);
                    ChunkState state;
                    bool private_state = false;
                    const ChunkCoord coord{get.chunk_x, get.chunk_y, get.chunk_z};
                    if (reserve_reply)
                        (*reserve_reply)(ChunkReplyCaptureBound(layout, ChunkReplyStateBytes(store.geometry(), 1U, 64U)));
                    if (session.transaction != nullptr) {
                        auto& txn = *session.transaction;
                        TxnRead(txn, std::span<const ChunkCoord>(&coord, 1));
                        const auto own = txn.writes.find(coord);
                        private_state = own != txn.writes.end();
                        state = own != txn.writes.end() ? own->second
                                                        : store.ReadChunkStateAt(*txn.snapshot, coord);
                    } else {
                        state = store.ReadChunkState(coord);
                    }
                    if (!state.written) return NullReply();
                    ReplyParts reply;
                    AppendChunkState(reply, store.geometry(), state, columns, private_state);
                    return FinishReply(std::move(reply), continuation);
                },
                [&](const cql::SetChunk& set) {
                    command_class = set.if_version.has_value() || set.if_absent ? MetricsRegistry::CommandClass::kConditional
                                                                              : MetricsRegistry::CommandClass::kChunkWrite;
                    RequireRight(session, set.table, Right::kWrite);
                    RequireNoVersionInTxn(session, set.if_version);
                    if (session.transaction != nullptr && set.if_absent)
                        throw std::invalid_argument("IF ABSENT is not used inside a transaction: COMMIT checks every chunk the transaction touched");
                    const auto lease = session.transaction != nullptr ? TxnLease(session, set.table)
                                                                      : AcquireNamedTable(session, set.table);
                    ChunkStore& store = lease.store();
                    const auto& geometry = store.geometry();
                    RequireDimensions(geometry, set.dims);
                    const ChunkCoord coord{set.chunk_x, set.chunk_y, set.chunk_z};
                    const auto change = [&](const std::function<void(ChunkState&)>& transform) {
                        if (session.transaction == nullptr)
                            return VersionReply(store.TransformChunkState(coord, set.if_version, set.if_absent, transform));
                        TxnWrite(*session.transaction, store, coord, [&](ChunkState& state) {
                            const auto current_vars = state.vars;
                            const auto current_metadata = !state.written && geometry.chunk_columns_require_initialization()
                                ? nullptr : state.chunk_columns;
                            if (!state.written && geometry.chunk_columns_require_initialization()) state.chunk_columns.reset();
                            const auto* checks = geometry.checks();
                            // The blocks before the statement, kept only to evaluate the changed ones.
                            std::vector<std::uint8_t> payload;
                            std::vector<std::uint8_t> presence;
                            if (checks != nullptr) {
                                payload = state.payload;
                                presence = state.presence_bitmap;
                            }
                            transform(state);
                            store.PrepareTxnChunkState(state, current_vars, current_metadata.get());
                            // A failing block refuses the statement; COMMIT checks again.
                            if (checks != nullptr)
                                checks->RequireChanged(coord, state.payload, state.presence_bitmap, state.vars, payload,
                                    presence, current_vars);
                            state.written = true;
                        });
                        return NullReply();
                    };
                    // Metadata statements have no state parameter. Handle them
                    // before inspecting the form's one-based parameter index.
                    if (set.metadata) {
                        std::vector<ColumnAssignment> values;
                        values.reserve(set.metadata->size());
                        for (const auto& assignment : *set.metadata) {
                            const auto& columns = geometry.chunk_schema().columns;
                            const auto at = std::find_if(columns.begin(), columns.end(),
                                [&](const auto& column) { return column.name == assignment.column; });
                            if (at == columns.end()) throw std::invalid_argument("unknown chunk column " + assignment.column);
                            const auto* parameter = std::get_if<cql::Parameter>(&assignment.value);
                            values.push_back({assignment.column, parameter != nullptr
                                ? ParameterValue(*at, parameter->index, parameters[parameter->index - 1U])
                                : LiteralValue(*at, assignment.value)});
                        }
                        return change([&](ChunkState& state) {
                            state.chunk_columns = std::make_shared<const ChunkColumnState>(AssignChunkColumns(
                                geometry.chunk_schema(), state.chunk_columns.get(), values, !state.written));
                        });
                    }
                    const auto& frame = parameters[set.state.index - 1U];
                    if (!frame.has_value()) {
                        throw std::invalid_argument("the chunk cannot be NULL; DELETE its blocks instead");
                    }
                    const auto columns = ColumnsOf(geometry.layout(), set.columns);
                    const std::span<const std::uint8_t> bytes(reinterpret_cast<const std::uint8_t*>(frame->data()), frame->size());
                    const auto limit = ChunkFormBytes(geometry, !columns.empty());
                    const auto form = ParseChunkFormSections(bytes, limit, ChunkFormReadMode::kMutation);
                    const std::uint64_t encoded_for = form.header.schema_version;
                    const std::uint64_t current = geometry.layout().schema().version;
                    if (encoded_for != current) {
                        return Protocol::Error(
                            "SCHEMA_MISMATCH",
                            "the chunk was encoded for schema version " + std::to_string(encoded_for) +
                                "; use DESCRIBE " + set.table + " and encode the chunk again with the current columns",
                            {{"current", std::to_string(current)}, {"actual", std::to_string(encoded_for)}, {"table", set.table}});
                    }
                    if (session.transaction != nullptr) {
                        // The transaction's limits are checked by the fewest
                        // bytes the decoded state takes, before the form is
                        // decoded or the chunk is read.
                        RequireTxnRoom(*session.transaction, coord, DecodedStateBytesAtLeast(geometry, form, !columns.empty()));
                    }
                    auto next = DecodeSectionedChunkState(geometry, bytes, kVarMaxChunkBytesLimit, columns, limit);
                    if (!columns.empty()) return change([&](ChunkState& state) {
                        if (!state.written && geometry.chunk_columns_require_initialization())
                            throw std::invalid_argument("a first write must supply the REQUIRED chunk columns");
                        state = MergeProjectedChunkState(geometry, state, next, columns, store.var_max_chunk_bytes());
                    });
                    if (session.transaction == nullptr)
                        return VersionReply(store.WriteChunkState(coord, std::move(next), set.if_version, set.if_absent));
                    return change([&](ChunkState& state) {
                        const auto version = state.version;
                        const auto time = state.commit_time_ms;
                        state = std::move(next);
                        state.version = version;
                        state.commit_time_ms = time;
                    });
                },
                [&](const cql::GetArea& get) {
                    command_class = MetricsRegistry::CommandClass::kRange;
                    RequireRight(session, get.table, Right::kRead);
                    const auto lease = session.transaction != nullptr ? TxnLease(session, get.table)
                                                                      : AcquireNamedTable(session, get.table);
                    ChunkStore& store = lease.store();
                    RequireDimensions(store.geometry(), get.dims);
                    const auto& layout = store.geometry().layout();
                    const auto columns = ColumnsOf(layout, get.columns);
                    // A downsampled read checks its factor and column types before any chunk is read.
                    const std::optional<Geometry> scaled = get.scale
                        ? std::optional<Geometry>(ScaledGeometry(store.geometry(), *get.scale, columns)) : std::nullopt;
                    const Geometry& form_geometry = scaled ? *scaled : store.geometry();
                    const ChunkFormScale* scale = get.scale ? &*get.scale : nullptr;
                    const ChunkAreaSelector selector{.dims = get.dims, .first = {get.x0, get.y0, get.z0},
                        .last = {get.x1, get.y1, get.z1}, .around = get.around, .radius = get.radius};
                    const bool paged = get.limit.has_value() || get.after.has_value();
                    const auto& limits = store.query_limits();
                    const auto limit = get.limit.value_or(limits.max_area_chunks);
                    if (limit == 0U || limit > limits.max_area_chunks)
                        throw std::invalid_argument("area page limit is outside the configured chunk bound");
                    if (!paged && detail::AreaRequestedExceeds(selector, limits.max_area_chunks))
                        throw std::invalid_argument("area exceeds its requested-chunk limit; use LIMIT");
                    const auto budget = limits.max_area_response_bytes;
                    const auto too_large = [budget] {
                        return std::out_of_range("area response exceeds the " + std::to_string(budget) +
                            "-byte limit; request fewer chunks");
                    };
                    if (reserve_reply) {
                        // The chunks one reply may return.
                        const auto count = AreaCellBound(selector, static_cast<std::size_t>(limit));
                        (*reserve_reply)(ChunkReplyCaptureBound(layout,
                            ChunkReplyStateBytes(store.geometry(), count, 96U, budget)));
                    }
                    std::vector<ChunkCoord> covered;
                    TxnAreaRead read;
                    if (session.transaction != nullptr) {
                        read.overlay = [&session](const ChunkCoord& coord) -> const ChunkState* {
                            const auto own = session.transaction->writes.find(coord);
                            return own == session.transaction->writes.end() ? nullptr : &own->second;
                        };
                        read.covered = &covered;
                    }
                    // Admitted rows: framing text before each encoded form.
                    std::vector<ReplyParts::Piece> rows;
                    std::size_t count = 0U;
                    std::size_t body_bytes = 0U;
                    auto prefix = AreaReplyPrefix(get.with_schema, layout.schema().version, paged, count);
                    auto suffix = AreaReplySuffix(paged, false, {}, get.dims);
                    if (prefix.size() > budget || suffix.size() > budget - prefix.size()) throw too_large();
                    // An entry that cannot fit an otherwise empty page ends a
                    // page that already admitted a cell, so the next page starts
                    // at it; as the first cell it is refused with its coordinate.
                    bool first_cell = true;
                    (void)store.VisitChunkArea(selector, get.after, static_cast<std::size_t>(limit),
                        [&](const ChunkCoord& coord, std::optional<ChunkRangeEntry>&& entry, bool more) {
                            const auto next_count = count + static_cast<std::size_t>(entry.has_value());
                            auto next_prefix = AreaReplyPrefix(get.with_schema, layout.schema().version, paged, next_count);
                            auto next_suffix = AreaReplySuffix(paged, more, coord, get.dims);
                            const auto single_prefix = AreaReplyPrefix(get.with_schema, layout.schema().version, paged,
                                static_cast<std::size_t>(entry.has_value()));
                            if (single_prefix.size() > budget || next_suffix.size() > budget - single_prefix.size()) throw too_large();
                            const auto row_budget = budget - single_prefix.size() - next_suffix.size();
                            ReplyParts::Piece row;
                            std::size_t row_bytes = 0U;
                            if (entry) {
                                Protocol::AppendArrayHeader(row.text, static_cast<std::size_t>(get.dims) + 1U);
                                Protocol::AppendInteger(row.text, coord.x);
                                Protocol::AppendInteger(row.text, coord.y);
                                if (get.dims == 3U) Protocol::AppendInteger(row.text, coord.z);
                                const bool private_state = session.transaction != nullptr && session.transaction->writes.contains(coord);
                                ChunkState state{.version = entry->version, .payload = std::move(entry->payload),
                                    .presence_bitmap = std::move(entry->presence_bitmap), .vars = std::move(entry->vars), .written = true,
                                    .commit_time_ms = entry->commit_time_ms, .chunk_columns = std::move(entry->chunk_columns)};
                                if (scaled) state = ScaleChunkState(store.geometry(), *scaled, *scale, columns, state);
                                const auto form_size = SectionedChunkStateBytes(form_geometry, state, kVarMaxChunkBytesLimit, columns, scale);
                                const auto bulk_overhead = 5U + std::to_string(form_size).size();
                                if (row.text.size() + bulk_overhead > row_budget ||
                                    form_size > row_budget - row.text.size() - bulk_overhead) {
                                    if (paged && !first_cell) return false;
                                    throw AreaEntryTooLargeError(coord, get.dims, budget,
                                        single_prefix.size() + next_suffix.size() + row.text.size() + bulk_overhead + form_size);
                                }
                                const auto form_bound = row_budget - row.text.size();
                                row.body = EncodeSectionedChunkState(form_geometry, state, kVarMaxChunkBytesLimit,
                                    columns, private_state, form_bound, scale);
                                row_bytes = row.text.size() + 5U + std::to_string(row.body.size()).size() + row.body.size();
                                if (row_bytes > row_budget) throw too_large();
                            }
                            if (next_prefix.size() > budget || next_suffix.size() > budget - next_prefix.size() ||
                                body_bytes > budget - next_prefix.size() - next_suffix.size() ||
                                row_bytes > budget - next_prefix.size() - next_suffix.size() - body_bytes) {
                                if (!paged) throw too_large();
                                return false;
                            }
                            if (entry) rows.push_back(std::move(row));
                            body_bytes += row_bytes;
                            first_cell = false;
                            count = next_count;
                            prefix = std::move(next_prefix);
                            suffix = std::move(next_suffix);
                            return true;
                        }, true, session.transaction ? &*session.transaction->snapshot : nullptr, read);
                    if (session.transaction != nullptr) TxnRead(*session.transaction, covered);
                    if (info != nullptr) info->chunks = count;
                    ReplyParts reply;
                    reply.text() = std::move(prefix);
                    for (auto& row : rows) {
                        reply.text() += row.text;
                        reply.AppendBulk(std::move(row.body));
                    }
                    reply.text() += suffix;
                    return FinishReply(std::move(reply), continuation);
                },
                [&](const cql::Aggregate& aggregate) {
                    command_class = MetricsRegistry::CommandClass::kRange;
                    RequireRight(session, aggregate.table, Right::kRead);
                    const auto lease = session.transaction != nullptr ? TxnLease(session, aggregate.table)
                                                                      : AcquireNamedTable(session, aggregate.table);
                    ChunkStore& store = lease.store();
                    RequireDimensions(store.geometry(), aggregate.dims);
                    Aggregator aggregator(store.geometry().layout(), aggregate.functions);
                    std::optional<BlockFilter> filter;
                    if (aggregate.where) filter.emplace(WhereFilter(store.geometry(), *aggregate.where, parameters));
                    const ChunkAreaSelector selector{.dims = aggregate.dims,
                        .first = {aggregate.x0, aggregate.y0, aggregate.z0},
                        .last = {aggregate.x1, aggregate.y1, aggregate.z1},
                        .around = aggregate.around, .radius = aggregate.radius};
                    const auto& limits = store.query_limits();
                    const auto max_chunks = limits.max_area_chunks;
                    if (detail::AreaRequestedExceeds(selector, max_chunks))
                        throw std::invalid_argument("AGGREGATE covers at most " + std::to_string(max_chunks) +
                            " chunks; aggregate smaller areas and combine their results");
                    const auto budget = limits.max_area_response_bytes;
                    const auto too_large = [budget] {
                        return std::out_of_range("aggregate response exceeds the " + std::to_string(budget) +
                            "-byte limit; aggregate a smaller area or fewer functions");
                    };
                    // A row is its header, its coordinates and the results;
                    // a map entry is the function's name and its result.
                    const std::size_t row_bytes = 8U + 23U * aggregate.dims + aggregator.ResultBytesBound();
                    std::size_t map_bytes = 8U + aggregator.ResultBytesBound();
                    for (const auto& function : aggregate.functions)
                        map_bytes += cql::AggregateFunctionName(function).size() + 8U;
                    if (reserve_reply) {
                        const auto rows = AreaCellBound(selector, max_chunks);
                        (*reserve_reply)(std::min(budget, !aggregate.by_chunk ? map_bytes
                            : rows > budget / row_bytes ? budget : 8U + rows * row_bytes));
                    }
                    std::vector<ChunkCoord> covered;
                    TxnAreaRead read;
                    if (session.transaction != nullptr) {
                        read.overlay = [&session](const ChunkCoord& coord) -> const ChunkState* {
                            const auto own = session.transaction->writes.find(coord);
                            return own == session.transaction->writes.end() ? nullptr : &own->second;
                        };
                        read.covered = &covered;
                    }
                    std::string rows;
                    std::size_t row_count = 0U;
                    std::size_t chunks = 0U;
                    const auto page = store.VisitChunkArea(selector, std::nullopt, max_chunks,
                        [&](const ChunkCoord& coord, std::optional<ChunkRangeEntry>&& entry, bool) {
                            if (!entry) return true;
                            ++chunks;
                            if (aggregate.by_chunk) aggregator.Reset();
                            if (filter) (void)filter->Apply(coord, entry->payload, entry->presence_bitmap, entry->vars,
                                entry->chunk_columns.get());
                            aggregator.Add(entry->payload, entry->presence_bitmap);
                            if (!aggregate.by_chunk || aggregator.blocks() == 0U) return true;
                            Protocol::AppendArrayHeader(rows, aggregate.dims + aggregator.functions());
                            Protocol::AppendInteger(rows, coord.x);
                            Protocol::AppendInteger(rows, coord.y);
                            if (aggregate.dims == 3U) Protocol::AppendInteger(rows, coord.z);
                            aggregator.AppendResults(rows);
                            ++row_count;
                            if (rows.size() > budget) throw too_large();
                            return true;
                        }, filter && filter->needs_vars(), session.transaction ? &*session.transaction->snapshot : nullptr, read);
                    if (page.has_more) throw std::logic_error("AGGREGATE left cells of its selector unvisited");
                    if (info != nullptr) info->chunks = chunks;
                    std::string reply;
                    if (aggregate.by_chunk) {
                        Protocol::AppendArrayHeader(reply, row_count);
                        reply += rows;
                    } else {
                        Protocol::AppendMapHeader(reply, aggregator.functions());
                        for (std::size_t index = 0U; index < aggregator.functions(); ++index) {
                            Protocol::AppendBulk(reply, cql::AggregateFunctionName(aggregate.functions[index]));
                            aggregator.AppendResult(reply, index);
                        }
                    }
                    if (reply.size() > budget) throw too_large();
                    if (session.transaction != nullptr) TxnRead(*session.transaction, covered);
                    return reply;
                },
                [&](const cql::CreateTable& create) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireCreateRight(session, create.table);
                    (void)catalog_->Create(create.table,
                        [&] { return DefinitionFrom(create, catalog_->default_options()); }, create.if_not_exists);
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::AlterTable& alter) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRight(session, alter.table, Right::kAdmin);
                    std::visit(
                        Overloaded{
                            [&](const cql::AddColumn& add) {
                                if (add.chunk_column) {
                                    (void)catalog_->ChangeChunkColumnsIfNeeded(alter.table, [&add](const TableSchema& current) -> std::optional<TableSchema> {
                                        if (add.if_not_exists && HasColumn(current, add.column.name)) return std::nullopt;
                                        return chunkdb::AddChunkColumn(current, ColumnFrom(add.column, current.next_column_id), true);
                                    });
                                    return;
                                }
                                (void)catalog_->ChangeColumnsIfNeeded(alter.table, [&add](const TableSchema& current) -> std::optional<TableSchema> {
                                    if (add.if_not_exists && HasColumn(current, add.column.name)) return std::nullopt;
                                    return chunkdb::AddColumn(current, ColumnFrom(add.column, current.next_column_id), true);
                                });
                            },
                            [&](const cql::DropColumn& drop) {
                                if (drop.chunk_column) {
                                    (void)catalog_->ChangeChunkColumnsIfNeeded(alter.table, [&drop](const TableSchema& current) -> std::optional<TableSchema> {
                                        if (drop.if_exists && !HasColumn(current, drop.column)) return std::nullopt;
                                        return chunkdb::DropChunkColumn(current, drop.column);
                                    });
                                    return;
                                }
                                (void)catalog_->ChangeColumnsIfNeeded(alter.table, [&drop](const TableSchema& current) -> std::optional<TableSchema> {
                                    if (drop.if_exists && !HasColumn(current, drop.column)) return std::nullopt;
                                    return chunkdb::DropColumn(current, drop.column);
                                });
                            },
                            [&](const cql::RenameColumn& rename) {
                                if (rename.chunk_column) {
                                    catalog_->ChangeChunkColumns(alter.table, [&rename](const TableSchema& current) {
                                        return chunkdb::RenameChunkColumn(current, rename.column, rename.new_name);
                                    });
                                    return;
                                }
                                catalog_->ChangeColumns(alter.table, [&rename](const TableSchema& current) {
                                    return chunkdb::RenameColumn(current, rename.column, rename.new_name);
                                });
                            },
                            [&](const cql::AlterColumnType& change) {
                                std::optional<Conversion> conversion = change.conversion;
                                if (!conversion.has_value()) {
                                    // A type that holds every value changes at
                                    // once; a narrower one after checking them.
                                    const auto table = catalog_->Require(alter.table);
                                    const auto info = table->Info();
                                    const auto at = std::find_if(
                                        info.schema.columns.begin(), info.schema.columns.end(),
                                        [&change](const Column& column) { return column.name == change.column; });
                                    if (at == info.schema.columns.end()) {
                                        throw std::invalid_argument("the table has no column " + change.column);
                                    }
                                    if (!HoldsEveryValue(at->type, change.type)) {
                                        catalog_->NarrowColumn(alter.table, change.column, change.type);
                                        return;
                                    }
                                    conversion = Conversion::kExact;
                                }
                                catalog_->ChangeColumns(alter.table, [&](const TableSchema& current) {
                                    return ChangeColumnType(current, change.column, change.type, *conversion);
                                });
                            },
                            [&](const cql::SetOption& set) {
                                catalog_->SetOptions(alter.table, OptionsFrom({set.option}));
                            },
                            [&](const cql::AlterColumnAttribute& attribute) {
                                if (attribute.change == cql::AlterColumnAttribute::Change::kDropNull) {
                                    // Only after checking every stored value.
                                    catalog_->DropColumnNull(alter.table, attribute.column);
                                    return;
                                }
                                catalog_->ChangeColumns(alter.table, [&attribute](const TableSchema& current) {
                                    return ChangeColumnAttributes(current, attribute.column, AttributesFrom(current, attribute));
                                });
                            },
                            [&](const cql::RenameTable& rename) {
                                // Grants move with the table: the new name must be one the user
                                // administers, through its name, a matching prefix or `*`.
                                RequireCreateRight(session, rename.new_name);
                                std::stop_source cancelled;
                                const auto request_stop = [&] { cancelled.request_stop(); };
                                std::stop_callback shutdown(session.backup_cancelled, request_stop);
                                std::stop_callback statement(StatementStopToken(), request_stop);
                                catalog_->RenameTable(alter.table, rename.new_name, config_.users.get(), cancelled.get_token());
                            },
                            [&](const cql::AddCheck& add) {
                                // Only after evaluating it for every stored block.
                                catalog_->AddCheck(alter.table, [&add](const TableSchema& current) { return CheckFrom(add.check, current); });
                            },
                            [&](const cql::DropCheck& drop) {
                                (void)catalog_->ChangeColumnsIfNeeded(alter.table, [&drop](const TableSchema& current) -> std::optional<TableSchema> {
                                    if (drop.if_exists && !HasCheck(current, drop.name)) return std::nullopt;
                                    return chunkdb::DropCheck(current, drop.name);
                                });
                            },
                        },
                        alter.change);
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::DropTable& drop) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRight(session, drop.table, Right::kAdmin);
                    catalog_->Drop(drop.table, config_.users.get(), drop.if_exists);
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::RecoverTable& recover) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRight(session, recover.table, Right::kAdmin);
                    std::stop_source cancelled;
                    const auto request_stop = [&] { cancelled.request_stop(); };
                    std::stop_callback shutdown(session.backup_cancelled, request_stop);
                    std::stop_callback statement(StatementStopToken(), request_stop);
                    catalog_->RecoverTable(recover.table, cancelled.get_token());
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::CompactTombstones& compact) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRight(session, compact.table, Right::kAdmin);
                    std::string reply;
                    Protocol::AppendInteger(reply, catalog_->CompactTombstones(compact.table, compact.before));
                    return reply;
                },
                [&](const cql::ShowTableStats& stats) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    // MONITOR sees every table, as SHOW STATUS lists them.
                    if (!HasServerRight(session, ServerRight::kMonitor)) RequireRight(session, stats.table, Right::kRead);
                    std::string reply;
                    std::filesystem::path dir;
                    Table::FeedCounts feeds;
                    // The directory is listed after the lease ends: an exclusive section
                    // waiting for the lease would stop every statement on the table.
                    {
                        const auto lease = AcquireNamedTable(session, stats.table);
                        auto& store = lease.store();
                        feeds = session.table->CountFeeds(lease);
                        dir = store.data_dir();
                        Protocol::AppendMapHeader(reply, 11U);
                        Protocol::AppendBulk(reply, "epoch");
                        Protocol::AppendBulk(reply, StoreIdHex(store.store_id()));
                        Protocol::AppendBulk(reply, "revision");
                        if (const auto revision = store.CompletedRevision()) Protocol::AppendInteger(reply, *revision);
                        else Protocol::AppendNull(reply);
                        Protocol::AppendBulk(reply, "dims");
                        Protocol::AppendInteger(reply, static_cast<std::uint64_t>(store.geometry().dims()));
                        Protocol::AppendBulk(reply, "chunks");
                        Protocol::AppendInteger(reply, store.PopulatedChunkCount());
                        Protocol::AppendBulk(reply, "tombstones");
                        Protocol::AppendInteger(reply, store.RetainedTombstoneCount());
                        Protocol::AppendBulk(reply, "retained_deletion_floor");
                        Protocol::AppendInteger(reply, store.RetainedDeletionFloor());
                        Protocol::AppendBulk(reply, "loaded_chunks");
                        Protocol::AppendInteger(reply, static_cast<std::uint64_t>(store.ApproxLoadedChunkCount()));
                    }
                    if (auto* hook = hook_.load(std::memory_order_acquire))
                        hook->Run(CommandEngineTestHook::Point::kBeforeTableStatsListing, stats.table);
                    const auto files = DirectoryUsage(dir, stats.table);
                    Protocol::AppendBulk(reply, "files");
                    Protocol::AppendInteger(reply, files.files);
                    Protocol::AppendBulk(reply, "disk_bytes");
                    Protocol::AppendInteger(reply, files.bytes);
                    Protocol::AppendBulk(reply, "slots");
                    Protocol::AppendInteger(reply, static_cast<std::uint64_t>(feeds.slots));
                    Protocol::AppendBulk(reply, "watches");
                    Protocol::AppendInteger(reply, static_cast<std::uint64_t>(feeds.watches));
                    return reply;
                },
                [&](const cql::ShowTables&) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    // Only the tables the user has a right on, fenced ones too.
                    std::set<std::string> listed;
                    for (const auto& info : catalog_->List()) listed.insert(info.name);
                    for (const auto& fence : catalog_->Fences()) listed.insert(fence.table);
                    std::vector<std::string> names;
                    for (const auto& name : listed) {
                        if (RightOnTable(session, name).has_value()) {
                            names.push_back(name);
                        }
                    }
                    std::string reply;
                    Protocol::AppendArrayHeader(reply, names.size());
                    for (const auto& name : names) {
                        Protocol::AppendBulk(reply, name);
                    }
                    return reply;
                },
                [&](const cql::Describe& describe) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRight(session, describe.table, Right::kRead);
                    const auto fenced = [&]() -> std::optional<std::string> {
                        if (const auto fence = catalog_->Fence(describe.table); fence && fence->info)
                            return DescribeReply(*fence->info, nullptr, &fence->reason);
                        return std::nullopt;
                    };
                    if (auto reply = fenced()) return std::move(*reply);
                    const auto table = catalog_->Require(describe.table);
                    if (const auto lease = table->Acquire())
                        return DescribeReply(table->Info(), &lease->store(), nullptr);
                    // Fenced or dropped since the first look.
                    if (auto reply = fenced()) return std::move(*reply);
                    throw TableNotFoundError("table '" + describe.table + "' does not exist");
                },
                [&](const cql::FlushWal&) {
                    command_class = MetricsRegistry::CommandClass::kBarrier;
                    if (const User* user = CurrentUser(session); config_.require_auth &&
                        (user == nullptr || !HasAnyGrant(*user, Right::kWrite))) {
                        throw PermissionDeniedError("WRITE on a table");
                    }
                    // Every table: a connection may have written to several.
                    catalog_->WalBarrier();
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::Ping&) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    return Protocol::SimpleString("PONG");
                },
                [&](const cql::Backup& backup) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    if (config_.require_auth && !HasServerRight(session, ServerRight::kBackup)) {
                        if (const User* user = CurrentUser(session); user == nullptr || !user->manages_users)
                            throw PermissionDeniedError("BACKUP or MANAGES USERS");
                    }
                    if (config_.backup_dir.empty())
                        throw std::invalid_argument("BACKUP is disabled: set --backup-dir <path>");
                    const auto target = ResolveBackupTarget(config_.backup_dir, backup.path);
                    BackupOptions options;
                    std::stop_source cancelled;
                    const auto request_stop = [&] { cancelled.request_stop(); };
                    std::stop_callback shutdown(session.backup_cancelled, request_stop);
                    std::stop_callback statement(StatementStopToken(), request_stop);
                    options.cancelled = cancelled.get_token();
                    options.backup_directory = config_.backup_dir;
                    std::string reply;
                    options.before_publish = [&](const BackupResult& result) {
                        if (reserve_reply) {
                            if (result.tables.size() > (std::numeric_limits<std::size_t>::max() - 1024U) / 512U)
                                throw ReplyAdmissionError("backup reply bound overflows");
                            (*reserve_reply)(1024U + 512U * result.tables.size());
                        }
                        Protocol::AppendMapHeader(reply, 4);
                        Protocol::AppendBulk(reply, "tables");
                        Protocol::AppendInteger(reply, static_cast<std::uint64_t>(result.tables.size()));
                        Protocol::AppendBulk(reply, "files");
                        Protocol::AppendInteger(reply, result.files_count);
                        Protocol::AppendBulk(reply, "bytes");
                        Protocol::AppendInteger(reply, result.bytes);
                        Protocol::AppendBulk(reply, "cuts");
                        Protocol::AppendArrayHeader(reply, result.tables.size());
                        for (const auto& cut : result.tables) {
                            Protocol::AppendMapHeader(reply, 3);
                            Protocol::AppendBulk(reply, "table");
                            Protocol::AppendBulk(reply, cut.name);
                            Protocol::AppendBulk(reply, "epoch");
                            Protocol::AppendBulk(reply, StoreIdHex(cut.epoch));
                            Protocol::AppendBulk(reply, "revision");
                            Protocol::AppendInteger(reply, cut.revision);
                        }
                    };
                    try {
                        (void)catalog_->BackupTo(target, options);
                    } catch (const std::exception&) {
                        if (auto* hook = hook_.load(std::memory_order_acquire))
                            hook->Run(CommandEngineTestHook::Point::kAfterBackupAborted, {});
                        throw;
                    }
                    return reply;
                },
                [&](const cql::RestoreTable& restore) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    // MANAGES USERS or ADMIN on `*`: the backup may hold any table. BACKUP,
                    // the right BACKUP TO also takes, needs ADMIN on the new name as
                    // CREATE TABLE does: the restored table falls under its grants.
                    // Every check reads this one user: another CurrentUser call
                    // replaces it after a user change and frees it.
                    const User* user = CurrentUser(session);
                    if (auto* hook = hook_.load(std::memory_order_acquire))
                        hook->Run(CommandEngineTestHook::Point::kAfterRestoreUserRead, restore.name);
                    if (config_.require_auth && (user == nullptr || !user->manages_users) && !HasAdminOnEveryTable(user)) {
                        if (user == nullptr || !chunkdb::HasServerRight(*user, ServerRight::kBackup))
                            throw PermissionDeniedError("BACKUP, MANAGES USERS or ADMIN on *");
                        if (const auto right = RightOn(*user, restore.name); !right.has_value() || *right < Right::kAdmin)
                            throw PermissionDeniedError("ADMIN on " + restore.name);
                    }
                    if (config_.backup_dir.empty())
                        throw std::invalid_argument("RESTORE TABLE is disabled: set --backup-dir <path>");
                    TableRestoreRequest request;
                    request.backup = ResolveBackupSource(config_.backup_dir, restore.path);
                    request.table = restore.table;
                    request.name = restore.name;
                    std::stop_source cancelled;
                    const auto request_stop = [&] { cancelled.request_stop(); };
                    std::stop_callback shutdown(session.backup_cancelled, request_stop);
                    std::stop_callback statement(StatementStopToken(), request_stop);
                    request.cancelled = cancelled.get_token();
                    // SHOW CONNECTIONS lists the copy's progress.
                    request.progress = [&](std::uint64_t files, std::uint64_t bytes, std::uint64_t total_files,
                                           std::uint64_t total_bytes) {
                        ReportStatementProgress({.files_done = files, .files_total = total_files,
                            .bytes_done = bytes, .bytes_total = total_bytes});
                        if (auto* hook = hook_.load(std::memory_order_acquire))
                            hook->Run(CommandEngineTestHook::Point::kAfterRestoreProgress, restore.name);
                    };
                    const auto result = catalog_->RestoreTable(request);
                    std::string reply;
                    Protocol::AppendMapHeader(reply, 6);
                    Protocol::AppendBulk(reply, "table");
                    Protocol::AppendBulk(reply, result.name);
                    Protocol::AppendBulk(reply, "epoch");
                    Protocol::AppendBulk(reply, StoreIdHex(result.epoch));
                    Protocol::AppendBulk(reply, "parent_epoch");
                    Protocol::AppendBulk(reply, StoreIdHex(result.parent_epoch));
                    Protocol::AppendBulk(reply, "fork_revision");
                    Protocol::AppendInteger(reply, result.fork_revision);
                    Protocol::AppendBulk(reply, "files");
                    Protocol::AppendInteger(reply, result.files);
                    Protocol::AppendBulk(reply, "bytes");
                    Protocol::AppendInteger(reply, result.bytes);
                    return reply;
                },
                [&](const cql::SwapTables& swap) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    // Grants stay with names: each table moves under a name the user administers.
                    RequireRight(session, swap.first, Right::kAdmin);
                    RequireRight(session, swap.second, Right::kAdmin);
                    std::stop_source cancelled;
                    const auto request_stop = [&] { cancelled.request_stop(); };
                    std::stop_callback shutdown(session.backup_cancelled, request_stop);
                    std::stop_callback statement(StatementStopToken(), request_stop);
                    catalog_->SwapTables(swap.first, swap.second, cancelled.get_token());
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::Begin&) { return TxnBegin(session); },
                [&](const cql::Commit&) {
                    command_class = MetricsRegistry::CommandClass::kConditional;
                    if (info != nullptr && session.transaction != nullptr) {
                        info->chunks = session.transaction->writes.size();
                        info->table = session.transaction->table;
                    }
                    return TxnCommit(session);
                },
                [&](const cql::Rollback&) {
                    EnterStatementDecision();
                    session.transaction.reset();
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::ScanChunks& scan) {
                    command_class = MetricsRegistry::CommandClass::kScan;
                    RequireRight(session, scan.table, Right::kRead);
                    const std::uint64_t limit = scan.limit.value_or(kMaxChunkScanLimit);
                    if (limit == 0U || limit > kMaxChunkScanLimit) {
                        throw std::invalid_argument(
                            "LIMIT must be between 1 and " + std::to_string(kMaxChunkScanLimit));
                    }
                    const auto lease = AcquireNamedTable(session, scan.table);
                    const auto& geometry = lease.store().geometry();
                    if (scan.after) RequireDimensions(geometry, scan.dims);
                    if (!scan.with_data && !scan.columns.empty())
                        throw std::invalid_argument("SCAN COLUMNS requires WITH DATA");
                    const auto columns = ColumnsOf(geometry.layout(), scan.columns);
                    const ChunkCoord cursor =
                        scan.after.has_value() ? ChunkCoord{scan.after->first, scan.after->second, scan.after_z} : ChunkCoord{};
                    // A page with data retains chunk states up to the response byte limit.
                    if (reserve_reply && scan.with_data)
                        (*reserve_reply)(ChunkReplyCaptureBound(geometry.layout(),
                            ChunkReplyStateBytes(geometry, static_cast<std::size_t>(limit), 128U)));
                    // WHERE keeps the matching blocks of each identity. Without
                    // SINCE a chunk with none is left out; with SINCE it stays,
                    // without data, so a consumer drops its copy.
                    std::optional<BlockFilter> filter;
                    std::optional<ChunkScanFilter> scan_filter;
                    if (scan.where) {
                        filter.emplace(WhereFilter(geometry, *scan.where, parameters));
                        scan_filter.emplace(ChunkScanFilter{.keep = [&](const ChunkCoord& coord, ChunkRangeEntry& state) {
                            if (state.presence_bitmap.empty()) return scan.since.has_value();
                            if (filter->Apply(coord, state.payload, state.presence_bitmap, state.vars, state.chunk_columns.get()) == 0U) {
                                if (!scan.since) return false;
                                state.presence_bitmap.clear();
                                state.vars = ChunkVars{};
                                return true;
                            }
                            CanonicalizeAbsentBlocks(geometry, state.presence_bitmap, &state.payload);
                            std::vector<VarKey> absent;
                            for (const auto value : state.vars)
                                if (!BlockPresent(state.presence_bitmap, value.key.block_index)) absent.push_back(value.key);
                            for (const auto& key : absent) (void)state.vars.Remove(key);
                            return true;
                        }, .needs_vars = filter->needs_vars()});
                    }
                    ChunkIdentityScanPage page;
                    try {
                        page = lease.store().ScanChunkIdentities(scan.after.has_value(), cursor,
                            static_cast<std::size_t>(limit), scan.since, scan.with_data,
                            scan_filter ? &*scan_filter : nullptr);
                    } catch (const TombstoneHistoryLostError& error) {
                        const auto floor = std::to_string(error.retained_deletion_floor());
                        const auto epoch = StoreIdHex(lease.store().store_id());
                        if (const auto position = error.position())
                            return Protocol::Error("RESYNC_REQUIRED", "requested scan history is no longer retained",
                                {{"reason", "history_lost"}, {"floor", floor}, {"epoch", epoch}, {"current", std::to_string(*position)}});
                        return Protocol::Error("RESYNC_REQUIRED", "requested scan history is no longer retained",
                            {{"reason", "history_lost"}, {"floor", floor}, {"epoch", epoch}});
                    }
                    if (info != nullptr) info->chunks = page.entries.size();
                    ReplyParts reply;
                    // WHERE adds `after`, since a page can end with identities
                    // it left out.
                    Protocol::AppendMapHeader(reply.text(), scan.where ? 6U : 5U);
                    Protocol::AppendBulk(reply.text(), "chunks");
                    Protocol::AppendArrayHeader(reply.text(), page.entries.size());
                    for (const auto& entry : page.entries) {
                        CheckStatementCancellation();
                        Protocol::AppendArrayHeader(reply.text(), static_cast<std::size_t>(geometry.dims()) + 1U + (scan.with_data ? 1U : 0U));
                        Protocol::AppendInteger(reply.text(), entry.coord.x);
                        Protocol::AppendInteger(reply.text(), entry.coord.y);
                        if (geometry.dims() == 3U) Protocol::AppendInteger(reply.text(), entry.coord.z);
                        Protocol::AppendInteger(reply.text(), entry.version);
                        if (scan.with_data) {
                            if (entry.data) AppendChunkState(reply, geometry, *entry.data, columns, false);
                            else Protocol::AppendNull(reply.text());
                        }
                        if (reply.size() > kMaxChunkRangeResponseBytes)
                            throw std::invalid_argument("scan response exceeds its byte limit");
                    }
                    Protocol::AppendBulk(reply.text(), "more");
                    Protocol::AppendBoolean(reply.text(), page.has_more);
                    if (scan.where) {
                        Protocol::AppendBulk(reply.text(), "after");
                        if (!page.has_more || !page.after) Protocol::AppendNull(reply.text());
                        else {
                            Protocol::AppendArrayHeader(reply.text(), geometry.dims());
                            Protocol::AppendInteger(reply.text(), page.after->x);
                            Protocol::AppendInteger(reply.text(), page.after->y);
                            if (geometry.dims() == 3U) Protocol::AppendInteger(reply.text(), page.after->z);
                        }
                    }
                    Protocol::AppendBulk(reply.text(), "epoch");
                    Protocol::AppendBulk(reply.text(), StoreIdHex(lease.store().store_id()));
                    Protocol::AppendBulk(reply.text(), "position");
                    if (page.position) Protocol::AppendInteger(reply.text(), *page.position);
                    else Protocol::AppendNull(reply.text());
                    Protocol::AppendBulk(reply.text(), "retained_deletion_floor");
                    Protocol::AppendInteger(reply.text(), page.retained_deletion_floor);
                    return FinishReply(std::move(reply), continuation);
                },
                [&](const cql::CreateUser& create) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireManagesUsers(session);
                    if (create.if_not_exists)
                        config_.users->Create(create.user, [&] { return VerifierFrom(create.verifier, parameters); },
                                              create.manages_users, true, create.requires_channel_binding);
                    else config_.users->Create(create.user, VerifierFrom(create.verifier, parameters), create.manages_users,
                                               false, create.requires_channel_binding);
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::AlterUser& alter) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    if (alter.verifier.has_value() || alter.added_verifier.has_value() || alter.drop_old_verifier) {
                        const auto verifier = [&] {
                            return VerifierFrom(alter.verifier ? *alter.verifier : *alter.added_verifier, parameters);
                        };
                        // A user may change their own password: the incarnation
                        // it logged in as, not a user created again under its name.
                        if (config_.require_auth && alter.user == session.user) {
                            const std::uint64_t incarnation = session.user_incarnation;
                            const bool current = alter.verifier
                                ? config_.users->SetOwnVerifier(alter.user, incarnation, verifier())
                                : alter.added_verifier ? config_.users->AddOwnVerifier(alter.user, incarnation, verifier())
                                : config_.users->DropOwnOldVerifier(alter.user, incarnation);
                            if (!current) {
                                session.close_after_reply = true;
                                throw UserDroppedError();
                            }
                            return Protocol::SimpleString("OK");
                        }
                        RequireManagesUsers(session);
                        if (alter.verifier) config_.users->SetVerifier(alter.user, verifier());
                        else if (alter.added_verifier) config_.users->AddVerifier(alter.user, verifier());
                        else config_.users->DropOldVerifier(alter.user);
                    } else if (alter.max_connections || alter.statements_per_sec) {
                        RequireManagesUsers(session);
                        config_.users->SetLimits(alter.user, alter.max_connections, alter.statements_per_sec);
                    } else if (alter.requires_channel_binding.has_value()) {
                        RequireManagesUsers(session);
                        config_.users->SetRequiresChannelBinding(alter.user, *alter.requires_channel_binding);
                    } else {
                        RequireManagesUsers(session);
                        config_.users->SetManagesUsers(alter.user, *alter.manages_users);
                    }
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::DropUser& drop) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireManagesUsers(session);
                    config_.users->Drop(drop.user, drop.if_exists);
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::GrantRight& grant) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireManagesUsers(session);
                    // Inside catalog admission: a RENAME TO moving the
                    // table's grants sees this change before or after it.
                    catalog_->ChangeGrants([&] {
                        if (grant.revoke) {
                            config_.users->Revoke(grant.user, grant.table, grant.right, grant.server_rights);
                        } else {
                            config_.users->Grant(grant.user, grant.table, grant.right, grant.server_rights);
                        }
                    });
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::ShowUsers&) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireManagesUsers(session);
                    const Users users = config_.users->Snapshot();
                    std::string reply;
                    Protocol::AppendArrayHeader(reply, users.users.size());
                    for (const auto& [name, user] : users.users) {
                        Protocol::AppendMapHeader(reply, 6);
                        Protocol::AppendBulk(reply, "name");
                        Protocol::AppendBulk(reply, name);
                        Protocol::AppendBulk(reply, "manages_users");
                        Protocol::AppendBoolean(reply, user.manages_users);
                        Protocol::AppendBulk(reply, "requires_channel_binding");
                        Protocol::AppendBoolean(reply, user.requires_channel_binding);
                        Protocol::AppendBulk(reply, "verifiers");
                        Protocol::AppendInteger(reply, std::uint64_t{user.added_verifier ? 2U : 1U});
                        // Prefix grants as written: `p*`, in name order with the others.
                        std::map<std::string, Right> grants = user.grants;
                        for (const auto& [prefix, right] : user.prefix_grants) grants.emplace(prefix + "*", right);
                        Protocol::AppendBulk(reply, "grants");
                        Protocol::AppendMapHeader(reply, grants.size());
                        for (const auto& [table, right] : grants) {
                            Protocol::AppendBulk(reply, table);
                            Protocol::AppendBulk(reply, RightName(right));
                        }
                        constexpr std::array<std::pair<ServerRight, const char*>, 3> kServerRightNames{{
                            {ServerRight::kMonitor, "MONITOR"}, {ServerRight::kBackup, "BACKUP"},
                            {ServerRight::kReplication, "REPLICATION"}}};
                        std::size_t held = 0U;
                        for (const auto& [right, _] : kServerRightNames) held += chunkdb::HasServerRight(user, right) ? 1U : 0U;
                        Protocol::AppendBulk(reply, "server_rights");
                        Protocol::AppendArrayHeader(reply, held);
                        for (const auto& [right, label] : kServerRightNames)
                            if (chunkdb::HasServerRight(user, right)) Protocol::AppendBulk(reply, label);
                    }
                    return reply;
                },
                [&](const cql::EnableFeature& enable) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRightOnEveryTable(session, Right::kAdmin);
                    (void)catalog_->EnableFeature(enable.name);
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::ShowStatus&) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireMonitor(session);
                    std::string reply;
                    const auto uptime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started_);
                    const auto status = catalog_->FeatureStatus();
                    const auto flags = [&](const FeatureFlags& value) {
                        Protocol::AppendBulk(reply, "enabled");
                        std::size_t count = 0U;
                        for (const auto& feature : kFeatureRegistry)
                            if ((value.incompat & feature.incompat) != 0U) ++count;
                        Protocol::AppendArrayHeader(reply, count);
                        for (const auto& feature : kFeatureRegistry)
                            if ((value.incompat & feature.incompat) != 0U) Protocol::AppendBulk(reply, feature.name);
                        Protocol::AppendBulk(reply, "compat"); Protocol::AppendInteger(reply, static_cast<std::uint64_t>(value.compat));
                        Protocol::AppendBulk(reply, "ro_compat"); Protocol::AppendInteger(reply, static_cast<std::uint64_t>(value.ro_compat));
                        Protocol::AppendBulk(reply, "incompat"); Protocol::AppendInteger(reply, static_cast<std::uint64_t>(value.incompat));
                    };
                    const auto readable = static_cast<std::size_t>(std::count_if(status.tables.begin(), status.tables.end(),
                        [](const auto& table) { return table.features.has_value(); }));
                    Protocol::AppendMapHeader(reply, 7U);
                    Protocol::AppendBulk(reply, "version"); Protocol::AppendBulk(reply, config_.server_version);
                    Protocol::AppendBulk(reply, "uptime_ms"); Protocol::AppendInteger(reply, static_cast<std::uint64_t>(uptime.count()));
                    Protocol::AppendBulk(reply, "read_only"); Protocol::AppendBoolean(reply, catalog_->access_mode() == AccessMode::kReadOnly);
                    Protocol::AppendBulk(reply, "directory"); Protocol::AppendMapHeader(reply, 4U); flags(status.directory);
                    // Tables whose features can be read; `fenced` lists the others.
                    Protocol::AppendBulk(reply, "tables"); Protocol::AppendArrayHeader(reply, readable);
                    for (const auto& table : status.tables) {
                        if (!table.features) continue;
                        Protocol::AppendMapHeader(reply, 7U);
                        Protocol::AppendBulk(reply, "name"); Protocol::AppendBulk(reply, table.name);
                        Protocol::AppendBulk(reply, "state"); Protocol::AppendBulk(reply, table.fence ? "fenced" : "ok");
                        Protocol::AppendBulk(reply, "reason");
                        if (table.fence) Protocol::AppendBulk(reply, *table.fence); else Protocol::AppendNull(reply);
                        flags(*table.features);
                    }
                    Protocol::AppendBulk(reply, "fenced"); Protocol::AppendArrayHeader(reply, status.tables.size() - readable);
                    for (const auto& table : status.tables) {
                        if (table.features) continue;
                        if (!table.fence) throw std::logic_error("a table without features must be fenced");
                        Protocol::AppendMapHeader(reply, 2U);
                        Protocol::AppendBulk(reply, "name"); Protocol::AppendBulk(reply, table.name);
                        Protocol::AppendBulk(reply, "reason"); Protocol::AppendBulk(reply, *table.fence);
                    }
                    Protocol::AppendBulk(reply, "activation_pending"); Protocol::AppendBoolean(reply, status.activation_pending);
                    return reply;
                },
                [&](const cql::ShowMetrics&) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireMonitor(session);
                    return HandleMetrics();
                },
                [&](const cql::ReloadTls&) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRightOnEveryTable(session, Right::kAdmin);
                    std::lock_guard lock(tls_reload_mutex_);
                    if (!tls_reload_) {
                        throw std::invalid_argument("this server has no TLS listener to reload");
                    }
                    tls_reload_();
                    return Protocol::SimpleString("OK");
                },
            },
            parsed.statement);
    } catch (const TransactionConflictError& e) {
        // The transaction ended without writing anything. A COMMIT closed
        // it; after any other statement it answers CONFLICT until COMMIT or
        // ROLLBACK.
        std::string reply = ErrorReply(e);
        if (session.transaction != nullptr) {
            session.transaction->Abort(reply);
        }
        return reply;
    } catch (const std::exception& e) {
        return ErrorReply(e);
    }
}


SessionTransaction::SessionTransaction(TxnBudget* budget, std::chrono::steady_clock::time_point started)
    : budget(budget), started(started) {
    std::lock_guard lock(budget->mutex);
    entry = budget->open.insert(budget->open.end(), this);
}

SessionTransaction::~SessionTransaction() {
    std::lock_guard lock(budget->mutex);
    budget->open.erase(entry);
    budget->bytes.fetch_sub(bytes);
}

TransactionConflictError CommandEngine::TxnDurationConflict() const {
    return TransactionConflictError(
        TxnConflictReason::kDuration,
        "the transaction ran longer than " + std::to_string(config_.txn_max_duration.count()) + " ms");
}

bool CommandEngine::TxnPastDuration(const SessionTransaction& txn, std::chrono::steady_clock::time_point now) const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(now - txn.started) >= config_.txn_max_duration;
}

bool CommandEngine::TxnHoldsBudget(const SessionTransaction& txn) {
    // Without a snapshot it holds nothing: its next table statement or
    // COMMIT checks the duration.
    return txn.snapshot != nullptr && !txn.aborted.has_value();
}

bool CommandEngine::TxnStatementStart(SessionState& session) {
    SessionTransaction* const txn = session.transaction.get();
    if (txn == nullptr) {
        return false;
    }
    const bool expired = TxnPastDuration(*txn, std::chrono::steady_clock::now());
    std::string reply = expired ? ErrorReply(TxnDurationConflict()) : std::string{};
    // Destroyed after the mutex is released.
    SessionTransaction::Released released;
    // Ended under the mutex, like EndExpiredTransactions, so a write that
    // finds the budget full sees the bytes released once it holds it.
    std::lock_guard lock(txn_budget_.mutex);
    if (expired && TxnHoldsBudget(*txn)) {
        released = txn->Detach(std::move(reply));
    }
    txn->in_statement = true;
    return true;
}

void CommandEngine::TxnStatementDone(SessionState& session, bool started) noexcept {
    // COMMIT and ROLLBACK destroy the transaction, which unregisters it.
    if (!started || session.transaction == nullptr) {
        return;
    }
    std::lock_guard lock(txn_budget_.mutex);
    session.transaction->in_statement = false;
}

void CommandEngine::EndExpiredTransactions() {
    const auto now = std::chrono::steady_clock::now();
    const std::string reply = ErrorReply(TxnDurationConflict());
    // The snapshots and private copies of the ended transactions, destroyed
    // after the mutex is released.
    std::vector<SessionTransaction::Released> released;
    // While this holds the mutex no session starts a statement on its
    // transaction or destroys it.
    std::lock_guard lock(txn_budget_.mutex);
    for (SessionTransaction* txn : txn_budget_.open) {
        // A statement using the transaction changes it without the mutex:
        // check `in_statement` first.
        if (!txn->in_statement && TxnHoldsBudget(*txn) && TxnPastDuration(*txn, now)) {
            released.push_back(txn->Detach(reply));
        }
    }
}

std::string CommandEngine::TxnBegin(SessionState& session) {
    if (session.transaction != nullptr) {
        throw std::invalid_argument("a transaction is already open; COMMIT or ROLLBACK it first");
    }
    auto transaction = std::make_unique<SessionTransaction>(&txn_budget_, std::chrono::steady_clock::now());
    EnterStatementDecision();
    session.transaction = std::move(transaction);
    return Protocol::SimpleString("OK");
}

Table::Lease CommandEngine::TxnLease(SessionState& session, const std::string& table) {
    auto& txn = *session.transaction;
    if (!txn.table.empty() && txn.table != table) {
        throw std::invalid_argument("a transaction covers one table: this one covers " + txn.table);
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - txn.started);
    if (elapsed >= config_.txn_max_duration) {
        throw TxnDurationConflict();
    }
    if (txn.snapshot == nullptr) {
        auto lease = AcquireNamedTable(session, table);
        txn.snapshot = lease.store().BeginTxnSnapshot(config_.txn_max_duration - elapsed);
        txn.table = table;
        return lease;
    }
    const auto changed = [&table] {
        return TransactionConflictError(
            TxnConflictReason::kTableChanged, "table '" + table + "' was altered or dropped during the transaction");
    };
    std::optional<Table::Lease> lease;
    try {
        lease.emplace(AcquireNamedTable(session, table));
    } catch (const TableNotFoundError&) {
        throw changed();
    }
    if (!lease->store().OwnsTxnSnapshot(*txn.snapshot)) {
        throw changed();
    }
    return std::move(*lease);
}

void CommandEngine::TxnRead(SessionTransaction& txn, std::span<const ChunkCoord> coords) const {
    const auto limit = catalog_->query_limits().txn_max_read_chunks;
    const auto too_many = [&] {
        throw std::invalid_argument(
            "a transaction reads at most " + std::to_string(limit) + " chunks");
    };
    if (txn.read_set.size() > limit) too_many();
    std::set<ChunkCoord, ChunkCoordLess> additions;
    for (const auto& coord : coords) {
        if (txn.read_set.contains(coord) || additions.contains(coord)) continue;
        if (additions.size() >= limit - txn.read_set.size()) too_many();
        additions.insert(coord);
    }
    txn.read_set.merge(additions);
}

namespace {

[[nodiscard]] std::invalid_argument TxnWrittenChunksError(std::size_t limit) {
    return std::invalid_argument("a transaction writes at most " + std::to_string(limit) + " chunks");
}

[[nodiscard]] std::invalid_argument TxnMaxBytesError(std::size_t bytes, std::size_t limit) {
    return std::invalid_argument(
        "the transaction's changes would take " + std::to_string(bytes) + " bytes, more than --txn-max-bytes (" +
        std::to_string(limit) + ")");
}

[[nodiscard]] std::invalid_argument TxnTotalBytesError(std::size_t limit) {
    return std::invalid_argument(
        "open transactions hold more than --txn-total-bytes (" + std::to_string(limit) +
        ") of changes; try again later");
}

}  // namespace

void CommandEngine::RequireTxnRoom(
    const SessionTransaction& txn,
    const ChunkCoord& coord,
    std::size_t after) {
    const auto own = txn.writes.find(coord);
    const auto limit = catalog_->query_limits().txn_max_written_chunks;
    if (own == txn.writes.end() && txn.writes.size() >= limit) {
        throw TxnWrittenChunksError(limit);
    }
    const std::size_t before = own == txn.writes.end() ? 0U : StateBytes(own->second);
    if (txn.bytes - before + after > config_.txn_max_bytes) {
        throw TxnMaxBytesError(txn.bytes - before + after, config_.txn_max_bytes);
    }
    // TxnWrite reserves the growth; this only refuses one that cannot fit
    // now, after ending expired transactions as TxnWrite does.
    if (after > before) {
        const auto fits = [&] { return txn_budget_.bytes.load() + (after - before) <= config_.txn_total_bytes; };
        if (!fits()) {
            EndExpiredTransactions();
            if (!fits()) {
                throw TxnTotalBytesError(config_.txn_total_bytes);
            }
        }
    }
}

void CommandEngine::TxnWrite(
    SessionTransaction& txn,
    ChunkStore& store,
    const ChunkCoord& coord,
    const std::function<void(ChunkState&)>& change) {
    auto own = txn.writes.find(coord);
    const bool created = own == txn.writes.end();
    if (created) {
        const auto limit = catalog_->query_limits().txn_max_written_chunks;
        if (txn.writes.size() >= limit) {
            throw TxnWrittenChunksError(limit);
        }
        own = txn.writes.emplace(coord, store.ReadChunkStateAt(*txn.snapshot, coord)).first;
    }
    ChunkState& state = own->second;
    const bool was_written = state.written;
    const std::size_t before = created ? 0U : StateBytes(state);
    // The copy as it was, put back when the change or a limit throws.
    std::optional<ChunkState> saved;
    if (!created) {
        saved = state;
    }
    const auto undo = [&] {
        if (created) {
            txn.writes.erase(own);
        } else {
            state = std::move(*saved);
        }
    };
    try {
        EnterStatementDecision();
        change(state);
        state.written = state.written || was_written ||
            ChunkPopulated(store.geometry(), state.presence_bitmap, state.chunk_columns.get());
        const std::size_t after = StateBytes(state);
        if (txn.bytes - before + after > config_.txn_max_bytes) {
            throw TxnMaxBytesError(txn.bytes - before + after, config_.txn_max_bytes);
        }
        if (after > before) {
            const std::size_t grown = after - before;
            const auto charge = [&] {
                if (txn_budget_.bytes.fetch_add(grown) + grown <= config_.txn_total_bytes) {
                    return true;
                }
                txn_budget_.bytes.fetch_sub(grown);
                return false;
            };
            // Transactions past their duration hold their bytes until their
            // session's next statement; ending them may make room.
            if (!charge()) {
                if (auto* hook = hook_.load(std::memory_order_acquire))
                    hook->Run(CommandEngineTestHook::Point::kBeforeTxnBudgetReclaim, txn.table);
                EndExpiredTransactions();
                if (!charge()) {
                    throw TxnTotalBytesError(config_.txn_total_bytes);
                }
            }
        } else {
            txn_budget_.bytes.fetch_sub(before - after);
        }
        txn.bytes = txn.bytes - before + after;
    } catch (...) {
        undo();
        throw;
    }
}

std::string CommandEngine::BlockWriteWithExpressions(
    SessionState& session,
    const cql::SetBlock* set,
    const cql::DeleteBlock* del,
    std::span<const std::optional<std::string>> parameters,
    MetricsRegistry::CommandClass& command_class) {
    const auto& expressions = set != nullptr ? *set->expressions : *del->expressions;
    const std::string& table = set != nullptr ? set->table : del->table;
    std::optional<std::uint64_t> if_version = set != nullptr ? set->if_version : del->if_version;
    const std::int64_t x = set != nullptr ? set->x : del->x;
    const std::int64_t y = set != nullptr ? set->y : del->y;
    const std::int64_t z = set != nullptr ? set->z : del->z;
    if (expressions.version_parameter != 0U) {
        // IF VERSION $n: a u64 frame.
        Column version;
        version.type = ColumnType{.kind = ColumnKind::kUnsigned, .size = 64};
        const auto value = ParameterValue(version, expressions.version_parameter, parameters[expressions.version_parameter - 1U]);
        if (std::holds_alternative<std::monostate>(value)) throw std::invalid_argument("IF VERSION cannot be NULL");
        if_version = std::get<std::uint64_t>(value);
    }
    command_class = expressions.condition || if_version ? MetricsRegistry::CommandClass::kConditional
                                                        : MetricsRegistry::CommandClass::kPointWrite;
    // A condition reads the block and needs READ and WRITE (docs/DESIGN-2.0.md
    // E3); WRITE includes READ.
    RequireRight(session, table, Right::kWrite);
    RequireNoVersionInTxn(session, if_version);
    const auto lease = session.transaction != nullptr ? TxnLease(session, table) : AcquireNamedTable(session, table);
    ChunkStore& store = lease.store();
    const Geometry& geometry = store.geometry();
    RequireDimensions(geometry, set != nullptr ? set->dims : del->dims);
    const auto& layout = geometry.layout();
    const BlockWritePlan plan = PlanBlockWrite(expressions, geometry);
    const std::vector<ColumnValue> values = ExpressionParameters(plan, parameters);
    // Single values with IF <expr> are decoded as without it.
    std::vector<ColumnAssignment> single;
    if (set != nullptr && !plan.assignments) {
        single.reserve(set->values.size());
        for (const auto& assignment : set->values) {
            const Column& column = layout.schema().columns[RequireColumn(layout, assignment.column)];
            const auto* parameter = std::get_if<cql::Parameter>(&assignment.value);
            single.push_back(ColumnAssignment{
                .column = assignment.column,
                .value = parameter != nullptr ? ParameterValue(column, parameter->index, parameters[parameter->index - 1U])
                                              : LiteralValue(column, assignment.value),
            });
        }
    }
    bool version_mismatch = false;
    const auto decide = [&](const BlockWriteView& block, std::vector<ColumnAssignment>& out) {
        if (if_version && block.version != *if_version) {
            version_mismatch = true;
            return false;
        }
        const expr::Inputs inputs{
            .exists = block.values != nullptr,
            .block = block.values != nullptr ? std::span<const ColumnValue>(*block.values) : std::span<const ColumnValue>{},
            .chunk = block.chunk_columns,
            .x = x,
            .y = y,
            .z = z,
            .now = block.commit_time_ms,
            .parameters = values,
        };
        if (plan.condition && !expr::EvaluateCondition(*plan.condition, inputs)) return false;
        if (!plan.assignments) {
            out = std::move(single);
            return true;
        }
        auto computed = expr::EvaluateAssignments(*plan.assignments, inputs);
        const auto& targets = plan.assignments->targets();
        out.reserve(targets.size());
        for (std::size_t i = 0; i < targets.size(); ++i) {
            out.push_back(ColumnAssignment{.column = layout.schema().columns[targets[i].slot].name, .value = std::move(computed[i])});
        }
        return true;
    };

    if (session.transaction == nullptr) {
        const auto result =
            store.WriteBlockDecided(x, y, z, del != nullptr, plan.block_slots, plan.reads_chunk_columns, decide);
        if (version_mismatch) return Protocol::Error("VERSION_MISMATCH", "", {{"current", std::to_string(result.version)}});
        if (!result.ok) return ErrorReply(ConditionFailedError(result.version));
        std::string reply;
        Protocol::AppendInteger(reply, result.version);
        return reply;
    }
    // The commit time of a transaction is known only at COMMIT.
    if (plan.reads_now) throw std::invalid_argument("NOW() is not used in a transaction's writes; pass the time as a parameter");
    auto& txn = *session.transaction;
    const ChunkCoord coord = BlockCoord(geometry, x, y, z);
    // What the statement reads of the transaction's view counts as a read of
    // the chunk (E2, E3).
    if (plan.reads_chunk) TxnRead(txn, std::span<const ChunkCoord>(&coord, 1));
    // Decided against the transaction's own copy of the chunk, or the block
    // at the snapshot, before the chunk joins the transaction's writes: a
    // refused statement takes no write room and copies nothing.
    std::optional<std::vector<ColumnValue>> block;
    std::vector<ColumnValue> chunk_columns;
    std::uint64_t version = 0;
    if (const auto own = txn.writes.find(coord); own != txn.writes.end()) {
        const ChunkState& state = own->second;
        block = geometry.dims() == 3U ? store.BlockInState3D(state, x, y, z) : store.BlockInState(state, x, y);
        if (plan.reads_chunk_columns) chunk_columns = store.ChunkColumnValues(state.chunk_columns.get());
        version = state.version;
    } else {
        auto read = geometry.dims() == 3U ? store.GetBlockWithVersion3DAt(*txn.snapshot, x, y, z)
                                          : store.GetBlockWithVersionAt(*txn.snapshot, x, y);
        block = std::move(read.values);
        version = read.version.value_or(0U);
        if (plan.reads_chunk_columns) {
            chunk_columns = store.ChunkColumnValues(store.ReadChunkStateAt(*txn.snapshot, coord).chunk_columns.get());
        }
    }
    std::vector<ColumnAssignment> assignments;
    const BlockWriteView view{.values = block ? &*block : nullptr, .chunk_columns = chunk_columns, .version = version};
    if (!decide(view, assignments)) return ErrorReply(ConditionFailedError(version));
    TxnWrite(txn, store, coord, [&](ChunkState& state) {
        if (del != nullptr) {
            if (geometry.dims() == 3U) store.UnsetBlockInState3D(state, x, y, z);
            else store.UnsetBlockInState(state, x, y);
        } else if (geometry.dims() == 3U) {
            store.SetBlockInState3D(state, x, y, z, assignments);
        } else {
            store.SetBlockInState(state, x, y, assignments);
        }
    });
    return NullReply();
}

std::string CommandEngine::TxnCommit(SessionState& session) {
    if (session.transaction == nullptr) {
        throw std::invalid_argument("no transaction is open");
    }
    // COMMIT ends the transaction whatever it answers.
    const std::unique_ptr<SessionTransaction> txn = std::move(session.transaction);
    if (txn->snapshot == nullptr) {
        return NullReply();
    }
    if (!txn->writes.empty()) {
        RequireRight(session, txn->table, Right::kWrite);
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - txn->started);
    if (elapsed >= config_.txn_max_duration) {
        throw TxnDurationConflict();
    }
    std::optional<Table::Lease> lease;
    const auto changed = [&txn] {
        return TransactionConflictError(
            TxnConflictReason::kTableChanged, "table '" + txn->table + "' was altered or dropped during the transaction");
    };
    try {
        lease.emplace(AcquireNamedTable(session, txn->table));
    } catch (const TableNotFoundError&) {
        throw changed();
    }
    ChunkStore& store = lease->store();
    if (!store.OwnsTxnSnapshot(*txn->snapshot)) {
        throw changed();
    }
    std::vector<ChunkCoord> read_set;
    read_set.reserve(txn->read_set.size());
    for (const auto& coord : txn->read_set) {
        if (!txn->writes.contains(coord)) {
            read_set.push_back(coord);
        }
    }
    std::vector<TxnChunkWrite> writes;
    writes.reserve(txn->writes.size());
    for (auto& [coord, state] : txn->writes) {
        writes.push_back(TxnChunkWrite{.coord = coord, .state = std::move(state)});
    }
    const std::uint64_t version = store.CommitTransaction(*txn->snapshot, read_set, std::move(writes));
    if (version == 0) {
        return NullReply();
    }
    std::string reply;
    Protocol::AppendInteger(reply, version);
    return reply;
}

}  // namespace chunkdb
