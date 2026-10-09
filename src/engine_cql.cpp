// Protocol 3: CQL statements over the typed column API, with typed replies
// (docs/CQL.md).

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/schema.hpp"
#include "chunk_store_internal.hpp"
#include "cql.hpp"
#include "table_options_text.hpp"
#include "user_registry.hpp"

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

[[nodiscard]] std::uint64_t LoadLittleEndian(std::string_view bytes) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(bytes[i])) << (8U * i);
    }
    return value;
}

[[nodiscard]] std::string Describe(const Column& column) {
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

void AppendValue(std::string& out, const Column& column, const ColumnValue& value) {
    std::visit(
        Overloaded{
            [&](std::monostate) { Protocol::AppendNull(out); },
            [&](std::uint64_t v) { Protocol::AppendInteger(out, v); },
            [&](std::int64_t v) { Protocol::AppendInteger(out, v); },
            [&](bool v) { Protocol::AppendBoolean(out, v); },
            [&](float v) { Protocol::AppendFloat(out, v); },
            [&](double v) { Protocol::AppendDouble(out, v); },
            [&](const BitsValue&) {
                const auto bytes = EncodeColumnValue(column, value);
                Protocol::AppendBulk(
                    out,
                    std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size()));
            },
            [&](const std::string& v) { Protocol::AppendBulk(out, v); },
            [&](const BytesValue& v) {
                Protocol::AppendBulk(
                    out,
                    std::string_view(reinterpret_cast<const char*>(v.bytes.data()), v.bytes.size()));
            },
        },
        value);
}

[[nodiscard]] std::size_t RequireColumn(const ChunkLayout& layout, std::string_view name) {
    const std::size_t index = layout.FindColumn(name);
    if (index == std::string_view::npos) {
        throw std::invalid_argument("the table has no column " + std::string(name));
    }
    return index;
}

void AppendLittleEndian(std::string& out, std::uint64_t value, std::size_t bytes) {
    for (std::size_t i = 0; i < bytes; ++i) {
        out.push_back(static_cast<char>((value >> (8U * i)) & 0xffU));
    }
}

// The bytes of `fixed`'s values and validity bits in the payload: up to the
// next fixed-width column, or the end.
[[nodiscard]] std::pair<std::size_t, std::size_t> SectionOf(
    const ChunkLayout& layout,
    const ChunkLayout::FixedColumn& fixed) {
    const auto& all = layout.fixed_columns();
    const auto next = std::find_if(all.begin(), all.end(), [&fixed](const ChunkLayout::FixedColumn& other) {
        return other.values > fixed.values;
    });
    const std::size_t end = next == all.end() ? layout.payload_bytes() : next->values;
    return {fixed.values, end - fixed.values};
}

// Version and schema version, both u64, open every chunk form.
constexpr std::size_t kChunkFormHeaderBytes = 16;

// The chunk form of docs/CQL.md: version u64, the schema version u64 its
// layout follows, the presence bitmap,
// for each of `columns` (indexes into the schema) its section of the
// payload, then the VARS entries of the text and bytes columns among them.
// `columns` empty means every column, which is the payload and the VARS
// section as they are.
[[nodiscard]] std::string EncodeChunkForm(
    const ChunkLayout& layout,
    std::uint64_t version,
    const std::vector<std::uint8_t>& payload,
    const std::vector<std::uint8_t>& presence,
    const ChunkVars& vars,
    const std::vector<std::size_t>& columns) {
    const auto bytes_of = [](const std::vector<std::uint8_t>& data, std::size_t offset, std::size_t size) {
        return std::string_view(reinterpret_cast<const char*>(data.data()) + offset, size);
    };
    std::string form;
    AppendLittleEndian(form, version, 8);
    AppendLittleEndian(form, layout.schema().version, 8);
    form += bytes_of(presence, 0, presence.size());
    if (columns.empty()) {
        form += bytes_of(payload, 0, payload.size());
        form += bytes_of(vars.Encode(), 0, vars.Encode().size());
        return form;
    }
    std::vector<std::uint32_t> var_ids;
    for (const std::size_t index : columns) {
        if (const auto* fixed = layout.FixedColumnAt(index); fixed != nullptr) {
            const auto [offset, size] = SectionOf(layout, *fixed);
            form += bytes_of(payload, offset, size);
        } else {
            var_ids.push_back(layout.schema().columns[index].id);
        }
    }
    for (const auto entry : vars) {
        if (std::find(var_ids.begin(), var_ids.end(), entry.key.column_id) == var_ids.end()) {
            continue;
        }
        AppendLittleEndian(form, entry.key.column_id, 4);
        AppendLittleEndian(form, entry.key.block_index, 4);
        AppendLittleEndian(form, entry.value.size(), 4);
        form.append(reinterpret_cast<const char*>(entry.value.data()), entry.value.size());
    }
    return form;
}

// The most bytes a chunk form of every column takes.
[[nodiscard]] std::size_t ChunkFormBytes(const Geometry& geometry, std::size_t var_max_chunk_bytes) {
    return kChunkFormHeaderBytes + ChunkPresenceBitmapBytes(geometry) + geometry.ChunkPayloadBytes() +
           var_max_chunk_bytes;
}

// The schema version a chunk form of every column was encoded for.
[[nodiscard]] std::uint64_t ChunkFormSchemaVersion(std::string_view form) {
    if (form.size() < kChunkFormHeaderBytes) {
        throw std::invalid_argument(
            "the chunk starts with its version and schema version (16 bytes), got " + std::to_string(form.size()));
    }
    return LoadLittleEndian(form.substr(8, 8));
}

// The state a chunk form of every column sends, for a layout of the schema
// version it was encoded for; its version is not used.
[[nodiscard]] ChunkState DecodeChunkForm(const Geometry& geometry, std::string_view form) {
    const std::size_t presence_bytes = ChunkPresenceBitmapBytes(geometry);
    const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
    const std::size_t header = kChunkFormHeaderBytes;
    if (form.size() < header + presence_bytes + payload_bytes) {
        throw std::invalid_argument(
            "the chunk takes at least " + std::to_string(header + presence_bytes + payload_bytes) +
            " bytes (version, schema version, presence, payload), got " + std::to_string(form.size()));
    }
    const auto* data = reinterpret_cast<const std::uint8_t*>(form.data());
    ChunkState state;
    state.presence_bitmap.assign(data + header, data + header + presence_bytes);
    state.payload.assign(data + header + presence_bytes, data + header + presence_bytes + payload_bytes);
    const std::size_t vars_offset = header + presence_bytes + payload_bytes;
    state.vars = ChunkVars::Decode(data + vars_offset, form.size() - vars_offset, geometry.ChunkBlockCount());
    return state;
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
    const ColumnValue value = LiteralValue(column, *definition.default_value);
    if (std::holds_alternative<std::monostate>(value)) {
        // A NULL column without a DEFAULT takes NULL.
        if (!column.nullable) {
            throw std::invalid_argument(Describe(column) + " cannot be NULL, so its DEFAULT cannot be NULL");
        }
        return column;
    }
    column.has_default = true;
    column.default_value =
        IsFixedWidth(column.type.kind) ? EncodeColumnValue(column, value) : EncodeVarValue(column, value);
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

// The value a column's encoded DEFAULT holds.
[[nodiscard]] ColumnValue DefaultOf(const Column& column) {
    if (IsFixedWidth(column.type.kind)) {
        return DecodeColumnValue(column, column.default_value.data());
    }
    return DecodeVarValue(column, column.default_value);
}

// DESCRIBE: a map of the table's name, schema version, columns, chunk and
// large-chunk sizes, and options.
[[nodiscard]] std::string DescribeReply(const TableInfo& info) {
    std::string reply;
    const auto key = [&reply](std::string_view name) { Protocol::AppendBulk(reply, name); };
    const auto pair = [&reply](std::uint64_t first, std::uint64_t second) {
        Protocol::AppendArrayHeader(reply, 2);
        Protocol::AppendInteger(reply, first);
        Protocol::AppendInteger(reply, second);
    };
    Protocol::AppendMapHeader(reply, 6);
    key("table");
    Protocol::AppendBulk(reply, info.name);
    key("version");
    Protocol::AppendInteger(reply, info.schema.version);
    key("columns");
    Protocol::AppendArrayHeader(reply, info.schema.columns.size());
    for (const auto& column : info.schema.columns) {
        Protocol::AppendMapHeader(reply, 6);
        key("id");
        Protocol::AppendInteger(reply, static_cast<std::uint64_t>(column.id));
        key("name");
        Protocol::AppendBulk(reply, column.name);
        key("type");
        Protocol::AppendBulk(reply, ColumnTypeName(column.type));
        key("null");
        Protocol::AppendBoolean(reply, column.nullable);
        key("required");
        Protocol::AppendBoolean(reply, column.required);
        key("default");
        if (column.has_default) {
            AppendValue(reply, column, DefaultOf(column));
        } else {
            Protocol::AppendNull(reply);
        }
    }
    key("chunk");
    pair(info.geometry.chunk_width_blocks, info.geometry.chunk_height_blocks);
    key("large");
    pair(info.geometry.large_chunk_width_chunks, info.geometry.large_chunk_height_chunks);
    key("options");
    Protocol::AppendMapHeader(reply, 6);
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

// The most bytes of a verifier sent as a parameter.
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
    return scram::ParseVerifier(std::get<cql::Text>(value).value);
}

[[nodiscard]] std::string VersionReply(const ChunkMutationResult& result) {
    if (!result.ok) {
        return Protocol::Error("VERSION_MISMATCH", "current=" + std::to_string(result.version));
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

[[nodiscard]] bool AllowedInTransaction(const cql::Statement& statement) {
    return std::holds_alternative<cql::GetBlock>(statement) || std::holds_alternative<cql::SetBlock>(statement) ||
           std::holds_alternative<cql::DeleteBlock>(statement) || std::holds_alternative<cql::GetChunk>(statement) ||
           std::holds_alternative<cql::SetChunk>(statement) || std::holds_alternative<cql::GetArea>(statement) ||
           std::holds_alternative<cql::Describe>(statement) || std::holds_alternative<cql::Ping>(statement) ||
           std::holds_alternative<cql::Begin>(statement) ||
           std::holds_alternative<cql::Commit>(statement) || std::holds_alternative<cql::Rollback>(statement);
}

[[nodiscard]] std::size_t StateBytes(const ChunkState& state) {
    return state.payload.size() + state.presence_bitmap.size() + state.vars.encoded_size();
}

}  // namespace

const User* CommandEngine::CurrentUser(SessionState& session) const {
    if (!config_.require_auth) {
        return nullptr;
    }
    // One atomic read while nothing changed; a changed or dropped user is
    // read again.
    const std::uint64_t generation = config_.users->Generation();
    if (session.user_generation != generation) {
        auto found = config_.users->Find(session.user);
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
        session.table = catalog_->Find(name);
        if (session.table == nullptr) {
            throw not_found();
        }
    }
    if (auto lease = session.table->Acquire(); lease.has_value()) {
        return std::move(*lease);
    }
    // Dropped since the last statement; a table of that name may exist
    // again.
    session.table = catalog_->Find(name);
    if (session.table == nullptr) {
        throw not_found();
    }
    auto lease = session.table->Acquire();
    if (!lease.has_value()) {
        throw not_found();
    }
    return std::move(*lease);
}

CommandEngine::PayloadRequest CommandEngine::PlanParameters(SessionState& session, std::string_view line) const {
    PayloadRequest request;
    if (!cql::MayHaveParameters(line)) {
        return request;
    }
    // Frames follow a line with parameters; if their lengths cannot be
    // bounded, they cannot be told apart from the next statement.
    const auto reject = [&request](std::string response) {
        request.plan = PayloadPlan::kReject;
        request.reject_response = std::move(response);
        return request;
    };
    cql::Parsed parsed;
    try {
        parsed = cql::Parse(StatementOf(line));
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
    if (std::holds_alternative<cql::CreateUser>(parsed.statement) ||
        std::holds_alternative<cql::AlterUser>(parsed.statement)) {
        request.parameter_limits = {kMaxVerifierBytes};
        request.plan = PayloadPlan::kParameters;
        return request;
    }
    if (const auto* chunk = std::get_if<cql::SetChunk>(&parsed.statement); chunk != nullptr) {
        const std::shared_ptr<Table> table = find(chunk->table);
        if (table == nullptr) {
            return reject(Protocol::Error("NO_TABLE", "table '" + chunk->table + "' does not exist"));
        }
        const auto info = table->Info();
        request.parameter_limits = {ChunkFormBytes(table->geometry(), info.options.var_max_chunk_bytes)};
        request.plan = PayloadPlan::kParameters;
        return request;
    }
    const auto* set = std::get_if<cql::SetBlock>(&parsed.statement);
    if (set == nullptr) {
        return reject(Protocol::Error("UNKNOWN_COMMAND", "this statement is not available yet"));
    }
    const std::shared_ptr<Table> table = find(set->table);
    if (table == nullptr) {
        return reject(Protocol::Error("NO_TABLE", "table '" + set->table + "' does not exist"));
    }
    // The statement checks the values again under its lease, so a column
    // that changes in between is not trusted.
    const Geometry geometry = table->geometry();
    const auto& layout = geometry.layout();
    request.parameter_limits.assign(parsed.parameters, 0);
    for (const auto& assignment : set->values) {
        const auto* parameter = std::get_if<cql::Parameter>(&assignment.value);
        if (parameter == nullptr) {
            continue;
        }
        const std::size_t index = layout.FindColumn(assignment.column);
        if (index == std::string_view::npos) {
            return reject(Protocol::Error("INVALID_ARGUMENT", "the table has no column " + assignment.column));
        }
        request.parameter_limits[parameter->index - 1U] = ParameterBytes(layout.schema().columns[index].type);
    }
    request.plan = PayloadPlan::kParameters;
    return request;
}

std::string CommandEngine::ExecuteStatement(
    SessionState& session,
    std::string_view line,
    std::span<const std::optional<std::string>> parameters,
    MetricsRegistry::CommandClass& command_class) {
    try {
        cql::Parsed parsed;
        try {
            parsed = cql::Parse(StatementOf(line));
        } catch (const cql::ParseError& e) {
            return Protocol::Error("SYNTAX", e.what());
        }
        if (parameters.size() != parsed.parameters) {
            return Protocol::Error(
                "PROTOCOL",
                "the statement takes " + std::to_string(parsed.parameters) + " parameters, got " +
                    std::to_string(parameters.size()));
        }
        if (session.transaction != nullptr && session.transaction->aborted.has_value()) {
            // Ended by a conflict: only COMMIT or ROLLBACK closes it.
            std::string reply = *session.transaction->aborted;
            if (std::holds_alternative<cql::Rollback>(parsed.statement)) {
                reply = Protocol::SimpleString("OK");
            }
            if (std::holds_alternative<cql::Rollback>(parsed.statement) ||
                std::holds_alternative<cql::Commit>(parsed.statement)) {
                session.transaction.reset();
            }
            return reply;
        }
        if (session.transaction != nullptr && !AllowedInTransaction(parsed.statement)) {
            return Protocol::Error(
                "INVALID_ARGUMENT",
                "inside a transaction only GET, SET and DELETE statements, DESCRIBE, PING, COMMIT and ROLLBACK run");
        }
        return std::visit(
            Overloaded{
                [&](const cql::GetBlock& get) {
                    command_class = MetricsRegistry::CommandClass::kPointRead;
                    RequireRight(session, get.table, Right::kRead);
                    const auto lease = session.transaction != nullptr ? TxnLease(session, get.table)
                                                                      : AcquireNamedTable(session, get.table);
                    ChunkStore& store = lease.store();
                    const auto& layout = store.geometry().layout();
                    std::vector<std::size_t> columns = ColumnsOf(layout, get.columns);
                    if (columns.empty()) {
                        columns.resize(layout.schema().columns.size());
                        for (std::size_t i = 0; i < columns.size(); ++i) {
                            columns[i] = i;
                        }
                    }
                    std::optional<std::vector<ColumnValue>> values;
                    if (session.transaction != nullptr) {
                        auto& txn = *session.transaction;
                        const ChunkCoord coord = store.geometry().BlockToChunk(get.x, get.y);
                        TxnRead(txn, std::span<const ChunkCoord>(&coord, 1));
                        const auto own = txn.writes.find(coord);
                        values = own != txn.writes.end() ? store.BlockInState(own->second, get.x, get.y)
                                                         : store.GetBlockAt(*txn.snapshot, get.x, get.y);
                    } else {
                        values = store.GetBlock(get.x, get.y);
                    }
                    std::string reply;
                    if (!values.has_value()) {
                        Protocol::AppendNull(reply);
                        return reply;
                    }
                    Protocol::AppendArrayHeader(reply, columns.size());
                    for (const std::size_t index : columns) {
                        AppendValue(reply, layout.schema().columns[index], (*values)[index]);
                    }
                    return reply;
                },
                [&](const cql::SetBlock& set) {
                    command_class = set.if_version.has_value() ? MetricsRegistry::CommandClass::kConditional
                                                               : MetricsRegistry::CommandClass::kPointWrite;
                    RequireRight(session, set.table, Right::kWrite);
                    RequireNoVersionInTxn(session, set.if_version);
                    const auto lease = session.transaction != nullptr ? TxnLease(session, set.table)
                                                                      : AcquireNamedTable(session, set.table);
                    ChunkStore& store = lease.store();
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
                            *session.transaction, store, store.geometry().BlockToChunk(set.x, set.y),
                            [&](ChunkState& state) { store.SetBlockInState(state, set.x, set.y, values); });
                        return NullReply();
                    }
                    return VersionReply(store.SetBlock(set.x, set.y, values, set.if_version));
                },
                [&](const cql::DeleteBlock& del) {
                    command_class = del.if_version.has_value() ? MetricsRegistry::CommandClass::kConditional
                                                               : MetricsRegistry::CommandClass::kPointWrite;
                    RequireRight(session, del.table, Right::kWrite);
                    RequireNoVersionInTxn(session, del.if_version);
                    if (session.transaction != nullptr) {
                        const auto lease = TxnLease(session, del.table);
                        ChunkStore& store = lease.store();
                        TxnWrite(
                            *session.transaction, store, store.geometry().BlockToChunk(del.x, del.y),
                            [&](ChunkState& state) { store.UnsetBlockInState(state, del.x, del.y); });
                        return NullReply();
                    }
                    const auto lease = AcquireNamedTable(session, del.table);
                    return VersionReply(lease.store().UnsetBlock(del.x, del.y, del.if_version));
                },
                [&](const cql::GetChunk& get) {
                    command_class = MetricsRegistry::CommandClass::kChunkRead;
                    RequireRight(session, get.table, Right::kRead);
                    const auto lease = session.transaction != nullptr ? TxnLease(session, get.table)
                                                                      : AcquireNamedTable(session, get.table);
                    ChunkStore& store = lease.store();
                    const auto& layout = store.geometry().layout();
                    const auto columns = ColumnsOf(layout, get.columns);
                    ChunkState state;
                    if (session.transaction != nullptr) {
                        auto& txn = *session.transaction;
                        const ChunkCoord coord{get.chunk_x, get.chunk_y};
                        TxnRead(txn, std::span<const ChunkCoord>(&coord, 1));
                        const auto own = txn.writes.find(coord);
                        state = own != txn.writes.end() ? own->second
                                                        : store.ReadChunkStateAt(*txn.snapshot, get.chunk_x, get.chunk_y);
                    } else {
                        state = store.ReadChunkState(get.chunk_x, get.chunk_y);
                    }
                    std::string reply;
                    Protocol::AppendBulk(
                        reply,
                        EncodeChunkForm(layout, state.version, state.payload, state.presence_bitmap, state.vars, columns));
                    return reply;
                },
                [&](const cql::SetChunk& set) {
                    command_class = set.if_version.has_value() ? MetricsRegistry::CommandClass::kConditional
                                                               : MetricsRegistry::CommandClass::kChunkWrite;
                    RequireRight(session, set.table, Right::kWrite);
                    RequireNoVersionInTxn(session, set.if_version);
                    const auto lease = session.transaction != nullptr ? TxnLease(session, set.table)
                                                                      : AcquireNamedTable(session, set.table);
                    ChunkStore& store = lease.store();
                    const auto& frame = parameters[set.state.index - 1U];
                    if (!frame.has_value()) {
                        throw std::invalid_argument("the chunk cannot be NULL; DELETE its blocks instead");
                    }
                    // A form encoded for another schema version would put its
                    // bytes in the wrong columns.
                    const std::uint64_t encoded_for = ChunkFormSchemaVersion(*frame);
                    const std::uint64_t current = store.geometry().layout().schema().version;
                    if (encoded_for != current) {
                        return Protocol::Error(
                            "SCHEMA_MISMATCH",
                            "current=" + std::to_string(current) + " the chunk was encoded for schema version " +
                                std::to_string(encoded_for) + "; DESCRIBE the table and encode it again");
                    }
                    if (session.transaction != nullptr) {
                        ChunkState next = DecodeChunkForm(store.geometry(), *frame);
                        TxnWrite(
                            *session.transaction, store, ChunkCoord{set.chunk_x, set.chunk_y},
                            [&](ChunkState& state) {
                                store.PrepareTxnChunkState(next, state.vars);
                                // The version stays the snapshot's until COMMIT.
                                next.version = state.version;
                                state = std::move(next);
                            });
                        return NullReply();
                    }
                    return VersionReply(store.WriteChunkState(
                        set.chunk_x, set.chunk_y, DecodeChunkForm(store.geometry(), *frame), set.if_version));
                },
                [&](const cql::GetArea& get) {
                    command_class = MetricsRegistry::CommandClass::kRange;
                    RequireRight(session, get.table, Right::kRead);
                    const auto lease = session.transaction != nullptr ? TxnLease(session, get.table)
                                                                      : AcquireNamedTable(session, get.table);
                    ChunkStore& store = lease.store();
                    const auto& layout = store.geometry().layout();
                    const auto columns = ColumnsOf(layout, get.columns);
                    std::vector<ChunkRangeEntry> entries;
                    if (session.transaction != nullptr) {
                        auto& txn = *session.transaction;
                        std::vector<ChunkCoord> covered;
                        const TxnAreaRead read{
                            .overlay = [&txn](const ChunkCoord& coord) -> const ChunkState* {
                                const auto own = txn.writes.find(coord);
                                return own != txn.writes.end() ? &own->second : nullptr;
                            },
                            .covered = &covered,
                        };
                        entries = get.around
                                      ? store.ReadChunkRadiusAt(*txn.snapshot, get.x0, get.y0, get.radius, true, read)
                                      : store.ReadChunkRangeAt(*txn.snapshot, get.x0, get.y0, get.x1, get.y1, true, read);
                        TxnRead(txn, covered);
                    } else {
                        entries = get.around ? store.ReadChunkRadius(get.x0, get.y0, get.radius, true)
                                             : store.ReadChunkRange(get.x0, get.y0, get.x1, get.y1, true);
                    }
                    std::string reply;
                    Protocol::AppendArrayHeader(reply, entries.size());
                    for (const auto& entry : entries) {
                        Protocol::AppendArrayHeader(reply, 3);
                        Protocol::AppendInteger(reply, entry.coord.x);
                        Protocol::AppendInteger(reply, entry.coord.y);
                        Protocol::AppendBulk(
                            reply,
                            EncodeChunkForm(layout, entry.version, entry.payload, entry.presence_bitmap, entry.vars, columns));
                    }
                    return reply;
                },
                [&](const cql::CreateTable& create) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRightOnEveryTable(session, Right::kAdmin);
                    TableSchema schema{
                        .version = 1,
                        .next_column_id = static_cast<std::uint32_t>(create.columns.size() + 1U),
                        .columns = {},
                        .history = {},
                        .pending = std::nullopt,
                    };
                    for (std::size_t i = 0; i < create.columns.size(); ++i) {
                        schema.columns.push_back(ColumnFrom(create.columns[i], static_cast<std::uint32_t>(i + 1U)));
                    }
                    GeometryConfig geometry;
                    geometry.chunk_width_blocks = create.chunk_width;
                    geometry.chunk_height_blocks = create.chunk_height;
                    if (create.large.has_value()) {
                        geometry.large_chunk_width_chunks = create.large->first;
                        geometry.large_chunk_height_chunks = create.large->second;
                    }
                    geometry.block_bits = FixedBitsPerBlock(schema);
                    const auto options = OptionsFrom(create.options).ApplyTo(catalog_->default_options());
                    (void)catalog_->Create(create.table, geometry, options, schema);
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::AlterTable& alter) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRight(session, alter.table, Right::kAdmin);
                    std::visit(
                        Overloaded{
                            [&](const cql::AddColumn& add) {
                                catalog_->ChangeColumns(alter.table, [&add](const TableSchema& current) {
                                    return chunkdb::AddColumn(current, ColumnFrom(add.column, current.next_column_id));
                                });
                            },
                            [&](const cql::DropColumn& drop) {
                                catalog_->ChangeColumns(alter.table, [&drop](const TableSchema& current) {
                                    return chunkdb::DropColumn(current, drop.column);
                                });
                            },
                            [&](const cql::RenameColumn& rename) {
                                catalog_->ChangeColumns(alter.table, [&rename](const TableSchema& current) {
                                    return chunkdb::RenameColumn(current, rename.column, rename.new_name);
                                });
                            },
                            [&](const cql::AlterColumnType& change) {
                                std::optional<Conversion> conversion = change.conversion;
                                if (!conversion.has_value()) {
                                    // A type that holds every value changes at
                                    // once; a narrower one after checking them.
                                    const auto table = catalog_->Find(alter.table);
                                    if (table == nullptr) {
                                        throw TableNotFoundError("table '" + alter.table + "' does not exist");
                                    }
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
                        },
                        alter.change);
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::DropTable& drop) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRight(session, drop.table, Right::kAdmin);
                    catalog_->Drop(drop.table);
                    if (config_.users != nullptr) {
                        // A table created later under this name starts without them.
                        config_.users->ForgetTable(drop.table);
                    }
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::ShowTables&) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    // Only the tables the user has a right on.
                    std::vector<std::string> names;
                    for (const auto& info : catalog_->List()) {
                        if (RightOnTable(session, info.name).has_value()) {
                            names.push_back(info.name);
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
                    const auto table = catalog_->Find(describe.table);
                    if (table == nullptr) {
                        throw TableNotFoundError("table '" + describe.table + "' does not exist");
                    }
                    return DescribeReply(table->Info());
                },
                [&](const cql::FlushWal&) {
                    command_class = MetricsRegistry::CommandClass::kBarrier;
                    if (const User* user = CurrentUser(session); config_.require_auth &&
                        (user == nullptr ||
                         std::none_of(user->grants.begin(), user->grants.end(), [](const auto& grant) {
                             return grant.second >= Right::kWrite;
                         }))) {
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
                [&](const cql::Begin&) { return TxnBegin(session); },
                [&](const cql::Commit&) {
                    command_class = MetricsRegistry::CommandClass::kConditional;
                    return TxnCommit(session);
                },
                [&](const cql::Rollback&) {
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
                    const ChunkCoord cursor =
                        scan.after.has_value() ? ChunkCoord{scan.after->first, scan.after->second} : ChunkCoord{0, 0};
                    const auto page = lease.store().ScanPopulatedChunks(
                        scan.after.has_value(), cursor, static_cast<std::size_t>(limit));
                    // A map: the chunks, and whether more follow the last one.
                    std::string reply;
                    Protocol::AppendMapHeader(reply, 2);
                    Protocol::AppendBulk(reply, "chunks");
                    Protocol::AppendArrayHeader(reply, page.coords.size());
                    for (const auto& coord : page.coords) {
                        Protocol::AppendArrayHeader(reply, 2);
                        Protocol::AppendInteger(reply, coord.x);
                        Protocol::AppendInteger(reply, coord.y);
                    }
                    Protocol::AppendBulk(reply, "more");
                    Protocol::AppendBoolean(reply, page.has_more);
                    return reply;
                },
                [&](const cql::CreateUser& create) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireManagesUsers(session);
                    config_.users->Create(create.user, VerifierFrom(create.verifier, parameters), create.manages_users);
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::AlterUser& alter) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    if (alter.verifier.has_value()) {
                        // A user may change their own password.
                        if (!(config_.require_auth && alter.user == session.user)) {
                            RequireManagesUsers(session);
                        }
                        if (config_.users == nullptr) {
                            throw std::invalid_argument("this server runs without users (--auth none)");
                        }
                        config_.users->SetVerifier(alter.user, VerifierFrom(*alter.verifier, parameters));
                    } else {
                        RequireManagesUsers(session);
                        config_.users->SetManagesUsers(alter.user, *alter.manages_users);
                    }
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::DropUser& drop) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireManagesUsers(session);
                    config_.users->Drop(drop.user);
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::GrantRight& grant) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireManagesUsers(session);
                    if (grant.revoke) {
                        config_.users->Revoke(grant.user, grant.table, grant.right);
                    } else {
                        config_.users->Grant(grant.user, grant.table, grant.right);
                    }
                    return Protocol::SimpleString("OK");
                },
                [&](const cql::ShowUsers&) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireManagesUsers(session);
                    const Users users = config_.users->Snapshot();
                    std::string reply;
                    Protocol::AppendArrayHeader(reply, users.users.size());
                    for (const auto& [name, user] : users.users) {
                        Protocol::AppendMapHeader(reply, 3);
                        Protocol::AppendBulk(reply, "name");
                        Protocol::AppendBulk(reply, name);
                        Protocol::AppendBulk(reply, "manages_users");
                        Protocol::AppendBoolean(reply, user.manages_users);
                        Protocol::AppendBulk(reply, "grants");
                        Protocol::AppendMapHeader(reply, user.grants.size());
                        for (const auto& [table, right] : user.grants) {
                            Protocol::AppendBulk(reply, table);
                            Protocol::AppendBulk(reply, RightName(right));
                        }
                    }
                    return reply;
                },
                [&](const cql::ShowMetrics&) {
                    command_class = MetricsRegistry::CommandClass::kAdmin;
                    RequireRightOnEveryTable(session, Right::kAdmin);
                    return HandleMetrics();
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


std::string CommandEngine::TxnBegin(SessionState& session) {
    if (session.transaction != nullptr) {
        throw std::invalid_argument("a transaction is already open; COMMIT or ROLLBACK it first");
    }
    session.transaction = std::make_unique<SessionTransaction>(&txn_total_bytes_, std::chrono::steady_clock::now());
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
        throw TransactionConflictError(
            TxnConflictReason::kDuration,
            "the transaction ran longer than " + std::to_string(config_.txn_max_duration.count()) + " ms");
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

void CommandEngine::TxnRead(SessionTransaction& txn, std::span<const ChunkCoord> coords) {
    std::size_t added = 0;
    for (const auto& coord : coords) {
        added += txn.read_set.contains(coord) ? 0U : 1U;
    }
    if (txn.read_set.size() + added > kMaxTxnReadChunks) {
        throw std::invalid_argument(
            "a transaction reads at most " + std::to_string(kMaxTxnReadChunks) + " chunks");
    }
    txn.read_set.insert(coords.begin(), coords.end());
}

void CommandEngine::TxnWrite(
    SessionTransaction& txn,
    ChunkStore& store,
    const ChunkCoord& coord,
    const std::function<void(ChunkState&)>& change) {
    auto own = txn.writes.find(coord);
    const bool created = own == txn.writes.end();
    if (created) {
        if (txn.writes.size() >= kMaxTxnWrittenChunks) {
            throw std::invalid_argument(
                "a transaction writes at most " + std::to_string(kMaxTxnWrittenChunks) + " chunks");
        }
        own = txn.writes.emplace(coord, store.ReadChunkStateAt(*txn.snapshot, coord.x, coord.y)).first;
    }
    ChunkState& state = own->second;
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
        change(state);
        const std::size_t after = StateBytes(state);
        if (txn.bytes - before + after > config_.txn_max_bytes) {
            throw std::invalid_argument(
                "the transaction's changes would take " + std::to_string(txn.bytes - before + after) +
                " bytes, more than --txn-max-bytes (" + std::to_string(config_.txn_max_bytes) + ")");
        }
        if (after > before) {
            const std::size_t grown = after - before;
            if (txn_total_bytes_.fetch_add(grown) + grown > config_.txn_total_bytes) {
                txn_total_bytes_.fetch_sub(grown);
                throw std::invalid_argument(
                    "open transactions hold more than --txn-total-bytes (" +
                    std::to_string(config_.txn_total_bytes) + ") of changes; try again later");
            }
        } else {
            txn_total_bytes_.fetch_sub(before - after);
        }
        txn.bytes = txn.bytes - before + after;
    } catch (...) {
        undo();
        throw;
    }
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
        throw TransactionConflictError(
            TxnConflictReason::kDuration,
            "the transaction ran longer than " + std::to_string(config_.txn_max_duration.count()) + " ms");
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
