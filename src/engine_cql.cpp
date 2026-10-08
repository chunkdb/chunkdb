// Protocol 3: CQL statements over the typed column API, with typed replies
// (docs/CQL_DESIGN.md).

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

// The chunk form of docs/CQL_DESIGN.md: version u64, the presence bitmap,
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
    return 8U + ChunkPresenceBitmapBytes(geometry) + geometry.ChunkPayloadBytes() + var_max_chunk_bytes;
}

// The state a chunk form of every column sends; its version is not used.
[[nodiscard]] ChunkState DecodeChunkForm(const Geometry& geometry, std::string_view form) {
    const std::size_t presence_bytes = ChunkPresenceBitmapBytes(geometry);
    const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
    if (form.size() < 8U + presence_bytes + payload_bytes) {
        throw std::invalid_argument(
            "the chunk takes at least " + std::to_string(8U + presence_bytes + payload_bytes) +
            " bytes (version, presence, payload), got " + std::to_string(form.size()));
    }
    const auto* data = reinterpret_cast<const std::uint8_t*>(form.data());
    ChunkState state;
    state.presence_bitmap.assign(data + 8, data + 8 + presence_bytes);
    state.payload.assign(data + 8 + presence_bytes, data + 8 + presence_bytes + payload_bytes);
    const std::size_t vars_offset = 8U + presence_bytes + payload_bytes;
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

[[nodiscard]] std::string VersionReply(const ChunkMutationResult& result) {
    if (!result.ok) {
        return Protocol::Error("VERSION_MISMATCH", "current=" + std::to_string(result.version));
    }
    std::string reply;
    Protocol::AppendInteger(reply, result.version);
    return reply;
}

}  // namespace

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
    const auto find = [&](const std::string& name) {
        return session.table != nullptr && session.table->name() == name ? session.table : catalog_->Find(name);
    };
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
        return std::visit(
            Overloaded{
                [&](const cql::GetBlock& get) {
                    command_class = MetricsRegistry::CommandClass::kPointRead;
                    const auto lease = AcquireNamedTable(session, get.table);
                    ChunkStore& store = lease.store();
                    const auto& layout = store.geometry().layout();
                    std::vector<std::size_t> columns = ColumnsOf(layout, get.columns);
                    if (columns.empty()) {
                        columns.resize(layout.schema().columns.size());
                        for (std::size_t i = 0; i < columns.size(); ++i) {
                            columns[i] = i;
                        }
                    }
                    const auto values = store.GetBlock(get.x, get.y);
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
                    command_class = MetricsRegistry::CommandClass::kPointWrite;
                    const auto lease = AcquireNamedTable(session, set.table);
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
                    return VersionReply(store.SetBlock(set.x, set.y, values, set.if_version));
                },
                [&](const cql::DeleteBlock& del) {
                    command_class = MetricsRegistry::CommandClass::kPointWrite;
                    const auto lease = AcquireNamedTable(session, del.table);
                    return VersionReply(lease.store().UnsetBlock(del.x, del.y, del.if_version));
                },
                [&](const cql::GetChunk& get) {
                    command_class = MetricsRegistry::CommandClass::kChunkRead;
                    const auto lease = AcquireNamedTable(session, get.table);
                    ChunkStore& store = lease.store();
                    const auto& layout = store.geometry().layout();
                    const auto columns = ColumnsOf(layout, get.columns);
                    const auto state = store.ReadChunkState(get.chunk_x, get.chunk_y);
                    std::string reply;
                    if (!state.has_value()) {
                        Protocol::AppendNull(reply);
                        return reply;
                    }
                    Protocol::AppendBulk(
                        reply,
                        EncodeChunkForm(layout, state->version, state->payload, state->presence_bitmap, state->vars, columns));
                    return reply;
                },
                [&](const cql::SetChunk& set) {
                    command_class = MetricsRegistry::CommandClass::kChunkWrite;
                    const auto lease = AcquireNamedTable(session, set.table);
                    ChunkStore& store = lease.store();
                    const auto& frame = parameters[set.state.index - 1U];
                    if (!frame.has_value()) {
                        throw std::invalid_argument("the chunk cannot be NULL; DELETE its blocks instead");
                    }
                    return VersionReply(store.WriteChunkState(
                        set.chunk_x, set.chunk_y, DecodeChunkForm(store.geometry(), *frame), set.if_version));
                },
                [&](const cql::GetArea& get) {
                    command_class = MetricsRegistry::CommandClass::kRange;
                    const auto lease = AcquireNamedTable(session, get.table);
                    ChunkStore& store = lease.store();
                    const auto& layout = store.geometry().layout();
                    const auto columns = ColumnsOf(layout, get.columns);
                    const auto entries = get.around ? store.ReadChunkRadius(get.x0, get.y0, get.radius, true)
                                                    : store.ReadChunkRange(get.x0, get.y0, get.x1, get.y1, true);
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
                [](const auto&) {
                    return Protocol::Error("UNKNOWN_COMMAND", "this statement is not available yet");
                },
            },
            parsed.statement);
    } catch (const std::exception& e) {
        return ErrorReply(e);
    }
}

}  // namespace chunkdb
