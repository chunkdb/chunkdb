#include <cassert>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <vector>

#include "chunkdb/chunk_columns.hpp"
#include "sectioned_chunk_form.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;

void Reject(const std::function<void()>& call) {
    bool rejected = false;
    try { call(); } catch (const std::invalid_argument&) { rejected = true; }
    assert(rejected);
}

TableSchema Schema() {
    TableSchema schema;
    schema.next_column_id = 18;
    schema.columns = {
        {.id = 9, .name = "status", .type = {ColumnKind::kUnsigned, 4}, .default_value = {}},
        {.id = 2, .name = "label", .type = {ColumnKind::kText, 16}, .nullable = true,
            .has_default = true, .default_value = {'x'}},
        {.id = 17, .name = "owner", .type = {ColumnKind::kUnsigned, 16}, .required = true, .default_value = {}},
    };
    return schema;
}

std::vector<ChunkFormChunkColumn> Entries(const Bytes& bytes) {
    return ParseChunkFormChunkColumns({.header = {}, .sections = {{5, bytes}}});
}

void DefaultsRequiredAndNull() {
    const auto schema = Schema();
    const auto baseline = DefaultChunkColumns(schema);
    assert((DecodeChunkColumns(schema, baseline.bytes) ==
        std::vector<ColumnValue>{std::uint64_t{0}, std::string("x"), std::uint64_t{0}}));
    assert(baseline.variable_bytes == 13);
    const auto entries = Entries(baseline.bytes);
    assert(entries.size() == 3 && entries[0].column_id == 2 && entries[1].column_id == 9 && entries[2].column_id == 17);
    assert(entries[1].value.size() == 1 && entries[1].value[0] == 0);
    assert(entries[2].value.size() == 2 && entries[2].value[0] == 0 && entries[2].value[1] == 0);
    Reject([&] { (void)AssignChunkColumns(schema, nullptr, {}, true); });
    Reject([&] { (void)AssignChunkColumns(schema, nullptr, {{"status", std::uint64_t{1}}}, true); });
    const auto initialized = AssignChunkColumns(schema, nullptr, {{"owner", std::uint64_t{256}}}, true);
    assert(std::get<std::uint64_t>(DecodeChunkColumns(schema, initialized.bytes)[2]) == 256);
    const auto cleared = AssignChunkColumns(schema, &initialized, {{"label", std::monostate{}}}, false);
    assert(cleared.variable_bytes == 0);
    assert(std::holds_alternative<std::monostate>(DecodeChunkColumns(schema, cleared.bytes)[1]));
    const auto empty = AssignChunkColumns(schema, &cleared, {{"label", std::string{}}}, false);
    assert(empty.variable_bytes == 12);
    assert(std::get<std::string>(DecodeChunkColumns(schema, empty.bytes)[1]).empty());
    Reject([&] { (void)AssignChunkColumns(schema, &initialized, {{"owner", std::monostate{}}}, false); });
    Reject([&] { (void)AssignChunkColumns(schema, &initialized, {{"missing", std::uint64_t{1}}}, false); });
    Reject([&] { (void)AssignChunkColumns(schema, &initialized,
        {{"status", std::uint64_t{1}}, {"status", std::uint64_t{2}}}, false); });
    Reject([&] { (void)AssignChunkColumns(schema, &initialized, {{"status", std::uint64_t{16}}}, false); });
    Reject([&] { (void)AssignChunkColumns(schema, &initialized, {{"label", std::string(17, 'a')}}, false); });
    Reject([&] { (void)AssignChunkColumns(schema, &initialized, {{"label", std::string(1, '\x80')}}, false); });
    assert(DefaultChunkColumns({}).bytes.empty());
    assert(DecodeChunkColumns({}, {}).empty());
    assert(ValidateChunkColumns({}, Bytes{0, 0, 0, 0}) == 0);
    assert(EncodeChunkColumns({}, {}).bytes.empty());
}

void RawValidationBeforeDecoding() {
    const auto schema = Schema();
    const auto row = DefaultChunkColumns(schema);
    for (std::size_t cut = 0; cut < row.bytes.size(); ++cut)
        Reject([&] { (void)DecodeChunkColumns(schema, std::span(row.bytes).first(cut)); });
    auto changed = row.bytes;
    changed.push_back(0);
    Reject([&] { (void)DecodeChunkColumns(schema, changed); });
    changed = row.bytes;
    changed[4] = 3; // Unknown stable ID, keeping a valid ascending directory.
    Reject([&] { (void)DecodeChunkColumns(schema, changed); });
    changed = row.bytes;
    changed[52] = 0x80; // Invalid UTF-8 in the first value.
    Reject([&] { (void)DecodeChunkColumns(schema, changed); });
    changed = row.bytes;
    changed[53] = 0x80; // Padding above the u4 value.
    Reject([&] { (void)DecodeChunkColumns(schema, changed); });
    changed = row.bytes;
    changed[10] = 1; // Reserved directory bytes.
    Reject([&] { (void)DecodeChunkColumns(schema, changed); });
    changed = row.bytes;
    changed[8] = 1; // NULL with a nonempty scalar.
    Reject([&] { (void)DecodeChunkColumns(schema, changed); });
    Reject([&] { (void)EncodeChunkColumns(schema, {}); });
}

void TranslationPreservesStableIdentity() {
    const auto initial = Schema();
    const auto original = AssignChunkColumns(initial, nullptr,
        {{"owner", std::uint64_t{9}}, {"label", std::string("saved")}}, true);
    const auto renamed = RenameChunkColumn(initial, "label", "note");
    const auto after_rename = TranslateChunkColumns(initial, renamed, original);
    assert(after_rename.bytes == original.bytes && after_rename.variable_bytes == original.variable_bytes);
    const auto dropped = DropChunkColumn(renamed, "note");
    const auto after_drop = TranslateChunkColumns(renamed, dropped, after_rename);
    assert(after_drop.variable_bytes == 0);
    Column replacement;
    replacement.name = "note";
    replacement.type = {ColumnKind::kText, 16};
    replacement.nullable = true;
    const auto added = AddChunkColumn(dropped, replacement);
    const auto after_add = TranslateChunkColumns(dropped, added, after_drop);
    const auto values = DecodeChunkColumns(added, after_add.bytes);
    assert(std::holds_alternative<std::monostate>(values.back()));
    assert(added.columns.back().id != initial.columns[1].id);
    Reject([&] { (void)TranslateChunkColumns(added, initial, after_add); });
    auto unrelated = added;
    unrelated.columns.front().name = "unrelated";
    Reject([&] { (void)TranslateChunkColumns(initial, unrelated, original); });
    const auto widened = ChangeColumnType(initial, "owner", {ColumnKind::kUnsigned, 32}, Conversion::kExact);
    const auto marked = AdvanceOtherDomain(widened, ColumnDomain::kChunk);
    const auto after_conversion = TranslateChunkColumns(initial, marked, original);
    const auto converted_values = DecodeChunkColumns(marked, after_conversion.bytes);
    assert(std::get<std::uint64_t>(converted_values[2]) == 9U);
    assert(std::get<std::string>(converted_values[1]) == "saved");
}
}

int main() {
    DefaultsRequiredAndNull();
    RawValidationBeforeDecoding();
    TranslationPreservesStableIdentity();
}
