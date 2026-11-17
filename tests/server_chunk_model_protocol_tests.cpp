#include <algorithm>
#include <limits>
#include <span>

#include "server_slots_test_utils.hpp"
#include "sectioned_chunk_form.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::slot_socket_test;
using Bytes = std::vector<std::uint8_t>;

std::string Coordinates(unsigned dims, int value) {
    return std::to_string(value) + " " + std::to_string(value) + (dims == 3U ? " " + std::to_string(value) : "");
}
void Create(Client& client, unsigned dims) {
    client.Ok("CREATE TABLE t (n u8, optional u16 NULL, label text(128)) CHUNK 2 x 2" +
        std::string(dims == 3U ? " x 2" : "") + " CHUNK COLUMNS (owner u8 DEFAULT 0, note text(128) NULL)");
}
ChunkFormSections Parse(std::string_view form) {
    return ParseChunkFormSections({reinterpret_cast<const std::uint8_t*>(form.data()), form.size()},
        std::numeric_limits<std::uint32_t>::max(), ChunkFormReadMode::kRead);
}
Bytes Copy(std::span<const std::uint8_t> bytes) { return {bytes.begin(), bytes.end()}; }
void CheckMetadata(const ChunkFormSections& form, unsigned owner, std::string_view note, bool null_note = false) {
    const auto columns = ParseChunkFormChunkColumns(form);
    assert(columns.size() == 2U && columns[0].column_id == 1U && columns[1].column_id == 2U);
    assert(!columns[0].is_null && Copy(columns[0].value) == Bytes{static_cast<std::uint8_t>(owner)});
    assert(columns[1].is_null == null_note);
    assert(Copy(columns[1].value) == Bytes(note.begin(), note.end()));
}
void CheckEntries(const Reply& reply, unsigned dims, const std::vector<std::pair<int, std::string>>& expected) {
    ExpectType(reply, '*');
    assert(reply.items.size() == expected.size());
    for (const auto& [coordinate, form] : expected) {
        const auto found = std::find_if(reply.items.begin(), reply.items.end(), [&](const Reply& row) {
            return row.items.size() == dims + 1U && row.items[0].value == std::to_string(coordinate);
        });
        assert(found != reply.items.end());
        for (unsigned axis = 0; axis < dims; ++axis)
            assert(found->items[axis].type == ':' && found->items[axis].value == std::to_string(coordinate));
        assert(found->items[dims].type == '$' && found->items[dims].value == form);
    }
}

void Area(bool tls, unsigned dims) {
    Harness harness(tls);
    auto client = harness.Connect(); Create(*client, dims);
    const auto point = Coordinates(dims, -1), zero = Coordinates(dims, 0), absent = Coordinates(dims, 1);
    (void)Number(client->Command("SET BLOCK " + point + " IN t n=7,optional=NULL,label='block'"));
    const auto version = Number(client->Command("SET CHUNK " + point + " IN t META owner=9,note='metadata'"));
    (void)Number(client->Command("SET CHUNK " + zero + " IN t META owner=3,note=NULL"));
    assert(client->Command("GET CHUNK " + absent + " FROM t").type == '_');
    for (const auto* projection : {"", " COLUMNS optional,n"}) {
        const auto full = client->Command("GET CHUNK " + point + " FROM t" + projection).value;
        const auto metadata_only = client->Command("GET CHUNK " + zero + " FROM t" + projection).value;
        const auto parsed = Parse(full);
        assert(parsed.header.chunk_version == version && parsed.header.commit_time_ms != 0U);
        assert(parsed.header.flags == (std::string_view(projection).empty() ? 0U : kChunkFormProjected));
        CheckMetadata(parsed, 9, "metadata"); CheckMetadata(Parse(metadata_only), 3, "", true);
        const auto block_columns = ParseChunkFormBlockColumns(parsed, dims == 3U ? 8U : 4U);
        assert(block_columns.size() == 2U);
        assert(block_columns[0].column_id == (std::string_view(projection).empty() ? 1U : 2U));
        const auto box = "GET AREA " + point + " TO " + absent + " FROM t" + projection;
        CheckEntries(client->Command(box), dims, {{-1, full}, {0, metadata_only}});
        const auto with_schema = client->Command(box + " WITH SCHEMA");
        assert(with_schema.items.size() == 2U && Number(with_schema.items[0]) == parsed.header.schema_version);
        CheckEntries(with_schema.items[1], dims, {{-1, full}, {0, metadata_only}});
        CheckEntries(client->Command("GET AREA AROUND " + point + " RADIUS 0 FROM t" + projection), dims, {{-1, full}});
    }
    (void)Number(client->Command("DELETE BLOCK " + point + " FROM t"));
    // Clearing blocks preserves metadata, so the area still includes this chunk.
    CheckEntries(client->Command("GET AREA " + point + " TO " + point + " FROM t"), dims,
        {{-1, client->Command("GET CHUNK " + point + " FROM t").value}});
    (void)Number(client->Command("SET CHUNK " + point + " IN t META owner=0,note=NULL"));
    assert(client->Command("GET AREA " + point + " TO " + point + " FROM t").items.empty());
    const auto tombstone = client->Command("GET CHUNK " + point + " FROM t");
    ExpectType(tombstone, '$'); CheckMetadata(Parse(tombstone.value), 0, "", true);
    const auto empty = client->Command("GET AREA " + absent + " TO " + absent + " FROM t WITH SCHEMA");
    assert(Number(empty.items[0]) == 1U && empty.items[1].items.empty());
}

void Put(Bytes& bytes, std::size_t at, std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) bytes[at + i] = static_cast<std::uint8_t>(value >> (i * 8U));
}
// Extend bytes actually returned by the server with opaque future sections.
// The mutation serializer intentionally refuses unknown kinds.
Bytes Extend(std::string_view bytes) {
    const auto parsed = Parse(bytes);
    const Bytes opaque{0xff, 0, 0x7f};
    auto sections = parsed.sections;
    sections.insert(sections.begin(), {0U, {}});
    sections.push_back({100U, opaque});
    Bytes extended(36U + 12U * sections.size(), 0);
    std::copy_n(bytes.begin(), 36U, extended.begin());
    Put(extended, 32U, sections.size(), 2U);
    for (std::size_t i = 0; i < sections.size(); ++i) {
        Put(extended, 36U + 12U * i, sections[i].kind, 2U);
        Put(extended, 40U + 12U * i, extended.size(), 4U);
        Put(extended, 44U + 12U * i, sections[i].body.size(), 4U);
        extended.insert(extended.end(), sections[i].body.begin(), sections[i].body.end());
    }
    return extended;
}
void UnknownSections(bool tls, unsigned dims) {
    Harness harness(tls);
    auto client = harness.Connect(); Create(*client, dims);
    const auto point = Coordinates(dims, -1);
    (void)Number(client->Command("SET BLOCK " + point + " IN t n=7,optional=NULL,label='block'"));
    (void)Number(client->Command("SET CHUNK " + point + " IN t META owner=9,note='metadata'"));
    for (const auto* projection : {"", " COLUMNS optional,n"}) {
        const auto area = client->Command("GET AREA " + point + " TO " + point + " FROM t" + projection);
        assert(area.items.size() == 1U && area.items[0].items.size() == dims + 1U);
        const auto& original = area.items[0].items[dims].value;
        const auto expected = Parse(original);
        if (std::string_view(projection).empty()) {
            assert(expected.sections.size() == 6U);
            for (unsigned i = 0; i < 6U; ++i) assert(expected.sections[i].kind == i + 1U);
        }
        const auto extended = Extend(original);
        const std::string_view wire(reinterpret_cast<const char*>(extended.data()), extended.size());
        const auto parsed = Parse(wire);
        assert(parsed.sections.size() == expected.sections.size() + 2U);
        assert(parsed.header.chunk_version == expected.header.chunk_version);
        assert(parsed.header.schema_version == expected.header.schema_version);
        assert(parsed.header.commit_time_ms == expected.header.commit_time_ms && parsed.header.flags == expected.header.flags);
        for (std::size_t i = 0; i < expected.sections.size(); ++i) {
            assert(parsed.sections[i + 1U].kind == expected.sections[i].kind);
            assert(Copy(parsed.sections[i + 1U].body) == Copy(expected.sections[i].body));
        }
        CheckMetadata(parsed, 9, "metadata");
        const auto count = dims == 3U ? 8U : 4U;
        const auto columns = ParseChunkFormBlockColumns(parsed, count);
        const auto original_columns = ParseChunkFormBlockColumns(expected, count);
        assert(columns.size() == original_columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i) {
            assert(columns[i].column_id == original_columns[i].column_id);
            assert(Copy(columns[i].validity) == Copy(original_columns[i].validity));
            for (std::size_t block = 0; block < count; ++block)
                assert(ChunkFormRawBlockValue(columns[i], count, block) == ChunkFormRawBlockValue(original_columns[i], count, block));
        }
        // Read compatibility must not relax the mutation boundary or change stored state.
        client->Send("SET CHUNK " + point + " IN t" + projection + " $1\r\n$" + std::to_string(wire.size()) + "\r\n" + std::string(wire) + "\r\n");
        const auto refused = client->Read();
        assert(refused.type == '-' && refused.value.find("unknown_form_section") != std::string::npos);
        assert(client->Command("GET CHUNK " + point + " FROM t" + projection).value == original);
        auto invalid = extended;
        Put(invalid, 40U + 12U * (parsed.sections.size() - 1U), invalid.size() + 1U, 4U);
        bool rejected = false;
        try { (void)Parse({reinterpret_cast<const char*>(invalid.data()), invalid.size()}); }
        catch (const std::invalid_argument&) { rejected = true; }
        assert(rejected);
    }
}

// Pages of one row each; AFTER continues from the last returned coordinates.
std::vector<Reply> ScanRows(Client& client, unsigned dims, std::string_view tail) {
    std::vector<Reply> rows;
    std::string after;
    for (bool more = true; more;) {
        assert(rows.size() < 8U);
        const auto page = client.Command("SCAN CHUNKS FROM t" + after + " LIMIT 1" + std::string(tail));
        const auto& chunks = Field(page, "chunks");
        ExpectType(chunks, '*');
        assert(chunks.items.size() <= 1U);
        more = Boolean(Field(page, "more"));
        if (chunks.items.empty()) { assert(!more); break; }
        const auto& row = chunks.items[0];
        assert(row.items.size() == dims + 2U);
        after = " AFTER";
        for (unsigned axis = 0; axis < dims; ++axis) after += " " + row.items[axis].value;
        rows.push_back(row);
    }
    return rows;
}
void Scan(bool tls, unsigned dims) {
    Harness harness(tls);
    auto client = harness.Connect(); Create(*client, dims);
    const auto point = Coordinates(dims, -1), zero = Coordinates(dims, 0), deleted = Coordinates(dims, 1);
    (void)Number(client->Command("SET BLOCK " + point + " IN t n=7,optional=NULL,label='block'"));
    (void)Number(client->Command("SET CHUNK " + point + " IN t META owner=9,note='metadata'"));
    (void)Number(client->Command("SET CHUNK " + zero + " IN t META owner=3,note=NULL"));
    (void)Number(client->Command("SET CHUNK " + deleted + " IN t META owner=4"));
    const auto deletion = Number(client->Command("SET CHUNK " + deleted + " IN t META owner=0,note=NULL"));
    for (const auto* projection : {"", " COLUMNS optional,n"}) {
        const auto full = client->Command("GET CHUNK " + point + " FROM t" + projection).value;
        const auto metadata_only = client->Command("GET CHUNK " + zero + " FROM t" + projection).value;
        for (const auto* since : {"", " SINCE 0"}) {
            const auto rows = ScanRows(*client, dims, std::string(since) + " WITH DATA" + projection);
            std::vector<long long> coordinates;
            for (const auto& row : rows) coordinates.push_back(std::stoll(row.items[0].value));
            std::sort(coordinates.begin(), coordinates.end());
            assert(coordinates == (std::string_view(since).empty() ? std::vector<long long>{-1, 0}
                                                                    : std::vector<long long>{-1, 0, 1}));
            for (const auto& row : rows) {
                const auto coordinate = std::stoll(row.items[0].value);
                for (unsigned axis = 0; axis < dims; ++axis)
                    assert(row.items[axis].type == ':' && std::stoll(row.items[axis].value) == coordinate);
                const auto& data = row.items[dims + 1U];
                if (coordinate == 1) {
                    assert(Number(row.items[dims]) == deletion && data.type == '_');
                    continue;
                }
                ExpectType(data, '$');
                const auto form = Parse(data.value);
                assert(Number(row.items[dims]) == form.header.chunk_version);
                assert(form.header.flags == (std::string_view(projection).empty() ? 0U : kChunkFormProjected));
                if (coordinate == -1) {
                    assert(data.value == full);
                    CheckMetadata(form, 9, "metadata");
                } else {
                    assert(coordinate == 0 && data.value == metadata_only);
                    CheckMetadata(form, 3, "", true);
                }
            }
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    test::FeedPhaseWatchdog::SuppressWindowsDialogs();
    const std::string_view selected = argc == 2 ? argv[1] : "";
    unsigned passed = 0, total = 0, skipped = 0;
    for (const bool tls : {false, true}) {
        for (const unsigned dims : {2U, 3U}) {
            const auto run = [&](std::string_view name, auto function) {
                if (!selected.empty() && selected != name) return;
#ifndef CHUNKDB_WITH_OPENSSL
                if (tls) { ++skipped; return; }
#endif
                ++total;
                const auto label = std::string(tls ? "TLS " : "plain ") + std::to_string(dims) + "D " + std::string(name);
                std::cout << label << std::endl;
                test::FeedPhaseWatchdog watchdog(label);
                try { function(tls, dims); RethrowBackgroundServerError(); ++passed; }
                catch (const std::exception& error) { std::cerr << "FAILED " << label << ": " << error.what() << std::endl; }
            };
            run("area", Area); run("unknown-sections", UnknownSections); run("scan", Scan);
        }
    }
    std::cout << passed << "/" << total << " chunk model protocol groups passed; " << skipped << " skipped\n";
    return passed == total && total != 0U ? 0 : 1;
}
