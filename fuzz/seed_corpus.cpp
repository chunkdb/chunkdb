#include "fuzz_common.hpp"
#include "backup.hpp"
#include "chunk_store_internal.hpp"
#include "feed_slot_records.hpp"
#include "migrations_records.hpp"
#include "store_manifest.hpp"
#include "wal_writer.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

namespace {
using chunkdb::fuzz::Bytes;
std::filesystem::path root;
void Save(std::string_view target, const std::string& name, const Bytes& bytes) {
    const auto dir = root / target;
    std::filesystem::create_directories(dir);
    std::ofstream out(dir / name, std::ios::binary | std::ios::trunc);
    out.exceptions(std::ios::failbit | std::ios::badbit);
    if (!bytes.empty()) out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}
Bytes Text(std::string_view text) { return {text.begin(), text.end()}; }
void Fixture(std::string_view target, const std::string& name, const Bytes& bytes) {
    Save(target, name, bytes);
    if (bytes.size() > 1U) {
        auto partial = bytes; partial.pop_back();
        Save(target, name + "-partial", partial);
        auto corrupt = bytes; corrupt.back() ^= 1U;
        Save(target, name + "-corrupt", corrupt);
    }
}
void ModelFixture(std::string_view target, const std::string& name, unsigned model, Bytes bytes) {
    bytes.insert(bytes.begin(), static_cast<std::uint8_t>(model));
    Fixture(target, name, bytes);
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) { std::cerr << "usage: chunkdb_fuzz_seeds CORPUS_DIRECTORY\n"; return 2; }
    root = argv[1];
    using namespace chunkdb;
    // Shapes and mutations used by wal_format/chunk_image/typed_columns tests.
    for (unsigned model = 0; model < 3U; ++model) {
        const auto& geometry = fuzz::Model(model);
        Bytes payload(geometry.ChunkPayloadBytes(), 0U);
        Bytes presence = FullPresenceBitmap(geometry);
        const auto name = "model-" + std::to_string(model);
        for (auto compression : {CheckpointCompression::kNone, CheckpointCompression::kZrle}) {
            ModelFixture("image", name + (compression == CheckpointCompression::kNone ? "-raw" : "-zrle"),
                model, SerializeChunkImage(geometry, fuzz::kCoord, payload, presence,
                    compression, 42U, 1727786400123ULL, fuzz::kId));
        }
        auto wal = BuildWalHeader(fuzz::kCoord, fuzz::kId, kKnownFeatures);
        ModelFixture("wal", name + "-header", model, wal);
        auto state = BuildChunkStateBytes(geometry, payload, presence);
        WalFrameBuilder frame(&wal, 1U, Text("fixture"), "admin");
        frame.AppendSpan(0U, state.data(), state.size());
        (void)frame.Finish(1U, 10U);
        ModelFixture("wal", name + "-span", model, wal);
        auto form = Bytes(16U, 0U); form[8U] = 1U;
        form.insert(form.end(), presence.begin(), presence.end());
        form.insert(form.end(), payload.begin(), payload.end());
        const auto vars = ChunkVars{}.Encode();
        form.insert(form.end(), vars.begin(), vars.end());
        ModelFixture("chunk_form", name, model, form);
    }
    // Equivalent to manifest, slot-record and migration-record unit fixtures.
    const auto& geometry = fuzz::Model(1U);
    StoreManifest manifest{kKnownFeatures, geometry.config(), fuzz::kId,
        EncodeTableOptions(TableOptions{}), geometry.layout().schema()};
    const auto before = SerializeStoreManifest(manifest);
    Fixture("manifest", "table", before);
    Fixture("manifest", "directory", SerializeDataDirManifest({kKnownFeatures, fuzz::kId, {}}));
    Fixture("slots", "active-lost", SerializeFeedSlotRecords(
        {fuzz::kId, 987U, {{"consumer_1", 123U, false}, {"lost", 456U, true}}}));
    const MigrationRecord record{"set_options", 1234U, "admin", "ALTER TABLE default SET checkpoint_updates = 512"};
    const auto ledger = EncodeMigrationRecords(fuzz::kId, {record});
    Fixture("migrations", "ledger", ledger);
    manifest.options = EncodeTableOptions(TableOptions{.checkpoint_update_interval = 512U});
    MigrationJournal journal;
    journal.data_dir_id = fuzz::kId; journal.record = record;
    journal.table = "default"; journal.table_id = fuzz::kId;
    journal.files = {{"tables/default/table.manifest", before, SerializeStoreManifest(manifest)},
        {std::string(kMigrationsFileName), std::nullopt, ledger}};
    Fixture("migrations", "journal", EncodeMigrationJournal(journal));
    Fixture("backup", "marker", SerializeBackupRecord({123U,
        {{"default", fuzz::kId, 3U}}, {{"chunkdb.manifest", 44U, 0U}, {"chunkdb.users", 100U, 1U}}}));
    // Request examples from the parser and server's framing/login fixtures.
    unsigned index = 0U;
    for (std::string_view statement : {"PING", "GET BLOCK 0 0 IN default", "SET BLOCK 0 0 IN default 11111111",
             "GET AREA 0 0 3 3 IN default", "WATCH CHUNKS 0 0 1 1 IN default", "SHOW TABLES",
             "BEGIN", "COMMIT", "CREATE TABLE world (value u8) CHUNK 4 4", "BACKUP TO 'copy'"})
        Fixture("cql", "statement-" + std::to_string(index++), Text(statement));
    Fixture("request", "ping", Text("PING\r\n"));
    Fixture("request", "pipeline", Text("BEGIN\r\nPING\r\nROLLBACK\r\n"));
    Fixture("request", "hello", Text("HELLO 3 AUTH SCRAM-SHA-256 $1\r\n$18\r\nn,,n=user,r=client\r\n"));
    Fixture("request", "proof", Text("AUTH $1\r\n$28\r\nc=biws,r=clientserver,p=AA==\r\n"));
    Fixture("request", "parameter", Text("SET BLOCK 0 0 IN default $1\r\n$8\r\n11111111\r\n"));
    Fixture("request", "null", Text("SET BLOCK 0 0 IN default $1\r\n_\r\n"));
    for (std::string_view target : {"cql", "request", "chunk_form", "wal", "image", "manifest", "slots", "migrations", "backup"})
        Save(target, "empty", {});
    return 0;
}
