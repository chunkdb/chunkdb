#include "fuzz_common.hpp"
#include "backup.hpp"
#include "feed_slot_records.hpp"
#include "migrations_records.hpp"
#include "store_manifest.hpp"

namespace {
void Parse(const chunkdb::fuzz::Bytes& bytes) {
#if CHUNKDB_FUZZ_METADATA == 1
    chunkdb::fuzz::Rejectable([&] { (void)chunkdb::ParseStoreManifest(bytes); });
    chunkdb::fuzz::Rejectable([&] { (void)chunkdb::ParseDataDirManifest(bytes); });
#elif CHUNKDB_FUZZ_METADATA == 2
    chunkdb::fuzz::Rejectable([&] { (void)chunkdb::ParseFeedSlotRecords(bytes, chunkdb::fuzz::kId); });
#elif CHUNKDB_FUZZ_METADATA == 3
    chunkdb::fuzz::Rejectable([&] { (void)chunkdb::DecodeMigrationRecords(bytes, chunkdb::fuzz::kId); });
    chunkdb::fuzz::Rejectable([&] { (void)chunkdb::DecodeMigrationJournal(bytes); });
#elif CHUNKDB_FUZZ_METADATA == 4
    chunkdb::fuzz::Rejectable([&] { (void)chunkdb::ParseBackupRecord(bytes); });
#endif
}
}
extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size > chunkdb::fuzz::kMaxInput) return 0;
    chunkdb::fuzz::Bytes bytes(data, data + size);
    Parse(bytes); // Corrupt checksum paths matter too.
    chunkdb::fuzz::RepairTrailingCrc(bytes);
    Parse(bytes); // Reach structures beyond the format's checksum barrier.
    return 0;
}
