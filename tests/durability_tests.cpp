#include <cassert>
#include <stdexcept>

#include "chunkdb/chunk_store.hpp"

int main() {
    assert(chunkdb::ParseDurabilityMode("relaxed") == chunkdb::DurabilityMode::kRelaxed);
    assert(chunkdb::ParseDurabilityMode("fsync-wal") == chunkdb::DurabilityMode::kFsyncWal);
    assert(
        chunkdb::ParseDurabilityMode("fsync-checkpoint") ==
        chunkdb::DurabilityMode::kFsyncCheckpoint);

    assert(std::string(chunkdb::DurabilityModeName(chunkdb::DurabilityMode::kRelaxed)) == "relaxed");
    assert(std::string(chunkdb::DurabilityModeName(chunkdb::DurabilityMode::kFsyncWal)) == "fsync-wal");
    assert(
        std::string(chunkdb::DurabilityModeName(chunkdb::DurabilityMode::kFsyncCheckpoint)) ==
        "fsync-checkpoint");

    bool thrown = false;
    try {
        (void)chunkdb::ParseDurabilityMode("unknown");
    } catch (const std::invalid_argument&) {
        thrown = true;
    }
    assert(thrown);

    return 0;
}
