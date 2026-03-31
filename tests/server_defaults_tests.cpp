#include <cassert>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/server_defaults.hpp"

int main() {
    assert(chunkdb::kDefaultWalGroupCommitUpdates == 8);
    assert(chunkdb::kDefaultMaxLoadedChunks == 65536);

    chunkdb::StoreConfig default_config{};
    assert(default_config.wal_group_commit_updates == chunkdb::kDefaultWalGroupCommitUpdates);
    assert(default_config.max_loaded_chunks == chunkdb::kDefaultMaxLoadedChunks);

    default_config.wal_group_commit_updates = 3;
    default_config.max_loaded_chunks = 42;
    assert(default_config.wal_group_commit_updates == 3);
    assert(default_config.max_loaded_chunks == 42);
    return 0;
}
