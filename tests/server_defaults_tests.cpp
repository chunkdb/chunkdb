#include <cassert>

#include "chunkdb/server_defaults.hpp"

int main() {
    assert(chunkdb::kDefaultWalGroupCommitUpdates == 8);
    assert(chunkdb::kDefaultMaxLoadedChunks == 65536);
    return 0;
}
