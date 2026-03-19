#pragma once

#include <cstddef>

namespace chunkdb {

inline constexpr std::size_t kDefaultWalGroupCommitUpdates = 8;
inline constexpr std::size_t kDefaultMaxLoadedChunks = 65536;

}  // namespace chunkdb
