#pragma once

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "test_utils.hpp"

// Helpers shared by the transaction tests.
namespace chunkdb::txn_test {

// TSan's deadlock detector tracks at most 64 held locks per thread,
// including the inner locks taken while a commit holds its chunk locks.
#if defined(__SANITIZE_THREAD__)
inline constexpr bool kThreadSanitizer = true;
#elif defined(__has_feature)
inline constexpr bool kThreadSanitizer = __has_feature(thread_sanitizer);
#else
inline constexpr bool kThreadSanitizer = false;
#endif

inline constexpr std::chrono::milliseconds kTxnDuration{60'000};

// Chunks of 4x4 blocks of `block_bits` bits; with 32 bits a chunk's block 0
// holds a counter (payload bytes 0-3, little-endian).
inline StoreConfig Config(
    const std::filesystem::path& data_dir,
    DurabilityMode mode = DurabilityMode::kFsyncWal,
    std::uint32_t block_bits = 32) {
    StoreConfig config{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = block_bits,
        },
        .data_dir = data_dir,
    };
    config.durability_mode = mode;
    config.checkpoint_update_interval = 8;
    config.checkpoint_wal_bytes = 4096;
    config.wal_group_commit_updates = 4;
    config.max_loaded_chunks = 256;
    return config;
}

class ScopedEnv {
  public:
    ScopedEnv(const char* key, const std::string& value) : key_(key) {
#ifdef _WIN32
        _putenv_s(key_, value.c_str());
#else
        setenv(key_, value.c_str(), 1);
#endif
    }
    ~ScopedEnv() {
#ifdef _WIN32
        _putenv_s(key_, "");
#else
        unsetenv(key_);
#endif
    }
    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;

  private:
    const char* key_;
};

inline std::uint32_t CounterOf(const ChunkState& state) {
    if ((state.presence_bitmap[0] & 1U) == 0U) {
        return 0;
    }
    return static_cast<std::uint32_t>(state.payload[0]) | (static_cast<std::uint32_t>(state.payload[1]) << 8U) |
           (static_cast<std::uint32_t>(state.payload[2]) << 16U) |
           (static_cast<std::uint32_t>(state.payload[3]) << 24U);
}

inline void SetCounter(ChunkState* state, std::uint32_t value) {
    for (std::size_t i = 0; i < 4U; ++i) {
        state->payload[i] = static_cast<std::uint8_t>((value >> (8U * i)) & 0xFFU);
    }
    state->presence_bitmap[0] = static_cast<std::uint8_t>(state->presence_bitmap[0] | 1U);
}

// The counter as a 32-bit block (bit n of the string is bit n of the value).
inline std::string CounterBits(std::uint32_t value) {
    std::string bits(32, '0');
    for (std::size_t i = 0; i < 32U; ++i) {
        if (((value >> i) & 1U) != 0U) {
            bits[i] = '1';
        }
    }
    return bits;
}

inline std::uint32_t ReadCounter(ChunkStore& store, const ChunkCoord& coord) {
    return CounterOf(store.ReadChunkState(coord.x, coord.y));
}

inline void WriteCounter(ChunkStore& store, const ChunkCoord& coord, std::uint32_t value) {
    store.SetBlockBits(coord.x * 4, coord.y * 4, CounterBits(value));
}

// Moves up to `amount` from one counter to another in a transaction, again
// after each conflict. Returns the commit's version (0 when nothing moved).
inline std::uint64_t Transfer(ChunkStore& store, const ChunkCoord& from, const ChunkCoord& to, std::uint32_t amount) {
    while (true) {
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        auto source = store.ReadChunkStateAt(*snapshot, from.x, from.y);
        auto target = store.ReadChunkStateAt(*snapshot, to.x, to.y);
        const std::uint32_t moved = std::min(amount, CounterOf(source));
        SetCounter(&source, CounterOf(source) - moved);
        SetCounter(&target, CounterOf(target) + moved);
        try {
            return store.CommitTransaction(
                *snapshot,
                {from, to},
                {TxnChunkWrite{.coord = from, .state = std::move(source)},
                 TxnChunkWrite{.coord = to, .state = std::move(target)}});
        } catch (const TransactionConflictError&) {
        }
    }
}

// Adds one to a counter in a transaction, again after each conflict.
inline std::uint64_t Increment(ChunkStore& store, const ChunkCoord& coord) {
    while (true) {
        auto snapshot = store.BeginTxnSnapshot(kTxnDuration);
        auto state = store.ReadChunkStateAt(*snapshot, coord.x, coord.y);
        SetCounter(&state, CounterOf(state) + 1U);
        try {
            return store.CommitTransaction(*snapshot, {coord}, {TxnChunkWrite{.coord = coord, .state = std::move(state)}});
        } catch (const TransactionConflictError&) {
        }
    }
}

inline bool HasTxnIntent(const std::filesystem::path& data_dir) {
    const auto dir = data_dir / ".chunkdb.intents";
    if (!std::filesystem::exists(dir)) {
        return false;
    }
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.path().filename().string().rfind("txn-", 0) == 0) {
            return true;
        }
    }
    return false;
}

template <class F>
bool Throws(F&& f) {
    try {
        f();
    } catch (const std::exception&) {
        return true;
    }
    return false;
}

template <class E, class F>
bool ThrowsAs(F&& f) {
    try {
        f();
    } catch (const E&) {
        return true;
    } catch (const std::exception&) {
        return false;
    }
    return false;
}

}  // namespace chunkdb::txn_test
