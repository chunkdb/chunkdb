#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb {

// One push of WATCH t REPLICATION as APPLY takes it
// (docs/design/REPLICATION_APPLY.md): its RESP3 bytes, decoded without the
// table, which the epoch names.
struct ReplicationPushValue {
    char type = 0;          // RESP3 prefix: '>', '*', '$', '+', ':', '(' or '_'.
    std::string_view text;  // Bulk and simple strings, inside the decoded bytes.
    std::int64_t integer = 0;
    std::uint64_t unsigned_value = 0;
    bool negative = false;  // ':' below zero.
    std::vector<ReplicationPushValue> items;
};

// The most values one push may hold, aggregates included.
inline constexpr std::size_t kMaxReplicationPushValues = 1U << 20U;

// Decodes exactly one RESP3 value filling `bytes`, which must outlive the
// result. Every length is bounded by the bytes left, the values by
// `max_values`, and nesting by `max_depth`. Throws std::invalid_argument
// for anything else.
[[nodiscard]] ReplicationPushValue DecodeReplicationValue(std::string_view bytes, std::size_t max_depth = 8U,
    std::size_t max_values = kMaxReplicationPushValues);

enum class ReplicationPushKind { kReplication, kProgress };

struct ReplicatedBlockRow {
    std::int64_t x = 0;
    std::int64_t y = 0;
    std::int64_t z = 0;
    // Each block column's value in its parameter encoding, null for NULL;
    // std::nullopt deletes the block.
    std::optional<std::vector<std::optional<std::string>>> after;
};

struct ReplicatedChunk {
    ChunkCoord coord{};
    std::uint64_t version = 0U;
    bool rows = false;
    // kind `form`: the chunk's form after the commit; null for a deletion.
    std::optional<std::string> form;
    // kind `rows`: the changed blocks and the chunk-column row after the
    // commit, std::nullopt when it did not change.
    std::vector<ReplicatedBlockRow> blocks;
    std::optional<std::vector<std::optional<std::string>>> chunk_row;
};

struct ReplicationPush {
    ReplicationPushKind kind = ReplicationPushKind::kReplication;
    StoreId epoch{};
    std::uint64_t revision = 0U;
    std::uint64_t commit_time_ms = 0U;
    std::optional<std::string> user;
    std::uint64_t schema_version = 0U;
    std::vector<ReplicatedChunk> chunks;
};

// The kind and epoch of a decoded push, before the table is known. Throws
// std::invalid_argument for a push APPLY does not take.
[[nodiscard]] ReplicationPush ReadReplicationPushHeader(const ReplicationPushValue& push);
// The whole push, including any table list, for the follower selected by epoch.
[[nodiscard]] ReplicationPush ReadReplicationPush(const ReplicationPushValue& push, std::uint8_t dims,
    std::string_view table);

// 32 hex digits to an epoch; std::nullopt for anything else.
[[nodiscard]] std::optional<StoreId> ParseEpochHex(std::string_view hex);

}  // namespace chunkdb
