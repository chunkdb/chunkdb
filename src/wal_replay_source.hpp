#pragma once

#include "wal_replay.hpp"

namespace chunkdb::wal_detail {

struct FrameWindow {
    const std::vector<std::uint8_t>& bytes;
    std::size_t offset;
};

// Each window remains valid until the next Frame call.
// Header is stable for the entire replay and includes the admitted header prefix.
class ReplaySource {
  public:
    virtual ~ReplaySource() = default;
    virtual std::size_t Size() const = 0;
    virtual const std::vector<std::uint8_t>& Header() const = 0;
    virtual FrameWindow Frame(std::size_t position) = 0;
    virtual bool AllZeroAfterHeader() = 0;
    virtual bool HasLaterFrameHeader(std::size_t position) = 0;
};

WalReplayResult ReplayFromSource(ReplaySource& source, const Geometry& geometry,
    const ChunkCoord& coord, const StoreId& table_id, const FeatureFlags& features,
    std::uint64_t base_revision, std::uint64_t base_schema_version,
    std::vector<std::uint8_t>* payload, std::vector<std::uint8_t>* presence,
    ChunkVars* vars, std::vector<WalFrameBoundary>* boundaries,
    std::shared_ptr<const ChunkColumnState>* chunk_columns,
    std::optional<std::uint64_t> base_commit_time_ms = {}, std::optional<WalKnownIdentity> retained_identity = {});

}  // namespace chunkdb::wal_detail
