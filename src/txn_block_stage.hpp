#pragma once

#include <memory>
#include <span>
#include <vector>

#include "txn_block_deltas.hpp"
#include "txn_history.hpp"

namespace chunkdb {

// Borrowed owners remain unchanged and exclusively locked until this stage
// is destroyed. Views are ordered by chunk coordinate within one table.
struct TxnBlockMutableChunk {
    ChunkCoord coord{};
    std::span<std::uint8_t> payload;
    std::span<std::uint8_t> presence;
    ChunkVars* vars = nullptr;
    std::shared_ptr<const ChunkColumnState>* chunk_columns = nullptr;
    std::uint64_t* version = nullptr;
    std::uint64_t* commit_time_ms = nullptr;
    bool* written = nullptr;
    std::size_t* present_blocks = nullptr;
};

// Physical preparation only; its caller owns revision allocation, WAL,
// durable decisions and publication while retaining canonical chunk locks.
class PreparedTxnBlockStage {
  public:
    [[nodiscard]] static PreparedTxnBlockStage Prepare(const TxnBlockDeltas& deltas,
        const StoreId& current_table_id, const Geometry& geometry,
        std::span<const TxnBlockMutableChunk> current, std::shared_ptr<TxnHistory> history,
        const std::shared_ptr<detail::ChunkStorageAccount>& account, std::size_t variable_limit);
    PreparedTxnBlockStage(PreparedTxnBlockStage&& other) noexcept;
    PreparedTxnBlockStage& operator=(PreparedTxnBlockStage&&) = delete;
    PreparedTxnBlockStage(const PreparedTxnBlockStage&) = delete;
    PreparedTxnBlockStage& operator=(const PreparedTxnBlockStage&) = delete;
    ~PreparedTxnBlockStage();

    [[nodiscard]] bool empty() const noexcept { return chunks_.empty(); }
    [[nodiscard]] std::size_t changed_chunks() const noexcept { return chunks_.size(); }
    // This tag is supplied by the future coordinator; this API allocates none.
    // Standalone preparation always captures history, regardless of OpenCount.
    void BindHistoryTag(std::uint64_t tag);
    void Apply() noexcept;
    void Undo() noexcept;
    // Only after the caller has crossed its irreversible publication point.
    void Commit() noexcept;

  private:
    enum class Phase { kPrepared, kBound, kApplied, kUndone, kCommitted, kMoved };
    struct ByteEdit {
        std::size_t chunk = 0U;
        std::size_t offset = 0U;
        bool presence = false;
        std::uint8_t before = 0U;
        std::uint8_t after = 0U;
    };
    struct ChunkEdit {
        TxnBlockMutableChunk target;
        detail::ChunkStoragePolicyPtr replacement_policy;
        ChunkVars replacement;
        TxnHistory::Reservation history;
        std::shared_ptr<const ChunkColumnState> metadata;
        const ChunkColumnState* original_metadata = nullptr;
        std::uint64_t before_version = 0U;
        std::uint64_t before_time = 0U;
        std::size_t before_present = 0U;
        std::size_t after_present = 0U;
        bool before_written = false;
        ChunkEdit(TxnBlockMutableChunk owner, detail::ChunkStoragePolicyPtr policy,
            ChunkVars next, TxnHistory::Reservation before,
            std::shared_ptr<const ChunkColumnState> next_metadata) noexcept;
        ChunkEdit(ChunkEdit&&) noexcept = default;
        ChunkEdit& operator=(ChunkEdit&&) = delete;
    };
    template <class T> using Storage = std::vector<T, detail::ChunkStorageAllocator<T>>;
    PreparedTxnBlockStage(std::shared_ptr<TxnHistory> history, detail::ChunkStoragePolicyPtr policy) noexcept;
    std::shared_ptr<TxnHistory> history_;
    detail::ChunkStoragePolicyPtr policy_;
    Storage<ChunkEdit> chunks_;
    Storage<ByteEdit> edits_;
    Phase phase_ = Phase::kPrepared;
};

}  // namespace chunkdb
