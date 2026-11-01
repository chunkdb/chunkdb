#pragma once

#include <map>
#include <memory_resource>
#include <optional>
#include <span>

#include "chunkdb/chunk_store.hpp"
#include "txn_block_rows.hpp"

namespace chunkdb {

struct TxnBlockKey {
    ChunkCoord chunk{};
    std::size_t index = 0U;
    friend bool operator<(const TxnBlockKey& left, const TxnBlockKey& right) noexcept {
        return left.chunk < right.chunk || (left.chunk == right.chunk && left.index < right.index);
    }
};

enum class TxnBlockAction { kObserve, kReplace, kDelete };

struct TxnBlockDelta {
    TxnBlockSnapshot before;
    TxnBlockAction action = TxnBlockAction::kObserve;
    TxnBlockRow after;
    explicit TxnBlockDelta(std::pmr::memory_resource* resource) : before(resource), after(resource) {}
};

// Single-transaction state; the caller serializes edits and keeps its layout,
// resource and journal alive through every prepared edit and promotion.
class TxnBlockDeltas {
  public:
    using Rows = std::pmr::map<TxnBlockKey, TxnBlockDelta>;
    using Promotions = std::pmr::map<ChunkCoord, std::uint64_t>;
    class Edit {
      public:
        Edit(Edit&& other) noexcept;
        Edit& operator=(Edit&&) = delete;
        Edit(const Edit&) = delete;
        Edit& operator=(const Edit&) = delete;
      private:
        friend class TxnBlockDeltas;
        Edit(TxnBlockDeltas* owner, TxnBlockKey key);
        TxnBlockDeltas* owner_;
        std::uint64_t generation_;
        TxnBlockKey key_;
        Rows::node_type node_;
        TxnBlockAction action_ = TxnBlockAction::kObserve;
        TxnBlockRow after_;
        bool changes_ = false;
    };
    class Promotion {
      public:
        Promotion(Promotion&& other) noexcept;
        Promotion& operator=(Promotion&&) = delete;
        Promotion(const Promotion&) = delete;
        Promotion& operator=(const Promotion&) = delete;
      private:
        friend class TxnBlockDeltas;
        explicit Promotion(TxnBlockDeltas* owner);
        TxnBlockDeltas* owner_;
        std::uint64_t generation_;
        Promotions::node_type node_;
    };

    TxnBlockDeltas(StoreId table_id, const ChunkLayout& layout, std::pmr::memory_resource* resource);
    TxnBlockDeltas(const TxnBlockDeltas&) = delete;
    TxnBlockDeltas& operator=(const TxnBlockDeltas&) = delete;
    [[nodiscard]] Edit Prepare(TxnBlockKey key, const TxnBlockSnapshot& snapshot,
        TxnBlockAction action, const TxnBlockRow* replacement = nullptr);
    void Publish(Edit edit);
    // The caller first incorporates sparse deltas into its admitted whole-chunk state.
    [[nodiscard]] Promotion PreparePromotion(ChunkCoord coord, std::uint64_t snapshot_version);
    void Publish(Promotion promotion);
    [[nodiscard]] const TxnBlockRow* Read(TxnBlockKey key) const noexcept;
    [[nodiscard]] const Rows& rows() const noexcept { return rows_; }
    [[nodiscard]] const Promotions& promotions() const noexcept { return promotions_; }
    [[nodiscard]] const StoreId& table_id() const noexcept { return table_id_; }
    [[nodiscard]] const ChunkLayout& layout() const noexcept { return layout_; }
    [[nodiscard]] std::pmr::memory_resource* resource() const noexcept { return rows_.get_allocator().resource(); }

  private:
    void CanEdit() const;
    StoreId table_id_;
    const ChunkLayout& layout_;
    Rows rows_;
    Promotions promotions_;
    std::uint64_t generation_ = 0U;
};

// Views are borrowed only while the caller holds all canonical current-chunk locks.
struct TxnBlockCurrentChunk {
    ChunkCoord coord{};
    std::uint64_t version = 0U;
    bool written = false;
    std::span<const std::uint8_t> payload;
    std::span<const std::uint8_t> presence;
    const ChunkVars* vars = nullptr;
    const ChunkColumnState* chunk_columns = nullptr;
};

struct TxnBlockPlannedRow {
    TxnBlockKey key;
    TxnBlockRow after;
    explicit TxnBlockPlannedRow(TxnBlockKey value, TxnBlockRow row) : key(value), after(std::move(row)) {}
};

struct TxnBlockPlannedVariable {
    VarKey key{};
    bool removed = false;
    std::pmr::vector<std::uint8_t> value;
    explicit TxnBlockPlannedVariable(std::pmr::memory_resource* resource) : value(resource) {}
};

struct TxnBlockPlannedChunk {
    ChunkCoord coord{};
    std::size_t resulting_variable_entries = 0U;
    std::size_t resulting_variable_bytes = 0U;
    std::size_t current_variable_data_capacity = 0U;
    std::size_t resulting_metadata_variable_bytes = 0U;
    std::size_t removed_variable_bytes = 0U;
    std::size_t added_variable_bytes = 0U;
    std::pmr::vector<TxnBlockPlannedVariable> variables;
    // ChunkVars does not expose offset capacity or the complete physical merge peak.
    std::optional<std::size_t> physical_staging_bytes;
    explicit TxnBlockPlannedChunk(std::pmr::memory_resource* resource) : variables(resource) {}
};

struct TxnBlockRebasePlan {
    std::pmr::vector<TxnBlockPlannedRow> rows;
    std::pmr::vector<TxnBlockPlannedChunk> chunks;
    explicit TxnBlockRebasePlan(std::pmr::memory_resource* resource) : rows(resource), chunks(resource) {}
};

// Validates every dependency before allocating a plan; it cannot apply one to storage.
[[nodiscard]] TxnBlockRebasePlan PlanTxnBlockRebase(const TxnBlockDeltas& deltas,
    const StoreId& current_table_id, const Geometry& geometry,
    std::span<const TxnBlockCurrentChunk> current, std::size_t variable_limit);

}  // namespace chunkdb
