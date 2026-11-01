#include "txn_block_deltas.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace chunkdb {
namespace {
std::pmr::memory_resource* RequireResource(std::pmr::memory_resource* resource) {
    if (!resource) throw std::invalid_argument("transaction block deltas need a memory resource");
    return resource;
}
void SwapRows(TxnBlockRow& left, TxnBlockRow& right) noexcept {
    std::swap(left.schema_version, right.schema_version);
    std::swap(left.present, right.present);
    left.cells.swap(right.cells);
}
void CopySnapshot(TxnBlockSnapshot& destination, const TxnBlockSnapshot& source) {
    auto row = CloneTxnBlockRow(source.row, destination.row.resource());
    SwapRows(destination.row, row);
    destination.version = source.version;
    destination.commit_time_ms = source.commit_time_ms;
    destination.written = source.written;
}
const TxnBlockCurrentChunk& FindCurrent(std::span<const TxnBlockCurrentChunk> current, ChunkCoord coord) {
    const auto found = std::lower_bound(current.begin(), current.end(), coord,
        [](const TxnBlockCurrentChunk& chunk, ChunkCoord key) { return chunk.coord < key; });
    if (found == current.end() || !(found->coord == coord))
        throw std::invalid_argument("transaction block rebase is missing a locked current chunk");
    return *found;
}
bool Removed(const Column& column, const TxnBlockCell* cell) {
    return !cell || cell->null || (!column.nullable && cell->bytes.empty());
}
bool EqualValue(std::optional<std::span<const std::uint8_t>> current, const Column& column, const TxnBlockCell* cell) {
    if (Removed(column, cell)) return !current.has_value();
    return current && std::equal(current->begin(), current->end(), cell->bytes.begin(), cell->bytes.end());
}
void Add(std::size_t& total, std::size_t amount) {
    if (amount > std::numeric_limits<std::size_t>::max() - total)
        throw std::invalid_argument("transaction variable-byte arithmetic overflow");
    total += amount;
}
}  // namespace

TxnBlockPlannedVariable& TxnBlockPlannedVariable::operator=(TxnBlockPlannedVariable&& other) {
    if (this == &other) return *this;
    if (value.get_allocator().resource() != other.value.get_allocator().resource())
        throw std::invalid_argument("transaction variable plans cannot move between memory resources");
    value = std::move(other.value);
    key = other.key; removed = other.removed;
    return *this;
}

TxnBlockDeltas::Edit::Edit(TxnBlockDeltas* owner, TxnBlockKey key)
    : owner_(owner), generation_(owner->generation_), key_(key), after_(owner->resource()) {}
TxnBlockDeltas::Promotion::Promotion(TxnBlockDeltas* owner)
    : owner_(owner), generation_(owner->generation_) {}
TxnBlockDeltas::Edit::Edit(Edit&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)), generation_(other.generation_), key_(other.key_),
      node_(std::move(other.node_)), action_(other.action_), after_(std::move(other.after_)), changes_(other.changes_) {}
TxnBlockDeltas::Promotion::Promotion(Promotion&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)), generation_(other.generation_), node_(std::move(other.node_)) {}

TxnBlockDeltas::TxnBlockDeltas(StoreId table_id, const ChunkLayout& layout, std::pmr::memory_resource* resource)
    : table_id_(table_id), layout_(layout), rows_(RequireResource(resource)), promotions_(resource) {
    if (std::none_of(table_id_.begin(), table_id_.end(), [](auto byte) { return byte != 0U; }))
        throw std::invalid_argument("transaction block journal needs a table identity");
}

void TxnBlockDeltas::CanEdit() const {
    if (generation_ == std::numeric_limits<std::uint64_t>::max())
        throw std::invalid_argument("transaction block journal generation is exhausted");
}

TxnBlockDeltas::Edit TxnBlockDeltas::Prepare(TxnBlockKey key, const TxnBlockSnapshot& snapshot,
    TxnBlockAction action, const TxnBlockRow* replacement) {
    CanEdit();
    if (key.index >= layout_.block_count() || promotions_.contains(key.chunk))
        throw std::invalid_argument("transaction block index is invalid or its chunk is promoted");
    ValidateTxnBlockRow(layout_, snapshot.row);
    if ((!snapshot.written && (snapshot.version != 0U || snapshot.commit_time_ms != 0U || snapshot.row.present)) ||
        (snapshot.written && snapshot.version == 0U))
        throw std::invalid_argument("transaction block snapshot producing identity is inconsistent");
    if (action == TxnBlockAction::kReplace) {
        if (!replacement || !replacement->present) throw std::invalid_argument("a replacement block must be present");
        ValidateTxnBlockRow(layout_, *replacement);
    } else if (replacement) {
        throw std::invalid_argument("only a replacement can supply a block row");
    }
    if (action != TxnBlockAction::kObserve && action != TxnBlockAction::kReplace && action != TxnBlockAction::kDelete)
        throw std::invalid_argument("invalid transaction block action");
    Edit edit(this, key); edit.action_ = action;
    const auto existing = rows_.find(key);
    if (existing == rows_.end()) {
        const auto first = rows_.lower_bound({key.chunk, 0U});
        if (first != rows_.end() && first->first.chunk == key.chunk &&
            (first->second.before.version != snapshot.version || first->second.before.commit_time_ms != snapshot.commit_time_ms ||
                first->second.before.written != snapshot.written))
            throw std::invalid_argument("transaction block observations disagree on their chunk snapshot");
        Rows staged(resource());
        auto inserted = staged.try_emplace(key, resource()).first;
        auto& entry = inserted->second;
        CopySnapshot(entry.before, snapshot); entry.action = action;
        entry.after.schema_version = layout_.schema().version;
        if (replacement) {
            auto row = CloneTxnBlockRow(*replacement, resource());
            SwapRows(entry.after, row);
        }
        edit.node_.emplace(staged.extract(inserted));
        edit.changes_ = true;
    } else if (action != TxnBlockAction::kObserve) {
        edit.after_.schema_version = layout_.schema().version;
        if (replacement) {
            auto row = CloneTxnBlockRow(*replacement, resource());
            SwapRows(edit.after_, row);
        }
        edit.changes_ = true;
    }
    return edit;
}

void TxnBlockDeltas::Publish(Edit edit) {
    if (edit.owner_ != this || edit.generation_ != generation_)
        throw std::invalid_argument("transaction block edit is stale or belongs to another journal");
    if (!edit.changes_) return;
    CanEdit();
    if (edit.node_) rows_.insert(std::move(*edit.node_));
    else {
        auto& entry = rows_.find(edit.key_)->second;
        SwapRows(entry.after, edit.after_); entry.action = edit.action_;
    }
    ++generation_;
}

TxnBlockDeltas::Promotion TxnBlockDeltas::PreparePromotion(ChunkCoord coord, std::uint64_t snapshot_version) {
    CanEdit();
    if (promotions_.contains(coord)) throw std::invalid_argument("transaction chunk is already promoted");
    for (const auto& [key, entry] : rows_)
        if (key.chunk == coord && entry.before.version != snapshot_version)
            throw std::invalid_argument("transaction promotion has a different snapshot revision");
    Promotion promotion(this);
    Promotions staged(resource());
    const auto inserted = staged.emplace(coord, snapshot_version).first;
    promotion.node_.emplace(staged.extract(inserted));
    return promotion;
}

void TxnBlockDeltas::Publish(Promotion promotion) {
    if (promotion.owner_ != this || promotion.generation_ != generation_ || !promotion.node_ || promotion.node_->empty())
        throw std::invalid_argument("transaction promotion is stale or belongs to another journal");
    CanEdit();
    const auto coord = promotion.node_->key();
    promotions_.insert(std::move(*promotion.node_));
    for (auto it = rows_.begin(); it != rows_.end();)
        if (it->first.chunk == coord) it = rows_.erase(it); else ++it;
    ++generation_;
}

const TxnBlockRow* TxnBlockDeltas::Read(TxnBlockKey key) const noexcept {
    const auto found = rows_.find(key);
    if (found == rows_.end()) return nullptr;
    return found->second.action == TxnBlockAction::kObserve ? &found->second.before.row : &found->second.after;
}

TxnBlockRebasePlan PlanTxnBlockRebase(const TxnBlockDeltas& deltas,
    const StoreId& current_table_id, const Geometry& geometry,
    std::span<const TxnBlockCurrentChunk> current, std::size_t variable_limit) {
    if (deltas.table_id() != current_table_id || geometry.layout().schema() != deltas.layout().schema() ||
        geometry.layout().block_count() != deltas.layout().block_count())
        throw TransactionConflictError(TxnConflictReason::kTableChanged, "transaction block rebase table or schema changed");
    RequireValidVarLimit(variable_limit);
    for (std::size_t i = 0U; i < current.size(); ++i) {
        (void)geometry.ChunkToLarge(current[i].coord);
        if (!current[i].vars || (i != 0U && !(current[i - 1U].coord < current[i].coord)))
            throw std::invalid_argument("transaction block current views must be complete and ordered");
    }
    for (const auto& [key, entry] : deltas.rows()) {
        const auto& chunk = FindCurrent(current, key.chunk);
        if (!MatchesTxnBlockRow(deltas.layout(), chunk.payload, chunk.presence, *chunk.vars, key.index, entry.before.row))
            throw TransactionConflictError(TxnConflictReason::kChunkChanged, "transaction block preimage conflict");
    }
    for (const auto& [coord, revision] : deltas.promotions())
        if (FindCurrent(current, coord).version != revision)
            throw TransactionConflictError(TxnConflictReason::kChunkChanged, "transaction promoted chunk revision conflict");

    TxnBlockRebasePlan plan(deltas.resource());
    for (const auto& [key, entry] : deltas.rows()) {
        if (entry.action == TxnBlockAction::kObserve) continue;
        const auto& chunk = FindCurrent(current, key.chunk);
        if (MatchesTxnBlockRow(deltas.layout(), chunk.payload, chunk.presence, *chunk.vars, key.index, entry.after)) continue;
        plan.rows.emplace_back(key, CloneTxnBlockRow(entry.after, deltas.resource()));
        if (plan.chunks.empty() || !(plan.chunks.back().coord == key.chunk)) {
            plan.chunks.emplace_back(deltas.resource());
            auto& output = plan.chunks.back(); output.coord = key.chunk;
            output.resulting_variable_entries = chunk.vars->size();
            output.resulting_variable_bytes = chunk.vars->encoded_size();
            output.current_variable_data_capacity = chunk.vars->Encode().capacity();
            output.resulting_metadata_variable_bytes = chunk.chunk_columns ? chunk.chunk_columns->variable_bytes : 0U;
            if (!chunk.written && entry.after.present) {
                for (const auto& column : geometry.chunk_schema().columns)
                    if (column.required && !column.has_default)
                        throw std::invalid_argument("the first block write must initialize REQUIRED chunk columns");
                const auto baseline = geometry.default_chunk_columns();
                output.resulting_metadata_variable_bytes = baseline ? baseline->variable_bytes : 0U;
            }
        }
        auto& output = plan.chunks.back();
        for (std::size_t column_index = 0U; column_index < deltas.layout().schema().columns.size(); ++column_index) {
            const auto& column = deltas.layout().schema().columns[column_index];
            if (IsFixedWidth(column.type.kind)) continue;
            const VarKey var_key{column.id, static_cast<std::uint32_t>(key.index)};
            const auto before = chunk.vars->Find(var_key);
            const auto* after = entry.after.present ? &entry.after.cells[column_index] : nullptr;
            if (EqualValue(before, column, after)) continue;
            output.variables.emplace_back(deltas.resource());
            auto& change = output.variables.back(); change.key = var_key; change.removed = Removed(column, after);
            if (before) { --output.resulting_variable_entries; Add(output.removed_variable_bytes, kVarEntryHeaderBytes + before->size()); }
            if (!change.removed) {
                change.value.assign(after->bytes.begin(), after->bytes.end());
                ++output.resulting_variable_entries;
                Add(output.added_variable_bytes, kVarEntryHeaderBytes + change.value.size());
            }
        }
    }
    for (auto& chunk : plan.chunks) {
        std::sort(chunk.variables.begin(), chunk.variables.end(), [](const auto& left, const auto& right) { return left.key < right.key; });
        const auto& before = FindCurrent(current, chunk.coord);
        if (chunk.removed_variable_bytes > chunk.resulting_variable_bytes)
            throw std::invalid_argument("transaction removed variable bytes exceed their current section");
        chunk.resulting_variable_bytes -= chunk.removed_variable_bytes;
        Add(chunk.resulting_variable_bytes, chunk.added_variable_bytes);
        const auto metadata_before = before.written && before.chunk_columns ? before.chunk_columns->variable_bytes : 0U;
        if (chunk.resulting_variable_bytes > kVarMaxChunkBytesLimit ||
            chunk.resulting_metadata_variable_bytes > kVarMaxChunkBytesLimit - chunk.resulting_variable_bytes)
            throw std::invalid_argument("transaction block and chunk values exceed the combined variable-byte limit");
        const auto after_bytes = chunk.resulting_variable_bytes + chunk.resulting_metadata_variable_bytes;
        if (after_bytes > variable_limit && after_bytes > before.vars->encoded_size() + metadata_before)
            throw std::invalid_argument("transaction block values grow beyond the configured variable-byte limit");
    }
    return plan;
}

}  // namespace chunkdb
