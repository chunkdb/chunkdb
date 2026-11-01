#include "txn_block_stage.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <limits>
#include <memory_resource>
#include <stdexcept>
#include <utility>

#include "sectioned_chunk_form.hpp"

namespace chunkdb {
namespace {
class PreparedMemoryResource final : public std::pmr::memory_resource {
  public:
    explicit PreparedMemoryResource(detail::ChunkStoragePolicyPtr policy) noexcept : policy_(std::move(policy)) {}
  private:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        return detail::AllocateChunkStorage(bytes, alignment, policy_.get(), policy_->upstream());
    }
    void do_deallocate(void* memory, std::size_t, std::size_t) override { detail::DeallocateChunkStorage(memory); }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
    detail::ChunkStoragePolicyPtr policy_;
};
struct Patch {
    std::size_t chunk = 0U;
    std::size_t offset = 0U;
    bool presence = false;
    std::uint8_t mask = 0U;
    std::uint8_t value = 0U;
};
std::size_t CountPresent(std::span<const std::uint8_t> presence) noexcept {
    std::size_t count = 0U;
    for (const auto byte : presence) count += std::popcount(byte);
    return count;
}
bool Present(std::span<const std::uint8_t> presence, std::size_t index) noexcept {
    return ((presence[index / 8U] >> (index % 8U)) & 1U) != 0U;
}
std::size_t ValidateMetadata(const TableSchema& schema, std::span<const std::uint8_t> bytes) {
    const auto count = ValidateChunkColumnDirectory(bytes);
    if (count != schema.columns.size()) throw std::invalid_argument("physical block stage metadata is incomplete");
    std::size_t variable = 0U;
    for (std::size_t i = 0U; i < count; ++i) {
        const auto entry = ChunkColumnDirectoryEntry(bytes, i);
        const auto found = std::find_if(schema.columns.begin(), schema.columns.end(),
            [&](const Column& column) { return column.id == entry.column_id; });
        if (found == schema.columns.end()) throw std::invalid_argument("physical block stage metadata has an unknown column");
        const auto& column = *found;
        if (entry.is_null) {
            if (!column.nullable) throw std::invalid_argument("physical block stage metadata has an invalid NULL");
        } else if (IsFixedWidth(column.type.kind)) {
            const auto width = FixedWidthBits(column.type);
            if (entry.value.size() != (width + 7U) / 8U ||
                (width % 8U != 0U && (entry.value.back() >> (width % 8U)) != 0U))
                throw std::invalid_argument("physical block stage metadata has invalid fixed bytes");
        } else {
            if (entry.value.size() > column.type.size || (column.type.kind == ColumnKind::kText && !IsUtf8(entry.value)))
                throw std::invalid_argument("physical block stage metadata has an invalid variable value");
            if (column.nullable || !entry.value.empty()) {
                if (variable > kVarMaxChunkBytesLimit - kVarEntryHeaderBytes ||
                    entry.value.size() > kVarMaxChunkBytesLimit - kVarEntryHeaderBytes - variable)
                    throw std::invalid_argument("physical block stage metadata exceeds its variable-byte bound");
                variable += kVarEntryHeaderBytes + entry.value.size();
            }
        }
    }
    return variable;
}
void ValidateOwner(const Geometry& geometry, const TxnBlockMutableChunk& chunk) {
    (void)geometry.ChunkToLarge(chunk.coord);
    if (!chunk.vars || !chunk.chunk_columns || !chunk.version || !chunk.commit_time_ms ||
        !chunk.written || !chunk.present_blocks || chunk.payload.size() != geometry.ChunkPayloadBytes() ||
        chunk.presence.size() != (geometry.ChunkBlockCount() + 7U) / 8U ||
        (chunk.payload.size() != 0U && !chunk.payload.data()) || !chunk.presence.data())
        throw std::invalid_argument("physical block stage has an incomplete current owner");
    if (*chunk.present_blocks != CountPresent(chunk.presence) ||
        (!*chunk.written && (*chunk.version != 0U || *chunk.commit_time_ms != 0U || *chunk.present_blocks != 0U)) ||
        (*chunk.written && *chunk.version == 0U))
        throw std::invalid_argument("physical block stage current producing state is inconsistent");
    if (geometry.ChunkBlockCount() % 8U != 0U &&
        (chunk.presence.back() >> (geometry.ChunkBlockCount() % 8U)) != 0U)
        throw std::invalid_argument("physical block stage presence padding is not zero");
    geometry.layout().RequireValidVarsView(*chunk.vars, chunk.presence);
    std::size_t metadata_bytes = 0U;
    if (*chunk.chunk_columns) {
        const auto& metadata = **chunk.chunk_columns;
        if (ValidateMetadata(geometry.chunk_schema(), metadata.bytes) != metadata.variable_bytes)
            throw std::invalid_argument("physical block stage metadata byte count differs");
        if (*chunk.written) metadata_bytes = metadata.variable_bytes;
    } else if (*chunk.written && !geometry.chunk_schema().columns.empty()) {
        throw std::invalid_argument("physical block stage written owner has no chunk metadata");
    }
    if (chunk.vars->encoded_size() > kVarMaxChunkBytesLimit ||
        metadata_bytes > kVarMaxChunkBytesLimit - chunk.vars->encoded_size())
        throw std::invalid_argument("physical block stage current owner exceeds its variable-byte bound");
}
const TxnBlockMutableChunk& FindMutable(std::span<const TxnBlockMutableChunk> current, ChunkCoord coord) {
    const auto found = std::lower_bound(current.begin(), current.end(), coord,
        [](const auto& chunk, ChunkCoord key) { return chunk.coord < key; });
    if (found == current.end() || !(found->coord == coord)) throw std::invalid_argument("physical block stage is missing a locked owner");
    return *found;
}
TxnRetainedStateView BeforeView(const TxnBlockMutableChunk& source) noexcept {
    const auto& metadata = *source.chunk_columns;
    return {.version = *source.version, .commit_time_ms = *source.commit_time_ms, .written = *source.written,
        .payload = source.payload, .presence_bitmap = source.presence, .vars = *source.vars,
        .legacy_metadata = metadata, .metadata_present = metadata != nullptr,
        .metadata_bytes = metadata ? std::span<const std::uint8_t>(metadata->bytes) : std::span<const std::uint8_t>{},
        .metadata_variable_bytes = metadata ? metadata->variable_bytes : 0U};
}
}  // namespace

PreparedTxnBlockStage::ChunkEdit::ChunkEdit(TxnBlockMutableChunk owner, detail::ChunkStoragePolicyPtr policy,
    ChunkVars next, TxnHistory::Reservation before, std::shared_ptr<const ChunkColumnState> next_metadata) noexcept
    : target(owner), replacement_policy(std::move(policy)), replacement(std::move(next)), history(std::move(before)),
      metadata(std::move(next_metadata)), original_metadata(owner.chunk_columns->get()),
      before_version(*owner.version), before_time(*owner.commit_time_ms), before_present(*owner.present_blocks),
      after_present(before_present), before_written(*owner.written) {}

PreparedTxnBlockStage::PreparedTxnBlockStage(std::shared_ptr<TxnHistory> history,
    detail::ChunkStoragePolicyPtr policy) noexcept
    : history_(std::move(history)), policy_(std::move(policy)),
      chunks_(detail::ChunkStorageAllocator<ChunkEdit>(policy_)), edits_(detail::ChunkStorageAllocator<ByteEdit>(policy_)) {}
PreparedTxnBlockStage::PreparedTxnBlockStage(PreparedTxnBlockStage&& other) noexcept
    : history_(std::move(other.history_)), policy_(std::move(other.policy_)), chunks_(std::move(other.chunks_)),
      edits_(std::move(other.edits_)), phase_(std::exchange(other.phase_, Phase::kMoved)) {}
PreparedTxnBlockStage::~PreparedTxnBlockStage() { if (phase_ == Phase::kApplied) Undo(); }

PreparedTxnBlockStage PreparedTxnBlockStage::Prepare(const TxnBlockDeltas& deltas,
    const StoreId& current_table_id, const Geometry& geometry, std::span<const TxnBlockMutableChunk> current,
    std::shared_ptr<TxnHistory> history, const std::shared_ptr<detail::ChunkStorageAccount>& account,
    std::size_t variable_limit) {
    if (!history || !account) throw std::invalid_argument("physical block stage needs history and an owning account");
    for (std::size_t i = 0U; i < current.size(); ++i) {
        ValidateOwner(geometry, current[i]);
        if (i != 0U && !(current[i - 1U].coord < current[i].coord))
            throw std::invalid_argument("physical block stage current owners are not unique and ordered");
        for (std::size_t j = 0U; j < i; ++j) {
            if (current[j].vars == current[i].vars || current[j].chunk_columns == current[i].chunk_columns ||
                current[j].version == current[i].version || current[j].commit_time_ms == current[i].commit_time_ms ||
                current[j].written == current[i].written || current[j].present_blocks == current[i].present_blocks ||
                (!current[i].payload.empty() && current[j].payload.data() == current[i].payload.data()) ||
                current[j].presence.data() == current[i].presence.data())
                throw std::invalid_argument("physical block stage aliases different current owners");
        }
    }
    auto policy = detail::ChunkStoragePolicy::Create(account);
    PreparedTxnBlockStage stage(std::move(history), policy);
    PreparedMemoryResource resource(policy);
    Storage<TxnBlockCurrentChunk> read{detail::ChunkStorageAllocator<TxnBlockCurrentChunk>(policy)};
    read.reserve(current.size());
    for (const auto& source : current)
        read.push_back({source.coord, *source.version, *source.written, source.payload, source.presence,
            source.vars, source.chunk_columns->get()});
    auto plan = PlanTxnBlockRebase(deltas, current_table_id, geometry, read, variable_limit, &resource);
    // Inspect the entire changed cohort before any original policy is adopted.
    for (const auto& chunk : plan.chunks) TxnOriginalVariables::Validate(*FindMutable(current, chunk.coord).vars);
    stage.chunks_.reserve(plan.chunks.size());
    for (const auto& chunk : plan.chunks) {
        const auto& source = FindMutable(current, chunk.coord);
        auto replacement_policy = detail::ChunkStoragePolicy::Create(account);
        auto replacement = detail::ChunkVarsStorageAccess::Merge(*source.vars, chunk.variables,
            chunk.resulting_variable_entries, chunk.resulting_variable_bytes, replacement_policy);
        auto before = stage.history_->ReservePhysical(source.coord, BeforeView(source), *source.vars, account);
        auto metadata = *source.written ? *source.chunk_columns : geometry.default_chunk_columns();
        stage.chunks_.emplace_back(source, std::move(replacement_policy), std::move(replacement),
            std::move(before), std::move(metadata));
    }

    Storage<Patch> patches{detail::ChunkStorageAllocator<Patch>(policy)};
    std::size_t chunk_index = 0U;
    for (const auto& row : plan.rows) {
        while (chunk_index < stage.chunks_.size() && !(stage.chunks_[chunk_index].target.coord == row.key.chunk)) ++chunk_index;
        assert(chunk_index < stage.chunks_.size());
        auto& chunk = stage.chunks_[chunk_index];
        const auto was_present = Present(chunk.target.presence, row.key.index);
        if (was_present != row.after.present) {
            if (row.after.present) ++chunk.after_present; else --chunk.after_present;
        }
        patches.push_back({chunk_index, row.key.index / 8U, true,
            static_cast<std::uint8_t>(1U << (row.key.index % 8U)),
            static_cast<std::uint8_t>(row.after.present ? 1U << (row.key.index % 8U) : 0U)});
        for (std::size_t column = 0U; column < geometry.layout().schema().columns.size(); ++column) {
            const auto* fixed = geometry.layout().FixedColumnAt(column);
            if (!fixed) continue;
            const auto* cell = row.after.present ? &row.after.cells[column] : nullptr;
            if (fixed->validity != ChunkLayout::kNoValidity)
                patches.push_back({chunk_index, fixed->validity + row.key.index / 8U, false,
                    static_cast<std::uint8_t>(1U << (row.key.index % 8U)),
                    static_cast<std::uint8_t>(cell && !cell->null ? 1U << (row.key.index % 8U) : 0U)});
            // Gather masks in storage order without copying the fixed payload.
            const auto start = fixed->values * 8U + row.key.index * fixed->width;
            std::size_t remaining = fixed->width, written = 0U;
            while (remaining != 0U) {
                const auto bit_at = start + written;
                const auto first = bit_at % 8U;
                const auto count = std::min<std::size_t>(remaining, 8U - first);
                Patch patch{chunk_index, bit_at / 8U, false, 0U, 0U};
                for (std::size_t bit = 0U; bit < count; ++bit) {
                    const auto mask = static_cast<std::uint8_t>(1U << (first + bit));
                    patch.mask |= mask;
                    if (cell && !cell->null && ((cell->bytes[(written + bit) / 8U] >> ((written + bit) % 8U)) & 1U) != 0U)
                        patch.value |= mask;
                }
                patches.push_back(patch); written += count; remaining -= count;
            }
        }
    }
    std::sort(patches.begin(), patches.end(), [](const Patch& left, const Patch& right) {
        if (left.chunk != right.chunk) return left.chunk < right.chunk;
        if (left.presence != right.presence) return left.presence < right.presence;
        return left.offset < right.offset;
    });
    stage.edits_.reserve(patches.size());
    for (std::size_t at = 0U; at < patches.size();) {
        const auto& first = patches[at];
        const auto source = first.presence ? stage.chunks_[first.chunk].target.presence : stage.chunks_[first.chunk].target.payload;
        ByteEdit edit{first.chunk, first.offset, first.presence, source[first.offset], source[first.offset]};
        do {
            const auto& patch = patches[at++];
            edit.after = static_cast<std::uint8_t>((edit.after & static_cast<std::uint8_t>(~patch.mask)) | patch.value);
        } while (at < patches.size() && patches[at].chunk == first.chunk && patches[at].presence == first.presence && patches[at].offset == first.offset);
        if (edit.after != edit.before) stage.edits_.push_back(edit);
    }
    return stage;
}

void PreparedTxnBlockStage::BindHistoryTag(std::uint64_t tag) {
    if (phase_ != Phase::kPrepared || tag == 0U) throw std::invalid_argument("physical block stage tag binding is invalid");
    for (const auto& chunk : chunks_) {
        if (tag <= chunk.before_version || *chunk.target.version != chunk.before_version ||
            *chunk.target.commit_time_ms != chunk.before_time || *chunk.target.written != chunk.before_written ||
            *chunk.target.present_blocks != chunk.before_present || chunk.target.chunk_columns->get() != chunk.original_metadata)
            throw std::invalid_argument("physical block stage original owner changed before tag binding");
    }
    for (auto& chunk : chunks_) chunk.history.mapped().tag = tag;
    phase_ = Phase::kBound;
}
void PreparedTxnBlockStage::Apply() noexcept {
    if (phase_ != Phase::kBound) std::terminate();
    for (auto& chunk : chunks_) {
        auto& before = *chunk.history.mapped().physical;
        before.originals->Attach();
        std::swap(*chunk.target.vars, chunk.replacement);
        std::swap(before.vars, chunk.replacement);
        chunk.target.chunk_columns->swap(chunk.metadata);
        *chunk.target.written = true;
        *chunk.target.present_blocks = chunk.after_present;
    }
    for (const auto& edit : edits_) {
        auto bytes = edit.presence ? chunks_[edit.chunk].target.presence : chunks_[edit.chunk].target.payload;
        bytes[edit.offset] = edit.after;
    }
    phase_ = Phase::kApplied;
}
void PreparedTxnBlockStage::Undo() noexcept {
    if (phase_ != Phase::kApplied) return;
    for (auto it = edits_.rbegin(); it != edits_.rend(); ++it) {
        auto bytes = it->presence ? chunks_[it->chunk].target.presence : chunks_[it->chunk].target.payload;
        bytes[it->offset] = it->before;
    }
    for (auto it = chunks_.rbegin(); it != chunks_.rend(); ++it) {
        auto& before = *it->history.mapped().physical;
        std::swap(*it->target.vars, before.vars);
        before.originals->RestoreRegular();
        it->target.chunk_columns->swap(it->metadata);
        *it->target.version = it->before_version;
        *it->target.commit_time_ms = it->before_time;
        *it->target.written = it->before_written;
        *it->target.present_blocks = it->before_present;
    }
    phase_ = Phase::kUndone;
}
void PreparedTxnBlockStage::Commit() noexcept {
    if (phase_ != Phase::kApplied) std::terminate();
    phase_ = Phase::kCommitted;
    for (auto& chunk : chunks_) {
        history_->Publish(chunk.history);
        chunk.replacement_policy->TransferToRegularStorage();
        chunk.replacement_policy = {};
    }
}

}  // namespace chunkdb
