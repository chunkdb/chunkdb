#pragma once

#include <stdexcept>
#include <array>
#include <limits>

#include "chunkdb/chunk_vars.hpp"

namespace chunkdb::detail {

// Internal physical-staging access; no runtime grouped writer is enabled here.
struct ChunkVarsStorageAccess {
    [[nodiscard]] static ChunkVars Clone(const ChunkVars& source, const ChunkStoragePolicyPtr& policy) {
        if (!policy.get()) throw std::invalid_argument("charged chunk storage clone needs an explicit policy");
        ChunkVars result(policy);
        result.data_.assign(source.data_.begin(), source.data_.end());
        result.offsets_.assign(source.offsets_.begin(), source.offsets_.end());
        return result;
    }
    [[nodiscard]] static std::size_t BufferAllocationBytes(const ChunkVars& vars) noexcept {
        return ChunkStorageAllocationBytes(vars.data_.data()) + ChunkStorageAllocationBytes(vars.offsets_.data());
    }
    [[nodiscard]] static std::size_t OffsetCapacityBytes(const ChunkVars& vars) noexcept {
        return vars.offsets_.capacity() * sizeof(std::uint32_t);
    }
    [[nodiscard]] static std::array<void*, 2U> Buffers(ChunkVars& vars) noexcept {
        return {vars.data_.data(), vars.offsets_.data()};
    }
    [[nodiscard]] static std::array<ChunkStoragePolicy*, 2U> Policies(const ChunkVars& vars) noexcept {
        return {vars.data_.get_allocator().policy(), vars.offsets_.get_allocator().policy()};
    }
    // Merge sparse planned variables directly into charged physical storage.
    template <class Changes>
    [[nodiscard]] static ChunkVars Merge(const ChunkVars& base, const Changes& changes,
        std::size_t expected_entries, std::size_t expected_bytes, const ChunkStoragePolicyPtr& policy) {
        if (!policy.get() || expected_bytes > kVarMaxChunkBytesLimit || expected_entries > expected_bytes / kVarEntryHeaderBytes)
            throw std::invalid_argument("invalid charged variable replacement extent or policy");
        ChunkVars result(policy);
        result.data_.reserve(expected_bytes);
        result.offsets_.reserve(expected_entries);
        const auto append = [&](VarKey key, std::span<const std::uint8_t> value) {
            if (value.size() > std::numeric_limits<std::uint32_t>::max() ||
                result.data_.size() > expected_bytes || kVarEntryHeaderBytes > expected_bytes - result.data_.size() ||
                value.size() > expected_bytes - result.data_.size() - kVarEntryHeaderBytes ||
                result.offsets_.size() >= expected_entries)
                throw std::invalid_argument("charged variable replacement exceeds its admitted logical extent");
            std::array<std::uint8_t, kVarEntryHeaderBytes> header{};
            const std::array<std::uint32_t, 3U> words{key.column_id, key.block_index, static_cast<std::uint32_t>(value.size())};
            for (std::size_t word = 0U; word < words.size(); ++word)
                for (unsigned byte = 0U; byte < 4U; ++byte)
                    header[word * 4U + byte] = static_cast<std::uint8_t>(words[word] >> (byte * 8U));
            result.offsets_.push_back(static_cast<std::uint32_t>(result.data_.size()));
            result.data_.insert(result.data_.end(), header.begin(), header.end());
            result.data_.insert(result.data_.end(), value.begin(), value.end());
        };
        std::size_t at = 0U;
        std::optional<VarKey> previous;
        for (const auto& change : changes) {
            if (previous && !(*previous < change.key)) throw std::invalid_argument("charged variable changes are not ordered");
            previous = change.key;
            while (at < base.size() && base.entry(at).key < change.key) {
                const auto entry = base.entry(at++); append(entry.key, entry.value);
            }
            if (at < base.size() && base.entry(at).key == change.key) ++at;
            if (!change.removed) append(change.key, change.value);
        }
        while (at < base.size()) { const auto entry = base.entry(at++); append(entry.key, entry.value); }
        if (result.encoded_size() != expected_bytes || result.size() != expected_entries)
            throw std::invalid_argument("charged variable replacement differs from its admitted totals");
        return result;
    }
};

}  // namespace chunkdb::detail
