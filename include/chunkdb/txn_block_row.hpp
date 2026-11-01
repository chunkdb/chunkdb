#pragma once

#include <cstdint>
#include <memory_resource>
#include <vector>

namespace chunkdb {

struct TxnBlockCell {
    std::uint32_t column_id = 0U;
    bool null = false;
    std::pmr::vector<std::uint8_t> bytes;

    explicit TxnBlockCell(std::pmr::memory_resource* resource);
    TxnBlockCell(TxnBlockCell&&) noexcept = default;
    TxnBlockCell& operator=(TxnBlockCell&&) = default;
    TxnBlockCell(const TxnBlockCell&) = delete;
    TxnBlockCell& operator=(const TxnBlockCell&) = delete;
    friend bool operator==(const TxnBlockCell&, const TxnBlockCell&) = default;
};

// The resource outlives the row and every nested value buffer.
// Fixed values retain raw bits, including NaN payloads and signed zero.
struct TxnBlockRow {
    std::uint64_t schema_version = 0U;
    bool present = false;
    std::pmr::vector<TxnBlockCell> cells;

    explicit TxnBlockRow(std::pmr::memory_resource* resource = std::pmr::get_default_resource());
    TxnBlockRow(TxnBlockRow&&) noexcept = default;
    TxnBlockRow& operator=(TxnBlockRow&&) = default;
    TxnBlockRow(const TxnBlockRow&) = delete;
    TxnBlockRow& operator=(const TxnBlockRow&) = delete;
    [[nodiscard]] std::pmr::memory_resource* resource() const noexcept { return cells.get_allocator().resource(); }
    friend bool operator==(const TxnBlockRow&, const TxnBlockRow&) = default;
};

struct TxnBlockSnapshot {
    TxnBlockRow row;
    std::uint64_t version = 0U;
    std::uint64_t commit_time_ms = 0U;
    bool written = false;

    explicit TxnBlockSnapshot(std::pmr::memory_resource* resource = std::pmr::get_default_resource()) : row(resource) {}
};

}  // namespace chunkdb
