#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

namespace chunkdb::server_detail {

// One timer per connection. The owner keeps entries at stable addresses and
// removes them before releasing their storage.
class ConnectionDeadlines {
    static constexpr std::size_t kUnqueued = static_cast<std::size_t>(-1);

  public:
    using TimePoint = std::chrono::steady_clock::time_point;

    ConnectionDeadlines() = default;
    ConnectionDeadlines(const ConnectionDeadlines&) = delete;
    ConnectionDeadlines& operator=(const ConnectionDeadlines&) = delete;

    class Entry {
      public:
        explicit Entry(std::uint64_t token) noexcept : token_(token) {}
        Entry(const Entry&) = delete;
        Entry& operator=(const Entry&) = delete;

      private:
        friend class ConnectionDeadlines;
        std::uint64_t token_ = 0U;
        TimePoint at_ = TimePoint::max();
        std::size_t index_ = kUnqueued;
    };

    void Set(Entry& entry, TimePoint at) {
        if (at == entry.at_) return;
        if (at == TimePoint::max()) { Remove(entry); return; }
        if (entry.index_ == kUnqueued) {
            heap_.push_back(&entry);
            entry.index_ = heap_.size() - 1U;
        }
        const auto before = std::exchange(entry.at_, at);
        if (at < before) SiftUp(entry.index_);
        else SiftDown(entry.index_);
    }

    void Remove(Entry& entry) noexcept {
        if (entry.index_ == kUnqueued) return;
        const auto index = entry.index_;
        const auto before = entry.at_;
        Swap(index, heap_.size() - 1U);
        heap_.pop_back();
        entry.index_ = kUnqueued;
        entry.at_ = TimePoint::max();
        if (index == heap_.size()) return;
        if (heap_[index]->at_ < before) SiftUp(index);
        else SiftDown(index);
    }

    [[nodiscard]] std::optional<TimePoint> Next() const noexcept {
        if (heap_.empty()) return std::nullopt;
        return heap_.front()->at_;
    }

    [[nodiscard]] std::optional<std::uint64_t> TakeDue(TimePoint now) noexcept {
        if (heap_.empty() || heap_.front()->at_ > now) return std::nullopt;
        auto& entry = *heap_.front();
        Remove(entry);
        return entry.token_;
    }

    [[nodiscard]] std::size_t size() const noexcept { return heap_.size(); }

  private:
    void Swap(std::size_t left, std::size_t right) noexcept {
        std::swap(heap_[left], heap_[right]);
        heap_[left]->index_ = left;
        heap_[right]->index_ = right;
    }

    void SiftUp(std::size_t index) noexcept {
        while (index != 0U) {
            const auto parent = (index - 1U) / 2U;
            if (heap_[parent]->at_ <= heap_[index]->at_) break;
            Swap(parent, index);
            index = parent;
        }
    }

    void SiftDown(std::size_t index) noexcept {
        while (index < heap_.size() / 2U) {
            auto child = index * 2U + 1U;
            if (child + 1U < heap_.size() && heap_[child + 1U]->at_ < heap_[child]->at_) ++child;
            if (heap_[index]->at_ <= heap_[child]->at_) break;
            Swap(index, child);
            index = child;
        }
    }

    std::vector<Entry*> heap_;
};

}  // namespace chunkdb::server_detail
