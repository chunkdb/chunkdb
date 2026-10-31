#include "reply_stream.hpp"

#include "chunkdb/protocol.hpp"
#include "statement_control.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <unordered_set>

namespace chunkdb {
namespace {
class ChunkContinuation final : public ReplyContinuation {
  public:
    ChunkContinuation(ChunkLayout layout, std::vector<ChunkRangeEntry> entries,
        std::vector<std::size_t> columns, bool area, bool with_schema, std::size_t max_bytes)
        : layout_(std::move(layout)), entries_(std::move(entries)), all_(columns.empty()), area_(area) {
        if (!area && entries_.size() != 1U) throw std::invalid_argument("a chunk reply needs one state");
        for (const auto column : columns) {
            if (column >= layout_.schema().columns.size()) throw std::invalid_argument("reply column is out of range");
            if (const auto* fixed = layout_.FixedColumnAt(column)) {
                const auto& fixed_columns = layout_.fixed_columns();
                const auto next = std::find_if(fixed_columns.begin(), fixed_columns.end(), [fixed](const auto& other) {
                    return other.values > fixed->values;
                });
                const auto end = next == fixed_columns.end() ? layout_.payload_bytes() : next->values;
                sections_.emplace_back(fixed->values, end - fixed->values);
            } else variable_ids_.insert(layout_.schema().columns[column].id);
        }
        if (area) {
            if (with_schema) {
                Protocol::AppendArrayHeader(header_, 2U);
                Protocol::AppendInteger(header_, layout_.schema().version);
            }
            Protocol::AppendArrayHeader(header_, entries_.size());
        }
        AddSize(header_.size(), max_bytes);
        for (const auto& entry : entries_) {
            CheckStatementCancellation();
            std::size_t size = 16U + entry.presence_bitmap.size();
            const auto add = [&](std::size_t bytes) {
                if (bytes > std::numeric_limits<std::size_t>::max() - size)
                    throw std::out_of_range("reply byte count overflows");
                size += bytes;
            };
            if (all_) { add(entry.payload.size()); add(entry.vars.encoded_size()); }
            else {
                for (const auto& [offset, bytes] : sections_) {
                    if (offset > entry.payload.size() || bytes > entry.payload.size() - offset)
                        throw std::logic_error("reply snapshot does not match its layout");
                    add(bytes);
                }
                for (const auto value : entry.vars) {
                    CheckStatementCancellation();
                    if (variable_ids_.contains(value.key.column_id)) add(kVarEntryHeaderBytes + value.value.size());
                }
            }
            forms_.push_back(size);
            std::string framing;
            if (area_) {
                Protocol::AppendArrayHeader(framing, 3U);
                Protocol::AppendInteger(framing, entry.coord.x);
                Protocol::AppendInteger(framing, entry.coord.y);
            }
            framing += "$" + std::to_string(size) + "\r\n";
            AddSize(framing.size(), max_bytes);
            AddSize(size, max_bytes);
            AddSize(2U, max_bytes);
        }
        current_ = header_;
    }
    std::string Next(std::size_t max_bytes) override {
        if (max_bytes == 0U) throw std::invalid_argument("reply continuation needs output credit");
        std::string out;
        out.reserve(std::min(max_bytes, size_));
        while (out.size() < max_bytes) {
            if (current_.empty()) {
                current_ = Part();
                if (current_.empty() && stage_ == Stage::kDone) break;
                if (current_.empty()) continue;
            }
            const auto take = std::min(max_bytes - out.size(), current_.size());
            out.append(current_.data(), take);
            current_.remove_prefix(take);
        }
        return out;
    }
    bool done() const noexcept override { return stage_ == Stage::kDone && current_.empty(); }
    std::size_t encoded_size() const noexcept override { return size_; }
  private:
    enum class Stage { kEntry, kPresence, kFixed, kVars, kVarValue, kTerminator, kDone };
    void AddSize(std::size_t bytes, std::size_t limit) {
        if (bytes > limit - std::min(size_, limit))
            throw std::out_of_range("reply exceeds the response byte limit");
        size_ += bytes;
    }
    static std::string_view Bytes(const std::vector<std::uint8_t>& bytes) {
        return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    }
    void Little(std::uint64_t value, std::size_t width) {
        for (std::size_t i = 0U; i < width; ++i) header_.push_back(static_cast<char>(value >> (8U * i)));
    }
    std::string_view Part() {
        if (stage_ == Stage::kDone) return {};
        if (stage_ == Stage::kEntry) {
            if (entry_ == entries_.size()) { stage_ = Stage::kDone; return {}; }
            header_.clear();
            const auto& entry = entries_[entry_];
            if (area_) {
                Protocol::AppendArrayHeader(header_, 3U);
                Protocol::AppendInteger(header_, entry.coord.x);
                Protocol::AppendInteger(header_, entry.coord.y);
            }
            header_ += "$" + std::to_string(forms_[entry_]) + "\r\n";
            Little(entry.version, 8U); Little(layout_.schema().version, 8U);
            section_ = variable_ = 0U;
            stage_ = Stage::kPresence;
            return header_;
        }
        const auto& entry = entries_[entry_];
        if (stage_ == Stage::kPresence) { stage_ = Stage::kFixed; return Bytes(entry.presence_bitmap); }
        if (stage_ == Stage::kFixed) {
            if (all_) { stage_ = Stage::kVars; return Bytes(entry.payload); }
            if (section_ < sections_.size()) {
                const auto [offset, size] = sections_[section_++];
                return Bytes(entry.payload).substr(offset, size);
            }
            stage_ = Stage::kVars;
        }
        if (stage_ == Stage::kVars) {
            if (all_) { stage_ = Stage::kTerminator; return Bytes(entry.vars.Encode()); }
            while (variable_ < entry.vars.size()) {
                const auto value = entry.vars.entry(variable_);
                if (!variable_ids_.contains(value.key.column_id)) { ++variable_; continue; }
                header_.clear();
                Little(value.key.column_id, 4U); Little(value.key.block_index, 4U); Little(value.value.size(), 4U);
                stage_ = Stage::kVarValue;
                return header_;
            }
            stage_ = Stage::kTerminator;
        }
        if (stage_ == Stage::kVarValue) {
            const auto value = entry.vars.entry(variable_++);
            stage_ = Stage::kVars;
            return {reinterpret_cast<const char*>(value.value.data()), value.value.size()};
        }
        if (stage_ == Stage::kTerminator) { ++entry_; stage_ = Stage::kEntry; return "\r\n"; }
        return {};
    }
    const ChunkLayout layout_;
    const std::vector<ChunkRangeEntry> entries_;
    const bool all_, area_;
    std::vector<std::pair<std::size_t, std::size_t>> sections_;
    std::unordered_set<std::uint32_t> variable_ids_;
    std::vector<std::size_t> forms_;
    std::size_t size_ = 0U, entry_ = 0U, section_ = 0U, variable_ = 0U;
    Stage stage_ = Stage::kEntry;
    std::string header_;
    std::string_view current_;
};
}
std::unique_ptr<ReplyContinuation> ChunkReply(ChunkLayout layout, std::vector<ChunkRangeEntry> entries,
    std::vector<std::size_t> columns, bool area, bool with_schema, std::size_t max_bytes) {
    return std::make_unique<ChunkContinuation>(std::move(layout), std::move(entries), std::move(columns),
        area, with_schema, max_bytes);
}
}  // namespace chunkdb
