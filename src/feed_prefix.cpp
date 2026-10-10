#include "feed_prefix.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <regex>
#include <stdexcept>

#include "chunk_store_internal.hpp"
#include "chunkdb/file_layout.hpp"
#include "wal_replay.hpp"

namespace chunkdb {
namespace {
template <typename T>
T Load(std::span<const std::uint8_t> bytes, std::size_t at) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) value |= static_cast<std::uint64_t>(bytes[at + i]) << (8U * i);
    return static_cast<T>(value);
}
std::uint64_t FrameSize(std::span<const std::uint8_t> bytes) {
    if (bytes.size() < kWalFrameFixedHeaderSize ||
        std::memcmp(bytes.data(), kWalFrameMagic, kWalFrameMagicSize) != 0)
        throw std::runtime_error("feed WAL frame header is invalid");
    const auto header = kWalFrameFixedHeaderSize + Load<std::uint16_t>(bytes, 22U) + kWalFrameHeaderCrcSize;
    return static_cast<std::uint64_t>(header) + Load<std::uint32_t>(bytes, 28U) + kWalFrameTrailerSize;
}
}

void FeedWalPrefixIndex::Seed(const std::filesystem::path& root, const Geometry& geometry, const StoreId& epoch, FeatureFlags features) {
    Clear();
    static const std::regex name(R"(^C_(-?[0-9]+)_(-?[0-9]+)\.wal$)");
    for (const auto& large : std::filesystem::directory_iterator(root)) {
        if (!large.is_directory() || !large.path().filename().string().starts_with("L_")) continue;
        for (const auto& item : std::filesystem::directory_iterator(large.path())) {
            std::smatch match;
            const auto filename = item.path().filename().string();
            if (!std::regex_match(filename, match, name)) continue;
            const ChunkCoord coord{std::stoll(match[1].str()), std::stoll(match[2].str())};
            auto entry = std::make_shared<Entry>();
            entries_.emplace(std::make_pair(coord.x, coord.y), entry);
            try {
                auto payload = std::vector<std::uint8_t>(geometry.layout().payload_bytes(), 0U);
                auto presence = std::vector<std::uint8_t>(ChunkPresenceBitmapBytes(geometry), 0U);
                ChunkVars vars;
                std::uint64_t revision = 0, schema_version = 0;
                const auto image_path = ChunkDataPath(root, geometry, coord);
                if (std::filesystem::exists(image_path)) {
                    auto image = ParseChunkImage(LoadFile(image_path), geometry, coord, epoch, features);
                    payload = std::move(image.payload);
                    presence = std::move(image.presence_bitmap);
                    vars = std::move(image.vars);
                    revision = image.revision;
                    schema_version = image.schema_version;
                }
                std::vector<WalFrameBoundary> boundaries;
                const auto replay = ReplayWal(LoadFile(item.path()), geometry, coord, epoch, features,
                    revision, schema_version, &payload, &presence, &vars, &boundaries);
                if (replay.torn_creation) continue;
                if (!replay.replayable || (replay.tail_truncated_or_corrupt && !replay.stopped_at_crash_tail))
                    throw std::runtime_error("feed WAL cannot be replayed: " + replay.stop_reason);
                if (!replay.vars_problem.empty())
                    throw std::runtime_error("feed WAL leaves inconsistent values: " + replay.vars_problem);
                for (const auto& boundary : boundaries) entry->ends.emplace(boundary.revision, boundary.end);
            } catch (const std::runtime_error& error) {
                // Crash-shaped tails are excluded by recovery. Genuine damage
                // remains terminal for a captured completed prefix.
                entry->error = item.path().string() + ": " + error.what();
            } catch (const std::invalid_argument& error) {
                entry->error = item.path().string() + ": " + error.what();
            }
        }
    }
}

FeedWalPrefixIndex::Prepared FeedWalPrefixIndex::Prepare(ChunkCoord coord, std::uint64_t before,
    std::span<const std::uint8_t> bytes) {
    Prepared prepared;
    prepared.reset = before <= kWalHeaderSize;
    std::size_t at = 0;
    if (bytes.size() >= kWalMagicSize && std::memcmp(bytes.data(), kWalMagic, kWalMagicSize) == 0) {
        if (before != 0U || bytes.size() < kWalHeaderSize)
            throw std::logic_error("unexpected feed WAL header in append batch");
        at = kWalHeaderSize;
    }
    std::uint64_t offset = std::max<std::uint64_t>(before, kWalHeaderSize), previous = 0;
    while (at < bytes.size()) {
        const auto tail = bytes.subspan(at);
        const auto size = FrameSize(tail);
        if (size > tail.size() || size > std::numeric_limits<std::uint64_t>::max() - offset)
            throw std::logic_error("feed WAL append batch is truncated or oversized");
        const auto revision = Load<std::uint64_t>(tail, 4U);
        if (revision == 0U || revision <= previous)
            throw std::logic_error("feed WAL append batch revisions do not increase");
        offset += size;
        prepared.added.emplace(revision, offset);
        previous = revision;
        at += static_cast<std::size_t>(size);
    }
    auto candidate = std::make_shared<Entry>();
    std::lock_guard lock(mutex_);
    const auto [it, inserted] = entries_.try_emplace(std::make_pair(coord.x, coord.y), std::move(candidate));
    (void)inserted;
    prepared.entry = it->second;
    if (!prepared.reset && !prepared.entry->ends.empty()) {
        if (prepared.entry->ends.rbegin()->second > before)
            throw std::logic_error("feed WAL append overlaps the recorded prefix");
        if (!prepared.added.empty() && prepared.added.begin()->first <= prepared.entry->ends.rbegin()->first)
            throw std::logic_error("feed WAL append revision overlaps the recorded prefix");
    }
    return prepared;
}

void FeedWalPrefixIndex::Commit(Prepared&& prepared) noexcept {
    if (!prepared.entry) return;
    std::lock_guard lock(mutex_);
    if (prepared.reset) prepared.entry->ends.clear();
    prepared.entry->error.clear();
    prepared.entry->ends.merge(prepared.added);
}
void FeedWalPrefixIndex::Truncate(ChunkCoord coord, std::uint64_t boundary) noexcept {
    std::lock_guard lock(mutex_);
    const auto it = entries_.find({coord.x, coord.y});
    if (it == entries_.end()) return;
    if (boundary == 0U) {
        entries_.erase(it);
        return;
    }
    auto& entry = *it->second;
    while (!entry.ends.empty() && entry.ends.rbegin()->second > boundary)
        entry.ends.erase(std::prev(entry.ends.end()));
    entry.error.clear();
}
void FeedWalPrefixIndex::Clear() noexcept { std::lock_guard lock(mutex_); entries_.clear(); }
std::vector<FeedWalPrefix> FeedWalPrefixIndex::Capture(std::uint64_t through) const {
    std::lock_guard lock(mutex_);
    std::vector<FeedWalPrefix> result;
    result.reserve(entries_.size());
    for (const auto& [coord, entry] : entries_) {
        if (!entry->error.empty()) throw std::runtime_error(entry->error);
        auto last = entry->ends.upper_bound(through);
        if (last == entry->ends.begin()) continue;
        --last;
        result.push_back({{coord.first, coord.second}, entry->ends.begin()->first, last->first, last->second});
    }
    return result;
}
} // namespace chunkdb
