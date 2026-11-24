#include "chunk_coordinate_index.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <limits>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <streambuf>
#include <utility>

#include "checkpoint.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "store_manifest.hpp"
#include "wal_writer.hpp"
#include "shared_file_input.hpp"

namespace chunkdb {
namespace {

std::atomic<CoordinatePublicationTestHook*> publication_hook{nullptr};

class SinkBuffer final : public std::streambuf {
  public:
    explicit SinkBuffer(const AtomicFileSink& sink) : sink_(sink) {}
  private:
    std::streamsize xsputn(const char* at, std::streamsize size) override {
        sink_(std::span(reinterpret_cast<const std::uint8_t*>(at), static_cast<std::size_t>(size)));
        return size;
    }
    int_type overflow(int_type value) override {
        if (traits_type::eq_int_type(value, traits_type::eof())) return traits_type::not_eof(value);
        const auto byte = static_cast<std::uint8_t>(traits_type::to_char_type(value));
        sink_(std::span(&byte, 1U));
        return value;
    }
    const AtomicFileSink& sink_;
};

std::uint64_t StreamBytes(std::istream& input, const std::filesystem::path& path) {
    input.seekg(0, std::ios::end);
    const auto size = input.tellg();
    if (size < 0) throw std::runtime_error("failed to measure index file " + path.string());
    input.seekg(0);
    if (!input) throw std::runtime_error("failed to rewind index file " + path.string());
    return static_cast<std::uint64_t>(size);
}

struct FileChecksum { std::uint64_t bytes; crypto::Sha256Digest digest; };
FileChecksum ChecksumFile(const std::filesystem::path& path) {
    SharedFileInput input(path);
    if (!input) throw std::runtime_error("failed to open index file " + path.string());
    const auto bytes = StreamBytes(input, path);
    return {bytes, IndexFileDigest(input, bytes)};
}

bool MatchesReplacement(const std::filesystem::path& path, const IndexInventoryEntry& entry) {
    std::error_code ec;
    const auto status = std::filesystem::symlink_status(path, ec);
    if (ec == std::errc::no_such_file_or_directory || (!ec && !std::filesystem::exists(status))) return false;
    if (ec || !std::filesystem::is_regular_file(status))
        throw std::runtime_error("invalid compaction index file " + path.string());
    const auto file = ChecksumFile(path);
    return file.bytes == entry.replacement_bytes && file.digest == entry.replacement_sha256;
}

void CopyPrefix(std::istream& input, std::uint64_t size, const AtomicFileSink& sink) {
    std::array<std::uint8_t, 4096U> buffer{};
    while (size != 0U) {
        const auto count = static_cast<std::size_t>(std::min<std::uint64_t>(size, buffer.size()));
        input.read(reinterpret_cast<char*>(buffer.data()), static_cast<std::streamsize>(count));
        if (input.gcount() != static_cast<std::streamsize>(count))
            throw std::runtime_error("index prefix shortened during replacement");
        sink(std::span(buffer).first(count));
        size -= count;
    }
}

void RewritePrefix(const std::filesystem::path& path, std::uint64_t size) {
    SharedFileInput input(path);
    if (!input) throw std::runtime_error("failed to open index prefix " + path.string());
    AtomicWriteStream(path, [&](const AtomicFileSink& sink) {
        CopyPrefix(input, size, sink);
        input.close();
    },
        true, true, nullptr, nullptr, false);
}

void WriteInventoryFile(const std::filesystem::path& path, const IndexInventoryHeader& header,
    std::span<const IndexInventoryEntry> entries, bool* replaced = nullptr) {
    AtomicWriteStream(path, [&](const AtomicFileSink& sink) {
        SinkBuffer buffer(sink);
        std::ostream output(&buffer);
        output.exceptions(std::ios::badbit | std::ios::failbit);
        WriteIndexInventory(output, header, entries);
    }, true, true, replaced, nullptr, false);
}

void Crash(const char* point) { if (ConsumeFailpointEnv(point)) ExitAbruptly(86); }

// A split leaves two runs of at least half this many coordinates. Smaller
// runs make a page visit fewer coordinates inside a changed run; larger ones
// make it examine fewer unchanged runs.
constexpr std::size_t kRunNodes = 64U;
constexpr ChunkCoord kLowestCoord{std::numeric_limits<std::int64_t>::min(),
    std::numeric_limits<std::int64_t>::min(), std::numeric_limits<std::int64_t>::min()};

void RaiseRun(CoordinateRun& run, std::uint64_t revision) noexcept {
    auto current = run.maximum_revision.load(std::memory_order_relaxed);
    while (current < revision && !run.maximum_revision.compare_exchange_weak(current, revision,
        std::memory_order_release, std::memory_order_relaxed)) {}
}

}  // namespace

CoordinateReservation::CoordinateReservation(CoordinateReservation&& other) noexcept { *this = std::move(other); }
CoordinateReservation& CoordinateReservation::operator=(CoordinateReservation&& other) noexcept {
    if (this != &other) {
        Reset();
        owner_ = std::exchange(other.owner_, nullptr);
        node_ = other.node_;
        cache_slot_ = other.cache_slot_;
        coord_ = other.coord_;
        inserted_ = other.inserted_;
        live_node_ = std::move(other.live_node_);
    }
    return *this;
}
CoordinateReservation::~CoordinateReservation() { Reset(); }
void CoordinateReservation::Reset() noexcept { if (owner_) owner_->ReleaseReservation(*this); owner_ = nullptr; }
void CoordinateReservation::Publish(bool populated, std::uint64_t revision) noexcept {
    if (owner_) owner_->PublishReservation(*this, populated, revision);
    owner_ = nullptr;
}

std::filesystem::path ChunkCoordinateIndex::LogPath(const LargeChunkCoord& coord) const {
    return LargeChunkDirectory(root_, coord, geometry_.dims()) / kChunkIndexFileName;
}
std::filesystem::path ChunkCoordinateIndex::StagePath(const LargeChunkCoord& coord) const {
    return LogPath(coord).string() + ".compact";
}
void ChunkCoordinateIndex::Check() const {
    if (failed()) throw std::runtime_error("coordinate index requires recovery before serving");
}

ChunkCoordinateIndex::ChunkCoordinateIndex(std::filesystem::path root, const Geometry& geometry,
    StoreId table_id, bool read_only)
    : root_(std::move(root)), geometry_(geometry), table_id_(table_id), read_only_(read_only) {
    ReserveRunSplits(1U);
    auto entry = std::move(spare_run_entries_.back());
    spare_run_entries_.pop_back();
    auto* first = free_runs_.back();
    free_runs_.pop_back();
    first->first = kLowestCoord;
    entry.key() = kLowestCoord;
    entry.mapped() = first;
    runs_.insert(std::move(entry));
    const auto path = root_ / kChunkIndexFileName;
    SharedFileInput input(path);
    if (!input) throw std::runtime_error("missing or unreadable coordinate inventory " + path.string());
    inventory_ = ReadIndexInventory(input, StreamBytes(input, path), geometry_, table_id_, [&](const auto& entry) {
        shards_.emplace(entry.coord, Shard{.inventory = entry});
    });
    inventory_generation_.store(inventory_.generation, std::memory_order_relaxed);
    if (inventory_.compaction_pending && read_only_)
        throw std::runtime_error("coordinate compaction requires writer recovery");
    deletion_floor_.store(inventory_.retained_deletion_floor, std::memory_order_relaxed);
    maximum_recovered_revision_ = inventory_.retained_deletion_floor;
    for (auto& [coord, shard] : shards_) {
        const auto log = LogPath(coord);
        auto selected = log;
        auto cut = shard.inventory.committed_bytes;
        if (inventory_.compaction_pending) {
            shard.installed_replacement = MatchesReplacement(log, shard.inventory);
            if (!shard.installed_replacement) {
                selected = StagePath(coord);
                if (!MatchesReplacement(selected, shard.inventory))
                    throw std::runtime_error("missing checked compaction replacement " + selected.string());
            }
            cut = shard.inventory.replacement_bytes;
        }
        SharedFileInput file(selected);
        if (!file) throw std::runtime_error("missing listed coordinate log " + selected.string());
        shard.observed_bytes = StreamBytes(file, selected);
        AdmitLog(shard, selected, cut);
    }
}

void ChunkCoordinateIndex::AdmitLog(const Shard& shard, const std::filesystem::path& path, std::uint64_t cut) {
    SharedFileInput input(path);
    if (!input) throw std::runtime_error("failed to read coordinate log " + path.string());
    const auto log = ReadChunkIndexLog(input, StreamBytes(input, path), geometry_, table_id_, shard.inventory.coord, cut);
    for (const auto& [coord, record] : log.latest) {
        (void)coord;
        maximum_recovered_revision_ = std::max(maximum_recovered_revision_, record.revision);
    }
    for (const auto& [coord, boundary] : log.empty_boundaries) {
        if (boundary.kind == ChunkIndexKind::kForget && boundary.revision > inventory_.retained_deletion_floor)
            throw std::runtime_error("coordinate forget record exceeds the retained deletion floor");
        ReserveRunSplits(1U);
        const auto [at, inserted] = coordinates_.try_emplace(coord);
        if (inserted) AttachRun(at);
        auto& node = at->second;
        node.retained_empty = boundary;
        node.revision.store(boundary.kind == ChunkIndexKind::kForget ? 0U : boundary.revision, std::memory_order_relaxed);
        RaiseRun(*node.run.load(std::memory_order_relaxed), node.revision.load(std::memory_order_relaxed));
        if (boundary.kind != ChunkIndexKind::kForget) any_written_.store(true, std::memory_order_relaxed);
    }
}

bool ChunkCoordinateIndex::InitializeEmpty(const std::filesystem::path& root, const Geometry& geometry,
    const StoreId& table_id) {
    const auto path = root / kChunkIndexFileName;
    if (std::filesystem::exists(path)) return false;
    std::vector<std::uint8_t> bytes;
    std::ostringstream output;
    WriteIndexInventory(output, IndexInventoryHeader{.table_id = table_id, .dims = geometry.dims()}, {});
    const auto text = output.str();
    bytes.assign(text.begin(), text.end());
    return PublishNewFile(path, bytes, "CHUNKDB_FAILPOINT_CRASH_INDEX_INVENTORY_BEFORE_PUBLISH_ONCE");
}

std::optional<StoreId> ChunkCoordinateIndex::UnpublishedEmptyIdentity(const std::filesystem::path& root,
    std::uint8_t dims) {
    const auto path = root / kChunkIndexFileName;
    if (!std::filesystem::exists(path)) return {};
    SharedFileInput input(path);
    if (!input) throw std::runtime_error("failed to read unpublished coordinate inventory");
    std::array<std::uint8_t, 24U> prefix{};
    input.read(reinterpret_cast<char*>(prefix.data()), prefix.size());
    if (input.gcount() != static_cast<std::streamsize>(prefix.size()))
        throw std::runtime_error("unpublished coordinate inventory is truncated");
    StoreId id;
    std::copy_n(prefix.begin() + 8U, id.size(), id.begin());
    if (std::all_of(id.begin(), id.end(), [](auto byte) { return byte == 0U; }))
        throw std::runtime_error("unpublished coordinate inventory has a zero table identity");
    GeometryConfig config{1U, 1U, 1U, 1U, 1U}; config.dims = dims;
    const Geometry shape(config);
    const auto header = ReadIndexInventory(input, StreamBytes(input, path), shape, id);
    if (header.count != 0U || header.retained_deletion_floor != 0U || header.compaction_pending)
        throw std::runtime_error("unpublished coordinate inventory retains storage decisions");
    for (const auto& item : std::filesystem::directory_iterator(root)) {
        const auto name = item.path().filename().string();
        if (name == kChunkIndexFileName || name.starts_with(std::string(kChunkIndexFileName) + ".tmp.")) continue;
        if (IsStoreEntryName(name)) throw std::runtime_error("unpublished inventory has other owned storage: " + name);
    }
    return id;
}

void ChunkCoordinateIndex::PublishInventory(const IndexInventoryHeader& header, std::span<const IndexInventoryEntry> entries) {
    WriteInventoryFile(root_ / kChunkIndexFileName, header, entries);
    inventory_generation_.store(header.generation, std::memory_order_release);
}

void ChunkCoordinateIndex::CompleteRecovery() {
    if (read_only_) return;
    std::lock_guard publication(publication_mutex_);
    std::vector<IndexInventoryEntry> entries;
    entries.reserve(shards_.size());
    auto checked_unlisted = unlisted_;
    std::vector<std::pair<ChunkCoordinateNode*, ChunkIndexRecord>> boundaries;
    try {
        const auto publications = (inventory_.compaction_pending ? 1U : 0U) + (!unlisted_.empty() ? 1U : 0U);
        if (inventory_.generation > std::numeric_limits<std::uint64_t>::max() - publications)
            throw std::overflow_error("coordinate inventory generation cannot advance during recovery");
        // Reconcile every orphan before repairing even an unrelated suffix.
        for (auto& [coord, shard] : checked_unlisted) {
            const auto path = LogPath(coord);
            SharedFileInput input(path);
            if (!input) throw std::runtime_error("missing unpublished coordinate log " + path.string());
            const auto log = ReadChunkIndexLog(input, StreamBytes(input, path), geometry_, table_id_, coord);
            shard.inventory.committed_bytes = log.valid_bytes;
            for (const auto& [chunk, boundary] : log.empty_boundaries) {
                if (boundary.kind == ChunkIndexKind::kForget)
                    throw std::runtime_error("unpublished forget record has no inventory decision");
                std::lock_guard lock(mutex_);
                const auto at = coordinates_.find(chunk);
                if (at == coordinates_.end() || at->second.revision.load(std::memory_order_acquire) < boundary.revision ||
                    (at->second.revision.load(std::memory_order_relaxed) == boundary.revision &&
                        at->second.populated.load(std::memory_order_relaxed)))
                    throw std::runtime_error("unpublished empty boundary lacks its committed artifact");
                boundaries.emplace_back(&at->second, boundary);
            }
        }
        for (auto& [coord, shard] : shards_) {
            auto entry = shard.inventory;
            if (inventory_.compaction_pending) {
                if (!shard.installed_replacement) {
                    if (const auto error = ReplacePathAtomically(StagePath(coord), LogPath(coord)); error)
                        throw std::system_error(error, "failed to install decided coordinate compaction");
                }
                Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_COMPACTION_AFTER_REPLACE_ONCE");
                SyncDirectoryPath(LogPath(coord).parent_path());
                entry.committed_bytes = entry.replacement_bytes;
                entry.replacement_bytes = 0U;
                entry.replacement_sha256 = {};
            } else if (shard.observed_bytes > entry.committed_bytes) {
                RewritePrefix(LogPath(coord), entry.committed_bytes);
                shard.observed_bytes = entry.committed_bytes;
            }
            entries.push_back(entry);
        }
        if (inventory_.compaction_pending) {
            auto finished = inventory_;
            finished.compaction_pending = false;
            finished.compaction_through = 0U;
            ++finished.generation;
            PublishInventory(finished, entries);
            inventory_ = finished;
            std::size_t at = 0U;
            for (auto& [coord, shard] : shards_) { (void)coord; shard.inventory = entries[at++]; }
        }
        if (!unlisted_.empty()) {
            auto adopted = shards_;
            for (const auto& [coord, shard] : checked_unlisted) adopted.emplace(coord, shard);
            // All orphan decisions are checked before any of them is repaired.
            for (const auto& [coord, shard] : checked_unlisted) {
                if (shard.observed_bytes > shard.inventory.committed_bytes)
                    RewritePrefix(LogPath(coord), shard.inventory.committed_bytes);
                SyncFilePath(LogPath(coord));
                SyncDirectoryPath(LogPath(coord).parent_path());
            }
            entries.clear(); entries.reserve(adopted.size());
            for (const auto& [coord, shard] : adopted) { (void)coord; entries.push_back(shard.inventory); }
            auto decision = inventory_; decision.count = adopted.size(); ++decision.generation;
            PublishInventory(decision, entries);
            shards_.swap(adopted);
            inventory_ = decision;
            {
                std::lock_guard lock(mutex_);
                for (const auto& [node, boundary] : boundaries) node->retained_empty = boundary;
            }
            unlisted_.clear();
        }
        // A final inventory no longer depends on these owned staging names.
        for (const auto& [coord, shard] : shards_) {
            (void)shard;
            const auto stage = StagePath(coord);
            if (std::filesystem::remove(stage)) SyncDirectoryPath(stage.parent_path());
        }
    } catch (const std::exception&) { failed_.store(true, std::memory_order_release); throw; }
}

void ChunkCoordinateIndex::AdmitUnlistedLog(const LargeChunkCoord& coord) {
    if (shards_.contains(coord) || unlisted_.contains(coord)) return;
    const auto path = LogPath(coord);
    SharedFileInput input(path);
    if (!input) throw std::runtime_error("failed to read unpublished coordinate log " + path.string());
    const auto size = StreamBytes(input, path);
    const auto checked = ReadChunkIndexLog(input, size, geometry_, table_id_, coord);
    unlisted_.emplace(coord, Shard{.inventory = {.coord = coord, .committed_bytes = checked.valid_bytes},
        .observed_bytes = size});
    for (const auto& [chunk, record] : checked.latest) {
        (void)chunk;
        maximum_recovered_revision_ = std::max(maximum_recovered_revision_, record.revision);
    }
}

void ChunkCoordinateIndex::ObserveRecovered(const ChunkCoord& coord, const ChunkState& state) {
    if (state.written != (state.version != 0U))
        throw std::invalid_argument("recovered coordinate has an inconsistent producing identity");
    ObserveRecoveredIdentity(coord, state.version, ChunkPopulated(geometry_, state.presence_bitmap, state.chunk_columns.get()));
}

void ChunkCoordinateIndex::ObserveRecoveredIdentity(const ChunkCoord& coord, std::uint64_t revision, bool populated) {
    if (revision == 0U && populated)
        throw std::invalid_argument("unwritten recovered coordinate is populated");
    std::lock_guard lock(mutex_);
    ReserveRunSplits(1U);
    const auto [at, inserted] = coordinates_.try_emplace(coord);
    if (inserted) AttachRun(at);
    auto& node = at->second;
    if (node.retained_empty && revision <= node.retained_empty->revision) {
        if (revision == node.retained_empty->revision && populated)
            throw std::runtime_error("committed empty boundary conflicts with a populated chunk");
        return;
    }
    if (revision == 0U) {
        if (!node.retained_empty) {
            DetachRun(node);
            coordinates_.erase(at);
        }
        return;
    }
    node.populated.store(populated, std::memory_order_relaxed);
    node.revision.store(revision, std::memory_order_release);
    RaiseRun(*node.run.load(std::memory_order_relaxed), revision);
    any_written_.store(true, std::memory_order_release);
    if (populated) populated_.insert(coord);
    else populated_.erase(coord);
    maximum_recovered_revision_ = std::max(maximum_recovered_revision_, revision);
}

void ChunkCoordinateIndex::RefreshReadOnly(std::span<const RecoveredCoordinateIdentity> updates,
    const ChunkCoordinateIndex& retained, const std::function<void()>& validate) {
    if (!read_only_ || !retained.read_only_) throw std::logic_error("incremental refresh requires a read-only index");
    std::map<ChunkCoord, ChunkCoordinateNode> nodes;
    std::set<ChunkCoord> populated;
    auto highest = maximum_recovered_revision_;
    for (const auto& update : updates) {
        if (update.revision == 0U && update.populated)
            throw std::invalid_argument("unwritten recovered coordinate is populated");
        auto& node = nodes.try_emplace(update.coord).first->second;
        node.retained_empty = retained.EmptyBoundary(update.coord);
        auto revision = update.revision;
        auto live = update.populated;
        if (node.retained_empty && revision <= node.retained_empty->revision) {
            if (revision == node.retained_empty->revision && live)
                throw std::runtime_error("committed empty boundary conflicts with a populated chunk");
            revision = node.retained_empty->kind == ChunkIndexKind::kForget ? 0U : node.retained_empty->revision;
            live = false;
        }
        node.revision.store(revision, std::memory_order_relaxed);
        node.populated.store(live, std::memory_order_relaxed);
        if (live) populated.insert(update.coord);
        else populated.erase(update.coord);
        highest = std::max(highest, revision);
    }
    validate();
    // Node handles and reserved run splits make this publication
    // allocation-free. Page readers take the same lock and cannot observe
    // half of a multi-coordinate refresh.
    std::lock_guard lock(mutex_);
    // A split takes a run above kRunNodes down to two of about half, so each
    // run splits once and then once per kRunNodes / 2 further insertions.
    ReserveRunSplits(std::min(nodes.size(), runs_.size() + nodes.size() / (kRunNodes / 2U) + 1U));
    for (auto at = nodes.begin(); at != nodes.end();) {
        const auto coord = at->first;
        populated_.erase(coord);
        if (const auto old = coordinates_.find(coord); old != coordinates_.end()) {
            DetachRun(old->second);
            coordinates_.erase(old);
        }
        auto node = nodes.extract(at++);
        if (node.mapped().revision.load(std::memory_order_relaxed) != 0U || node.mapped().retained_empty) {
            const auto inserted = coordinates_.insert(std::move(node)).position;
            AttachRun(inserted);
        }
    }
    populated_.merge(populated);
    maximum_recovered_revision_ = highest;
    if (highest != 0U) any_written_.store(true, std::memory_order_release);
    deletion_floor_.store(retained.retained_deletion_floor(), std::memory_order_release);
    inventory_generation_.store(retained.inventory_generation(), std::memory_order_release);
}

ChunkCoordinateNode* ChunkCoordinateIndex::Find(const ChunkCoord& coord) {
    Check();
    std::lock_guard lock(mutex_);
    const auto at = coordinates_.find(coord);
    return at == coordinates_.end() ? nullptr : &at->second;
}

bool ChunkCoordinateIndex::IsPopulationCandidate(const ChunkCoord& coord) const {
    Check();
    std::lock_guard lock(mutex_);
    return populated_.contains(coord);
}

CoordinateReservation ChunkCoordinateIndex::Prepare(const ChunkCoord& coord, ChunkCoordinateNode** cache_slot) {
    Check();
    CoordinateReservation reservation;
    reservation.owner_ = this;
    reservation.coord_ = coord;
    reservation.cache_slot_ = cache_slot;
    if (*cache_slot == nullptr) {
        std::lock_guard lock(mutex_);
        ReserveRunSplits(1U);
        const auto [at, inserted] = coordinates_.try_emplace(coord);
        if (inserted) AttachRun(at);
        reservation.inserted_ = inserted;
        *cache_slot = &at->second;
    }
    reservation.node_ = *cache_slot;
    if (!reservation.node_->populated.load(std::memory_order_acquire)) {
        std::set<ChunkCoord> staging;
        staging.insert(coord);
        reservation.live_node_ = staging.extract(staging.begin());
    }
    return reservation;
}

void ChunkCoordinateIndex::ReleaseReservation(CoordinateReservation& reservation) noexcept {
    if (!reservation.inserted_) return;
    std::lock_guard lock(mutex_);
    if (reservation.node_->revision.load(std::memory_order_relaxed) != 0U) std::terminate();
    *reservation.cache_slot_ = nullptr;
    DetachRun(*reservation.node_);
    coordinates_.erase(reservation.coord_);
}

void ChunkCoordinateIndex::PublishReservation(CoordinateReservation& reservation, bool populated,
    std::uint64_t revision) noexcept {
    if (revision == 0U) std::terminate();
    const auto previous_revision = reservation.node_->revision.load(std::memory_order_relaxed);
    if (revision <= previous_revision) std::terminate();
    const bool previous = reservation.node_->populated.load(std::memory_order_relaxed);
    // The revision and its run go first: a snapshot page that finds a
    // coordinate no longer populated under the index lock also finds the
    // revision that emptied it (SnapshotPage). The store and the run load
    // pair with SplitRun's run store and revision load: either the split
    // reads this revision or this write raises the run the split moved the
    // coordinate to.
    reservation.node_->revision.store(revision, std::memory_order_seq_cst);
    RaiseRun(*reservation.node_->run.load(std::memory_order_seq_cst), revision);
    if (previous != populated) {
        std::lock_guard lock(mutex_);
        if (populated) {
            if (reservation.live_node_.empty() || !populated_.insert(std::move(reservation.live_node_)).inserted)
                std::terminate();
        } else populated_.erase(reservation.coord_);
        reservation.node_->populated.store(populated, std::memory_order_relaxed);
    }
    if (previous != populated) {
        if (auto* hook = publication_hook.load(std::memory_order_acquire)) hook->AfterPopulationChange(reservation.coord_);
    }
    if (previous_revision == 0U) any_written_.store(true, std::memory_order_release);
}

void ChunkCoordinateIndex::ReserveRunSplits(std::size_t splits) {
    if (spare_run_entries_.size() < splits) spare_run_entries_.reserve(splits);
    while (spare_run_entries_.size() < splits) {
        Runs staging;
        staging.emplace(ChunkCoord{}, nullptr);
        spare_run_entries_.push_back(staging.extract(staging.begin()));
    }
    while (free_runs_.size() < splits) {
        // Every run can return to the free list without allocating.
        free_runs_.reserve(run_storage_.size() + 1U);
        free_runs_.push_back(&run_storage_.emplace_back());
    }
}

void ChunkCoordinateIndex::AttachRun(Coordinates::iterator at) noexcept {
    const auto run = std::prev(runs_.upper_bound(at->first));
    at->second.run.store(run->second, std::memory_order_seq_cst);
    RaiseRun(*run->second, at->second.revision.load(std::memory_order_seq_cst));
    if (++run->second->nodes > kRunNodes) SplitRun(run);
}

void ChunkCoordinateIndex::DetachRun(ChunkCoordinateNode& node) noexcept {
    auto* run = node.run.load(std::memory_order_relaxed);
    if (--run->nodes != 0U || run == runs_.begin()->second) return;
    // The previous run's range absorbs this empty one.
    runs_.erase(run->first);
    free_runs_.push_back(run);
}

void ChunkCoordinateIndex::SplitRun(Runs::iterator run) noexcept {
    const auto next = std::next(run);
    auto at = coordinates_.lower_bound(run->first);
    std::advance(at, static_cast<std::ptrdiff_t>(run->second->nodes / 2U));
    auto* upper = free_runs_.back();
    free_runs_.pop_back();
    upper->maximum_revision.store(0U, std::memory_order_relaxed);
    upper->first = at->first;
    upper->nodes = 0U;
    std::uint64_t maximum = 0U;
    for (; at != coordinates_.end() && (next == runs_.end() || at->first < next->first); ++at) {
        at->second.run.store(upper, std::memory_order_seq_cst);
        maximum = std::max(maximum, at->second.revision.load(std::memory_order_seq_cst));
        ++upper->nodes;
    }
    RaiseRun(*upper, maximum);
    // The lower run keeps its maximum: an upper bound stays one.
    run->second->nodes -= upper->nodes;
    auto entry = std::move(spare_run_entries_.back());
    spare_run_entries_.pop_back();
    entry.key() = upper->first;
    entry.mapped() = upper;
    runs_.insert(std::move(entry));
}

template <class Visit>
bool ChunkCoordinateIndex::WalkChanged(bool has_from, const ChunkCoord& from, bool inclusive, std::uint64_t since,
    std::size_t budget, ChunkCoord& resume, Visit&& visit) const {
    const auto stop = [&](const ChunkCoord& coord) { resume = coord; return true; };
    auto run = has_from ? std::prev(runs_.upper_bound(from)) : runs_.begin();
    std::size_t examined = 0U;
    for (bool first = true; run != runs_.end(); ++run, first = false) {
        const auto next = std::next(run);
        if (!first && examined++ >= budget) return stop(run->first);
        if (run->second->maximum_revision.load(std::memory_order_acquire) <= since) continue;
        auto at = !first || !has_from ? coordinates_.lower_bound(run->first)
            : inclusive ? coordinates_.lower_bound(from) : coordinates_.upper_bound(from);
        for (; at != coordinates_.end() && (next == runs_.end() || at->first < next->first); ++at) {
            if (examined++ >= budget) return stop(at->first);
            if (at->second.revision.load(std::memory_order_acquire) <= since) continue;
            if (!visit(at->first)) return stop(at->first);
        }
    }
    return false;
}

CoordinatePage ChunkCoordinateIndex::Page(bool has_cursor, const ChunkCoord& cursor, std::size_t limit,
    std::optional<std::uint64_t> since) const {
    Check();
    if (limit == 0U || limit > kMaxChunkScanLimit + 1U) throw std::invalid_argument("invalid coordinate scan limit");
    if (since && *since < retained_deletion_floor())
        throw std::invalid_argument("tombstone_history_lost: requested revision is below retained deletion floor");
    CoordinatePage page;
    page.coords.reserve(limit);
    std::lock_guard lock(mutex_);
    if (since) {
        ChunkCoord resume{};
        (void)WalkChanged(has_cursor, cursor, false, *since, std::numeric_limits<std::size_t>::max(), resume,
            [&](const ChunkCoord& coord) {
                if (page.coords.size() == limit) { page.more = true; return false; }
                page.coords.push_back(coord);
                return true;
            });
    } else {
        auto at = has_cursor ? populated_.upper_bound(cursor) : populated_.begin();
        for (; at != populated_.end(); ++at) {
            if (page.coords.size() == limit) { page.more = true; break; }
            page.coords.push_back(*at);
        }
    }
    return page;
}

void SetCoordinatePublicationTestHook(CoordinatePublicationTestHook* hook) noexcept {
    publication_hook.store(hook, std::memory_order_release);
}

CoordinatePage ChunkCoordinateIndex::SnapshotPage(bool has_cursor, const ChunkCoord& cursor, std::size_t limit,
    std::uint64_t snapshot) const {
    Check();
    if (limit == 0U || limit > kMaxChunkScanLimit + 1U) throw std::invalid_argument("invalid coordinate scan limit");
    CoordinatePage page;
    page.coords.reserve(limit);
    // Populated at the snapshot: populated now, or changed after it. Once
    // true for a fixed snapshot this stays true (revisions only grow), so
    // the walk can release the lock between batches, letting writers publish,
    // and resume where it stopped. Within one batch both sets are read under
    // the lock that a write emptying a coordinate takes after it published
    // the revision and raised its run.
    constexpr std::size_t kBatchNodes = 256U;
    bool has_from = has_cursor;
    ChunkCoord from = cursor;
    bool inclusive = false;
    std::vector<ChunkCoord> changed;
    changed.reserve(limit + 1U);
    for (;;) {
        std::lock_guard lock(mutex_);
        changed.clear();
        const auto wanted = limit + 1U - page.coords.size();
        ChunkCoord stop{};
        const bool stopped = WalkChanged(has_from, from, inclusive, snapshot, kBatchNodes, stop,
            [&](const ChunkCoord& coord) {
                if (changed.size() == wanted) return false;
                changed.push_back(coord);
                return true;
            });
        // Every coordinate before `stop` is decided: merge the populated ones.
        auto live = !has_from ? populated_.begin()
            : inclusive ? populated_.lower_bound(from) : populated_.upper_bound(from);
        auto change = changed.begin();
        for (;;) {
            const bool has_live = live != populated_.end() && (!stopped || *live < stop);
            const bool has_change = change != changed.end();
            if (!has_live && !has_change) break;
            ChunkCoord next;
            if (has_live && (!has_change || !(*change < *live))) {
                next = *live;
                if (has_change && *change == *live) ++change;
                ++live;
            } else next = *change++;
            if (page.coords.size() == limit) { page.more = true; return page; }
            page.coords.push_back(next);
        }
        if (!stopped) return page;
        has_from = true;
        from = stop;
        inclusive = true;
    }
}

std::optional<ChunkIndexRecord> ChunkCoordinateIndex::EmptyBoundary(const ChunkCoord& coord) const {
    Check();
    std::lock_guard lock(mutex_);
    const auto at = coordinates_.find(coord);
    return at == coordinates_.end() ? std::nullopt : at->second.retained_empty;
}

CoordinatePage ChunkCoordinateIndex::CurrentPage(bool has_cursor, const ChunkCoord& cursor, std::size_t limit) const {
    Check();
    if (limit == 0U || limit > kMaxChunkScanLimit) throw std::invalid_argument("invalid current coordinate page limit");
    CoordinatePage page;
    page.coords.reserve(limit);
    std::lock_guard lock(mutex_);
    auto at = has_cursor ? coordinates_.upper_bound(cursor) : coordinates_.begin();
    for (; at != coordinates_.end(); ++at) {
        if (at->second.revision.load(std::memory_order_acquire) == 0U) continue;
        if (page.coords.size() == limit) { page.more = true; break; }
        page.coords.push_back(at->first);
    }
    return page;
}

void ChunkCoordinateIndex::PublishTombstone(const ChunkIndexRecord& record) {
    Check();
    if (read_only_ || record.kind != ChunkIndexKind::kDeleted)
        throw std::invalid_argument("only a writer may retain an empty chunk boundary");
    if (record.schema_version > geometry_.layout().schema().version)
        throw std::invalid_argument("collection boundary schema is newer than the table");
    const auto large = geometry_.ChunkToLarge(record.coord);
    const auto bytes = SerializeChunkIndexRecord(record, geometry_, large);
    std::lock_guard publication(publication_mutex_);
    ChunkCoordinateNode* node;
    {
        std::lock_guard lock(mutex_);
        const auto at = coordinates_.find(record.coord);
        if (at == coordinates_.end() || at->second.revision.load(std::memory_order_acquire) != record.revision ||
            at->second.populated.load(std::memory_order_relaxed))
            throw std::logic_error("collection boundary is not the committed empty coordinate");
        node = &at->second;
        if (node->retained_empty) {
            const auto& old = *node->retained_empty;
            if (old.revision > record.revision || (old.revision == record.revision &&
                old.commit_time_ms != record.commit_time_ms))
                throw std::runtime_error("collection boundary conflicts with its retained identity");
            if (old.kind == ChunkIndexKind::kDeleted && old.revision == record.revision) return;
        }
    }
    auto next = shards_;
    auto [at, inserted] = next.try_emplace(large);
    auto& shard = at->second;
    const auto header = SerializeChunkIndexHeader({table_id_, geometry_.dims(), large});
    const auto log_path = LogPath(large);
    if (inserted) {
        if (std::filesystem::exists(log_path))
            throw std::runtime_error("unlisted coordinate log requires recovery before collection");
        shard.inventory = {.coord = large, .committed_bytes = header.size()};
    } else if (std::filesystem::file_size(log_path) != shard.inventory.committed_bytes) {
        throw std::runtime_error("coordinate log has an unpublished append requiring recovery");
    }
    if (shard.inventory.committed_bytes > std::numeric_limits<std::uint64_t>::max() - bytes.size())
        throw std::overflow_error("coordinate log byte count overflows");
    shard.inventory.committed_bytes += bytes.size();
    shard.observed_bytes = shard.inventory.committed_bytes;
    auto decision = inventory_;
    decision.count = next.size();
    if (decision.generation == std::numeric_limits<std::uint64_t>::max())
        throw std::overflow_error("coordinate inventory generation cannot advance");
    ++decision.generation;
    std::vector<IndexInventoryEntry> entries;
    entries.reserve(next.size());
    for (const auto& [coord, item] : next) { (void)coord; entries.push_back(item.inventory); }
    try {
        EnsureDirectoryPathExists(log_path.parent_path(), true);
        if (inserted) {
            AtomicWriteStream(log_path, [&](const AtomicFileSink& sink) { sink(header); sink(bytes); },
                true, true, nullptr, nullptr, false);
        } else {
            WalAppendStream output(log_path, std::ios::binary | std::ios::app);
            if (!output.is_open()) throw BuildWalOpenError(log_path, output.error());
            output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size())).flush();
            if (!output.good()) throw std::runtime_error("failed to append coordinate boundary " + log_path.string());
            output.close();
            if (!output.good()) throw std::runtime_error("failed to close coordinate boundary " + log_path.string());
            SyncFilePath(log_path);
        }
        Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_AFTER_LOG_SYNC_ONCE");
        PublishInventory(decision, entries);
        Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_AFTER_INVENTORY_PUBLISH_ONCE");
        shards_.swap(next);
        inventory_ = decision;
        {
            std::lock_guard lock(mutex_);
            node->retained_empty = record;
        }
    } catch (const std::exception&) { failed_.store(true, std::memory_order_release); throw; }
}

void ChunkCoordinateIndex::Compact(std::uint64_t before, std::uint64_t completed_frontier) {
    Check();
    if (read_only_) throw std::invalid_argument("tombstone compaction requires a writer");
    const auto bounds = ComputeTombstoneCompactionBounds(before, completed_frontier, retained_deletion_floor());
    if (!bounds || bounds->through == 0U) return;
    std::lock_guard publication(publication_mutex_);
    auto pending = inventory_;
    if (pending.generation > std::numeric_limits<std::uint64_t>::max() - 2U)
        throw std::overflow_error("coordinate inventory generation cannot complete compaction");
    ++pending.generation;
    pending.compaction_pending = true;
    pending.compaction_through = bounds->through;
    pending.retained_deletion_floor = bounds->retained_deletion_floor;
    auto replacements = shards_;
    try {
        for (auto& [coord, shard] : replacements) {
            const auto path = LogPath(coord);
            SharedFileInput input(path);
            if (!input) throw std::runtime_error("missing coordinate log before compaction " + path.string());
            const auto size = StreamBytes(input, path);
            if (size != shard.inventory.committed_bytes)
                throw std::runtime_error("unpublished coordinate suffix prevents compaction");
            const auto checked = ReadChunkIndexLog(input, size, geometry_, table_id_, coord, shard.inventory.committed_bytes);
            const auto header = SerializeChunkIndexHeader({table_id_, geometry_.dims(), coord});
            AtomicWriteStream(StagePath(coord), [&](const AtomicFileSink& sink) {
                sink(header);
                const auto emit = [&](const ChunkIndexRecord& record) {
                    const auto bytes = SerializeChunkIndexRecord(record, geometry_, coord); sink(bytes);
                };
                const auto emit_empty = [&](const ChunkIndexRecord& boundary) {
                    if (TombstoneVersionDiscarded(boundary.revision, bounds->through)) return;
                    if (boundary.kind == ChunkIndexKind::kForget) {
                        auto deleted = boundary; deleted.kind = ChunkIndexKind::kDeleted; emit(deleted);
                    }
                    emit(boundary);
                };
                for (const auto& [chunk, record] : checked.latest) {
                    if (record.kind != ChunkIndexKind::kLive) emit_empty(record);
                    else {
                        if (const auto old = checked.empty_boundaries.find(chunk); old != checked.empty_boundaries.end())
                            emit_empty(old->second);
                        emit(record);
                    }
                }
            }, true, false, nullptr, nullptr, false);
            Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_COMPACTION_AFTER_STAGE_RENAME_ONCE");
            SyncDirectoryPath(StagePath(coord).parent_path());
            const auto checksum = ChecksumFile(StagePath(coord));
            shard.inventory.replacement_bytes = checksum.bytes;
            shard.inventory.replacement_sha256 = checksum.digest;
        }
        Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_COMPACTION_AFTER_STAGE_DIRECTORY_SYNC_ONCE");
        std::vector<IndexInventoryEntry> entries;
        entries.reserve(replacements.size());
        for (const auto& [coord, shard] : replacements) { (void)coord; entries.push_back(shard.inventory); }
        PublishInventory(pending, entries);
        inventory_ = pending;
        deletion_floor_.store(pending.retained_deletion_floor, std::memory_order_release);
        Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_COMPACTION_AFTER_DECISION_ONCE");
        for (auto& [coord, shard] : replacements) {
            if (const auto error = ReplacePathAtomically(StagePath(coord), LogPath(coord)); error)
                throw std::system_error(error, "failed to replace compacted coordinate log");
            Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_COMPACTION_AFTER_REPLACE_ONCE");
            SyncDirectoryPath(LogPath(coord).parent_path());
            shard.inventory.committed_bytes = shard.inventory.replacement_bytes;
            shard.inventory.replacement_bytes = 0U;
            shard.inventory.replacement_sha256 = {};
        }
        Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_COMPACTION_AFTER_DIRECTORY_SYNC_ONCE");
        entries.clear();
        for (const auto& [coord, shard] : replacements) { (void)coord; entries.push_back(shard.inventory); }
        auto finished = pending;
        finished.compaction_pending = false;
        finished.compaction_through = 0U;
        ++finished.generation;
        PublishInventory(finished, entries);
        inventory_ = finished;
        shards_.swap(replacements);
        Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_COMPACTION_AFTER_FINAL_INVENTORY_ONCE");
        // Exclusive table admission must reopen the store before serving again;
        // cached node pointers remain valid throughout destruction of this owner.
        failed_.store(true, std::memory_order_release);
    } catch (const std::exception&) { failed_.store(true, std::memory_order_release); throw; }
}

std::uint64_t ChunkCoordinateIndex::populated_count() const {
    Check();
    std::lock_guard lock(mutex_);
    return populated_.size();
}

std::uint64_t ChunkCoordinateIndex::tombstone_count() const {
    Check();
    std::lock_guard lock(mutex_);
    std::uint64_t count = 0U;
    for (const auto& [coord, node] : coordinates_) {
        (void)coord;
        if (node.retained_empty && node.retained_empty->kind == ChunkIndexKind::kDeleted &&
            !node.populated.load(std::memory_order_acquire) &&
            node.revision.load(std::memory_order_acquire) == node.retained_empty->revision) ++count;
    }
    return count;
}

}  // namespace chunkdb
