#include <cassert>
#include <fstream>
#include <functional>

#include "archive_base_format.hpp"
#include "checkpoint.hpp"
#include "feed_archive.hpp"
#include "feed_slots.hpp"
#include "feed_test_utils.hpp"
#include "feature_flags.hpp"
#include "chunkdb/chunk_columns.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;

CatalogConfig Config(const std::filesystem::path& path) {
    auto config = feed_test::Config(path);
    config.slot_sync_interval = std::chrono::hours(1);
    return config;
}

void Save(const std::filesystem::path& path, const Bytes& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close();
    assert(output);
}

void Refuse(const std::function<void()>& action, std::string_view reason) {
    bool refused = false;
    try { action(); }
    catch (const std::exception& error) { refused = std::string_view(error.what()).find(reason) != std::string_view::npos; }
    assert(refused);
}

void Drain(Table& table, FeedPosition start) {
    auto reader = table.ReadFeedArchive(start);
    while (reader.Next()) {}
}

void RetainedRequiredBaseAndLivePrefix() {
    test::ScopedTempDir directory("chunkdb-archive-required-base");
    TableCatalog catalog(Config(directory.path()));
    auto definition = TableDefinition{.geometry = txn_test::Config({}).geometry,
        .options = catalog.default_options(), .schema = SingleBitsColumnSchema(32U)};
    definition.chunk_schema = TableSchema{.next_column_id = 2U, .columns = {
        {.id = 1U, .name = "owner", .type = {ColumnKind::kUnsigned, 8U}, .required = true, .default_value = {}}}};
    const auto table = catalog.Create("default", [&] { return definition; });
    const auto root = directory.path() / "tables/default";
    std::uint64_t base_revision = 0U, base_time = 0U, first = 0U;
    {
        auto lease = table->Acquire();
        auto& store = lease->store();
        auto state = store.ReadChunkState(0, 0);
        state.chunk_columns = std::make_shared<const ChunkColumnState>(AssignChunkColumns(
            store.geometry().chunk_schema(), nullptr, {{"owner", std::uint64_t{0U}}}, true));
        const auto result = store.WriteChunkState({0, 0}, std::move(state), {});
        assert(result.ok && result.version != 0U);
        base_revision = result.version;
        base_time = store.ReadChunkState(0, 0).commit_time_ms;
        store.CheckpointForTests(0, 0);
        assert(!std::filesystem::exists(ChunkDataPath(root, store.geometry(), {0, 0})));
        assert(!std::filesystem::exists(ChunkWalPath(root, store.geometry(), {0, 0})));
    }
    const auto slot = table->CreateFeedSlot("consumer");
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, feed_test::Bits(5U));
        first = lease->store().GetChunkVersion(0, 0);
        assert(first > base_revision);
    }
    FeedSlotTestAccess::Sync(*table);
    {
        auto reader = FeedSlotTestAccess::CompletedPrefix(*table, "consumer", slot.position);
        const auto entry = reader.Next();
        assert(entry && entry->position.revision == first && entry->blocks.size() == 1U);
        assert(!reader.Next());
    }
    Drain(*table, slot.position);
    const auto descriptor_path = root / kFeedArchiveDirName / ArchiveBaseFileName({0, 0}, 2U, first);
    {
        auto lease = table->Acquire();
        lease->store().CheckpointForTests(0, 0);
        const auto descriptor = ReadFeedArchiveBaseDescriptor(descriptor_path, lease->store().geometry(),
            table->Info().store_id, {0, 0}, first);
        assert(descriptor && descriptor->kind == ArchiveBaseKind::kRetainedEmpty);
        assert(descriptor->base_revision == base_revision && descriptor->base_time_ms == base_time);
        assert(descriptor->image_length == 0U && descriptor->base_schema == 1U);
    }
    Drain(*table, slot.position);
}

void ImageBindingAndMissingDescriptor() {
    test::ScopedTempDir directory("chunkdb-archive-image-binding");
    TableCatalog catalog(Config(directory.path()));
    const auto table = feed_test::CreateDefault(catalog);
    const auto slot = table->CreateFeedSlot("consumer");
    const auto root = directory.path() / "tables/default";
    std::uint64_t first = 0U;
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, feed_test::Bits(1U));
        lease->store().CheckpointForTests(0, 0);
        lease->store().SetBlockBits(0, 0, feed_test::Bits(2U));
        first = lease->store().GetChunkVersion(0, 0);
        lease->store().CheckpointForTests(0, 0);
    }
    FeedSlotTestAccess::Sync(*table);
    const auto descriptor_path = root / kFeedArchiveDirName / ArchiveBaseFileName({0, 0}, 2U, first);
    const auto pinned = root / kFeedArchiveDirName / (ChunkFileStem({0, 0}, 2U) + "." + std::to_string(first) + ".chk");
    const auto descriptor_bytes = LoadFile(descriptor_path);
    const auto original = LoadFile(pinned);
    std::filesystem::remove(descriptor_path);
    Refuse([&] { Drain(*table, slot.position); }, "descriptor is missing");
    Save(descriptor_path, descriptor_bytes);
    {
        auto lease = table->Acquire();
        const auto& geometry = lease->store().geometry();
        auto image = ParseChunkImage(original, geometry, {0, 0}, table->Info().store_id, lease->store().features());
        image.payload[0] ^= 1U;
        Save(pinned, SerializeChunkImage(geometry, {0, 0}, image.payload, image.presence_bitmap,
            CheckpointCompression::kNone, image.revision, image.commit_time_ms, table->Info().store_id,
            &image.vars, image.chunk_columns.get()));
    }
    Refuse([&] { Drain(*table, slot.position); }, "digest mismatch");
    Save(pinned, original);
    auto changed_header = original;
    const auto flags = ReadLe32(changed_header, 16U) ^ kFeatureFeedSlots;
    for (unsigned i = 0U; i < 4U; ++i) changed_header[16U + i] = static_cast<std::uint8_t>(flags >> (8U * i));
    const auto crc_at = ChunkImageHeaderBytes(2U, ReadLe16(changed_header, 10U)) - 4U;
    const auto header_crc = Crc32(changed_header.data(), crc_at);
    for (unsigned i = 0U; i < 4U; ++i) changed_header[crc_at + i] = static_cast<std::uint8_t>(header_crc >> (8U * i));
    assert(Crc32(changed_header) == Crc32(original));
    Save(pinned, changed_header);
    Refuse([&] { Drain(*table, slot.position); }, "mismatch");
    Save(pinned, original);
    auto corrupt = descriptor_bytes;
    corrupt.back() ^= 1U;
    Save(descriptor_path, corrupt);
    Refuse([&] { Drain(*table, slot.position); }, "checksum mismatch");
    Save(descriptor_path, descriptor_bytes);
    Drain(*table, slot.position);
}

void RetryKeepsOriginalDescriptor() {
    test::ScopedTempDir directory("chunkdb-archive-base-retry");
    TableCatalog catalog(Config(directory.path()));
    const auto table = feed_test::CreateDefault(catalog);
    const auto slot = table->CreateFeedSlot("consumer");
    const auto root = directory.path() / "tables/default";
    auto lease = table->Acquire();
    auto& store = lease->store();
    store.SetBlockBits(0, 0, feed_test::Bits(1U));
    const auto first = store.GetChunkVersion(0, 0);
    {
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_CHECKPOINT_AFTER_IMAGE_REPLACE_ONCE", "1");
        Refuse([&] { store.CheckpointForTests(0, 0); }, "after image replacement");
    }
    const auto descriptor_path = root / kFeedArchiveDirName / ArchiveBaseFileName({0, 0}, 2U, first);
    const auto before = LoadFile(descriptor_path);
    store.CheckpointForTests(0, 0);
    assert(LoadFile(descriptor_path) == before);
    const auto descriptor = ReadFeedArchiveBaseDescriptor(descriptor_path, store.geometry(), table->Info().store_id, {0, 0}, first);
    assert(descriptor && descriptor->kind == ArchiveBaseKind::kNeverWritten && descriptor->base_revision == 0U);
    lease.reset();
    FeedSlotTestAccess::Sync(*table);
    Drain(*table, slot.position);
}

void RetryRepaysDescriptorSyncBeforeReplacingImage() {
    test::ScopedTempDir directory("chunkdb-archive-base-sync-retry");
    TableCatalog catalog(Config(directory.path()));
    const auto table = feed_test::CreateDefault(catalog);
    const auto root = directory.path() / "tables/default";
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, feed_test::Bits(1U));
        lease->store().CheckpointForTests(0, 0);
    }
    (void)table->CreateFeedSlot("consumer");
    auto lease = table->Acquire();
    auto& store = lease->store();
    const auto image = ChunkDataPath(root, store.geometry(), {0, 0});
    const auto original = LoadFile(image);
    store.SetBlockBits(0, 0, feed_test::Bits(2U));
    const auto first = store.GetChunkVersion(0, 0);
    const auto descriptor = root / kFeedArchiveDirName / ArchiveBaseFileName({0, 0}, 2U, first);
    {
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_FEED_BASE_AFTER_RENAME_ONCE", "1");
        Refuse([&] { store.CheckpointForTests(0, 0); }, "after replace before directory sync");
    }
    assert(std::filesystem::exists(descriptor) && LoadFile(image) == original);
    const auto pinned = root / kFeedArchiveDirName / (ChunkFileStem({0, 0}, 2U) + "." + std::to_string(first) + ".chk");
    const auto live_bases = std::filesystem::file_size(descriptor) + std::filesystem::file_size(pinned);
    assert(table->ListFeedSlots().front().retained_bytes == live_bases);
    const auto frozen = LoadFile(descriptor);
    bool observed = false;
    struct ClearObserver { ~ClearObserver() { SetDirectorySyncObserverForTests({}); } } clear;
    SetDirectorySyncObserverForTests([&](const auto& path) {
        if (!std::filesystem::equivalent(path, root / kFeedArchiveDirName)) return;
        observed = true;
        assert(LoadFile(image) == original);
        throw std::runtime_error("injected archive descriptor retry sync failure");
    });
    Refuse([&] { store.CheckpointForTests(0, 0); }, "archive descriptor retry sync failure");
    SetDirectorySyncObserverForTests({});
    assert(observed && LoadFile(image) == original && LoadFile(descriptor) == frozen);
    store.CheckpointForTests(0, 0);
    assert(LoadFile(descriptor) == frozen && LoadFile(image) != original);
}

void DescriptorBytesParticipateInRetentionLimit() {
    test::ScopedTempDir directory("chunkdb-archive-descriptor-retention");
    TableCatalog catalog(Config(directory.path()));
    const auto table = feed_test::CreateDefault(catalog);
    (void)table->CreateFeedSlot("consumer");
    {
        auto lease = table->Acquire();
        lease->store().SetBlockBits(0, 0, feed_test::Bits(1U));
        lease->store().CheckpointForTests(0, 0);
    }
    const auto archive = directory.path() / "tables/default" / kFeedArchiveDirName;
    std::uint64_t wal_bytes = 0U, descriptor_bytes = 0U;
    for (const auto& entry : std::filesystem::directory_iterator(archive)) {
        if (entry.path().extension() == ".wal") wal_bytes += entry.file_size();
        else if (entry.path().extension() == ".base") descriptor_bytes += entry.file_size();
        else assert(false && "first archive has no image base");
    }
    assert(wal_bytes != 0U && descriptor_bytes == 116U);
    assert(table->ListFeedSlots().front().retained_bytes == wal_bytes + descriptor_bytes);
    TableOptionsUpdate limit;
    limit.slot_max_bytes = static_cast<std::size_t>(wal_bytes);
    catalog.SetOptions("default", limit);
    FeedSlotTestAccess::Retain(*catalog.Find("default"));
    const auto lost = catalog.Find("default")->ListFeedSlots(true);
    assert(lost.size() == 1U && lost.front().lost);
    assert(std::filesystem::is_empty(archive));
}
}  // namespace

int main() {
    RetainedRequiredBaseAndLivePrefix();
    ImageBindingAndMissingDescriptor();
    RetryKeepsOriginalDescriptor();
    RetryRepaysDescriptorSyncBeforeReplacingImage();
    DescriptorBytesParticipateInRetentionLimit();
}
