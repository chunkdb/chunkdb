#include "commit_owner_bootstrap.hpp"

#include <algorithm>
#include <array>
#include <stdexcept>

#include "chunkdb/crc32.hpp"

namespace chunkdb {
namespace {
[[noreturn]] void Bad(const char* reason) {
    throw std::invalid_argument(std::string("invalid owner bootstrap decision: ") + reason);
}
std::uint64_t Get(std::span<const std::uint8_t> bytes, std::size_t at, unsigned count) {
    std::uint64_t value = 0U;
    for (unsigned i = 0U; i < count; ++i) value |= std::uint64_t(bytes[at + i]) << (8U * i);
    return value;
}
void Put(std::span<std::uint8_t> bytes, std::size_t at, std::uint64_t value, unsigned count) {
    for (unsigned i = 0U; i < count; ++i) bytes[at + i] = static_cast<std::uint8_t>(value >> (8U * i));
}
void Target(const CommitOwnerBootstrapDecision& decision) {
    if (decision.kind == CommitOwnerBootstrapKind::kDirectory) {
        const auto manifest = ParseDataDirManifest(decision.manifest);
        if (!manifest.commit_owner || manifest.commit_owner->role != CommitOwnerRole::kCatalog ||
            manifest.commit_owner->directory_id != manifest.data_dir_id) Bad("directory manifest scope");
    } else if (decision.kind == CommitOwnerBootstrapKind::kStandalone) {
        const auto manifest = ParseStoreManifest(decision.manifest);
        if (!manifest.commit_owner || manifest.commit_owner->role != CommitOwnerRole::kStandalone ||
            manifest.commit_owner->directory_id != manifest.store_id) Bad("standalone manifest scope");
    } else Bad("decision kind");
}
}  // namespace

CommitOwnerBootstrapHeader InspectCommitOwnerBootstrapHeader(std::span<const std::uint8_t> header) {
    constexpr std::array<std::uint8_t, 4U> magic{'C', 'K', 'G', 'B'};
    if (header.size() != kCommitOwnerBootstrapHeaderBytes || !std::equal(magic.begin(), magic.end(), header.begin()) ||
        Get(header, 4U, 2U) != 1U || header[7U] != 0U || Get(header, 56U, 4U) != 0U) Bad("header shape");
    if (Get(header, 60U, 4U) != Crc32(header.data(), 60U)) Bad("header checksum");
    CommitOwnerBootstrapHeader result;
    result.kind = static_cast<CommitOwnerBootstrapKind>(header[6U]);
    result.bytes = Get(header, 8U, 8U); result.manifest_bytes = Get(header, 16U, 8U);
    std::copy_n(header.begin() + 24, result.manifest_sha256.size(), result.manifest_sha256.begin());
    std::size_t minimum = 0U, maximum = 0U;
    if (result.kind == CommitOwnerBootstrapKind::kDirectory) { minimum = kDataDirManifestMinSize; maximum = kDataDirManifestMaxSize; }
    else if (result.kind == CommitOwnerBootstrapKind::kStandalone) { minimum = kStoreManifestMinSize; maximum = kStoreManifestMaxSize; }
    else Bad("decision kind");
    if (result.manifest_bytes < minimum || result.manifest_bytes > maximum || result.bytes > kMaxCommitOwnerBootstrapBytes ||
        result.bytes != result.manifest_bytes + 68U ||
        std::all_of(result.manifest_sha256.begin(), result.manifest_sha256.end(), [](auto byte) { return byte == 0U; }))
        Bad("bounded header extent or digest");
    return result;
}

std::vector<std::uint8_t> SerializeCommitOwnerBootstrapDecision(const CommitOwnerBootstrapDecision& decision) {
    Target(decision);
    if (decision.manifest.size() > kStoreManifestMaxSize) Bad("manifest byte ceiling");
    std::vector<std::uint8_t> bytes(decision.manifest.size() + 68U, 0U);
    bytes[0U] = 'C'; bytes[1U] = 'K'; bytes[2U] = 'G'; bytes[3U] = 'B'; bytes[4U] = 1U;
    bytes[6U] = static_cast<std::uint8_t>(decision.kind);
    Put(bytes, 8U, bytes.size(), 8U); Put(bytes, 16U, decision.manifest.size(), 8U);
    const auto digest = crypto::Sha256Of(decision.manifest);
    std::copy(digest.begin(), digest.end(), bytes.begin() + 24);
    Put(bytes, 60U, Crc32(bytes.data(), 60U), 4U);
    std::copy(decision.manifest.begin(), decision.manifest.end(), bytes.begin() + 64);
    Put(bytes, bytes.size() - 4U, Crc32(bytes.data(), bytes.size() - 4U), 4U);
    (void)InspectCommitOwnerBootstrapHeader(std::span(bytes).first(kCommitOwnerBootstrapHeaderBytes));
    return bytes;
}

CommitOwnerBootstrapDecision ParseCommitOwnerBootstrapDecision(std::span<const std::uint8_t> bytes) {
    if (bytes.size() < 68U || bytes.size() > kMaxCommitOwnerBootstrapBytes) Bad("complete extent");
    const auto header = InspectCommitOwnerBootstrapHeader(bytes.first(kCommitOwnerBootstrapHeaderBytes));
    if (header.bytes != bytes.size() || Get(bytes, bytes.size() - 4U, 4U) != Crc32(bytes.data(), bytes.size() - 4U))
        Bad("complete checksum or length");
    const auto manifest = bytes.subspan(kCommitOwnerBootstrapHeaderBytes, static_cast<std::size_t>(header.manifest_bytes));
    if (crypto::Sha256Of(manifest) != header.manifest_sha256) Bad("target raw digest");
    CommitOwnerBootstrapDecision decision{header.kind, {manifest.begin(), manifest.end()}};
    Target(decision);
    return decision;
}
}  // namespace chunkdb
