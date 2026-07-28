// Regression tests for the security fixes in the 2026-07-28 audit remediation.
//
// These exist because every one of the fixes below was silently reverted once,
// when a refactor moved the affected function to a new translation unit from a
// base that predated the fix. The full suite stayed green throughout: none of
// these behaviors had a test. Each case here pins one fix so that a future move
// of the same code fails loudly instead of quietly restoring the defect.

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"

#include "chunk_store_internal.hpp"

namespace {

chunkdb::Geometry TestGeometry() {
    return chunkdb::Geometry(chunkdb::GeometryConfig{
        .large_chunk_width_chunks = 2,
        .large_chunk_height_chunks = 2,
        .chunk_width_blocks = 4,
        .chunk_height_blocks = 4,
        .block_bits = 4,
    });
}

// WriteRegionSlotState used to bounds-check slot_index only via the trailing
// SetRegionSlotPresent call, which runs *after* the payload copy and the
// slot_crc store. An out-of-range index therefore wrote to the heap before it
// was ever rejected. The check must reject the index before any write happens.
//
// Observing that ordering needs care: "did it throw?" cannot distinguish the
// two versions, because the late SetRegionSlotPresent check throws the very
// same message. And with a normally-sized image the premature write lands
// past the end of the buffers, so it is invisible to the object itself (only
// a sanitizer would see it).
//
// So the image here is built deliberately inconsistent: slot_count is halved
// while the three buffers stay sized for the full span. An index in
// [slot_count, full_slots) is then out of range by contract, yet its write
// target is still inside allocated memory. Without the fix the payload and
// CRC land in the buffer and are observable; with the fix nothing is touched.
// Neither path is undefined behavior, which is what makes this a usable
// regression test rather than a sanitizer-only one.
void TestWriteRegionSlotStateRejectsOutOfRangeSlot() {
    const auto geometry = TestGeometry();
    constexpr std::size_t kSpanChunks = 4;
    const chunkdb::ChunkCoord coord{.x = 0, .y = 0};
    const auto addr = chunkdb::ComputeRegionChunkAddress(coord, kSpanChunks);

    auto image = chunkdb::BuildEmptyRegionFileImage(geometry, addr, kSpanChunks);
    const std::uint32_t full_slots = image.slot_count;
    assert(full_slots == kSpanChunks * kSpanChunks);
    assert(full_slots >= 2U);

    const std::vector<std::uint8_t> payload(image.payload_bytes, 0xAB);

    // Keep the buffers at full size, but declare only half the slots valid.
    image.slot_count = full_slots / 2U;
    const std::uint32_t out_of_range = image.slot_count;

    const auto payloads_before = image.slot_payloads;
    const auto crc_before = image.slot_crc;
    const auto presence_before = image.present_bitmap;

    bool threw = false;
    try {
        chunkdb::WriteRegionSlotState(&image, out_of_range, payload);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw && "out-of-range slot index must be rejected");
    assert(image.slot_payloads == payloads_before && "payload must not be written before the check");
    assert(image.slot_crc == crc_before && "slot CRC must not be written before the check");
    assert(image.present_bitmap == presence_before && "presence bitmap must be untouched");

    // Restore consistency and confirm the guard is not over-broad: a valid
    // index must still round-trip.
    image.slot_count = full_slots;
    const std::uint32_t valid = full_slots - 1U;
    chunkdb::WriteRegionSlotState(&image, valid, payload);
    assert(chunkdb::RegionSlotPresent(image, valid));
    assert(chunkdb::ExtractRegionSlotState(image, valid) == payload);
}

// WalPathForConditionalIntent reconstructs a WAL path from a filename read off
// disk by splitting on "__". Without component validation a planted name such
// as "..__..__etc__passwd.rollback" resolves outside the data directory, and
// the result feeds std::filesystem::resize_file() and remove() during startup
// recovery. Legitimate names are always coordinate-derived and can never
// contain an empty, "." or ".." component.
void TestWalPathForConditionalIntentRejectsTraversal() {
    const std::filesystem::path data_dir = "/tmp/chunkdb-intent-grammar";

    // A legitimate WAL path must round-trip unchanged.
    const std::filesystem::path wal_path = data_dir / "L_0_0" / "C_1_2.wal";
    const auto intent_path = chunkdb::ConditionalIntentPathForWal(data_dir, wal_path);
    assert(chunkdb::WalPathForConditionalIntent(data_dir, intent_path) == wal_path);

    // Negative coordinates round-trip too: std::to_string emits '-', never '_'.
    const std::filesystem::path negative_wal = data_dir / "L_-1_-3" / "C_-5_-8.wal";
    const auto negative_intent = chunkdb::ConditionalIntentPathForWal(data_dir, negative_wal);
    assert(chunkdb::WalPathForConditionalIntent(data_dir, negative_intent) == negative_wal);

    const auto intent_dir = chunkdb::ConditionalIntentDirectory(data_dir);
    const auto rejects = [&](const std::string& file_name) {
        bool threw = false;
        try {
            (void)chunkdb::WalPathForConditionalIntent(data_dir, intent_dir / file_name);
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        assert(threw && "malformed intent file name must be rejected");
    };

    rejects("..__..__etc__passwd.rollback");   // classic traversal
    rejects("..__C_0_0.wal.rollback");         // single parent escape
    rejects("__L_0_0__C_0_0.wal.rollback");    // leading empty component
    rejects("L_0_0____C_0_0.wal.rollback");    // empty component in the middle
    rejects("L_0_0__..__C_0_0.wal.rollback");  // parent escape in the middle
    rejects("L_0_0__.__C_0_0.wal.rollback");   // current-dir component
    rejects("L_0_0__C_0_0.wal__.rollback");    // trailing empty component

    // A name that does not carry the suffix at all is rejected as before.
    bool threw = false;
    try {
        (void)chunkdb::WalPathForConditionalIntent(data_dir, intent_dir / "L_0_0__C_0_0.wal");
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw && "a non-intent file name must be rejected");
}

// The server must fail closed rather than listen with authentication silently
// disabled. This is the guardrail that the container image's baked-in
// CHUNKDB_TOKEN=dev-token used to bypass: with a default token present the
// process always started, and the credential was publicly known.
void TestEngineRefusesAuthEnabledWithEmptyToken() {
    bool threw = false;
    try {
        const chunkdb::EngineConfig config{
            .auth_token = "",
            .require_auth = true,
        };
        chunkdb::CommandEngine engine(config, nullptr);
        (void)engine;
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw && "auth enabled with an empty token must be rejected at construction");
}

}  // namespace

int main() {
    TestWriteRegionSlotStateRejectsOutOfRangeSlot();
    TestWalPathForConditionalIntentRejectsTraversal();
    TestEngineRefusesAuthEnabledWithEmptyToken();
    return 0;
}
