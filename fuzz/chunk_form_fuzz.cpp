#include "fuzz_common.hpp"
#include "chunk_form.hpp"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size == 0 || size > chunkdb::fuzz::kMaxInput) return 0;
    chunkdb::fuzz::Rejectable([&] {
        const auto& geometry = chunkdb::fuzz::Model(data[0]);
        const auto form = chunkdb::fuzz::Text(data + 1, size - 1);
        const auto version = chunkdb::ChunkFormSchemaVersion(form);
        (void)geometry.LayoutAt(version); // Statement chooses the wire schema.
        auto state = chunkdb::DecodeChunkForm(geometry, form);
        geometry.layout().RequireValidVars(state.vars, state.presence_bitmap);
    });
    return 0;
}
