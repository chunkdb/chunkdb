// Generated positive seeds must decode, or the fuzz targets start from inputs
// rejected before the decoders they are meant to reach.
#include "fuzz_common.hpp"
#include "chunk_form.hpp"

#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>

int main(int argc, char** argv) {
    if (argc != 2) { std::cerr << "usage: chunkdb_fuzz_seed_check CORPUS_DIRECTORY\n"; return 2; }
    int failures = 0;
    for (unsigned model = 0; model < chunkdb::fuzz::kModelCount; ++model) {
        const auto path = std::filesystem::path(argv[1]) / "chunk_form" / ("model-" + std::to_string(model));
        std::ifstream in(path, std::ios::binary);
        const std::string bytes{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
        try {
            if (bytes.empty()) throw std::runtime_error("missing seed");
            const auto& geometry = chunkdb::fuzz::Model(static_cast<std::uint8_t>(bytes[0]));
            const auto form = std::string_view(bytes).substr(1);
            if (chunkdb::ChunkFormSchemaVersion(form) != geometry.layout().schema().version)
                throw std::runtime_error("seed schema version differs from its model");
            const auto state = chunkdb::DecodeChunkForm(geometry, form);
            geometry.layout().RequireValidVars(state.vars, state.presence_bitmap);
            if (geometry.layout().has_var_columns() && state.vars.empty())
                throw std::runtime_error("seed has no text or bytes values");
        } catch (const std::exception& error) {
            std::cerr << path.string() << ": " << error.what() << "\n";
            ++failures;
        }
    }
    return failures == 0 ? 0 : 1;
}
