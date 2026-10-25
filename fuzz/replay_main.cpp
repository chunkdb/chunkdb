// Runs the fuzz entry point over files and directories without libFuzzer, so
// seed and regression inputs are checked by a normal build on every platform.
#include <cstddef>
#include <cstdlib>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size);

namespace {
std::size_t RunFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        std::cerr << "cannot read " << path << "\n";
        std::exit(2);
    }
    const std::vector<char> bytes{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    std::cerr << "replay " << path.string() << "\n";
    (void)LLVMFuzzerTestOneInput(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size());
    return 1;
}
}  // namespace

int main(int argc, char** argv) {
    std::size_t count = 0;
    for (int i = 1; i < argc; ++i) {
        const std::filesystem::path path = argv[i];
        if (std::filesystem::is_directory(path)) {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
                if (entry.is_regular_file() && entry.path().filename() != ".gitkeep") {
                    count += RunFile(entry.path());
                }
            }
        } else {
            count += RunFile(path);
        }
    }
    std::cout << "replayed " << count << " inputs\n";
    return 0;
}
