// chunkdb_chunk_map_bench options and data directories; these do not run the
// benchmark.
#include <cassert>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunk_map_bench_args.hpp"

namespace {

namespace cmb = chunkdb::chunk_map_bench;

std::filesystem::path TempDir(const std::string& name) {
    const auto dir = std::filesystem::temp_directory_path() / ("chunkdb-chunk-map-bench-test-" + name);
    std::filesystem::remove_all(dir);
    return dir;
}

void WriteFile(const std::filesystem::path& path) {
    std::ofstream out(path);
    out << "kept\n";
    assert(out.good());
}

cmb::Args Parse(std::vector<std::string> words) {
    words.insert(words.begin(), "chunkdb_chunk_map_bench");
    std::vector<const char*> argv;
    for (const auto& word : words) {
        argv.push_back(word.c_str());
    }
    return cmb::ParseArgs(static_cast<int>(argv.size()), argv.data());
}

bool Rejected(const std::vector<std::string>& words) {
    try {
        (void)Parse(words);
    } catch (const std::invalid_argument&) {
        return true;
    }
    return false;
}

// A directory given with --data-dir keeps everything the benchmark did not
// make; only the run directories go.
void TestGivenDataDirKeepsOtherFiles() {
    const auto parent = TempDir("given");
    std::filesystem::create_directories(parent);
    WriteFile(parent / "other.txt");
    {
        const cmb::DataDirs dirs(parent, false);
        for (std::size_t run = 0; run < 2; ++run) {
            const cmb::RunDir dir(dirs, run);
            std::filesystem::create_directories(dir.path() / "large");
            WriteFile(dir.path() / "large" / "chunk.wal");
        }
        assert(!std::filesystem::exists(parent / "run-0"));
        assert(!std::filesystem::exists(parent / "run-1"));
    }
    assert(std::filesystem::exists(parent / "other.txt"));
    std::filesystem::remove_all(parent);
}

// A directory the benchmark made is removed when it is empty at the end, and
// kept when something else was put in it.
void TestMadeDataDirIsRemovedOnlyWhenEmpty() {
    const auto parent = TempDir("made");
    {
        const cmb::DataDirs dirs(parent / "nested", false);
        const cmb::RunDir dir(dirs, 0);
        std::filesystem::create_directories(dir.path());
    }
    assert(!std::filesystem::exists(parent / "nested"));
    {
        const cmb::DataDirs dirs(parent / "nested", false);
        WriteFile(parent / "nested" / "other.txt");
    }
    assert(std::filesystem::exists(parent / "nested" / "other.txt"));
    std::filesystem::remove_all(parent);
}

// A run directory that exists already stops the run and stays as it was.
void TestExistingRunDirIsNotReplaced() {
    const auto parent = TempDir("existing");
    std::filesystem::create_directories(parent / "run-0");
    WriteFile(parent / "run-0" / "other.txt");
    {
        const cmb::DataDirs dirs(parent, false);
        bool threw = false;
        try {
            const cmb::RunDir dir(dirs, 0);
        } catch (const std::runtime_error&) {
            threw = true;
        }
        assert(threw);
    }
    assert(std::filesystem::exists(parent / "run-0" / "other.txt"));
    std::filesystem::remove_all(parent);
}

// A failed run deletes its directory too.
void TestFailedRunRemovesItsDir() {
    const auto parent = TempDir("failed");
    {
        const cmb::DataDirs dirs(parent, false);
        try {
            const cmb::RunDir dir(dirs, 0);
            std::filesystem::create_directories(dir.path());
            WriteFile(dir.path() / "chunk.wal");
            throw std::runtime_error("run failed");
        } catch (const std::runtime_error&) {
        }
        assert(!std::filesystem::exists(parent / "run-0"));
    }
    assert(!std::filesystem::exists(parent));
}

void TestKeepDataKeepsEverything() {
    const auto parent = TempDir("keep");
    {
        const cmb::DataDirs dirs(parent, true);
        const cmb::RunDir dir(dirs, 0);
        std::filesystem::create_directories(dir.path());
        WriteFile(dir.path() / "chunk.wal");
    }
    assert(std::filesystem::exists(parent / "run-0" / "chunk.wal"));
    std::filesystem::remove_all(parent);
}

void TestDefaultDataDirsAreDistinct() {
    const auto first = Parse({});
    const auto second = Parse({});
    assert(first.data_dir != second.data_dir);
    assert(first.data_dir == std::filesystem::temp_directory_path() / first.data_dir.filename());
    assert(!first.help && first.runs == 15 && first.ops == 204800 && first.chunks == 16384);
}

void TestSizesMustBePositiveDigits() {
    const auto args = Parse({"--chunks", "256", "--ops", "2048", "--runs", "2", "--data-dir", "x", "--keep-data"});
    assert(args.chunks == 256 && args.ops == 2048 && args.runs == 2);
    assert(args.data_dir == std::filesystem::path("x") && args.keep_data);
    for (const std::string flag : {"--chunks", "--ops", "--runs"}) {
        for (const std::string value : {"-1", "-1024", "+1024", " 1024", "", "0", "1024x", "99999999999999999999999"}) {
            assert(Rejected({flag, value}));
        }
    }
    assert(Rejected({"--ops", "1000"}));
    assert(Rejected({"--chunks", "1"}));
    assert(Rejected({"--data-dir", ""}));
    assert(Rejected({"--runs"}));
    assert(Rejected({"--unknown"}));
    assert(Parse({"--help"}).help);
}

}  // namespace

int main() {
    TestGivenDataDirKeepsOtherFiles();
    TestMadeDataDirIsRemovedOnlyWhenEmpty();
    TestExistingRunDirIsNotReplaced();
    TestFailedRunRemovesItsDir();
    TestKeepDataKeepsEverything();
    TestDefaultDataDirsAreDistinct();
    TestSizesMustBePositiveDigits();
    return 0;
}
