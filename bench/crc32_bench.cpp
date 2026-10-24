// CRC32 throughput of each implementation this build and CPU can run, in
// MB/s, for buffers of 64 B, 1 KiB, 16 KiB and 1 MiB: the median of 9 runs.
//
//   chunkdb_crc32_bench [--runs N] [--bytes-per-run N]

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "crc32_impl.hpp"

namespace {

using chunkdb::crc32_detail::Crc32ExtendWith;
using chunkdb::crc32_detail::Crc32Impl;

double MegabytesPerSecond(Crc32Impl impl, const std::vector<std::uint8_t>& buffer, std::size_t size,
                          std::size_t bytes_per_run, std::uint32_t* sink) {
    const std::size_t calls = std::max<std::size_t>(1, bytes_per_run / size);
    std::uint32_t crc = *sink;
    const auto start = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < calls; ++i) {
        crc = Crc32ExtendWith(impl, crc, buffer.data() + (i & 7U), size);
    }
    const std::chrono::duration<double> elapsed = std::chrono::steady_clock::now() - start;
    *sink = crc;
    return static_cast<double>(calls * size) / 1e6 / elapsed.count();
}

}  // namespace

int main(int argc, char** argv) {
    int runs = 9;
    std::size_t bytes_per_run = 256U << 20U;
    for (int i = 1; i + 1 < argc; i += 2) {
        if (std::strcmp(argv[i], "--runs") == 0) {
            runs = std::atoi(argv[i + 1]);
        } else if (std::strcmp(argv[i], "--bytes-per-run") == 0) {
            bytes_per_run = static_cast<std::size_t>(std::strtoull(argv[i + 1], nullptr, 10));
        }
    }
    const std::size_t sizes[] = {64, 1024, 16 * 1024, 1024 * 1024};
    std::vector<std::uint8_t> buffer(sizes[3] + 8);
    for (std::size_t i = 0; i < buffer.size(); ++i) {
        buffer[i] = static_cast<std::uint8_t>(i * 2654435761U >> 13U);
    }
    std::uint32_t sink = 0;
    std::printf("%-8s %12s %12s %12s %12s   (MB/s, median of %d)\n", "impl", "64 B", "1 KiB", "16 KiB", "1 MiB", runs);
    for (const Crc32Impl impl : chunkdb::crc32_detail::kAllCrc32Impls) {
        if (!chunkdb::crc32_detail::Crc32ImplAvailable(impl)) {
            continue;
        }
        std::printf("%-8s", chunkdb::crc32_detail::Crc32ImplName(impl));
        for (const std::size_t size : sizes) {
            std::vector<double> results;
            for (int run = 0; run < runs; ++run) {
                results.push_back(MegabytesPerSecond(impl, buffer, size, bytes_per_run, &sink));
            }
            std::sort(results.begin(), results.end());
            std::printf(" %12.0f", results[results.size() / 2]);
        }
        std::printf("\n");
    }
    std::printf("active: %s (checksum %08x)\n", chunkdb::crc32_detail::Crc32ImplName(chunkdb::crc32_detail::ActiveCrc32Impl()),
                static_cast<unsigned>(sink));
    return 0;
}
