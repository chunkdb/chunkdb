#include <iostream>
#include <string>

#include "chunkdb/server_bench.hpp"

int main(int argc, char** argv) {
    try {
        const chunkdb::server_bench::Args args = chunkdb::server_bench::ParseArgs(argc, argv);
        if (args.show_help) {
            std::cout << chunkdb::server_bench::UsageText();
            return 0;
        }

        const auto report = chunkdb::server_bench::Run(args);
        if (args.output_mode == chunkdb::server_bench::OutputMode::kJson) {
            std::cout << chunkdb::server_bench::RenderJsonReport(report) << "\n";
        } else {
            std::cout << chunkdb::server_bench::RenderHumanReport(report);
        }
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "server benchmark failed: " << e.what() << "\n";
        return 1;
    }
}
