#include <charconv>
#include <iostream>
#include "backup.hpp"

int main(int argc, char** argv) {
    const auto usage = "Usage: chunkdb_restore <backup> <data-dir> [--archive <path> --seal-ms <capture-ms>]\n";
    if (argc == 2 && (std::string_view(argv[1]) == "--help" || std::string_view(argv[1]) == "-h")) {
        std::cout << usage;
        return 0;
    }
    if (argc != 3 && (argc != 7 || std::string_view(argv[3]) != "--archive" || std::string_view(argv[5]) != "--seal-ms")) {
        std::cerr << usage;
        return 2;
    }
    try {
        if (argc == 3) chunkdb::RestoreBackup(argv[1], argv[2]);
        else {
            const std::string_view text(argv[6]);
            std::uint64_t captured_ms = 0U;
            const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), captured_ms);
            if (error != std::errc{} || end != text.data() + text.size() || captured_ms == 0U) {
                std::cerr << "chunkdb_restore: --seal-ms requires a positive capture timestamp\n";
                return 2;
            }
            for (const auto& diagnosis : chunkdb::RestoreArchiveSeal(argv[1], argv[2], argv[4], captured_ms))
                std::cerr << "chunkdb_restore: " << diagnosis << '\n';
        }
    }
    catch (const chunkdb::BackupPublicationUnknownError& error) {
        std::cerr << "chunkdb_restore: " << error.what() << '\n';
        return 3;
    }
    catch (const std::exception& error) {
        std::cerr << "chunkdb_restore: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
