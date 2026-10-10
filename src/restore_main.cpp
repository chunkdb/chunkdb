#include <iostream>
#include "backup.hpp"

int main(int argc, char** argv) {
    if (argc == 2 && (std::string_view(argv[1]) == "--help" || std::string_view(argv[1]) == "-h")) {
        std::cout << "Usage: chunkdb_restore <backup> <data-dir>\n";
        return 0;
    }
    if (argc != 3) {
        std::cerr << "Usage: chunkdb_restore <backup> <data-dir>\n";
        return 2;
    }
    try { chunkdb::RestoreBackup(argv[1], argv[2]); }
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
