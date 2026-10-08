// chunkdb_admin: offline administration of a data directory.
//
//   chunkdb_admin --data-dir <dir> reset-password <user> --password-file <file>

#include <exception>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include "admin.hpp"

namespace {

void PrintUsage() {
    std::cerr << "Usage: chunkdb_admin --data-dir <dir> reset-password <user> --password-file <file>\n"
              << "  Gives <user> the password in <file> (its first line). The server must be stopped.\n";
}

}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::string> args(argv + 1, argv + argc);
    std::string data_dir;
    std::string password_file;
    std::vector<std::string> positional;
    for (std::size_t i = 0; i < args.size(); ++i) {
        if ((args[i] == "--data-dir" || args[i] == "--password-file") && i + 1 < args.size()) {
            (args[i] == "--data-dir" ? data_dir : password_file) = args[i + 1];
            ++i;
        } else if (args[i] == "--help" || args[i] == "-h") {
            PrintUsage();
            return 0;
        } else {
            positional.push_back(args[i]);
        }
    }
    if (data_dir.empty() || password_file.empty() || positional.size() != 2 || positional[0] != "reset-password") {
        PrintUsage();
        return 2;
    }
    try {
        std::ifstream file(password_file, std::ios::binary);
        if (!file) {
            throw std::invalid_argument("cannot read " + password_file);
        }
        std::string password;
        std::getline(file, password);
        if (!password.empty() && password.back() == '\r') {
            password.pop_back();
        }
        chunkdb::ResetPassword(data_dir, positional[1], password);
        std::cout << "password of " << positional[1] << " reset\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "chunkdb_admin: " << e.what() << "\n";
        return 1;
    }
}
