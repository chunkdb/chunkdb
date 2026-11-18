// chunkdb_admin: offline administration of a data directory.
//
//   chunkdb_admin --data-dir <dir> reset-password <user> --password-file <file>
//   chunkdb_admin --data-dir <dir> rebuild-version-clock

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
              << "       chunkdb_admin --data-dir <dir> rebuild-version-clock\n"
              << "  reset-password gives <user> the password in <file> (its first line).\n"
              << "  rebuild-version-clock writes each table's lost or damaged revision clock above every revision on disk.\n"
              << "  The server must be stopped.\n";
}

int ResetPasswordCommand(const std::string& data_dir, const std::string& user, const std::string& password_file) {
    std::ifstream file(password_file, std::ios::binary);
    if (!file) {
        throw std::invalid_argument("cannot read " + password_file);
    }
    std::string password;
    std::getline(file, password);
    if (!password.empty() && password.back() == '\r') {
        password.pop_back();
    }
    chunkdb::ResetPassword(data_dir, user, password);
    std::cout << "password of " << user << " reset\n";
    return 0;
}

int RebuildVersionClockCommand(const std::string& data_dir) {
    for (const auto& table : chunkdb::RebuildVersionClock(data_dir, std::cerr)) {
        std::cout << "table " << table.table << ": " << (table.rewritten ? "rebuilt" : "kept")
                  << ", next revision " << table.ceiling << ", highest on disk " << table.highest_revision << "\n";
    }
    return 0;
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
    const bool reset = !password_file.empty() && positional.size() == 2 && positional[0] == "reset-password";
    const bool rebuild = password_file.empty() && positional.size() == 1 && positional[0] == "rebuild-version-clock";
    if (data_dir.empty() || (!reset && !rebuild)) {
        PrintUsage();
        return 2;
    }
    try {
        return reset ? ResetPasswordCommand(data_dir, positional[1], password_file) : RebuildVersionClockCommand(data_dir);
    } catch (const std::exception& e) {
        std::cerr << "chunkdb_admin: " << e.what() << "\n";
        return 1;
    }
}
