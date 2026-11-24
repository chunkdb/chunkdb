// chunkdb_admin: offline administration of a data directory.
//
//   chunkdb_admin --data-dir <dir> reset-password <user> --password-file <file>
//   chunkdb_admin --data-dir <dir> rebuild-version-clock
//   chunkdb_admin --data-dir <dir> quarantine --table <table> --chunk <cx> <cy> [<cz>] [--force]

#include <charconv>
#include <exception>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <string>
#include <vector>

#include "admin.hpp"

namespace {

void PrintUsage() {
    std::cerr << "Usage: chunkdb_admin --data-dir <dir> reset-password <user> --password-file <file>\n"
              << "       chunkdb_admin --data-dir <dir> rebuild-version-clock\n"
              << "       chunkdb_admin --data-dir <dir> quarantine --table <table> --chunk <cx> <cy> [<cz>] [--force]\n"
              << "  reset-password gives <user> the password in <file> (its first line).\n"
              << "  rebuild-version-clock writes each table's lost or damaged revision clock above every revision on disk.\n"
              << "  quarantine moves a damaged chunk's image and WAL into the table's .chunkdb.quarantine directory,\n"
              << "  so the table opens without the chunk; --force moves a chunk the checks find no damage in.\n"
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
                  << ", next revision " << table.ceiling << ", highest on disk " << table.highest_revision
                  << (table.marker_rewritten ? ", marker rewritten" : "") << "\n";
    }
    return 0;
}

int QuarantineCommand(const std::string& data_dir, const std::string& table, const std::vector<std::int64_t>& chunk,
    bool force) {
    const chunkdb::ChunkCoord coord{chunk[0], chunk[1], chunk.size() == 3U ? chunk[2] : 0};
    const auto result = chunkdb::QuarantineChunk(data_dir, table, static_cast<std::uint8_t>(chunk.size()), coord, force, std::cerr);
    std::cout << (result.resumed ? "finished quarantine of" : "quarantined") << " chunk";
    for (const auto axis : chunk) std::cout << ' ' << axis;
    std::cout << " of table " << table << " in " << result.entry.generic_string() << ":";
    for (const auto& file : result.files) std::cout << ' ' << file;
    if (!result.damage.empty()) std::cout << " (" << result.damage << ")";
    std::cout << "\n";
    if (result.slots_marked != 0U)
        std::cout << result.slots_marked << " slots start their next watch with resync\n";
    if (result.table_errors != 0U) {
        std::cout << "the table still has " << result.table_errors << " errors; see the findings above\n";
        return 1;
    }
    return 0;
}

std::optional<std::int64_t> Coordinate(const std::string& text) {
    std::int64_t value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size()) return std::nullopt;
    return value;
}

}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::string> args(argv + 1, argv + argc);
    std::string data_dir;
    std::string password_file;
    std::string table;
    std::vector<std::int64_t> chunk;
    bool chunk_given = false;
    bool force = false;
    std::vector<std::string> positional;
    for (std::size_t i = 0; i < args.size(); ++i) {
        if ((args[i] == "--data-dir" || args[i] == "--password-file" || args[i] == "--table") && i + 1 < args.size()) {
            (args[i] == "--data-dir" ? data_dir : args[i] == "--table" ? table : password_file) = args[i + 1];
            ++i;
        } else if (args[i] == "--chunk") {
            chunk_given = true;
            while (i + 1 < args.size() && chunk.size() < 3U) {
                const auto axis = Coordinate(args[i + 1]);
                if (!axis) break;
                chunk.push_back(*axis);
                ++i;
            }
        } else if (args[i] == "--force") {
            force = true;
        } else if (args[i] == "--help" || args[i] == "-h") {
            PrintUsage();
            return 0;
        } else {
            positional.push_back(args[i]);
        }
    }
    const bool quarantine_options = !table.empty() || chunk_given || force;
    const bool reset = !password_file.empty() && !quarantine_options && positional.size() == 2 &&
        positional[0] == "reset-password";
    const bool rebuild = password_file.empty() && !quarantine_options && positional.size() == 1 &&
        positional[0] == "rebuild-version-clock";
    const bool quarantine = password_file.empty() && positional.size() == 1 && positional[0] == "quarantine" &&
        !table.empty() && (chunk.size() == 2U || chunk.size() == 3U);
    if (data_dir.empty() || (!reset && !rebuild && !quarantine)) {
        PrintUsage();
        return 2;
    }
    try {
        if (quarantine) return QuarantineCommand(data_dir, table, chunk, force);
        return reset ? ResetPasswordCommand(data_dir, positional[1], password_file) : RebuildVersionClockCommand(data_dir);
    } catch (const std::exception& e) {
        std::cerr << "chunkdb_admin: " << e.what() << "\n";
        return 1;
    }
}
