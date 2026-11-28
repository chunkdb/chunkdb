// chunkdb_restore: restores a completed backup offline.
//
//   chunkdb_restore <backup> <data-dir>
//   chunkdb_restore --table <table> [--as <name>] [--area <cx0> <cy0> [<cz0>] TO <cx1> <cy1> [<cz1>]]
//                   [--allow-split] <backup> <data-dir>

#include <charconv>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "backup.hpp"
#include "chunkdb/table_catalog.hpp"
#include "store_manifest.hpp"

namespace {

constexpr const char* kUsage =
    "Usage: chunkdb_restore <backup> <data-dir>\n"
    "       chunkdb_restore --table <table> [--as <name>] [--area <cx0> <cy0> [<cz0>] TO <cx1> <cy1> [<cz1>]]\n"
    "                       [--allow-split] <backup> <data-dir>\n"
    "  Without --table, restores the whole backup into an absent or empty <data-dir>.\n"
    "  With --table, adds that table of the backup to <data-dir>, an existing data directory or a new one,\n"
    "  as a new table named <name> (default <table>); the server must be stopped.\n"
    "  --area restores only the chunks in that range; --allow-split restores an area that splits commits\n"
    "  which also changed chunks outside it.\n";

std::optional<std::int64_t> Coordinate(const std::string& text) {
    std::int64_t value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size()) return std::nullopt;
    return value;
}

struct Arguments {
    std::string table;
    std::string name;
    std::optional<chunkdb::ChunkAreaSelector> area;
    bool allow_split = false;
    std::vector<std::string> positional;
};

// Throws std::invalid_argument for arguments that do not fit the usage.
Arguments Parse(const std::vector<std::string>& args) {
    Arguments parsed;
    bool area_given = false;
    for (std::size_t i = 0; i < args.size(); ++i) {
        const auto& arg = args[i];
        if ((arg == "--table" || arg == "--as") && i + 1 < args.size()) {
            (arg == "--table" ? parsed.table : parsed.name) = args[++i];
        } else if (arg == "--area" && !area_given) {
            area_given = true;
            std::vector<std::int64_t> first, last;
            while (i + 1 < args.size() && Coordinate(args[i + 1])) first.push_back(*Coordinate(args[++i]));
            if (i + 1 >= args.size() || (args[i + 1] != "TO" && args[i + 1] != "to"))
                throw std::invalid_argument("--area takes <cx0> <cy0> [<cz0>] TO <cx1> <cy1> [<cz1>]");
            ++i;
            while (i + 1 < args.size() && last.size() < first.size() && Coordinate(args[i + 1]))
                last.push_back(*Coordinate(args[++i]));
            if ((first.size() != 2U && first.size() != 3U) || last.size() != first.size())
                throw std::invalid_argument("--area takes two or three coordinates on each side of TO");
            chunkdb::ChunkAreaSelector area;
            area.dims = static_cast<std::uint8_t>(first.size());
            area.first = {first[0], first[1], area.dims == 3U ? first[2] : 0};
            area.last = {last[0], last[1], area.dims == 3U ? last[2] : 0};
            if (area.first.x > area.last.x || area.first.y > area.last.y || area.first.z > area.last.z)
                throw std::invalid_argument("--area corners are reversed");
            parsed.area = area;
        } else if (arg == "--allow-split") {
            parsed.allow_split = true;
        } else if (arg.starts_with("--")) {
            throw std::invalid_argument("unknown or incomplete option " + arg);
        } else {
            parsed.positional.push_back(arg);
        }
    }
    if (parsed.positional.size() != 2U) throw std::invalid_argument("give a backup and a data directory");
    if (parsed.table.empty() && (!parsed.name.empty() || parsed.area || parsed.allow_split))
        throw std::invalid_argument("--as, --area and --allow-split need --table");
    if (parsed.name.empty()) parsed.name = parsed.table;
    return parsed;
}

int RestoreTable(const Arguments& arguments) {
    const auto backup = std::filesystem::canonical(arguments.positional[0]);
    const auto data_dir = std::filesystem::weakly_canonical(arguments.positional[1]);
    const auto inside = [](const std::filesystem::path& path, const std::filesystem::path& root) {
        const auto relative = path.lexically_relative(root);
        return !relative.empty() && *relative.begin() != "..";
    };
    if (inside(data_dir, backup) || inside(backup, data_dir))
        throw std::invalid_argument("the data directory and the backup must not contain each other");
    chunkdb::TableRestoreRequest request;
    request.backup = backup;
    request.table = arguments.table;
    request.name = arguments.name;
    request.area = arguments.area;
    request.allow_split = arguments.allow_split;
    const auto result = chunkdb::RestoreTableOffline(data_dir, request);
    std::cout << "restored table " << arguments.table << " as " << result.name << ": epoch "
              << chunkdb::StoreIdHex(result.epoch) << ", parent epoch " << chunkdb::StoreIdHex(result.parent_epoch)
              << " at revision " << result.fork_revision << ", " << result.files << " files";
    if (arguments.area) std::cout << ", " << result.chunks << " chunks in the area";
    std::cout << "\n";
    if (!result.split_revisions.empty()) {
        std::cout << "split commits:";
        for (const auto revision : result.split_revisions) std::cout << ' ' << revision;
        std::cout << "\n";
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::string> args(argv + 1, argv + argc);
    if (args.size() == 1U && (args[0] == "--help" || args[0] == "-h")) {
        std::cout << kUsage;
        return 0;
    }
    Arguments arguments;
    try {
        arguments = Parse(args);
    } catch (const std::exception& error) {
        std::cerr << "chunkdb_restore: " << error.what() << '\n' << kUsage;
        return 2;
    }
    try {
        if (!arguments.table.empty()) return RestoreTable(arguments);
        chunkdb::RestoreBackup(arguments.positional[0], arguments.positional[1]);
    } catch (const chunkdb::BackupPublicationUnknownError& error) {
        std::cerr << "chunkdb_restore: " << error.what() << '\n';
        return 3;
    } catch (const std::exception& error) {
        std::cerr << "chunkdb_restore: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
