// chunkdb_docs_check: documentation checks.
//
//   examples      compare marked Markdown examples with output from the
//                 current server and CLI
//   walkthrough   run the tutorial-to-reference copy path against one server
//   links         check local Markdown targets and heading anchors
//   image-tags    exercise the release workflow's image tag calculation
//   docker-quickstart-commands
//                 extract and rewrite the Docker quick-start page commands
//                 for scripts/test/docker-quickstart.sh, and redact its logs
//
// Exit status: 0 = pass, 1 = check failed, 2 = invalid invocation.

#include <csignal>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <optional>
#include <string>
#include <vector>

#include "docs_check.hpp"

namespace fs = std::filesystem;
using namespace chunkdb::docs_check;

namespace {

constexpr const char* kUsage =
    "Usage:\n"
    "  chunkdb_docs_check examples [--root DIR] [--server PATH] --cli PATH [--record] [PAGE...]\n"
    "  chunkdb_docs_check walkthrough [--root DIR] [--server PATH] --cli PATH\n"
    "  chunkdb_docs_check links [--root DIR]\n"
    "  chunkdb_docs_check image-tags [--root DIR] [--workflow PATH]\n"
    "  chunkdb_docs_check docker-quickstart-commands [--page FILE] [--reset-page FILE] --check-blocks\n"
    "  chunkdb_docs_check docker-quickstart-commands [--page FILE] [--reset-page FILE]\n"
    "      --container NAME --image IMAGE [--port PORT]\n"
    "      (--code | --block | --password-export | --last-line) BLOCK\n"
    "  chunkdb_docs_check docker-quickstart-commands --redact [--secret FILE]... < LOG > REDACTED\n"
    "--root defaults to the current directory, --server to ROOT/build/chunkdb_server,\n"
    "--page to docs/QUICK_START.md and --reset-page to docs/PASSWORD_RESET.md.\n";

class Arguments {
  public:
    Arguments(int argc, char** argv) : args_(argv + 2, argv + argc) {}

    bool Flag(const std::string& name) {
        for (auto it = args_.begin(); it != args_.end(); ++it) {
            if (*it == name) {
                args_.erase(it);
                return true;
            }
        }
        return false;
    }

    std::vector<std::string> Values(const std::string& name) {
        std::vector<std::string> values;
        for (std::size_t i = 0; i < args_.size();) {
            if (args_[i] == name) {
                if (i + 1 >= args_.size()) throw UsageError("argument " + name + ": expected one argument");
                values.push_back(args_[i + 1]);
                args_.erase(args_.begin() + static_cast<std::ptrdiff_t>(i), args_.begin() + static_cast<std::ptrdiff_t>(i) + 2);
            } else if (args_[i].rfind(name + "=", 0) == 0) {
                values.push_back(args_[i].substr(name.size() + 1));
                args_.erase(args_.begin() + static_cast<std::ptrdiff_t>(i));
            } else {
                ++i;
            }
        }
        return values;
    }

    std::optional<std::string> Value(const std::string& name) {
        std::vector<std::string> values = Values(name);
        if (values.empty()) return std::nullopt;
        return values.back();
    }

    std::string Required(const std::string& name) {
        std::optional<std::string> value = Value(name);
        if (!value) throw UsageError("the following arguments are required: " + name);
        return *value;
    }

    std::vector<std::string> Positionals() {
        for (const std::string& arg : args_) {
            if (arg.rfind("--", 0) == 0) throw UsageError("unrecognized arguments: " + arg);
        }
        return std::move(args_);
    }

    void NoPositionals() {
        if (!args_.empty()) throw UsageError("unrecognized arguments: " + args_.front());
    }

  private:
    std::vector<std::string> args_;
};

ExamplesOptions ReadExamplesOptions(Arguments& args, bool pages) {
    ExamplesOptions options;
    options.root = args.Value("--root").value_or(".");
    options.server = args.Value("--server").value_or("");
    options.cli = args.Required("--cli");
    if (pages) {
        options.record = args.Flag("--record");
        for (const std::string& page : args.Positionals()) options.pages.emplace_back(page);
    } else {
        args.NoPositionals();
    }
    return options;
}

int DockerQuickStartCommands(Arguments& args) {
    if (args.Flag("--redact")) {
        std::vector<std::string> secrets;
        for (const std::string& file : args.Values("--secret")) secrets.push_back(ReadText(file));
        args.NoPositionals();
        std::cin >> std::noskipws;
        const std::string input((std::istreambuf_iterator<char>(std::cin)), std::istreambuf_iterator<char>());
        std::cout << Redact(TranslateNewlines(input), secrets) << std::flush;
        return 0;
    }
    const fs::path page = args.Value("--page").value_or("docs/QUICK_START.md");
    const fs::path reset_page = args.Value("--reset-page").value_or("docs/PASSWORD_RESET.md");
    if (args.Flag("--check-blocks")) {
        args.NoPositionals();
        (void)ExtractQuickStart(ReadQuickStartPages(page, reset_page));
        std::cout << "PASS: all nine named shell fences present exactly once\n";
        return 0;
    }
    const std::string container = args.Required("--container");
    const std::string image = args.Required("--image");
    const std::optional<std::string> port = args.Value("--port");
    std::optional<std::string> mode;
    std::string block;
    for (const char* option : {"--code", "--block", "--password-export", "--last-line"}) {
        if (const std::optional<std::string> value = args.Value(option)) {
            if (mode) throw UsageError("choose one of --code, --block, --password-export and --last-line");
            mode = option;
            block = *value;
        }
    }
    if (!mode) throw UsageError("one of --code, --block, --password-export, --last-line or --check-blocks is required");
    args.NoPositionals();
    QuickStartFixture fixture = MakeQuickStartFixture(ReadQuickStartPages(page, reset_page), container, image);
    fixture.port = port;
    if (*mode == "--code") {
        std::cout << QuickStartCode(fixture, block);
    } else if (*mode == "--block") {
        std::cout << QuickStartBlock(fixture, block);
    } else if (*mode == "--password-export") {
        std::cout << QuickStartPasswordExport(fixture, block);
    } else {
        std::cout << QuickStartLastLine(fixture, block);
    }
    std::cout << std::flush;
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2 || std::string(argv[1]) == "--help" || std::string(argv[1]) == "-h") {
        (argc < 2 ? std::cerr : std::cout) << kUsage;
        return argc < 2 ? 2 : 0;
    }
#ifdef SIGPIPE
    // A write to a child that exited is an error, not a signal.
    std::signal(SIGPIPE, SIG_IGN);
#endif
    const std::string command = argv[1];
    try {
        Arguments args(argc, argv);
        if (command == "examples") return RunExamples(ReadExamplesOptions(args, true), std::cout);
        if (command == "walkthrough") return RunWalkthrough(ReadExamplesOptions(args, false), std::cout);
        if (command == "links") {
            const fs::path root = args.Value("--root").value_or(".");
            args.NoPositionals();
            return RunLinks(root, std::cout, std::cerr);
        }
        if (command == "image-tags") {
            const fs::path root = args.Value("--root").value_or(".");
            const fs::path workflow =
                args.Value("--workflow").value_or((root / ".github/workflows/release-binaries.yml").string());
            args.NoPositionals();
            return RunImageTags(workflow, std::cout, std::cerr);
        }
        if (command == "docker-quickstart-commands") return DockerQuickStartCommands(args);
        throw UsageError("unknown command: " + command);
    } catch (const UsageError& error) {
        std::cout << std::flush;
        std::cerr << kUsage << "chunkdb_docs_check " << command << ": error: " << error.what() << "\n";
        return 2;
    } catch (const std::exception& error) {
        std::cout << std::flush;
        std::cerr << "chunkdb_docs_check " << command << ": " << error.what() << "\n";
        return 1;
    }
}
