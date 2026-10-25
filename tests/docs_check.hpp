#pragma once

// Documentation checks: checked Markdown examples against a running server
// and chunk-cli, local links and anchors, the release image tag step, and the
// Docker quick-start command extraction used by scripts/test/docker-quickstart.sh.

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <map>
#include <optional>
#include <ostream>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace chunkdb::docs_check {

// An invalid page or example marking (a page author's error).
class DocsError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// An invalid command line (exit status 2).
class UsageError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// A command or wait that ran past its deadline.
class TimeoutError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Unicode text helpers: whitespace, word characters, strip and line splitting
// as the documentation patterns use them.
std::u32string DecodeUtf8(std::string_view text);
std::string EncodeUtf8(std::u32string_view text);
bool IsSpace(char32_t c);
bool IsWord(char32_t c);
std::string Strip(std::string_view text);
std::vector<std::string> SplitLines(std::string_view text);
// \r\n and \r become \n, as text-mode reads do.
std::string TranslateNewlines(std::string_view text);
// Reads a UTF-8 file with \r\n and \r translated to \n.
std::string ReadText(const std::filesystem::path& path);
std::string ReplaceAll(std::string text, std::string_view from, std::string_view to);

// Checked examples: an input fence tagged `cql docs-check[ session=NAME]`,
// `sh docs-check-watch`, `sh docs-check-replay` or `sh docs-check-shell`,
// immediately followed by an `output` fence.
struct Fence {
    std::string tag;
    std::string code;
    std::size_t start = 0;
    std::size_t end = 0;
    std::size_t code_start = 0;
    std::size_t code_end = 0;
};

struct Example {
    std::string kind;
    std::string session;
    std::string code;
    Fence output;
};

std::vector<Example> Examples(const std::string& text);

// Replaces only fields with documented volatility.
std::string Normalize(std::string text);

std::vector<std::string> UnifiedDiff(const std::vector<std::string>& a, const std::vector<std::string>& b,
                                     const std::string& fromfile, const std::string& tofile);

// Local Markdown links and heading anchors.
struct Link {
    std::string url;
    std::size_t start = 0;
};

struct SplitUrl {
    std::string scheme;
    std::string netloc;
    std::string path;
    std::string query;
    std::string fragment;
};

std::vector<Link> FindLinks(const std::u32string& text);
SplitUrl UrlSplit(std::string_view url);
std::string Unquote(std::string_view text);
std::set<std::string> Anchors(const std::string& text);

// Docker quick-start pages.
extern const std::vector<std::string> kQuickStartBlocks;

std::map<std::string, std::string> ExtractQuickStart(const std::string& text);
std::string ReadQuickStartPages(const std::filesystem::path& page, const std::filesystem::path& reset_page);

struct QuickStartFixture {
    std::map<std::string, std::string> blocks;
    std::string container;
    std::string volume;
    std::string image;
    std::optional<std::string> port;
};

// Extracts the blocks and checks that the page still names the published image.
QuickStartFixture MakeQuickStartFixture(const std::string& pages, const std::string& container,
                                        const std::string& image);
std::string QuickStartCode(const QuickStartFixture& fixture, const std::string& name);
std::string QuickStartBlock(const QuickStartFixture& fixture, const std::string& name);
std::string QuickStartPasswordExport(const QuickStartFixture& fixture, const std::string& name);
std::string QuickStartLastLine(const QuickStartFixture& fixture, const std::string& name);
std::string Redact(std::string text, const std::vector<std::string>& secrets);
std::string ShellQuote(std::string_view text);

// Checks that need a POSIX system (processes, signals, bash).
bool ProcessChecksSupported();

struct ExamplesOptions {
    std::filesystem::path root;
    std::filesystem::path server;
    std::filesystem::path cli;
    bool record = false;
    std::vector<std::filesystem::path> pages;
};

int RunExamples(const ExamplesOptions& options, std::ostream& out);
int RunWalkthrough(const ExamplesOptions& options, std::ostream& out);
int RunLinks(const std::filesystem::path& root, std::ostream& out, std::ostream& err);
int RunImageTags(const std::filesystem::path& workflow, std::ostream& out, std::ostream& err);

// Starts `CLI --uri URI shell` and waits for its first prompt; exposed for tests.
void OpenShell(const std::filesystem::path& cli, const std::string& uri, std::chrono::milliseconds deadline);

}  // namespace chunkdb::docs_check
