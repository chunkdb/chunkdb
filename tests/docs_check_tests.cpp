// Unit tests for chunkdb_docs_check: example markings, normalizers, diffs,
// anchors and the Docker quick-start extraction. No server or Docker needed.
// Usage: chunkdb_docs_check_test SOURCE_DIR

#include <cassert>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "docs_check.hpp"

#ifndef _WIN32
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#endif

namespace fs = std::filesystem;
using namespace chunkdb::docs_check;

namespace {

template <class Error>
void ExpectThrows(const std::function<void()>& body, const std::string& needle = "") {
    try {
        body();
    } catch (const Error& error) {
        if (std::string(error.what()).find(needle) == std::string::npos) {
            std::cerr << "unexpected message: " << error.what() << " (wanted " << needle << ")\n";
            assert(false);
        }
        return;
    }
    std::cerr << "expected an error containing: " << needle << "\n";
    assert(false);
}

std::string Replace(std::string text, const std::string& from, const std::string& to) {
    return ReplaceAll(std::move(text), from, to);
}

std::string ReplaceFirst(std::string text, const std::string& from, const std::string& to) {
    const std::size_t at = text.find(from);
    assert(at != std::string::npos);
    return text.replace(at, from.size(), to);
}

void UnclosedMarkedFencesRejected() {
    const std::string valid = "```cql docs-check\nPING\n```\n\n```output\nPONG\n```\n";
    for (const std::string suffix : {"```cql docs-check\nPING\n", "```cql docs-check\nPING\n```\n\n```output\nPONG\n",
                                     "```cql docs-check\nPING\n```output\nPONG\n```\n"}) {
        ExpectThrows<DocsError>([&] { (void)Examples(valid + suffix); }, "line");
    }
}

void ExpectedOutputRequired() {
    for (const std::string text : {"```cql docs-check\nPING\n```", "```cql docs-check\nPING\n```\n\n```text\nPONG\n```",
                                   "```cql docs-check\nPING\n```\nexplanation\n```output\nPONG\n```"}) {
        ExpectThrows<DocsError>([&] { (void)Examples(text); });
    }
}

void UnknownMarkerRejected() {
    ExpectThrows<DocsError>([] { (void)Examples("```sh docs-check-unknown\ntrue\n```\n\n```output\nOK\n```"); },
                            "unknown example tag");
    ExpectThrows<DocsError>([] { (void)Examples("```cql docs-check-unknown\nPING\n```\n\n```output\nPONG\n```"); },
                            "unknown example tag");
}

void TwoConnectionsAndWatchPreserveSource() {
    const std::string source =
        "```cql docs-check session=first\nBEGIN\n```\n\n```output\nOK\n```\n"
        "```cql docs-check session=second\nPING\n```\n\n```output\nPONG\n```\n"
        "```sh docs-check-watch\nchunk-cli watch world\n```\n\n```output\nstart <epoch>:0\n```";
    const auto examples = Examples(source);
    assert(examples.size() == 3);
    assert(examples[0].kind == "cql" && examples[0].session == "first" && examples[0].code == "BEGIN");
    assert(examples[1].kind == "cql" && examples[1].session == "second" && examples[1].code == "PING");
    assert(examples[2].kind == "watch" && examples[2].session == "main" && examples[2].code == "chunk-cli watch world");
    assert(examples[2].output.code == "start <epoch>:0");
    assert(source.substr(examples[0].output.code_start,
                         examples[0].output.code_end - examples[0].output.code_start) == "OK");
}

void UnmarkedFencesIgnored() {
    const std::string source =
        "```sh\necho one\n```\n````text\n```cql\n````\n```\n```cql docs-check\nPING\n```\n```output\nPONG\n```\n";
    // The ```` line opens a fence; ```cql inside it is content; the bare ```
    // line closes it.
    const auto examples = Examples(source);
    assert(examples.size() == 1 && examples[0].code == "PING" && examples[0].output.code == "PONG");
    ExpectThrows<DocsError>([] { (void)Examples("```sh\n```\n"); }, "invalid fence at line 1");
    ExpectThrows<DocsError>([] { (void)Examples("text\n```sh\necho\n"); }, "unclosed fence at line 2");
}

void OnlyVolatileMetadataChanges() {
    const std::string epoch = "0123456789abcdef0123456789abcdef";
    const std::string source = "start " + epoch +
                               ":2\nchange revision 3 time_ms 123 user admin\n"
                               "1) name = step, applied_ms = 456, user = admin\n"
                               "server_version = 2.0.0\nGenerated password: secret\n";
    assert(Normalize(source) ==
           "start <epoch>:2\nchange revision 3 time_ms <time-ms> user admin\n"
           "1) name = step, applied_ms = <time-ms>, user = admin\n"
           "server_version = <server-version>\nGenerated password: <generated-password>\n");
    const std::string data = "value = '" + epoch + "'\nvalue = 'epoch = " + epoch +
                             ",'\nvalue = 'time_ms 123'\nversion = 42\n42\n";
    assert(Normalize(data) == data);
    assert(Normalize("schema " + epoch + ":4\n") == "schema <epoch>:4\n");
    assert(Normalize("  1) table = world, epoch = " + epoch + ", acked = 3\n") ==
           "  1) table = world, epoch = <epoch>, acked = 3\n");
    assert(Normalize("change revision 2 time_ms -5 user admin\r\n") ==
           "change revision 2 time_ms <time-ms> user admin\n");
}

void MultilineSlotEpochPreservesFields() {
    const std::string epoch = "e8a2a1d9c318cdeefba7349821885917";
    const std::string source =
        "OK\nOK\n1)\n  table = feed_demo\n  name = consumer\n"
        "  epoch = " + epoch + "\n  acked = 0\n  retained_bytes = 0\n"
        "  lost = false\n  lost_reason = NULL\n  filters:\n"
        "    area = NULL\n    column_ids = NULL\n    columns = NULL\n"
        "    no_before = false\n    chunks = false\n";
    for (const std::string& changed :
         {source, Replace(source, "acked = 0", "acked = 37"),
          Replace(source, "retained_bytes = 0", "retained_bytes = 4096"), Replace(source, "lost = false", "lost = true"),
          Replace(source, "lost_reason = NULL", "lost_reason = schema_filter"),
          Replace(source, "area = NULL", "area = [-2, 0, 3, 4]"),
          Replace(source, "columns = NULL", "columns = ['" + epoch + "']"),
          Replace(source, "no_before = false", "no_before = true"), Replace(source, "chunks = false", "chunks = true")}) {
        for (const std::string ending : {"\n", "\r\n"}) {
            assert(Normalize(Replace(changed, "\n", ending)) ==
                   Replace(changed, "  epoch = " + epoch + "\n", "  epoch = <epoch>\n"));
        }
    }
}

void DescribeEpochPreservesSchemaAndFrontier() {
    const std::string epoch = "3c346ca7ea46b36307de83b35d8968cf";
    const std::string source =
        "table = schema_demo\nversion = 1\nchunk = 2 x 2\nlarge = 8 x 8\n"
        "epoch = " + epoch + "\nrevision = 0\ndims = 2\ncolumns:\n"
        "  kind u8\n  name text(16) NULL\noptions:\n"
        "  var_max_chunk_bytes = 1048576\n";
    for (const std::string& changed :
         {source, Replace(source, "version = 1", "version = 17"), Replace(source, "chunk = 2 x 2", "chunk = 4 x 8"),
          Replace(source, "large = 8 x 8", "large = 16 x 32"), Replace(source, "revision = 0", "revision = 37"),
          Replace(Replace(Replace(source, "chunk = 2 x 2", "chunk = 2 x 2 x 2"), "large = 8 x 8", "large = 8 x 8 x 8"), "dims = 2", "dims = 3"),
          Replace(source, "revision = 0", "revision = NULL"), Replace(source, "dims = 2", "dims = 3"),
          Replace(source, "  kind u8", "  epoch text(32) DEFAULT '" + epoch + "'"),
          Replace(source, "1048576", "2097152")}) {
        for (const std::string ending : {"\n", "\r\n"}) {
            assert(Normalize(Replace(changed, "\n", ending)) ==
                   Replace(changed, "epoch = " + epoch + "\n", "epoch = <epoch>\n"));
        }
    }
    const std::string unrelated = "table = world\nepoch = " + epoch + "\nvalue = " + epoch + "\n";
    assert(Normalize(unrelated) == unrelated);
}

void ChunkTimesRequireACompleteSummary() {
    const std::string source = "version = 7\nschema_version = 3\ncommit_time_ms = 123\npresent = 2 of 8 blocks\n"
        "projected = true\nblock 1 1 1\n  commit_time_ms = 456\n";
    for (const std::string indent : {"", "  "}) {
        auto indented = indent + Replace(source, "\n", "\n" + indent);
        if (!indent.empty()) indented.resize(indented.size() - indent.size());
        const auto expected = Replace(indented, "commit_time_ms = 123", "commit_time_ms = <time-ms>");
        assert(Normalize(indented) == expected);
        assert(Normalize(Replace(indented, "\n", "\r\n")) == expected);
    }
    for (const auto& unrelated : {std::string("commit_time_ms = 123\n"),
             Replace(source, "schema_version = 3", "schema_version = '3'"),
             Replace(source, "present = 2 of 8 blocks", "present = '2 of 8 blocks'"),
             Replace(source, "commit_time_ms = 123", "commit_time_ms = NULL")})
        assert(Normalize(unrelated) == unrelated);
}

void ScanEpochPreservesCoordinatesAndRevisions() {
    const std::string epoch = "0123456789abcdef0123456789abcdef";
    const std::string source = "chunks:\n  1) [-1, 2, 37]\nmore = true\nepoch = " + epoch +
        "\nposition = 41\nretained_deletion_floor = 13\n";
    for (const auto& changed : {source, Replace(source, "[-1, 2, 37]", "[-1, 2, -3, 37]"),
             Replace(source, "position = 41", "position = NULL"),
             Replace(source, "chunks:\n  1) [-1, 2, 37]", "chunks = []")}) {
        const auto expected = Replace(changed, epoch, "<epoch>");
        assert(Normalize(changed) == expected);
        assert(Normalize(Replace(changed, "\n", "\r\n")) == expected);
    }
    for (const auto& unrelated : {std::string("epoch = ") + epoch + "\n", Replace(source, "chunks:", "values:"),
             Replace(source, "[-1, 2, 37]", "[-1, 2]"),
             Replace(source, "retained_deletion_floor = 13", "retained_deletion_floor = '13'")})
        assert(Normalize(unrelated) == unrelated);
}

void SlotEpochRequiresMetadataContext() {
    const std::string epoch = "0123456789abcdef0123456789abcdef";
    const std::string unrelated = "  epoch = " + epoch + "\nvalue = " + epoch + "\n" + "value = 'epoch = " + epoch +
                                  "'\n" + "1)\n  table = world\n  name = consumer\n  epoch = " + epoch + "\n" +
                                  "  value = 3\n";
    assert(Normalize(unrelated) == unrelated);
    const std::string legacy =
        "1) table = world, name = consumer, epoch = " + epoch + ", acked = 37, retained_bytes = 4096, lost = true\n";
    for (const std::string ending : {"\n", "\r\n"}) {
        assert(Normalize(Replace(legacy, "\n", ending)) == Replace(legacy, epoch, "<epoch>"));
    }
}

std::string Diff(const std::vector<std::string>& a, const std::vector<std::string>& b) {
    std::string out;
    for (const std::string& line : UnifiedDiff(a, b, "expected", "actual")) out += (out.empty() ? "" : "\n") + line;
    return out;
}

// Expected values were recorded from the previous checker's unified diffs.
void UnifiedDiffMatchesDifflib() {
    assert(Diff({"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, {"a", "b", "X", "d", "e", "f", "g", "h", "i", "j", "k"}) ==
           "--- expected\n+++ actual\n@@ -1,6 +1,6 @@\n a\n b\n-c\n+X\n d\n e\n f\n@@ -8,3 +8,4 @@\n h\n i\n j\n+k");
    assert(Diff({"OK", "2", "3"}, {"OK", "2", "4"}) == "--- expected\n+++ actual\n@@ -1,3 +1,3 @@\n OK\n 2\n-3\n+4");
    assert(Diff({}, {"x"}) == "--- expected\n+++ actual\n@@ -0,0 +1 @@\n+x");
    assert(Diff({"x"}, {}) == "--- expected\n+++ actual\n@@ -1 +0,0 @@\n-x");
    assert(Diff({"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"},
                {"a", "B", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "M", "n"}) ==
           "--- expected\n+++ actual\n@@ -1,5 +1,5 @@\n a\n-b\n+B\n c\n d\n e\n@@ -10,5 +10,5 @@\n j\n k\n l\n-m\n+M\n n");
    assert(Diff({"same"}, {"same"}).empty());
}

void TextHelpersMatchReference() {
    assert((SplitLines("a\r\nb\rc\n\nd") == std::vector<std::string>{"a", "b", "c", "", "d"}));
    assert(SplitLines("").empty());
    assert((SplitLines("\n") == std::vector<std::string>{""}));
    assert(Strip(" \t\xC2\xA0x y\xE3\x80\x80\n") == "x y");
    assert(TranslateNewlines("a\r\nb\rc") == "a\nb\nc");
}

// Expected values were recorded from the previous link checker.
void AnchorsMatchReference() {
    const std::string sample =
        "# Title\n## Rule 1 \xE2\x80\x94 Never `assume`\n### Title\n```sh\n# not a heading\n```\n"
        "#### Ends with hashes ##\n# Title\nText # no\n####### seven\n#NoSpace\n"
        "## Caf\xC3\xA9 \xC3\x9Cn\xC3\xAF" "code \xC2\xB2\n## A/B: c.d (e) ##  \n";
    const std::set<std::string> expected{"ab-cd-e-", "caf\xC3\xA9-\xC3\xBCn\xC3\xAF" "code-\xC2\xB2",
                                         "ends-with-hashes", "rule-1--never-assume", "title", "title-1", "title-2"};
    assert(Anchors(sample) == expected);
}

void LinksMatchReference() {
    std::vector<std::string> urls;
    for (const Link& link :
         FindLinks(DecodeUtf8("see [a](b.md) and ![i](img.png \"t\") [x] (y) [c](d.md#e \"f\") [g](h i) [[n]](o.md)"))) {
        urls.push_back(link.url);
    }
    assert((urls == std::vector<std::string>{"b.md", "img.png", "d.md#e"}));
    const SplitUrl web = UrlSplit("https://example.com/a#b");
    assert(web.scheme == "https" && web.netloc == "example.com" && web.path == "/a" && web.fragment == "b");
    const SplitUrl local = UrlSplit("reference/FEED.md?x=1#durable%20slots");
    assert(local.scheme.empty() && local.netloc.empty() && local.path == "reference/FEED.md" && local.query == "x=1");
    assert(Unquote(local.fragment) == "durable slots");
    assert(UrlSplit("#top").path.empty() && UrlSplit("#top").fragment == "top");
    assert(UrlSplit("mailto:x@y").scheme == "mailto");
    assert(Unquote("%zz%41%") == "%zzA%");
}

std::string Pages(const fs::path& source) {
    return ReadQuickStartPages(source / "docs/QUICK_START.md", source / "docs/PASSWORD_RESET.md");
}

void ExactPageCommands(const fs::path& source) {
    const auto blocks = ExtractQuickStart(Pages(source));
    assert(blocks.size() == 9);
    assert(blocks.at("write").find("CREATE TABLE world") != std::string::npos);
    assert(blocks.at("reset").find("reset-password admin --password-file /dev/stdin") != std::string::npos);
    assert(blocks.at("write").rfind("chunk-cli --uri \"$CHUNKDB_URI\" shell <<'CQL'\n", 0) == 0);
}

void DuplicateRejected(const fs::path& source) {
    ExpectThrows<DocsError>(
        [&] { (void)ExtractQuickStart(Pages(source) + "\n<!-- docker-quickstart:health -->\n```sh\nfalse\n```"); },
        "duplicate");
}

void MissingOrWrongShellRejected(const fs::path& source) {
    const std::string text = Pages(source);
    for (const std::string& broken : {Replace(text, "docker-quickstart:health", "docker-quickstart:unknown"),
                                      ReplaceFirst(text, "```sh", "```text")}) {
        ExpectThrows<DocsError>([&] { (void)ExtractQuickStart(broken); }, "quick-start blocks differ");
    }
}

void ChangedPageImageRejected(const fs::path& source) {
    const std::string changed = Replace(Pages(source), "ghcr.io/chunkdb/chunkdb:2", "ghcr.io/chunkdb/chunkdb:2.1.0");
    ExpectThrows<DocsError>([&] { (void)MakeQuickStartFixture(changed, "chunkdb-page-test", "chunkdb:local-check"); },
                            "published image changed");
}

void FixtureChangesOnlyEnvironment(const fs::path& source) {
    QuickStartFixture fixture = MakeQuickStartFixture(Pages(source), "chunkdb-page-0123456789ab", "chunkdb:local-check");
    fixture.port = "45423";
    for (const std::string& name : kQuickStartBlocks) {
        const std::string code = QuickStartCode(fixture, name);
        assert(code.find("chunkdb-quickstart") == std::string::npos);
        assert(code.find("127.0.0.1:4242/") == std::string::npos);
    }
    assert(SplitLines(ExtractQuickStart(Pages(source)).at("write")) == SplitLines(QuickStartCode(fixture, "write")));
    assert(QuickStartCode(fixture, "start").find("127.0.0.1::4242") != std::string::npos);
    assert(QuickStartCode(fixture, "start").find("chunkdb-page-0123456789ab-data") != std::string::npos);
    assert(QuickStartCode(fixture, "start").find("chunkdb:local-check") != std::string::npos);
    assert(QuickStartBlock(fixture, "login").rfind("set -eu\n", 0) == 0);
    assert(QuickStartBlock(fixture, "login").find("printf 'PING\\nexit\\n' | chunk-cli --uri \"$CHUNKDB_URI\" shell") !=
           std::string::npos);
    assert(QuickStartLastLine(fixture, "reset-login") == "chunk-cli --uri \"$CHUNKDB_URI\" PING");
    assert(Redact("dummy-secret", {"dummy-secret"}).find("dummy-secret") == std::string::npos);
    assert(Redact("Generated password: 123secret\n", {}) == "Generated password: <redacted>\n");
    assert(Redact("x Generated password: \nGenerated password: a\r\n", {}) ==
           "x Generated password: \nGenerated password: <redacted>\r\n");
    assert(ShellQuote("chunkdb:quickstart-ci") == "chunkdb:quickstart-ci");
    assert(ShellQuote("it's") == "'it'\"'\"'s'");
    assert(ShellQuote("") == "''");
}

void ResetPasswordComesFromPage(const fs::path& source) {
    const std::string page = Replace(Pages(source), "choose-a-new-private-password", "different documented secret");
    const QuickStartFixture fixture = MakeQuickStartFixture(page, "chunkdb-page-test", "chunkdb:local-check");
    const std::string secret = "different documented secret";
    assert(QuickStartPasswordExport(fixture, "reset-login") == "export CHUNKDB_PASSWORD='" + secret + "'");
    const std::string logged = Redact(QuickStartCode(fixture, "reset"), {secret});
    assert(logged.find(secret) == std::string::npos);
    assert(logged.find("<redacted>") != std::string::npos);
    ExpectThrows<DocsError>([&] { (void)QuickStartPasswordExport(fixture, "write"); },
                            "expected one password export in page block: write");
}

#ifndef _WIN32
std::set<int> OpenDescriptors() {
    std::set<int> open;
    for (int fd = 0; fd < 256; ++fd) {
        if (::fcntl(fd, F_GETFD) != -1) open.insert(fd);
    }
    return open;
}

void InitialPromptFailureReapsChild() {
    const fs::path directory = fs::temp_directory_path() / ("chunkdb-docs-check-test-" + std::to_string(::getpid()));
    fs::create_directories(directory);
    const fs::path cli = directory / "chunk-cli";
    const fs::path pid_file = directory / "pid";
    {
        std::ofstream script(cli);
        script << "#!/bin/sh\necho $$ > '" << pid_file.string() << "'\nexec cat > /dev/null\n";
    }
    fs::permissions(cli, fs::perms::owner_all);
    const std::set<int> before = OpenDescriptors();
    ExpectThrows<TimeoutError>([&] { OpenShell(cli, "chunk://127.0.0.1/", std::chrono::milliseconds(1000)); },
                               "timeout waiting for initial shell prompt");
    pid_t pid = 0;
    {
        std::ifstream pid_in(pid_file);
        pid_in >> pid;
    }
    assert(pid > 0);
    // Reaped, not a zombie: the pid no longer exists.
    assert(::kill(pid, 0) == -1 && errno == ESRCH);
    assert(OpenDescriptors() == before);
    fs::remove_all(directory);
}
#endif

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: chunkdb_docs_check_test SOURCE_DIR\n";
        return 2;
    }
    const fs::path source = argv[1];
#ifndef _WIN32
    ::signal(SIGPIPE, SIG_IGN);
#endif
    UnclosedMarkedFencesRejected();
    ExpectedOutputRequired();
    UnknownMarkerRejected();
    TwoConnectionsAndWatchPreserveSource();
    UnmarkedFencesIgnored();
    OnlyVolatileMetadataChanges();
    MultilineSlotEpochPreservesFields();
    DescribeEpochPreservesSchemaAndFrontier();
    ChunkTimesRequireACompleteSummary();
    ScanEpochPreservesCoordinatesAndRevisions();
    SlotEpochRequiresMetadataContext();
    UnifiedDiffMatchesDifflib();
    TextHelpersMatchReference();
    AnchorsMatchReference();
    LinksMatchReference();
    ExactPageCommands(source);
    DuplicateRejected(source);
    MissingOrWrongShellRejected(source);
    ChangedPageImageRejected(source);
    FixtureChangesOnlyEnvironment(source);
    ResetPasswordComesFromPage(source);
#ifndef _WIN32
    if (ProcessChecksSupported()) InitialPromptFailureReapsChild();
#endif
    std::cout << "docs_check tests passed\n";
    return 0;
}
