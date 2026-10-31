// chunkdb_budget_compare checks on recorded and synthetic samples; these do not
// run benchmarks. Expected numbers were recorded from the earlier Python
// comparator (Python 3.14) on the same inputs.
#include <algorithm>
#include <cassert>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "budget_compare.hpp"

namespace {

namespace bc = chunkdb::budget_compare;
using bc::Value;

Value I(std::int64_t value) { return Value::Int(bc::BigInt(value)); }
Value F(double value) { return Value::Float(value); }
Value S(const std::string& value) { return Value::Str(value); }
Value Obj(std::initializer_list<std::pair<std::string, Value>> members) {
    Value out = Value::Object();
    for (const auto& [key, value] : members) {
        out.Set(key, value);
    }
    return out;
}
Value Arr(std::initializer_list<Value> items) { return Value::Array(std::vector<Value>(items)); }

Value& At(Value& value, std::string_view key) {
    Value* found = value.Find(key);
    assert(found != nullptr);
    return *found;
}
const Value& At(const Value& value, std::string_view key) {
    const Value* found = value.Find(key);
    assert(found != nullptr);
    return *found;
}

std::string Hex64(char c) { return std::string(64, c); }

// Value of a number of any kind as a double.
double Num(const Value& value) {
    if (value.IsFloat()) {
        return value.AsFloat();
    }
    return *value.AsInt().ToDouble();
}

bool Contains(const std::string& text, const std::string& part) { return text.find(part) != std::string::npos; }

// Runs fn and returns the error it throws; asserts that it throws.
bc::Error Throws(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const bc::Error& error) {
        return error;
    }
    assert(false && "expected an error");
    return bc::Error(bc::ErrorKind::kRuntime, "");
}

const Value kMachine = bc::ParseJson(R"({
    "model": "fixture", "cpu": "fixture CPU", "physical_cores": 4,
    "logical_cores": 8, "ram_bytes": 17179869184, "architecture": "fixture",
    "os_version": "fixture OS", "filesystem": {"path": "/fixture", "type": "fixture FS"},
    "load_sampling": "start/end observations", "process_niceness": 0})");

const std::vector<std::string> kScenarios = {"world", "canvas", "simulation"};
const std::vector<std::string> kKinds = {"ping", "set_block", "get_block", "set_chunk", "get_chunk", "get_area"};

// The synthetic sample of the earlier test suite: rates are kept as given, so
// integer and float rates both appear, as they may in a report.
Value Sample(const std::string& build = "before", const std::string& mode = "relaxed", std::int64_t requests = 100,
             std::int64_t clients = 2, std::int64_t keyspace = 64, std::int64_t seed = 1337, const Value& rate = I(100)) {
    Value rows = Value::Array();
    for (const std::string& scenario : kScenarios) {
        Value mix = Value::Object();
        for (const std::string& kind : kKinds) {
            mix.Set(kind, I(0));
        }
        mix.Set(scenario == "simulation" ? "get_chunk" : "set_block", I(requests));
        const std::int64_t blocks = requests * (scenario == "simulation" ? 9 : 1);
        const double r = Num(rate);
        rows.Append(Obj({{"test", S(scenario)},
                         {"completed_requests", I(requests)},
                         {"completed_blocks", I(blocks)},
                         {"returned_area_chunks", I(0)},
                         {"request_mix", mix},
                         {"duration_s", F(static_cast<double>(requests) / r)},
                         {"throughput_req_s", rate},
                         {"blocks_per_request", F(static_cast<double>(blocks) / static_cast<double>(requests))},
                         {"throughput_blocks_s", F(static_cast<double>(blocks) * r / static_cast<double>(requests))},
                         {"latency_ms", Obj({{"p50", F(0.25)}, {"p99", F(0.75)}})}}));
    }
    Value report = Obj({{"server_mode", S("spawn")},
                        {"requests", I(requests)},
                        {"requested_clients", I(clients)},
                        {"pipeline", I(1)},
                        {"io_timeout_ms", I(bc::kIoTimeoutMs)},
                        {"keyspace", I(keyspace)},
                        {"seed", I(seed)},
                        {"durability_mode", S(mode)},
                        {"source_commit", S(build)},
                        {"source_state", S("clean")},
                        {"build_type", S("Release")},
                        {"benchmark_source_sha256", S(Hex64('a'))},
                        {"chunk_width_blocks", I(3)},
                        {"chunk_height_blocks", I(3)},
                        {"results", rows}});
    return Obj({{"pair", I(seed - 1336)},
                {"order", I((seed % 2 == 1) == (build == "before") ? 1 : 2)},
                {"report", report},
                {"command", Arr({S("/" + build + "/chunkdb_server_bench")})},
                {"binary_sha256", S(Hex64('b'))},
                {"load_before", Arr({I(1), I(2), I(3)})},
                {"load_after", Arr({I(2), I(3), I(4)})},
                {"stderr", S("")}});
}

void Validate(const Value& report) { bc::ValidateRun(report, S("relaxed"), I(100), I(2), I(64), I(1337)); }

Value& Row(Value& report, std::size_t index) { return At(report, "results").Items()[index]; }

std::filesystem::path TempDir() {
    static int counter = 0;
    const auto path = std::filesystem::temp_directory_path() /
                      ("chunkdb_budget_compare_test_" + std::to_string(bc::NowMicros()) + "_" + std::to_string(++counter));
    std::filesystem::create_directories(path);
    return path;
}

std::string Slurp(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    std::stringstream text;
    text << in.rdbuf();
    return text.str();
}

// Path.read_text(): files are written in text mode, so CRLF on Windows.
std::string ReadText(const std::filesystem::path& path) {
    std::string text = Slurp(path);
    for (std::size_t at = text.find("\r\n"); at != std::string::npos; at = text.find("\r\n", at)) {
        text.erase(at, 1);
    }
    return text;
}

void Spit(const std::filesystem::path& path, const std::string& text) {
    std::ofstream out(path, std::ios::binary);
    out << text;
}

struct Profile {
    std::string name;
    std::string mode;
    std::int64_t requests;
    std::int64_t clients;
    std::int64_t keyspace;
    bool gates;
};
const std::vector<Profile> kProfiles = {{"relaxed", "relaxed", 200000, 4, 2048, true},
                                        {"fsync-wal", "fsync-wal", 5000, 16, 256, false}};

Value Samples(const std::vector<Value>& before, const std::vector<Value>& after) {
    return Obj({{"before", Value::Array(before)}, {"after", Value::Array(after)}});
}

// A noise control as the earlier tests built it: both sides the "before" build.
Value Control(const std::vector<std::string>& wanted, std::int64_t runs, std::int64_t relaxed_clients = 4,
              const std::string& window = "fixture") {
    Value profiles = Value::Array();
    for (const Profile& profile : kProfiles) {
        if (std::find(wanted.begin(), wanted.end(), profile.name) == wanted.end()) {
            continue;
        }
        const std::int64_t clients = profile.name == "relaxed" ? relaxed_clients : profile.clients;
        Value samples = Obj({{"before", Value::Array()}, {"after", Value::Array()}});
        for (const std::string side : {"before", "after"}) {
            for (std::int64_t i = 0; i < runs; ++i) {
                Value sample = Sample("before", profile.mode, profile.requests, clients, profile.keyspace, 1337 + i);
                sample.Set("order", I(((i + 1) % 2 == 1) == (side == "before") ? 1 : 2));
                At(samples, side).Append(sample);
            }
        }
        profiles.Append(bc::SummarizeProfile(S(profile.name), S(profile.mode), I(profile.requests), I(clients),
                                             I(profile.keyspace), Value::Bool(profile.gates), samples, I(5), Value()));
    }
    return Obj({{"schema_version", I(2)},
                {"window_id", S(window)},
                {"measurement_kind", S("noise-floor")},
                {"paired_method", bc::BootstrapMethod()},
                {"started_utc", S("2000-01-01T00:00:00+00:00")},
                {"finished_utc", S("2000-01-01T00:00:01+00:00")},
                {"machine", kMachine},
                {"profiles", profiles}});
}

struct MainResult {
    int status = 0;
    std::vector<std::tuple<std::string, std::int64_t, std::string>> calls;  // build, seed, watch
    std::string out;
    std::string err;
    int machine_calls = 0;
};

std::string Option(const std::vector<std::string>& args, const std::string& name, const std::string& fallback) {
    for (std::size_t i = 0; i + 1 < args.size(); ++i) {
        if (args[i] == name) {
            return args[i + 1];
        }
    }
    return fallback;
}

bc::Hooks FakeHooks(MainResult& result, const Value& after_rate) {
    bc::Hooks hooks;
    hooks.run_once = [&result, after_rate](const bc::RunRequest& request) {
        result.calls.emplace_back(request.build_dir, request.seed, request.watch.value_or("<none>"));
        auto sample = Sample(request.build_dir, request.mode, request.requests, request.clients, request.keyspace,
                             request.seed, request.build_dir == "after" ? after_rate : I(100));
        At(sample, "report").Set("watch", S(request.watch.value_or("")));
        return sample;
    };
    hooks.machine_info = [&result]() {
        ++result.machine_calls;
        return kMachine;
    };
    hooks.load_average = []() { return Arr({I(1), I(2), I(3)}); };
    hooks.file_hash = [](const std::string&) { return Hex64('b'); };
    hooks.now_micros = bc::NowMicros;
    return hooks;
}

// run_main of the earlier suite: a matching control, then the comparison.
MainResult RunMain(const std::vector<std::string>& arguments, const Value& after_rate = I(100)) {
    const std::int64_t runs = std::stoll(Option(arguments, "--runs", "15"));
    std::vector<std::string> wanted;
    const std::string profiles = Option(arguments, "--profiles", "relaxed,fsync-wal");
    for (std::size_t start = 0;;) {
        const std::size_t comma = profiles.find(',', start);
        wanted.push_back(profiles.substr(start, comma == std::string::npos ? std::string::npos : comma - start));
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    const std::int64_t clients = std::stoll(Option(arguments, "--relaxed-concurrency", "4"));
    const auto directory = TempDir();
    const auto noise = directory / "noise.json";
    Value control = Control(wanted, runs, clients);
    const auto watch_both = Option(arguments, "--watch-both", "");
    if (!watch_both.empty()) {
        for (auto& profile : At(control, "profiles").Items()) {
            profile.Set("watch_before", S(watch_both));
            profile.Set("watch_after", S(watch_both));
            for (const auto side : {"before", "after"}) {
                for (auto& sample : At(At(profile, "samples"), side).Items())
                    At(sample, "report").Set("watch", S(watch_both));
            }
        }
    }
    Spit(noise, bc::DumpJson(control));
    MainResult result;
    std::vector<std::string> args = {"before", "after", "--window-id", "fixture", "--noise-reference", noise.string()};
    args.insert(args.end(), arguments.begin(), arguments.end());
    std::ostringstream out;
    std::ostringstream err;
    result.status = bc::Main(args, FakeHooks(result, after_rate), out, err);
    result.out = out.str();
    result.err = err.str();
    std::filesystem::remove_all(directory);
    return result;
}

// ---- Ported checks ----

void TestActualBlocksGeometryAndClippedAreaValidation() {
    Value report = At(Sample(), "report");
    Value& row = Row(report, 0);
    At(row, "request_mix").Set("set_block", I(99));
    At(row, "request_mix").Set("get_area", I(1));
    row.Set("returned_area_chunks", I(1));
    row.Set("completed_blocks", I(108));
    row.Set("blocks_per_request", F(1.08));
    row.Set("throughput_blocks_s", I(108));
    Validate(report);
    row.Set("completed_blocks", I(117));
    assert(Contains(Throws([&] { Validate(report); }).what(), "block count"));
}

void TestMissingMetricsAndInvalidValuesAreRejected() {
    const std::vector<std::function<void(Value&)>> changes = {
        [](Value& r) { r.Set("io_timeout_ms", I(30000)); },
        [](Value& r) { r.Erase("benchmark_source_sha256"); },
        [](Value& r) { r.Set("benchmark_source_sha256", S("wrong")); },
        [](Value& r) { Row(r, 0).Set("throughput_req_s", F(std::nan(""))); },
        [](Value& r) { Row(r, 0).Set("throughput_blocks_s", I(200)); },
        [](Value& r) { At(Row(r, 0), "request_mix").Set("set_block", I(99)); },
    };
    for (const auto& change : changes) {
        Value report = At(Sample(), "report");
        change(report);
        assert(Throws([&] { Validate(report); }).kind() == bc::ErrorKind::kValue);
    }
    Value report = At(Sample(), "report");
    report.Set("io_timeout_ms", I(30000));
    assert(std::string(Throws([&] { Validate(report); }).what()) ==
           "Benchmark io_timeout_ms: expected 5000, got 30000");
}

double Round(double value, int digits) {
    char buffer[64];
    const auto end = std::to_chars(buffer, buffer + sizeof(buffer), value, std::chars_format::fixed, digits).ptr;
    double rounded = 0;
    std::from_chars(buffer, end, rounded);
    return rounded;
}

void TestSerializedDurationAndRateRoundingAreValid() {
    Value report = At(Sample("before", "relaxed", 100, 2, 64, 1337, F(1234.567891)), "report");
    for (Value& row : At(report, "results").Items()) {
        row.Set("duration_s", F(Round(At(row, "duration_s").AsFloat(), 6)));
        row.Set("throughput_req_s", F(Round(At(row, "throughput_req_s").AsFloat(), 3)));
        row.Set("throughput_blocks_s", F(Round(At(row, "throughput_blocks_s").AsFloat(), 6)));
    }
    Validate(report);
}

Value Summarize(const Value& samples, const Value& budget = I(5)) {
    return bc::SummarizeProfile(S("relaxed"), S("relaxed"), I(100), I(2), I(64), Value::Bool(true), samples, budget,
                                Value());
}

void TestMedianRangePercentilesAndLoad() {
    std::vector<Value> before;
    std::vector<Value> after;
    const std::int64_t before_rates[] = {80, 100, 140};
    const std::int64_t after_rates[] = {90, 110, 150};
    for (int i = 0; i < 3; ++i) {
        before.push_back(Sample("before", "relaxed", 100, 2, 64, 1337 + i, I(before_rates[i])));
        after.push_back(Sample("after", "relaxed", 100, 2, 64, 1337 + i, I(after_rates[i])));
    }
    const Value result = Summarize(Samples(before, after));
    const Value& first = At(result, "results").Items()[0];
    assert(bc::DumpJson(At(At(first, "before"), "throughput_req_s")) ==
           bc::DumpJson(Obj({{"median", I(100)}, {"min", I(80)}, {"max", I(140)}, {"runs", I(3)}})));
    assert(At(At(At(first, "before"), "p50_ms"), "median").AsFloat() == 0.25);
    assert(std::fabs(At(first, "change_percent").AsFloat() - 10) < 1e-9);
    assert(bc::DumpJson(At(result, "load_average_range")) ==
           bc::DumpJson(Arr({Obj({{"min", I(1)}, {"max", I(2)}}), Obj({{"min", I(2)}, {"max", I(3)}}),
                             Obj({{"min", I(3)}, {"max", I(4)}})})));
    assert(At(result, "passed").IsBool() && At(result, "passed").AsBool());
    // Paired numbers of the same samples, recorded from Python.
    const Value& paired = At(first, "paired");
    assert(At(paired, "median_ratio").AsFloat() == 1.1);
    assert(bc::DumpJson(At(paired, "ci_ratio")) == "[\n  1.0714285714285714,\n  1.125\n]");
    assert(At(paired, "ratio_sd").AsFloat() == 0.0268055482123755);
    assert(At(paired, "decision").AsString() == "regression criterion not met");
}

void TestGeometryBuildTypeAndBinaryMutationAreRejected() {
    for (const auto& [field, value] : std::vector<std::pair<std::string, Value>>{{"chunk_width_blocks", I(4)},
                                                                                 {"build_type", S("Debug")}}) {
        Value after = Sample("after");
        At(after, "report").Set(field, value);
        const Value samples = Samples({Sample()}, {after});
        assert(Contains(Throws([&] { (void)Summarize(samples); }).what(), "same geometry and build type"));
    }
    Value changed = Sample();
    changed.Set("binary_sha256", S(Hex64('c')));
    const Value samples = Samples({Sample(), changed}, {Sample("after"), Sample("after")});
    assert(Contains(Throws([&] { (void)Summarize(samples); }).what(), "changed between runs"));
}

void TestReportOnlyExitZeroAndRelaxedDropExitOne() {
    MainResult result = RunMain({"--runs", "1", "--profiles", "fsync-wal"}, I(20));
    assert(result.status == 0);
    assert(Contains(result.out, "reported only"));
    result = RunMain({"--runs", "2", "--profiles", "relaxed"}, I(20));
    assert(result.status == 1);
    assert(Contains(result.out, "OVER BUDGET"));
}

void TestDefaultFifteenAlternatingPairsAndWatch() {
    const MainResult result = RunMain({"--profiles", "relaxed", "--watch", "default"});
    assert(result.status == 0);
    assert(result.calls.size() == 30);
    using Call = std::tuple<std::string, std::int64_t, std::string>;
    assert(result.calls[0] == Call("before", 1337, "<none>"));
    assert(result.calls[1] == Call("after", 1337, "default"));
    assert(result.calls[2] == Call("after", 1338, "default"));
    assert(result.calls[3] == Call("before", 1338, "<none>"));
    assert(result.calls[28] == Call("before", 1351, "<none>"));
    assert(result.calls[29] == Call("after", 1351, "default"));
}

void TestJsonAndMarkdownUseOneMeasurementSet() {
    const auto directory = TempDir();
    const auto json = directory / "report.json";
    const auto markdown = directory / "report.md";
    const MainResult result =
        RunMain({"--runs", "3", "--profiles", "relaxed", "--json", json.string(), "--markdown", markdown.string()});
    const Value report = bc::ParseJson(ReadText(json));
    const std::string text = ReadText(markdown);
    assert(result.status == 0);
    assert(result.calls.size() == 6);
    assert(result.out.empty());
    assert(text == bc::RenderMarkdown(report));
    assert(bc::DumpJson(report) + "\n" == ReadText(json));
    const Value& after = At(At(At(report, "profiles").Items()[0], "samples"), "after");
    assert(At(after.Items()[1], "order").AsInt() == bc::BigInt(1));
    assert(Contains(text, "| Verdict | Change req/s |"));
    assert(Contains(text, "Same-window noise reference"));
    assert(Contains(text, "fixture CPU"));
    std::filesystem::remove_all(directory);
}

void TestSubscriberBudgetUsesMatchedWatchesAndControl() {
    using Call = std::tuple<std::string, std::int64_t, std::string>;
    for (const std::string watch : {"default", "default:10", "default:100:slots"}) {
        const auto directory = TempDir();
        const auto json = directory / "budget.json";
        const auto result = RunMain({"--runs", "2", "--profiles", "relaxed", "--watch-both", watch,
                                     "--json", json.string()});
        if (result.status != 0) std::fprintf(stderr, "%s", result.err.c_str());
        assert(result.status == 0 && result.calls.size() == 4U);
        assert(result.calls[0] == Call("before", 1337, watch));
        assert(result.calls[1] == Call("after", 1337, watch));
        assert(result.calls[2] == Call("after", 1338, watch));
        assert(result.calls[3] == Call("before", 1338, watch));
        const auto report = bc::ParseJson(ReadText(json));
        const auto& profile = At(report, "profiles").Items().front();
        assert(At(profile, "watch_before").AsString() == watch);
        assert(At(profile, "watch_after").AsString() == watch);
        const auto& control = At(At(report, "noise_reference"), "report");
        assert(At(At(control, "profiles").Items().front(), "watch_before").AsString() == watch);
        // A watched comparison must refuse an unwatched control before running.
        const auto noise = directory / "noise.json";
        Spit(noise, bc::DumpJson(Control({"relaxed"}, 2, 4)));
        MainResult rejected;
        std::ostringstream out, err;
        assert(bc::Main({"before", "after", "--runs", "2", "--profiles", "relaxed", "--watch-both", watch,
                         "--window-id", "fixture", "--noise-reference", noise.string()},
                        FakeHooks(rejected, I(100)), out, err) == 2);
        assert(rejected.calls.empty());
        Value damaged = control;
        for (auto& sample : At(At(At(damaged, "profiles").Items().front(), "samples"), "before").Items())
            At(sample, "report").Set("watch", S(""));
        Spit(noise, bc::DumpJson(damaged));
        assert(bc::Main({"before", "after", "--runs", "2", "--profiles", "relaxed", "--watch-both", watch,
                         "--window-id", "fixture", "--noise-reference", noise.string()},
                        FakeHooks(rejected, I(100)), out, err) == 2);
        assert(rejected.calls.empty());
        std::filesystem::remove_all(directory);
    }
    for (const auto& extra : std::vector<std::vector<std::string>>{
             {"--watch-both", ""}, {"--watch-both", "default", "--watch", "default"}}) {
        const auto result = RunMain(extra);
        assert(result.status == 2 && result.calls.empty());
    }
    const auto directory = TempDir();
    const auto json = directory / "noise.json";
    MainResult control;
    auto hooks = FakeHooks(control, I(100));
    hooks.run_once = [&control](const bc::RunRequest& request) {
        control.calls.emplace_back(request.build_dir, request.seed, request.watch.value_or("<none>"));
        auto sample = Sample("before", request.mode, request.requests, request.clients, request.keyspace, request.seed);
        At(sample, "report").Set("watch", S(request.watch.value_or("")));
        return sample;
    };
    std::ostringstream out, err;
    assert(bc::Main({"before", "before", "--runs", "2", "--profiles", "relaxed", "--noise-floor",
                     "--watch-both", "default", "--window-id", "fixture", "--json", json.string()},
                    hooks, out, err) == 0);
    assert(control.calls.size() == 4U);
    for (const auto& call : control.calls) assert(std::get<2>(call) == "default");
    const auto report = bc::ParseJson(ReadText(json));
    assert(At(At(report, "profiles").Items().front(), "watch_before").AsString() == "default");
    std::filesystem::remove_all(directory);
}

Value Paired(const std::vector<Value>& before, const std::vector<Value>& after, const Value& budget = I(5)) {
    return bc::PairedStatistics(Samples(before, after), "world", budget);
}

void TestConstantPairedRatioStrictThresholdAndOrder() {
    for (const double ratio : {0.94, 0.95, 1.0}) {
        std::vector<Value> before;
        std::vector<Value> after;
        for (int i = 0; i < 3; ++i) {
            before.push_back(Sample("before", "relaxed", 100, 2, 64, 1337 + i, I(100 * (i + 1))));
            after.push_back(Sample("after", "relaxed", 100, 2, 64, 1337 + i, F(100.0 * (i + 1) * ratio)));
        }
        std::reverse(after.begin(), after.end());
        const Value result = Paired(before, after);
        assert(bc::PyEqual(At(result, "ci_ratio"), Arr({F(ratio), F(ratio)})));
        assert(At(result, "regression").AsBool() == (ratio < 0.95));
        assert(At(result, "ratio_sd").AsFloat() == 0.0);
    }
}

void TestType7IndependentQuantilesAndMedianOfRatios() {
    assert(bc::Type7({0, 10, 20, 30, 40}, 0.025) == 1);
    assert(bc::Type7({0, 10, 20, 30, 40}, 0.975) == 39);
    std::vector<Value> before;
    std::vector<Value> after;
    const std::int64_t before_rates[] = {1, 10, 100};
    const std::int64_t after_rates[] = {2, 5, 80};
    for (int i = 0; i < 3; ++i) {
        before.push_back(Sample("before", "relaxed", 100, 2, 64, 1337 + i, I(before_rates[i])));
        after.push_back(Sample("after", "relaxed", 100, 2, 64, 1337 + i, I(after_rates[i])));
    }
    const Value result = Paired(before, after);
    assert(At(result, "median_ratio").AsFloat() == 0.8);
    // All 27 equally likely bootstrap triples: median min .5, max 2; each tail has >2.5% mass.
    assert(bc::PyEqual(At(result, "ci_ratio"), Arr({F(0.5), I(2)})));
    assert(At(result, "median_ratio").AsFloat() != 0.5);  // not the ratio of separate medians
    assert(At(result, "decision").AsString() == "inconclusive");
    assert(At(result, "ratio_sd").AsFloat() == 0.7937253933193772);
}

void TestInvalidPairAndNonfiniteRatio() {
    const std::vector<std::function<void(Value&)>> changes = {
        [](Value& x) { At(x, "after").Append(At(x, "after").Items()[0]); },
        [](Value& x) { At(x, "after").Items()[0].Set("pair", I(2)); },
        [](Value& x) { Row(At(At(x, "after").Items()[0], "report"), 0).Set("throughput_req_s", Value::Bool(true)); },
        [](Value& x) { Row(At(At(x, "after").Items()[0], "report"), 0).Set("throughput_req_s", F(INFINITY)); },
        [](Value& x) { Row(At(At(x, "before").Items()[0], "report"), 0).Set("throughput_req_s", I(0)); },
    };
    for (const auto& change : changes) {
        Value samples = Samples({Sample()}, {Sample("after")});
        change(samples);
        assert(Throws([&] { (void)bc::PairedStatistics(samples, "world", I(5)); }).kind() == bc::ErrorKind::kValue);
    }
}

void TestOnePairHasNoInferenceOrPassClaim() {
    const MainResult result = RunMain({"--runs", "1", "--profiles", "relaxed"}, I(20));
    assert(result.status == 0);
    assert(Contains(result.out, "insufficient inference"));
    assert(!Contains(result.out, "OVER BUDGET"));
}

void TestDefaultAndOverridesMatchClientsWorkers() {
    for (const auto& [setting, count] :
         std::vector<std::pair<std::string, int>>{{"", 4}, {"4", 4}, {"8", 8}, {"16", 16}}) {
        std::vector<std::string> args = {"--runs", "1", "--profiles", "relaxed"};
        if (!setting.empty()) {
            args.push_back("--relaxed-concurrency");
            args.push_back(setting);
        }
        const MainResult result = RunMain(args);
        assert(result.status == 0);
        const std::string expected = "clients/workers " + std::to_string(count) + "/" + std::to_string(count);
        assert(Contains(result.out, expected));
        // The comparison and its reference both show the matched setting.
        assert(result.out.find(expected) != result.out.rfind(expected));
    }
}

void TestNoiseReferenceReusesDifferentComparisonBaselineAndRejectsDamage() {
    const Value control = Control({"relaxed"}, 2, 2, "window");
    // Control() used the default relaxed profile; rebuild with clients 2 as the earlier test did.
    Value samples = Obj({{"before", Value::Array()}, {"after", Value::Array()}});
    for (const std::string side : {"before", "after"}) {
        for (int i = 0; i < 2; ++i) {
            Value sample = Sample("before", "relaxed", 100, 2, 64, 1337 + i);
            sample.Set("order", I(((i + 1) % 2 == 1) == (side == "before") ? 1 : 2));
            At(samples, side).Append(sample);
        }
    }
    const Value profile = Summarize(samples);
    Value reference = control;
    reference.Set("profiles", Arr({profile}));
    const Value settings = Obj({{"relaxed", bc::ProfileSettings(profile)}});
    const auto directory = TempDir();
    const std::string path = (directory / "noise.json").string();
    Spit(path, bc::DumpJson(reference));
    // No comparison binary is a parameter: the common window noise baseline can differ.
    const Value read = bc::ReadNoiseReference(path, "window", settings, bc::NowMicros());
    assert(At(At(At(At(At(read, "report"), "profiles").Items()[0], "builds"), "before"), "source_commit").AsString() ==
           "before");
    assert(At(read, "sha256").AsString() == bc::BytesSha256(Slurp(path)));
    Value unavailable = reference;
    At(unavailable, "machine").Set("physical_cores", Value());
    At(unavailable, "machine").Set("logical_cores", Value());
    At(unavailable, "machine").Set("ram_bytes", Value());
    At(unavailable, "machine").Set("model", S("unavailable"));
    Spit(path, bc::DumpJson(unavailable));
    (void)bc::ReadNoiseReference(path, "window", settings, bc::NowMicros());
    const std::vector<std::function<void(Value&)>> changes = {
        [](Value& r) { r.Set("window_id", S("other")); },
        [](Value& r) { r.Set("finished_utc", S("2099-01-01T00:00:00+00:00")); },
        [](Value& r) { r.Set("paired_method", Value::Object()); },
        [](Value& r) { At(r, "profiles").Items()[0].Set("clients", I(4)); },
        [](Value& r) { At(At(At(r, "profiles").Items()[0], "samples"), "after").Items()[0].Set("binary_sha256", S(Hex64('c'))); },
        [](Value& r) { At(At(At(r, "profiles").Items()[0], "samples"), "after").Items()[0].Set("pair", I(2)); },
        [](Value& r) {
            At(At(At(At(At(r, "profiles").Items()[0], "results").Items()[0], "before"), "throughput_req_s"), "median") =
                I(999);
        },
        [](Value& r) { At(At(At(r, "profiles").Items()[0], "samples"), "after").Items().pop_back(); },
    };
    for (const auto& change : changes) {
        Value damaged = reference;
        change(damaged);
        Spit(path, bc::DumpJson(damaged));
        assert(Throws([&] { (void)bc::ReadNoiseReference(path, "window", settings, bc::NowMicros()); }).kind() ==
               bc::ErrorKind::kValue);
    }
    std::filesystem::remove_all(directory);
}

void TestDerivedInferenceOverflowIsRejectedBeforeVerdict() {
    for (const auto& rates : std::vector<std::vector<double>>{{1e308, 1e308}, {1e308, 1e307}, {1, 1, 1e308}}) {
        std::vector<Value> before;
        std::vector<Value> after;
        for (std::size_t i = 0; i < rates.size(); ++i) {
            before.push_back(Sample("before", "relaxed", 100, 2, 64, 1337 + static_cast<std::int64_t>(i), I(1)));
            after.push_back(Sample("after", "relaxed", 100, 2, 64, 1337 + static_cast<std::int64_t>(i), F(rates[i])));
        }
        const std::string message = Throws([&] { (void)Paired(before, after); }).what();
        assert(Contains(message, "derived") || Contains(message, "Nonfinite"));
    }
    assert(Contains(Throws([] { (void)bc::Summary({F(1e308), F(1e308)}); }).what(), "Nonfinite derived summary"));
}

void TestDamagedRenderMetadataIsRejectedBeforeMeasurement() {
    const std::vector<std::function<void(Value&)>> damages = {
        [](Value& r) { r.Erase("machine"); },
        [](Value& r) { At(r, "machine").Erase("filesystem"); },
        [](Value& r) { At(r, "machine").Set("ram_bytes", S("wrong")); },
        [](Value& r) { At(r, "machine").Set("ram_bytes", Value::Int(*bc::BigInt::Parse("1" + std::string(400, '0')))); },
        [](Value& r) { At(r, "profiles").Items()[0].Set("budget_percent", F(std::nan(""))); },
        [](Value& r) { r.Set("noise_reference", Obj({{"report", Value::Object()}})); },
    };
    for (const auto& damage : damages) {
        const auto directory = TempDir();
        const auto path = directory / "noise.json";
        (void)RunMain({"--runs", "2", "--profiles", "relaxed", "--json", path.string()});
        // Use the valid generated control, not the source comparison that wraps it.
        Value control = At(At(bc::ParseJson(ReadText(path)), "noise_reference"), "report");
        damage(control);
        // NaN is written as Python writes it, with allow_nan.
        std::string text;
        try {
            text = bc::DumpJson(control);
        } catch (const bc::Error&) {
            Value copy = control;
            At(copy, "profiles").Items()[0].Set("budget_percent", S("@NAN@"));
            text = bc::DumpJson(copy);
            text.replace(text.find("\"@NAN@\""), 7, "NaN");
        }
        Spit(path, text);
        MainResult result;
        std::ostringstream out;
        std::ostringstream err;
        const int status = bc::Main({"before", "after", "--runs", "2", "--profiles", "relaxed", "--window-id",
                                     "fixture", "--noise-reference", path.string()},
                                    FakeHooks(result, I(100)), out, err);
        assert(status == 2);
        assert(result.calls.empty() && result.machine_calls == 0);
        std::filesystem::remove_all(directory);
    }
}

void TestNoiseFloorPreflightAndMissingReferenceDoNotMeasure() {
    for (const auto& extra : std::vector<std::vector<std::string>>{
             {"--window-id", "fixture"},
             {"--window-id", "fixture", "--noise-reference", "missing.json"},
             {"--window-id", "fixture", "--noise-floor", "--watch", "default"}}) {
        std::vector<std::string> args = {"before", "after"};
        args.insert(args.end(), extra.begin(), extra.end());
        MainResult result;
        std::ostringstream out;
        std::ostringstream err;
        assert(bc::Main(args, FakeHooks(result, I(100)), out, err) == 2);
        assert(result.calls.empty() && result.machine_calls == 0);
    }
    MainResult result;
    bc::Hooks hooks = FakeHooks(result, I(100));
    auto count = std::make_shared<int>(0);
    hooks.file_hash = [count](const std::string&) { return Hex64((*count)++ == 0 ? 'a' : 'b'); };
    std::ostringstream out;
    std::ostringstream err;
    assert(bc::Main({"before", "after", "--window-id", "fixture", "--noise-floor"}, hooks, out, err) == 2);
    assert(Contains(err.str(), "Noise control requires identical binaries"));
    assert(result.calls.empty());
}

struct FakeRun {
    std::string stdout_text;
    std::string stderr_text;
    bc::ExitStatus status;
    std::vector<std::string> command;
};

bc::RunEnvironment FakeEnvironment(std::shared_ptr<FakeRun> run, std::vector<std::string> hashes,
                                   std::vector<Value> loads) {
    bc::RunEnvironment environment;
    auto hash_index = std::make_shared<std::size_t>(0);
    environment.file_hash = [hashes, hash_index](const std::string&) {
        return hashes[std::min((*hash_index)++, hashes.size() - 1)];
    };
    auto load_index = std::make_shared<std::size_t>(0);
    environment.load_average = [loads, load_index]() {
        return loads.empty() ? Value() : loads[std::min((*load_index)++, loads.size() - 1)];
    };
    environment.free_port = [] { return 12345; };
    environment.now_micros = [] { return std::int64_t{1792900000000000}; };
    environment.spawn = [run](const std::vector<std::string>& command, const std::string& out_path,
                              const std::string& err_path) {
        run->command = command;
        Spit(out_path, run->stdout_text);
        Spit(err_path, run->stderr_text);
        return run->status;
    };
    return environment;
}

void TestRunOncePreservesRawOutputAndDetectsBinaryChanges() {
    const Value report = At(Sample(), "report");
    const auto directory = TempDir();
    auto run = std::make_shared<FakeRun>();
    run->stdout_text = "starting benchmark\n" + bc::DumpJson(report);
    run->stderr_text = "fixture diagnostic\r\n";
    bc::RunRequest request;
    request.build_dir = "before";
    request.mode = "relaxed";
    request.requests = 100;
    request.clients = 2;
    request.keyspace = 64;
    request.seed = 1337;
    request.watch = "world";
    request.record_prefix = (directory / "records" / "relaxed-pair01-before").string();
    const Value value = bc::RunOnce(request, FakeEnvironment(run, {Hex64('b')}, {Arr({I(1), I(2), I(3)}),
                                                                                 Arr({I(2), I(3), I(4)})}));
    assert(bc::PyEqual(At(value, "report"), report));
    assert(At(value, "stderr").AsString() == "fixture diagnostic\n");  // universal newlines
    assert(bc::PyEqual(At(value, "load_after"), Arr({I(2), I(3), I(4)})));
    assert(run->command.size() >= 2 && run->command[run->command.size() - 2] == "--watch" &&
           run->command.back() == "world");
    assert(Contains(run->command[0], "chunkdb_server_bench") && run->command[4] == "12345");
    // Raw output, exit status and UTC times are on disk.
    assert(Slurp(request.record_prefix + ".stdout") == run->stdout_text);
    assert(Slurp(request.record_prefix + ".stderr") == run->stderr_text);
    const Value record = bc::ParseJson(ReadText(request.record_prefix + ".status.json"));
    assert(At(record, "exit_status").AsInt() == bc::BigInt(0) && At(record, "signal").IsNull());
    assert(At(record, "started_utc").AsString() == "2026-10-25T03:46:40+00:00");
    assert(At(record, "command").Items().size() == run->command.size());

    assert(Contains(Throws([&] {
                        (void)bc::RunOnce(request, FakeEnvironment(run, {Hex64('b'), Hex64('c')}, {}));
                    }).what(),
                    "changed during measurement"));

    // A failed process keeps its output and status before anything is parsed.
    run->stdout_text = "partial";
    run->stderr_text = "server benchmark failed: boom\n";
    run->status = bc::ExitStatus{true, 3};
    const bc::Error failed = Throws([&] { (void)bc::RunOnce(request, FakeEnvironment(run, {Hex64('b')}, {})); });
    assert(Contains(failed.what(), "returned non-zero exit status 3"));
    assert(Slurp(request.record_prefix + ".stderr") == run->stderr_text);
    assert(At(bc::ParseJson(ReadText(request.record_prefix + ".status.json")), "exit_status").AsInt() == bc::BigInt(3));
    std::filesystem::remove_all(directory);
}

void TestInvalidArgumentsFailBeforeMachineOrMeasurement() {
    for (const auto& options : std::vector<std::vector<std::string>>{{"--runs", "0"},
                                                                    {"--profiles", ""},
                                                                    {"--profiles", "unknown"},
                                                                    {"--profiles", "relaxed,relaxed"},
                                                                    {"--budget", "nan"},
                                                                    {"--json", "--markdown"},
                                                                    {"--json", "same", "--markdown", "same"}}) {
        std::vector<std::string> args = {"before", "after"};
        args.insert(args.end(), options.begin(), options.end());
        MainResult result;
        std::ostringstream out;
        std::ostringstream err;
        assert(bc::Main(args, FakeHooks(result, I(100)), out, err) == 2);
        assert(result.calls.empty() && result.machine_calls == 0);
    }
}

// ---- Computed numbers against the Python comparator ----

void TestPythonRandomStream() {
    // random.Random(20261011): getrandbits(32) and randrange(n), recorded.
    bc::PythonRandom bits(20261011);
    for (const std::uint32_t expected : {2112486402U, 1488691330U, 2129246409U, 1451729388U, 1198009315U}) {
        assert(bits.NextUint32() == expected);
    }
    bc::PythonRandom fifteen(20261011);
    for (const std::uint32_t expected : {7U, 5U, 7U, 5U, 4U, 0U, 0U, 5U, 7U, 0U, 11U, 0U, 11U, 13U, 12U, 10U, 5U, 3U, 10U, 9U}) {
        assert(fifteen.RandBelow(15) == expected);
    }
    bc::PythonRandom three(20261011);
    for (const std::uint32_t expected : {1U, 1U, 1U, 1U, 1U, 0U, 0U, 1U, 1U, 0U, 2U, 0U, 2U, 2U, 1U, 0U, 2U, 2U, 2U, 0U}) {
        assert(three.RandBelow(3) == expected);
    }
}

void TestStatisticsMatchPython() {
    assert(bc::SampleStdev({1.5, 2.5, 2.5, 2.75, 3.25, 4.75}) == 1.0810874155219827);
    assert(bc::SampleStdev({0.97, 1.01, 0.99, 1.04, 0.95}) == 0.03492849839314599);
    assert(bc::SampleStdev({1.0, 1.0000000000000002}) == 1.5700924586837752e-16);
    assert(bc::MedianOf({3, 1, 2}) == 2 && bc::MedianOf({4, 1, 2, 3}) == 2.5);
    // Seven realistic pairs: median ratio, 95% interval and SD as Python printed them.
    const double before[] = {41234.567, 40987.123, 41500.001, 40011.5, 41876.25, 40555.0, 41111.111};
    const double after[] = {39876.543, 40100.25, 41987.0, 38999.999, 40500.5, 40001.0, 40777.777};
    std::vector<Value> b;
    std::vector<Value> a;
    for (int i = 0; i < 7; ++i) {
        b.push_back(Sample("before", "relaxed", 100, 2, 64, 1337 + i, F(before[i])));
        a.push_back(Sample("after", "relaxed", 100, 2, 64, 1337 + i, F(after[i])));
    }
    const Value result = Paired(b, a, F(5.0));
    assert(At(result, "median_ratio").AsFloat() == 0.9783621553530362);
    assert(At(result, "ci_ratio").Items()[0].AsFloat() == 0.9671472493358407);
    assert(At(result, "ci_ratio").Items()[1].AsFloat() == 0.9918918756537619);
    assert(At(result, "ratio_sd").AsFloat() == 0.015859773880869153);
    assert(At(result, "change_percent").AsFloat() == -2.1637844646963833);
    assert(At(result, "decision").AsString() == "regression criterion not met");
}

void TestPythonNumberSemantics() {
    // Summaries keep Python's int/float results: an even median of ints is a float.
    assert(bc::DumpJson(At(bc::Summary({I(1), I(2), I(3)}), "median")) == "2");
    assert(bc::DumpJson(At(bc::Summary({I(1), I(3)}), "median")) == "2.0");
    assert(bc::DumpJson(At(bc::Summary({I(100), F(100.0)}), "min")) == "100");
    assert(bc::FloatRepr(1e16) == "1e+16" && bc::FloatRepr(1e15) == "1000000000000000.0");
    assert(bc::FloatRepr(0.0001) == "0.0001" && bc::FloatRepr(1e-05) == "1e-05" && bc::FloatRepr(-0.0) == "-0.0");
    assert(bc::FloatRepr(5e-324) == "5e-324" && bc::FloatRepr(1.7976931348623157e308) == "1.7976931348623157e+308");
    assert(bc::FormatNumber(F(1234567.891), ",.1f") == "1,234,567.9");
    assert(bc::FormatNumber(F(-0.001), "+.2f") == "-0.00" && bc::FormatNumber(F(0.0), "+.2f") == "+0.00");
    assert(bc::FormatNumber(F(2.675), ".2f") == "2.67" && bc::FormatNumber(F(0.125), ".2f") == "0.12");
    assert(bc::FormatNumber(I(200000), ",") == "200,000" && bc::FormatNumber(F(200000.0), ",") == "200,000.0");
    assert(bc::FormatNumber(I(80), ",.1f") == "80.0");
    const bc::BigInt big = *bc::BigInt::Parse("9007199254740993");
    assert(*bc::TrueDivide(big, bc::BigInt(1)) == 9007199254740992.0);
    assert(!bc::TrueDivide(*bc::BigInt::Parse("1" + std::string(400, '0')), bc::BigInt(1073741824)));
    assert(*bc::TrueDivide(bc::BigInt(1), bc::BigInt(3)) == 1.0 / 3.0);
    assert(bc::PyEqual(I(1), F(1.0)) && bc::PyEqual(Value::Bool(true), I(1)) && !bc::PyEqual(F(NAN), F(NAN)));
    assert(bc::PyRepr(bc::ParseJson(R"(["it's", "a\"b", 1.5, null, true, {"k": []}])")) ==
           R"(["it's", 'a"b', 1.5, None, True, {'k': []}])");
    assert(bc::DumpJson(bc::ParseJson("[1, 1.0, -0, -0.0, 1E2, \"\\u00e9\\ud83d\\ude00\"]")) ==
           "[\n  1,\n  1.0,\n  0,\n  -0.0,\n  100.0,\n  \"\\u00e9\\ud83d\\ude00\"\n]");
    assert(std::string(Throws([] { (void)bc::ParseJson("[1,]"); }).what()) ==
           "Illegal trailing comma before end of array: line 1 column 3 (char 2)");
    assert(bc::IsoUtc(946684800000000) == "2000-01-01T00:00:00+00:00");
    assert(bc::IsoUtc(946684800000001) == "2000-01-01T00:00:00.000001+00:00");
    assert(*bc::ParseIsoTime("2000-01-01T00:00:00Z") == 946684800000000);
    assert(!bc::ParseIsoTime("2000-01-01T00:00:00"));
}

}  // namespace

int main() {
    TestActualBlocksGeometryAndClippedAreaValidation();
    TestMissingMetricsAndInvalidValuesAreRejected();
    TestSerializedDurationAndRateRoundingAreValid();
    TestMedianRangePercentilesAndLoad();
    TestGeometryBuildTypeAndBinaryMutationAreRejected();
    TestReportOnlyExitZeroAndRelaxedDropExitOne();
    TestDefaultFifteenAlternatingPairsAndWatch();
    TestSubscriberBudgetUsesMatchedWatchesAndControl();
    TestJsonAndMarkdownUseOneMeasurementSet();
    TestConstantPairedRatioStrictThresholdAndOrder();
    TestType7IndependentQuantilesAndMedianOfRatios();
    TestInvalidPairAndNonfiniteRatio();
    TestOnePairHasNoInferenceOrPassClaim();
    TestDefaultAndOverridesMatchClientsWorkers();
    TestNoiseReferenceReusesDifferentComparisonBaselineAndRejectsDamage();
    TestDerivedInferenceOverflowIsRejectedBeforeVerdict();
    TestDamagedRenderMetadataIsRejectedBeforeMeasurement();
    TestNoiseFloorPreflightAndMissingReferenceDoNotMeasure();
    TestRunOncePreservesRawOutputAndDetectsBinaryChanges();
    TestInvalidArgumentsFailBeforeMachineOrMeasurement();
    TestPythonRandomStream();
    TestStatisticsMatchPython();
    TestPythonNumberSemantics();
    return 0;
}
