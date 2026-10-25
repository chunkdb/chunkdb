#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "budget_compare.hpp"

namespace chunkdb::budget_compare {

namespace {

constexpr std::string_view kScenarios = "world,canvas,simulation";
constexpr std::array<std::string_view, 3> kScenarioNames = {"world", "canvas", "simulation"};
constexpr std::array<std::string_view, 6> kKinds = {"ping", "set_block", "get_block", "set_chunk", "get_chunk", "get_area"};
constexpr std::array<std::string_view, 6> kBuildFields = {"source_commit", "source_state", "build_type",
                                                         "benchmark_source_sha256", "chunk_width_blocks",
                                                         "chunk_height_blocks"};
constexpr int kResamples = 100000;
constexpr std::uint64_t kBootstrapSeed = 20261011;

struct Profile {
    std::string_view name;
    std::string_view mode;
    std::int64_t requests;
    std::int64_t clients;
    std::int64_t keyspace;
    bool gates;
};

constexpr std::array<Profile, 2> kProfiles = {{
    {"relaxed", "relaxed", 200000, 4, 2048, true},
    {"fsync-wal", "fsync-wal", 5000, 16, 256, false},
}};

std::string_view Description(std::string_view scenario) {
    if (scenario == "world") {
        return "Per client: move and read AREA AROUND radius 2 each 50 requests; one SET CHUNK; otherwise SET/GET BLOCK.";
    }
    if (scenario == "canvas") {
        return "Per client: one GET AREA viewport (up to 4x4 chunks) each 20 requests; otherwise random SET BLOCK.";
    }
    return "Per client: alternate GET CHUNK and SET CHUNK while sweeping the region, starting with GET.";
}

[[noreturn]] void Fail(ErrorKind kind, const std::string& message) { throw Error(kind, message); }

// ---- Python operations on values ----

std::string TypeName(const Value& value) {
    switch (value.kind()) {
        case Value::Kind::kNull: return "NoneType";
        case Value::Kind::kBool: return "bool";
        case Value::Kind::kInt: return "int";
        case Value::Kind::kFloat: return "float";
        case Value::Kind::kString: return "str";
        case Value::Kind::kArray: return "list";
        case Value::Kind::kObject: return "dict";
    }
    return "object";
}

Value S(std::string_view text) { return Value::Str(std::string(text)); }
Value I(std::int64_t value) { return Value::Int(BigInt(value)); }

std::size_t CodePointCount(const std::string& text) {
    std::size_t count = 0;
    for (const char c : text) {
        if ((static_cast<unsigned char>(c) & 0xC0) != 0x80) {
            ++count;
        }
    }
    return count;
}

// container[key] for a string key.
const Value& GetItem(const Value& container, std::string_view key) {
    if (container.IsObject()) {
        if (const Value* found = container.Find(key)) {
            return *found;
        }
        Fail(ErrorKind::kKey, PyRepr(S(key)));
    }
    if (container.IsArray()) {
        Fail(ErrorKind::kType, "list indices must be integers or slices, not str");
    }
    if (container.IsString()) {
        Fail(ErrorKind::kType, "string indices must be integers, not 'str'");
    }
    Fail(ErrorKind::kType, "'" + TypeName(container) + "' object is not subscriptable");
}

std::vector<Value> Iterate(const Value& value);

// container[index] for a small integer index.
Value GetIndex(const Value& container, std::size_t index) {
    if (container.IsArray()) {
        if (index >= container.Items().size()) {
            Fail(ErrorKind::kIndex, "list index out of range");
        }
        return container.Items()[index];
    }
    if (container.IsString()) {
        const std::vector<Value> chars = Iterate(container);
        if (index >= chars.size()) {
            Fail(ErrorKind::kIndex, "string index out of range");
        }
        return chars[index];
    }
    if (container.IsObject()) {
        Fail(ErrorKind::kKey, std::to_string(index));
    }
    Fail(ErrorKind::kType, "'" + TypeName(container) + "' object is not subscriptable");
}

// value.get(key): nullptr when absent.
const Value* Get(const Value& value, std::string_view key) {
    if (!value.IsObject()) {
        Fail(ErrorKind::kAttribute, "'" + TypeName(value) + "' object has no attribute 'get'");
    }
    return value.Find(key);
}

Value GetOr(const Value& value, std::string_view key, const Value& fallback = Value()) {
    const Value* found = Get(value, key);
    return found != nullptr ? *found : fallback;
}

std::size_t Len(const Value& value) {
    switch (value.kind()) {
        case Value::Kind::kString: return CodePointCount(value.AsString());
        case Value::Kind::kArray: return value.Items().size();
        case Value::Kind::kObject: return value.Members().size();
        default: Fail(ErrorKind::kType, "object of type '" + TypeName(value) + "' has no len()");
    }
}

std::vector<Value> Iterate(const Value& value) {
    if (value.IsArray()) {
        return value.Items();
    }
    std::vector<Value> out;
    if (value.IsObject()) {
        for (const Member& member : value.Members()) {
            out.push_back(Value::Str(member.key));
        }
        return out;
    }
    if (value.IsString()) {
        const std::string& text = value.AsString();
        for (std::size_t i = 0; i < text.size();) {
            std::size_t end = i + 1;
            while (end < text.size() && (static_cast<unsigned char>(text[end]) & 0xC0) == 0x80) {
                ++end;
            }
            out.push_back(Value::Str(text.substr(i, end - i)));
            i = end;
        }
        return out;
    }
    Fail(ErrorKind::kType, "'" + TypeName(value) + "' object is not iterable");
}

void RequireHashable(const Value& value) {
    if (value.IsArray() || value.IsObject()) {
        Fail(ErrorKind::kType, "cannot use '" + TypeName(value) + "' as a set element (unhashable type: '" +
                                   TypeName(value) + "')");
    }
}

bool Truthy(const Value& value) {
    switch (value.kind()) {
        case Value::Kind::kNull: return false;
        case Value::Kind::kBool: return value.AsBool();
        case Value::Kind::kInt: return !value.AsInt().IsZero();
        case Value::Kind::kFloat: return value.AsFloat() != 0.0;
        case Value::Kind::kString: return !value.AsString().empty();
        case Value::Kind::kArray: return !value.Items().empty();
        case Value::Kind::kObject: return !value.Members().empty();
    }
    return false;
}

bool IsNumber(const Value& value) { return value.IsBool() || value.IsInt() || value.IsFloat(); }

BigInt IntegerOf(const Value& value) { return value.IsBool() ? BigInt(value.AsBool() ? 1 : 0) : value.AsInt(); }

double ToFloat(const Value& value) {
    if (value.IsFloat()) {
        return value.AsFloat();
    }
    const std::optional<double> converted = IntegerOf(value).ToDouble();
    if (!converted) {
        Fail(ErrorKind::kOverflow, "int too large to convert to float");
    }
    return *converted;
}

enum class Op { kAdd, kSub, kMul, kDiv };

Value Arith(const Value& a, const Value& b, Op op) {
    if (!IsNumber(a) || !IsNumber(b)) {
        static constexpr std::array<std::string_view, 4> kSymbols = {"+", "-", "*", "/"};
        Fail(ErrorKind::kType, "unsupported operand type(s) for " + std::string(kSymbols[static_cast<int>(op)]) +
                                   ": '" + TypeName(a) + "' and '" + TypeName(b) + "'");
    }
    if (!a.IsFloat() && !b.IsFloat()) {
        const BigInt x = IntegerOf(a);
        const BigInt y = IntegerOf(b);
        switch (op) {
            case Op::kAdd: return Value::Int(x + y);
            case Op::kSub: return Value::Int(x - y);
            case Op::kMul: return Value::Int(x * y);
            case Op::kDiv: {
                if (y.IsZero()) {
                    Fail(ErrorKind::kZeroDivision, "division by zero");
                }
                const std::optional<double> quotient = TrueDivide(x, y);
                if (!quotient) {
                    Fail(ErrorKind::kOverflow, "integer division result too large for a float");
                }
                return Value::Float(*quotient);
            }
        }
    }
    const double x = ToFloat(a);
    const double y = ToFloat(b);
    switch (op) {
        case Op::kAdd: return Value::Float(x + y);
        case Op::kSub: return Value::Float(x - y);
        case Op::kMul: return Value::Float(x * y);
        case Op::kDiv:
            if (y == 0.0) {
                Fail(ErrorKind::kZeroDivision, "float division by zero");
            }
            return Value::Float(x / y);
    }
    return Value();
}

Value Add(const Value& a, const Value& b) { return Arith(a, b, Op::kAdd); }
Value Sub(const Value& a, const Value& b) { return Arith(a, b, Op::kSub); }
Value Mul(const Value& a, const Value& b) { return Arith(a, b, Op::kMul); }
Value Div(const Value& a, const Value& b) { return Arith(a, b, Op::kDiv); }

// Three-way numeric comparison; std::nullopt when unordered (NaN).
std::optional<int> NumericOrder(const Value& a, const Value& b, std::string_view symbol) {
    if (!IsNumber(a) || !IsNumber(b)) {
        Fail(ErrorKind::kType, "'" + std::string(symbol) + "' not supported between instances of '" + TypeName(a) +
                                   "' and '" + TypeName(b) + "'");
    }
    if (a.IsFloat() && b.IsFloat()) {
        const double x = a.AsFloat();
        const double y = b.AsFloat();
        if (std::isnan(x) || std::isnan(y)) {
            return std::nullopt;
        }
        return x < y ? -1 : (x > y ? 1 : 0);
    }
    if (!a.IsFloat() && !b.IsFloat()) {
        return Compare(IntegerOf(a), IntegerOf(b));
    }
    const double f = a.IsFloat() ? a.AsFloat() : b.AsFloat();
    if (std::isnan(f)) {
        return std::nullopt;
    }
    int order = 0;
    if (std::isinf(f)) {
        order = f > 0 ? -1 : 1;  // integer compared with the infinity
    } else {
        order = CompareExact(IntegerOf(a.IsFloat() ? b : a), f);
    }
    return a.IsFloat() ? -order : order;
}

bool Less(const Value& a, const Value& b) {
    const auto order = NumericOrder(a, b, "<");
    return order && *order < 0;
}
bool LessEqual(const Value& a, const Value& b) {
    const auto order = NumericOrder(a, b, "<=");
    return order && *order <= 0;
}
bool Greater(const Value& a, const Value& b) {
    const auto order = NumericOrder(a, b, ">");
    return order && *order > 0;
}

Value Abs(const Value& value) {
    if (value.IsFloat()) {
        return Value::Float(std::fabs(value.AsFloat()));
    }
    const BigInt integer = IntegerOf(value);
    return Value::Int(integer.IsNegative() ? BigInt(0) - integer : integer);
}

bool IsFinite(const Value& value) { return value.IsFloat() ? std::isfinite(value.AsFloat()) : std::isfinite(ToFloat(value)); }

void PositiveNumber(const Value& value, const std::string& name, bool allow_zero = false) {
    if (value.IsBool() || !IsNumber(value) || !IsFinite(value) || Less(value, I(0)) ||
        (PyEqual(value, I(0)) && !allow_zero)) {
        Fail(ErrorKind::kValue, "Invalid " + name + ": " + PyRepr(value));
    }
}

const Value& FiniteNumber(const Value& value, const std::string& name) {
    if (value.IsBool() || !IsNumber(value) || !IsFinite(value)) {
        Fail(ErrorKind::kValue, "Nonfinite derived " + name);
    }
    return value;
}

bool IsHexDigest(const Value& value) {
    if (!value.IsString() || Len(value) != 64) {
        return false;
    }
    return value.AsString().find_first_not_of("0123456789abcdef") == std::string::npos;
}

// min()/max(): the first extreme element.
Value Extreme(const std::vector<Value>& values, bool maximum) {
    if (values.empty()) {
        Fail(ErrorKind::kValue, std::string(maximum ? "max" : "min") + "() iterable argument is empty");
    }
    std::size_t best = 0;
    for (std::size_t i = 1; i < values.size(); ++i) {
        if (maximum ? Greater(values[i], values[best]) : Less(values[i], values[best])) {
            best = i;
        }
    }
    return values[best];
}

// statistics.median of numeric values.
Value Median(const std::vector<Value>& values) {
    if (values.empty()) {
        Fail(ErrorKind::kValue, "no median for empty data");
    }
    std::vector<Value> sorted = values;
    std::stable_sort(sorted.begin(), sorted.end(), [](const Value& a, const Value& b) { return Less(a, b); });
    const std::size_t n = sorted.size();
    if (n % 2 == 1) {
        return sorted[n / 2];
    }
    return Div(Add(sorted[n / 2 - 1], sorted[n / 2]), I(2));
}

// positive_number() of a float, without building a value: the bootstrap
// checks each of its draws.
void PositiveFloat(double value, const char* name) {
    if (!std::isfinite(value) || value <= 0) {
        Fail(ErrorKind::kValue, std::string("Invalid ") + name + ": " + FloatRepr(value));
    }
}

// inference_median(): statistics.median of the values in place, then checked.
double InferenceMedian(std::vector<double>& values) {
    std::sort(values.begin(), values.end());
    const std::size_t n = values.size();
    const double value = n % 2 == 1 ? values[n / 2] : (values[n / 2 - 1] + values[n / 2]) / 2;
    PositiveFloat(value, "derived paired median");
    return value;
}

std::string Capitalize(const std::string& text) {
    std::string out = text;
    for (std::size_t i = 0; i < out.size(); ++i) {
        const char c = out[i];
        if (i == 0 && c >= 'a' && c <= 'z') {
            out[i] = static_cast<char>(c - 'a' + 'A');
        } else if (i != 0 && c >= 'A' && c <= 'Z') {
            out[i] = static_cast<char>(c - 'A' + 'a');
        }
    }
    return out;
}

std::string ReplaceAll(std::string text, std::string_view from, std::string_view to) {
    for (std::size_t at = text.find(from); at != std::string::npos; at = text.find(from, at + to.size())) {
        text.replace(at, from.size(), to);
    }
    return text;
}

std::string OrUnavailable(const Value& value, std::string_view fallback = "unavailable") {
    return Truthy(value) ? PyStr(value) : std::string(fallback);
}

}  // namespace

// ---- The comparator ----

Value BootstrapMethod() {
    Value method = Value::Object();
    method.Set("resamples", I(kResamples));
    method.Set("seed", I(static_cast<std::int64_t>(kBootstrapSeed)));
    method.Set("statistic", S("median of paired after/before req/s ratios"));
    method.Set("interval", S("95% percentile"));
    method.Set("quantile", S("Type7"));
    // The generator identity of the method; kept so that references stay comparable.
    method.Set("rng", S("Python random.Random; reset per scenario/profile"));
    method.Set("ratio_sd", S("sample n-1"));
    method.Set("pairing", S("pair,seed"));
    return method;
}

double MedianOf(std::vector<double> values) {
    if (values.empty()) {
        Fail(ErrorKind::kValue, "no median for empty data");
    }
    std::sort(values.begin(), values.end());
    const std::size_t n = values.size();
    if (n % 2 == 1) {
        return values[n / 2];
    }
    return (values[n / 2 - 1] + values[n / 2]) / 2;
}

double Type7(const std::vector<double>& sorted, double probability) {
    const double index = static_cast<double>(sorted.size() - 1) * probability;
    const double low = std::floor(index);
    const double high = std::ceil(index);
    const double low_value = sorted[static_cast<std::size_t>(low)];
    const double high_value = sorted[static_cast<std::size_t>(high)];
    const double step = (high_value - low_value) * (index - low);
    return low_value + step;
}

void ValidateRun(const Value& report, const Value& mode, const Value& requests, const Value& clients,
                 const Value& keyspace, const Value& seed) {
    const std::array<std::pair<std::string_view, Value>, 8> expected = {{
        {"server_mode", S("spawn")},
        {"requests", requests},
        {"requested_clients", clients},
        {"pipeline", I(1)},
        {"io_timeout_ms", I(kIoTimeoutMs)},
        {"keyspace", keyspace},
        {"seed", seed},
        {"durability_mode", mode},
    }};
    for (const auto& [field, value] : expected) {
        const Value got = GetOr(report, field);
        if (!PyEqual(got, value)) {
            Fail(ErrorKind::kValue, "Benchmark " + std::string(field) + ": expected " + PyRepr(value) + ", got " +
                                        PyRepr(GetOr(report, field)));
        }
    }
    for (const std::string_view field : kBuildFields) {
        if (report.Find(field) == nullptr) {
            Fail(ErrorKind::kValue, "Benchmark lacks " + std::string(field) +
                                        "; rebuild both benchmarks with reporting support");
        }
    }
    if (!IsHexDigest(GetItem(report, "benchmark_source_sha256"))) {
        Fail(ErrorKind::kValue, "Invalid benchmark source SHA256");
    }
    if (!Truthy(GetItem(report, "source_commit")) || !Truthy(GetItem(report, "build_type"))) {
        Fail(ErrorKind::kValue, "Empty benchmark source/build provenance");
    }
    const Value& width = GetItem(report, "chunk_width_blocks");
    const Value& height = GetItem(report, "chunk_height_blocks");
    if (!width.IsInt() || !height.IsInt() || !Greater(width, I(0)) || !Greater(height, I(0))) {
        Fail(ErrorKind::kValue, "Invalid benchmark chunk geometry");
    }
    const Value rows = GetOr(report, "results", Value::Array());
    bool complete = Len(rows) == kScenarioNames.size();
    if (complete) {
        std::vector<std::string> names;
        bool all_strings = true;
        for (const Value& row : Iterate(rows)) {
            const Value& test = GetItem(row, "test");
            RequireHashable(test);
            if (test.IsString()) {
                names.push_back(test.AsString());
            } else {
                all_strings = false;
            }
        }
        complete = all_strings;
        for (const std::string_view scenario : kScenarioNames) {
            complete = complete && std::find(names.begin(), names.end(), scenario) != names.end();
        }
        for (const std::string& name : names) {
            complete = complete && std::find(kScenarioNames.begin(), kScenarioNames.end(), name) != kScenarioNames.end();
        }
    }
    if (!complete) {
        Fail(ErrorKind::kValue, "Benchmark scenario set is incomplete or duplicated");
    }
    for (const Value& row : rows.Items()) {
        if (!PyEqual(GetOr(row, "completed_requests"), requests)) {
            Fail(ErrorKind::kValue, "Benchmark request count mismatch");
        }
        const Value mix = GetOr(row, "request_mix");
        bool valid_mix = mix.IsObject() && mix.Members().size() == kKinds.size();
        if (valid_mix) {
            for (const Member& member : mix.Members()) {
                valid_mix = valid_mix && std::find(kKinds.begin(), kKinds.end(), member.key) != kKinds.end();
            }
        }
        if (valid_mix) {
            for (const Member& member : mix.Members()) {
                if (!member.value.IsInt() || member.value.AsInt().IsNegative()) {
                    valid_mix = false;
                    break;
                }
            }
        }
        if (valid_mix) {
            BigInt total;
            for (const Member& member : mix.Members()) {
                total = total + member.value.AsInt();
            }
            valid_mix = PyEqual(Value::Int(total), requests);
        }
        if (!valid_mix) {
            Fail(ErrorKind::kValue, "Benchmark lacks valid actual request mix counts");
        }
        const Value blocks = GetOr(row, "completed_blocks");
        const Value area_chunks = GetOr(row, "returned_area_chunks");
        if (!blocks.IsInt() || blocks.AsInt().IsNegative() || !area_chunks.IsInt() || area_chunks.AsInt().IsNegative()) {
            Fail(ErrorKind::kValue, "Benchmark lacks valid completed block/area counts");
        }
        const BigInt expected_blocks =
            GetItem(mix, "set_block").AsInt() + GetItem(mix, "get_block").AsInt() +
            width.AsInt() * height.AsInt() *
                (GetItem(mix, "set_chunk").AsInt() + GetItem(mix, "get_chunk").AsInt() + area_chunks.AsInt());
        if (!(blocks.AsInt() == expected_blocks)) {
            Fail(ErrorKind::kValue, "Benchmark block count disagrees with actual mix/geometry");
        }
        for (const std::string_view name : {"duration_s", "throughput_req_s", "blocks_per_request", "throughput_blocks_s"}) {
            PositiveNumber(GetOr(row, name), std::string(name), name != "duration_s" && name != "throughput_req_s");
        }
        for (const std::string_view name : {"p50", "p99"}) {
            const Value latency = GetOr(row, "latency_ms", Value::Object());
            PositiveNumber(GetOr(latency, name), std::string(name), true);
        }
        const Value per_request = Abs(Sub(GetItem(row, "blocks_per_request"), Div(blocks, requests)));
        if (Greater(per_request, Value::Float(0.0000005001))) {
            Fail(ErrorKind::kValue, "Benchmark blocks/request denominator mismatch");
        }
        // The benchmark prints six decimal places. Account for duration rounding
        // when checking rates from a very short run, rather than a fixed tolerance.
        const Value& duration = GetItem(row, "duration_s");
        const Value rounding = Value::Float(0.0000005);
        const std::array<std::tuple<std::string_view, Value, Value>, 2> rates = {{
            {"throughput_req_s", requests, Value::Float(0.0005)},
            {"throughput_blocks_s", blocks, rounding},
        }};
        for (const auto& [name, count, rate_rounding] : rates) {
            const Value rate = Div(count, duration);
            const Value shifted = Sub(duration, rounding);
            const Value& larger = Greater(rounding, shifted) ? rounding : shifted;
            const Value tolerance =
                Add(Add(Div(Mul(count, rounding), Mul(duration, larger)), rate_rounding), Value::Float(1e-9));
            if (Greater(Abs(Sub(GetItem(row, name), rate)), tolerance)) {
                Fail(ErrorKind::kValue, "Benchmark " + std::string(name) + " denominator mismatch");
            }
        }
    }
}

Value Summary(const std::vector<Value>& values) {
    Value result = Value::Object();
    result.Set("median", Median(values));
    result.Set("min", Extreme(values, false));
    result.Set("max", Extreme(values, true));
    result.Set("runs", I(static_cast<std::int64_t>(values.size())));
    for (const std::string_view key : {"median", "min", "max"}) {
        FiniteNumber(GetItem(result, key), "summary " + std::string(key));
    }
    return result;
}

namespace {

Value SummarizeSide(const Value& samples, std::string_view scenario) {
    std::vector<Value> rows;
    for (const Value& sample : Iterate(samples)) {
        std::optional<Value> found;
        for (const Value& row : Iterate(GetItem(GetItem(sample, "report"), "results"))) {
            if (PyEqual(GetItem(row, "test"), S(scenario))) {
                found = row;
                break;
            }
        }
        if (!found) {
            Fail(ErrorKind::kRuntime, "StopIteration");
        }
        rows.push_back(std::move(*found));
    }
    Value result = Value::Object();
    for (const std::string_view name :
         {"throughput_req_s", "throughput_blocks_s", "blocks_per_request", "completed_blocks", "returned_area_chunks"}) {
        std::vector<Value> values;
        for (const Value& row : rows) {
            values.push_back(GetItem(row, name));
        }
        result.Set(std::string(name), Summary(values));
    }
    for (const std::string_view name : {"p50", "p99"}) {
        std::vector<Value> values;
        for (const Value& row : rows) {
            values.push_back(GetItem(GetItem(row, "latency_ms"), name));
        }
        result.Set(std::string(name) + "_ms", Summary(values));
    }
    Value mix = Value::Object();
    for (const std::string_view kind : kKinds) {
        std::vector<Value> values;
        for (const Value& row : rows) {
            values.push_back(Div(Mul(I(100), GetItem(GetItem(row, "request_mix"), kind)),
                                 GetItem(row, "completed_requests")));
        }
        mix.Set(std::string(kind), Summary(values));
    }
    result.Set("request_mix_percent", std::move(mix));
    return result;
}

}  // namespace

Value PairedStatistics(const Value& samples, std::string_view scenario, const Value& budget) {
    struct Entry {
        BigInt pair;
        BigInt seed;
        Value sample;
    };
    std::array<std::vector<Entry>, 2> indexed;
    const std::array<std::string_view, 2> sides = {"before", "after"};
    for (std::size_t s = 0; s < sides.size(); ++s) {
        for (const Value& sample : Iterate(GetItem(samples, sides[s]))) {
            const Value pair = GetOr(sample, "pair");
            const Value seed = GetOr(GetItem(sample, "report"), "seed");
            if (!pair.IsInt() || Less(pair, I(1)) || !seed.IsInt()) {
                Fail(ErrorKind::kValue, "Invalid pair/seed identity");
            }
            for (const Entry& old : indexed[s]) {
                if (old.pair == pair.AsInt()) {
                    Fail(ErrorKind::kValue, "Duplicate pair/seed identity");
                }
            }
            indexed[s].push_back(Entry{pair.AsInt(), seed.AsInt(), sample});
        }
    }
    auto find = [](const std::vector<Entry>& entries, const BigInt& pair, const BigInt& seed) -> const Entry* {
        for (const Entry& entry : entries) {
            if (entry.pair == pair && entry.seed == seed) {
                return &entry;
            }
        }
        return nullptr;
    };
    bool matched = !indexed[0].empty() && indexed[0].size() == indexed[1].size();
    for (const Entry& entry : indexed[0]) {
        matched = matched && find(indexed[1], entry.pair, entry.seed) != nullptr;
    }
    if (!matched) {
        Fail(ErrorKind::kValue, "Missing/unmatched pair/seed identities");
    }
    std::vector<const Entry*> keys;
    for (const Entry& entry : indexed[0]) {
        keys.push_back(&entry);
    }
    std::sort(keys.begin(), keys.end(), [](const Entry* a, const Entry* b) {
        const int order = Compare(a->pair, b->pair);
        return order != 0 ? order < 0 : Compare(a->seed, b->seed) < 0;
    });
    Value pairs = Value::Array();
    std::vector<double> ratios;
    for (const Entry* key : keys) {
        std::array<Value, 2> rates;
        Value orders = Value::Object();
        for (std::size_t s = 0; s < sides.size(); ++s) {
            const Value& sample = find(indexed[s], key->pair, key->seed)->sample;
            std::vector<Value> rows;
            for (const Value& row : Iterate(GetItem(GetItem(sample, "report"), "results"))) {
                if (PyEqual(GetItem(row, "test"), S(scenario))) {
                    rows.push_back(row);
                }
            }
            if (rows.size() != 1) {
                Fail(ErrorKind::kValue, "Missing/duplicate paired scenario");
            }
            rates[s] = GetItem(rows[0], "throughput_req_s");
            PositiveNumber(rates[s], "paired throughput");
            orders.Set(std::string(sides[s]), GetOr(sample, "order"));
        }
        const Value ratio = Div(rates[1], rates[0]);
        PositiveNumber(ratio, "paired ratio");
        Value entry = Value::Object();
        entry.Set("pair", Value::Int(key->pair));
        entry.Set("seed", Value::Int(key->seed));
        entry.Set("before_req_s", rates[0]);
        entry.Set("after_req_s", rates[1]);
        entry.Set("ratio", ratio);
        entry.Set("order", std::move(orders));
        pairs.Append(std::move(entry));
        ratios.push_back(ratio.AsFloat());
    }
    std::vector<double> sorted_ratios = ratios;
    const double point = InferenceMedian(sorted_ratios);
    const double change = 100 * (point - 1);
    FiniteNumber(Value::Float(change), "paired change");
    Value result = Value::Object();
    result.Set("pairs", std::move(pairs));
    result.Set("median_ratio", Value::Float(point));
    result.Set("change_percent", Value::Float(change));
    result.Set("ratio_sd", Value());
    result.Set("ci_ratio", Value());
    result.Set("regression", Value());
    result.Set("decision", S("insufficient inference"));
    if (ratios.size() < 2) {
        return result;
    }
    // random.Random(seed) reset per call; each resample draws len(ratios)
    // indices with randrange(len(ratios)).
    PythonRandom random(kBootstrapSeed);
    const auto count = static_cast<std::uint32_t>(ratios.size());
    std::vector<double> draws;
    draws.reserve(kResamples);
    std::vector<double> resample(ratios.size());
    for (int r = 0; r < kResamples; ++r) {
        for (double& value : resample) {
            value = ratios[random.RandBelow(count)];
        }
        draws.push_back(InferenceMedian(resample));
    }
    std::sort(draws.begin(), draws.end());
    const std::array<double, 2> interval = {Type7(draws, 0.025), Type7(draws, 0.975)};
    for (const double value : interval) {
        PositiveNumber(Value::Float(value), "derived paired CI endpoint");
        FiniteNumber(Value::Float(100 * (value - 1)), "paired CI change");
    }
    double sd = 0.0;
    try {
        sd = SampleStdev(ratios);
    } catch (const Error& error) {
        if (error.kind() != ErrorKind::kOverflow) {
            throw;
        }
        Fail(ErrorKind::kValue, "Nonfinite derived paired SD");
    }
    PositiveNumber(Value::Float(sd), "derived paired SD", true);
    FiniteNumber(Value::Float(100 * sd), "paired SD percentage points");
    const Value threshold = Sub(I(1), Div(budget, I(100)));
    const Value low = Value::Float(interval[0]);
    const Value high = Value::Float(interval[1]);
    const bool regression = Less(high, threshold);
    result.Set("ratio_sd", Value::Float(sd));
    result.Set("ci_ratio", Value::Array({low, high}));
    result.Set("regression", Value::Bool(regression));
    result.Set("decision", S(regression                                                  ? "regression criterion met"
                             : LessEqual(low, threshold) && LessEqual(threshold, high) ? "inconclusive"
                                                                                       : "regression criterion not met"));
    return result;
}

Value ProfileSettings(const Value& profile) {
    Value settings = Value::Object();
    for (const std::string_view key :
         {"name", "durability_mode", "requests_per_run", "clients", "server_workers", "pipeline", "io_timeout_ms",
          "keyspace_blocks_per_axis", "runs_per_build", "watch_before", "watch_after"}) {
        settings.Set(std::string(key), GetItem(profile, key));
    }
    return settings;
}

Value SummarizeProfile(const Value& name, const Value& mode, const Value& requests, const Value& clients,
                       const Value& keyspace, const Value& gates, const Value& samples, const Value& budget,
                       const Value& watch) {
    if (!samples.IsObject()) {
        Fail(ErrorKind::kAttribute, "'" + TypeName(samples) + "' object has no attribute 'items'");
    }
    Value builds = Value::Object();
    std::vector<Value> observations;
    for (const Member& member : samples.Members()) {
        const Value first = GetIndex(member.value, 0);
        Value build = Value::Object();
        for (const std::string_view field : kBuildFields) {
            build.Set(std::string(field), GetItem(GetItem(first, "report"), field));
        }
        build.Set("binary_sha256", GetItem(first, "binary_sha256"));
        const Value command = GetIndex(GetItem(first, "command"), 0);
        if (!command.IsString()) {
            Fail(ErrorKind::kType, "argument should be a str or an os.PathLike object where __fspath__ returns a str, "
                                   "not '" + TypeName(command) + "'");
        }
        build.Set("directory", Value::Str(PathParent(command.AsString())));
        for (const Value& sample : Iterate(member.value)) {
            bool changed = false;
            for (const std::string_view field : kBuildFields) {
                if (!PyEqual(GetItem(GetItem(sample, "report"), field), GetItem(build, field))) {
                    changed = true;
                    break;
                }
            }
            if (changed || !PyEqual(GetItem(sample, "binary_sha256"), GetItem(build, "binary_sha256"))) {
                Fail(ErrorKind::kValue, member.key + " build/geometry changed between runs");
            }
            for (const std::string_view key : {"load_before", "load_after"}) {
                const Value& load = GetItem(sample, key);
                if (!load.IsNull()) {
                    observations.push_back(load);
                }
            }
        }
        builds.Set(member.key, std::move(build));
    }
    for (const std::string_view field : {"chunk_width_blocks", "chunk_height_blocks", "build_type"}) {
        if (!PyEqual(GetItem(GetItem(builds, "before"), field), GetItem(GetItem(builds, "after"), field))) {
            Fail(ErrorKind::kValue, "Compared builds must use the same geometry and build type");
        }
    }
    Value results = Value::Array();
    bool any_unknown = false;
    bool any_regression = false;
    for (const std::string_view scenario : kScenarioNames) {
        Value before = SummarizeSide(GetItem(samples, "before"), scenario);
        Value after = SummarizeSide(GetItem(samples, "after"), scenario);
        const Value change = Mul(I(100), Sub(Div(GetItem(GetItem(after, "throughput_req_s"), "median"),
                                                 GetItem(GetItem(before, "throughput_req_s"), "median")),
                                             I(1)));
        FiniteNumber(change, "ratio-of-medians change");
        Value paired = PairedStatistics(samples, scenario, budget);
        const Value& regression = GetItem(paired, "regression");
        const std::string verdict = regression.IsNull() ? "insufficient inference"
                                    : !Truthy(gates)    ? "reported only"
                                    : Truthy(regression) ? "OVER BUDGET"
                                                         : PyStr(GetItem(paired, "decision"));
        any_unknown = any_unknown || regression.IsNull();
        any_regression = any_regression || Truthy(regression);
        Value row = Value::Object();
        row.Set("scenario", S(scenario));
        row.Set("request", S(Description(scenario)));
        row.Set("before", std::move(before));
        row.Set("after", std::move(after));
        row.Set("change_percent", change);
        row.Set("paired", std::move(paired));
        row.Set("verdict", Value::Str(verdict));
        results.Append(std::move(row));
    }
    Value load;
    if (!observations.empty()) {
        load = Value::Array();
        for (std::size_t i = 0; i < 3; ++i) {
            std::vector<Value> column;
            for (const Value& row : observations) {
                column.push_back(GetIndex(row, i));
            }
            Value range = Value::Object();
            range.Set("min", Extreme(column, false));
            range.Set("max", Extreme(column, true));
            load.Append(std::move(range));
        }
    }
    Value profile = Value::Object();
    profile.Set("name", name);
    profile.Set("durability_mode", mode);
    profile.Set("requests_per_run", requests);
    profile.Set("clients", clients);
    profile.Set("server_workers", clients);
    profile.Set("pipeline", I(1));
    profile.Set("io_timeout_ms", I(kIoTimeoutMs));
    profile.Set("keyspace_blocks_per_axis", keyspace);
    profile.Set("runs_per_build", I(static_cast<std::int64_t>(Len(GetItem(samples, "before")))));
    profile.Set("watch_before", Value());
    profile.Set("watch_after", watch);
    profile.Set("gates", gates);
    profile.Set("budget_percent", budget);
    profile.Set("builds", std::move(builds));
    profile.Set("load_average_range", std::move(load));
    profile.Set("results", std::move(results));
    profile.Set("samples", samples);
    profile.Set("passed", any_unknown ? Value() : Value::Bool(!Truthy(gates) || !any_regression));
    return profile;
}

namespace {

bool IsStr(const Value* value) { return value != nullptr && value->IsString(); }

void ValidateControlEnvelope(const Value& reference) {
    if (!reference.IsObject() || !GetOr(reference, "noise_reference").IsNull()) {
        Fail(ErrorKind::kValue, "Noise control cannot contain another reference");
    }
    const Value machine = GetOr(reference, "machine");
    if (!machine.IsObject()) {
        Fail(ErrorKind::kValue, "Noise control requires machine metadata");
    }
    for (const std::string_view key : {"model", "cpu", "os_version", "architecture", "load_sampling"}) {
        if (!IsStr(machine.Find(key))) {
            Fail(ErrorKind::kValue, "Noise control lacks valid machine " + std::string(key));
        }
    }
    for (const std::string_view key : {"physical_cores", "logical_cores", "ram_bytes"}) {
        const Value* value = machine.Find(key);
        if (value == nullptr || (!value->IsNull() && (!value->IsInt() || !Greater(*value, I(0))))) {
            Fail(ErrorKind::kValue, "Noise control lacks valid machine " + std::string(key));
        }
    }
    const Value& ram = GetItem(machine, "ram_bytes");
    if (!ram.IsNull()) {
        Value gib;
        try {
            gib = Div(ram, I(std::int64_t{1} << 30));
        } catch (const Error& error) {
            if (error.kind() != ErrorKind::kOverflow) {
                throw;
            }
            Fail(ErrorKind::kValue, "Noise control RAM cannot be displayed in GiB");
        }
        FiniteNumber(gib, "noise control RAM GiB");
    }
    const Value filesystem = GetOr(machine, "filesystem");
    if (!filesystem.IsObject() || !IsStr(filesystem.Find("type")) || !IsStr(filesystem.Find("path"))) {
        Fail(ErrorKind::kValue, "Noise control lacks valid filesystem metadata");
    }
    if (!GetOr(reference, "profiles").IsArray()) {
        Fail(ErrorKind::kValue, "Noise control requires profile list");
    }
    for (const std::string_view key : {"started_utc", "finished_utc"}) {
        if (!IsStr(reference.Find(key))) {
            Fail(ErrorKind::kValue, "Noise control lacks " + std::string(key));
        }
    }
}

// recomputed != profile, where recomputed["samples"] is profile["samples"].
bool SameProfile(const Value& recomputed, const Value& profile) {
    if (!profile.IsObject() || recomputed.Members().size() != profile.Members().size()) {
        return false;
    }
    for (const Member& member : recomputed.Members()) {
        const Value* other = profile.Find(member.key);
        if (other == nullptr) {
            return false;
        }
        if (member.key != "samples" && !PyEqual(member.value, *other)) {
            return false;
        }
    }
    return true;
}

}  // namespace

Value ReadNoiseReference(const std::string& filename, const std::string& window_id, const Value& settings,
                         std::int64_t started_micros) {
    const std::string raw = ReadFileBytes(filename);
    Value reference = ParseJsonBytes(raw);
    ValidateControlEnvelope(reference);
    if (!PyEqual(GetOr(reference, "schema_version"), I(2)) ||
        !PyEqual(GetOr(reference, "measurement_kind"), S("noise-floor")) ||
        !PyEqual(GetOr(reference, "window_id"), Value::Str(window_id))) {
        Fail(ErrorKind::kValue, "Noise reference requires completed same-window schema2 control");
    }
    if (!PyEqual(GetOr(reference, "paired_method"), BootstrapMethod())) {
        Fail(ErrorKind::kValue, "Noise reference analysis method differs");
    }
    const std::optional<std::int64_t> start = ParseIsoTime(GetItem(reference, "started_utc").AsString());
    const std::optional<std::int64_t> end = ParseIsoTime(GetItem(reference, "finished_utc").AsString());
    if (!start || !end || !(*start <= *end && *end <= started_micros)) {
        Fail(ErrorKind::kValue, "Noise control must complete before comparison");
    }
    const Value& profiles = GetItem(reference, "profiles");
    bool same_set = Len(profiles) == settings.Members().size();
    if (same_set) {
        std::vector<Value> names;
        for (const Value& profile : Iterate(profiles)) {
            const Value& name = GetItem(profile, "name");
            RequireHashable(name);
            names.push_back(name);
        }
        for (const Value& name : names) {
            same_set = same_set && name.IsString() && settings.Find(name.AsString()) != nullptr;
        }
        for (const Member& member : settings.Members()) {
            same_set = same_set && std::any_of(names.begin(), names.end(), [&](const Value& name) {
                           return name.IsString() && name.AsString() == member.key;
                       });
        }
    }
    if (!same_set) {
        Fail(ErrorKind::kValue, "Noise reference profile set differs");
    }
    for (const Value& profile : profiles.Items()) {
        if (!PyEqual(ProfileSettings(profile), GetItem(settings, GetItem(profile, "name").AsString()))) {
            Fail(ErrorKind::kValue, "Noise reference configuration differs");
        }
        PositiveNumber(GetItem(profile, "budget_percent"), "noise budget", true);
        if (!GetItem(profile, "gates").IsBool()) {
            Fail(ErrorKind::kValue, "Noise control lacks valid gate policy");
        }
        const Value& n = GetItem(profile, "runs_per_build");
        for (const std::string_view side : {"before", "after"}) {
            const Value& samples = GetItem(GetItem(profile, "samples"), side);
            if (!PyEqual(I(static_cast<std::int64_t>(Len(samples))), n)) {
                Fail(ErrorKind::kValue, "Noise reference incomplete samples");
            }
            for (const Value& sample : Iterate(samples)) {
                const Value& pair = GetItem(sample, "pair");
                if (!pair.IsInt() || !(LessEqual(I(1), pair) && LessEqual(pair, n)) ||
                    !PyEqual(GetItem(sample, "order"), I(pair.AsInt().IsOdd() == (side == "before") ? 1 : 2))) {
                    Fail(ErrorKind::kValue, "Noise reference pair/order differs");
                }
                ValidateRun(GetItem(sample, "report"), GetItem(profile, "durability_mode"),
                            GetItem(profile, "requests_per_run"), GetItem(profile, "clients"),
                            GetItem(profile, "keyspace_blocks_per_axis"), Value::Int(BigInt(1336) + pair.AsInt()));
                if (!IsHexDigest(GetItem(sample, "binary_sha256"))) {
                    Fail(ErrorKind::kValue, "Invalid noise binary SHA256");
                }
                for (const std::string_view key : {"load_before", "load_after"}) {
                    const Value& load = GetItem(sample, key);
                    if (load.IsNull()) {
                        continue;
                    }
                    if (Len(load) != 3) {
                        Fail(ErrorKind::kValue, "Invalid noise load sample");
                    }
                    for (const Value& value : Iterate(load)) {
                        PositiveNumber(value, "noise load", true);
                    }
                }
            }
        }
        const Value recomputed = SummarizeProfile(
            GetItem(profile, "name"), GetItem(profile, "durability_mode"), GetItem(profile, "requests_per_run"),
            GetItem(profile, "clients"), GetItem(profile, "keyspace_blocks_per_axis"), GetItem(profile, "gates"),
            GetItem(profile, "samples"), GetItem(profile, "budget_percent"), Value());
        if (!SameProfile(recomputed, profile)) {
            Fail(ErrorKind::kValue, "Noise reference summaries/provenance do not match raw samples");
        }
        const Value& builds = GetItem(profile, "builds");
        std::vector<std::string_view> fields(kBuildFields.begin(), kBuildFields.end());
        fields.push_back("binary_sha256");
        for (const std::string_view key : fields) {
            if (!PyEqual(GetItem(GetItem(builds, "before"), key), GetItem(GetItem(builds, "after"), key))) {
                Fail(ErrorKind::kValue, "Noise control requires identical binaries");
            }
        }
    }
    Value result = Value::Object();
    result.Set("path", Value::Str(RealPath(filename)));
    result.Set("sha256", Value::Str(BytesSha256(raw)));
    result.Set("report", std::move(reference));
    return result;
}

namespace {

std::string Metric(const Value& value, int digits = 1) {
    const std::string spec = ",." + std::to_string(digits) + "f";
    return FormatNumber(GetItem(value, "median"), spec) + " [" + FormatNumber(GetItem(value, "min"), spec) +
           "\xE2\x80\x93" + FormatNumber(GetItem(value, "max"), spec) + "]";
}

std::string MixText(const Value& side) {
    std::string out;
    bool first = true;
    for (const Member& member : GetItem(side, "request_mix_percent").Members()) {
        if (!Greater(GetItem(member.value, "max"), I(0))) {
            continue;
        }
        out += (first ? "" : "; ") + member.key + " " + Metric(member.value, 3) + "%";
        first = false;
    }
    return out;
}

std::string Join(const std::vector<std::string>& lines, std::string_view separator) {
    std::string out;
    for (std::size_t i = 0; i < lines.size(); ++i) {
        out += (i == 0 ? "" : std::string(separator)) + lines[i];
    }
    return out;
}

}  // namespace

std::string RenderMarkdown(const Value& report) {
    const Value& machine = GetItem(report, "machine");
    const Value& ram_bytes = GetItem(machine, "ram_bytes");
    const std::string ram =
        Truthy(ram_bytes) ? FormatNumber(Div(ram_bytes, I(std::int64_t{1} << 30)), ".2f") + " GiB" : "unavailable";
    const Value& filesystem = GetItem(machine, "filesystem");
    const Value* niceness = Get(machine, "process_niceness");
    std::vector<std::string> lines = {
        "# Hot-path budget comparison",
        "",
        "UTC: " + PyStr(GetItem(report, "started_utc")) + " to " + PyStr(GetItem(report, "finished_utc")) + ".",
        "Machine: " + PyStr(GetItem(machine, "model")) + "; CPU " + PyStr(GetItem(machine, "cpu")) + "; cores " +
            OrUnavailable(GetItem(machine, "physical_cores")) + " physical / " +
            OrUnavailable(GetItem(machine, "logical_cores")) + " logical; RAM " + ram + ".",
        "OS: " + PyStr(GetItem(machine, "os_version")) + "; architecture " + PyStr(GetItem(machine, "architecture")) +
            ". Benchmark temporary-store filesystem: " + PyStr(GetItem(filesystem, "type")) + " at `" +
            PyStr(GetItem(filesystem, "path")) + "`.",
        "Values are medians [min\xE2\x80\x93max] across runs. Latency is the median of each run's p50/p99, with the "
        "range of those run percentiles.",
        "Blocks count positions handled by successful requests, not changed blocks or bytes; point requests count one "
        "and chunk/area forms use actual geometry and returned entries.",
        "Load sampling: " + PyStr(GetItem(machine, "load_sampling")) +
            ". Process niceness: " + (niceness != nullptr ? PyStr(*niceness) : std::string("unavailable")) + ".",
        "",
    };
    for (const Value& profile : Iterate(GetItem(report, "profiles"))) {
        const Value& load_range = GetItem(profile, "load_average_range");
        std::string load = "unavailable";
        if (Truthy(load_range)) {
            std::vector<std::string> parts;
            for (const Value& part : Iterate(load_range)) {
                parts.push_back(FormatNumber(GetItem(part, "min"), ".2f") + "\xE2\x80\x93" +
                                FormatNumber(GetItem(part, "max"), ".2f"));
            }
            load = Join(parts, " / ");
        }
        lines.push_back("## " + PyStr(GetItem(profile, "name")));
        lines.push_back("");
        lines.push_back("Durability `" + PyStr(GetItem(profile, "durability_mode")) + "`; clients/workers " +
                        PyStr(GetItem(profile, "clients")) + "/" + PyStr(GetItem(profile, "server_workers")) +
                        "; pipeline " + PyStr(GetItem(profile, "pipeline")) + "; I/O timeout " +
                        PyStr(GetItem(profile, "io_timeout_ms")) + " ms; requests/run " +
                        FormatNumber(GetItem(profile, "requests_per_run"), ",") + "; keyspace " +
                        PyStr(GetItem(profile, "keyspace_blocks_per_axis")) + " blocks/axis; " +
                        PyStr(GetItem(profile, "runs_per_build")) +
                        " alternating runs/build; load average ranges (1/5/15 min) " + load + ".");
        lines.push_back(Truthy(GetItem(profile, "gates"))
                            ? "Gate: paired upper95% ratio CI below1\xE2\x88\x92" +
                                  PyStr(GetItem(profile, "budget_percent")) + "/100"
                            : std::string("Gate: reported only (does not affect exit status)."));
        lines.push_back("WATCH before: " + OrUnavailable(GetItem(profile, "watch_before"), "none") +
                        "; after: " + OrUnavailable(GetItem(profile, "watch_after"), "none") + ".");
        const Value& builds = GetItem(profile, "builds");
        if (!builds.IsObject()) {
            Fail(ErrorKind::kAttribute, "'" + TypeName(builds) + "' object has no attribute 'items'");
        }
        for (const Member& member : builds.Members()) {
            const Value& build = member.value;
            lines.push_back(Capitalize(member.key) + ": `" + PyStr(GetItem(build, "source_commit")) + "` (" +
                            PyStr(GetItem(build, "source_state")) + "), " + PyStr(GetItem(build, "build_type")) +
                            "; chunk " + PyStr(GetItem(build, "chunk_width_blocks")) + "x" +
                            PyStr(GetItem(build, "chunk_height_blocks")) + "; binary SHA256 `" +
                            PyStr(GetItem(build, "binary_sha256")) + "`; benchmark source SHA256 `" +
                            PyStr(GetItem(build, "benchmark_source_sha256")) + "`.");
        }
        lines.push_back("");
        lines.push_back(
            "| Scenario / request | Actual mix before / after (%) | Blocks/request before / after | Before req/s | "
            "After req/s | Before blocks/s | After blocks/s | Before p50 ms | After p50 ms | Before p99 ms | After p99 "
            "ms | Verdict | Change req/s |");
        lines.push_back("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |");
        for (const Value& row : Iterate(GetItem(profile, "results"))) {
            const Value& before = GetItem(row, "before");
            const Value& after = GetItem(row, "after");
            std::vector<std::string> cells = {
                "`" + PyStr(GetItem(row, "scenario")) + "`: " + PyStr(GetItem(row, "request")),
                MixText(before) + " / " + MixText(after),
                Metric(GetItem(before, "blocks_per_request"), 3) + " / " +
                    Metric(GetItem(after, "blocks_per_request"), 3),
            };
            for (const auto& [name, digits] : std::array<std::pair<std::string_view, int>, 4>{
                     {{"throughput_req_s", 1}, {"throughput_blocks_s", 1}, {"p50_ms", 3}, {"p99_ms", 3}}}) {
                cells.push_back(Metric(GetItem(before, name), digits));
                cells.push_back(Metric(GetItem(after, name), digits));
            }
            cells.push_back(PyStr(GetItem(row, "verdict")));
            cells.push_back(FormatNumber(GetItem(row, "change_percent"), "+.2f") + "%");
            for (std::string& cell : cells) {
                cell = ReplaceAll(cell, "|", "\\|");
            }
            lines.push_back("| " + Join(cells, " | ") + " |");
        }
        lines.push_back("");
    }
    const Value* window = Get(report, "window_id");
    const Value* kind = Get(report, "measurement_kind");
    lines.push_back("## Paired inference");
    lines.push_back("");
    lines.push_back("Window: `" + (window != nullptr ? PyStr(*window) : std::string("unavailable")) +
                    "`; kind: " + (kind != nullptr ? PyStr(*kind) : std::string("unavailable")) + ".");
    lines.push_back("100000 whole-pair resamples; seed20261011 resets per row; median ratio;95% percentile CI, Type7; "
                    "sample SD n\xE2\x88\x92" "1.");
    lines.push_back("Matching uses(pair,seed), not execution order. Pairs are exchangeable; cross-pair drift is not "
                    "modeled. Small pair counts limit inference; one pair has no CI/SD or PASS claim.");
    lines.push_back("Regression requires upper CI ratio strictly below1\xE2\x88\x92" "budget/100. Inconclusive is not "
                    "equivalence. Exit0 means no gated criterion met, not proof of equal speed.");
    lines.push_back("Build source_commit may identify an instrumentation overlay; underlying production core is "
                    "unavailable here. External reviewed manifests can supply that mapping. Source fingerprint covers "
                    "benchmark inputs, not whole-core contents.");
    lines.push_back("");
    lines.push_back("| Profile / scenario | Before req/s | After req/s | Before blocks/s | After blocks/s | Paired median "
                    "change |95% CI change | Ratio SD pp | CI decision |");
    lines.push_back("| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    for (const Value& profile : Iterate(GetItem(report, "profiles"))) {
        for (const Value& row : Iterate(GetItem(profile, "results"))) {
            const Value& paired = GetItem(row, "paired");
            const Value& interval = GetItem(paired, "ci_ratio");
            const std::string ci =
                interval.IsNull()
                    ? "unavailable"
                    : "[" + FormatNumber(Mul(I(100), Sub(GetIndex(interval, 0), I(1))), "+.2f") + "%, " +
                          FormatNumber(Mul(I(100), Sub(GetIndex(interval, 1), I(1))), "+.2f") + "%]";
            const Value& ratio_sd = GetItem(paired, "ratio_sd");
            const std::string sd = ratio_sd.IsNull() ? "unavailable" : FormatNumber(Mul(I(100), ratio_sd), ".2f");
            const Value& before = GetItem(row, "before");
            const Value& after = GetItem(row, "after");
            lines.push_back("| " + PyStr(GetItem(profile, "name")) + " / " + PyStr(GetItem(row, "scenario")) + " | " +
                            Metric(GetItem(before, "throughput_req_s")) + " | " +
                            Metric(GetItem(after, "throughput_req_s")) + " | " +
                            Metric(GetItem(before, "throughput_blocks_s")) + " | " +
                            Metric(GetItem(after, "throughput_blocks_s")) + " | " +
                            FormatNumber(GetItem(paired, "change_percent"), "+.2f") + "% | " + ci + " | " + sd +
                            " | " + PyStr(GetItem(paired, "decision")) + " |");
        }
    }
    const Value* reference = Get(report, "noise_reference");
    if (reference != nullptr && Truthy(*reference)) {
        lines.push_back("");
        lines.push_back("## Same-window noise reference");
        lines.push_back("");
        lines.push_back("Raw control `" + PyStr(GetItem(*reference, "path")) + "` SHA256 `" +
                        PyStr(GetItem(*reference, "sha256")) + "`.");
        lines.push_back("Its actual build source_commit and binary baseline are printed separately below and may differ "
                        "from this comparison\xE2\x80\x99s before build. Noise is not subtracted.");
        lines.push_back(RenderMarkdown(GetItem(*reference, "report")));
    }
    return Join(lines, "\n") + "\n";
}

// ---- Times ----

namespace {

std::int64_t DaysFromCivil(std::int64_t year, unsigned month, unsigned day) {
    year -= month <= 2 ? 1 : 0;
    const std::int64_t era = (year >= 0 ? year : year - 399) / 400;
    const auto yoe = static_cast<unsigned>(year - era * 400);
    const unsigned doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<std::int64_t>(doe) - 719468;
}

void CivilFromDays(std::int64_t z, std::int64_t& year, unsigned& month, unsigned& day) {
    z += 719468;
    const std::int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const auto doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    day = doy - (153 * mp + 2) / 5 + 1;
    month = mp < 10 ? mp + 3 : mp - 9;
    year = static_cast<std::int64_t>(yoe) + era * 400 + (month <= 2 ? 1 : 0);
}

std::string Pad(std::int64_t value, std::size_t width) {
    std::string text = std::to_string(value);
    return std::string(width > text.size() ? width - text.size() : 0, '0') + text;
}

}  // namespace

std::string IsoUtc(std::int64_t micros) {
    std::int64_t days = micros / 86400000000LL;
    std::int64_t rest = micros % 86400000000LL;
    if (rest < 0) {
        rest += 86400000000LL;
        --days;
    }
    std::int64_t year = 0;
    unsigned month = 0;
    unsigned day = 0;
    CivilFromDays(days, year, month, day);
    const std::int64_t seconds = rest / 1000000;
    const std::int64_t fraction = rest % 1000000;
    std::string out = Pad(year, 4) + "-" + Pad(month, 2) + "-" + Pad(day, 2) + "T" + Pad(seconds / 3600, 2) + ":" +
                      Pad(seconds / 60 % 60, 2) + ":" + Pad(seconds % 60, 2);
    if (fraction != 0) {
        out += "." + Pad(fraction, 6);
    }
    return out + "+00:00";
}

namespace {

// The datetime.fromisoformat parser of CPython 3.14 (_datetimemodule.c),
// over the UTF-8 text with '\0' past its end.
class IsoText {
  public:
    explicit IsoText(std::string text) : text_(std::move(text)) {}
    [[nodiscard]] char At(std::size_t i) const { return i < text_.size() ? text_[i] : '\0'; }
    [[nodiscard]] std::size_t size() const { return text_.size(); }

    // parse_digits: n ASCII digits at p, or std::nullopt.
    std::optional<std::size_t> Digits(std::size_t p, int& value, std::size_t n) const {
        value = 0;
        for (std::size_t i = 0; i < n; ++i, ++p) {
            const char c = At(p);
            if (c < '0' || c > '9') {
                return std::nullopt;
            }
            value = value * 10 + (c - '0');
        }
        return p;
    }

  private:
    std::string text_;
};

bool IsDigit(char c) { return c >= '0' && c <= '9'; }
bool IsLeap(int year) { return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0); }

int DaysInMonth(int year, int month) {
    static constexpr int kDays[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return kDays[month - 1] + (month == 2 && IsLeap(year) ? 1 : 0);
}

std::int64_t Ordinal(int year, int month, int day) {
    return DaysFromCivil(year, static_cast<unsigned>(month), static_cast<unsigned>(day)) + 719163;
}

long SeparatorLocation(const IsoText& s) {
    const auto len = static_cast<long>(s.size());
    if (len == 7) {
        return 7;
    }
    if (s.At(4) == '-') {
        if (s.At(5) == 'W') {
            if (len < 8) {
                return -1;
            }
            if (len > 8 && s.At(8) == '-') {
                if (len == 9) {
                    return -1;
                }
                if (len > 10 && IsDigit(s.At(10))) {
                    return 8;
                }
                return 10;
            }
            return 8;
        }
        return 10;
    }
    if (s.At(4) == 'W') {
        std::size_t idx = 7;
        for (; idx < s.size(); ++idx) {
            if (!IsDigit(s.At(idx))) {
                break;
            }
        }
        if (idx < 9) {
            return static_cast<long>(idx);
        }
        return idx % 2 == 0 ? 7 : 8;
    }
    return 8;
}

int IsoToYmd(int iso_year, int week, int weekday, int& year, int& month, int& day) {
    if (week <= 0 || week >= 53) {
        bool out_of_range = true;
        if (week == 53) {
            const auto first_weekday = static_cast<int>((Ordinal(iso_year, 1, 1) + 6) % 7);
            if (first_weekday == 3 || (first_weekday == 2 && IsLeap(iso_year))) {
                out_of_range = false;
            }
        }
        if (out_of_range) {
            return -2;
        }
    }
    if (weekday <= 0 || weekday >= 8) {
        return -3;
    }
    const std::int64_t first_day = Ordinal(iso_year, 1, 1);
    const auto first_weekday = static_cast<int>((first_day + 6) % 7);
    std::int64_t monday = first_day - first_weekday;
    if (first_weekday > 3) {
        monday += 7;
    }
    std::int64_t y = 0;
    unsigned m = 0;
    unsigned d = 0;
    CivilFromDays(monday + (week - 1) * 7 + weekday - 1 - 719163, y, m, d);
    year = static_cast<int>(y);
    month = static_cast<int>(m);
    day = static_cast<int>(d);
    return 0;
}

int ParseDate(const IsoText& s, std::size_t len, int& year, int& month, int& day) {
    std::optional<std::size_t> p = s.Digits(0, year, 4);
    if (!p) {
        return -1;
    }
    const bool separator = s.At(*p) == '-';
    if (separator) {
        ++*p;
    }
    if (s.At(*p) == 'W') {
        ++*p;
        int week = 0;
        int weekday = 0;
        p = s.Digits(*p, week, 2);
        if (!p) {
            return -3;
        }
        if (*p < len) {
            if (separator && s.At((*p)++) != '-') {
                return -2;
            }
            p = s.Digits(*p, weekday, 1);
            if (!p) {
                return -4;
            }
        } else {
            weekday = 1;
        }
        const int rv = IsoToYmd(year, week, weekday, year, month, day);
        return rv != 0 ? -3 + rv : 0;
    }
    p = s.Digits(*p, month, 2);
    if (!p) {
        return -1;
    }
    if (separator && s.At((*p)++) != '-') {
        return -2;
    }
    p = s.Digits(*p, day, 2);
    return p ? 0 : -1;
}

// parse_hh_mm_ss_ff over [start, end).
int ParseClock(const IsoText& s, std::size_t start, std::size_t end, int& hour, int& minute, int& second,
               int& microsecond) {
    hour = minute = second = microsecond = 0;
    int* values[3] = {&hour, &minute, &second};
    std::size_t p = start;
    bool has_separator = true;
    for (std::size_t i = 0; i < 3; ++i) {
        const std::optional<std::size_t> next = s.Digits(p, *values[i], 2);
        if (!next) {
            return -3;
        }
        p = *next;
        const char c = s.At(p++);
        if (i == 0) {
            has_separator = c == ':';
        }
        if (p >= end) {
            return c != '\0' ? 1 : 0;
        }
        if (has_separator && c == ':') {
            if (i == 2) {
                return -4;
            }
            continue;
        }
        if (c == '.' || c == ',') {
            if (i < 2) {
                return -3;
            }
            break;
        }
        if (!has_separator) {
            --p;
        } else {
            return -4;
        }
    }
    const std::size_t remaining = end - p;
    const std::size_t count = std::min<std::size_t>(remaining, 6);
    const std::optional<std::size_t> next = s.Digits(p, microsecond, count);
    if (!next) {
        return -3;
    }
    p = *next;
    static constexpr int kCorrection[] = {100000, 10000, 1000, 100, 10};
    if (count >= 1 && count < 6) {
        microsecond *= kCorrection[count - 1];
    }
    while (IsDigit(s.At(p))) {
        ++p;
    }
    return s.At(p) != '\0' ? 1 : 0;
}

int ParseTime(const IsoText& s, std::size_t start, std::size_t length, int& hour, int& minute, int& second,
              int& microsecond, int& tz_offset, int& tz_microsecond) {
    const std::size_t end = start + length;
    std::size_t tz = start;
    do {
        const char c = s.At(tz);
        if (c == 'Z' || c == '+' || c == '-') {
            break;
        }
    } while (++tz < end);
    int rv = ParseClock(s, start, tz, hour, minute, second, microsecond);
    if (rv < 0) {
        return rv;
    }
    if (tz == end) {
        return rv == 1 ? -5 : 0;
    }
    if (s.At(tz) == 'Z') {
        tz_offset = 0;
        tz_microsecond = 0;
        return s.At(tz + 1) != '\0' ? -5 : 1;
    }
    const int sign = s.At(tz) == '-' ? -1 : 1;
    int tz_hour = 0;
    int tz_minute = 0;
    int tz_second = 0;
    rv = ParseClock(s, tz + 1, end, tz_hour, tz_minute, tz_second, tz_microsecond);
    tz_offset = sign * (tz_hour * 3600 + tz_minute * 60 + tz_second);
    tz_microsecond *= sign;
    return rv != 0 ? -5 : 1;
}

// repr() of a datetime.timedelta of whole seconds and microseconds.
std::string TimedeltaRepr(std::int64_t seconds, std::int64_t micros) {
    std::int64_t total = seconds * 1000000 + micros;
    std::int64_t days = total / 86400000000LL;
    std::int64_t rest = total % 86400000000LL;
    if (rest < 0) {
        rest += 86400000000LL;
        --days;
    }
    std::vector<std::string> parts;
    if (days != 0) {
        parts.push_back("days=" + std::to_string(days));
    }
    if (rest / 1000000 != 0) {
        parts.push_back("seconds=" + std::to_string(rest / 1000000));
    }
    if (rest % 1000000 != 0) {
        parts.push_back("microseconds=" + std::to_string(rest % 1000000));
    }
    std::string text = "datetime.timedelta(";
    for (std::size_t i = 0; i < parts.size(); ++i) {
        text += (i == 0 ? "" : ", ") + parts[i];
    }
    return text + (parts.empty() ? "0)" : ")");
}

}  // namespace

std::optional<std::int64_t> ParseIsoTime(const std::string& text) {
    const std::string invalid = "Invalid isoformat string: " + PyRepr(Value::Str(text));
    // A non-ASCII separator counts as one character, as 'T'.
    std::string normalized = text;
    for (const std::size_t position : {std::size_t{7}, std::size_t{8}, std::size_t{10}}) {
        if (position < normalized.size() && (static_cast<unsigned char>(normalized[position]) & 0x80) != 0) {
            bool ascii_before = true;
            for (std::size_t i = 0; i < position; ++i) {
                ascii_before = ascii_before && (static_cast<unsigned char>(normalized[i]) & 0x80) == 0;
            }
            if (ascii_before) {
                std::size_t end = position + 1;
                while (end < normalized.size() && (static_cast<unsigned char>(normalized[end]) & 0xC0) == 0x80) {
                    ++end;
                }
                normalized.replace(position, end - position, "T");
            }
            break;
        }
    }
    const IsoText s(normalized);
    const long separator = SeparatorLocation(s);
    if (separator < 0) {
        Fail(ErrorKind::kValue, invalid);
    }
    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    int microsecond = 0;
    int tz_offset = 0;
    int tz_microsecond = 0;
    const auto sep = static_cast<std::size_t>(separator);
    int rv = ParseDate(s, sep, year, month, day);
    if (rv == 0 && s.size() > sep) {
        rv = ParseTime(s, sep + 1, s.size() - sep - 1, hour, minute, second, microsecond, tz_offset, tz_microsecond);
    }
    if (rv < 0) {
        Fail(ErrorKind::kValue, invalid);
    }
    std::int64_t offset_micros = 0;
    if (rv == 1 && tz_offset != 0) {
        offset_micros = static_cast<std::int64_t>(tz_offset) * 1000000 + tz_microsecond;
        if (offset_micros <= -86400000000LL || offset_micros >= 86400000000LL) {
            Fail(ErrorKind::kValue,
                 "offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24), not " +
                     TimedeltaRepr(tz_offset, tz_microsecond));
        }
    }
    if (hour == 24 && month >= 1 && month <= 12 && year >= 1 && year <= 9999 && day <= DaysInMonth(year, month)) {
        if (minute == 0 && second == 0 && microsecond == 0) {
            hour = 0;
            if (++day > DaysInMonth(year, month)) {
                day = 1;
                if (++month > 12) {
                    month = 1;
                    ++year;
                }
            }
        } else {
            Fail(ErrorKind::kValue, "minute, second, and microsecond must be 0 when hour is 24");
        }
    }
    if (year < 1 || year > 9999) {
        Fail(ErrorKind::kValue, "year must be in 1..9999, not " + std::to_string(year));
    }
    if (month < 1 || month > 12) {
        Fail(ErrorKind::kValue, "month must be in 1..12, not " + std::to_string(month));
    }
    if (day < 1 || day > DaysInMonth(year, month)) {
        Fail(ErrorKind::kValue, "day " + std::to_string(day) + " must be in range 1.." +
                                    std::to_string(DaysInMonth(year, month)) + " for month " + std::to_string(month) +
                                    " in year " + std::to_string(year));
    }
    if (hour > 23) {
        Fail(ErrorKind::kValue, "hour must be in 0..23, not " + std::to_string(hour));
    }
    if (minute > 59) {
        Fail(ErrorKind::kValue, "minute must be in 0..59, not " + std::to_string(minute));
    }
    if (second > 59) {
        Fail(ErrorKind::kValue, "second must be in 0..59, not " + std::to_string(second));
    }
    if (rv != 1) {
        return std::nullopt;
    }
    const std::int64_t local = (Ordinal(year, month, day) - 719163) * 86400000000LL +
                               (static_cast<std::int64_t>(hour) * 3600 + minute * 60 + second) * 1000000 +
                               microsecond;
    return local - offset_micros;
}

// ---- One benchmark process ----

Value RunOnce(const RunRequest& request, const RunEnvironment& environment) {
    const std::string bench = AbsolutePath(PathJoin(request.build_dir, BenchBinaryName()));
    std::vector<std::string> command = {bench,
                                        "--server-mode",
                                        "spawn",
                                        "--port",
                                        std::to_string(environment.free_port()),
                                        "--tests",
                                        std::string(kScenarios),
                                        "--requests",
                                        std::to_string(request.requests),
                                        "--clients",
                                        std::to_string(request.clients),
                                        "--keyspace",
                                        std::to_string(request.keyspace),
                                        "--seed",
                                        std::to_string(request.seed),
                                        "--durability-mode",
                                        request.mode,
                                        "--server-workers",
                                        std::to_string(request.clients),
                                        "--io-timeout-ms",
                                        std::to_string(kIoTimeoutMs),
                                        "--log-level",
                                        "error",
                                        "--output",
                                        "json"};
    if (request.watch && !request.watch->empty()) {
        command.push_back("--watch");
        command.push_back(*request.watch);
    }
    Value command_value = Value::Array();
    for (const std::string& part : command) {
        command_value.Append(Value::Str(part));
    }
    const std::string binary_hash = environment.file_hash(bench);
    const Value before = environment.load_average();
    const std::string stdout_path = request.record_prefix + ".stdout";
    const std::string stderr_path = request.record_prefix + ".stderr";
    CreateDirectories(PathParent(request.record_prefix));
    const std::int64_t started = environment.now_micros();
    const ExitStatus status = environment.spawn(command, stdout_path, stderr_path);
    const std::int64_t finished = environment.now_micros();
    Value record = Value::Object();
    record.Set("command", command_value);
    record.Set("started_utc", Value::Str(IsoUtc(started)));
    record.Set("finished_utc", Value::Str(IsoUtc(finished)));
    record.Set("exit_status", status.exited ? I(status.code) : Value());
    record.Set("signal", status.exited ? Value() : I(status.code));
    WriteTextFile(request.record_prefix + ".status.json", DumpJson(record) + "\n");
    const std::string out = DecodeChildText(ReadFileBytes(stdout_path));
    const std::string err = DecodeChildText(ReadFileBytes(stderr_path));
    if (!status.exited || status.code != 0) {
        Fail(ErrorKind::kRuntime, "Command '" + PyRepr(command_value) + "'" +
                                      (status.exited ? " returned non-zero exit status " + std::to_string(status.code)
                                                     : " died with signal " + std::to_string(status.code)) +
                                      ". Raw output: " + request.record_prefix + ".stdout, .stderr, .status.json");
    }
    const Value after = environment.load_average();
    if (environment.file_hash(bench) != binary_hash) {
        Fail(ErrorKind::kValue, "Benchmark binary changed during measurement");
    }
    const std::size_t brace = out.find('{');
    if (brace == std::string::npos) {
        Fail(ErrorKind::kValue, "substring not found");
    }
    Value parsed = ParseJson(std::string_view(out).substr(brace));
    ValidateRun(parsed, S(request.mode), I(request.requests), I(request.clients), I(request.keyspace),
                I(request.seed));
    Value sample = Value::Object();
    sample.Set("command", std::move(command_value));
    sample.Set("binary_sha256", Value::Str(binary_hash));
    sample.Set("load_before", before);
    sample.Set("load_after", after);
    sample.Set("stderr", Value::Str(err));
    sample.Set("report", std::move(parsed));
    return sample;
}

// ---- Command line ----

namespace {

struct UsageError {
    std::string message;
};

constexpr std::string_view kUsage =
    "usage: chunkdb_budget_compare [-h] [--runs RUNS] [--budget BUDGET] [--profiles PROFILES]\n"
    "                              [--watch WATCH] [--json [FILE]] [--markdown [FILE]]\n"
    "                              [--noise-floor] --window-id WINDOW_ID\n"
    "                              [--noise-reference NOISE_REFERENCE]\n"
    "                              [--relaxed-concurrency {4,8,16}]\n"
    "                              before after\n";

constexpr std::string_view kHelp =
    "\n"
    "Compare two builds with self-contained hot-path budget tables.\n"
    "\n"
    "Alternate builds for 15 runs per profile by default. The relaxed profile gates\n"
    "the upper 95% paired-bootstrap median-ratio bound at a 5% drop; fsync-wal is\n"
    "reported only. Use --noise-floor with the same binary twice at the start of each\n"
    "window; source comparisons require that completed --noise-reference and\n"
    "--window-id. Rebuild both benchmarks with block-count and build-provenance\n"
    "reporting before comparing them. Each benchmark process's stdout, stderr, exit\n"
    "status and UTC start/end are saved before parsing, in FILE.runs/ next to the\n"
    "--json FILE or in a new directory under the temporary directory.\n"
    "See CONTRIBUTING.md, \"Hot-path budgets\".\n"
    "\n"
    "    chunkdb_budget_compare BEFORE AFTER --window-id WINDOW --noise-reference noise.json --markdown --json result.json\n"
    "\n"
    "positional arguments:\n"
    "  before\n"
    "  after\n"
    "\n"
    "options:\n"
    "  -h, --help            show this help message and exit\n"
    "  --runs RUNS           alternating runs per build (default 15)\n"
    "  --budget BUDGET       paired CI regression threshold (default 5%)\n"
    "  --profiles PROFILES\n"
    "  --watch WATCH         attach a WATCH only to the after build (table name)\n"
    "  --json [FILE]         write full artifact JSON; omit FILE for stdout\n"
    "  --markdown [FILE]     write pasteable Markdown; omit FILE for stdout\n"
    "  --noise-floor         same-binary window-start calibration without older reference\n"
    "  --window-id WINDOW_ID\n"
    "                        coordinator-assigned measurement window identity\n"
    "  --noise-reference NOISE_REFERENCE\n"
    "                        completed same-window raw control JSON; required for comparisons\n"
    "  --relaxed-concurrency {4,8,16}\n"
    "                        matched relaxed clients/server workers (default 4)\n";

struct Options {
    std::optional<std::string> before;
    std::optional<std::string> after;
    BigInt runs = BigInt(15);
    double budget = 5.0;
    std::string profiles = "relaxed,fsync-wal";
    std::optional<std::string> watch;
    std::optional<std::string> json;
    std::optional<std::string> markdown;
    bool noise_floor = false;
    std::optional<std::string> window_id;
    std::optional<std::string> noise_reference;
    BigInt relaxed_concurrency = BigInt(4);
    bool help = false;
};

enum class Arity { kOne, kOptional, kFlag, kHelp };

struct OptionSpec {
    std::string_view name;
    Arity arity;
};

constexpr std::array<OptionSpec, 11> kOptionSpecs = {{
    {"--help", Arity::kHelp},
    {"--runs", Arity::kOne},
    {"--budget", Arity::kOne},
    {"--profiles", Arity::kOne},
    {"--watch", Arity::kOne},
    {"--json", Arity::kOptional},
    {"--markdown", Arity::kOptional},
    {"--noise-floor", Arity::kFlag},
    {"--window-id", Arity::kOne},
    {"--noise-reference", Arity::kOne},
    {"--relaxed-concurrency", Arity::kOne},
}};

bool IsSpace(std::uint32_t c) {
    return c == ' ' || (c >= 0x09 && c <= 0x0D) || (c >= 0x1C && c <= 0x1F) || c == 0x85 || c == 0xA0 ||
           c == 0x1680 || (c >= 0x2000 && c <= 0x200A) || c == 0x2028 || c == 0x2029 || c == 0x202F || c == 0x205F ||
           c == 0x3000;
}

// str.strip()
std::string StripText(const std::string& text) {
    std::vector<std::pair<std::size_t, std::size_t>> spans;  // byte offset, length per code point
    std::vector<std::uint32_t> codes;
    for (std::size_t i = 0; i < text.size();) {
        const auto lead = static_cast<unsigned char>(text[i]);
        std::size_t length = lead < 0x80 ? 1 : lead < 0xE0 ? 2 : lead < 0xF0 ? 3 : 4;
        length = std::min(length, text.size() - i);
        std::uint32_t code = length == 1 ? lead : lead & (0x7FU >> length);
        for (std::size_t k = 1; k < length; ++k) {
            code = (code << 6) | (static_cast<unsigned char>(text[i + k]) & 0x3FU);
        }
        spans.emplace_back(i, length);
        codes.push_back(code);
        i += length;
    }
    std::size_t first = 0;
    std::size_t last = codes.size();
    while (first < last && IsSpace(codes[first])) {
        ++first;
    }
    while (last > first && IsSpace(codes[last - 1])) {
        --last;
    }
    if (first == last) {
        return {};
    }
    return text.substr(spans[first].first, spans[last - 1].first + spans[last - 1].second - spans[first].first);
}

// int(text): digits with single underscores between them, optional sign.
std::optional<BigInt> ParsePythonInt(const std::string& raw) {
    std::string text = StripText(raw);
    bool negative = false;
    if (!text.empty() && (text.front() == '+' || text.front() == '-')) {
        negative = text.front() == '-';
        text.erase(0, 1);
    }
    std::string digits;
    for (std::size_t i = 0; i < text.size(); ++i) {
        if (text[i] == '_') {
            if (i == 0 || i + 1 == text.size() || text[i + 1] == '_') {
                return std::nullopt;
            }
            continue;
        }
        if (text[i] < '0' || text[i] > '9') {
            return std::nullopt;
        }
        digits += text[i];
    }
    if (digits.empty()) {
        return std::nullopt;
    }
    std::optional<BigInt> value = BigInt::Parse(digits);
    if (value && negative) {
        value = BigInt(0) - *value;
    }
    return value;
}

// float(text)
std::optional<double> ParsePythonFloat(const std::string& raw) {
    std::string text = StripText(raw);
    bool negative = false;
    if (!text.empty() && (text.front() == '+' || text.front() == '-')) {
        negative = text.front() == '-';
        text.erase(0, 1);
    }
    std::string lower;
    for (const char c : text) {
        lower += (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
    }
    if (lower == "inf" || lower == "infinity") {
        return negative ? -std::numeric_limits<double>::infinity() : std::numeric_limits<double>::infinity();
    }
    if (lower == "nan") {
        return std::numeric_limits<double>::quiet_NaN();
    }
    std::string cleaned;
    bool digits_before = false;
    bool digits_after = false;
    bool seen_point = false;
    bool seen_exponent = false;
    for (std::size_t i = 0; i < lower.size(); ++i) {
        const char c = lower[i];
        if (c == '_') {
            const bool between = i > 0 && i + 1 < lower.size() && lower[i - 1] >= '0' && lower[i - 1] <= '9' &&
                                 lower[i + 1] >= '0' && lower[i + 1] <= '9';
            if (!between) {
                return std::nullopt;
            }
            continue;
        }
        if (c >= '0' && c <= '9') {
            (seen_exponent ? digits_after : digits_before) = true;
        } else if (c == '.' && !seen_point && !seen_exponent) {
            seen_point = true;
        } else if (c == 'e' && !seen_exponent && digits_before) {
            seen_exponent = true;
            if (i + 1 < lower.size() && (lower[i + 1] == '+' || lower[i + 1] == '-')) {
                cleaned += c;
                cleaned += lower[++i];
                continue;
            }
        } else {
            return std::nullopt;
        }
        cleaned += c;
    }
    if (!digits_before || (seen_exponent && !digits_after)) {
        return std::nullopt;
    }
    double value = 0.0;
    const auto result = std::from_chars(cleaned.data(), cleaned.data() + cleaned.size(), value);
    if (result.ptr != cleaned.data() + cleaned.size()) {
        return std::nullopt;
    }
    if (result.ec == std::errc::result_out_of_range) {
        // Large magnitudes become infinity and tiny ones zero, as in Python.
        const std::size_t e = cleaned.find('e');
        const bool tiny = e != std::string::npos && cleaned[e + 1] == '-';
        value = tiny ? 0.0 : std::numeric_limits<double>::infinity();
    }
    return negative ? -value : value;
}

bool LooksLikeNegativeNumber(const std::string& arg) {
    // argparse's '^-\d+$|^-\d*\.\d+$'
    if (arg.size() < 2 || arg[0] != '-') {
        return false;
    }
    const std::string rest = arg.substr(1);
    const std::size_t point = rest.find('.');
    auto all_digits = [](std::string_view s) {
        return !s.empty() && s.find_first_not_of("0123456789") == std::string_view::npos;
    };
    if (point == std::string::npos) {
        return all_digits(rest);
    }
    const std::string_view before = std::string_view(rest).substr(0, point);
    return (before.empty() || all_digits(before)) && all_digits(std::string_view(rest).substr(point + 1));
}

// Whether argparse reads arg as an option string rather than a value.
bool IsOptionLike(const std::string& arg) {
    if (arg.empty() || arg[0] != '-' || arg == "-") {
        return false;
    }
    if (LooksLikeNegativeNumber(arg)) {
        return false;
    }
    if (arg == "--") {
        return true;
    }
    const std::string name = arg.substr(0, arg.find('='));
    for (const OptionSpec& spec : kOptionSpecs) {
        if (spec.name == name || name == "-h") {
            return true;
        }
    }
    if (arg.find(' ') != std::string::npos) {
        return false;
    }
    return true;
}

void ApplyValue(Options& options, std::string_view name, const std::string& value) {
    if (name == "--runs") {
        const std::optional<BigInt> parsed = ParsePythonInt(value);
        if (!parsed) {
            throw UsageError{"argument --runs: invalid int value: " + PyRepr(Value::Str(value))};
        }
        options.runs = *parsed;
    } else if (name == "--budget") {
        const std::optional<double> parsed = ParsePythonFloat(value);
        if (!parsed) {
            throw UsageError{"argument --budget: invalid float value: " + PyRepr(Value::Str(value))};
        }
        options.budget = *parsed;
    } else if (name == "--profiles") {
        options.profiles = value;
    } else if (name == "--watch") {
        options.watch = value;
    } else if (name == "--json") {
        options.json = value;
    } else if (name == "--markdown") {
        options.markdown = value;
    } else if (name == "--window-id") {
        options.window_id = value;
    } else if (name == "--noise-reference") {
        options.noise_reference = value;
    } else if (name == "--relaxed-concurrency") {
        const std::optional<BigInt> parsed = ParsePythonInt(value);
        if (!parsed) {
            throw UsageError{"argument --relaxed-concurrency: invalid int value: " + PyRepr(Value::Str(value))};
        }
        if (!(*parsed == BigInt(4) || *parsed == BigInt(8) || *parsed == BigInt(16))) {
            throw UsageError{"argument --relaxed-concurrency: invalid choice: " +
                             PyRepr(Value::Str(parsed->ToString())) + " (choose from '4', '8', '16')"};
        }
        options.relaxed_concurrency = *parsed;
    }
}

// argparse: options with unique-prefix abbreviations and --name=value, two
// positionals, required --window-id.
Options ParseOptions(const std::vector<std::string>& args) {
    Options options;
    std::vector<std::string> extras;
    bool seen_window = false;
    bool positional_only = false;
    auto add_positional = [&](const std::string& arg) {
        if (!options.before) {
            options.before = arg;
        } else if (!options.after) {
            options.after = arg;
        } else {
            extras.push_back(arg);
        }
    };
    for (std::size_t i = 0; i < args.size(); ++i) {
        const std::string& arg = args[i];
        if (positional_only || !IsOptionLike(arg)) {
            add_positional(arg);
            continue;
        }
        if (arg == "--") {
            positional_only = true;
            continue;
        }
        if (arg == "-h") {
            options.help = true;
            return options;
        }
        if (arg.rfind("--", 0) != 0) {
            extras.push_back(arg);
            continue;
        }
        const std::size_t equals = arg.find('=');
        const std::string name = arg.substr(0, equals);
        std::optional<std::string> explicit_value;
        if (equals != std::string::npos) {
            explicit_value = arg.substr(equals + 1);
        }
        const OptionSpec* spec = nullptr;
        for (const OptionSpec& candidate : kOptionSpecs) {
            if (candidate.name == name) {
                spec = &candidate;
            }
        }
        if (spec == nullptr) {
            std::vector<const OptionSpec*> matches;
            for (const OptionSpec& candidate : kOptionSpecs) {
                if (candidate.name.rfind(name, 0) == 0) {
                    matches.push_back(&candidate);
                }
            }
            if (matches.size() > 1) {
                std::string names;
                for (const OptionSpec* match : matches) {
                    names += (names.empty() ? "" : ", ") + std::string(match->name);
                }
                throw UsageError{"ambiguous option: " + arg + " could match " + names};
            }
            if (matches.empty()) {
                extras.push_back(arg);
                continue;
            }
            spec = matches.front();
        }
        const std::string option(spec->name);
        switch (spec->arity) {
            case Arity::kHelp:
                if (explicit_value) {
                    throw UsageError{"argument -h/--help: ignored explicit argument " +
                                     PyRepr(Value::Str(*explicit_value))};
                }
                options.help = true;
                return options;
            case Arity::kFlag:
                if (explicit_value) {
                    throw UsageError{"argument " + option + ": ignored explicit argument " +
                                     PyRepr(Value::Str(*explicit_value))};
                }
                options.noise_floor = true;
                break;
            case Arity::kOne:
                if (explicit_value) {
                    ApplyValue(options, option, *explicit_value);
                } else if (i + 1 < args.size() && !positional_only && !IsOptionLike(args[i + 1])) {
                    ApplyValue(options, option, args[++i]);
                } else {
                    throw UsageError{"argument " + option + ": expected one argument"};
                }
                break;
            case Arity::kOptional:
                if (explicit_value) {
                    ApplyValue(options, option, *explicit_value);
                } else if (i + 1 < args.size() && !IsOptionLike(args[i + 1])) {
                    ApplyValue(options, option, args[++i]);
                } else {
                    ApplyValue(options, option, "-");
                }
                break;
        }
        seen_window = seen_window || option == "--window-id";
    }
    std::vector<std::string> missing;
    if (!options.before) {
        missing.emplace_back("before");
    }
    if (!options.after) {
        missing.emplace_back("after");
    }
    if (!seen_window) {
        missing.emplace_back("--window-id");
    }
    if (!missing.empty()) {
        std::string names;
        for (const std::string& name : missing) {
            names += (names.empty() ? "" : ", ") + name;
        }
        throw UsageError{"the following arguments are required: " + names};
    }
    if (!extras.empty()) {
        std::string names;
        for (const std::string& extra : extras) {
            names += (names.empty() ? "" : " ") + extra;
        }
        throw UsageError{"unrecognized arguments: " + names};
    }
    return options;
}

std::vector<std::string> Split(const std::string& text, char separator) {
    std::vector<std::string> parts;
    std::size_t start = 0;
    while (true) {
        const std::size_t at = text.find(separator, start);
        parts.push_back(text.substr(start, at == std::string::npos ? std::string::npos : at - start));
        if (at == std::string::npos) {
            return parts;
        }
        start = at + 1;
    }
}

std::string LoadText(const Value& load) {
    // repr() of the list, as the progress line printed it.
    return PyRepr(load);
}

std::string RecordStamp(std::int64_t micros) {
    // YYYYMMDDTHHMMSS.ffffffZ of the comparison start.
    const std::int64_t fraction = ((micros % 1000000) + 1000000) % 1000000;
    const std::string iso = IsoUtc(micros - fraction);
    std::string stamp;
    for (const char c : iso.substr(0, 19)) {
        if (c != '-' && c != ':') {
            stamp += c;
        }
    }
    const std::string digits = std::to_string(1000000 + fraction).substr(1);
    return stamp + "." + digits + "Z";
}

std::string Pair2(std::int64_t pair) { return pair < 10 ? "0" + std::to_string(pair) : std::to_string(pair); }

}  // namespace

std::string Strip(const std::string& text) { return StripText(text); }

int Main(const std::vector<std::string>& args, const Hooks& hooks, std::ostream& out, std::ostream& err) {
    auto usage_error = [&](const std::string& message) {
        err << kUsage << "chunkdb_budget_compare: error: " << message << "\n";
        return 2;
    };
    Options options;
    try {
        options = ParseOptions(args);
    } catch (const UsageError& error) {
        return usage_error(error.message);
    }
    if (options.help) {
        out << kUsage << kHelp;
        return 0;
    }
    const std::vector<std::string> wanted = Split(options.profiles, ',');
    if (Compare(options.runs, BigInt(1)) < 0 || !std::isfinite(options.budget) || options.budget < 0) {
        return usage_error("runs must be positive and budget must be finite/nonnegative");
    }
    bool known = !wanted.empty();
    for (std::size_t i = 0; i < wanted.size(); ++i) {
        const bool listed = std::any_of(kProfiles.begin(), kProfiles.end(),
                                        [&](const Profile& profile) { return profile.name == wanted[i]; });
        const bool repeated = std::find(wanted.begin(), wanted.begin() + static_cast<std::ptrdiff_t>(i), wanted[i]) !=
                              wanted.begin() + static_cast<std::ptrdiff_t>(i);
        known = known && listed && !repeated;
    }
    if (!known) {
        return usage_error("profiles must be distinct known profile names");
    }
    if (options.json == "-" && options.markdown == "-") {
        return usage_error("JSON and Markdown cannot both use stdout; give one an output filename");
    }
    if (options.json && options.markdown && !options.json->empty() && !options.markdown->empty() &&
        *options.json != "-" && *options.markdown != "-" && RealPath(*options.json) == RealPath(*options.markdown)) {
        return usage_error("JSON and Markdown require different output filenames");
    }
    const bool watch_set = options.watch && !options.watch->empty();
    const bool reference_set = options.noise_reference && !options.noise_reference->empty();
    if (StripText(*options.window_id).empty() || (options.noise_floor && (watch_set || reference_set))) {
        return usage_error("window ID required; noise-floor cannot attach WATCH or another reference");
    }
    if (!options.noise_floor && !reference_set) {
        return usage_error("source comparisons require --noise-reference");
    }
    const std::optional<std::int64_t> runs = options.runs.ToInt64();
    if (!runs) {
        return usage_error("runs must be positive and budget must be finite/nonnegative");
    }
    std::vector<Profile> profiles;
    for (const Profile& profile : kProfiles) {
        if (std::find(wanted.begin(), wanted.end(), profile.name) != wanted.end()) {
            Profile chosen = profile;
            if (chosen.name == "relaxed") {
                chosen.clients = *options.relaxed_concurrency.ToInt64();
            }
            profiles.push_back(chosen);
        }
    }
    const std::int64_t started = hooks.now_micros();
    Value reference;
    try {
        if (options.noise_floor) {
            const std::string before_binary = PathJoin(*options.before, BenchBinaryName());
            const std::string after_binary = PathJoin(*options.after, BenchBinaryName());
            if (hooks.file_hash(before_binary) != hooks.file_hash(after_binary)) {
                Fail(ErrorKind::kValue, "Noise control requires identical binaries");
            }
        } else {
            Value settings = Value::Object();
            for (const Profile& profile : profiles) {
                Value entry = Value::Object();
                entry.Set("name", S(profile.name));
                entry.Set("durability_mode", S(profile.mode));
                entry.Set("requests_per_run", I(profile.requests));
                entry.Set("clients", I(profile.clients));
                entry.Set("server_workers", I(profile.clients));
                entry.Set("pipeline", I(1));
                entry.Set("io_timeout_ms", I(kIoTimeoutMs));
                entry.Set("keyspace_blocks_per_axis", I(profile.keyspace));
                entry.Set("runs_per_build", I(*runs));
                entry.Set("watch_before", Value());
                entry.Set("watch_after", Value());
                settings.Set(std::string(profile.name), std::move(entry));
            }
            reference = ReadNoiseReference(*options.noise_reference, *options.window_id, settings, started);
        }
    } catch (const Error& error) {
        if (error.kind() == ErrorKind::kValue || error.kind() == ErrorKind::kKey || error.kind() == ErrorKind::kType ||
            error.kind() == ErrorKind::kOs) {
            return usage_error(error.what());
        }
        err << "chunkdb_budget_compare: " << error.what() << "\n";
        return 1;
    }
    try {
        Value report = Value::Object();
        report.Set("schema_version", I(2));
        report.Set("window_id", Value::Str(*options.window_id));
        report.Set("measurement_kind", S(options.noise_floor ? "noise-floor" : "source-comparison"));
        report.Set("paired_method", BootstrapMethod());
        report.Set("noise_reference", std::move(reference));
        report.Set("started_utc", Value::Str(IsoUtc(started)));
        report.Set("machine", hooks.machine_info());
        report.Set("profiles", Value::Array());
        const std::string records =
            options.json && !options.json->empty() && *options.json != "-"
                ? *options.json + ".runs"
                : PathJoin(TemporaryDirectory(), "chunkdb_budget_compare-" + RecordStamp(started));
        err << "raw benchmark output: " << records << "\n" << std::flush;
        const Value watch = options.watch ? Value::Str(*options.watch) : Value();
        for (const Profile& profile : profiles) {
            Value samples = Value::Object();
            samples.Set("before", Value::Array());
            samples.Set("after", Value::Array());
            for (std::int64_t run = 0; run < *runs; ++run) {
                const std::array<std::string_view, 2> order =
                    run % 2 == 0 ? std::array<std::string_view, 2>{"before", "after"}
                                 : std::array<std::string_view, 2>{"after", "before"};
                for (std::size_t position = 0; position < order.size(); ++position) {
                    const std::string side(order[position]);
                    err << profile.name << ": run" << run + 1 << "/" << *runs << " " << side
                        << " load_average=" << LoadText(hooks.load_average()) << "\n"
                        << std::flush;
                    RunRequest request;
                    request.build_dir = side == "before" ? *options.before : *options.after;
                    request.mode = std::string(profile.mode);
                    request.requests = profile.requests;
                    request.clients = profile.clients;
                    request.keyspace = profile.keyspace;
                    request.seed = 1337 + run;
                    if (side == "after") {
                        request.watch = options.watch;
                    }
                    request.record_prefix =
                        PathJoin(records, std::string(profile.name) + "-pair" + Pair2(run + 1) + "-" + side);
                    Value sample = hooks.run_once(request);
                    sample.Set("pair", I(run + 1));
                    sample.Set("order", I(static_cast<std::int64_t>(position) + 1));
                    samples.Find(side)->Append(std::move(sample));
                }
            }
            report.Find("profiles")->Append(SummarizeProfile(S(profile.name), S(profile.mode), I(profile.requests),
                                                             I(profile.clients), I(profile.keyspace),
                                                             Value::Bool(profile.gates), samples,
                                                             Value::Float(options.budget), watch));
        }
        if (options.noise_floor) {
            std::vector<std::string_view> fields(kBuildFields.begin(), kBuildFields.end());
            fields.push_back("binary_sha256");
            for (const Value& profile : report.Find("profiles")->Items()) {
                const Value& builds = GetItem(profile, "builds");
                for (const std::string_view key : fields) {
                    if (!PyEqual(GetItem(GetItem(builds, "before"), key), GetItem(GetItem(builds, "after"), key))) {
                        Fail(ErrorKind::kValue, "Noise control build provenance differs");
                    }
                }
            }
        }
        report.Set("finished_utc", Value::Str(IsoUtc(hooks.now_micros())));
        bool unknown = false;
        bool all_passed = true;
        for (const Value& profile : report.Find("profiles")->Items()) {
            const Value& passed = GetItem(profile, "passed");
            unknown = unknown || passed.IsNull();
            all_passed = all_passed && Truthy(passed);
        }
        report.Set("passed", unknown ? Value() : Value::Bool(all_passed));
        std::vector<std::pair<std::string, std::string>> outputs;
        if (options.json) {
            outputs.emplace_back(*options.json, DumpJson(report) + "\n");
        }
        if (options.markdown || !options.json) {
            outputs.emplace_back(options.markdown && !options.markdown->empty() ? *options.markdown : "-",
                                 RenderMarkdown(report));
        }
        for (const auto& [destination, text] : outputs) {
            // Text is written as strict UTF-8; a lone surrogate (from \ud800 in
            // a reference) fails as Python's encoder fails, after the file opens.
            const std::optional<std::string> unencodable = Utf8EncodeError(text);
            if (destination == "-") {
                if (unencodable) {
                    Fail(ErrorKind::kValue, *unencodable);
                }
                out << text << std::flush;
            } else {
                if (unencodable) {
                    WriteTextFile(destination, "");
                    Fail(ErrorKind::kValue, *unencodable);
                }
                WriteTextFile(destination, text);
            }
        }
        const Value& passed = GetItem(report, "passed");
        return passed.IsBool() && !passed.AsBool() ? 1 : 0;
    } catch (const Error& error) {
        err << "chunkdb_budget_compare: " << error.what() << "\n";
        return 1;
    }
}

}  // namespace chunkdb::budget_compare
