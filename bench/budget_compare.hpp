#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

// chunkdb_budget_compare: compares two builds of chunkdb_server_bench on the
// hot-path budget scenarios (CONTRIBUTING.md, "Hot-path budgets").
//
// Reports are dynamic JSON values with the semantics of the values the earlier
// Python comparator used: integers of any size, floats printed by the shortest
// round trip, object members in insertion order, and cross-type numeric
// comparison. The JSON artifact, the Markdown and the paired statistics are the
// same as that comparator's, so noise references written by either are
// accepted by the other.
namespace chunkdb::budget_compare {

// The failure classes the comparator distinguishes. Before measurement, kValue,
// kKey, kType and kOs are usage errors (exit status 2); every other failure, and
// any failure after measurement starts, exits with status 1.
enum class ErrorKind {
    kValue,
    kKey,
    kType,
    kAttribute,
    kOs,
    kOverflow,
    kZeroDivision,
    kIndex,
    kRuntime,
};

class Error : public std::runtime_error {
  public:
    Error(ErrorKind kind, const std::string& message);
    [[nodiscard]] ErrorKind kind() const noexcept { return kind_; }

  private:
    ErrorKind kind_;
};

// Signed integer of any size.
class BigInt {
  public:
    BigInt() = default;
    BigInt(std::int64_t value);  // NOLINT(google-explicit-constructor)
    BigInt(bool negative, std::vector<std::uint32_t> magnitude);

    // Optional '-' and ASCII digits; std::nullopt for anything else.
    [[nodiscard]] static std::optional<BigInt> Parse(std::string_view text);
    [[nodiscard]] std::string ToString() const;
    [[nodiscard]] std::size_t DecimalDigits() const;

    [[nodiscard]] bool IsZero() const noexcept { return magnitude_.empty(); }
    [[nodiscard]] bool IsNegative() const noexcept { return negative_; }
    [[nodiscard]] bool IsOdd() const noexcept { return !magnitude_.empty() && (magnitude_[0] & 1U) != 0; }
    [[nodiscard]] std::optional<std::int64_t> ToInt64() const noexcept;
    [[nodiscard]] const std::vector<std::uint32_t>& Magnitude() const noexcept { return magnitude_; }

    // float(self): correctly rounded; std::nullopt when too large for a double.
    [[nodiscard]] std::optional<double> ToDouble() const;

    friend int Compare(const BigInt& a, const BigInt& b);
    friend bool operator==(const BigInt& a, const BigInt& b) { return Compare(a, b) == 0; }
    friend BigInt operator+(const BigInt& a, const BigInt& b);
    friend BigInt operator-(const BigInt& a, const BigInt& b);
    friend BigInt operator*(const BigInt& a, const BigInt& b);

  private:
    bool negative_ = false;
    std::vector<std::uint32_t> magnitude_;
};

// a / b correctly rounded to a double (Python's int true division); std::nullopt
// when the quotient is too large for a double. b must not be zero.
[[nodiscard]] std::optional<double> TrueDivide(const BigInt& a, const BigInt& b);
// -1, 0 or 1 comparing an integer with a finite double exactly.
[[nodiscard]] int CompareExact(const BigInt& a, double b);

struct Member;

class Value {
  public:
    enum class Kind { kNull, kBool, kInt, kFloat, kString, kArray, kObject };

    Value() = default;
    static Value Bool(bool value);
    static Value Int(BigInt value);
    static Value Float(double value);
    static Value Str(std::string value);
    static Value Array();
    static Value Array(std::vector<Value> items);
    static Value Object();

    [[nodiscard]] Kind kind() const noexcept { return kind_; }
    [[nodiscard]] bool IsNull() const noexcept { return kind_ == Kind::kNull; }
    [[nodiscard]] bool IsBool() const noexcept { return kind_ == Kind::kBool; }
    [[nodiscard]] bool IsInt() const noexcept { return kind_ == Kind::kInt; }
    [[nodiscard]] bool IsFloat() const noexcept { return kind_ == Kind::kFloat; }
    [[nodiscard]] bool IsString() const noexcept { return kind_ == Kind::kString; }
    [[nodiscard]] bool IsArray() const noexcept { return kind_ == Kind::kArray; }
    [[nodiscard]] bool IsObject() const noexcept { return kind_ == Kind::kObject; }

    [[nodiscard]] bool AsBool() const noexcept { return bool_; }
    [[nodiscard]] const BigInt& AsInt() const noexcept { return int_; }
    [[nodiscard]] double AsFloat() const noexcept { return float_; }
    [[nodiscard]] const std::string& AsString() const noexcept { return string_; }
    [[nodiscard]] const std::vector<Value>& Items() const noexcept { return items_; }
    [[nodiscard]] std::vector<Value>& Items() noexcept { return items_; }
    [[nodiscard]] const std::vector<Member>& Members() const noexcept { return members_; }
    [[nodiscard]] std::vector<Member>& Members() noexcept { return members_; }

    // Object member lookup; nullptr when absent.
    [[nodiscard]] const Value* Find(std::string_view key) const;
    [[nodiscard]] Value* Find(std::string_view key);
    // Replaces an existing member in place or appends a new one.
    Value& Set(const std::string& key, Value value);
    bool Erase(std::string_view key);
    void Append(Value value);

  private:
    Kind kind_ = Kind::kNull;
    bool bool_ = false;
    BigInt int_;
    double float_ = 0.0;
    std::string string_;
    std::vector<Value> items_;
    std::vector<Member> members_;
};

struct Member {
    std::string key;
    Value value;
};

// Strings are WTF-8: UTF-8 that may also encode lone surrogates, as Python
// strings may hold them.

// json.loads of a document already decoded to text.
[[nodiscard]] Value ParseJson(std::string_view text);
// json.loads(bytes): UTF-8 (with or without a BOM), UTF-16 or UTF-32.
[[nodiscard]] Value ParseJsonBytes(std::string_view bytes);
// json.dumps(value, indent=2, allow_nan=False), ASCII-only.
[[nodiscard]] std::string DumpJson(const Value& value);
// Strict UTF-8 decoding with Python's universal newlines, as subprocess text
// mode reads child output.
[[nodiscard]] std::string DecodeChildText(std::string_view bytes);

// Python's message for text that strict UTF-8 cannot encode (lone
// surrogates), or std::nullopt.
[[nodiscard]] std::optional<std::string> Utf8EncodeError(const std::string& text);

// repr() and str() of a value, and repr() of a float.
[[nodiscard]] std::string PyRepr(const Value& value);
[[nodiscard]] std::string PyStr(const Value& value);
[[nodiscard]] std::string FloatRepr(double value);
// format(value, spec) for the specs the Markdown uses: ",.Nf", ".Nf", "+.Nf"
// and ",".
[[nodiscard]] std::string FormatNumber(const Value& value, std::string_view spec);

// Python ==, with numeric comparison across int, float and bool.
[[nodiscard]] bool PyEqual(const Value& a, const Value& b);

// Python's random.Random(seed): MT19937 with Python's init_by_array seeding and
// getrandbits/_randbelow draws.
class PythonRandom {
  public:
    explicit PythonRandom(std::uint64_t seed);
    [[nodiscard]] std::uint32_t NextUint32();
    // getrandbits(k) for 0 <= k <= 32.
    [[nodiscard]] std::uint32_t GetRandBits(int k);
    // randrange(n) for 0 < n < 2**32.
    [[nodiscard]] std::uint32_t RandBelow(std::uint32_t n);

  private:
    std::uint32_t state_[624];
    int index_ = 624;
};

// statistics.median of floats.
[[nodiscard]] double MedianOf(std::vector<double> values);
// statistics.stdev of finite floats: the correctly rounded square root of the
// exact sample variance.
[[nodiscard]] double SampleStdev(const std::vector<double>& values);
// Type 7 linear quantile of sorted values.
[[nodiscard]] double Type7(const std::vector<double>& sorted, double probability);

// The comparator, step by step.
inline constexpr std::int64_t kIoTimeoutMs = 5000;
[[nodiscard]] Value BootstrapMethod();
void ValidateRun(const Value& report, const Value& mode, const Value& requests, const Value& clients,
                 const Value& keyspace, const Value& seed);
[[nodiscard]] Value Summary(const std::vector<Value>& values);
[[nodiscard]] Value PairedStatistics(const Value& samples, std::string_view scenario, const Value& budget);
[[nodiscard]] Value SummarizeProfile(const Value& name, const Value& mode, const Value& requests, const Value& clients,
                                     const Value& keyspace, const Value& gates, const Value& samples,
                                     const Value& budget, const Value& watch, bool watch_both = false);
[[nodiscard]] Value ProfileSettings(const Value& profile);
// started_micros: UTC microseconds since the epoch.
[[nodiscard]] Value ReadNoiseReference(const std::string& filename, const std::string& window_id,
                                       const Value& settings, std::int64_t started_micros);
[[nodiscard]] std::string RenderMarkdown(const Value& report);

// datetime.isoformat() of UTC microseconds since the epoch.
[[nodiscard]] std::string IsoUtc(std::int64_t micros);
// datetime.fromisoformat(): UTC microseconds, or std::nullopt for a naive time.
// Throws kValue for text that is not an ISO 8601 date and time.
[[nodiscard]] std::optional<std::int64_t> ParseIsoTime(const std::string& text);

// One benchmark process.
struct RunRequest {
    std::string build_dir;
    std::string mode;
    std::int64_t requests = 0;
    std::int64_t clients = 0;
    std::int64_t keyspace = 0;
    std::int64_t seed = 0;
    std::optional<std::string> watch;
    // The raw output goes to <record_prefix>.stdout and .stderr and the exit
    // status and UTC times to <record_prefix>.status.json, all written before
    // the output is parsed.
    std::string record_prefix;
};

struct ExitStatus {
    bool exited = true;
    int code = 0;  // the exit code, or the signal number when !exited
};

struct RunEnvironment {
    std::function<std::string(const std::string& path)> file_hash;
    std::function<Value()> load_average;
    std::function<int()> free_port;
    std::function<std::int64_t()> now_micros;
    // Runs command with stdout and stderr written to the two files and waits.
    std::function<ExitStatus(const std::vector<std::string>& command, const std::string& stdout_path,
                             const std::string& stderr_path)>
        spawn;
};

[[nodiscard]] Value RunOnce(const RunRequest& request, const RunEnvironment& environment);

struct Hooks {
    std::function<Value(const RunRequest&)> run_once;
    std::function<Value()> machine_info;
    std::function<Value()> load_average;
    std::function<std::string(const std::string& path)> file_hash;
    std::function<std::int64_t()> now_micros;
};

// Platform services (budget_compare_system.cpp).
[[nodiscard]] RunEnvironment SystemRunEnvironment();
[[nodiscard]] Hooks SystemHooks();
[[nodiscard]] Value MachineInfo();
// [1, 5, 15 minute load averages], or null where the platform has none.
[[nodiscard]] Value LoadAverage();
// Lowercase hex SHA-256 of a file; throws kOs.
[[nodiscard]] std::string FileSha256(const std::string& path);
[[nodiscard]] std::string BytesSha256(std::string_view bytes);
[[nodiscard]] std::int64_t NowMicros();
// Whole file contents; throws kOs with Python's "[Errno N] ..." message.
[[nodiscard]] std::string ReadFileBytes(const std::string& path);
void WriteTextFile(const std::string& path, std::string_view text);
void CreateDirectories(const std::string& path);
// os.path.abspath, os.path.realpath, tempfile.gettempdir and PurePath helpers.
[[nodiscard]] std::string AbsolutePath(const std::string& path);
[[nodiscard]] std::string RealPath(const std::string& path);
[[nodiscard]] std::string TemporaryDirectory();
[[nodiscard]] std::string PathParent(const std::string& path);
[[nodiscard]] std::string PathJoin(const std::string& directory, const std::string& name);
[[nodiscard]] std::string BenchBinaryName();

// str.strip()
[[nodiscard]] std::string Strip(const std::string& text);
// argv after the program name as internal strings (Python's os.fsdecode, or
// the UTF-16 command line on Windows).
[[nodiscard]] std::vector<std::string> CommandLineArguments(int argc, char** argv);

// The command line of the earlier comparator; returns the exit status.
[[nodiscard]] int Main(const std::vector<std::string>& args, const Hooks& hooks, std::ostream& out,
                       std::ostream& err);

}  // namespace chunkdb::budget_compare
