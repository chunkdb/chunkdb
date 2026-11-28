#include "server_bench_system.hpp"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string_view>
#include <system_error>

#ifndef _WIN32
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
#endif

namespace chunkdb::server_bench {
namespace {

using Clock = std::chrono::steady_clock;
using Values = std::vector<std::pair<std::string, double>>;

#ifdef __linux__
std::optional<std::string> ReadText(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) return std::nullopt;
    std::ostringstream text;
    text << input.rdbuf();
    if (input.bad()) return std::nullopt;
    return text.str();
}

std::vector<std::string> Words(std::string_view line) {
    std::vector<std::string> words;
    std::istringstream input{std::string(line)};
    for (std::string word; input >> word;) words.push_back(word);
    return words;
}

// A counter that does not parse reads as 0.
double Number(std::string_view text) {
    std::uint64_t value = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    return parsed.ec == std::errc{} ? static_cast<double>(value) : 0.0;
}
#endif

const double* Find(const Values& values, const std::string& name) {
    for (const auto& [key, value] : values)
        if (key == name) return &value;
    return nullptr;
}

std::string Format(const std::string& name, double value) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(name.ends_with("_s") ? 6 : name.ends_with("_ms") ? 3 : 0) << value;
    return out.str();
}

#ifndef _WIN32
double Seconds(const timeval& value) {
    return static_cast<double>(value.tv_sec) + static_cast<double>(value.tv_usec) / 1e6;
}

void ReadRusage(SystemSample* sample) {
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) != 0) return;
    auto& c = sample->counters;
    c.emplace_back("process_cpu_user_s", Seconds(usage.ru_utime));
    c.emplace_back("process_cpu_system_s", Seconds(usage.ru_stime));
    c.emplace_back("process_voluntary_switches", static_cast<double>(usage.ru_nvcsw));
    c.emplace_back("process_involuntary_switches", static_cast<double>(usage.ru_nivcsw));
    c.emplace_back("process_major_faults", static_cast<double>(usage.ru_majflt));
    c.emplace_back("process_minor_faults", static_cast<double>(usage.ru_minflt));
    c.emplace_back("process_block_in", static_cast<double>(usage.ru_inblock));
    c.emplace_back("process_block_out", static_cast<double>(usage.ru_oublock));
#ifdef __APPLE__
    const double max_rss_kib = static_cast<double>(usage.ru_maxrss) / 1024.0;
#else
    const double max_rss_kib = static_cast<double>(usage.ru_maxrss);
#endif
    sample->levels.emplace_back("process_max_rss_kib", max_rss_kib);
}
#endif

#ifdef __linux__
double TicksPerSecond() {
    const long ticks = sysconf(_SC_CLK_TCK);
    return ticks > 0 ? static_cast<double>(ticks) : 100.0;
}

void ReadProcessIo(SystemSample* sample) {
    const auto text = ReadText("/proc/self/io");
    if (!text) return;
    std::istringstream lines(*text);
    for (std::string line; std::getline(lines, line);) {
        const auto words = Words(line);
        if (words.size() != 2U) continue;
        if (words[0] == "read_bytes:") sample->counters.emplace_back("process_read_bytes", Number(words[1]));
        if (words[0] == "write_bytes:") sample->counters.emplace_back("process_write_bytes", Number(words[1]));
        if (words[0] == "cancelled_write_bytes:")
            sample->counters.emplace_back("process_cancelled_write_bytes", Number(words[1]));
    }
}

struct ThreadStat {
    std::uint64_t start_ticks = 0U;
    double blkio_s = 0.0;
};

std::optional<ThreadStat> ReadThreadStat(const std::filesystem::path& path, double ticks) {
    const auto stat = ReadText(path);
    if (!stat) return std::nullopt;
    const auto paren = stat->rfind(')');
    if (paren == std::string::npos) return std::nullopt;
    const auto fields = Words(std::string_view(*stat).substr(paren + 1U));
    // The remaining fields begin at field 3. Fields 22 and 42 are the
    // thread's start time and block I/O delay, both in clock ticks.
    if (fields.size() <= 19U) return std::nullopt;
    const auto& start = fields[19];
    ThreadStat result;
    const auto parsed = std::from_chars(start.data(), start.data() + start.size(), result.start_ticks);
    if (parsed.ec != std::errc{} || parsed.ptr != start.data() + start.size()) return std::nullopt;
    if (fields.size() > 39U) result.blkio_s = Number(fields[39]) / ticks;
    return result;
}

void ReadThreads(SystemSample* sample) {
    std::error_code ec;
    const double ticks = TicksPerSecond();
    for (std::filesystem::directory_iterator it("/proc/self/task", ec), end; !ec && it != end; it.increment(ec)) {
        const auto stat_path = it->path() / "stat";
        const auto before = ReadThreadStat(stat_path, ticks);
        if (!before) continue;
        // schedstat reports on-CPU and run-queue nanoseconds. Verify the
        // thread's identity on both sides of that read in case its TID is reused.
        const auto schedstat = ReadText(it->path() / "schedstat");
        if (!schedstat) continue;
        const auto after = ReadThreadStat(stat_path, ticks);
        if (!after || before->start_ticks != after->start_ticks) continue;
        const auto times = Words(*schedstat);
        if (times.size() < 2U) continue;
        SystemSample::Thread thread{
            .cpu_s = Number(times[0]) / 1e9,
            .runqueue_s = Number(times[1]) / 1e9,
            .blkio_s = after->blkio_s,
            .start_ticks = after->start_ticks,
        };
        sample->threads.emplace_back(it->path().filename().string(), thread);
    }
}

void ReadCpu(SystemSample* sample) {
    const auto text = ReadText("/proc/stat");
    if (!text) return;
    const auto words = Words(text->substr(0, text->find('\n')));
    if (words.size() < 9U || words[0] != "cpu") return;
    const double ticks = TicksPerSecond();
    auto field = [&](std::size_t index) { return Number(words[index]) / ticks; };
    auto& c = sample->counters;
    c.emplace_back("machine_cpu_user_s", field(1) + field(2));
    c.emplace_back("machine_cpu_system_s", field(3) + field(6) + field(7));
    c.emplace_back("machine_cpu_idle_s", field(4));
    c.emplace_back("machine_cpu_iowait_s", field(5));
    c.emplace_back("machine_cpu_steal_s", field(8));
}

void ReadPressure(SystemSample* sample) {
    for (const char* resource : {"cpu", "memory", "io"}) {
        const auto text = ReadText(std::string("/proc/pressure/") + resource);
        if (!text) continue;
        std::istringstream lines(*text);
        for (std::string line; std::getline(lines, line);) {
            const auto words = Words(line);
            if (words.empty() || (words[0] != "some" && words[0] != "full")) continue;
            for (const auto& word : words)
                if (word.starts_with("total="))
                    sample->counters.emplace_back(
                        std::string("psi_") + resource + "_" + words[0] + "_s", Number(word.substr(6)) / 1e6);
        }
    }
}

void ReadMemory(SystemSample* sample) {
    if (const auto text = ReadText("/proc/vmstat")) {
        std::istringstream lines(*text);
        for (std::string line; std::getline(lines, line);) {
            const auto words = Words(line);
            if (words.size() != 2U) continue;
            const auto& name = words[0];
            if (name == "pgpgin" || name == "pgpgout" || name == "pswpin" || name == "pswpout" ||
                name == "pgmajfault" || name == "nr_dirtied" || name == "nr_written")
                sample->counters.emplace_back("vm_" + name, Number(words[1]));
            if (name == "nr_dirty" || name == "nr_writeback" || name == "nr_dirty_threshold" ||
                name == "nr_dirty_background_threshold")
                sample->levels.emplace_back("vm_" + name, Number(words[1]));
        }
    }
    if (const auto text = ReadText("/proc/meminfo")) {
        double swap_total = 0.0;
        double swap_free = 0.0;
        std::istringstream lines(*text);
        for (std::string line; std::getline(lines, line);) {
            const auto words = Words(line);
            if (words.size() < 2U) continue;
            if (words[0] == "MemAvailable:") sample->levels.emplace_back("mem_available_kib", Number(words[1]));
            if (words[0] == "SwapTotal:") swap_total = Number(words[1]);
            if (words[0] == "SwapFree:") swap_free = Number(words[1]);
        }
        sample->levels.emplace_back("mem_swap_used_kib", swap_total - swap_free);
    }
}

// Block devices; flushes need Linux 5.5 or later.
void ReadDisks(SystemSample* sample) {
    const auto text = ReadText("/proc/diskstats");
    if (!text) return;
    std::istringstream lines(*text);
    for (std::string line; std::getline(lines, line);) {
        const auto words = Words(line);
        if (words.size() < 14U || words[2].starts_with("loop") || words[2].starts_with("ram")) continue;
        const std::string prefix = "disk_" + words[2] + "_";
        auto& c = sample->counters;
        c.emplace_back(prefix + "writes", Number(words[7]));
        c.emplace_back(prefix + "write_sectors", Number(words[9]));
        c.emplace_back(prefix + "write_ms", Number(words[10]));
        c.emplace_back(prefix + "io_ms", Number(words[12]));
        if (words.size() >= 20U) {
            c.emplace_back(prefix + "flushes", Number(words[18]));
            c.emplace_back(prefix + "flush_ms", Number(words[19]));
        }
    }
}
#endif

void ReadEngine(const SystemWindow::EngineStats& engine, SystemSample* sample) {
    if (engine) sample->engine = engine();
    if (!sample->engine) return;
    const auto& e = *sample->engine;
    auto& c = sample->counters;
    c.emplace_back("engine_checkpoints", static_cast<double>(e.checkpoints));
    c.emplace_back("engine_background_checkpoints", static_cast<double>(e.background_checkpoints));
    c.emplace_back("engine_evictions", static_cast<double>(e.evictions));
    c.emplace_back("engine_wal_batch_flushes", static_cast<double>(e.wal_batch_flushes));
    c.emplace_back("engine_wal_barriers", static_cast<double>(e.wal_barriers));
    c.emplace_back("engine_version_clock_extensions", static_cast<double>(e.version_clock_extensions));
    c.emplace_back("engine_version_clock_extension_s", static_cast<double>(e.version_clock_extension_us) / 1e6);
    c.emplace_back("engine_version_clock_wait_s", static_cast<double>(e.version_clock_wait_us) / 1e6);
}

SystemSample Read(const SystemWindow::EngineStats& engine) {
    SystemSample sample;
#ifndef _WIN32
    ReadRusage(&sample);
#endif
#ifdef __linux__
    ReadProcessIo(&sample);
    ReadThreads(&sample);
    ReadCpu(&sample);
    ReadPressure(&sample);
    ReadMemory(&sample);
    ReadDisks(&sample);
    if (const auto text = ReadText("/proc/sys/kernel/task_delayacct")) {
        const auto words = Words(*text);
        sample.levels.emplace_back("threads_delayacct", words.empty() ? 0.0 : Number(words[0]));
    }
#endif
    ReadEngine(engine, &sample);
    sample.at = Clock::now();
    return sample;
}

}  // namespace

void SystemSamplerGaps::Observe(Clock::time_point at) {
    const double gap_ms = std::chrono::duration<double, std::milli>(at - last_).count();
    last_ = at;
    max_gap_ms_ = std::max(max_gap_ms_, gap_ms);
    if (gap_ms > 100.0) ++gaps_over_100ms_;
}

SystemWindow::SystemWindow(EngineStats engine) : engine_(std::move(engine)), start_(Read(engine_)), gaps_(start_.at) {
    sampler_ = std::jthread([this] {
        std::unique_lock lock(mutex_);
        while (!stop_changed_.wait_for(lock, std::chrono::milliseconds(10), [this] { return stop_; })) {
            gaps_.Observe(Clock::now());
        }
    });
}

SystemWindow::~SystemWindow() { StopSampler(); }

void SystemWindow::StopSampler() {
    {
        std::lock_guard lock(mutex_);
        if (stop_) return;
        // A stalled sampler may never have reached its last timer wake-up.
        gaps_.Observe(Clock::now());
        stop_ = true;
    }
    stop_changed_.notify_one();
}

std::vector<std::pair<std::string, std::string>> SystemWindow::Finish() {
    // StopSampler freezes the gap totals before the final snapshot.
    StopSampler();
    auto out = SystemStatsBetween(start_, Read(engine_));
    out.emplace_back("sampler_max_gap_ms", Format("sampler_max_gap_ms", gaps_.max_gap_ms()));
    out.emplace_back("sampler_gaps_over_100ms", Format("sampler_gaps_over_100ms", static_cast<double>(gaps_.gaps_over_100ms())));
    return out;
}

std::vector<std::pair<std::string, std::string>> SystemStatsBetween(const SystemSample& start, const SystemSample& end) {
    const double window_s = std::chrono::duration<double>(end.at - start.at).count();
    std::vector<std::pair<std::string, std::string>> out;
    auto add = [&](const std::string& name, double value) { out.emplace_back(name, Format(name, value)); };

    add("window_s", window_s);
    // A device is reported when one of its counters moved in the window.
    auto disk_moved = [&](const std::string& name) {
        const auto prefix = name.substr(0, name.find('_', 5U) + 1U);
        for (const auto& [other, value] : end.counters) {
            if (!other.starts_with(prefix)) continue;
            const auto* before = Find(start.counters, other);
            if (before != nullptr && value != *before) return true;
        }
        return false;
    };
    for (const auto& [name, value] : end.counters) {
        const auto* before = Find(start.counters, name);
        if (before == nullptr) continue;
        if (name.starts_with("disk_") && !disk_moved(name)) continue;
        add(name, value - *before);
    }
    for (const auto& [name, value] : end.levels) {
        if (const auto* before = Find(start.levels, name)) add(name + "_start", *before);
        add(name + "_end", value);
    }
    if (!end.threads.empty()) {
        // Threads alive at both ends of the window; off-CPU time includes
        // idle waits for work as well as lock and I/O waits.
        std::size_t measured = 0;
        double cpu_s = 0.0, runqueue_s = 0.0, blkio_s = 0.0, off_cpu_s = 0.0;
        for (const auto& [tid, thread] : end.threads) {
            const auto before = std::find_if(start.threads.begin(), start.threads.end(), [&](const auto& entry) {
                return entry.first == tid && entry.second.start_ticks == thread.start_ticks;
            });
            if (before == start.threads.end()) continue;
            ++measured;
            const double cpu = thread.cpu_s - before->second.cpu_s;
            const double runqueue = thread.runqueue_s - before->second.runqueue_s;
            cpu_s += cpu;
            runqueue_s += runqueue;
            blkio_s += thread.blkio_s - before->second.blkio_s;
            off_cpu_s += std::max(0.0, window_s - cpu - runqueue);
        }
        add("threads_measured", static_cast<double>(measured));
        add("threads_cpu_s", cpu_s);
        add("threads_runqueue_s", runqueue_s);
        add("threads_off_cpu_s", off_cpu_s);
        add("threads_blkio_s", blkio_s);
    }
    return out;
}

}  // namespace chunkdb::server_bench
