#pragma once

#include <chrono>
#include <ios>
#include <new>

namespace chunkdb {

inline constexpr auto kSlowFeedSlotOperation = std::chrono::seconds(1);

// Formatting or writing a diagnostic must not turn completed storage or
// delivery work into a failure.
template <typename Report>
void LogFeedSlotDiagnostic(Report report) {
    try {
        report();
    } catch (const std::bad_alloc&) {
        // The diagnostic could not be formatted.
    } catch (const std::ios_base::failure&) {
        // The diagnostic could not be written.
    }
}

}  // namespace chunkdb
