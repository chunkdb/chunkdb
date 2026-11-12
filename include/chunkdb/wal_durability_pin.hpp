#pragma once

#include <cstdint>

namespace chunkdb {

class WalAppendStream;

// A sync-capable native generation; framing and directory durability belong
// to the caller. No pathname is retained or reopened.
class WalDurabilityPin {
  public:
    WalDurabilityPin() noexcept = default;
    ~WalDurabilityPin();
    WalDurabilityPin(const WalDurabilityPin&) = delete;
    WalDurabilityPin& operator=(const WalDurabilityPin&) = delete;
    WalDurabilityPin(WalDurabilityPin&& other) noexcept;
    WalDurabilityPin& operator=(WalDurabilityPin&& other) noexcept;

    [[nodiscard]] bool is_open() const noexcept { return native_ != -1; }
    void Sync() const;

  private:
    friend class WalAppendStream;
    explicit WalDurabilityPin(std::intptr_t native) noexcept : native_(native) {}
    void Close() noexcept;
    std::intptr_t native_ = -1;
};

}  // namespace chunkdb
