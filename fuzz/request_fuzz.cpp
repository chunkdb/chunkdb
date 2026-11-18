#include "fuzz_common.hpp"
#include "cql.hpp"
#include "scram.hpp"
#include "server_io.hpp"

namespace {
// Feed the actual connection buffer/framing reader in small fragments. EOF
// is explicit and incomplete lines/bodies never become executable requests.
class Input {
  public:
    Input(std::string_view bytes, std::size_t fragment) : bytes_(bytes), fragment_(fragment) {}
    bool Line(std::string& line) {
        while (!pending_.extract_line(&line, 65536U)) {
            if (!Fill()) return false;
            pending_.enforce_partial_line_limit(65536U);
        }
        return true;
    }
    bool Frames(const chunkdb::server_detail::ParameterFrameRules& rules,
                std::vector<std::optional<std::string>>* parameters, bool* total_exceeded) {
        chunkdb::server_detail::ParameterFrameDecoder decoder(rules);
        while (!decoder.Consume(pending_, 65536U)) {
            if (!Fill()) return false;
        }
        *total_exceeded = decoder.total_exceeded();
        *parameters = decoder.TakeParameters();
        return true;
    }
  private:
    bool Fill() {
        if (cursor_ == bytes_.size()) return false;
        const auto count = std::min(fragment_, bytes_.size() - cursor_);
        pending_.append(bytes_.data() + cursor_, count); cursor_ += count;
        return true;
    }
    std::string_view bytes_; std::size_t fragment_, cursor_ = 0;
    chunkdb::server_detail::PendingLineBuffer pending_;
};
void Parse(std::string_view bytes, std::size_t fragment) {
    Input input(bytes, fragment);
    std::string line;
    while (input.Line(line)) {
        const bool hello = line.starts_with("HELLO ");
        const bool auth = line.starts_with("AUTH ");
        std::vector<std::size_t> limits;
        if ((hello || auth) && line.find("$1") != std::string::npos) limits = {1024};
        else {
            const auto parsed = chunkdb::cql::Parse(line);
            // A bounded schema's largest column body. The same admission
            // reader consumes the exact supplied per-column limits in server.
            limits.assign(parsed.parameters, 64U);
        }
        std::vector<std::optional<std::string>> parameters;
        // A statement-wide byte bound small enough for two parameters to cross.
        bool total_exceeded = false;
        const chunkdb::server_detail::ParameterFrameRules rules{
            .frame_count = limits.size(), .limits = limits, .total_limit = 128U};
        if (!input.Frames(rules, &parameters, &total_exceeded) || total_exceeded) return;
        if (parameters.empty() || !parameters.front()) continue;
        if (hello) (void)chunkdb::scram::ParseClientFirst(*parameters.front());
        if (auth) {
            // Server proof validation, with no peer-controlled PBKDF work.
            const auto verifier = chunkdb::scram::DecoyVerifier(chunkdb::fuzz::kId, "user");
            chunkdb::scram::ServerExchange exchange(
                chunkdb::scram::ParseClientFirst("n,,n=user,r=client"), verifier, "server");
            (void)exchange.Finish(*parameters.front());
        }
    }
}
}
extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size > chunkdb::fuzz::kMaxInput) return 0;
    const auto text = chunkdb::fuzz::Text(data, size);
    chunkdb::fuzz::Rejectable([&] { Parse(text, 1); });
    chunkdb::fuzz::Rejectable([&] { Parse(text, 4096); });
    return 0;
}
