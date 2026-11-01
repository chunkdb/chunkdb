#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "commit_request_fingerprint.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
using Parameters = std::vector<Bytes>;

void Record(CommitRequestFingerprint& fingerprint, std::string_view text, const Parameters& parameters) {
    std::vector<std::optional<std::string>> frames;
    for (const auto& parameter : parameters) frames.emplace_back(std::string(parameter.begin(), parameter.end()));
    fingerprint.AddStatement(text, frames);
}

crypto::Sha256Digest Statement(std::string_view text, const Parameters& parameters,
    std::optional<std::span<const std::uint8_t>> tag = {}) {
    CommitRequestFingerprint fingerprint;
    Record(fingerprint, text, parameters);
    return fingerprint.Finish(tag);
}

void ExactFraming() {
    // Independent byte fixture: domain, statement kind, text length/text,
    // two parameters (binary and empty), then a present one-byte tag.
    const Bytes fixture{
        'c','h','u','n','k','d','b','.','c','o','m','m','i','t','.','r','e','q','u','e','s','t',0,1,
        1, 3,0,0,0,0,0,0,0, 'S','E','T', 2,0,0,0,0,0,0,0,
        1, 2,0,0,0,0,0,0,0, 0,255, 1, 0,0,0,0,0,0,0,0,
        2,1, 1,0,0,0,0,0,0,0, 5};
    const Bytes tag{5U};
    assert(Statement("SET", {{0U, 255U}, {}}, tag) == crypto::Sha256Of(fixture));
    assert(Statement("SET", {{0U, 255U}, {}}, tag) == Statement("SET", {{0U, 255U}, {}}, tag));
}

void DistinctRequests() {
    assert(Statement("SET", {{'a'}, {'b','c'}}) != Statement("SET", {{'a','b'}, {'c'}}));
    assert(Statement("SET", {{0x11U}, {1U, 0x22U}}) != Statement("SET", {{0x11U, 1U}, {0x22U}}));
    assert(Statement("SET", {}) != Statement("SET", {{}}));
    assert(Statement("SET", {{0U, 1U}}) != Statement("SET", {{0U}, {1U}}));
    assert(Statement("SET", {{1U}}) != Statement("SET", {{1U, 0U}}));
    assert(Statement("SET", {}) != Statement("set", {}));
    assert(Statement("SET ", {}) != Statement("SET", {}));
    const Bytes empty;
    const Bytes tag{0U, 1U};
    assert(Statement("SET", {}) != Statement("SET", {}, empty));
    assert(Statement("SET", {}, empty) != Statement("SET", {}, tag));
    const std::array<char, 4> binary_text{'S', 'E', 'T', '\0'};
    assert(Statement(std::string_view{binary_text.data(), binary_text.size()}, {}) != Statement("SET", {}));
}

void TransactionTranscript() {
    CommitRequestFingerprint first;
    Record(first, "BEGIN ID $1", {{1U}});
    const auto begin_only = first.Finish();
    Record(first, "SET BLOCK 1 2 IN world n = $1", {{3U}});
    const auto body = first.Finish();
    assert(begin_only != body);
    // Finishing a probe cannot consume or reset the in-flight transcript.
    assert(first.Finish() == body);
    first.AddStatement("COMMIT", {});
    assert(first.Finish() != body);

    CommitRequestFingerprint changed;
    Record(changed, "BEGIN ID $1", {{1U}});
    Record(changed, "SET BLOCK 1 2 IN world n = $1", {{4U}});
    changed.AddStatement("COMMIT", {});
    assert(first.Finish() != changed.Finish());

    CommitRequestFingerprint split;
    split.AddStatement("a", {});
    split.AddStatement("b", {});
    assert(split.Finish() != Statement("ab", {}));
    CommitRequestFingerprint reversed;
    reversed.AddStatement("b", {});
    reversed.AddStatement("a", {});
    assert(split.Finish() != reversed.Finish());
    CommitRequestFingerprint empty;
    assert(empty.Finish() != Statement("", {}));
}

void NullParameters() {
    const std::vector<std::optional<std::string>> null_parameter{std::nullopt};
    const std::vector<std::optional<std::string>> empty_parameter{std::string{}};
    CommitRequestFingerprint null_write, empty_write;
    null_write.AddStatement("SET BLOCK 1 2 IN world label = $1", null_parameter);
    empty_write.AddStatement("SET BLOCK 1 2 IN world label = $1", empty_parameter);
    assert(null_write.Finish() != empty_write.Finish());

    CommitRequestFingerprint null_transaction;
    Record(null_transaction, "BEGIN ID $1", {{1U}});
    auto empty_transaction = null_transaction;
    null_transaction.AddStatement("SET BLOCK 1 2 IN world label = $1", null_parameter);
    empty_transaction.AddStatement("SET BLOCK 1 2 IN world label = $1", empty_parameter);
    null_transaction.AddStatement("COMMIT", {});
    empty_transaction.AddStatement("COMMIT", {});
    assert(null_transaction.Finish() != empty_transaction.Finish());
    const std::vector<std::optional<std::string>> null_first{std::nullopt, std::string{}};
    const std::vector<std::optional<std::string>> null_last{std::string{}, std::nullopt};
    CommitRequestFingerprint first, last;
    first.AddStatement("SET BLOCK 1 2 IN world a = $1, b = $2", null_first);
    last.AddStatement("SET BLOCK 1 2 IN world a = $1, b = $2", null_last);
    assert(first.Finish() != last.Finish());
}

template <typename Function> void RejectFraming(Function&& function) {
    bool rejected = false;
    try { function(); }
    catch (const std::logic_error&) { rejected = true; }
    assert(rejected);
}

void StreamedDiscardedBodies() {
    std::string payload(65537U, '\0');
    for (std::size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<char>(i % 251U);
    const std::vector<std::optional<std::string>> parameters{payload, std::nullopt, std::string{}};
    CommitRequestFingerprint complete, streamed, no_body;
    for (auto* fingerprint : {&complete, &streamed, &no_body}) Record(*fingerprint, "BEGIN ID $1", {{7U}});
    constexpr std::string_view text = "SET BLOCK 1 2 IN world a = $1, b = $2, c = $3";
    complete.AddStatement(text, parameters);
    streamed.BeginStatement(text, parameters.size());
    RejectFraming([&] { (void)streamed.Finish(); });
    streamed.BeginParameter(payload.size());
    for (std::size_t at = 0; at < payload.size();) {
        const auto bytes = std::min<std::size_t>(4096U, payload.size() - at);
        streamed.ParameterBytes(crypto::Bytes(payload).subspan(at, bytes));
        at += bytes;
    }
    RejectFraming([&] { (void)streamed.Finish(); });
    streamed.BeginParameter(std::nullopt);
    streamed.BeginParameter(0U);
    streamed.ParameterBytes({});
    for (auto* fingerprint : {&complete, &streamed, &no_body}) fingerprint->AddStatement("COMMIT", {});
    assert(streamed.Finish() == complete.Finish());
    assert(streamed.Finish() != no_body.Finish());
    const Bytes tag{0U, 255U};
    assert(streamed.Finish(tag) == complete.Finish(tag));
}

void IncompleteAndInvalidFraming() {
    const Bytes one{1U}, two{1U, 2U};
    CommitRequestFingerprint incomplete;
    incomplete.BeginStatement("SET", 1U);
    incomplete.BeginParameter(2U);
    incomplete.ParameterBytes(one);
    RejectFraming([&] { (void)incomplete.Finish(); });
    incomplete.ParameterBytes(one);
    assert(incomplete.Finish() == Statement("SET", {{1U, 1U}}));

    CommitRequestFingerprint overrun;
    overrun.BeginStatement("SET", 1U);
    overrun.BeginParameter(1U);
    RejectFraming([&] { overrun.ParameterBytes(two); });
    RejectFraming([&] { overrun.ParameterBytes(one); });
    RejectFraming([&] { overrun.AddStatement("COMMIT", {}); });
    RejectFraming([&] { (void)overrun.Finish(); });

    CommitRequestFingerprint extra;
    extra.BeginStatement("SET", 0U);
    RejectFraming([&] { extra.BeginParameter(std::nullopt); });
    RejectFraming([&] { (void)extra.Finish(); });

    CommitRequestFingerprint missing;
    missing.BeginStatement("SET", 1U);
    RejectFraming([&] { missing.BeginStatement("COMMIT", 0U); });
    RejectFraming([&] { (void)missing.Finish(); });

    CommitRequestFingerprint overlapping;
    overlapping.BeginStatement("SET", 2U);
    overlapping.BeginParameter(1U);
    RejectFraming([&] { overlapping.BeginParameter(0U); });
    RejectFraming([&] { (void)overlapping.Finish(); });

    CommitRequestFingerprint suffix;
    suffix.AddStatement("SET", {});
    RejectFraming([&] { suffix.ParameterBytes(one); });
    RejectFraming([&] { (void)suffix.Finish(); });
}
}  // namespace

int main() {
    ExactFraming();
    DistinctRequests();
    TransactionTranscript();
    NullParameters();
    StreamedDiscardedBodies();
    IncompleteAndInvalidFraming();
}
