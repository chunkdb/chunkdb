#include "server_io.hpp"

namespace chunkdb::server_detail {

ParameterFrameDecoder::ParameterFrameDecoder(ParameterFrameRules rules) : rules_(rules) {
    if ((!rules.discard && rules.limits.size() != rules.frame_count) ||
        (rules.opaque != nullptr && !rules.opaque->empty() && rules.opaque->size() != rules.frame_count) ||
        (rules.rejections != nullptr && !rules.rejections->empty() && rules.rejections->size() != rules.frame_count)) {
        throw std::invalid_argument("parameter frame rules have inconsistent counts");
    }
    if (!rules.discard) parameters_.reserve(rules.frame_count);
}

void ParameterFrameDecoder::FinishFrame() {
    if (!rules_.discard) {
        if (opaque_) parameters_.emplace_back(std::nullopt);
        else parameters_.emplace_back(std::move(value_));
    }
    value_.clear();
    ++frame_;
    phase_ = Phase::kHeader;
}

bool ParameterFrameDecoder::Consume(PendingLineBuffer& pending, std::size_t max_line_bytes) {
    while (!complete()) {
        if (phase_ == Phase::kHeader) {
            std::string header;
            if (!pending.extract_line(&header, max_line_bytes)) {
                pending.enforce_partial_line_limit(max_line_bytes);
                return false;
            }
            const auto length = Protocol::ParseFrameHeader(header);
            if (!length) {
                if (!rules_.discard) parameters_.emplace_back(std::nullopt);
                ++frame_;
                continue;
            }
            const bool option_too_long = !rules_.discard && *length > rules_.limits[frame_] &&
                rules_.rejections != nullptr && !rules_.rejections->empty() &&
                !(*rules_.rejections)[frame_].empty();
            if (option_too_long) rejection_ = (*rules_.rejections)[frame_];
            opaque_ = rules_.discard || option_too_long ||
                (rules_.opaque != nullptr && !rules_.opaque->empty() && (*rules_.opaque)[frame_]);
            if (opaque_ && *length > rules_.opaque_limit - opaque_total_)
                throw std::invalid_argument("unparsed parameter bytes exceed the configured limit");
            if (opaque_) opaque_total_ += *length;
            if (!opaque_ && *length > rules_.limits[frame_])
                throw std::invalid_argument("$" + std::to_string(frame_ + 1U) +
                    " is longer than its column holds (" + std::to_string(rules_.limits[frame_]) + " bytes)");
            if (*length > rules_.total_limit - total_) {
                total_exceeded_ = true;
                return true;
            }
            total_ += *length;
            remaining_ = *length;
            if (!opaque_) value_.reserve(*length);
            phase_ = Phase::kBody;
        }
        if (phase_ == Phase::kBody) {
            const auto consumed = opaque_ ? pending.discard_bytes(remaining_) : pending.extract_bytes(remaining_, &value_);
            remaining_ -= consumed;
            if (remaining_ != 0U) return false;
            phase_ = Phase::kTerminator;
        }
        if (phase_ == Phase::kTerminator) {
            std::string terminator;
            if (!pending.extract_line(&terminator, max_line_bytes)) {
                pending.enforce_partial_line_limit(max_line_bytes);
                return false;
            }
            if (terminator != "\r\n" && terminator != "\n")
                throw std::invalid_argument("a parameter must be followed by an empty line");
            FinishFrame();
        }
    }
    return true;
}

}  // namespace chunkdb::server_detail
