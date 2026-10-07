#pragma once

#include <filesystem>
#include <optional>
#include <span>
#include <stdexcept>
#include <vector>

#include "chunk_store_internal.hpp"

namespace chunkdb {

[[nodiscard]] std::runtime_error BuildWalOpenError(
    const std::filesystem::path& path,
    int err);
// Stages one mutation as a WAL frame at the end of `batch`. Construct (with
// the mutation's tag, if any), append every changed span, then Finish() with
// the mutation's revision and commit time; a caller that abandons the frame
// truncates the batch back to its previous size (the ordinary rollback path
// already does).
class WalFrameBuilder {
  public:
    explicit WalFrameBuilder(
        std::vector<std::uint8_t>* batch,
        const std::vector<std::uint8_t>& tag = {});

    // Appends one SPAN record writing `size` bytes at `byte_offset` of the
    // chunk state.
    void AppendSpan(std::uint32_t byte_offset, const std::uint8_t* bytes, std::size_t size);
    // Value records (kWalRecordVar*). A frame takes VAR_PUT and VAR_DEL in
    // strictly ascending key order, or one VAR_REPLACE.
    void AppendVarPut(VarKey key, std::span<const std::uint8_t> value);
    void AppendVarDel(VarKey key);
    void AppendVarReplace(const ChunkVars& vars);
    // The records of an applied value update: `vars` is the chunk's values
    // afterwards, `undo` what ApplyVarUpdate returned.
    void AppendVarUpdate(const ChunkVars& vars, const VarUndo& undo);

    // Writes the frame header and trailer; returns the total bytes the frame
    // added to the batch. Requires at least one record.
    [[nodiscard]] std::size_t Finish(std::uint64_t revision, std::uint64_t commit_time_ms);

    [[nodiscard]] std::size_t record_count() const noexcept { return record_count_; }

  private:
    void BeginRecord(std::uint8_t type, std::size_t body_size);
    void RequireVarOrder(VarKey key);

    std::vector<std::uint8_t>* batch_;
    std::size_t header_index_;
    std::size_t records_begin_;
    std::uint16_t tlv_size_ = 0;
    std::size_t record_count_ = 0;
    bool finished_ = false;
    bool has_var_replace_ = false;
    std::optional<VarKey> last_var_key_;
};

}  // namespace chunkdb
