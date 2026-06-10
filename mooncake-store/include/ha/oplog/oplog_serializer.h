// mooncake-store/include/oplog_serializer.h
#pragma once

#include <string>

#include "ha/oplog/oplog_manager.h"

namespace mooncake {

// Serialize an OpLogEntry to JSON string (with base64-encoded payload).
// Format is backend-agnostic; all storage backends should use this.
std::string SerializeOpLogEntry(const OpLogEntry& entry);

// Deserialize a JSON string to OpLogEntry.
// Returns true on success, false on parse error or size validation failure.
bool DeserializeOpLogEntry(const std::string& json_str, OpLogEntry& entry);

// Stage 2: compute the batch checksum over the stable header fields of the
// batch and the stable fields of each entry. Per-entry payload integrity is
// already guarded by OpLogEntry::checksum and is intentionally NOT mixed in
// here a second time. Callers MUST call this and assign the result to
// batch.batch_checksum before serializing, so that DeserializeOpLogBatchRecord
// can recompute and verify it.
uint32_t ComputeOpLogBatchChecksum(const OpLogBatchRecord& batch);

// Stage 2: serialize an OpLogBatchRecord to a JSON string. Per-entry payloads
// are base64-encoded (same convention as SerializeOpLogEntry). The caller is
// expected to have set batch.batch_checksum via ComputeOpLogBatchChecksum;
// the serializer stores whatever value is present so that callers can also
// drive negative tests (poisoning the checksum, etc.).
std::string SerializeOpLogBatchRecord(const OpLogBatchRecord& batch);

// Stage 2: deserialize an OpLogBatchRecord from a JSON string. Returns true
// only when every contract holds:
//   - JSON parses cleanly;
//   - every per-entry size passes OpLogManager::ValidateEntrySize;
//   - entry local_index values are contiguous from 0 to entries.size() - 1;
//   - the stored batch_checksum equals the recomputed
//     ComputeOpLogBatchChecksum over the parsed fields.
// On any contract violation the function returns false and `batch` is left
// in an unspecified-but-safe state.
bool DeserializeOpLogBatchRecord(const std::string& json_str,
                                 OpLogBatchRecord& batch);

}  // namespace mooncake
