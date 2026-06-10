#include "ha/oplog/oplog_serializer.h"

#include <glog/logging.h>
#include <xxhash.h>

#include <cstring>
#include <sstream>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "utils/base64.h"

namespace mooncake {

namespace {

// Stage 2 helpers for stable, byte-deterministic batch checksums.
//
// We deliberately do NOT round-trip through JSON for checksum purposes:
// JsonCpp does not guarantee key ordering or numeric encoding to be
// byte-identical across builds, and we need the checksum computed by the
// producer (Stage 4 LogWriter) to match the one recomputed by the consumer
// (Stage 5 strict replay) on every platform. Packing into a little-endian
// byte buffer is unambiguous and cheap.

inline void AppendU32(std::string& buf, uint32_t v) {
    char bytes[4];
    bytes[0] = static_cast<char>(v & 0xFF);
    bytes[1] = static_cast<char>((v >> 8) & 0xFF);
    bytes[2] = static_cast<char>((v >> 16) & 0xFF);
    bytes[3] = static_cast<char>((v >> 24) & 0xFF);
    buf.append(bytes, sizeof(bytes));
}

inline void AppendU64(std::string& buf, uint64_t v) {
    char bytes[8];
    for (int i = 0; i < 8; ++i) {
        bytes[i] = static_cast<char>((v >> (i * 8)) & 0xFF);
    }
    buf.append(bytes, sizeof(bytes));
}

inline void AppendI64(std::string& buf, int64_t v) {
    AppendU64(buf, static_cast<uint64_t>(v));
}

inline void AppendU8(std::string& buf, uint8_t v) {
    buf.append(1, static_cast<char>(v));
}

inline void AppendLenPrefixedString(std::string& buf, const std::string& s) {
    AppendU32(buf, static_cast<uint32_t>(s.size()));
    buf.append(s);
}

Json::Value EntryToJson(const OpLogEntry& entry) {
    Json::Value root;
    root["sequence_id"] = static_cast<Json::UInt64>(entry.sequence_id);
    root["timestamp_ms"] = static_cast<Json::UInt64>(entry.timestamp_ms);
    root["op_type"] = static_cast<int>(entry.op_type);
    root["tenant_id"] = entry.tenant_id;
    root["object_key"] = entry.object_key;
    // CRITICAL: Base64 encode binary payload to prevent UTF-8 corruption in
    // JSON
    root["payload"] = base64::Encode(entry.payload);
    root["checksum"] = static_cast<Json::UInt>(entry.checksum);
    root["prefix_hash"] = static_cast<Json::UInt>(entry.prefix_hash);
    // Stage 2 batch coordinates. Always emitted; older consumers ignore
    // unknown keys, newer ones use them.
    root["shard_id"] = static_cast<Json::UInt>(entry.shard_id);
    root["batch_id"] = static_cast<Json::UInt64>(entry.batch_id);
    root["local_index"] = static_cast<Json::UInt>(entry.local_index);
    return root;
}

bool EntryFromJson(const Json::Value& root, OpLogEntry& entry) {
    try {
        entry.sequence_id = root["sequence_id"].asUInt64();
        entry.timestamp_ms = root["timestamp_ms"].asUInt64();
        entry.op_type = static_cast<OpType>(root["op_type"].asInt());

        // Compatibility: old OpLog entries may not have tenant_id
        entry.tenant_id = NormalizeTenantId(root.isMember("tenant_id")
                                                ? root["tenant_id"].asString()
                                                : "default");

        entry.object_key = root["object_key"].asString();
        // CRITICAL: Base64 decode payload to restore binary data
        entry.payload = base64::Decode(root["payload"].asString());
        entry.checksum = root["checksum"].asUInt();
        entry.prefix_hash = root["prefix_hash"].asUInt();

        // Stage 2: batch coordinates default to 0 for legacy on-the-wire
        // payloads. Forward compatibility is one-way — older binaries simply
        // ignore the extra keys.
        entry.shard_id =
            root.isMember("shard_id") ? root["shard_id"].asUInt() : 0u;
        entry.batch_id =
            root.isMember("batch_id") ? root["batch_id"].asUInt64() : 0u;
        entry.local_index =
            root.isMember("local_index") ? root["local_index"].asUInt() : 0u;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to deserialize OpLogEntry: " << e.what();
        return false;
    }

    std::string size_reason;
    if (!OpLogManager::ValidateEntrySize(entry, &size_reason)) {
        LOG(ERROR) << "OpLogSerializer: entry size rejected, sequence_id="
                   << entry.sequence_id << ", key=" << entry.object_key
                   << ", reason=" << size_reason;
        return false;
    }

    return true;
}

std::string WriteJsonCompact(const Json::Value& root) {
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";  // Compact format
    std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());
    std::ostringstream oss;
    writer->write(root, &oss);
    return oss.str();
}

}  // namespace

std::string SerializeOpLogEntry(const OpLogEntry& entry) {
    return WriteJsonCompact(EntryToJson(entry));
}

bool DeserializeOpLogEntry(const std::string& json_str, OpLogEntry& entry) {
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errors;

    if (!reader->parse(json_str.data(), json_str.data() + json_str.size(),
                       &root, &errors)) {
        LOG(ERROR) << "Failed to parse JSON: " << errors;
        return false;
    }

    return EntryFromJson(root, entry);
}

uint32_t ComputeOpLogBatchChecksum(const OpLogBatchRecord& batch) {
    // Pre-sized buffer of stable header + per-entry identity. Per-entry
    // payload bytes are intentionally NOT included — they are already covered
    // by OpLogEntry::checksum and re-mixing them would only burn CPU without
    // catching anything entry-level verification does not already catch.
    std::string buf;
    buf.reserve(64 + batch.owner_token.size() + batch.entries.size() * 64);

    AppendU32(buf, batch.shard_id);
    AppendU64(buf, batch.batch_id);
    AppendI64(buf, static_cast<int64_t>(batch.producer_view_version));
    AppendLenPrefixedString(buf, batch.owner_token);
    AppendU32(buf, static_cast<uint32_t>(batch.entries.size()));

    for (const auto& e : batch.entries) {
        AppendU64(buf, e.sequence_id);
        AppendU64(buf, e.timestamp_ms);
        AppendU8(buf, static_cast<uint8_t>(e.op_type));
        AppendLenPrefixedString(buf, e.tenant_id);
        AppendLenPrefixedString(buf, e.object_key);
        AppendU32(buf, e.checksum);
        AppendU32(buf, e.prefix_hash);
        AppendU32(buf, e.shard_id);
        AppendU64(buf, e.batch_id);
        AppendU32(buf, e.local_index);
    }

    return static_cast<uint32_t>(XXH32(buf.data(), buf.size(), 0));
}

std::string SerializeOpLogBatchRecord(const OpLogBatchRecord& batch) {
    Json::Value root;
    root["shard_id"] = static_cast<Json::UInt>(batch.shard_id);
    root["batch_id"] = static_cast<Json::UInt64>(batch.batch_id);
    root["producer_view_version"] =
        static_cast<Json::Int64>(batch.producer_view_version);
    // Owner token is an opaque ASCII string from the HA backend (etcd lease
    // id, etc.). Plain JSON string is sufficient.
    root["owner_token"] = batch.owner_token;
    root["batch_checksum"] = static_cast<Json::UInt>(batch.batch_checksum);

    Json::Value entries(Json::arrayValue);
    for (const auto& e : batch.entries) {
        entries.append(EntryToJson(e));
    }
    root["entries"] = std::move(entries);

    return WriteJsonCompact(root);
}

bool DeserializeOpLogBatchRecord(const std::string& json_str,
                                 OpLogBatchRecord& batch) {
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errors;

    if (!reader->parse(json_str.data(), json_str.data() + json_str.size(),
                       &root, &errors)) {
        LOG(ERROR) << "OpLogSerializer: batch parse failed: " << errors;
        return false;
    }

    OpLogBatchRecord parsed;
    try {
        parsed.shard_id = root["shard_id"].asUInt();
        parsed.batch_id = root["batch_id"].asUInt64();
        parsed.producer_view_version =
            static_cast<ViewVersionId>(root["producer_view_version"].asInt64());
        parsed.owner_token =
            root.isMember("owner_token") ? root["owner_token"].asString() : "";
        parsed.batch_checksum = root["batch_checksum"].asUInt();

        if (!root.isMember("entries") || !root["entries"].isArray()) {
            LOG(ERROR) << "OpLogSerializer: batch entries missing or not array";
            return false;
        }
        const Json::Value& entries = root["entries"];
        parsed.entries.reserve(entries.size());
        for (const auto& e : entries) {
            OpLogEntry entry;
            if (!EntryFromJson(e, entry)) {
                return false;
            }
            parsed.entries.push_back(std::move(entry));
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "OpLogSerializer: batch field decode failed: "
                   << e.what();
        return false;
    }

    // Contract: local_index must run contiguously from 0 to entries.size()-1.
    // Any gap or out-of-order index is a structural error that must be
    // rejected before higher layers (Stage 5 strict applier) get a chance to
    // observe a "valid-looking but skip-friendly" batch.
    for (uint32_t i = 0; i < parsed.entries.size(); ++i) {
        if (parsed.entries[i].local_index != i) {
            LOG(ERROR) << "OpLogSerializer: batch local_index gap at i=" << i
                       << ", got=" << parsed.entries[i].local_index;
            return false;
        }
    }

    // Contract: stored batch_checksum must match the recomputed value over
    // the parsed header + entry-identity fields. This catches both
    // accidental corruption and the negative-test "poisoned checksum" path.
    const uint32_t recomputed = ComputeOpLogBatchChecksum(parsed);
    if (recomputed != parsed.batch_checksum) {
        LOG(ERROR) << "OpLogSerializer: batch checksum mismatch, stored="
                   << parsed.batch_checksum << ", recomputed=" << recomputed
                   << ", shard_id=" << parsed.shard_id
                   << ", batch_id=" << parsed.batch_id;
        return false;
    }

    batch = std::move(parsed);
    return true;
}

}  // namespace mooncake
