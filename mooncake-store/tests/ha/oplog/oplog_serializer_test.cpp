#include "ha/oplog/oplog_serializer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <xxhash.h>

#include <string>

namespace mooncake::test {

class OpLogSerializerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OpLogSerializerTest");
        FLAGS_logtostderr = 1;
    }
    void TearDown() override { google::ShutdownGoogleLogging(); }

    static OpLogEntry MakeEntry(uint64_t seq, OpType type,
                                const std::string& key,
                                const std::string& payload) {
        OpLogEntry e;
        e.sequence_id = seq;
        e.timestamp_ms = 1234567890;
        e.op_type = type;
        e.object_key = key;
        e.payload = payload;
        e.checksum =
            static_cast<uint32_t>(XXH32(payload.data(), payload.size(), 0));
        e.prefix_hash =
            key.empty()
                ? 0
                : static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
        return e;
    }
};

TEST_F(OpLogSerializerTest, RoundTrip_PutEnd) {
    OpLogEntry in = MakeEntry(1, OpType::PUT_END, "key1", "value1");
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(json, out));
    EXPECT_EQ(in.sequence_id, out.sequence_id);
    EXPECT_EQ(in.timestamp_ms, out.timestamp_ms);
    EXPECT_EQ(in.op_type, out.op_type);
    EXPECT_EQ(in.object_key, out.object_key);
    EXPECT_EQ(in.payload, out.payload);
    EXPECT_EQ(in.checksum, out.checksum);
    EXPECT_EQ(in.prefix_hash, out.prefix_hash);
}

TEST_F(OpLogSerializerTest, RoundTrip_Remove) {
    OpLogEntry in = MakeEntry(42, OpType::REMOVE, "obj/to/remove", "");
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(json, out));
    EXPECT_EQ(in.sequence_id, out.sequence_id);
    EXPECT_EQ(in.op_type, out.op_type);
    EXPECT_EQ(in.object_key, out.object_key);
    EXPECT_EQ(in.payload, out.payload);
}

TEST_F(OpLogSerializerTest, RoundTrip_PutRevoke) {
    OpLogEntry in = MakeEntry(99, OpType::PUT_REVOKE, "revoked_key", "meta");
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(json, out));
    EXPECT_EQ(in.op_type, out.op_type);
    EXPECT_EQ(in.payload, out.payload);
}

TEST_F(OpLogSerializerTest, RoundTrip_BinaryPayload) {
    // Payload with null bytes, high bytes — must survive base64 round-trip
    std::string binary_payload;
    for (int i = 0; i < 256; ++i) {
        binary_payload.push_back(static_cast<char>(i));
    }
    OpLogEntry in = MakeEntry(7, OpType::PUT_END, "bin_key", binary_payload);
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(json, out));
    EXPECT_EQ(in.payload, out.payload);
}

TEST_F(OpLogSerializerTest, RoundTrip_EmptyPayload) {
    OpLogEntry in = MakeEntry(10, OpType::REMOVE, "key", "");
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(json, out));
    EXPECT_EQ("", out.payload);
}

TEST_F(OpLogSerializerTest, RoundTrip_EmptyKey) {
    OpLogEntry in = MakeEntry(11, OpType::PUT_END, "", "payload");
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(json, out));
    EXPECT_EQ("", out.object_key);
    EXPECT_EQ(0u, out.prefix_hash);
}

TEST_F(OpLogSerializerTest, Deserialize_InvalidJson) {
    OpLogEntry out;
    EXPECT_FALSE(DeserializeOpLogEntry("{ not valid json }", out));
}

TEST_F(OpLogSerializerTest, Deserialize_EmptyString) {
    OpLogEntry out;
    EXPECT_FALSE(DeserializeOpLogEntry("", out));
}

TEST_F(OpLogSerializerTest, Deserialize_MissingFields) {
    // JSON with only partial fields — should fail or produce defaults
    std::string partial = R"({"sequence_id": 1})";
    OpLogEntry out;
    // Behavior depends on JsonCpp defaults for missing fields.
    // At minimum, should not crash.
    (void)DeserializeOpLogEntry(partial, out);
}

TEST_F(OpLogSerializerTest, Deserialize_KeyTooLarge) {
    OpLogEntry in = MakeEntry(1, OpType::PUT_END, "k", "v");
    in.object_key.assign(OpLogManager::kMaxObjectKeySize + 1, 'k');
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    EXPECT_FALSE(DeserializeOpLogEntry(json, out));
}

TEST_F(OpLogSerializerTest, Deserialize_PayloadTooLarge) {
    OpLogEntry in = MakeEntry(1, OpType::PUT_END, "k", "");
    in.payload.assign(OpLogManager::kMaxPayloadSize + 1, 'p');
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    EXPECT_FALSE(DeserializeOpLogEntry(json, out));
}

// ===========================================================================
// Stage 2: OpLogBatchRecord serializer tests (plan §5.3 RED -> §5.4 GREEN).
//
// These tests fix the durable batch record contract that downstream stages
// (Stage 3 etcd batch store, Stage 4 LogWriter, Stage 5 strict replay) all
// depend on. The serializer must:
//   - round-trip every batch header field plus per-entry batch coordinates;
//   - reject a record whose stored batch_checksum disagrees with the bytes;
//   - reject a record whose entries have a gap in local_index;
//   - keep legacy single-entry serialization compatible after the new entry
//     fields are introduced (older payloads must still parse with batch
//     fields defaulting to 0).
// ===========================================================================

namespace {

OpLogEntry MakeEntryForBatch(uint64_t seq, OpType type, const std::string& key,
                             const std::string& payload, uint32_t shard_id,
                             uint64_t batch_id, uint32_t local_index) {
    OpLogEntry e;
    e.sequence_id = seq;
    e.timestamp_ms = 1234567890;
    e.op_type = type;
    e.tenant_id = "default";
    e.object_key = key;
    e.payload = payload;
    e.checksum =
        static_cast<uint32_t>(XXH32(payload.data(), payload.size(), 0));
    e.prefix_hash =
        key.empty() ? 0u
                    : static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
    e.shard_id = shard_id;
    e.batch_id = batch_id;
    e.local_index = local_index;
    return e;
}

OpLogBatchRecord MakeContiguousBatch(uint32_t shard_id, uint64_t batch_id,
                                     ViewVersionId view_version,
                                     const ha::OwnerToken& owner_token,
                                     size_t entry_count) {
    OpLogBatchRecord batch;
    batch.shard_id = shard_id;
    batch.batch_id = batch_id;
    batch.producer_view_version = view_version;
    batch.owner_token = owner_token;
    batch.entries.reserve(entry_count);
    for (uint32_t i = 0; i < entry_count; ++i) {
        batch.entries.push_back(MakeEntryForBatch(
            /*seq=*/100 + i, OpType::PUT_END,
            /*key=*/"batch-key-" + std::to_string(i),
            /*payload=*/"payload-" + std::to_string(i), shard_id, batch_id, i));
    }
    batch.batch_checksum = ComputeOpLogBatchChecksum(batch);
    return batch;
}

}  // namespace

TEST_F(OpLogSerializerTest,
       SerializeOpLogBatchRecordRoundTripPreservesShardBatchAndEntries) {
    OpLogBatchRecord in = MakeContiguousBatch(/*shard_id=*/0, /*batch_id=*/7,
                                              /*view_version=*/42,
                                              /*owner_token=*/"owner-token-abc",
                                              /*entry_count=*/3);

    std::string blob = SerializeOpLogBatchRecord(in);
    ASSERT_FALSE(blob.empty());

    OpLogBatchRecord out;
    ASSERT_TRUE(DeserializeOpLogBatchRecord(blob, out));
    EXPECT_EQ(in.shard_id, out.shard_id);
    EXPECT_EQ(in.batch_id, out.batch_id);
    EXPECT_EQ(in.producer_view_version, out.producer_view_version);
    EXPECT_EQ(in.owner_token, out.owner_token);
    EXPECT_EQ(in.batch_checksum, out.batch_checksum);
    ASSERT_EQ(in.entries.size(), out.entries.size());
    for (size_t i = 0; i < in.entries.size(); ++i) {
        const auto& a = in.entries[i];
        const auto& b = out.entries[i];
        EXPECT_EQ(a.sequence_id, b.sequence_id) << "i=" << i;
        EXPECT_EQ(a.op_type, b.op_type) << "i=" << i;
        EXPECT_EQ(a.tenant_id, b.tenant_id) << "i=" << i;
        EXPECT_EQ(a.object_key, b.object_key) << "i=" << i;
        EXPECT_EQ(a.payload, b.payload) << "i=" << i;
        EXPECT_EQ(a.checksum, b.checksum) << "i=" << i;
        EXPECT_EQ(a.prefix_hash, b.prefix_hash) << "i=" << i;
        EXPECT_EQ(a.shard_id, b.shard_id) << "i=" << i;
        EXPECT_EQ(a.batch_id, b.batch_id) << "i=" << i;
        EXPECT_EQ(a.local_index, b.local_index) << "i=" << i;
    }
}

TEST_F(OpLogSerializerTest,
       DeserializeOpLogBatchRecordRejectsBadBatchChecksum) {
    OpLogBatchRecord in = MakeContiguousBatch(/*shard_id=*/0, /*batch_id=*/9,
                                              /*view_version=*/3,
                                              /*owner_token=*/"owner",
                                              /*entry_count=*/2);

    std::string blob = SerializeOpLogBatchRecord(in);
    ASSERT_FALSE(blob.empty());

    // Round-trip parses, then poison the serialized batch_checksum. We do not
    // hand-edit JSON bytes — instead we deserialize, flip a bit in
    // batch_checksum, re-serialize, and verify the next deserialize call
    // refuses the payload.
    OpLogBatchRecord intermediate;
    ASSERT_TRUE(DeserializeOpLogBatchRecord(blob, intermediate));
    intermediate.batch_checksum =
        intermediate.batch_checksum ^ 0xDEADBEEFu;  // poison
    std::string poisoned = SerializeOpLogBatchRecord(intermediate);

    OpLogBatchRecord out;
    EXPECT_FALSE(DeserializeOpLogBatchRecord(poisoned, out));
}

TEST_F(OpLogSerializerTest,
       DeserializeOpLogBatchRecordRejectsNonContiguousLocalIndex) {
    OpLogBatchRecord in = MakeContiguousBatch(/*shard_id=*/0, /*batch_id=*/12,
                                              /*view_version=*/5,
                                              /*owner_token=*/"owner",
                                              /*entry_count=*/3);

    // Introduce a gap: 0, 1, 3 — and recompute batch_checksum so the rejection
    // is unambiguously caused by the local_index gap, not by a stale checksum.
    ASSERT_EQ(3u, in.entries.size());
    in.entries[2].local_index = 3;
    in.batch_checksum = ComputeOpLogBatchChecksum(in);

    std::string blob = SerializeOpLogBatchRecord(in);
    ASSERT_FALSE(blob.empty());

    OpLogBatchRecord out;
    EXPECT_FALSE(DeserializeOpLogBatchRecord(blob, out));
}

TEST_F(OpLogSerializerTest,
       LegacyOpLogEntryRoundTripStillReadableAfterBatchFieldsAdded) {
    // (a) An entry produced *today*, leaving the new batch fields at their
    //     defaults (shard_id=0, batch_id=0, local_index=0), must round-trip
    //     without loss.
    OpLogEntry legacy_default =
        MakeEntry(1, OpType::PUT_END, "legacy-key", "legacy-payload");
    EXPECT_EQ(0u, legacy_default.shard_id);
    EXPECT_EQ(0u, legacy_default.batch_id);
    EXPECT_EQ(0u, legacy_default.local_index);

    std::string blob = SerializeOpLogEntry(legacy_default);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(blob, out));
    EXPECT_EQ(legacy_default.sequence_id, out.sequence_id);
    EXPECT_EQ(legacy_default.payload, out.payload);
    EXPECT_EQ(0u, out.shard_id);
    EXPECT_EQ(0u, out.batch_id);
    EXPECT_EQ(0u, out.local_index);

    // (b) An on-the-wire payload produced by an *older* serializer (no batch
    //     fields at all) must still parse, with batch fields defaulting to 0.
    //     This is the durability contract: rolling forward the binary must
    //     never break decoding of older OpLog records already in etcd.
    const std::string legacy_blob =
        R"({"sequence_id":17,"timestamp_ms":1234567890,"op_type":1,)"
        R"("tenant_id":"default","object_key":"k","payload":"dg==",)"
        R"("checksum":123,"prefix_hash":456})";  // base64("v") = "dg=="
    OpLogEntry from_legacy;
    ASSERT_TRUE(DeserializeOpLogEntry(legacy_blob, from_legacy));
    EXPECT_EQ(17u, from_legacy.sequence_id);
    EXPECT_EQ(OpType::PUT_END, from_legacy.op_type);
    EXPECT_EQ("k", from_legacy.object_key);
    EXPECT_EQ("v", from_legacy.payload);
    EXPECT_EQ(123u, from_legacy.checksum);
    EXPECT_EQ(456u, from_legacy.prefix_hash);
    EXPECT_EQ(0u, from_legacy.shard_id);
    EXPECT_EQ(0u, from_legacy.batch_id);
    EXPECT_EQ(0u, from_legacy.local_index);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
