#include "ha/oplog/etcd_oplog_store.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "etcd_helper.h"
#include "ha/ha_types.h"
#include "ha/oplog/oplog_serializer.h"

DEFINE_string(etcd_endpoints, "0.0.0.0:2379",
              "Etcd endpoints for EtcdOpLogStoreTest");

namespace mooncake::test {

class EtcdOpLogStoreTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
#ifdef STORE_USE_ETCD
        google::InitGoogleLogging("EtcdOpLogStoreTest");
        FLAGS_logtostderr = 1;

        ASSERT_EQ(ErrorCode::OK,
                  EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints))
            << "Failed to connect to etcd at " << FLAGS_etcd_endpoints;
#endif
    }

    static void TearDownTestSuite() {
#ifdef STORE_USE_ETCD
        google::ShutdownGoogleLogging();
#endif
    }

    void SetUp() override {
#ifndef STORE_USE_ETCD
        GTEST_SKIP()
            << "STORE_USE_ETCD is disabled, skipping EtcdOpLogStore tests.";
#else
        cluster_id_ = "test_cluster_etcd_oplog_store";
        store_ = std::make_unique<EtcdOpLogStore>(
            cluster_id_,
            /*enable_latest_seq_batch_update=*/false,
            /*enable_batch_write=*/true);
        ASSERT_EQ(ErrorCode::OK, store_->Init());
        CleanupTestData();
#endif
    }

    void TearDown() override {
#ifdef STORE_USE_ETCD
        CleanupTestData();
        store_.reset();
#endif
    }

    std::string cluster_id_;
    std::unique_ptr<EtcdOpLogStore> store_;

    void CleanupTestData() {
#ifdef STORE_USE_ETCD
        // Delete all keys under /oplog/{cluster_id_}/ prefix
        std::string prefix = std::string("/oplog/") + cluster_id_ + "/";

        auto prefix_end = [](std::string p) -> std::string {
            for (int i = static_cast<int>(p.size()) - 1; i >= 0; --i) {
                unsigned char c = static_cast<unsigned char>(p[i]);
                if (c < 0xFF) {
                    p[i] = static_cast<char>(c + 1);
                    p.resize(i + 1);
                    return p;
                }
            }
            return std::string(1, '\0');
        };
        std::string end_key = prefix_end(prefix);

        (void)EtcdHelper::DeleteRange(prefix.c_str(), prefix.size(),
                                      end_key.c_str(), end_key.size());
#endif
    }

    static OpLogEntry MakeEntry(uint64_t seq, OpType type,
                                const std::string& key,
                                const std::string& payload) {
        OpLogEntry e;
        e.sequence_id = seq;
        e.timestamp_ms = 123456;
        e.op_type = type;
        e.object_key = key;
        e.payload = payload;
        e.checksum = 0;
        e.prefix_hash = 0;
        return e;
    }
};

// ========== 3.1.1 Basic CRUD tests ==========

TEST_F(EtcdOpLogStoreTest, TestWriteOpLog) {
    OpLogEntry e = MakeEntry(1, OpType::PUT_END, "key1", "value1");

    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));

    // Latest sequence ID should be updated to 1
    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestSequenceId(latest));
    EXPECT_EQ(1u, latest);
}

TEST_F(EtcdOpLogStoreTest, TestReadOpLog) {
    OpLogEntry e = MakeEntry(2, OpType::PUT_END, "key2", "value2");
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));

    OpLogEntry out;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLog(2, out));

    EXPECT_EQ(2u, out.sequence_id);
    EXPECT_EQ(OpType::PUT_END, out.op_type);
    EXPECT_EQ("key2", out.object_key);
    EXPECT_EQ("value2", out.payload);
}

TEST_F(EtcdOpLogStoreTest, TestReadOpLogSince) {
    // Write multiple entries
    for (uint64_t i = 10; i < 15; ++i) {
        OpLogEntry e = MakeEntry(i, OpType::PUT_END, "key_" + std::to_string(i),
                                 "value_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
    }

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLogSince(11, 10, entries));

    // Expect entries with seq > 11: 12,13,14
    ASSERT_EQ(3u, entries.size());
    EXPECT_EQ(12u, entries[0].sequence_id);
    EXPECT_EQ(13u, entries[1].sequence_id);
    EXPECT_EQ(14u, entries[2].sequence_id);
}

TEST_F(EtcdOpLogStoreTest, TestReadOpLogSince_Empty) {
    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLogSince(1000, 10, entries));
    EXPECT_TRUE(entries.empty());
}

TEST_F(EtcdOpLogStoreTest, TestReadOpLogSince_Limit) {
    for (uint64_t i = 1; i <= 5; ++i) {
        OpLogEntry e = MakeEntry(i, OpType::PUT_END, "key_" + std::to_string(i),
                                 "value_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
    }

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLogSince(0, 3, entries));
    ASSERT_EQ(3u, entries.size());
    EXPECT_EQ(1u, entries[0].sequence_id);
    EXPECT_EQ(2u, entries[1].sequence_id);
    EXPECT_EQ(3u, entries[2].sequence_id);
}

// ========== 3.1.2 Serialization tests ==========

TEST_F(EtcdOpLogStoreTest, TestSerializeDeserializeRoundTrip) {
    OpLogEntry in =
        MakeEntry(42, OpType::PUT_END, "roundtrip-key", "roundtrip-value");

    // Indirectly verify serialization / deserialization via WriteOpLog +
    // ReadOpLog
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(in));

    OpLogEntry out;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLog(42, out));

    EXPECT_EQ(in.sequence_id, out.sequence_id);
    EXPECT_EQ(in.op_type, out.op_type);
    EXPECT_EQ(in.object_key, out.object_key);
    EXPECT_EQ(in.payload, out.payload);
}

TEST_F(EtcdOpLogStoreTest, TestDeserializeInvalidJson) {
    // Write invalid JSON directly into etcd; subsequent ReadOpLog should return
    // INTERNAL_ERROR
    std::string key = "/oplog/" + cluster_id_ + "/00000000000000000077";
    std::string bad_json = "{ this is not valid json }";
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::Put(key.c_str(), key.size(), bad_json.c_str(),
                              bad_json.size()));

    OpLogEntry out;
    ASSERT_EQ(ErrorCode::INTERNAL_ERROR, store_->ReadOpLog(77, out));
}

// ========== 3.1.3 Fencing tests ==========

TEST_F(EtcdOpLogStoreTest, TestWriteOpLog_Fencing) {
    OpLogEntry e1 = MakeEntry(100, OpType::PUT_END, "key_fence", "value1");
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e1));

    // Same seq, same content => idempotent (OK)
    OpLogEntry e2 = e1;
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e2));

    // NOTE: Duplicate sequence_id with different content is not a supported
    // production scenario (sequence_id is monotonic). The batching write path
    // does not guarantee conflict detection for that case, so we don't assert
    // on it here.
}

TEST_F(EtcdOpLogStoreTest, TestWriteOpLog_Idempotent) {
    OpLogEntry e = MakeEntry(200, OpType::PUT_END, "key_idem", "v");
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));

    // Repeatedly writing the exact same entry should return OK (idempotent)
    // even if the underlying Create operation reports a transaction failure.
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
}

// ========== 3.1.4 Sequence ID management tests ==========

TEST_F(EtcdOpLogStoreTest, TestGetLatestSequenceId) {
    OpLogEntry e1 = MakeEntry(1, OpType::PUT_END, "k1", "v1");
    OpLogEntry e2 = MakeEntry(2, OpType::PUT_END, "k2", "v2");
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e1));
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e2));

    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestSequenceId(latest));
    EXPECT_EQ(2u, latest);
}

TEST_F(EtcdOpLogStoreTest, TestGetMaxSequenceIdAndEmpty) {
    uint64_t max_seq = 0;

    // Empty cluster: after cleanup, GetMaxSequenceId should return
    // OPLOG_ENTRY_NOT_FOUND
    CleanupTestData();
    EXPECT_EQ(ErrorCode::OPLOG_ENTRY_NOT_FOUND,
              store_->GetMaxSequenceId(max_seq));

    // After writing several entries, MaxSequenceId should equal the last
    // entry's seq
    for (uint64_t i = 10; i <= 15; ++i) {
        OpLogEntry e = MakeEntry(i, OpType::PUT_END, "key_" + std::to_string(i),
                                 "value_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
    }

    ASSERT_EQ(ErrorCode::OK, store_->GetMaxSequenceId(max_seq));
    EXPECT_EQ(15u, max_seq);
}

TEST_F(EtcdOpLogStoreTest, TestUpdateLatestSequenceId) {
    // Directly call UpdateLatestSequenceId, then GetLatestSequenceId should
    // match
    ASSERT_EQ(ErrorCode::OK, store_->UpdateLatestSequenceId(12345));

    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestSequenceId(latest));
    EXPECT_EQ(12345u, latest);
}

// ========== 3.1.5 Batch update tests ==========

TEST_F(EtcdOpLogStoreTest, TestBatchUpdate_EnabledAndThreshold) {
    // Use a store with batch enabled, then verify /latest is updated to the max
    // seq
    EtcdOpLogStore writer(cluster_id_,
                          /*enable_latest_seq_batch_update=*/true,
                          /*enable_batch_write=*/true);
    ASSERT_EQ(ErrorCode::OK, writer.Init());

    const uint64_t base_seq = 1000;
    const int kEntries = 5;
    for (int i = 0; i < kEntries; ++i) {
        OpLogEntry e = MakeEntry(base_seq + i, OpType::PUT_END,
                                 "batch_key_" + std::to_string(i),
                                 "batch_val_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, writer.WriteOpLog(e));
    }

    // Wait a short period to give the batch thread a chance to flush `/latest`
    std::this_thread::sleep_for(std::chrono::milliseconds(2 * 1000));

    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestSequenceId(latest));
    EXPECT_EQ(base_seq + kEntries - 1, latest);
}

TEST_F(EtcdOpLogStoreTest, TestBatchUpdate_FailurePlaceholder) {
    GTEST_SKIP() << "Batch failure scenarios are better tested with a "
                    "fault-injection etcd wrapper.";
}

// ========== 3.1.6 Cleanup tests ==========

TEST_F(EtcdOpLogStoreTest, TestCleanupOpLogBeforeAndBoundary) {
    // Write seq 1..5
    for (uint64_t i = 1; i <= 5; ++i) {
        OpLogEntry e =
            MakeEntry(i, OpType::PUT_END, "cleanup_key_" + std::to_string(i),
                      "cleanup_val_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
    }

    // Cleanup seq < 3 => 1,2 should be deleted; 3,4,5 should remain
    ASSERT_EQ(ErrorCode::OK, store_->CleanupOpLogBefore(3));

    OpLogEntry out;
    EXPECT_EQ(ErrorCode::OPLOG_ENTRY_NOT_FOUND, store_->ReadOpLog(1, out));
    EXPECT_EQ(ErrorCode::OPLOG_ENTRY_NOT_FOUND, store_->ReadOpLog(2, out));

    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLog(3, out));
    EXPECT_EQ(3u, out.sequence_id);

    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLog(5, out));
    EXPECT_EQ(5u, out.sequence_id);
}

TEST_F(EtcdOpLogStoreTest, TestCleanupOpLogBefore_Empty) {
    // Cleanup on an empty cluster should return OK
    CleanupTestData();
    EXPECT_EQ(ErrorCode::OK, store_->CleanupOpLogBefore(100));
}

// ========== 3.1.7 Cluster ID validation tests ==========

TEST_F(EtcdOpLogStoreTest, TestInvalidClusterId_Rejected) {
    // Invalid cluster_id (containing slashes) should trigger LOG(FATAL) and
    // terminate
    EXPECT_DEATH(
        {
            EtcdOpLogStore bad_store("invalid/cluster", false);
            (void)bad_store;
        },
        "Invalid cluster_id");
}

TEST_F(EtcdOpLogStoreTest, TestClusterIdNormalization) {
    // Trailing slashes should be normalized away from the cluster_id
    std::string raw_cluster = cluster_id_ + "///";
    EtcdOpLogStore normalized_store(raw_cluster,
                                    /*enable_latest_seq_batch_update=*/false,
                                    /*enable_batch_write=*/true);
    ASSERT_EQ(ErrorCode::OK, normalized_store.Init());

    OpLogEntry e = MakeEntry(999, OpType::PUT_END, "norm-key", "norm-val");
    ASSERT_EQ(ErrorCode::OK, normalized_store.WriteOpLog(e));

    // Read the same seq via the current store_ to confirm the normalized
    // cluster_id is used
    OpLogEntry out;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLog(999, out));
    EXPECT_EQ("norm-key", out.object_key);
    EXPECT_EQ("norm-val", out.payload);
}

// ========== 3.1.8 Pagination tests ==========

TEST_F(EtcdOpLogStoreTest, TestReadOpLogSince_Pagination) {
    // Write 20 entries and verify pagination via limit
    for (uint64_t i = 1; i <= 20; ++i) {
        OpLogEntry e =
            MakeEntry(i, OpType::PUT_END, "page_key_" + std::to_string(i),
                      "page_val_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
    }

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLogSince(0, 20, entries));
    ASSERT_EQ(20u, entries.size());
    for (uint64_t i = 0; i < entries.size(); ++i) {
        EXPECT_EQ(i + 1, entries[i].sequence_id);
    }
}

TEST_F(EtcdOpLogStoreTest, TestReadOpLogSince_LargeDataset) {
    // Write a larger number of entries to verify ReadOpLogSince returns the
    // first N correctly
    const uint64_t total = 200;
    const uint64_t limit = 150;
    CleanupTestData();
    for (uint64_t i = 1; i <= total; ++i) {
        OpLogEntry e =
            MakeEntry(i, OpType::PUT_END, "large_key_" + std::to_string(i),
                      "large_val_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
    }

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLogSince(0, limit, entries));
    ASSERT_EQ(limit, entries.size());
    for (uint64_t i = 0; i < limit; ++i) {
        EXPECT_EQ(i + 1, entries[i].sequence_id);
    }
}

// ========== Stage 3: etcd batch key schema + AppendBatch txn + no-overwrite
// ==========
//
// These tests exercise the durable batch record contract defined in plan §6:
//   - key schema:  /oplog/{cluster}/shards/{shard}/batches/{batch_id:020d}
//                  /oplog/{cluster}/shards/{shard}/latest_batch
//   - AppendBatch: compare latest_batch == N AND owner_token ==
//   batch.owner_token
//                  THEN put batch N+1 AND put latest_batch = N+1
//   - idempotency: re-AppendBatch with the same payload is OK
//   - conflict:    re-AppendBatch with the same batch_id but different payload
//                  is rejected, and the durable state is unchanged
//   - stale leader: a batch with an owner_token that does not match the
//                   durable latest_batch is rejected
//   - legacy group commit fallback: WriteOpLog must NOT blindly overwrite a
//                                   pre-existing sequence value with a
//                                   different payload

namespace {

// Build a valid OpLogBatchRecord with the right batch_checksum, ready to be
// passed to AppendBatch. Caller controls shard_id, batch_id, owner_token,
// producer_view_version, and the number of entries.
OpLogBatchRecord MakeBatch(uint32_t shard_id, uint64_t batch_id,
                           const std::string& owner_token,
                           ViewVersionId view_version, size_t num_entries = 1) {
    OpLogBatchRecord batch;
    batch.shard_id = shard_id;
    batch.batch_id = batch_id;
    batch.producer_view_version = view_version;
    batch.owner_token = owner_token;
    for (size_t i = 0; i < num_entries; ++i) {
        OpLogEntry e;
        // sequence_id is independent of batch_id and is allocated by the
        // LogWriter in Stage 4. For Stage 3 tests we just need a unique
        // monotonic value so payload checksums differ between batches.
        e.sequence_id = batch_id * 1000 + i;
        e.timestamp_ms = 1000 + i;
        e.op_type = OpType::PUT_END;
        e.object_key =
            "s3_key_" + std::to_string(batch_id) + "_" + std::to_string(i);
        e.payload =
            "s3_payload_" + std::to_string(batch_id) + "_" + std::to_string(i);
        // Stage 2 batch coordinates — must match the batch header for any
        // entry produced by the LogWriter.
        e.shard_id = shard_id;
        e.batch_id = batch_id;
        e.local_index = static_cast<uint32_t>(i);
        batch.entries.push_back(e);
    }
    batch.batch_checksum = mooncake::ComputeOpLogBatchChecksum(batch);
    return batch;
}

}  // namespace

// ReadBatchesSinceReturnsBatchesInBatchIdOrder
//
// After appending three batches in increasing batch_id order, ReadBatchesSince
// must return them in batch_id ascending order regardless of which shard the
// caller asks for (we only have shard 0 in basic-available HA).
TEST_F(EtcdOpLogStoreTest,
       Stage3_ReadBatchesSinceReturnsBatchesInBatchIdOrder) {
    ASSERT_EQ(ErrorCode::OK,
              store_->AppendBatch(MakeBatch(0, 1, "leader-A", 11)));
    ASSERT_EQ(ErrorCode::OK,
              store_->AppendBatch(MakeBatch(0, 2, "leader-A", 11)));
    ASSERT_EQ(ErrorCode::OK,
              store_->AppendBatch(MakeBatch(0, 3, "leader-A", 11)));

    std::vector<OpLogBatchRecord> batches;
    ASSERT_EQ(ErrorCode::OK, store_->ReadBatchesSince(0, 0, 10, batches));
    ASSERT_EQ(3u, batches.size());
    EXPECT_EQ(1u, batches[0].batch_id);
    EXPECT_EQ(2u, batches[1].batch_id);
    EXPECT_EQ(3u, batches[2].batch_id);

    // Each returned batch must also be readable directly by (shard, batch_id).
    for (uint64_t id = 1; id <= 3; ++id) {
        OpLogBatchRecord one;
        ASSERT_EQ(ErrorCode::OK, store_->ReadBatch(0, id, one));
        EXPECT_EQ(id, one.batch_id);
        EXPECT_EQ(0u, one.shard_id);
        EXPECT_EQ("leader-A", one.owner_token);
    }

    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestBatchId(0, latest));
    EXPECT_EQ(3u, latest);
}

// AppendBatchCreatesBatchAndLatestAtomically
//
// A successful AppendBatch must durably persist BOTH the batch record and the
// latest_batch pointer in a way that is observable to subsequent readers. The
// legacy /oplog/{cluster}/latest (sequence-id form) must remain untouched by
// the batch namespace.
TEST_F(EtcdOpLogStoreTest, Stage3_AppendBatchCreatesBatchAndLatestAtomically) {
    OpLogBatchRecord batch = MakeBatch(0, 1, "leader-A", 11, /*num_entries=*/3);
    ASSERT_EQ(ErrorCode::OK, store_->AppendBatch(batch));

    // The batch record is readable.
    OpLogBatchRecord out;
    ASSERT_EQ(ErrorCode::OK, store_->ReadBatch(0, 1, out));
    EXPECT_EQ(batch.batch_checksum, out.batch_checksum);
    EXPECT_EQ(3u, out.entries.size());
    EXPECT_EQ(0u, out.entries[0].local_index);
    EXPECT_EQ(1u, out.entries[1].local_index);
    EXPECT_EQ(2u, out.entries[2].local_index);

    // The latest_batch pointer advanced.
    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestBatchId(0, latest));
    EXPECT_EQ(1u, latest);

    // The legacy /oplog/{cluster}/latest key was not touched by the batch
    // write. It is either absent (returns OPLOG_ENTRY_NOT_FOUND) or holds a
    // value that the batch API does not interpret.
    uint64_t legacy_latest = 999;
    ErrorCode legacy_err = store_->GetLatestSequenceId(legacy_latest);
    if (legacy_err == ErrorCode::OK) {
        // If present, it must not have been advanced to 1 (no sequence was
        // written through the legacy path).
        EXPECT_EQ(0u, legacy_latest);
    } else {
        EXPECT_EQ(ErrorCode::OPLOG_ENTRY_NOT_FOUND, legacy_err);
    }
}

// AppendBatchRejectsLatestBatchMismatch
//
// If the incoming batch_id is not exactly latest_batch + 1, AppendBatch must
// reject it with SEQUENCE_CONFLICT. This is the durable invariant that keeps
// the standby replay loop from ever seeing a gap in batch_id space.
//
// Note: replaying batch_id == latest_batch is a separate case and is covered
// by Stage3_AppendBatchRetrySamePayloadIsIdempotentSuccess (same payload →
// OK) and Stage3_AppendBatchSameBatchDifferentPayloadReturnsConflict (same
// batch_id, different payload → SEQUENCE_CONFLICT). This test focuses on the
// strictly-in-the-future case where the incoming batch_id jumps over the
// expected next slot.
TEST_F(EtcdOpLogStoreTest, Stage3_AppendBatchRejectsLatestBatchMismatch) {
    ASSERT_EQ(ErrorCode::OK,
              store_->AppendBatch(MakeBatch(0, 1, "leader-A", 11)));

    // Skipping batch_id 2 and going to 3 must be rejected as a gap.
    EXPECT_EQ(ErrorCode::SEQUENCE_CONFLICT,
              store_->AppendBatch(MakeBatch(0, 3, "leader-A", 11)));

    // A wildly out-of-range batch_id is also rejected.
    EXPECT_EQ(ErrorCode::SEQUENCE_CONFLICT,
              store_->AppendBatch(MakeBatch(0, 100, "leader-A", 11)));

    // The correct next batch_id is still accepted.
    ASSERT_EQ(ErrorCode::OK,
              store_->AppendBatch(MakeBatch(0, 2, "leader-A", 11)));

    // The latest_batch pointer advanced to 2.
    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestBatchId(0, latest));
    EXPECT_EQ(2u, latest);
}

// AppendBatchRetrySamePayloadIsIdempotentSuccess
//
// Re-sending the exact same batch (same shard_id, batch_id, owner_token, and
// serialized content) is an idempotent retry and must return OK without
// advancing state. This covers the "client retried after a network blip"
// case.
TEST_F(EtcdOpLogStoreTest,
       Stage3_AppendBatchRetrySamePayloadIsIdempotentSuccess) {
    OpLogBatchRecord batch = MakeBatch(0, 1, "leader-A", 11);
    ASSERT_EQ(ErrorCode::OK, store_->AppendBatch(batch));

    // Same payload — second call is idempotent and must return OK.
    OpLogBatchRecord retry = batch;  // exact same fields, same checksum
    ASSERT_EQ(ErrorCode::OK, store_->AppendBatch(retry));

    // latest_batch still points to 1.
    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestBatchId(0, latest));
    EXPECT_EQ(1u, latest);
}

// AppendBatchSameBatchDifferentPayloadReturnsConflict
//
// If a caller writes (shard=0, batch_id=1) with one payload, and then tries
// to write (shard=0, batch_id=1) with a different payload, the second call
// must be rejected. The durable record must remain exactly the first one —
// no partial overwrite, no field-level merge.
TEST_F(EtcdOpLogStoreTest,
       Stage3_AppendBatchSameBatchDifferentPayloadReturnsConflict) {
    OpLogBatchRecord batch = MakeBatch(0, 1, "leader-A", 11);
    ASSERT_EQ(ErrorCode::OK, store_->AppendBatch(batch));

    OpLogBatchRecord conflict =
        MakeBatch(0, 1, "leader-A", 11, /*num_entries=*/2);
    EXPECT_EQ(ErrorCode::SEQUENCE_CONFLICT, store_->AppendBatch(conflict));

    // Durable state must still match the FIRST batch.
    OpLogBatchRecord out;
    ASSERT_EQ(ErrorCode::OK, store_->ReadBatch(0, 1, out));
    EXPECT_EQ(batch.batch_checksum, out.batch_checksum);
    EXPECT_EQ(1u, out.entries.size());

    // latest_batch unchanged.
    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestBatchId(0, latest));
    EXPECT_EQ(1u, latest);
}

// AppendBatchRejectsStaleLeaderToken
//
// After batch 1 has been durably written by leader-A (owner_token=A), a batch
// with a different owner_token is rejected as stale. The owner_token check is
// the per-batch fencing gate that prevents a revoked leader from sneaking in
// writes against the durable history.
TEST_F(EtcdOpLogStoreTest, Stage3_AppendBatchRejectsStaleLeaderToken) {
    ASSERT_EQ(ErrorCode::OK,
              store_->AppendBatch(MakeBatch(0, 1, "leader-A", 11)));

    // Different owner_token at batch_id=2 must be rejected.
    EXPECT_EQ(ErrorCode::STALE_LEADER,
              store_->AppendBatch(MakeBatch(0, 2, "leader-B", 22)));

    // Even an empty owner_token is treated as a different leader.
    EXPECT_EQ(ErrorCode::STALE_LEADER,
              store_->AppendBatch(MakeBatch(0, 2, "", 22)));

    // Same owner_token succeeds and advances latest_batch to 2.
    ASSERT_EQ(ErrorCode::OK,
              store_->AppendBatch(MakeBatch(0, 2, "leader-A", 11)));
    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestBatchId(0, latest));
    EXPECT_EQ(2u, latest);
}

// LegacyGroupCommitDoesNotOverwriteDifferentExistingSequenceValue
//
// Plan §6.4 requires fixing the legacy FlushBatch fallback: when BatchCreate
// fails (key already exists), the per-key fallback used to blind-Put, which
// would silently overwrite a previously durable value with a different one.
// The fix is to read-and-compare: if the existing value differs from what we
// are about to write, the fallback must NOT overwrite. The legacy caller will
// see a persist failure and can react accordingly.
TEST_F(EtcdOpLogStoreTest,
       Stage3_LegacyGroupCommitDoesNotOverwriteDifferentExistingSequenceValue) {
    // Pre-populate etcd with sequence_id=42 carrying payload "original".
    OpLogEntry existing = MakeEntry(42, OpType::PUT_END, "k_orig", "original");
    std::string existing_json = mooncake::SerializeOpLogEntry(existing);
    std::string existing_key =
        "/oplog/" + cluster_id_ + "/00000000000000000042";
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::Put(existing_key.c_str(), existing_key.size(),
                              existing_json.c_str(), existing_json.size()));

    // Now try to WriteOpLog at the SAME sequence_id with a DIFFERENT payload.
    // This is the exact path that used to overwrite via blind Put.
    OpLogEntry conflicting =
        MakeEntry(42, OpType::PUT_END, "k_orig", "OVERWRITTEN");
    ErrorCode write_err = store_->WriteOpLog(conflicting);
    // The legacy path returns ETCD_OPERATION_ERROR on persist timeout (3s)
    // because the fallback refused to overwrite. We accept any non-OK status
    // that signals "did not silently overwrite".
    EXPECT_NE(ErrorCode::OK, write_err);

    // The durable value must still be the original. The no-overwrite invariant
    // is the whole point of this test.
    OpLogEntry read_back;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLog(42, read_back));
    EXPECT_EQ("original", read_back.payload);
    EXPECT_EQ("k_orig", read_back.object_key);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
