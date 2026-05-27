#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "ha/oplog/mock_oplog_store.h"
#include "types.h"

namespace mooncake::test {

class MasterServiceHATest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterServiceHATest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kDefaultSegmentBase = 0x300000000;
    static constexpr size_t kDefaultSegmentSize = 1024 * 1024 * 16;

    Segment MakeSegment(std::string name = "test_segment",
                        size_t base = kDefaultSegmentBase,
                        size_t size = kDefaultSegmentSize) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        return segment;
    }

    struct MountedSegmentContext {
        UUID segment_id;
        UUID client_id;
    };

    MountedSegmentContext PrepareSimpleSegment(
        MasterService& service, std::string name = "test_segment",
        size_t base = kDefaultSegmentBase,
        size_t size = kDefaultSegmentSize) const {
        Segment segment = MakeSegment(std::move(name), base, size);
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        return {.segment_id = segment.id, .client_id = client_id};
    }

    std::string PutObject(MasterService& service, const UUID& client_id,
                          const std::string& key,
                          size_t slice_length = 1024) const {
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start = service.PutStart(client_id, key, slice_length, config);
        EXPECT_TRUE(put_start.has_value());
        EXPECT_TRUE(service.PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
        return key;
    }
};

// Test that RemoveByRegex publishes REMOVE OpLog entries for matched keys.
TEST_F(MasterServiceHATest, RemoveByRegexPublishesRemoveOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();

    std::vector<std::string> keys;
    for (int i = 0; i < 5; ++i) {
        keys.push_back("regex_key_" + std::to_string(i));
        PutObject(*service, client_id, keys.back());
    }

    // Wait for leases to expire so RemoveByRegex can remove them.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    size_t entries_before = mock_store->EntryCount();
    auto res = service->RemoveByRegex("^regex_key_", /*force=*/true);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(5, res.value());

    size_t entries_after = mock_store->EntryCount();
    EXPECT_EQ(entries_before + 5, entries_after);

    // Verify all entries are REMOVE ops for the expected keys.
    std::vector<std::string> removed_keys;
    uint64_t max_seq = 0;
    EXPECT_EQ(ErrorCode::OK, mock_store->GetMaxSequenceId(max_seq));
    for (uint64_t seq = entries_before + 1; seq <= max_seq; ++seq) {
        OpLogEntry entry;
        EXPECT_EQ(ErrorCode::OK, mock_store->ReadOpLog(seq, entry));
        EXPECT_EQ(OpType::REMOVE, entry.op_type);
        removed_keys.push_back(entry.object_key);
    }
    EXPECT_EQ(5u, removed_keys.size());
    for (const auto& key : keys) {
        EXPECT_TRUE(std::find(removed_keys.begin(), removed_keys.end(), key) !=
                    removed_keys.end())
            << "Missing REMOVE OpLog for key=" << key;
    }
}

// Test that BatchRemove skips erase when OpLog persist fails.
TEST_F(MasterServiceHATest, BatchRemovePersistFailureSkipsErase) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();

    std::vector<std::string> keys;
    for (int i = 0; i < 3; ++i) {
        keys.push_back("batch_key_" + std::to_string(i));
        PutObject(*service, client_id, keys.back());
    }

    // Wait for leases to expire.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    // Make the OpLog store fail on write.
    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    auto results = service->BatchRemove(keys, /*force=*/true);
    ASSERT_EQ(keys.size(), results.size());

    // All results should report failure because persist failed.
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_FALSE(results[i].has_value())
            << "Expected failure for key=" << keys[i];
    }

    // Keys should still exist in metadata because erase was skipped.
    for (const auto& key : keys) {
        auto exist = service->ExistKey(key);
        ASSERT_TRUE(exist.has_value());
        EXPECT_TRUE(exist.value()) << "Key should still exist: " << key;
    }

    // Restore the store and retry — removal should succeed now.
    mock_store->SetWriteError(ErrorCode::OK);
    results = service->BatchRemove(keys, /*force=*/true);
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_TRUE(results[i].has_value())
            << "Retry should succeed for key=" << keys[i];
    }
    for (const auto& key : keys) {
        auto exist = service->ExistKey(key);
        ASSERT_TRUE(exist.has_value());
        EXPECT_FALSE(exist.value()) << "Key should be removed: " << key;
    }
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
