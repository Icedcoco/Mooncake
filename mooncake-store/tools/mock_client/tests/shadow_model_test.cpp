#include "mock_client/deterministic_data.h"
#include "mock_client/shadow_model.h"

#include <gtest/gtest.h>

namespace mock_client {
namespace {

TEST(ShadowModelTest, PutUpsertAndRemoveTrackKnownSuccessfulState) {
    ShadowModel model;
    const auto initial = MakeObjectIdentity(7, "run-a", 0, 11, 1, 4096);

    ASSERT_TRUE(model.ApplyPut(initial, 2, "client-a", "segment-a"));
    const auto* put_entry = model.Lookup(initial.key);
    ASSERT_NE(put_entry, nullptr);
    EXPECT_TRUE(put_entry->exists);
    EXPECT_EQ(put_entry->identity.generation, 1);
    EXPECT_EQ(put_entry->expected_replica_count, 2);
    EXPECT_EQ(put_entry->owner_client_id, "client-a");
    EXPECT_EQ(put_entry->owner_segment, "segment-a");

    const auto updated = MakeObjectIdentity(7, "run-a", 0, 11, 2, 8192);
    ASSERT_TRUE(model.ApplyUpsert(updated, 2, "client-a", "segment-a"));
    const auto* upsert_entry = model.Lookup(initial.key);
    ASSERT_NE(upsert_entry, nullptr);
    EXPECT_EQ(upsert_entry->identity.generation, 2);
    EXPECT_EQ(upsert_entry->identity.logical_size, 8192);

    ASSERT_TRUE(model.ApplyRemove(initial.key));
    const auto* removed_entry = model.Lookup(initial.key);
    ASSERT_NE(removed_entry, nullptr);
    EXPECT_FALSE(removed_entry->exists);
    EXPECT_EQ(removed_entry->identity.generation, 2);
}

TEST(ShadowModelTest, UpsertMustAdvanceGenerationExactlyOnce) {
    ShadowModel model;
    const auto initial = MakeObjectIdentity(7, "run-a", 0, 11, 1, 4096);
    ASSERT_TRUE(model.ApplyPut(initial, 1, "client-a", "segment-a"));

    const auto skipped = MakeObjectIdentity(7, "run-a", 0, 11, 3, 4096);
    EXPECT_FALSE(model.ApplyUpsert(skipped, 1, "client-a", "segment-a"));
    ASSERT_NE(model.Lookup(initial.key), nullptr);
    EXPECT_EQ(model.Lookup(initial.key)->identity.generation, 1);
}

TEST(ShadowModelTest, UnknownMutationMustBeResolvedBeforeNextMutation) {
    ShadowModel model;
    const auto id = MakeObjectIdentity(7, "run-a", 0, 11, 1, 4096);
    model.MarkUnknown(id.key);

    EXPECT_FALSE(model.CanMutate(id.key));
    EXPECT_FALSE(model.ApplyPut(id, 1, "client-a", "segment-a"));

    ASSERT_TRUE(model.ResolveUnknown(id.key, ShadowObservation::Absent));
    EXPECT_TRUE(model.CanMutate(id.key));
    const auto* entry = model.Lookup(id.key);
    ASSERT_NE(entry, nullptr);
    EXPECT_FALSE(entry->exists);
    EXPECT_FALSE(entry->uncertain);
    EXPECT_EQ(entry->identity.generation, 0);
}

TEST(ShadowModelTest, PresentObservationWithoutMetadataStaysFrozen) {
    ShadowModel model;
    const auto id = MakeObjectIdentity(7, "run-a", 0, 11, 1, 4096);
    model.MarkUnknown(id.key);

    EXPECT_FALSE(model.ResolveUnknown(id.key, ShadowObservation::Present));
    EXPECT_FALSE(model.CanMutate(id.key));
}

TEST(ShadowModelTest, ReconcileStoresOnlyNormalizedReplicaSummaries) {
    ShadowModel model;
    const auto id = MakeObjectIdentity(7, "run-a", 0, 11, 1, 4096);
    ASSERT_TRUE(model.ApplyPut(id, 1, "client-a", "segment-a"));

    mooncake::Replica::Descriptor descriptor;
    descriptor.id = 49;
    descriptor.status = mooncake::ReplicaStatus::COMPLETE;
    mooncake::DiskDescriptor disk;
    disk.file_path = "/internal/path/that-is-not-retained";
    disk.object_size = 4096;
    descriptor.descriptor_variant = std::move(disk);
    mooncake::GetReplicaListResponse response;
    response.replicas.push_back(std::move(descriptor));
    response.object_checksum = id.checksum;

    EXPECT_TRUE(model.ReconcileReplicaList(id.key, response));
    const auto* entry = model.Lookup(id.key);
    ASSERT_NE(entry, nullptr);
    ASSERT_EQ(entry->replicas.size(), 1);
    EXPECT_EQ(entry->replicas.front().status, mooncake::ReplicaStatus::COMPLETE);
    EXPECT_EQ(entry->replicas.front().descriptor_id, 49);
    EXPECT_TRUE(entry->replicas.front().segment_name.empty());
    EXPECT_TRUE(entry->replicas.front().segment_endpoint.empty());
}

}  // namespace
}  // namespace mock_client
