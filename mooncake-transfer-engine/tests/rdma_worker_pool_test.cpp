// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#define private public
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"
#undef private

#include "transfer_metadata.h"

#if defined(__has_feature)
#define MC_HAS_FEATURE(x) __has_feature(x)
#else
#define MC_HAS_FEATURE(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || MC_HAS_FEATURE(address_sanitizer)
#include <sanitizer/lsan_interface.h>
#define MC_LSAN_IGNORE_OBJECT(p) __lsan_ignore_object(p)
#else
#define MC_LSAN_IGNORE_OBJECT(p) ((void)(p))
#endif

namespace mooncake {
namespace {

TransferMetadata::SegmentDesc MakeTwoRailSegment() {
    TransferMetadata::SegmentDesc desc;
    desc.name = "tcp-peer:12345";
    desc.rdma_server_name = "rdma-peer:12345";
    desc.devices.push_back({"mlx5_0", 0, "", ""});
    desc.devices.push_back({"mlx5_1", 0, "", ""});

    TransferMetadata::BufferDesc buffer;
    buffer.addr = 0x1000;
    buffer.length = 4096;
    buffer.rkey = {111, 222};
    desc.buffers.push_back(buffer);
    return desc;
}

class RdmaWorkerPoolTest : public ::testing::Test {
   protected:
    void SetUp() override {
        transport_ = new RdmaTransport();
        MC_LSAN_IGNORE_OBJECT(transport_);
        context_ = new RdmaContext(*transport_, "mlx5_local");
        MC_LSAN_IGNORE_OBJECT(context_);
        pool_ = std::make_unique<WorkerPool>(*context_);
    }

    RdmaTransport* transport_ = nullptr;
    RdmaContext* context_ = nullptr;
    std::unique_ptr<WorkerPool> pool_;
};

TEST_F(RdmaWorkerPoolTest, SelectAvailableRailSkipsPausedRail) {
    auto desc = MakeTwoRailSegment();
    for (int i = 0; i < WorkerPool::kRailErrorThreshold; ++i) {
        pool_->markRailFailed("rdma-peer:12345@mlx5_0");
    }

    int device_id = 0;
    uint32_t dest_rkey = 0;
    std::string peer_nic_path;
    ASSERT_TRUE(pool_->selectAvailableRail(&desc, /*buffer_id=*/0, device_id,
                                           dest_rkey, peer_nic_path));

    EXPECT_EQ(device_id, 1);
    EXPECT_EQ(dest_rkey, 222u);
    EXPECT_EQ(peer_nic_path, "rdma-peer:12345@mlx5_1");
}

TEST_F(RdmaWorkerPoolTest, SelectAvailableRailUsesRdmaServerNameForAltRails) {
    auto desc = MakeTwoRailSegment();
    for (int i = 0; i < WorkerPool::kRailErrorThreshold; ++i) {
        pool_->markRailFailed("rdma-peer:12345@mlx5_0");
    }
    for (int i = 0; i < WorkerPool::kRailErrorThreshold; ++i) {
        pool_->markRailFailed("rdma-peer:12345@mlx5_1");
    }

    int device_id = 0;
    uint32_t dest_rkey = 0;
    std::string peer_nic_path;
    EXPECT_FALSE(pool_->selectAvailableRail(&desc, /*buffer_id=*/0, device_id,
                                            dest_rkey, peer_nic_path));
}

}  // namespace
}  // namespace mooncake
