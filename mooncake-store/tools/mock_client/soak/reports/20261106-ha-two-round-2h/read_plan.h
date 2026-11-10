#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>

namespace mock_client {
struct ReadPlan {
    unsigned rounds;
    std::vector<size_t> indices;
};

// Mixed workloads read every attempted key; legacy workloads sample successes.
inline ReadPlan PlanReads(unsigned get_batches_per_put, uint64_t batch_number,
                          unsigned verify_every,
                          const std::vector<bool>& put_success) {
    ReadPlan plan{get_batches_per_put ? get_batches_per_put : 1, {}};
    if (!get_batches_per_put && batch_number % verify_every != 0) return plan;
    for (size_t i = 0; i < put_success.size(); ++i)
        if (get_batches_per_put || put_success[i]) plan.indices.push_back(i);
    return plan;
}
}  // namespace mock_client
