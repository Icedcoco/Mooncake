#include "read_plan.h"
#include <cassert>
#include <vector>

int main() {
    using mock_client::PlanReads;
    // Mixed reads retain the requested ratio even when some puts fail.
    const std::vector<bool> partial{true, false, true, false};
    auto mixed = PlanReads(2, 1, 16, partial);
    assert(mixed.rounds == 2);
    assert((mixed.indices == std::vector<size_t>{0, 1, 2, 3}));
    auto all_failed = PlanReads(2, 0, 16, std::vector<bool>(128, false));
    assert(all_failed.rounds * all_failed.indices.size() == 256);
    // The old periodic check still samples only successful puts.
    auto legacy = PlanReads(0, 16, 16, partial);
    assert(legacy.rounds == 1);
    assert((legacy.indices == std::vector<size_t>{0, 2}));
    assert(PlanReads(0, 1, 16, partial).indices.empty());
    assert(PlanReads(0, 0, 16, std::vector<bool>(4, false)).indices.empty());
    // A verification boundary must not add a third round to mixed reads.
    assert(PlanReads(2, 16, 16, partial).rounds == 2);
    return 0;
}
