#pragma once

#include "master_config.h"

#include "ha/ha_types.h"

namespace mooncake {
namespace ha {

// Pre-flight validation of the supervisor-level configuration against the
// HA capability matrix. Returns ErrorCode::OK if the configuration is
// admissible; otherwise returns a non-OK error identifying the violating
// field. The supervisor's Start() calls this before bringing up the
// leader/standby machinery.
ErrorCode ValidateMasterServiceSupervisorHAConfig(
    const MasterServiceSupervisorConfig& config);

class MasterServiceSupervisor {
   public:
    explicit MasterServiceSupervisor(
        const MasterServiceSupervisorConfig& config);

    int Start();

   private:
    MasterServiceSupervisorConfig config_;
};

}  // namespace ha
}  // namespace mooncake
