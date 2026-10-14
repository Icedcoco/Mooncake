#include "mock_client/config.h"

#include <iostream>
#include <optional>
#include <string_view>

#include <gflags/gflags.h>

DEFINE_string(config, "", "Path to the mock-client JSON configuration");
DEFINE_string(operations, "",
              "Override operations, e.g. put:single=1,put:batch=3");

int main(int argc, char** argv) {
    gflags::SetUsageMessage(
        "Validate a mock-client config: mooncake_master_mock_client --config=FILE");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_config.empty()) {
        std::cerr << "--config is required\n";
        return 2;
    }

    std::optional<std::string_view> operation_override;
    gflags::CommandLineFlagInfo operations_flag;
    if (gflags::GetCommandLineFlagInfo("operations", &operations_flag) &&
        !operations_flag.is_default) {
        operation_override = FLAGS_operations;
    }

    auto config = mock_client::MockClientConfig::LoadFile(FLAGS_config,
                                                          operation_override);
    if (!config) {
        std::cerr << "Invalid mock-client config: " << config.error().message
                  << '\n';
        return 2;
    }

    std::cout << "Configuration valid: " << FLAGS_config << " ("
              << config->operations.size() << " selected operations)\n";
    return 0;
}
