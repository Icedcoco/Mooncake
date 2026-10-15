#include "mock_client/master_api.h"
namespace mock_client {
bool IsTransportError(mooncake::ErrorCode error) {
    return error == mooncake::ErrorCode::RPC_FAIL ||
           error == mooncake::ErrorCode::RPC_TIMEOUT;
}
}  // namespace mock_client
