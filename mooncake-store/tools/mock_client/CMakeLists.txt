add_library(
  mock_client_core STATIC
  src/config.cpp
  src/operation_selection.cpp
  src/deterministic_data.cpp
  src/shadow_model.cpp
  src/master_session.cpp
  src/operation_executor.cpp
  src/rpc_event.cpp)
target_include_directories(
  mock_client_core
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
         ${CMAKE_CURRENT_SOURCE_DIR}/../../include
         ${CMAKE_CURRENT_SOURCE_DIR}/../../include/cachelib_memory_allocator)
target_link_libraries(
  mock_client_core PUBLIC JsonCpp::JsonCpp yalantinglibs::yalantinglibs
                          cachelib_memory_allocator glog::glog)

add_library(mock_client_master_api STATIC src/master_client_api.cpp)
target_link_libraries(mock_client_master_api PUBLIC mock_client_core
                                                    mooncake_store)
add_executable(mooncake_master_mock_client src/main.cpp)
target_link_libraries(mooncake_master_mock_client PRIVATE mock_client_master_api
                                                          gflags::gflags)

if(BUILD_UNIT_TESTS)
  add_executable(
    mock_client_core_test
    ../../src/types.cpp
    tests/master_session_test.cpp
    tests/operation_executor_test.cpp
    tests/config_test.cpp
    tests/operation_selection_test.cpp
    tests/deterministic_data_test.cpp
    tests/shadow_model_test.cpp)
  target_link_libraries(mock_client_core_test PRIVATE mock_client_core gtest
                                                      gtest_main pthread)
  add_test(NAME mock_client_core_test COMMAND mock_client_core_test)
endif()

# Build the paired pressure runner with the same library as the deployed master.
add_executable(mooncake_durable_pressure
               soak/reports/20261106-ha-two-round-2h/pressure.cpp)
target_link_libraries(mooncake_durable_pressure PRIVATE mock_client_master_api)
if(BUILD_UNIT_TESTS)
  add_executable(mock_client_live_window_test
                 soak/reports/20261106-ha-two-round-2h/window_test.cpp)
  # This standalone test uses assert(), including in optimized lab builds.
  target_compile_options(mock_client_live_window_test PRIVATE -UNDEBUG)
  add_test(NAME mock_client_live_window_test
           COMMAND mock_client_live_window_test)
  add_executable(mock_client_read_plan_test
                 soak/reports/20261106-ha-two-round-2h/read_plan_test.cpp)
  target_compile_options(mock_client_read_plan_test PRIVATE -UNDEBUG)
  add_test(NAME mock_client_read_plan_test COMMAND mock_client_read_plan_test)
endif()
