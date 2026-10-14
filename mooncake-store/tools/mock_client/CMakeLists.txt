add_library(
  mock_client_core STATIC src/config.cpp src/operation_selection.cpp
                          src/deterministic_data.cpp src/shadow_model.cpp)
target_include_directories(
  mock_client_core
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
         ${CMAKE_CURRENT_SOURCE_DIR}/../../include
         ${CMAKE_CURRENT_SOURCE_DIR}/../../include/cachelib_memory_allocator)
target_link_libraries(
  mock_client_core PUBLIC JsonCpp::JsonCpp yalantinglibs::yalantinglibs
                          cachelib_memory_allocator glog::glog)

add_executable(mooncake_master_mock_client src/main.cpp)
target_link_libraries(mooncake_master_mock_client
                      PRIVATE mock_client_core mooncake_store gflags::gflags)

if(BUILD_UNIT_TESTS)
  add_executable(
    mock_client_core_test
    tests/config_test.cpp tests/operation_selection_test.cpp
    tests/deterministic_data_test.cpp tests/shadow_model_test.cpp)
  target_link_libraries(mock_client_core_test PRIVATE mock_client_core gtest
                                                      gtest_main pthread)
  add_test(NAME mock_client_core_test COMMAND mock_client_core_test)
endif()
