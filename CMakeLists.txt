cmake_minimum_required(VERSION 3.20)
project(chunk VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(CHUNKDB_BUILD_TESTS "Build chunkdb tests" ON)
option(CHUNKDB_WITH_TLS "Enable TLS support (OpenSSL)" ON)

add_library(chunkdb_core STATIC
    src/bit_codec.cpp
    src/chunk_store.cpp
    src/crc32.cpp
    src/engine.cpp
    src/file_layout.cpp
    src/geometry.cpp
    src/protocol.cpp
    src/uri.cpp
)

target_include_directories(chunkdb_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_executable(chunkdb_server
    src/main.cpp
    src/server.cpp
)

target_link_libraries(chunkdb_server PRIVATE chunkdb_core)

add_executable(chunkdb_bench
    bench/chunkdb_bench.cpp
)
target_link_libraries(chunkdb_bench PRIVATE chunkdb_core)

if(CHUNKDB_WITH_TLS)
    find_package(OpenSSL)
    if(OpenSSL_FOUND)
        target_link_libraries(chunkdb_server PRIVATE OpenSSL::SSL OpenSSL::Crypto)
        target_compile_definitions(chunkdb_server PRIVATE CHUNKDB_WITH_OPENSSL=1)
    else()
        message(WARNING "OpenSSL not found; chunkdb_server will build without TLS support")
    endif()
endif()

if(CHUNKDB_BUILD_TESTS)
    enable_testing()

    add_executable(chunkdb_geometry_bit_test tests/geometry_bit_tests.cpp)
    target_link_libraries(chunkdb_geometry_bit_test PRIVATE chunkdb_core)
    add_test(NAME geometry_bit COMMAND chunkdb_geometry_bit_test)

    add_executable(chunkdb_storage_test tests/storage_tests.cpp)
    target_link_libraries(chunkdb_storage_test PRIVATE chunkdb_core)
    add_test(NAME storage COMMAND chunkdb_storage_test)

    add_executable(chunkdb_protocol_test tests/protocol_tests.cpp)
    target_link_libraries(chunkdb_protocol_test PRIVATE chunkdb_core)
    add_test(NAME protocol COMMAND chunkdb_protocol_test)

    add_executable(chunkdb_auth_test tests/auth_tests.cpp)
    target_link_libraries(chunkdb_auth_test PRIVATE chunkdb_core)
    add_test(NAME auth COMMAND chunkdb_auth_test)

    add_executable(chunkdb_error_test tests/error_tests.cpp)
    target_link_libraries(chunkdb_error_test PRIVATE chunkdb_core)
    add_test(NAME error_handling COMMAND chunkdb_error_test)

    add_executable(chunkdb_concurrency_test tests/concurrency_tests.cpp)
    target_link_libraries(chunkdb_concurrency_test PRIVATE chunkdb_core)
    add_test(NAME concurrency COMMAND chunkdb_concurrency_test)

    add_executable(chunkdb_uri_test tests/uri_tests.cpp)
    target_link_libraries(chunkdb_uri_test PRIVATE chunkdb_core)
    add_test(NAME uri COMMAND chunkdb_uri_test)

    add_executable(chunkdb_process_lock_test tests/process_lock_tests.cpp)
    target_link_libraries(chunkdb_process_lock_test PRIVATE chunkdb_core)
    add_test(NAME process_lock COMMAND chunkdb_process_lock_test)

    add_executable(chunkdb_durability_test tests/durability_tests.cpp)
    target_link_libraries(chunkdb_durability_test PRIVATE chunkdb_core)
    add_test(NAME durability COMMAND chunkdb_durability_test)

    add_executable(chunkdb_cache_eviction_test tests/cache_eviction_tests.cpp)
    target_link_libraries(chunkdb_cache_eviction_test PRIVATE chunkdb_core)
    add_test(NAME cache_eviction COMMAND chunkdb_cache_eviction_test)
endif()
