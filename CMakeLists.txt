cmake_minimum_required(VERSION 3.20)
project(chunk VERSION 0.1.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(CHUNKDB_BUILD_TESTS "Build chunkdb tests" ON)
option(CHUNKDB_WITH_TLS "Enable TLS support (OpenSSL)" ON)
option(
    CHUNKDB_BUILD_EXPERIMENTAL_LAYOUT
    "Build experimental fs_region layout benchmark/test targets"
    OFF
)
option(
    CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS
    "Use serialized chunk locking on MinGW instead of std::shared_mutex"
    ON
)

add_library(chunkdb_core STATIC
    src/bit_codec.cpp
    src/chunk_store.cpp
    src/crc32.cpp
    src/engine.cpp
    src/file_layout.cpp
    src/geometry.cpp
    src/lifecycle_log.cpp
    src/logging.cpp
    src/protocol.cpp
    src/uri.cpp
)

target_include_directories(chunkdb_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(MINGW)
    target_compile_definitions(
        chunkdb_core
        PUBLIC CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS=$<IF:$<BOOL:${CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS}>,1,0>
    )
elseif(CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS)
    message(STATUS "CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS is ignored on non-MinGW platforms")
endif()

add_executable(chunkdb_server
    src/main.cpp
    src/server.cpp
)

target_link_libraries(chunkdb_server PRIVATE chunkdb_core)
target_compile_definitions(
    chunkdb_server
    PRIVATE
        CHUNKDB_VERSION_STR="${PROJECT_VERSION}"
        CHUNKDB_BUILD_TYPE_STR="$<IF:$<BOOL:${CMAKE_BUILD_TYPE}>,${CMAKE_BUILD_TYPE},unknown>"
)

add_executable(chunkdb_bench
    bench/chunkdb_bench.cpp
)
target_link_libraries(chunkdb_bench PRIVATE chunkdb_core)

if(CHUNKDB_BUILD_EXPERIMENTAL_LAYOUT)
    add_executable(chunkdb_layout_ab_bench
        bench/layout_ab_bench.cpp
    )
    target_link_libraries(chunkdb_layout_ab_bench PRIVATE chunkdb_core)
endif()

add_executable(chunkdb_server_bench
    bench/chunkdb_server_bench.cpp
    src/server.cpp
)
target_link_libraries(chunkdb_server_bench PRIVATE chunkdb_core)

if(WIN32)
    target_link_libraries(chunkdb_server PRIVATE ws2_32)
    target_link_libraries(chunkdb_server_bench PRIVATE ws2_32)
    if(CHUNKDB_BUILD_EXPERIMENTAL_LAYOUT)
        target_link_libraries(chunkdb_layout_ab_bench PRIVATE psapi)
    endif()
endif()

if(CHUNKDB_WITH_TLS)
    find_package(OpenSSL)
    if(OpenSSL_FOUND)
        target_link_libraries(chunkdb_server PRIVATE OpenSSL::SSL OpenSSL::Crypto)
        target_compile_definitions(chunkdb_server PRIVATE CHUNKDB_WITH_OPENSSL=1)

        target_link_libraries(chunkdb_server_bench PRIVATE OpenSSL::SSL OpenSSL::Crypto)
        target_compile_definitions(chunkdb_server_bench PRIVATE CHUNKDB_WITH_OPENSSL=1)
    else()
        message(WARNING "OpenSSL not found; server targets will build without TLS support")
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

    set(CHUNKDB_EXPERIMENTAL_TESTS "")
    if(CHUNKDB_BUILD_EXPERIMENTAL_LAYOUT)
        add_executable(chunkdb_storage_layout_region_test tests/storage_layout_region_tests.cpp)
        target_link_libraries(chunkdb_storage_layout_region_test PRIVATE chunkdb_core)
        add_test(NAME storage_layout_region COMMAND chunkdb_storage_layout_region_test)
        list(APPEND CHUNKDB_EXPERIMENTAL_TESTS storage_layout_region)
    endif()

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

    add_executable(chunkdb_concurrency_eviction_stress_test tests/concurrency_eviction_stress_test.cpp)
    target_link_libraries(chunkdb_concurrency_eviction_stress_test PRIVATE chunkdb_core)
    add_test(NAME concurrency_eviction_stress COMMAND chunkdb_concurrency_eviction_stress_test)

    add_executable(chunkdb_durability_kill_recovery_test tests/durability_kill_recovery_test.cpp)
    target_link_libraries(chunkdb_durability_kill_recovery_test PRIVATE chunkdb_core)
    add_test(NAME durability_kill_recovery COMMAND chunkdb_durability_kill_recovery_test)

    add_executable(chunkdb_hot_contention_eviction_cycle_stress_test tests/hot_contention_eviction_cycle_stress_test.cpp)
    target_link_libraries(chunkdb_hot_contention_eviction_cycle_stress_test PRIVATE chunkdb_core)
    add_test(NAME hot_contention_eviction_cycle_stress COMMAND chunkdb_hot_contention_eviction_cycle_stress_test)

    add_executable(chunkdb_wal_recovery_scenarios_test tests/wal_recovery_scenarios_tests.cpp)
    target_link_libraries(chunkdb_wal_recovery_scenarios_test PRIVATE chunkdb_core)
    add_test(NAME wal_recovery_scenarios COMMAND chunkdb_wal_recovery_scenarios_test)

    add_executable(chunkdb_wal_checkpoint_cycle_test tests/wal_checkpoint_cycle_tests.cpp)
    target_link_libraries(chunkdb_wal_checkpoint_cycle_test PRIVATE chunkdb_core)
    add_test(NAME wal_checkpoint_cycle COMMAND chunkdb_wal_checkpoint_cycle_test)

    add_executable(chunkdb_wal_group_commit_test tests/wal_group_commit_tests.cpp)
    target_link_libraries(chunkdb_wal_group_commit_test PRIVATE chunkdb_core)
    add_test(NAME wal_group_commit COMMAND chunkdb_wal_group_commit_test)

    add_executable(chunkdb_server_integration_test
        tests/server_integration_tests.cpp
        src/server.cpp
    )
    target_link_libraries(chunkdb_server_integration_test PRIVATE chunkdb_core)
    if(WIN32)
        target_link_libraries(chunkdb_server_integration_test PRIVATE ws2_32)
    endif()
    if(CHUNKDB_WITH_TLS AND OpenSSL_FOUND)
        target_link_libraries(chunkdb_server_integration_test PRIVATE OpenSSL::SSL OpenSSL::Crypto)
        target_compile_definitions(chunkdb_server_integration_test PRIVATE CHUNKDB_WITH_OPENSSL=1)
    endif()
    add_test(NAME server_integration COMMAND chunkdb_server_integration_test)

    add_executable(chunkdb_durability_crash_hardening_test EXCLUDE_FROM_ALL tests/durability_crash_hardening_tests.cpp)
    target_link_libraries(chunkdb_durability_crash_hardening_test PRIVATE chunkdb_core)
    add_test(NAME durability_crash_hardening COMMAND chunkdb_durability_crash_hardening_test)

    set(CHUNKDB_SMOKE_TESTS
        geometry_bit
        storage
        protocol
        auth
        error_handling
        concurrency
        uri
        process_lock
        durability
        cache_eviction
        durability_kill_recovery
        wal_recovery_scenarios
        wal_checkpoint_cycle
        wal_group_commit
        server_integration
    )
    if(WIN32)
        add_test(NAME server_bench_cleanup_smoke COMMAND chunkdb_server_bench --ops 200 --port 4242)
        list(APPEND CHUNKDB_SMOKE_TESTS server_bench_cleanup_smoke)
    endif()
    set(CHUNKDB_STRESS_TESTS
        concurrency_eviction_stress
        hot_contention_eviction_cycle_stress
    )
    set(CHUNKDB_CRASH_TESTS
        durability_crash_hardening
    )

    set_tests_properties(${CHUNKDB_SMOKE_TESTS} PROPERTIES LABELS "smoke")
    set_tests_properties(${CHUNKDB_STRESS_TESTS} PROPERTIES LABELS "stress")
    set_tests_properties(${CHUNKDB_CRASH_TESTS} PROPERTIES LABELS "crash")
    if(CHUNKDB_EXPERIMENTAL_TESTS)
        set_tests_properties(${CHUNKDB_EXPERIMENTAL_TESTS} PROPERTIES LABELS "experimental")
    endif()
endif()
