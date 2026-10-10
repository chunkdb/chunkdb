cmake_minimum_required(VERSION 3.20)
project(chunk VERSION 1.3.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(CHUNKDB_BUILD_TESTS "Build chunkdb tests" ON)
option(CHUNKDB_WITH_TLS "Enable TLS support (OpenSSL)" ON)
option(CHUNKDB_WERROR "Treat compiler warnings as errors" OFF)
option(
    CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS
    "Use serialized chunk locking on MinGW instead of std::shared_mutex"
    ON
)
option(CHUNKDB_ENABLE_PACKAGING "Enable CPack archive packaging configuration" ON)

if(MSVC)
    add_compile_options(/W4)
    if(CHUNKDB_WERROR)
        add_compile_options(/WX)
    endif()
else()
    add_compile_options(-Wall -Wextra)
    if(CHUNKDB_WERROR)
        add_compile_options(-Werror)
    endif()
endif()

add_library(chunkdb_core STATIC
    src/atomic_write.cpp
    src/barrier.cpp
    src/bit_codec.cpp
    src/block_ops.cpp
    src/checkpoint.cpp
    src/chunk_cache.cpp
    src/chunk_format.cpp
    src/feed_protocol.cpp
    src/change_feed.cpp
    src/feed_slot_records.cpp
    src/feed_slots.cpp
    src/slot_watch.cpp
    src/feed_archive.cpp
    src/feed_prefix.cpp
    src/chunk_ops.cpp
    src/chunk_store.cpp
    src/conditional_intent.cpp
    src/crc32.cpp
    src/crypto.cpp
    src/scram.cpp
    src/users.cpp
    src/user_registry.cpp
    src/durability_io.cpp
    src/engine.cpp
    src/engine_cql.cpp
    src/eviction.cpp
    src/chunk_vars.cpp
    src/feature_flags.cpp
    src/file_layout.cpp
    src/geometry.cpp
    src/lifecycle_log.cpp
    src/logging.cpp
    src/maintenance.cpp
    src/metrics.cpp
    src/process_lock.cpp
    src/protocol.cpp
    src/snapshot_generation.cpp
    src/store_manifest.cpp
    src/chunk_layout.cpp
    src/cql.cpp
    src/schema.cpp
    src/store_resources.cpp
    src/table_catalog.cpp
    src/table_catalog_migrations.cpp
    src/migrations_records.cpp
    src/migrations_apply.cpp
    src/uri.cpp
    src/txn_commit.cpp
    src/txn_history.cpp
    src/txn_intent.cpp
    src/version_clock.cpp
    src/wal_replay.cpp
    src/wal_stream_pool.cpp
    src/wal_writer.cpp
    src/world_read.cpp
    src/zrle.cpp
)

target_include_directories(chunkdb_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(WIN32)
    # Auth-failure source bucketing uses inet_pton/inet_ntop (ws2_32).
    target_link_libraries(chunkdb_core PUBLIC ws2_32)
endif()

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
    src/server_socket.cpp
    src/server_io.cpp
    src/server_tls.cpp
    src/server_feed.cpp
    src/server_connection.cpp
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

add_executable(chunkdb_large_world_bench
    bench/large_world_bench.cpp
)
target_link_libraries(chunkdb_large_world_bench PRIVATE chunkdb_core)

add_executable(chunkdb_compression_bench
    bench/compression_bench.cpp
)
target_link_libraries(chunkdb_compression_bench PRIVATE chunkdb_core)

# Offline tools.
add_library(chunkdb_tools STATIC
    src/admin.cpp
    src/verify.cpp
)
target_link_libraries(chunkdb_tools PUBLIC chunkdb_core)
target_include_directories(chunkdb_tools PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)

add_executable(chunkdb_verify src/verify_main.cpp)
target_link_libraries(chunkdb_verify PRIVATE chunkdb_tools)

add_executable(chunkdb_admin src/admin_main.cpp)
target_link_libraries(chunkdb_admin PRIVATE chunkdb_tools)

add_library(chunkdb_server_bench_lib STATIC
    bench/server_bench_lib.cpp
    src/server.cpp
    src/server_socket.cpp
    src/server_io.cpp
    src/server_tls.cpp
    src/server_feed.cpp
    src/server_connection.cpp
)
target_link_libraries(chunkdb_server_bench_lib PUBLIC chunkdb_core)
target_include_directories(chunkdb_server_bench_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

add_executable(chunkdb_server_bench
    bench/chunkdb_server_bench.cpp
)
target_link_libraries(chunkdb_server_bench PRIVATE chunkdb_server_bench_lib)

if(WIN32)
    target_link_libraries(chunkdb_server PRIVATE ws2_32)
    target_link_libraries(chunkdb_server_bench_lib PUBLIC ws2_32)
    target_link_libraries(chunkdb_large_world_bench PRIVATE psapi)
endif()

if(CHUNKDB_WITH_TLS)
    find_package(OpenSSL)
    if(OpenSSL_FOUND)
        target_link_libraries(chunkdb_server PRIVATE OpenSSL::SSL OpenSSL::Crypto)
        target_compile_definitions(chunkdb_server PRIVATE CHUNKDB_WITH_OPENSSL=1)

        target_link_libraries(chunkdb_server_bench_lib PUBLIC OpenSSL::SSL OpenSSL::Crypto)
        target_compile_definitions(chunkdb_server_bench_lib PRIVATE CHUNKDB_WITH_OPENSSL=1)
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

    add_executable(chunkdb_server_defaults_test tests/server_defaults_tests.cpp)
    target_link_libraries(chunkdb_server_defaults_test PRIVATE chunkdb_core)
    add_test(NAME server_defaults COMMAND chunkdb_server_defaults_test)

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
    target_include_directories(
        chunkdb_wal_recovery_scenarios_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_test(NAME wal_recovery_scenarios COMMAND chunkdb_wal_recovery_scenarios_test)

    add_executable(chunkdb_wal_format_test tests/wal_format_tests.cpp)
    target_link_libraries(chunkdb_wal_format_test PRIVATE chunkdb_core)
    target_include_directories(
        chunkdb_wal_format_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_dependencies(chunkdb_wal_format_test chunkdb_verify)
    add_test(NAME wal_format COMMAND chunkdb_wal_format_test $<TARGET_FILE:chunkdb_verify>)

    add_executable(chunkdb_chunk_image_test tests/chunk_image_tests.cpp)
    target_link_libraries(chunkdb_chunk_image_test PRIVATE chunkdb_core)
    target_include_directories(
        chunkdb_chunk_image_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_test(NAME chunk_image COMMAND chunkdb_chunk_image_test)

    add_executable(chunkdb_schema_test tests/schema_tests.cpp)
    target_link_libraries(chunkdb_schema_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_schema_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME schema COMMAND chunkdb_schema_test)

    add_executable(chunkdb_schema_versions_test tests/schema_versions_tests.cpp)
    target_link_libraries(chunkdb_schema_versions_test PRIVATE chunkdb_tools)
    target_include_directories(chunkdb_schema_versions_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME schema_versions COMMAND chunkdb_schema_versions_test)

    add_executable(chunkdb_column_types_test tests/column_types_tests.cpp)
    target_link_libraries(chunkdb_column_types_test PRIVATE chunkdb_core)
    add_test(NAME column_types COMMAND chunkdb_column_types_test)

    add_executable(chunkdb_narrowing_test tests/narrowing_tests.cpp)
    target_link_libraries(chunkdb_narrowing_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_narrowing_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME narrowing COMMAND chunkdb_narrowing_test)

    add_executable(chunkdb_cql_test tests/cql_tests.cpp)
    target_link_libraries(chunkdb_cql_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_cql_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME cql COMMAND chunkdb_cql_test)

    add_executable(chunkdb_crypto_test tests/crypto_tests.cpp)
    target_link_libraries(chunkdb_crypto_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_crypto_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME crypto COMMAND chunkdb_crypto_test)

    add_executable(chunkdb_scram_test tests/scram_tests.cpp)
    target_link_libraries(chunkdb_scram_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_scram_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME scram COMMAND chunkdb_scram_test)

    add_executable(chunkdb_users_file_test tests/users_file_tests.cpp)
    target_link_libraries(chunkdb_users_file_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_users_file_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME users_file COMMAND chunkdb_users_file_test)

    add_executable(chunkdb_user_registry_test tests/user_registry_tests.cpp)
    target_link_libraries(chunkdb_user_registry_test PRIVATE chunkdb_tools)
    target_include_directories(chunkdb_user_registry_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME user_registry COMMAND chunkdb_user_registry_test)

    add_executable(chunkdb_users_engine_test tests/users_engine_tests.cpp)
    target_link_libraries(chunkdb_users_engine_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_users_engine_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME users_engine COMMAND chunkdb_users_engine_test)

    add_executable(chunkdb_migrations_records_test tests/migrations_records_tests.cpp)
    target_link_libraries(chunkdb_migrations_records_test PRIVATE chunkdb_tools)
    target_include_directories(chunkdb_migrations_records_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME migrations_records COMMAND chunkdb_migrations_records_test)

    add_executable(chunkdb_migrations_engine_test tests/migrations_engine_tests.cpp)
    target_link_libraries(chunkdb_migrations_engine_test PRIVATE chunkdb_core)
    add_test(NAME migrations_engine COMMAND chunkdb_migrations_engine_test)

    add_executable(chunkdb_cql_engine_test tests/cql_engine_tests.cpp)
    target_link_libraries(chunkdb_cql_engine_test PRIVATE chunkdb_core)
    add_test(NAME cql_engine COMMAND chunkdb_cql_engine_test)
    add_executable(chunkdb_txn_engine_test tests/txn_engine_tests.cpp)
    target_link_libraries(chunkdb_txn_engine_test PRIVATE chunkdb_core)
    add_test(NAME txn_engine COMMAND chunkdb_txn_engine_test)

    add_executable(chunkdb_typed_columns_test tests/typed_columns_tests.cpp)
    target_link_libraries(chunkdb_typed_columns_test PRIVATE chunkdb_core)
    add_test(NAME typed_columns COMMAND chunkdb_typed_columns_test)

    add_executable(chunkdb_table_catalog_test tests/table_catalog_tests.cpp)
    target_link_libraries(chunkdb_table_catalog_test PRIVATE chunkdb_core)
    target_include_directories(
        chunkdb_table_catalog_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_test(NAME table_catalog COMMAND chunkdb_table_catalog_test)

    add_executable(chunkdb_chunk_vars_test tests/chunk_vars_tests.cpp)
    target_link_libraries(chunkdb_chunk_vars_test PRIVATE chunkdb_tools)
    target_include_directories(
        chunkdb_chunk_vars_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_test(NAME chunk_vars COMMAND chunkdb_chunk_vars_test)
    set_tests_properties(chunk_vars PROPERTIES TIMEOUT 600)

    add_executable(chunkdb_server_process_test tests/server_process_tests.cpp)
    add_dependencies(chunkdb_server_process_test chunkdb_server)
    add_test(
        NAME server_process
        COMMAND chunkdb_server_process_test $<TARGET_FILE:chunkdb_server>
    )
    set_tests_properties(server_process PROPERTIES TIMEOUT 300)

    add_executable(chunkdb_store_manifest_test tests/store_manifest_tests.cpp)
    target_link_libraries(chunkdb_store_manifest_test PRIVATE chunkdb_core)
    target_include_directories(
        chunkdb_store_manifest_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    # Runs the server and verify binaries as child processes.
    add_dependencies(chunkdb_store_manifest_test chunkdb_server chunkdb_verify)
    add_test(
        NAME store_manifest
        COMMAND chunkdb_store_manifest_test
            $<TARGET_FILE:chunkdb_server>
            $<TARGET_FILE:chunkdb_verify>
    )
    set_tests_properties(store_manifest PROPERTIES TIMEOUT 300)

    add_executable(chunkdb_wal_checkpoint_cycle_test tests/wal_checkpoint_cycle_tests.cpp)
    target_link_libraries(chunkdb_wal_checkpoint_cycle_test PRIVATE chunkdb_core)
    add_test(NAME wal_checkpoint_cycle COMMAND chunkdb_wal_checkpoint_cycle_test)

    add_executable(chunkdb_world_ops_test tests/world_ops_tests.cpp)
    target_link_libraries(chunkdb_world_ops_test PRIVATE chunkdb_core)
    add_test(NAME world_ops COMMAND chunkdb_world_ops_test)

    add_executable(chunkdb_world_ops_regression_test tests/world_ops_regression_tests.cpp)
    target_link_libraries(chunkdb_world_ops_regression_test PRIVATE chunkdb_core)
    add_test(NAME world_ops_regression COMMAND chunkdb_world_ops_regression_test)

    add_executable(
        chunkdb_read_only_intent_replay_test
        tests/read_only_intent_replay_tests.cpp
    )
    target_link_libraries(chunkdb_read_only_intent_replay_test PRIVATE chunkdb_core)
    target_include_directories(
        chunkdb_read_only_intent_replay_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_test(
        NAME read_only_intent_replay
        COMMAND chunkdb_read_only_intent_replay_test
    )

    add_executable(chunkdb_wal_group_commit_test tests/wal_group_commit_tests.cpp)
    target_link_libraries(chunkdb_wal_group_commit_test PRIVATE chunkdb_core)
    add_test(NAME wal_group_commit COMMAND chunkdb_wal_group_commit_test)

    add_executable(
        chunkdb_security_regression_test
        tests/security_regression_tests.cpp
    )
    target_link_libraries(chunkdb_security_regression_test PRIVATE chunkdb_core)
    target_include_directories(
        chunkdb_security_regression_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_test(
        NAME security_regression
        COMMAND chunkdb_security_regression_test
    )

    add_executable(chunkdb_server_integration_test
        tests/server_integration_tests.cpp
        src/server.cpp
        src/server_socket.cpp
        src/server_io.cpp
        src/server_tls.cpp
        src/server_feed.cpp
        src/server_connection.cpp
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
    add_test(NAME feed_watch COMMAND chunkdb_server_integration_test --feed-watch)
    set_tests_properties(feed_watch PROPERTIES TIMEOUT 180 LABELS "smoke")
    # A listen that unexpectedly succeeds would otherwise wait forever.
    set_tests_properties(server_integration PROPERTIES TIMEOUT 300)

    add_executable(chunkdb_feed_completed_prefix_reader_test tests/feed_completed_prefix_reader_tests.cpp)
    target_link_libraries(chunkdb_feed_completed_prefix_reader_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_feed_completed_prefix_reader_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME feed_completed_prefix_reader COMMAND chunkdb_feed_completed_prefix_reader_test)
    set_tests_properties(feed_completed_prefix_reader PROPERTIES TIMEOUT 60 LABELS "smoke")

    add_executable(chunkdb_server_slots_protocol_test
        tests/server_slots_protocol_tests.cpp
        src/server.cpp src/server_socket.cpp src/server_io.cpp src/server_tls.cpp
        src/server_feed.cpp src/server_connection.cpp
    )
    target_link_libraries(chunkdb_server_slots_protocol_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_server_slots_protocol_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    if(WIN32)
        target_link_libraries(chunkdb_server_slots_protocol_test PRIVATE ws2_32)
    endif()
    if(CHUNKDB_WITH_TLS AND OpenSSL_FOUND)
        target_link_libraries(chunkdb_server_slots_protocol_test PRIVATE OpenSSL::SSL OpenSSL::Crypto)
        target_compile_definitions(chunkdb_server_slots_protocol_test PRIVATE CHUNKDB_WITH_OPENSSL=1)
    endif()
    add_test(NAME feed_slots_protocol COMMAND chunkdb_server_slots_protocol_test)
    set_tests_properties(feed_slots_protocol PROPERTIES TIMEOUT 180 LABELS "smoke")

    add_executable(chunkdb_server_migrations_protocol_test
        tests/server_migrations_protocol_tests.cpp
        src/server.cpp src/server_socket.cpp src/server_io.cpp src/server_tls.cpp
        src/server_feed.cpp src/server_connection.cpp
    )
    target_link_libraries(chunkdb_server_migrations_protocol_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_server_migrations_protocol_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    if(WIN32)
        target_link_libraries(chunkdb_server_migrations_protocol_test PRIVATE ws2_32)
    endif()
    if(CHUNKDB_WITH_TLS AND OpenSSL_FOUND)
        target_link_libraries(chunkdb_server_migrations_protocol_test PRIVATE OpenSSL::SSL OpenSSL::Crypto)
        target_compile_definitions(chunkdb_server_migrations_protocol_test PRIVATE CHUNKDB_WITH_OPENSSL=1)
    endif()
    add_test(NAME migrations_protocol COMMAND chunkdb_server_migrations_protocol_test)
    set_tests_properties(migrations_protocol PROPERTIES TIMEOUT 180 LABELS "smoke")

    add_executable(chunkdb_server_slots_process_test tests/server_slots_process_tests.cpp)
    target_link_libraries(chunkdb_server_slots_process_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_server_slots_process_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    if(WIN32)
        target_link_libraries(chunkdb_server_slots_process_test PRIVATE ws2_32)
    endif()
    add_dependencies(chunkdb_server_slots_process_test chunkdb_server)
    add_test(NAME feed_slots_process COMMAND chunkdb_server_slots_process_test $<TARGET_FILE:chunkdb_server>)
    set_tests_properties(feed_slots_process PROPERTIES TIMEOUT 120 LABELS "smoke")

    add_executable(chunkdb_server_bench_test tests/server_bench_tests.cpp)
    target_link_libraries(chunkdb_server_bench_test PRIVATE chunkdb_server_bench_lib)
    if(WIN32)
        target_link_libraries(chunkdb_server_bench_test PRIVATE ws2_32)
    endif()
    add_test(NAME server_bench COMMAND chunkdb_server_bench_test)

    add_executable(chunkdb_durability_crash_hardening_test tests/durability_crash_hardening_tests.cpp)
    target_link_libraries(chunkdb_durability_crash_hardening_test PRIVATE chunkdb_core)
    add_test(NAME durability_crash_hardening COMMAND chunkdb_durability_crash_hardening_test)

    add_executable(
        chunkdb_snapshot_generation_linger_test
        tests/snapshot_generation_linger_tests.cpp
    )
    target_link_libraries(chunkdb_snapshot_generation_linger_test PRIVATE chunkdb_core)
    add_test(
        NAME snapshot_generation_linger
        COMMAND chunkdb_snapshot_generation_linger_test
    )

    add_executable(chunkdb_txn_history_test tests/txn_history_tests.cpp)
    target_link_libraries(chunkdb_txn_history_test PRIVATE chunkdb_core)
    target_include_directories(
        chunkdb_txn_history_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_test(NAME txn_history COMMAND chunkdb_txn_history_test)

    add_executable(chunkdb_txn_snapshot_test tests/txn_snapshot_tests.cpp)
    target_link_libraries(chunkdb_txn_snapshot_test PRIVATE chunkdb_core)
    target_include_directories(
        chunkdb_txn_snapshot_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_test(NAME txn_snapshot COMMAND chunkdb_txn_snapshot_test)

    add_executable(chunkdb_txn_commit_test tests/txn_commit_tests.cpp)
    target_link_libraries(chunkdb_txn_commit_test PRIVATE chunkdb_tools)
    target_include_directories(
        chunkdb_txn_commit_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_test(NAME txn_commit COMMAND chunkdb_txn_commit_test)

    add_executable(chunkdb_txn_crash_test tests/txn_crash_tests.cpp)
    target_link_libraries(chunkdb_txn_crash_test PRIVATE chunkdb_core)
    target_include_directories(
        chunkdb_txn_crash_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_test(NAME txn_crash COMMAND chunkdb_txn_crash_test)

    add_executable(chunkdb_txn_kill_test tests/txn_kill_tests.cpp)
    target_link_libraries(chunkdb_txn_kill_test PRIVATE chunkdb_core)
    target_include_directories(
        chunkdb_txn_kill_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_test(NAME txn_kill COMMAND chunkdb_txn_kill_test)

    add_executable(chunkdb_txn_concurrency_test tests/txn_concurrency_tests.cpp)
    target_link_libraries(chunkdb_txn_concurrency_test PRIVATE chunkdb_core)
    target_include_directories(
        chunkdb_txn_concurrency_test
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    add_test(NAME txn_concurrency COMMAND chunkdb_txn_concurrency_test)

    add_executable(chunkdb_feed_order_test tests/feed_order_tests.cpp)
    target_link_libraries(chunkdb_feed_order_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_feed_order_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME feed_order COMMAND chunkdb_feed_order_test)

    add_executable(chunkdb_feed_slot_records_test tests/feed_slot_records_tests.cpp)
    target_link_libraries(chunkdb_feed_slot_records_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_feed_slot_records_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME feed_slot_records COMMAND chunkdb_feed_slot_records_test)

    add_executable(chunkdb_feed_archive_reader_test tests/feed_archive_reader_tests.cpp)
    target_link_libraries(chunkdb_feed_archive_reader_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_feed_archive_reader_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME feed_archive_reader COMMAND chunkdb_feed_archive_reader_test)

    add_executable(chunkdb_feed_slots_test tests/feed_slots_tests.cpp)
    target_link_libraries(chunkdb_feed_slots_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_feed_slots_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME feed_slots COMMAND chunkdb_feed_slots_test)

    add_executable(chunkdb_feed_slots_verify_test tests/feed_slots_verify_tests.cpp)
    target_link_libraries(chunkdb_feed_slots_verify_test PRIVATE chunkdb_tools)
    target_include_directories(chunkdb_feed_slots_verify_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME feed_slots_verify COMMAND chunkdb_feed_slots_verify_test)

    add_executable(chunkdb_feed_slots_crash_test tests/feed_slots_crash_tests.cpp)
    target_link_libraries(chunkdb_feed_slots_crash_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_feed_slots_crash_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME feed_slots_crash COMMAND chunkdb_feed_slots_crash_test)

    add_executable(chunkdb_feed_slots_durability_test tests/feed_slots_durability_tests.cpp)
    target_link_libraries(chunkdb_feed_slots_durability_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_feed_slots_durability_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME feed_slots_durability COMMAND chunkdb_feed_slots_durability_test)

    add_executable(chunkdb_feed_watermark_test tests/feed_watermark_tests.cpp)
    target_link_libraries(chunkdb_feed_watermark_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_feed_watermark_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME feed_watermark COMMAND chunkdb_feed_watermark_test)

    add_executable(chunkdb_feed_lifecycle_test tests/feed_lifecycle_tests.cpp)
    target_link_libraries(chunkdb_feed_lifecycle_test PRIVATE chunkdb_core)
    target_include_directories(chunkdb_feed_lifecycle_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    add_test(NAME feed_lifecycle COMMAND chunkdb_feed_lifecycle_test)

    set(CHUNKDB_TEST_TARGETS
        chunkdb_feed_slot_records_test
        chunkdb_feed_archive_reader_test
        chunkdb_feed_slots_test
        chunkdb_feed_slots_verify_test
        chunkdb_feed_slots_crash_test
        chunkdb_feed_slots_durability_test
        chunkdb_feed_order_test
        chunkdb_feed_watermark_test
        chunkdb_feed_lifecycle_test
        chunkdb_geometry_bit_test
        chunkdb_storage_test
        chunkdb_store_manifest_test
        chunkdb_chunk_vars_test
        chunkdb_server_process_test
        chunkdb_table_catalog_test
        chunkdb_schema_test
        chunkdb_typed_columns_test
        chunkdb_schema_versions_test
        chunkdb_column_types_test
        chunkdb_narrowing_test
        chunkdb_cql_test
        chunkdb_cql_engine_test
        chunkdb_migrations_engine_test
        chunkdb_migrations_records_test
        chunkdb_txn_engine_test
        chunkdb_crypto_test
        chunkdb_scram_test
        chunkdb_users_file_test
        chunkdb_user_registry_test
        chunkdb_users_engine_test
        chunkdb_chunk_image_test
        chunkdb_wal_format_test
        chunkdb_protocol_test
        chunkdb_auth_test
        chunkdb_error_test
        chunkdb_concurrency_test
        chunkdb_uri_test
        chunkdb_server_defaults_test
        chunkdb_process_lock_test
        chunkdb_durability_test
        chunkdb_cache_eviction_test
        chunkdb_concurrency_eviction_stress_test
        chunkdb_durability_kill_recovery_test
        chunkdb_hot_contention_eviction_cycle_stress_test
        chunkdb_wal_recovery_scenarios_test
        chunkdb_wal_checkpoint_cycle_test
        chunkdb_world_ops_test
        chunkdb_world_ops_regression_test
        chunkdb_read_only_intent_replay_test
        chunkdb_wal_group_commit_test
        chunkdb_security_regression_test
        chunkdb_server_integration_test
        chunkdb_feed_completed_prefix_reader_test
        chunkdb_server_slots_protocol_test
        chunkdb_server_migrations_protocol_test
        chunkdb_server_slots_process_test
        chunkdb_server_bench_test
        chunkdb_durability_crash_hardening_test
        chunkdb_snapshot_generation_linger_test
        chunkdb_txn_history_test
        chunkdb_txn_snapshot_test
        chunkdb_txn_commit_test
        chunkdb_txn_crash_test
        chunkdb_txn_kill_test
        chunkdb_txn_concurrency_test
    )
    if(MSVC)
        set(CHUNKDB_TEST_UNDEBUG_OPTION /UNDEBUG)
    else()
        set(CHUNKDB_TEST_UNDEBUG_OPTION -UNDEBUG)
    endif()
    foreach(CHUNKDB_TEST_TARGET IN LISTS CHUNKDB_TEST_TARGETS)
        target_compile_options(${CHUNKDB_TEST_TARGET} PRIVATE ${CHUNKDB_TEST_UNDEBUG_OPTION})
    endforeach()

    # Automated guard: every test executable (named chunkdb_*_test) must be in
    # CHUNKDB_TEST_TARGETS so it keeps assertions enabled (-UNDEBUG) in every
    # build configuration, including Release. Without this a new assertion-based
    # test target could be added and silently run with asserts compiled out,
    # and a warning-as-error Release build could fail on now-unused variables.
    get_property(CHUNKDB_ALL_BUILDSYSTEM_TARGETS
        DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY BUILDSYSTEM_TARGETS)
    foreach(CHUNKDB_CANDIDATE_TARGET IN LISTS CHUNKDB_ALL_BUILDSYSTEM_TARGETS)
        if(CHUNKDB_CANDIDATE_TARGET MATCHES "^chunkdb_.*_test$")
            if(NOT CHUNKDB_CANDIDATE_TARGET IN_LIST CHUNKDB_TEST_TARGETS)
                message(FATAL_ERROR
                    "Test target '${CHUNKDB_CANDIDATE_TARGET}' is not listed in "
                    "CHUNKDB_TEST_TARGETS. Add it there so assertions stay enabled "
                    "(-UNDEBUG) in every build configuration.")
            endif()
        endif()
    endforeach()

    set(CHUNKDB_SMOKE_TESTS
        feed_slot_records
        feed_archive_reader
        feed_slots
        feed_slots_verify
        feed_slots_durability
        feed_order
        feed_watermark
        feed_lifecycle
        geometry_bit
        storage
        store_manifest
        chunk_vars
        server_process
        table_catalog
        schema
        typed_columns
        schema_versions
        column_types
        narrowing
        cql
        cql_engine
        txn_engine
        crypto
        scram
        users_file
        user_registry
        users_engine
        chunk_image
        wal_format
        protocol
        auth
        error_handling
        concurrency
        uri
        server_defaults
        process_lock
        durability
        cache_eviction
        durability_kill_recovery
        wal_recovery_scenarios
        wal_checkpoint_cycle
        wal_group_commit
        world_ops
        world_ops_regression
        read_only_intent_replay
        security_regression
        server_integration
        server_bench
        txn_history
        txn_snapshot
        txn_commit
        txn_kill
        txn_concurrency
    )
    if(WIN32)
        add_test(
            NAME server_bench_cleanup_smoke
            COMMAND chunkdb_server_bench --server-mode spawn --requests 200 --tests ping --clients 2 --port 4242
        )
        list(APPEND CHUNKDB_SMOKE_TESTS server_bench_cleanup_smoke)
    endif()
    set(CHUNKDB_STRESS_TESTS
        concurrency_eviction_stress
        hot_contention_eviction_cycle_stress
    )
    # Keep in sync with the build targets in .github/workflows/durability-crash.yml.
    set(CHUNKDB_CRASH_TESTS
        feed_slots_crash
        durability_crash_hardening
        snapshot_generation_linger
        txn_crash
    )

    set_tests_properties(${CHUNKDB_SMOKE_TESTS} PROPERTIES LABELS "smoke")
    set_tests_properties(${CHUNKDB_STRESS_TESTS} PROPERTIES LABELS "stress")
    set_tests_properties(${CHUNKDB_CRASH_TESTS} PROPERTIES LABELS "crash")
endif()

if(CHUNKDB_ENABLE_PACKAGING)
    include(GNUInstallDirs)

    install(
        TARGETS
            chunkdb_server
            chunkdb_bench
            chunkdb_server_bench
            chunkdb_verify
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )

    install(
        FILES
            README.md
            LICENSE
            docs/DURABILITY_CONTRACT.md
            docs/KNOWN_LIMITATIONS.md
            docs/RELEASE_CHECKLIST.md
        DESTINATION ${CMAKE_INSTALL_DOCDIR}
    )

    set(CPACK_PACKAGE_NAME "chunkdb")
    set(CPACK_PACKAGE_VENDOR "chunkdb")
    set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
    set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}")
    set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
    set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")
    set(CPACK_STRIP_FILES OFF)
    set(CPACK_GENERATOR "TGZ;ZIP")
    include(CPack)
endif()
