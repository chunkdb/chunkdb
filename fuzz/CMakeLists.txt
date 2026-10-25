# Each target is one harness source; a macro selects the decoder it reaches.
# Core is instrumented too when fuzzing; instrumenting only the entry point misses decoder bugs.
set(CHUNKDB_FUZZ_TARGETS cql request chunk_form wal image manifest slots migrations backup)
set(cql_source cql_fuzz.cpp)
set(request_source request_fuzz.cpp)
set(chunk_form_source chunk_form_fuzz.cpp)
set(wal_source storage_fuzz.cpp)
set(image_source storage_fuzz.cpp)
foreach(format manifest slots migrations backup)
    set(${format}_source metadata_fuzz.cpp)
endforeach()
set(wal_definition CHUNKDB_FUZZ_STORAGE=1)
set(image_definition CHUNKDB_FUZZ_STORAGE=2)
set(manifest_definition CHUNKDB_FUZZ_METADATA=1)
set(slots_definition CHUNKDB_FUZZ_METADATA=2)
set(migrations_definition CHUNKDB_FUZZ_METADATA=3)
set(backup_definition CHUNKDB_FUZZ_METADATA=4)

function(chunkdb_fuzz_target prefix name source)
    add_executable(${prefix}_${name} ${source})
    target_link_libraries(${prefix}_${name} PRIVATE chunkdb_core)
    target_include_directories(${prefix}_${name} PRIVATE ${PROJECT_SOURCE_DIR}/src)
    if(DEFINED ${name}_definition)
        target_compile_definitions(${prefix}_${name} PRIVATE ${${name}_definition})
    endif()
endfunction()

add_executable(chunkdb_fuzz_seeds seed_corpus.cpp)
target_link_libraries(chunkdb_fuzz_seeds PRIVATE chunkdb_core)
target_include_directories(chunkdb_fuzz_seeds PRIVATE ${PROJECT_SOURCE_DIR}/src)
set_target_properties(chunkdb_fuzz_seeds PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/fuzz/bin")

foreach(name ${CHUNKDB_FUZZ_TARGETS})
    if(CHUNKDB_FUZZ)
        chunkdb_fuzz_target(chunkdb_fuzz ${name} ${${name}_source})
        target_link_options(chunkdb_fuzz_${name} PRIVATE -fsanitize=fuzzer,address,undefined)
        set_target_properties(chunkdb_fuzz_${name} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/fuzz/bin")
        file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/fuzz/corpus/${name}")
    endif()
    if(CHUNKDB_BUILD_TESTS)
        # The same entry point without libFuzzer: generated seeds and the
        # kept crash inputs under fuzz/regression/<target> run in ctest.
        chunkdb_fuzz_target(chunkdb_fuzz_replay ${name} "${${name}_source};replay_main.cpp")
        add_test(NAME fuzz_replay_${name}
            COMMAND chunkdb_fuzz_replay_${name}
                "${CMAKE_CURRENT_SOURCE_DIR}/regression/${name}"
                "${CMAKE_BINARY_DIR}/fuzz/replay-seeds/${name}")
        set_tests_properties(fuzz_replay_${name} PROPERTIES
            TIMEOUT 120 LABELS "smoke" FIXTURES_REQUIRED fuzz_seeds)
    endif()
endforeach()

if(CHUNKDB_BUILD_TESTS)
    add_test(NAME fuzz_seeds COMMAND chunkdb_fuzz_seeds "${CMAKE_BINARY_DIR}/fuzz/replay-seeds")
    set_tests_properties(fuzz_seeds PROPERTIES TIMEOUT 120 LABELS "smoke" FIXTURES_SETUP fuzz_seeds)
endif()
