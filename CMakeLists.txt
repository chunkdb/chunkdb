cmake_minimum_required(VERSION 3.20)
project(chunk VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(CHUNKDB_BUILD_TESTS "Build chunkdb tests" ON)

add_library(chunkdb_core STATIC
    src/bit_codec.cpp
    src/chunk_store.cpp
    src/crc32.cpp
    src/file_layout.cpp
    src/geometry.cpp
)

target_include_directories(chunkdb_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_executable(chunkdb_server
    src/main.cpp
)

target_link_libraries(chunkdb_server PRIVATE chunkdb_core)

if(CHUNKDB_BUILD_TESTS)
    enable_testing()

    add_executable(chunkdb_geometry_bit_test tests/geometry_bit_tests.cpp)
    target_link_libraries(chunkdb_geometry_bit_test PRIVATE chunkdb_core)
    add_test(NAME geometry_bit COMMAND chunkdb_geometry_bit_test)

    add_executable(chunkdb_storage_test tests/storage_tests.cpp)
    target_link_libraries(chunkdb_storage_test PRIVATE chunkdb_core)
    add_test(NAME storage COMMAND chunkdb_storage_test)
endif()
