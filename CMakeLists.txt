cmake_minimum_required(VERSION 3.20)
project(chunk VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(CHUNKDB_BUILD_TESTS "Build chunkdb tests" ON)

add_library(chunkdb_core STATIC
    src/dummy.cpp
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
    add_executable(chunkdb_smoke_test tests/smoke_test.cpp)
    add_test(NAME smoke COMMAND chunkdb_smoke_test)
endif()
