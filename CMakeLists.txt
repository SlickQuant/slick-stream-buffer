# 3.21 is the real floor: PROJECT_IS_TOP_LEVEL below is 3.21+, and FetchContent_MakeAvailable
# is 3.14+.
cmake_minimum_required(VERSION 3.21)

project(slick-stream-buffer
        VERSION 2.0.0
        DESCRIPTION "A C++ lock-free SPMC byte stream buffer"
        LANGUAGES CXX)

# Options:
option(BUILD_SLICK_STREAM_BUFFER_TESTS "Build tests" ${PROJECT_IS_TOP_LEVEL})
# Off by default: the benchmark is a measuring tool, not part of a normal build.
option(BUILD_SLICK_STREAM_BUFFER_BENCH "Build benchmarks" OFF)

find_package(slick-shm CONFIG QUIET)

if (NOT slick-shm_FOUND)
    include(FetchContent)

    # Disable slick-shm tests and examples, but allow installation
    set(SLICK_SHM_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(SLICK_SHM_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(
        slick-shm
        GIT_REPOSITORY https://github.com/SlickQuant/slick-shm.git
        GIT_TAG v0.1.6 # See https://github.com/SlickQuant/slick-shm/releases for latest version
    )
    FetchContent_MakeAvailable(slick-shm)
else()
    message(STATUS "Found slick-shm ${slick-shm_VERSION}: ${slick-shm_DIR}")
endif()

add_library(slick-stream-buffer INTERFACE)
add_library(slick::stream_buffer ALIAS slick-stream-buffer)

target_include_directories(slick-stream-buffer INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
set_target_properties(slick-stream-buffer PROPERTIES EXPORT_NAME stream_buffer)

# The headers require C++20 ([[likely]]/[[unlikely]]). This must ride on the target, not on
# CMAKE_CXX_STANDARD: that variable applies only to targets built here, so installed consumers
# would silently compile the header under whatever standard they happen to use - C++17 via
# slick::shm, which quietly drops the branch hints.
target_compile_features(slick-stream-buffer INTERFACE cxx_std_20)

target_link_libraries(slick-stream-buffer INTERFACE slick::shm)
if(UNIX AND NOT APPLE)
    target_link_libraries(slick-stream-buffer INTERFACE rt atomic)
endif()

if(BUILD_SLICK_STREAM_BUFFER_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(BUILD_SLICK_STREAM_BUFFER_BENCH)
    add_subdirectory(bench)
endif()

# Installation rules
include(GNUInstallDirs)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# Generate and install CMake config files for vcpkg compatibility
include(CMakePackageConfigHelpers)

# Create the config file
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/slick-stream-bufferConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/slick-stream-bufferConfig.cmake"
    INSTALL_DESTINATION lib/cmake/slick-stream-buffer
)

# Create the version file
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/slick-stream-bufferConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

# Install the config files
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/slick-stream-bufferConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/slick-stream-bufferConfigVersion.cmake"
    DESTINATION lib/cmake/slick-stream-buffer
)

# Install the target
install(TARGETS slick-stream-buffer
    EXPORT slick-stream-bufferTargets
)

# Install the export set
install(EXPORT slick-stream-bufferTargets
    FILE slick-stream-bufferTargets.cmake
    NAMESPACE slick::
    DESTINATION lib/cmake/slick-stream-buffer
)

message(STATUS "${PROJECT_NAME}: ${PROJECT_VERSION}")
