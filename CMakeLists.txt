cmake_minimum_required(VERSION 3.10)

project(slick-stream-buffer
        VERSION 1.0.2
        DESCRIPTION "A C++ lock-free SPMC byte stream buffer with a Boost.Asio DynamicBuffer adapter"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)

# Options:
option(BUILD_SLICK_STREAM_BUFFER_TESTS "Build tests" ${PROJECT_IS_TOP_LEVEL})

find_package(slick-shm CONFIG QUIET)

if (NOT slick-shm_FOUND)
    include(FetchContent)

    # Disable slick-shm tests and examples, but allow installation
    set(SLICK_SHM_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(SLICK_SHM_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(
        slick-shm
        GIT_REPOSITORY https://github.com/SlickQuant/slick-shm.git
        GIT_TAG v0.1.4
    )
    FetchContent_MakeAvailable(slick-shm)
else()
    message(STATUS "slick-shm: ${slick-shm_VERSION}")
endif()

add_library(slick-stream-buffer INTERFACE)
add_library(slick::stream_buffer ALIAS slick-stream-buffer)

target_include_directories(slick-stream-buffer INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
set_target_properties(slick-stream-buffer PROPERTIES EXPORT_NAME stream_buffer)

target_link_libraries(slick-stream-buffer INTERFACE slick::shm)
if(UNIX AND NOT APPLE)
    target_link_libraries(slick-stream-buffer INTERFACE rt atomic)
endif()

if(BUILD_SLICK_STREAM_BUFFER_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Installation rules
install(DIRECTORY include/ DESTINATION include)

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
