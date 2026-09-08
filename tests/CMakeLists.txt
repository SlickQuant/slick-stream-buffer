find_package(GTest QUIET)

if(NOT GTest_FOUND)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.17.0
  )
  # For Windows: Prevent overriding the parent project's compiler/linker settings
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

add_executable(slick-stream-buffer-tests
  stream_buffer_tests.cpp
  stream_buffer_shm_tests.cpp
)
if(MSVC)
  add_compile_options(/wd4996)
endif()

target_link_libraries(slick-stream-buffer-tests PRIVATE slick::stream_buffer GTest::gtest_main)



include(GoogleTest)
gtest_discover_tests(slick-stream-buffer-tests)
