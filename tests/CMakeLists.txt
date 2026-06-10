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

# Enable loss detection in tests
target_compile_definitions(slick-stream-buffer-tests PRIVATE SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION=1)

# Boost.Asio DynamicBuffer adapter tests - only the needed component (beast transitively
# provides asio). System Boost installs only export header-only libs as Boost::headers.
find_package(Boost CONFIG QUIET COMPONENTS beast)
if(NOT Boost_FOUND)
  find_package(Boost CONFIG QUIET)
endif()
if(Boost_FOUND)
  message(STATUS "Boost ${Boost_VERSION} found - building dynamic_stream_buffer tests")
  target_sources(slick-stream-buffer-tests PRIVATE dynamic_stream_buffer_tests.cpp)
  if(TARGET Boost::beast)
    target_link_libraries(slick-stream-buffer-tests PRIVATE Boost::beast)
  else()
    target_link_libraries(slick-stream-buffer-tests PRIVATE Boost::headers)
  endif()
else()
  message(STATUS "Boost not found - skipping dynamic_stream_buffer tests")
endif()

include(GoogleTest)
gtest_discover_tests(slick-stream-buffer-tests)
