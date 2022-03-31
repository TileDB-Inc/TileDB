include(TileDBAssertions)

find_package(Catch_EP REQUIRED)

add_executable(
  test_assert
  test_assert.cc
)

target_link_libraries(test_assert PUBLIC Catch2::Catch2)

if (TILEDB_ASSERTIONS)
  target_compile_definitions(
    test_assert
    PRIVATE
    -DTILEDB_ASSERTIONS
  )
endif()

add_executable(
  try_assert
  try_assert.cc
)

add_dependencies(test_assert try_assert)

add_test(
  NAME "test_ci_asserts"
  COMMAND test_assert
)