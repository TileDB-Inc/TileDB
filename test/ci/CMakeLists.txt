include(TileDBAssertions)

find_package(Catch2 REQUIRED)

add_executable(
  test_assert
  test_assert.cc
)

target_include_directories(test_assert
  PRIVATE
    "${TILEDB_CORE_INCLUDE_DIR}"
    Catch2::Catch2
)

target_link_libraries(test_assert PUBLIC Catch2::Catch2WithMain)

if (TILEDB_ASSERTIONS)
  target_compile_definitions(
    test_assert
    PRIVATE
    -DTILEDB_ASSERTIONS
  )
endif()

target_compile_definitions(
  test_assert
  PRIVATE
  -DTILEDB_PATH_TO_TRY_ASSERT="$<TARGET_FILE:try_assert>"
)

add_executable(
  try_assert
  try_assert.cc
)

add_dependencies(test_assert try_assert)

add_test(
  NAME "test_ci_asserts"
  COMMAND test_assert
)
