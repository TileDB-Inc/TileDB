add_executable(
  test_assert
  test_assert.cc
)

target_include_directories(test_assert
  PRIVATE
    "${TILEDB_CORE_INCLUDE_DIR}"
    Catch2::Catch2
)

target_link_libraries(test_assert PUBLIC tiledb_Catch2WithMain)

target_compile_definitions(
  test_assert
  PRIVATE
  -DTILEDB_PATH_TO_TRY_ASSERT="$<TARGET_FILE:try_assert>"
  -DTILEDB_PATH_TO_TRY_LIBC_ASSERT="$<TARGET_FILE:try_libc_assert>"
)

add_executable(
  try_assert
  try_assert.cc
)

target_link_libraries(try_assert PUBLIC assert baseline)

target_include_directories(try_assert
  PRIVATE
    "${TILEDB_CORE_INCLUDE_DIR}"
    Catch2::Catch2
)

add_executable(
  try_libc_assert
  try_libc_assert.cc
)

add_dependencies(test_assert try_assert try_libc_assert)

add_test(
  NAME "test_ci_asserts"
  COMMAND test_assert
)
