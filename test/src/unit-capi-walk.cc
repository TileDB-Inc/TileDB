/**
 * @file   unit-capi-error.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * Tests for the C API walk code.
 */

#include "catch.hpp"
#include "constants.h"
#include "posix_filesystem.h"
#include "tiledb.h"

#include <iostream>

// Test directory
#ifdef HAVE_HDFS
const std::string TEMP_DIR = "hdfs:///tiledb_test/";
const std::string FULL_TEMP_DIR = "hdfs://localhost:9000/tiledb_test";
#else
const std::string TEMP_DIR = "tiledb_test";
const std::string FULL_TEMP_DIR =
    std::string("file://") + tiledb::posix::current_dir() + "/tiledb_test";
#endif

// Returns true if the input path exists
bool dir_exists(std::string path) {
#ifdef HAVE_HDFS
  std::string cmd = std::string("hadoop fs -test -d ") + path;
#else
  std::string cmd = std::string("test -d ") + path;
#endif
  return (system(cmd.c_str()) == 0);
}

// Deletes a directory
bool remove_dir(std::string path) {
#ifdef HAVE_HDFS
  std::string cmd = std::string("hadoop fs -rm -r -f ") + path;
#else
  std::string cmd = std::string("rm -rf ") + path;
#endif
  return (system(cmd.c_str()) == 0);
}

// Removes TEMP_DIR
void clean_up() {
  if (dir_exists(TEMP_DIR))
    REQUIRE(remove_dir(TEMP_DIR));
}

/**
 * Create the following directory hierarchy:
 * TEMP_DIR
 *    |_ dense_arrays
 *    |       |_ __tiledb_group.tdb
 *    |       |_ array_A
 *    |       |     |_ __array_metadata.tdb
 *    |       |_ array_B
 *    |             |_ __array_metadata.tdb
 *    |_ sparse_arrays
 *            |_ __tiledb_group.tdb
 *            |_ array_C
 *            |     |_ __array_metadata.tdb
 *            |_ array_D
 *                  |_ __array_metadata.tdb
 */
void create_hierarchy() {
#ifdef HAVE_HDFS
  std::string cmd_1 = std::string("hadoop fs -mkdir ") + TEMP_DIR;
  std::string cmd_2 =
      std::string("hadoop fs -mkdir ") + TEMP_DIR + "/dense_arrays";
  std::string cmd_3 = std::string("hadoop fs -touchz ") + TEMP_DIR +
                      "/dense_arrays/__tiledb_group.tdb";
  std::string cmd_4 =
      std::string("hadoop fs -mkdir ") + TEMP_DIR + "/dense_arrays/array_A";
  std::string cmd_5 = std::string("hadoop fs -touchz ") + TEMP_DIR +
                      "/dense_arrays/array_A/__array_metadata.tdb";
  std::string cmd_6 =
      std::string("hadoop fs -mkdir ") + TEMP_DIR + "/dense_arrays/array_B";
  std::string cmd_7 = std::string("hadoop fs -touchz ") + TEMP_DIR +
                      "/dense_arrays/array_B/__array_metadata.tdb";
  std::string cmd_8 =
      std::string("hadoop fs -mkdir ") + TEMP_DIR + "/sparse_arrays";
  std::string cmd_9 = std::string("hadoop fs -touchz ") + TEMP_DIR +
                      "/sparse_arrays/__tiledb_group.tdb";
  std::string cmd_10 =
      std::string("hadoop fs -mkdir ") + TEMP_DIR + "/sparse_arrays/array_C";
  std::string cmd_11 = std::string("hadoop fs -touchz ") + TEMP_DIR +
                       "/sparse_arrays/array_C/__array_metadata.tdb";
  std::string cmd_12 =
      std::string("hadoop fs -mkdir ") + TEMP_DIR + "/sparse_arrays/array_D";
  std::string cmd_13 = std::string("hadoop fs -touchz ") + TEMP_DIR +
                       "/sparse_arrays/array_D/__array_metadata.tdb";
#else
  std::string cmd_1 = std::string("mkdir ") + TEMP_DIR;
  std::string cmd_2 = std::string("mkdir ") + TEMP_DIR + "/dense_arrays";
  std::string cmd_3 =
      std::string("touch ") + TEMP_DIR + "/dense_arrays/__tiledb_group.tdb";
  std::string cmd_4 =
      std::string("mkdir ") + TEMP_DIR + "/dense_arrays/array_A";
  std::string cmd_5 = std::string("touch ") + TEMP_DIR +
                      "/dense_arrays/array_A/__array_metadata.tdb";
  std::string cmd_6 =
      std::string("mkdir ") + TEMP_DIR + "/dense_arrays/array_B";
  std::string cmd_7 = std::string("touch ") + TEMP_DIR +
                      "/dense_arrays/array_B/__array_metadata.tdb";
  std::string cmd_8 = std::string("mkdir ") + TEMP_DIR + "/sparse_arrays";
  std::string cmd_9 =
      std::string("touch ") + TEMP_DIR + "/sparse_arrays/__tiledb_group.tdb";
  std::string cmd_10 =
      std::string("mkdir ") + TEMP_DIR + "/sparse_arrays/array_C";
  std::string cmd_11 = std::string("touch ") + TEMP_DIR +
                       "/sparse_arrays/array_C/__array_metadata.tdb";
  std::string cmd_12 =
      std::string("mkdir ") + TEMP_DIR + "/sparse_arrays/array_D";
  std::string cmd_13 = std::string("touch ") + TEMP_DIR +
                       "/sparse_arrays/array_D/__array_metadata.tdb";
#endif
  REQUIRE(system(cmd_1.c_str()) == 0);
  REQUIRE(system(cmd_2.c_str()) == 0);
  REQUIRE(system(cmd_3.c_str()) == 0);
  REQUIRE(system(cmd_4.c_str()) == 0);
  REQUIRE(system(cmd_5.c_str()) == 0);
  REQUIRE(system(cmd_6.c_str()) == 0);
  REQUIRE(system(cmd_7.c_str()) == 0);
  REQUIRE(system(cmd_8.c_str()) == 0);
  REQUIRE(system(cmd_9.c_str()) == 0);
  REQUIRE(system(cmd_10.c_str()) == 0);
  REQUIRE(system(cmd_11.c_str()) == 0);
  REQUIRE(system(cmd_12.c_str()) == 0);
  REQUIRE(system(cmd_13.c_str()) == 0);
}

void create_golden_output(std::string* golden) {
  // Preorder traversal
  (*golden) += FULL_TEMP_DIR + "/dense_arrays GROUP\n";
  (*golden) += FULL_TEMP_DIR + "/dense_arrays/array_A ARRAY\n";
  (*golden) += FULL_TEMP_DIR + "/dense_arrays/array_B ARRAY\n";
  (*golden) += FULL_TEMP_DIR + "/sparse_arrays GROUP\n";
  (*golden) += FULL_TEMP_DIR + "/sparse_arrays/array_C ARRAY\n";
  (*golden) += FULL_TEMP_DIR + "/sparse_arrays/array_D ARRAY\n";

  // Postorder traversal
  (*golden) += FULL_TEMP_DIR + "/dense_arrays/array_A ARRAY\n";
  (*golden) += FULL_TEMP_DIR + "/dense_arrays/array_B ARRAY\n";
  (*golden) += FULL_TEMP_DIR + "/dense_arrays GROUP\n";
  (*golden) += FULL_TEMP_DIR + "/sparse_arrays/array_C ARRAY\n";
  (*golden) += FULL_TEMP_DIR + "/sparse_arrays/array_D ARRAY\n";
  (*golden) += FULL_TEMP_DIR + "/sparse_arrays GROUP\n";
}

/** Writes an object path and type to a file. */
int write_path(const char* path, tiledb_object_t type, void* data) {
  // Cast data to string
  auto* str = static_cast<std::string*>(data);

  // Simply print the path and type
  (*str) += (std::string(path) + " ");
  switch (type) {
    case TILEDB_ARRAY:
      (*str) += "ARRAY";
      break;
    case TILEDB_GROUP:
      (*str) += "GROUP";
      break;
    default:
      (*str) += "INVALID";
  }
  (*str) += "\n";

  // Always iterate till the end
  return 1;
}

TEST_CASE("C API: Test walk", "[capi], [walk]") {
  // Initialization
  clean_up();
  create_hierarchy();
  std::string golden;
  create_golden_output(&golden);

  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_create(&ctx);
  CHECK(rc == TILEDB_OK);

  // Preorder and postorder traversals
  std::string walk_str;
  tiledb_walk(ctx, TEMP_DIR.c_str(), TILEDB_PREORDER, write_path, &walk_str);
  tiledb_walk(ctx, TEMP_DIR.c_str(), TILEDB_POSTORDER, write_path, &walk_str);

  // Check against the golden output
  CHECK_THAT(golden, Catch::Equals(walk_str));

  // Clean up
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);
  clean_up();
}
