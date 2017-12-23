/**
 * @file   unit-capi-walk.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
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
#include "uri.h"

#include <iostream>

#ifdef HAVE_S3
#include "s3.h"
#endif

struct WalkFx {
  WalkFx() {
#ifdef HAVE_S3
    tiledb::Status st = s3_.connect();
    REQUIRE(st.ok());
#endif
  }

// Test directory
#ifdef HAVE_HDFS
  const std::string TEMP_DIR = "hdfs:///tiledb_test/";
  const std::string FULL_TEMP_DIR = "hdfs://localhost:9000/tiledb_test";
#elif HAVE_S3
  tiledb::S3 s3_;
  const char *S3_BUCKET = "tiledb";
  const std::string TEMP_DIR = "s3://tiledb/tiledb_test/";
  const std::string FULL_TEMP_DIR = "s3://tiledb/tiledb_test";
#else
  const std::string TEMP_DIR = "tiledb_test";
  const std::string FULL_TEMP_DIR =
      std::string("file://") + tiledb::posix::current_dir() + "/tiledb_test";
#endif

  bool dir_exists(std::string path) {
#ifdef HAVE_HDFS
    std::string cmd = std::string("hadoop fs -test -d ") + path;
    return (system(cmd.c_str()) == 0);
#elif HAVE_S3
    if (!s3_.bucket_exists(S3_BUCKET)) {
      tiledb::Status st = s3_.create_bucket(S3_BUCKET);
      REQUIRE(st.ok());
    }
    bool ret = s3_.is_dir(tiledb::URI(path));
    return ret;
#else
    std::string cmd = std::string("test -d ") + path;
    return (system(cmd.c_str()) == 0);
#endif
  }

  bool remove_dir(std::string path) {
#ifdef HAVE_HDFS
    std::string cmd = std::string("hadoop fs -rm -r -f ") + path;
    return (system(cmd.c_str()) == 0);
#elif HAVE_S3
    tiledb::Status st = s3_.remove_path(tiledb::URI(path));
    REQUIRE(st.ok());
    return true;
#else
    std::string cmd = std::string("rm -r -f ") + path;
    return (system(cmd.c_str()) == 0);
#endif
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
   *    |       |     |_ __array_metadata.tdb
   *    |       |     |_ __array_metadata.tdb
   *    |       |_ kv
   *    |             |_ __kv.tdb
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
    std::string cmd_14 =
        std::string("hadoop fs -mkdir ") + TEMP_DIR + "/dense_arrays/kv";
    std::string cmd_15 = std::string("hadoop fs -touchz ") + TEMP_DIR +
                         "/dense_arrays/kv/__kv.tdb";
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
    REQUIRE(system(cmd_14.c_str()) == 0);
    REQUIRE(system(cmd_15.c_str()) == 0);
#elif HAVE_S3
    tiledb::Status st;
    st = s3_.create_dir(tiledb::URI(TEMP_DIR));
    REQUIRE(st.ok());
    st = s3_.create_dir(tiledb::URI(TEMP_DIR + "/dense_arrays"));
    REQUIRE(st.ok());
    st = s3_.create_file(
        tiledb::URI(TEMP_DIR + "/dense_arrays/__tiledb_group.tdb"));
    REQUIRE(st.ok());
    st = s3_.create_dir(tiledb::URI(TEMP_DIR + "/dense_arrays/array_A"));
    REQUIRE(st.ok());
    st = s3_.create_file(
        tiledb::URI(TEMP_DIR + "/dense_arrays/array_A/__array_metadata.tdb"));
    REQUIRE(st.ok());
    st = s3_.create_dir(tiledb::URI(TEMP_DIR + "/dense_arrays/array_B"));
    REQUIRE(st.ok());
    st = s3_.create_file(
        tiledb::URI(TEMP_DIR + "/dense_arrays/array_B/__array_metadata.tdb"));
    REQUIRE(st.ok());
    st = s3_.create_dir(tiledb::URI(TEMP_DIR + "/sparse_arrays"));
    REQUIRE(st.ok());
    st = s3_.create_file(
        tiledb::URI(TEMP_DIR + "/sparse_arrays/__tiledb_group.tdb"));
    REQUIRE(st.ok());
    st = s3_.create_dir(tiledb::URI(TEMP_DIR + "/sparse_arrays/array_C"));
    REQUIRE(st.ok());
    st = s3_.create_file(
        tiledb::URI(TEMP_DIR + "/sparse_arrays/array_C/__array_metadata.tdb"));
    REQUIRE(st.ok());
    st = s3_.create_dir(tiledb::URI(TEMP_DIR + "/sparse_arrays/array_D"));
    REQUIRE(st.ok());
    st = s3_.create_file(
        tiledb::URI(TEMP_DIR + "/sparse_arrays/array_D/__array_metadata.tdb"));
    REQUIRE(st.ok());
    st = s3_.create_dir(tiledb::URI(TEMP_DIR + "/dense_arrays/kv"));
    REQUIRE(st.ok());
    st = s3_.create_file(tiledb::URI(TEMP_DIR + "/dense_arrays/kv/__kv.tdb"));
    REQUIRE(st.ok());
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
    std::string cmd_14 = std::string("mkdir ") + TEMP_DIR + "/dense_arrays/kv";
    std::string cmd_15 =
        std::string("touch ") + TEMP_DIR + "/dense_arrays/kv/__kv.tdb";
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
    REQUIRE(system(cmd_14.c_str()) == 0);
    REQUIRE(system(cmd_15.c_str()) == 0);
#endif
  }

  void create_golden_output(std::string *golden) {
    // Preorder traversal
    (*golden) += FULL_TEMP_DIR + "/dense_arrays GROUP\n";
    (*golden) += FULL_TEMP_DIR + "/dense_arrays/array_A ARRAY\n";
    (*golden) += FULL_TEMP_DIR + "/dense_arrays/array_B ARRAY\n";
    (*golden) += FULL_TEMP_DIR + "/dense_arrays/kv KEY_VALUE\n";
    (*golden) += FULL_TEMP_DIR + "/sparse_arrays GROUP\n";
    (*golden) += FULL_TEMP_DIR + "/sparse_arrays/array_C ARRAY\n";
    (*golden) += FULL_TEMP_DIR + "/sparse_arrays/array_D ARRAY\n";

    // Postorder traversal
    (*golden) += FULL_TEMP_DIR + "/dense_arrays/array_A ARRAY\n";
    (*golden) += FULL_TEMP_DIR + "/dense_arrays/array_B ARRAY\n";
    (*golden) += FULL_TEMP_DIR + "/dense_arrays/kv KEY_VALUE\n";
    (*golden) += FULL_TEMP_DIR + "/dense_arrays GROUP\n";
    (*golden) += FULL_TEMP_DIR + "/sparse_arrays/array_C ARRAY\n";
    (*golden) += FULL_TEMP_DIR + "/sparse_arrays/array_D ARRAY\n";
    (*golden) += FULL_TEMP_DIR + "/sparse_arrays GROUP\n";
  }

  /** Writes an object path and type to a file. */
  static int write_path(const char *path, tiledb_object_t type, void *data) {
    // Cast data to string
    auto *str = static_cast<std::string *>(data);

    // Simply print the path and type
    (*str) += (std::string(path) + " ");
    switch (type) {
      case TILEDB_ARRAY:
        (*str) += "ARRAY";
        break;
      case TILEDB_GROUP:
        (*str) += "GROUP";
        break;
      case TILEDB_KEY_VALUE:
        (*str) += "KEY_VALUE";
        break;
      default:
        (*str) += "INVALID";
    }
    (*str) += "\n";

    // Always iterate till the end
    return 1;
  }
};

TEST_CASE_METHOD(WalkFx, "C API: Test walk", "[capi], [walk]") {
  // Initialization
  clean_up();
  create_hierarchy();
  std::string golden;
  create_golden_output(&golden);
  tiledb_ctx_t *ctx;
  int rc = tiledb_ctx_create(&ctx);
  CHECK(rc == TILEDB_OK);

  // Preorder and postorder traversals
  std::string walk_str;
  rc = tiledb_walk(
      ctx, TEMP_DIR.c_str(), TILEDB_PREORDER, write_path, &walk_str);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_walk(
      ctx, TEMP_DIR.c_str(), TILEDB_POSTORDER, write_path, &walk_str);
  CHECK(rc == TILEDB_OK);

  // Check against the golden output
  CHECK_THAT(golden, Catch::Equals(walk_str));

  // Clean up
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);
  clean_up();
}
