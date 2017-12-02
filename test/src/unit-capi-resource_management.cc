/**
 * @file   unit-capi-resource_management.cc
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
 * Tests for the C API resource management code
 */

#include "catch.hpp"
#include "posix_filesystem.h"
#include "tiledb.h"

#include <cassert>
#include <iostream>

struct ResourceMgmtRx {
// Workspace folder name
#ifdef HAVE_HDFS
  const std::string URI_PREFIX = "hdfs://";
  const std::string TEMP_DIR = "/tiledb_test/";
#else
  const std::string URI_PREFIX = "file://";
  const std::string TEMP_DIR = tiledb::posix::current_dir() + "/";
#endif
  const std::string GROUP = "my_group/";

  // TileDB context
  tiledb_ctx_t* ctx_;

  ResourceMgmtRx() {
    // Initialize context
    int rc = tiledb_ctx_create(&ctx_);
    if (rc != TILEDB_OK) {
      std::cerr << "ResourceMgmtRx() Error creating tiledb_ctx_t" << std::endl;
      std::exit(1);
    }
    // cleanup temporary test group if it exists
    if (dir_exists(TEMP_DIR + GROUP)) {
      bool success = remove_dir(TEMP_DIR + GROUP);
      if (!success) {
        tiledb_ctx_free(ctx_);
        std::cerr << "ResourceMgmtRx() Error existing deleting test group"
                  << std::endl;
        std::exit(1);
      }
    }
  }

  ~ResourceMgmtRx() {
    // Finalize TileDB context
    tiledb_ctx_free(ctx_);

    // cleanup temporary test group if it exists
    bool success = remove_dir(TEMP_DIR + GROUP);
    if (!success) {
      std::cerr << "ResourceMgmtRx() Error deleting test group" << std::endl;
      std::exit(1);
    }
  }

  bool dir_exists(std::string path) {
#ifdef HAVE_HDFS
    std::string cmd = std::string("hadoop fs -test -d ") + path;
#else
    std::string cmd = std::string("test -d ") + path;
#endif
    return (system(cmd.c_str()) == 0);
  }

  bool remove_dir(std::string path) {
#ifdef HAVE_HDFS
    std::string cmd = std::string("hadoop fs -rm -r -f ") + path;
#else
    std::string cmd = std::string("rm -r -f ") + path;
#endif
    return (system(cmd.c_str()) == 0);
  }

  std::string group_path(std::string path = "") {
    return URI_PREFIX + TEMP_DIR + GROUP + path;
  }

  void create_test_array(std::string array_uri) {
    tiledb_attribute_t* a1;
    tiledb_attribute_create(ctx_, &a1, "a1", TILEDB_FLOAT32);

    // Domain and tile extents
    int64_t dim_domain[] = {
        1,
    };
    int64_t tile_extents[] = {
        1,
    };

    // Create dimension
    tiledb_dimension_t* d1;
    tiledb_dimension_create(
        ctx_, &d1, "d1", TILEDB_INT64, &dim_domain[0], &tile_extents[0]);

    // Domain
    tiledb_domain_t* domain;
    tiledb_domain_create(ctx_, &domain, TILEDB_INT64);
    tiledb_domain_add_dimension(ctx_, domain, d1);

    // Create array_metadata metadata
    tiledb_array_metadata_t* array_metadata;
    tiledb_array_metadata_create(ctx_, &array_metadata, array_uri.c_str());
    tiledb_array_metadata_set_array_type(ctx_, array_metadata, TILEDB_DENSE);
    tiledb_array_metadata_set_domain(ctx_, array_metadata, domain);
    tiledb_array_metadata_add_attribute(ctx_, array_metadata, a1);

    // Check array metadata
    REQUIRE(tiledb_array_metadata_check(ctx_, array_metadata) == TILEDB_OK);

    // Create array
    REQUIRE(tiledb_array_create(ctx_, array_metadata) == TILEDB_OK);

    tiledb_dimension_free(ctx_, d1);
  }

  const char* error_message() {
    tiledb_error_t* err;
    tiledb_error_last(ctx_, &err);
    const char* msg;
    tiledb_error_message(ctx_, err, &msg);
    return msg;
  }
};

TEST_CASE_METHOD(ResourceMgmtRx, "C API: Test TileDB Object Type", "[capi]") {
  // Test GROUP object type
  REQUIRE(tiledb_group_create(ctx_, group_path().c_str()) == TILEDB_OK);

  tiledb_object_t type;
  REQUIRE(tiledb_object_type(ctx_, group_path().c_str(), &type) == TILEDB_OK);
  CHECK(type == TILEDB_GROUP);

  // Test invalid object type
  REQUIRE(
      tiledb_object_type(ctx_, group_path("test_array").c_str(), &type) ==
      TILEDB_OK);
  CHECK(type == TILEDB_INVALID);

  // Test array object type
  create_test_array(group_path("test_array"));
  REQUIRE(
      tiledb_object_type(ctx_, group_path("test_array").c_str(), &type) ==
      TILEDB_OK);
  CHECK(type == TILEDB_ARRAY);
}

TEST_CASE_METHOD(ResourceMgmtRx, "C API: Test TileDB Delete", "[capi]") {
  REQUIRE(tiledb_group_create(ctx_, group_path().c_str()) == TILEDB_OK);

  // Test deleting TileDB Objects
  create_test_array(group_path("test_array").c_str());
  CHECK(tiledb_delete(ctx_, group_path("test_array").c_str()) == TILEDB_OK);

  REQUIRE(
      tiledb_group_create(ctx_, group_path("test_group").c_str()) == TILEDB_OK);
  CHECK(tiledb_delete(ctx_, group_path("test_group").c_str()) == TILEDB_OK);

  // Deleting an invalid path should raise an error
  CHECK(tiledb_delete(ctx_, group_path("foo").c_str()) == TILEDB_ERR);

  // Test recursive group delete
  tiledb_object_t type;
  REQUIRE(
      tiledb_group_create(ctx_, group_path("level1/").c_str()) == TILEDB_OK);
  REQUIRE(
      tiledb_group_create(ctx_, group_path("level1/level2/").c_str()) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_group_create(ctx_, group_path("level1/level2/level3/").c_str()) ==
      TILEDB_OK);

  REQUIRE(
      tiledb_object_type(ctx_, group_path("level1/").c_str(), &type) ==
      TILEDB_OK);
  CHECK(type == TILEDB_GROUP);
  REQUIRE(
      tiledb_object_type(ctx_, group_path("level1/level2/").c_str(), &type) ==
      TILEDB_OK);
  CHECK(type == TILEDB_GROUP);
  REQUIRE(
      tiledb_object_type(
          ctx_, group_path("level1/level2/level3").c_str(), &type) ==
      TILEDB_OK);
  CHECK(type == TILEDB_GROUP);

  REQUIRE(tiledb_delete(ctx_, group_path("level1/").c_str()) == TILEDB_OK);
  REQUIRE(
      tiledb_object_type(
          ctx_, group_path("level1/level2/level3").c_str(), &type) ==
      TILEDB_OK);
  CHECK(type == TILEDB_INVALID);
  REQUIRE(
      tiledb_object_type(ctx_, group_path("level1/level2/").c_str(), &type) ==
      TILEDB_OK);
  CHECK(type == TILEDB_INVALID);
  REQUIRE(
      tiledb_object_type(ctx_, group_path("level1/").c_str(), &type) ==
      TILEDB_OK);
  CHECK(type == TILEDB_INVALID);
}

TEST_CASE_METHOD(ResourceMgmtRx, "C API: Test TileDB Move", "[capi]") {
  // Move group
  REQUIRE(tiledb_group_create(ctx_, group_path().c_str()) == TILEDB_OK);
  REQUIRE(
      tiledb_group_create(ctx_, group_path("old_group1").c_str()) == TILEDB_OK);
  REQUIRE(
      tiledb_group_create(ctx_, group_path("old_group2").c_str()) == TILEDB_OK);

  CHECK(
      tiledb_move(
          ctx_,
          group_path("old_group1").c_str(),
          group_path("new_group1").c_str(),
          false) == TILEDB_OK);

  tiledb_object_t type;
  CHECK(
      tiledb_object_type(ctx_, group_path("new_group1").c_str(), &type) ==
      TILEDB_OK);
  CHECK(type == TILEDB_GROUP);

  // Check error on name conflict
  CHECK(
      tiledb_move(
          ctx_,
          group_path("new_group1").c_str(),
          group_path("old_group2").c_str(),
          false) == TILEDB_ERR);

  // Check force move works on name conflict
  CHECK(
      tiledb_move(
          ctx_,
          group_path("new_group1").c_str(),
          group_path("old_group2").c_str(),
          true) == TILEDB_OK);

  // Check move array
  create_test_array(group_path("test_array").c_str());
  CHECK(
      tiledb_move(
          ctx_,
          group_path("test_array").c_str(),
          group_path("new_test_array").c_str(),
          false) == TILEDB_OK);

  // Check error on invalid path
  CHECK(
      tiledb_move(ctx_, "invalid_path", "another_invalid_path", false) ==
      TILEDB_ERR);
}