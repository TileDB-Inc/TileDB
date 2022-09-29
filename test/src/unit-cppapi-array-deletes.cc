/**
 * @file   unit-cppapi-array-deletes.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests the C++ API for array delete related functions.
 */

#include <test/support/tdb_catch.h>
#include <test/support/src/helpers.h>
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb;

struct CPPArrayDeletesFx {
  Context ctx;
  VFS vfs;
  const std::string array_name = "cpp_unit_array_deletes";

  CPPArrayDeletesFx()
      : vfs(ctx) {
    using namespace tiledb;

    if (vfs.is_dir(array_name))
      vfs.remove_dir(array_name);

    Domain domain(ctx);
    domain.add_dimension(Dimension::create<int>(ctx, "d", {{0, 11}}, 12));

    ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
    schema.add_attribute(Attribute::create<int>(ctx, "a"));
    tiledb::Array::create(array_name, schema);
  }

  ~CPPArrayDeletesFx() {
    if (vfs.is_dir(array_name))
      vfs.remove_dir(array_name);
  }

  void read_array(tiledb::Array array) {
    // Read from the array
    array.open(TILEDB_READ);
    std::vector<int> subarray = {0, 1};
    std::vector<int> a_read(2);
    Query query_r(ctx, array);
    query_r.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_read);
    query_r.submit();
    array.close();

    for (int i = 0; i < 2; i++) {
      REQUIRE(a_read[i] == i);
    }
  }

  Array write_array() {
    std::vector<int> data = {0, 1};
    auto array = tiledb::Array(ctx, array_name, TILEDB_MODIFY_EXCLUSIVE);
    auto query = tiledb::Query(ctx, array, TILEDB_MODIFY_EXCLUSIVE);
    query.set_data_buffer("a", data).set_subarray({0, 1}).submit();
    query.set_data_buffer("a", data).set_subarray({2, 3}).submit();
    query.set_data_buffer("a", data).set_subarray({4, 5}).submit();
    query.set_data_buffer("a", data).set_subarray({6, 7}).submit();
    CHECK(tiledb::test::num_fragments(array_name) == 4);
    array.close();
    return array;
  }

  Array write_fragments() {
    std::vector<int> data = {0, 1};

    // Write fragment at timestamp 1
    auto array = tiledb::Array(ctx, array_name, TILEDB_MODIFY_EXCLUSIVE, 1);
    auto query = tiledb::Query(ctx, array, TILEDB_MODIFY_EXCLUSIVE);
    query.set_data_buffer("a", data).set_subarray({0, 1}).submit();
    array.close();
    CHECK(tiledb::test::num_fragments(array_name) == 1);

    // Write fragment at timestamp 3
    array.open(TILEDB_MODIFY_EXCLUSIVE, 3);
    query.set_data_buffer("a", data).set_subarray({2, 3}).submit();
    array.close();
    CHECK(tiledb::test::num_fragments(array_name) == 2);

    // Write fragment at timestamp 5
    array.open(TILEDB_MODIFY_EXCLUSIVE, 5);
    query.set_data_buffer("a", data).set_subarray({4, 5}).submit();
    array.close();
    CHECK(tiledb::test::num_fragments(array_name) == 3);

    // Write fragment at timestamp 7
    array.open(TILEDB_MODIFY_EXCLUSIVE, 7);
    query.set_data_buffer("a", data).set_subarray({6, 7}).submit();
    query.finalize();
    array.close();
    CHECK(tiledb::test::num_fragments(array_name) == 4);

    return array;
  }
};

TEST_CASE_METHOD(
    CPPArrayDeletesFx,
    "C++ API: Deletion of invalid writes",
    "[cppapi][array-deletes][invalid]") {
  /* Note: An array must be open in MODIFY_EXCLUSIVE mode to delete data */
  std::vector<int> data = {0, 1};
  auto array = tiledb::Array(ctx, array_name, TILEDB_WRITE);
  auto query = tiledb::Query(ctx, array, TILEDB_WRITE);
  query.set_data_buffer("a", data).set_subarray({0, 1}).submit();
  query.set_data_buffer("a", data).set_subarray({2, 3}).submit();
  query.set_data_buffer("a", data).set_subarray({4, 5}).submit();
  query.finalize();

  // Ensure expected data was written
  CHECK(tiledb::test::num_commits(array_name) == 3);
  CHECK(tiledb::test::num_fragments(array_name) == 3);
  auto schemas =
      vfs.ls(array_name + "/" + tiledb::sm::constants::array_schema_dir_name);
  CHECK(schemas.size() == 1);

  // Try to delete data
  REQUIRE_THROWS_WITH(
      array.delete_array(array_name),
      Catch::Contains("Query type must be MODIFY_EXCLUSIVE"));
  array.close();

  // Ensure nothing was deleted
  CHECK(tiledb::test::num_commits(array_name) == 3);
  CHECK(tiledb::test::num_fragments(array_name) == 3);
  schemas =
      vfs.ls(array_name + "/" + tiledb::sm::constants::array_schema_dir_name);
  CHECK(schemas.size() == 1);
}

TEST_CASE_METHOD(
    CPPArrayDeletesFx,
    "C++ API: Deletion of fragment writes",
    "[cppapi][array-deletes][fragments]") {
  // Write fragments at timestamps 1, 3, 5, 7
  auto array = write_fragments();
  std::string commit_dir = tiledb::test::get_commit_dir(array_name);
  std::vector<std::string> commits{vfs.ls(commit_dir)};

  // Conditionally consolidate and vacuum
  bool consolidate = GENERATE(true, false);
  bool vacuum = GENERATE(true, false);
  if (!consolidate && vacuum) {
    return;
  }

  if (consolidate) {
    // Consolidate commits
    auto config = ctx.config();
    config["sm.consolidation.mode"] = "commits";
    Array::consolidate(ctx, array_name, &config);

    // Validate working directory
    CHECK(tiledb::test::num_fragments(array_name) == 4);
    commits = vfs.ls(commit_dir);
    CHECK(commits.size() == 5);
    bool con_exists = false;
    for (auto commit : commits) {
      if (tiledb::sm::utils::parse::ends_with(
              commit, tiledb::sm::constants::con_commits_file_suffix)) {
        con_exists = true;
      }
    }
    CHECK(con_exists);
  }

  if (vacuum) {
    // Vacumm commits
    auto config = ctx.config();
    config["sm.vacuum.mode"] = "commits";
    Array::vacuum(ctx, array_name, &config);

    // Validate working directory
    CHECK(tiledb::test::num_fragments(array_name) == 4);
    commits = vfs.ls(commit_dir);
    CHECK(commits.size() == 1);
    CHECK(tiledb::sm::utils::parse::ends_with(
        commits[0], tiledb::sm::constants::con_commits_file_suffix));
  }

  // Delete fragments
  array.open(TILEDB_MODIFY_EXCLUSIVE);
  array.delete_fragments(array_name, 2, 6);
  CHECK(tiledb::test::num_fragments(array_name) == 2);
  array.close();

  // Check commits directory after deletion
  if (consolidate) {
    int con_file_count = 0;
    int ign_file_count = 0;
    commits = vfs.ls(commit_dir);
    for (auto commit : commits) {
      if (tiledb::sm::utils::parse::ends_with(
              commit, tiledb::sm::constants::con_commits_file_suffix)) {
        con_file_count++;
      }
      if (tiledb::sm::utils::parse::ends_with(
              commit, tiledb::sm::constants::ignore_file_suffix)) {
        ign_file_count++;
      }
    }
    /* Note: An ignore file is written by delete_fragments if there are
     * consolidated commits to be ignored by the delete. */
    CHECK(con_file_count == 1);
    CHECK(ign_file_count == 1);
    if (vacuum) {
      CHECK(commits.size() == 2);
    } else {
      CHECK(commits.size() == 4);
    }
  }

  // Read from the array
  read_array(array);
}

TEST_CASE_METHOD(
    CPPArrayDeletesFx,
    "C++ API: Deletion of fragment writes consolidated with timestamps",
    "[cppapi][array-deletes][fragments][consolidation_with_timestamps]") {
  // Write fragments at timestamps 1, 3, 5, 7
  auto array = write_fragments();
  uint32_t num_commits = 4;
  int num_fragments = 4;

  // Consolidate fragments at timestamps 1 - 3
  auto config = ctx.config();
  config["sm.consolidation.mode"] = "fragments";
  config["sm.consolidation.timestamp_start"] = "1";
  config["sm.consolidation.timestamp_end"] = "3";
  Array::consolidate(ctx, array_name, &config);
  num_commits += 2;
  num_fragments++;

  // Validate working directory
  CHECK(tiledb::test::num_fragments(array_name) == num_fragments);
  std::string commit_dir = tiledb::test::get_commit_dir(array_name);
  std::vector<std::string> commits{vfs.ls(commit_dir)};
  CHECK(commits.size() == num_commits);
  bool vac_exists = false;
  for (auto commit : commits) {
    if (tiledb::sm::utils::parse::ends_with(
            commit, tiledb::sm::constants::vacuum_file_suffix)) {
      vac_exists = true;
    }
  }
  CHECK(vac_exists);

  // Conditionally vacuum fragments before deletion
  bool vacuum = GENERATE(true, false);
  if (vacuum) {
    auto config = ctx.config();
    config["sm.vacuum.mode"] = "fragments";
    Array::vacuum(ctx, array_name, &config);
    num_commits -= 3;
    num_fragments -= 2;

    // Validate working directory
    CHECK(tiledb::test::num_fragments(array_name) == num_fragments);
    commits = vfs.ls(commit_dir);
    CHECK(commits.size() == num_commits);
  }

  // Delete fragments at timestamps 2 - 4
  array.open(TILEDB_MODIFY_EXCLUSIVE);
  array.delete_fragments(array_name, 2, 4);
  if (!vacuum) {
    // Vacuum after deletion
    auto config = ctx.config();
    config["sm.vacuum.mode"] = "fragments";
    Array::vacuum(ctx, array_name, &config);
    num_commits -= 3;
    num_fragments -= 2;

    // Validate working directory
    CHECK(tiledb::test::num_fragments(array_name) == num_fragments);
    commits = vfs.ls(commit_dir);
    CHECK(commits.size() == num_commits);
  }
  array.close();

  // Validate working directory
  CHECK(tiledb::test::num_fragments(array_name) == num_fragments);
  commits = vfs.ls(commit_dir);
  CHECK(commits.size() == num_commits);

  // Read from the array
  read_array(array);
}

TEST_CASE_METHOD(
    CPPArrayDeletesFx,
    "C++ API: Deletion of array data",
    "[cppapi][array-deletes][array]") {
  // Write array data
  auto array = write_array();
  std::string extraneous_file_path = array_name + "/extraneous_file";
  vfs.touch(extraneous_file_path);

  // Write metadata
  array.open(TILEDB_MODIFY_EXCLUSIVE);
  int v = 100;
  array.put_metadata("aaa", TILEDB_INT32, 1, &v);
  array.close();

  // Check write
  CHECK(tiledb::test::num_commits(array_name) == 4);
  CHECK(tiledb::test::num_fragments(array_name) == 4);
  auto schemas =
      vfs.ls(array_name + "/" + tiledb::sm::constants::array_schema_dir_name);
  CHECK(schemas.size() == 1);
  auto meta =
      vfs.ls(array_name + "/" + tiledb::sm::constants::array_metadata_dir_name);
  CHECK(meta.size() == 1);

  // Conditionally consolidate
  /* Note: there's no need to vacuum; delete_array will delete all fragments */
  bool consolidate = GENERATE(true, false);
  std::string commit_dir = tiledb::test::get_commit_dir(array_name);
  std::vector<std::string> commits{vfs.ls(commit_dir)};

  if (consolidate) {
    // Consolidate commits
    auto config = ctx.config();
    config["sm.consolidation.mode"] = "commits";
    Array::consolidate(ctx, array_name, &config);

    // Consolidate fragment metadata
    config["sm.consolidation.mode"] = "fragment_meta";
    Array::consolidate(ctx, array_name, &config);

    // Validate working directory
    CHECK(tiledb::test::num_fragments(array_name) == 4);
    auto frag_meta = vfs.ls(
        array_name + "/" + tiledb::sm::constants::array_fragment_meta_dir_name);
    CHECK(frag_meta.size() == 1);
    commits = vfs.ls(commit_dir);
    CHECK(commits.size() == 5);
    bool con_exists = false;
    for (auto commit : commits) {
      if (tiledb::sm::utils::parse::ends_with(
              commit, tiledb::sm::constants::con_commits_file_suffix)) {
        con_exists = true;
      }
    }
    CHECK(con_exists);
  }

  // Delete array data
  array.open(TILEDB_MODIFY_EXCLUSIVE);
  array.delete_array(array_name);
  array.close();

  // Check working directory after delete
  REQUIRE(vfs.is_file(extraneous_file_path));
  CHECK(tiledb::test::num_fragments(array_name) == 0);
  schemas =
      vfs.ls(array_name + "/" + tiledb::sm::constants::array_schema_dir_name);
  CHECK(schemas.size() == 0);
  meta =
      vfs.ls(array_name + "/" + tiledb::sm::constants::array_metadata_dir_name);
  CHECK(meta.size() == 0);
  auto frag_meta = vfs.ls(
      array_name + "/" + tiledb::sm::constants::array_fragment_meta_dir_name);
  CHECK(frag_meta.size() == 0);

  // Check commit directory after delete
  if (consolidate) {
    int con_file_count = 0;
    int ign_file_count = 0;
    commits = vfs.ls(commit_dir);
    for (auto commit : commits) {
      if (tiledb::sm::utils::parse::ends_with(
              commit, tiledb::sm::constants::con_commits_file_suffix)) {
        con_file_count++;
      }
      if (tiledb::sm::utils::parse::ends_with(
              commit, tiledb::sm::constants::ignore_file_suffix)) {
        ign_file_count++;
      }
    }
    /* Note: An ignore file is written by delete_fragments if there are
     * consolidated commits to be ignored by the delete. */
    CHECK(con_file_count == 1);
    CHECK(ign_file_count == 1);
    CHECK(tiledb::test::num_commits(array_name) == 2);
  } else {
    CHECK(tiledb::test::num_commits(array_name) == 0);
  }

  // Try to read array
  REQUIRE_THROWS_WITH(
      array.open(TILEDB_READ), Catch::Contains("Array does not exist"));
}

// TODO: remove once tiledb_vfs_copy_dir is implemented for windows.
#ifndef _WIN32
TEST_CASE(
    "C++ API: Deletion of older-versioned array data",
    "[cppapi][array-deletes][array][older_version]") {
  if constexpr (is_experimental_build) {
    return;
  }
  // Get the v11 array
  const std::string array_name = "cpp_unit_array_deletes_v11";
  Context ctx;
  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
  std::string v11_arrays_dir =
      std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays/sparse_array_v11";
  vfs.copy_dir(v11_arrays_dir.c_str(), array_name);

  // Prepare buffers
  std::vector<int> buffer_a1{0, 1, 2, 3};
  std::vector<uint64_t> buffer_a2{0, 1, 3, 6};
  std::string buffer_var_a2("abbcccdddd");
  std::vector<float> buffer_a3{0.1f, 0.2f, 1.1f, 1.2f, 2.1f, 2.2f, 3.1f, 3.2f};
  std::vector<uint64_t> buffer_coords_dim1{1, 1, 1, 2};
  std::vector<uint64_t> buffer_coords_dim2{1, 2, 4, 3};

  // Write array
  auto array = tiledb::Array(ctx, array_name, TILEDB_MODIFY_EXCLUSIVE);
  auto query = tiledb::Query(ctx, array, TILEDB_MODIFY_EXCLUSIVE);
  query.set_data_buffer("a1", buffer_a1);
  query.set_data_buffer(
      "a2", (void*)buffer_var_a2.c_str(), buffer_var_a2.size());
  query.set_offsets_buffer("a2", buffer_a2);
  query.set_data_buffer("a3", buffer_a3);
  query.set_data_buffer("d1", buffer_coords_dim1);
  query.set_data_buffer("d2", buffer_coords_dim2);
  query.submit();
  query.finalize();
  array.close();
  std::string extraneous_file_path = array_name + "/extraneous_file";
  vfs.touch(extraneous_file_path);

  // Check write
  auto schemas =
      vfs.ls(array_name + "/" + tiledb::sm::constants::array_schema_dir_name);
  CHECK(schemas.size() == 1);
  auto uris = vfs.ls(array_name);
  bool ok_exists = false;
  std::string ok_prefix;
  for (auto uri : uris) {
    if (tiledb::sm::utils::parse::ends_with(
            uri, tiledb::sm::constants::ok_file_suffix)) {
      ok_exists = true;
      ok_prefix = uri.substr(0, uri.find_last_of("."));
    }
  }
  CHECK(ok_exists);
  auto tdb_dir = vfs.ls(ok_prefix);
  CHECK(tdb_dir.size() == 7);
  for (auto tdb : tdb_dir) {
    CHECK(tiledb::sm::utils::parse::ends_with(
        tdb, tiledb::sm::constants::file_suffix));
  }

  // Delete array data
  array.open(TILEDB_MODIFY_EXCLUSIVE);
  array.delete_array(array_name);
  array.close();

  // Check working directory after delete
  uris = vfs.ls(array_name);
  for (auto uri : uris) {
    CHECK(!tiledb::sm::utils::parse::starts_with(uri, ok_prefix));
  }
  REQUIRE(vfs.is_file(extraneous_file_path));
  schemas =
      vfs.ls(array_name + "/" + tiledb::sm::constants::array_schema_dir_name);
  CHECK(schemas.size() == 0);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}
#endif
