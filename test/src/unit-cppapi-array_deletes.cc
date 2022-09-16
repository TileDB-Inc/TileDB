/**
 * @file   unit-cppapi-array_deletes.cc
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
#include "helpers.h"
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
    "C++ API: Deletion of invalid fragment writes",
    "[cppapi][array_deletes][fragments][invalid]") {
  /* Note: An array must be open in MODIFY_EXCLUSIVE mode to delete_fragments */
  std::vector<int> data = {0, 1};
  uint64_t timestamp_start = 0;
  uint64_t timestamp_end = UINT64_MAX;
  auto array = tiledb::Array(ctx, array_name, TILEDB_WRITE);
  auto query = tiledb::Query(ctx, array, TILEDB_WRITE);
  query.set_data_buffer("a", data).set_subarray({0, 1}).submit();
  query.set_data_buffer("a", data).set_subarray({2, 3}).submit();
  query.set_data_buffer("a", data).set_subarray({4, 5}).submit();
  query.finalize();

  // Delete fragments
  CHECK(tiledb::test::num_fragments(array_name) == 3);
  REQUIRE_THROWS_WITH(
      array.delete_fragments(array_name, timestamp_start, timestamp_end),
      Catch::Contains("Query type must be MODIFY_EXCLUSIVE"));
  CHECK(tiledb::test::num_fragments(array_name) == 3);
  array.close();
}

TEST_CASE_METHOD(
    CPPArrayDeletesFx,
    "C++ API: Deletion of valid fragment writes",
    "[cppapi][array_deletes][fragments][valid]") {
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

TEST_CASE_METHOD(
    CPPArrayDeletesFx,
    "C++ API: Deletion of fragment writes consolidated with timestamps",
    "[cppapi][array_deletes][fragments][consolidation_with_timestamps]") {
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
