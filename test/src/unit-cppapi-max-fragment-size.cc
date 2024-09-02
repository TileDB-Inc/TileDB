/**
 * @file   unit-cppapi-max-fragment-size.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB Inc.
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
 * Tests the C++ API for maximum fragment size.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/common/scoped_executor.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/constants.h"

#include <numeric>

using namespace tiledb;

struct CPPMaxFragmentSizeFx {
  const int max_domain = 1000000;
  const char* array_name = "cpp_max_fragment_size";

  CPPMaxFragmentSizeFx()
      : vfs_(ctx_) {
    if (vfs_.is_dir(array_name)) {
      vfs_.remove_dir(array_name);
    }
  }

  void create_simple_sparse_array() {
    // Create a simple schema with one int dimension and one int attribute.
    Domain domain(ctx_);
    auto d1 = Dimension::create<int>(ctx_, "d1", {{1, max_domain}}, 2);
    domain.add_dimensions(d1);

    auto a1 = Attribute::create<int>(ctx_, "a1");

    ArraySchema schema(ctx_, TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.add_attributes(a1);
    schema.set_capacity(10);

    Array::create(array_name, schema);
  }

  void write_simple_sparse_array(
      uint64_t fragment_size,
      uint64_t start_val,
      uint64_t step,
      std::vector<uint64_t> write_sizes) {
    // Open array and create query.
    Array array(ctx_, array_name, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);

    // Set the maximum size for the fragments.
    query.ptr().get()->query_->set_fragment_size(fragment_size);
    query.set_layout(TILEDB_GLOBAL_ORDER);

    // Perform writes of the requested sizes.
    for (auto num_vals : write_sizes) {
      // Fill in the dimension and attribute with increasing numbers.
      std::vector<int> d1_buff(num_vals);
      std::vector<int> a1_buff(num_vals);

      for (int i = 0; i < static_cast<int>(num_vals); i++) {
        d1_buff[i] = start_val + 1 + i * step;
        a1_buff[i] = start_val + i * step;
      }

      // Perform the write.
      query.set_data_buffer("d1", d1_buff);
      query.set_data_buffer("a1", a1_buff);
      REQUIRE(query.submit() == Query::Status::COMPLETE);

      start_val += num_vals;
    }

    // Finalize the query.
    query.finalize();
  }

  void read_simple_sparse_array(uint64_t num_vals) {
    std::vector<int> d1_buff(num_vals);
    std::vector<int> a1_buff(num_vals);

    // Read the whole array.
    Array array(ctx_, array_name, TILEDB_READ);
    Query query(ctx_, array, TILEDB_READ);
    query.set_data_buffer("d1", d1_buff);
    query.set_data_buffer("a1", a1_buff);
    query.set_layout(TILEDB_GLOBAL_ORDER);
    REQUIRE(query.submit() == Query::Status::COMPLETE);

    // Validate each data points.
    for (int i = 0; i < static_cast<int>(num_vals); i++) {
      CHECK(d1_buff[i] == i + 1);
      CHECK(a1_buff[i] == i);
    }
  }

  void create_complex_sparse_array() {
    // Create a schema with two dimensions, one int attribute and one string
    // attribute. The second dimension will only have one possible value to
    // keep the data order simple for validation.
    Domain domain(ctx_);
    auto d1 = Dimension::create<int>(ctx_, "d1", {{1, max_domain}}, 2);
    auto d2 = Dimension::create<int>(ctx_, "d2", {{1, 1}}, 1);
    domain.add_dimensions(d1, d2);

    auto a1 = Attribute::create<int>(ctx_, "a1");

    auto a2 = Attribute::create<std::string>(ctx_, "a2");
    a2.set_nullable(true);

    ArraySchema schema(ctx_, TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.add_attributes(a1, a2);
    schema.set_capacity(10);

    Array::create(array_name, schema);
  }

  void write_complex_sparse_array(
      uint64_t fragment_size,
      uint64_t start_val,
      uint64_t step,
      std::vector<uint64_t> write_sizes) {
    // Open array and create query.
    Array array(ctx_, array_name, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);

    // Set the maximum size for the fragments.
    query.ptr().get()->query_->set_fragment_size(fragment_size);
    query.set_layout(TILEDB_GLOBAL_ORDER);

    // Perform writes of the requested sizes.
    for (auto num_vals : write_sizes) {
      // Fill one the dimension and attributes with increasing numbers. One
      // dimension will have the same value across the board.
      std::vector<int> d1_buff(num_vals);
      std::vector<int> d2_buff(num_vals, 1);
      std::vector<int> a1_buff(num_vals);

      // For the string attribute, convert the increasing value from int to
      // string.
      std::vector<uint64_t> a2_offsets;
      std::vector<uint8_t> a2_val(num_vals, 1);
      a2_offsets.reserve(num_vals);
      std::string a2_var;
      uint64_t offset = 0;

      for (int i = 0; i < static_cast<int>(num_vals); i++) {
        d1_buff[i] = start_val + 1 + i * step;
        a1_buff[i] = start_val + i * step;
        auto val = std::to_string(start_val + i * step);
        a2_offsets.emplace_back(offset);
        offset += val.size();
        a2_var += val;
      }

      // Perform the write.
      query.set_data_buffer("d1", d1_buff);
      query.set_data_buffer("d2", d2_buff);
      query.set_data_buffer("a1", a1_buff);
      query.set_offsets_buffer("a2", a2_offsets);
      query.set_data_buffer("a2", a2_var);
      query.set_validity_buffer("a2", a2_val);
      REQUIRE(query.submit() == Query::Status::COMPLETE);

      start_val += num_vals;
    }

    // Finalize the query.
    query.finalize();
  }

  void read_complex_sparse_array(uint64_t num_vals) {
    std::vector<int> d1_buff(num_vals);
    std::vector<int> d2_buff(num_vals);
    std::vector<int> a1_buff(num_vals);
    std::vector<uint64_t> a2_offsets(num_vals);
    std::vector<uint8_t> a2_val(num_vals);
    std::string a2_var;
    a2_var.resize(num_vals * std::to_string(num_vals).size());

    // Read the whole array.
    Array array(ctx_, array_name, TILEDB_READ);
    Query query(ctx_, array, TILEDB_READ);
    query.set_data_buffer("d1", d1_buff);
    query.set_data_buffer("d2", d2_buff);
    query.set_data_buffer("a1", a1_buff);
    query.set_data_buffer("a2", a2_var);
    query.set_offsets_buffer("a2", a2_offsets);
    query.set_validity_buffer("a2", a2_val);
    query.set_layout(TILEDB_GLOBAL_ORDER);
    REQUIRE(query.submit() == Query::Status::COMPLETE);

    // Validate each data points.
    uint64_t offset = 0;
    for (int i = 0; i < static_cast<int>(num_vals); i++) {
      CHECK(d1_buff[i] == i + 1);
      CHECK(d2_buff[i] == 1);
      CHECK(a1_buff[i] == i);

      auto val = std::to_string(i);
      CHECK(a2_offsets[i] == offset);
      for (uint64_t c = 0; c < val.size(); c++) {
        CHECK(a2_var[offset + c] == val[c]);
      }
      offset += val.size();
      CHECK(a2_val[i] == 1);
    }
  }

  void consolidate_fragments(
      uint64_t max_fragment_size = std::numeric_limits<uint64_t>::max()) {
    auto config = ctx_.config();
    config["sm.consolidation.max_fragment_size"] = max_fragment_size;
    config["sm.mem.consolidation.buffers_weight"] = "1";
    config["sm.mem.consolidation.reader_weight"] = "5000";
    config["sm.mem.consolidation.writer_weight"] = "5000";
    Array::consolidate(ctx_, array_name, &config);
  }

  void vacuum_fragments() {
    auto config = ctx_.config();
    Array::vacuum(ctx_, array_name, &config);
  }

  void consolidate_commits() {
    auto config = ctx_.config();
    config["sm.consolidation.mode"] = "commits";
    Array::consolidate(ctx_, array_name, &config);
  }

  void vacuum_commits() {
    auto config = ctx_.config();
    config["sm.vacuum.mode"] = "commits";
    Array::vacuum(ctx_, array_name, &config);
  }

  void check_num_commits_files(
      uint64_t exp_num_wrt,
      uint64_t exp_num_con_commits,
      uint64_t exp_num_ign,
      uint64_t exp_num_vac) {
    tiledb::test::CommitsDirectory commits_dir(vfs_, array_name);
    CHECK(
        commits_dir.file_count(tiledb::sm::constants::write_file_suffix) ==
        exp_num_wrt);
    CHECK(
        commits_dir.file_count(
            tiledb::sm::constants::con_commits_file_suffix) ==
        exp_num_con_commits);
    CHECK(
        commits_dir.file_count(tiledb::sm::constants::ignore_file_suffix) ==
        exp_num_ign);
    CHECK(
        commits_dir.file_count(tiledb::sm::constants::vacuum_file_suffix) ==
        exp_num_vac);
  }

  void validate_disjoint_domains() {
    // Load non empty domains from fragment info.
    FragmentInfo fragment_info(ctx_, array_name);
    fragment_info.load();
    auto num_frags = fragment_info.fragment_num();
    std::vector<std::pair<int, int>> non_empty_domains(num_frags);
    for (uint64_t f = 0; f < num_frags; f++) {
      fragment_info.get_non_empty_domain(f, 0, &non_empty_domains[f]);
    }
    std::sort(non_empty_domains.begin(), non_empty_domains.end());

    // Make sure the non empty domains are now disjoint.
    CHECK(non_empty_domains[0].first == 1);
    CHECK(non_empty_domains[non_empty_domains.size() - 1].second == 10000);
    for (uint64_t f = 0; f < num_frags - 1; f++) {
      CHECK(non_empty_domains[f].second + 1 == non_empty_domains[f + 1].first);
    }
  }

  ~CPPMaxFragmentSizeFx() {
    if (vfs_.is_dir(array_name)) {
      vfs_.remove_dir(array_name);
    }
  }

  Context ctx_;
  VFS vfs_;
};

TEST_CASE_METHOD(
    CPPMaxFragmentSizeFx,
    "C++ API: Max fragment size, simple schema",
    "[cppapi][max-frag-size][simple]") {
  create_simple_sparse_array();

  SECTION("no continuations") {
    write_simple_sparse_array(10000, 0, 1, {10000});
  }

  SECTION("with continuations") {
    write_simple_sparse_array(10000, 0, 1, {5000, 2495, 2505});
  }
  read_simple_sparse_array(10000);
  CHECK(tiledb::test::num_fragments(array_name) == 15);
}

TEST_CASE_METHOD(
    CPPMaxFragmentSizeFx,
    "C++ API: Max fragment size, complex schema",
    "[cppapi][max-frag-size][complex]") {
  create_complex_sparse_array();

  SECTION("no continuations") {
    write_complex_sparse_array(10000, 0, 1, {10000});
  }

  SECTION("with continuations") {
    write_complex_sparse_array(10000, 0, 1, {5000, 2495, 2505});
  }

  read_complex_sparse_array(10000);
  CHECK(tiledb::test::num_fragments(array_name) == 39);
}

TEST_CASE_METHOD(
    CPPMaxFragmentSizeFx,
    "C++ API: Max fragment size, consolidate multiple fragments write",
    "[cppapi][max-frag-size][consolidate]") {
  create_simple_sparse_array();
  write_simple_sparse_array(10000, 0, 1, {5000, 2495, 2505});
  CHECK(tiledb::test::num_fragments(array_name) == 15);
  write_simple_sparse_array(
      std::numeric_limits<uint64_t>::max(), 10000, 1, {100});
  CHECK(tiledb::test::num_fragments(array_name) == 16);

  // Run fragment consolidation and vacuum.
  check_num_commits_files(1, 1, 0, 0);
  consolidate_fragments();
  check_num_commits_files(2, 1, 0, 1);
  vacuum_fragments();
  check_num_commits_files(1, 1, 1, 0);
  read_simple_sparse_array(10100);

  // Run commits consolidation, it should clean up the commits directory.
  consolidate_commits();
  check_num_commits_files(1, 2, 1, 0);
  vacuum_commits();
  check_num_commits_files(0, 1, 0, 0);
  read_simple_sparse_array(10100);
}

TEST_CASE_METHOD(
    CPPMaxFragmentSizeFx,
    "C++ API: Max fragment size, disentangle fragments, simple schema",
    "[cppapi][max-frag-size][simple][consolidate][disentangle]") {
  create_simple_sparse_array();

  // Write 2 fragments with alternating values.
  write_simple_sparse_array(std::numeric_limits<uint64_t>::max(), 0, 2, {5000});
  write_simple_sparse_array(std::numeric_limits<uint64_t>::max(), 1, 2, {5000});

  // Run fragment consolidation and vacuum.
  consolidate_fragments(10000);
  check_num_commits_files(2, 1, 0, 1);
  vacuum_fragments();
  check_num_commits_files(0, 1, 0, 0);
  read_simple_sparse_array(10000);

  // Validate the fragment domains are now disjoint.
  validate_disjoint_domains();
}

TEST_CASE_METHOD(
    CPPMaxFragmentSizeFx,
    "C++ API: Max fragment size, disentangle fragments, complex schema",
    "[cppapi][max-frag-size][complex][consolidate][disentangle]") {
  create_complex_sparse_array();

  // Write 2 fragments with alternating values.
  write_complex_sparse_array(
      std::numeric_limits<uint64_t>::max(), 0, 2, {5000});
  write_complex_sparse_array(
      std::numeric_limits<uint64_t>::max(), 1, 2, {5000});

  // Run fragment consolidation and vacuum.
  consolidate_fragments(10000);
  check_num_commits_files(2, 1, 0, 1);
  vacuum_fragments();
  check_num_commits_files(0, 1, 0, 0);
  read_complex_sparse_array(10000);

  // Validate the fragment domains are now disjoint.
  validate_disjoint_domains();
}

// This test exists to show the lack of a bug in the GlobalOrderWriter when
// using the maximum fragment size setting. Previously, we could get into a
// situation when a write starts and the currently active fragment can't fit
// any more tiles. Before the changes in this PR we ended up in a convoluted
// code path that eventually leads us to writing the wrong last_tile_cell_num
// value in the FragmentMetdata stored on disk. This issue is then not detected
// until a read against the last tile of the fragment detects a mismatch in the
// expected size when Tile::load_chunk_data is called.
//
// The underlying bug had two specific contributing factors. First, the use of
// std::vector::operator[] is specified as undefined behavior for out-of-bounds
// reads. In our case, we ended up calling dim_tiles[-1].cell_num() which
// returned a non-obvious garbage value rather than segfaulting or some other
// obviously wrong value.
//
// The second part of this bug is how we get there. The GlobalOrderWriter has a
// mode for writing fragments up to a certain size. When we resumed a write
// with a partially filled fragment on disk, we did not check whether the first
// tile would fit in the existing fragment. This failure to check lead us to
// attempt to write zero tiles to the existing fragment which is how the
// bad call to dim_tiles[-1] happened. The fix for this part of the bug is
// simply to call GlobalOrderWriter::start_new_fragment so the current fragment
// is flushed and committed and the write can continue as normal.
//
// If you're looking at this thinking this should be in a regression test, you
// would be correct. Except for the fact that the regression tests are only
// linked against the shared library and not TIELDB_CORE_OBJECTS. The issue here
// is that the `Query::set_fragment_size` is not wrapped by the C API so we
// have to link against TILEDB_CORE_OBJECTS.
TEST_CASE(
    "Global Order Writer Resume Writes Bug is Fixed",
    "[global-order-writer][bug][sc34072]") {
  std::string array_name = "cpp_max_fragment_size_bug";
  Context ctx;

  auto cleanup = [&]() {
    auto obj = Object::object(ctx, array_name);
    if (obj.type() == Object::Type::Array) {
      Object::remove(ctx, array_name);
    }
  };

  // Remove any existing arrays.
  cleanup();

  // Remove the array at the end of this test.
  ScopedExecutor deferred(cleanup);

  // Create a sparse array (dense arrays are unaffected)
  auto dim = Dimension::create<uint64_t>(ctx, "dim", {{0, UINT64_MAX - 1}});
  Domain domain(ctx);
  domain.add_dimension(dim);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.set_domain(domain);
  schema.set_capacity(1024 * 1024);

  Array::create(array_name, schema);

  std::vector<uint64_t> data(1024 * 1024);

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);

  // Set our max fragment size to force fragment writes
  query.ptr()->query_->set_fragment_size(1080000);

  query.set_layout(TILEDB_GLOBAL_ORDER).set_data_buffer("dim", data);

  std::iota(data.begin(), data.end(), 0);
  REQUIRE(query.submit() == Query::Status::COMPLETE);

  std::iota(data.begin(), data.end(), 1024 * 1024);
  REQUIRE(query.submit() == Query::Status::COMPLETE);

  // Consolidate without a max fragment size showing that we can read the
  // entire array.
  REQUIRE_NOTHROW(Array::consolidate(ctx, array_name));

  array.close();
}

TEST_CASE(
    "Setting max_fragment_size in Dense consolidation",
    "[global-order-writer][max-frag-size-consolidation]") {
  std::string array_name = "cpp_max_fragment_size_bug";
  Context ctx;

  auto cleanup = [&]() {
    auto obj = Object::object(ctx, array_name);
    if (obj.type() == Object::Type::Array) {
      Object::remove(ctx, array_name);
    }
  };

  cleanup();

  bool more_than_one_loop = false;
  SECTION("More than one loop write") {
    more_than_one_loop = true;
  }

  SECTION("One loop write") {
    more_than_one_loop = false;
  }

  // Remove the array at the end of this test.
  ScopedExecutor deferred(cleanup);

  // Create an array with exactly 9 tiles and tile extend 1
  Domain domain(ctx);
  ArraySchema schema(ctx, TILEDB_DENSE);
  auto d1 = tiledb::Dimension::create<int32_t>(ctx, "d1", {{0, 2}}, 1);
  auto d2 = tiledb::Dimension::create<int32_t>(ctx, "d2", {{0, 2}}, 1);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  auto a1 = tiledb::Attribute::create<int32_t>(ctx, "a");
  schema.add_attribute(a1);

  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.set_domain(domain);

  Array::create(array_name, schema);

  // Populate array with data from 1 to 9
  int value = 0;
  for (int i = 0; i < 3; i++) {
    Array array(ctx, array_name, TILEDB_WRITE);
    Query query(ctx, array);
    query.set_layout(TILEDB_ROW_MAJOR);
    tiledb::Subarray sub(ctx, array);
    sub.set_subarray({i, i, 0, 2});
    query.set_subarray(sub);
    std::vector<int32_t> data = {++value, ++value, ++value};
    query.set_data_buffer("a", data);
    query.submit();
    array.close();
  }

  // Read data to validate write and num of fragments.
  CHECK(tiledb::test::num_fragments(array_name) == 3);

  Array array(ctx, array_name, TILEDB_READ);
  tiledb::Subarray sub(ctx, array);
  sub.set_subarray({0, 2, 0, 2});
  std::vector<int32_t> a(9);
  Query query(ctx, array, TILEDB_READ);
  query.set_subarray(sub).set_layout(TILEDB_ROW_MAJOR).set_data_buffer("a", a);
  query.submit();
  array.close();

  for (int i = 0; i < 9; i++) {
    CHECK(a[i] == i + 1);
  }

  // Consolidate with a size limitation for the fragment. This will result in
  // the creation of two new fragments.
  tiledb::Config cfg;
  cfg.set("sm.consolidation.max_fragment_size", "150");
  cfg.set("sm.consolidation.buffer_size", "10000");  // speed up consolidation
  if (more_than_one_loop) {
    cfg.set("sm.consolidation.buffer_size", "10");
  }

  ctx = Context(cfg);
  Array::consolidate(ctx, array_name);
  Array::vacuum(ctx, array_name);

  // Check that we now have 2 fragments instead of 3
  CHECK(tiledb::test::num_fragments(array_name) == 2);

  // Read data to validate correctness
  Array array2(ctx, array_name, TILEDB_READ);
  tiledb::Subarray sub2(ctx, array2);
  sub2.set_subarray({0, 2, 0, 2});
  std::vector<int32_t> a2(9);
  Query query2(ctx, array2, TILEDB_READ);
  query2.set_subarray(sub2)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a2);
  query2.submit();
  array2.close();

  for (int i = 0; i < 9; i++) {
    CHECK(a2[i] == i + 1);
  }
}

TEST_CASE(
    "Setting max_fragment_size in Dense consolidation one dim",
    "[global-order-writer][max-frag-size-consolidation]") {
  std::string array_name = "cpp_max_fragment_size_bug";
  Context ctx;

  auto cleanup = [&]() {
    auto obj = Object::object(ctx, array_name);
    if (obj.type() == Object::Type::Array) {
      Object::remove(ctx, array_name);
    }
  };

  cleanup();

  // Remove the array at the end of this test.
  ScopedExecutor deferred(cleanup);

  // Create an array with exactly 9 tiles and tile extend 1
  Domain domain(ctx);
  ArraySchema schema(ctx, TILEDB_DENSE);
  auto d1 = tiledb::Dimension::create<int32_t>(ctx, "d1", {{1, 9}}, 3);
  domain.add_dimension(d1);

  auto a1 = tiledb::Attribute::create<int32_t>(ctx, "a");
  schema.add_attribute(a1);

  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.set_domain(domain);

  Array::create(array_name, schema);

  // Populate array with data from 1 to 9
  int value = 0;
  for (int i = 1; i < 10; i += 3) {
    Array array(ctx, array_name, TILEDB_WRITE);
    Query query(ctx, array);
    query.set_layout(TILEDB_ROW_MAJOR);
    tiledb::Subarray sub(ctx, array);
    sub.set_subarray({i, i + 2});
    query.set_subarray(sub);
    std::vector<int32_t> data = {++value, ++value, ++value};
    query.set_data_buffer("a", data);
    query.submit();
    array.close();
  }

  // Read data to validate write and num of fragments.
  // CHECK(tiledb::test::num_fragments(array_name) == 3);

  Array array(ctx, array_name, TILEDB_READ);
  tiledb::Subarray sub(ctx, array);
  sub.set_subarray({1, 9});
  std::vector<int32_t> a(9);
  Query query(ctx, array, TILEDB_READ);
  query.set_subarray(sub).set_layout(TILEDB_ROW_MAJOR).set_data_buffer("a", a);
  query.submit();
  array.close();

  for (int i = 0; i < 9; i++) {
    CHECK(a[i] == i + 1);
  }

  // Consolidate with a size limitation for the fragment. This will result in
  // the creation of two new fragments.
  tiledb::Config cfg;
  cfg.set("sm.consolidation.max_fragment_size", "80");
  cfg.set("sm.consolidation.buffer_size", "10000");  // speed up consolidation
  ctx = Context(cfg);
  Array::consolidate(ctx, array_name);
  Array::vacuum(ctx, array_name);

  // Check that we now have 2 fragments instead of 3
  CHECK(tiledb::test::num_fragments(array_name) == 2);

  // Read data to validate correctness
  Array array2(ctx, array_name, TILEDB_READ);
  tiledb::Subarray sub2(ctx, array2);
  sub2.set_subarray({1, 9});
  std::vector<int32_t> a2(9);
  Query query2(ctx, array2, TILEDB_READ);
  query2.set_subarray(sub2)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a2);
  query2.submit();
  array2.close();

  for (int i = 0; i < 9; i++) {
    CHECK(a2[i] == i + 1);
  }
}
