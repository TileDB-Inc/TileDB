/**
 * @file   unit-cppapi-max-fragment-size.cc
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
 * Tests the C++ API for maximum fragment size.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/utils.h"

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
      std::vector<uint64_t> write_sizes) {
    // Open array and create query.
    Array array(ctx_, array_name, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);

    // Set the maximum size for the fragments.
    query.ptr().get()->query_->set_fragment_size(fragment_size);

    // Perform writes of the requested sizes.
    for (auto num_vals : write_sizes) {
      // Fill in the dimension and attribute with increasing numbers.
      std::vector<int> d1_buff(num_vals);
      std::iota(d1_buff.begin(), d1_buff.end(), start_val + 1);

      std::vector<int> a1_buff(num_vals);
      std::iota(a1_buff.begin(), a1_buff.end(), start_val);

      // Perform the write.
      query.set_data_buffer("d1", d1_buff);
      query.set_data_buffer("a1", a1_buff);
      query.set_layout(TILEDB_GLOBAL_ORDER);
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
      std::vector<uint64_t> write_sizes) {
    // Open array and create query.
    Array array(ctx_, array_name, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);

    // Set the maximum size for the fragments.
    query.ptr().get()->query_->set_fragment_size(fragment_size);

    // Perform writes of the requested sizes.
    for (auto num_vals : write_sizes) {
      // Fill one the dimension and attributes with increasing numbers. One
      // dimension will have the same value across the board.
      std::vector<int> d1_buff(num_vals);
      std::iota(d1_buff.begin(), d1_buff.end(), start_val + 1);

      std::vector<int> d2_buff(num_vals, 1);

      std::vector<int> a1_buff(num_vals);
      std::iota(a1_buff.begin(), a1_buff.end(), start_val);

      // For the string attribute, convert the increasing value from int to
      // string.
      std::vector<uint64_t> a2_offsets;
      std::vector<uint8_t> a2_val(num_vals, 1);
      a2_offsets.reserve(num_vals);
      std::string a2_var;
      uint64_t offset = 0;
      for (uint64_t i = start_val; i < start_val + num_vals; i++) {
        auto val = std::to_string(i);
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
      query.set_layout(TILEDB_GLOBAL_ORDER);
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

  void consolidate_fragments() {
    auto config = ctx_.config();
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
    std::string commit_dir = tiledb::test::get_commit_dir(array_name);
    std::vector<std::string> commits{vfs_.ls(commit_dir)};
    uint64_t num_wrt = 0;
    uint64_t num_con_commits = 0;
    uint64_t num_ign = 0;
    uint64_t num_vac = 0;
    for (auto commit : commits) {
      if (tiledb::sm::utils::parse::ends_with(
              commit, tiledb::sm::constants::write_file_suffix)) {
        num_wrt++;
      } else if (tiledb::sm::utils::parse::ends_with(
                     commit, tiledb::sm::constants::con_commits_file_suffix)) {
        num_con_commits++;
      } else if (tiledb::sm::utils::parse::ends_with(
                     commit, tiledb::sm::constants::ignore_file_suffix)) {
        num_ign++;
      } else if (tiledb::sm::utils::parse::ends_with(
                     commit, tiledb::sm::constants::vacuum_file_suffix)) {
        num_vac++;
      }
    }

    CHECK(num_wrt == exp_num_wrt);
    CHECK(num_con_commits == exp_num_con_commits);
    CHECK(num_ign == exp_num_ign);
    CHECK(num_vac == exp_num_vac);
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
    write_simple_sparse_array(10000, 0, {10000});
  }

  SECTION("with continuations") {
    write_simple_sparse_array(10000, 0, {5000, 2495, 2505});
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
    write_complex_sparse_array(10000, 0, {10000});
  }

  SECTION("with continuations") {
    write_complex_sparse_array(10000, 0, {5000, 2495, 2505});
  }

  read_complex_sparse_array(10000);
  CHECK(tiledb::test::num_fragments(array_name) == 39);
}

TEST_CASE_METHOD(
    CPPMaxFragmentSizeFx,
    "C++ API: Max fragment size, consolidate multiple fragments write",
    "[cppapi][max-frag-size][consolidate]") {
  create_simple_sparse_array();
  write_simple_sparse_array(10000, 0, {5000, 2495, 2505});
  CHECK(tiledb::test::num_fragments(array_name) == 15);
  write_simple_sparse_array(std::numeric_limits<uint64_t>::max(), 10000, {100});
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
