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

#include <test/support/assert_helpers.h>
#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>
#include "test/support/rapidcheck/array_templates.h"
#include "test/support/src/array_helpers.h"
#include "test/support/src/array_templates.h"
#include "test/support/src/fragment_info_helpers.h"
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/fragment_info/fragment_info_api_internal.h"
#include "tiledb/api/c_api/subarray/subarray_api_internal.h"
#include "tiledb/common/arithmetic.h"
#include "tiledb/common/scoped_executor.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/query/writers/global_order_writer.h"
#include "tiledb/sm/tile/arithmetic.h"
#include "tiledb/sm/tile/test/arithmetic.h"
#include "tiledb/sm/tile/tile.h"

#include <numeric>
#include <ranges>

using namespace tiledb;
using namespace tiledb::test;

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

/**
 * @return the number of cells contained within a subarray, or `std::nullopt` if
 * overflow
 */
std::optional<uint64_t> subarray_num_cells(
    std::span<const templates::Domain<uint64_t>> subarray) {
  uint64_t num_cells = 1;
  for (const auto& dim : subarray) {
    auto maybe = checked_arithmetic<uint64_t>::mul(num_cells, dim.num_cells());
    if (!maybe.has_value()) {
      return std::nullopt;
    }
    num_cells = maybe.value();
  }
  return num_cells;
}

/**
 * Creates an array with the provided `dimensions` and then
 * runs a global order write into `subarray` using `max_fragment_size` to bound
 * the fragment size.
 *
 * Asserts that all created fragments respect `max_fragment_size` and that the
 * data read back out for `subarray` matches what we wrote into it.
 *
 * @return a list of the domains written to each fragment in ascending order
 */
template <templates::FragmentType F, typename Asserter = AsserterCatch>
std::vector<std::vector<templates::Domain<uint64_t>>>
instance_dense_global_order(
    const Context& ctx,
    const std::string& array_name,
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order,
    uint64_t max_fragment_size,
    const std::vector<templates::Dimension<Datatype::UINT64>>& dimensions,
    const std::vector<templates::Domain<uint64_t>>& subarray,
    const F& attributes,
    std::optional<uint64_t> write_unit_num_cells = std::nullopt) {
  Domain domain(ctx);
  for (uint64_t d = 0; d < dimensions.size(); d++) {
    const std::string dname = "d" + std::to_string(d);
    auto dim = Dimension::create<uint64_t>(
        ctx,
        dname,
        {{dimensions[d].domain.lower_bound, dimensions[d].domain.upper_bound}},
        dimensions[d].extent);
    domain.add_dimension(dim);
  }

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.set_tile_order(tile_order);
  schema.set_cell_order(cell_order);

  const std::vector<std::tuple<Datatype, uint32_t, bool>> ddl_attributes =
      templates::ddl::physical_type_attributes<F>();
  for (uint64_t a = 0; a < ddl_attributes.size(); a++) {
    const std::string aname = "a" + std::to_string(a + 1);
    auto aa =
        Attribute::create(
            ctx,
            aname,
            static_cast<tiledb_datatype_t>(std::get<0>(ddl_attributes[a])))
            .set_cell_val_num(std::get<1>(ddl_attributes[a]))
            .set_nullable(std::get<2>(ddl_attributes[a]));
    schema.add_attribute(aa);
  }

  Array::create(array_name, schema);
  test::DeleteArrayGuard del(ctx.ptr().get(), array_name.c_str());

  std::vector<uint64_t> api_subarray;
  api_subarray.reserve(2 * subarray.size());
  for (const auto& sub_dim : subarray) {
    api_subarray.push_back(sub_dim.lower_bound);
    api_subarray.push_back(sub_dim.upper_bound);
  }

  std::vector<uint64_t> tile_extents;
  for (const auto& dimension : dimensions) {
    tile_extents.push_back(dimension.extent);
  }

  sm::NDRange smsubarray;

  // write data, should be split into multiple fragments
  templates::query::fragment_field_sizes_t<F> cursor;
  {
    Array array(ctx, array_name, TILEDB_WRITE);

    Subarray sub(ctx, array);
    sub.set_subarray(api_subarray);

    Query query(ctx, array, TILEDB_WRITE);
    query.set_layout(TILEDB_GLOBAL_ORDER);
    query.set_subarray(sub);
    query.ptr().get()->query_->set_fragment_size(max_fragment_size);

    smsubarray = sub.ptr()->subarray()->ndrange(0);

    sm::NDRange smsubarray_aligned = smsubarray;
    array.schema()
        .ptr()
        ->array_schema()
        ->domain()
        .expand_to_tiles_when_no_current_domain(smsubarray_aligned);

    uint64_t cells_written = 0;
    while (templates::query::num_cells<Asserter>(attributes, cursor) <
           attributes.num_cells()) {
      const uint64_t cells_this_write = std::min(
          attributes.num_cells() - cells_written,
          write_unit_num_cells.value_or(attributes.num_cells()));

      const F attributes_this_write =
          attributes.slice(cells_written, cells_this_write);

      auto field_sizes = templates::query::make_field_sizes<Asserter, F>(
          attributes_this_write, cells_this_write);
      templates::query::accumulate_cursor(
          attributes_this_write, cursor, field_sizes);

      templates::query::set_fields<Asserter, F>(
          ctx.ptr().get(),
          query.ptr().get(),
          field_sizes,
          const_cast<F&>(attributes_this_write),
          [](unsigned d) { return "d" + std::to_string(d + 1); },
          [](unsigned a) { return "a" + std::to_string(a + 1); });

      const auto status = query.submit();
      ASSERTER(status == Query::Status::COMPLETE);

      const uint64_t cells_written_this_write =
          templates::query::num_cells<Asserter>(
              attributes_this_write, field_sizes);
      ASSERTER(cells_written_this_write == cells_this_write);

      cells_written += cells_written_this_write;
      ASSERTER(
          cells_written ==
          templates::query::num_cells<Asserter>(attributes, cursor));

      const auto w = dynamic_cast<const sm::GlobalOrderWriter*>(
          query.ptr()->query_->strategy());
      ASSERTER(w);
      const auto g = w->get_global_state();
      ASSERTER(g);

      // Check assumptions about memory buffering.
      // There may be a tail of tiles for which we cannot infer whether they
      // would fit in the current fragment while also forming a rectangle.
      // The writer keeps these in memory until it has enough information
      // in the next `submit`. Check our assumptions about those tiles.
      uint64_t in_memory_size = 0;
      std::optional<uint64_t> in_memory_num_tiles;
      for (const auto& field : g->last_tiles_) {
        // NB: there should always be at least one tile which contains the
        // state of the current fragment
        ASSERTER(!field.second.empty());

        for (uint64_t t = 0; t < field.second.size() - 1; t++) {
          const auto s = field.second[t].filtered_size();
          ASSERTER(s.has_value());
          in_memory_size += s.value();
        }

        if (in_memory_num_tiles.has_value()) {
          ASSERTER(field.second.size() - 1 == in_memory_num_tiles.value());
        } else {
          in_memory_num_tiles = field.second.size() - 1;
        }
      }
      // it should be an error if they exceed the max fragment size
      ASSERTER(in_memory_size <= max_fragment_size);

      // and if they form a rectangle then we could have written some out
      ASSERTER(in_memory_num_tiles.has_value());
      for (uint64_t num_tiles = 0; num_tiles < in_memory_num_tiles.value();
           num_tiles++) {
        const sm::IsRectangularDomain rectangle =
            sm::is_rectangular_domain<uint64_t>(
                static_cast<sm::Layout>(tile_order),
                tile_extents,
                smsubarray_aligned,
                g->dense_.domain_tile_offset_,
                g->frag_meta_->tile_index_base() + num_tiles);
        if (num_tiles == 0) {
          ASSERTER(rectangle == sm::IsRectangularDomain::Yes);
        } else {
          // if `Never` then we should have started a new fragment
          // to avoid buffering up until we hit the tile size
          ASSERTER(rectangle == sm::IsRectangularDomain::No);
        }
      }
    }

    query.finalize();
  }

  // then read back
  F read;
  {
    templates::query::resize(read, cursor);

    Array array(ctx, array_name, TILEDB_READ);

    Subarray sub(ctx, array);
    sub.set_subarray(api_subarray);

    Query query(ctx, array, TILEDB_READ);
    query.set_layout(TILEDB_GLOBAL_ORDER);
    query.set_subarray(sub);

    auto read_field_sizes =
        templates::query::make_field_sizes<Asserter, F>(read);
    templates::query::set_fields<Asserter>(
        ctx.ptr().get(),
        query.ptr().get(),
        read_field_sizes,
        read,
        [](unsigned d) { return "d" + std::to_string(d + 1); },
        [](unsigned a) { return "a" + std::to_string(a + 1); });

    auto st = query.submit();
    ASSERTER(st == Query::Status::COMPLETE);

    ASSERTER(read_field_sizes == cursor);
  }

  const std::vector<std::vector<templates::Domain<uint64_t>>> fragment_domains =
      collect_and_validate_fragment_domains<uint64_t, Asserter>(
          ctx,
          static_cast<sm::Layout>(tile_order),
          array_name,
          tile_extents,
          smsubarray,
          max_fragment_size);

  // this is last because a fragment domain mismatch is more informative
  ASSERTER(read == attributes);

  return fragment_domains;
}

template <typename Asserter>
std::vector<std::vector<templates::Domain<uint64_t>>>
instance_dense_global_order(
    const Context& ctx,
    const std::string& array_name,
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order,
    uint64_t max_fragment_size,
    const std::vector<templates::Dimension<Datatype::UINT64>>& dimensions,
    const std::vector<templates::Domain<uint64_t>>& subarray,
    std::optional<uint64_t> write_unit_num_cells = std::nullopt) {
  const std::optional<uint64_t> num_cells = subarray_num_cells(subarray);
  ASSERTER(num_cells.has_value());

  const int a_offset = 77;
  std::vector<int> a_write;
  a_write.reserve(num_cells.value());
  for (int i = 0; i < static_cast<int64_t>(num_cells.value()); i++) {
    a_write.push_back(a_offset + i);
  }

  templates::Fragment<std::tuple<>, std::tuple<int>> attributes;
  std::get<0>(attributes.attributes()) = a_write;

  return instance_dense_global_order<decltype(attributes), Asserter>(
      ctx,
      array_name,
      tile_order,
      cell_order,
      max_fragment_size,
      dimensions,
      subarray,
      attributes,
      write_unit_num_cells);
}

/**
 * Tests that the max fragment size parameter is properly respected
 * for global order writes to dense arrays.
 */
TEST_CASE("C++ API: Max fragment size dense array", "[cppapi][max-frag-size]") {
  VFSTestSetup vfs;
  Context ctx(vfs.ctx());

  const std::string array_name =
      vfs.array_uri("max_fragment_size_dense_global_order");

  const tiledb_layout_t tile_order =
      GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  const tiledb_layout_t cell_order =
      GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

  DYNAMIC_SECTION(
      "tile_order = " << sm::layout_str(static_cast<sm::Layout>(tile_order))
                      << ", cell_order = "
                      << sm::layout_str(static_cast<sm::Layout>(cell_order))) {
    // each tile is a full row of a 2D array
    // NB: since each tile is a whole row we observe the same results regardless
    // of tile order
    SECTION("Row tiles") {
      using Dim = templates::Dimension<Datatype::UINT64>;
      using Dom = templates::Domain<uint64_t>;

      constexpr uint64_t max_fragment_size = 64 * 1024;

      constexpr size_t span_d2 = 10000;
      const std::vector<Dim> dimensions = {
          Dim(0, std::numeric_limits<uint64_t>::max() - 1, 1),
          Dim(0, span_d2 - 1, span_d2)};

      const uint64_t base_d1 = 12345;
      const uint64_t num_rows = GENERATE(1, 2, 4, 8);
      const std::vector<Dom> subarray = {
          Dom(base_d1 + 0, base_d1 + num_rows - 1), Dom(0, span_d2 - 1)};

      const uint64_t write_unit_num_cells = GENERATE(0, 64, 1024, 1024 * 1024);

      DYNAMIC_SECTION(
          "num_rows = " << num_rows << ", write_unit_num_cells = "
                        << write_unit_num_cells) {
        const auto actual = instance_dense_global_order<AsserterCatch>(
            ctx,
            array_name,
            tile_order,
            cell_order,
            max_fragment_size,
            dimensions,
            subarray,
            write_unit_num_cells == 0 ?
                std::nullopt :
                std::optional<uint64_t>{write_unit_num_cells});

        std::vector<std::vector<Dom>> expect;
        for (uint64_t r = 0; r < num_rows; r++) {
          expect.push_back(
              {Dom(base_d1 + r, base_d1 + r), Dom(0, span_d2 - 1)});
        }

        CHECK(expect == actual);
      }
    }

    // each tile is some rectangle of a 2D array
    SECTION("Rectangle tiles") {
      using Dim = templates::Dimension<Datatype::UINT64>;
      using Dom = templates::Domain<uint64_t>;

      const uint64_t d1_extent = GENERATE(8, 4);
      constexpr size_t d2_span = 10000;
      REQUIRE(d2_span % d1_extent == 0);  // for row major

      const uint64_t d1_subarray = 16;
      REQUIRE(d2_span % d1_subarray == 0);  // for column major

      const std::vector<Dim> dimensions = {
          Dim(0, std::numeric_limits<uint64_t>::max() - 1, d1_extent),
          Dim(0, d2_span - 1, d2_span / d1_extent)};

      const uint64_t d1_start_offset = GENERATE(0, 1);
      const uint64_t d1_end_offset = GENERATE(0, 1);
      const uint64_t d1_start = 100 + d1_start_offset;
      const uint64_t d1_end = d1_start + d1_subarray - 1 - d1_end_offset;
      const std::vector<Dom> subarray = {
          Dom(d1_start, d1_end), Dom(0, d2_span - 1)};

      const uint64_t max_fragment_size = 4 * 64 * 1024;

      const uint64_t write_unit_num_cells = GENERATE(0, 64, 1024, 1024 * 1024);

      DYNAMIC_SECTION(
          "start_offset = "
          << d1_start_offset << ", end_offset = " << d1_end_offset
          << ", extent = " << d1_extent
          << ", write_unit_num_cells = " << write_unit_num_cells) {
        if (d1_extent == 8) {
          const auto expect = Catch::Matchers::ContainsSubstring(
              "Fragment size is too small to subdivide dense subarray into "
              "multiple fragments");
          REQUIRE_THROWS(
              instance_dense_global_order<AsserterCatch>(
                  ctx,
                  array_name,
                  tile_order,
                  cell_order,
                  max_fragment_size,
                  dimensions,
                  subarray),
              expect);
        } else if (d1_start_offset + d1_end_offset > 0) {
          // if this constraint is ever relaxed this test must be extended
          // with new inputs which are offset within a tile
          const auto expect = Catch::Matchers::ContainsSubstring(
              "the subarray must coincide with the tile bounds");
          REQUIRE_THROWS(
              instance_dense_global_order<AsserterCatch>(
                  ctx,
                  array_name,
                  tile_order,
                  cell_order,
                  max_fragment_size,
                  dimensions,
                  subarray,
                  write_unit_num_cells == 0 ?
                      std::nullopt :
                      std::optional<uint64_t>(write_unit_num_cells)),
              expect);
        } else {
          std::vector<std::vector<Dom>> expect;
          if (tile_order == TILEDB_ROW_MAJOR) {
            expect = {
                {Dom(d1_start + 0 * d1_extent, d1_start + 1 * d1_extent - 1),
                 Dom(0, d2_span - 1)},
                {Dom(d1_start + 1 * d1_extent, d1_start + 2 * d1_extent - 1),
                 Dom(0, d2_span - 1)},
                {Dom(d1_start + 2 * d1_extent, d1_start + 3 * d1_extent - 1),
                 Dom(0, d2_span - 1)},
                {Dom(d1_start + 3 * d1_extent, d1_start + 4 * d1_extent - 1),
                 Dom(0, d2_span - 1)}};
          } else {
            expect = {
                {Dom(d1_start, d1_start + d1_subarray - 1),
                 Dom(0 * (d2_span / 4), 1 * (d2_span / 4) - 1)},
                {Dom(d1_start, d1_start + d1_subarray - 1),
                 Dom(1 * (d2_span / 4), 2 * (d2_span / 4) - 1)},
                {Dom(d1_start, d1_start + d1_subarray - 1),
                 Dom(2 * (d2_span / 4), 3 * (d2_span / 4) - 1)},
                {Dom(d1_start, d1_start + d1_subarray - 1),
                 Dom(3 * (d2_span / 4), 4 * (d2_span / 4) - 1)},
            };
          }

          const auto actual = instance_dense_global_order<AsserterCatch>(
              ctx,
              array_name,
              tile_order,
              cell_order,
              max_fragment_size,
              dimensions,
              subarray);

          CHECK(expect == actual);
        }
      }
    }

    // Each tile is a rectangular prism of height 1
    // Use the same inputs as above except there is a third outer dimension with
    // extent 1
    SECTION("Flat rectangular prism tiles") {
      using Dim = templates::Dimension<Datatype::UINT64>;
      using Dom = templates::Domain<uint64_t>;

      const uint64_t d0_extent = 1;
      const Dom d0_height(0, 0);

      const uint64_t d1_extent = GENERATE(8, 4);
      constexpr size_t d2_span = 10000;
      REQUIRE(d2_span % d1_extent == 0);  // for row major

      const uint64_t d1_subarray = 16;
      REQUIRE(d2_span % d1_subarray == 0);  // for column major

      const std::vector<Dim> dimensions = {
          Dim(0, std::numeric_limits<uint64_t>::max() - 1, d0_extent),
          Dim(0, std::numeric_limits<uint64_t>::max() - 1, d1_extent),
          Dim(0, d2_span - 1, d2_span / d1_extent)};

      const uint64_t d1_start_offset = GENERATE(0, 1);
      const uint64_t d1_end_offset = GENERATE(0, 1);
      const uint64_t d1_start = 100 + d1_start_offset;
      const uint64_t d1_end = d1_start + d1_subarray - 1 - d1_end_offset;
      const std::vector<Dom> subarray = {
          d0_height, Dom(d1_start, d1_end), Dom(0, d2_span - 1)};

      const uint64_t max_fragment_size = 4 * 64 * 1024;

      const uint64_t write_unit_num_cells = GENERATE(0, 64, 1024, 1024 * 1024);

      DYNAMIC_SECTION(
          "start_offset = "
          << d1_start_offset << ", end_offset = " << d1_end_offset
          << ", extent = " << d1_extent
          << ", write_unit_num_cells = " << write_unit_num_cells) {
        if (d1_extent == 8) {
          const auto expect = Catch::Matchers::ContainsSubstring(
              "Fragment size is too small to subdivide dense subarray into "
              "multiple fragments");
          REQUIRE_THROWS(
              instance_dense_global_order<AsserterCatch>(
                  ctx,
                  array_name,
                  tile_order,
                  cell_order,
                  max_fragment_size,
                  dimensions,
                  subarray),
              expect);
        } else if (d1_start_offset + d1_end_offset > 0) {
          // if this constraint is ever relaxed this test must be extended
          // with new inputs which are offset within a tile
          const auto expect = Catch::Matchers::ContainsSubstring(
              "the subarray must coincide with the tile bounds");
          REQUIRE_THROWS(instance_dense_global_order<AsserterCatch>(
              ctx,
              array_name,
              tile_order,
              cell_order,
              max_fragment_size,
              dimensions,
              subarray,
              write_unit_num_cells == 0 ?
                  std::nullopt :
                  std::optional<uint64_t>(write_unit_num_cells)));
        } else {
          std::vector<std::vector<Dom>> expect;
          if (tile_order == TILEDB_ROW_MAJOR) {
            expect = {
                {d0_height,
                 Dom(d1_start + 0 * d1_extent, d1_start + 1 * d1_extent - 1),
                 Dom(0, d2_span - 1)},
                {d0_height,
                 Dom(d1_start + 1 * d1_extent, d1_start + 2 * d1_extent - 1),
                 Dom(0, d2_span - 1)},
                {d0_height,
                 Dom(d1_start + 2 * d1_extent, d1_start + 3 * d1_extent - 1),
                 Dom(0, d2_span - 1)},
                {d0_height,
                 Dom(d1_start + 3 * d1_extent, d1_start + 4 * d1_extent - 1),
                 Dom(0, d2_span - 1)}};
          } else {
            expect = {
                {d0_height,
                 Dom(d1_start, d1_start + d1_subarray - 1),
                 Dom(0 * (d2_span / 4), 1 * (d2_span / 4) - 1)},
                {d0_height,
                 Dom(d1_start, d1_start + d1_subarray - 1),
                 Dom(1 * (d2_span / 4), 2 * (d2_span / 4) - 1)},
                {d0_height,
                 Dom(d1_start, d1_start + d1_subarray - 1),
                 Dom(2 * (d2_span / 4), 3 * (d2_span / 4) - 1)},
                {d0_height,
                 Dom(d1_start, d1_start + d1_subarray - 1),
                 Dom(3 * (d2_span / 4), 4 * (d2_span / 4) - 1)},
            };
          }

          const auto actual = instance_dense_global_order<AsserterCatch>(
              ctx,
              array_name,
              tile_order,
              cell_order,
              max_fragment_size,
              dimensions,
              subarray);

          CHECK(expect == actual);
        }
      }
    }
  }

  // examples found from the rapidcheck test
  SECTION("Shrinking") {
    using Dim = templates::Dimension<Datatype::UINT64>;
    using Dom = templates::Domain<uint64_t>;

    SECTION("Example 1") {
      Dim d1(0, 0, 1);
      Dim d2(0, 0, 1);
      Dom s1(0, 0);
      Dom s2(0, 0);
      const uint64_t max_fragment_size = 24;

      instance_dense_global_order<AsserterCatch>(
          ctx,
          array_name,
          tile_order,
          cell_order,
          max_fragment_size,
          {d1, d2},
          {s1, s2});
    }

    SECTION("Example 2") {
      Dim d1(1, 26, 2);
      Dim d2(0, 0, 1);
      Dom s1(1, 2);
      Dom s2(0, 0);
      const uint64_t max_fragment_size = 28;

      instance_dense_global_order<AsserterCatch>(
          ctx,
          array_name,
          tile_order,
          cell_order,
          max_fragment_size,
          {d1, d2},
          {s1, s2});
    }
  }
}

/**
 * @return a generator which prdocues subarrays whose bounds are aligned to the
 * tiles of `arraydomain`
 */
namespace rc {
template <sm::Datatype D>
Gen<std::vector<typename templates::Dimension<D>::domain_type>>
make_tile_aligned_subarray(
    const std::vector<templates::Dimension<D>>& arraydomain) {
  using Dom = typename templates::Dimension<D>::domain_type;

  // dense subarrays have to be aligned to tile boundaries
  // so choose the tiles in each dimension that the subarray will overlap
  std::vector<Gen<templates::Domain<uint64_t>>> gen_subarray_tiles;
  for (const auto& dimension : arraydomain) {
    const uint64_t tile_ub =
        (dimension.domain.upper_bound - dimension.domain.lower_bound) /
        dimension.extent;
    gen_subarray_tiles.push_back(make_range(
        templates::Domain<uint64_t>(0, std::min<uint64_t>(64, tile_ub))));
  }

  return gen::exec([gen_subarray_tiles, arraydomain]() {
    std::vector<templates::Domain<uint64_t>> subarray_tiles;
    for (const auto& gen_dim : gen_subarray_tiles) {
      subarray_tiles.push_back(*gen_dim);
    }

    std::vector<Dom> subarray;
    auto to_subarray = [&]() -> std::vector<Dom>& {
      subarray.clear();
      for (uint64_t d = 0; d < arraydomain.size(); d++) {
        subarray.push_back(Dom(
            arraydomain[d].domain.lower_bound +
                subarray_tiles[d].lower_bound * arraydomain[d].extent,
            arraydomain[d].domain.lower_bound +
                (subarray_tiles[d].upper_bound + 1) * arraydomain[d].extent -
                1));
      }
      return subarray;
    };

    uint64_t num_cells_per_tile = 1;
    for (const auto& dim : arraydomain) {
      num_cells_per_tile *= dim.extent;
    }

    // clamp to a hopefully reasonable limit (if the other attempts failed)
    // avoid too many cells, and avoid too many tiles
    std::optional<uint64_t> num_cells;
    while (!(num_cells = subarray_num_cells(to_subarray())).has_value() ||
           num_cells.value() >= 1024 * 1024 * 4 ||
           (num_cells.value() / num_cells_per_tile) >= 16 * 1024) {
      for (uint64_t d = subarray.size(); d > 0; --d) {
        auto& dtiles = subarray_tiles[d - 1];
        if (dtiles.num_cells() > 4) {
          dtiles.upper_bound = (dtiles.lower_bound + dtiles.upper_bound) / 2;
          break;
        }
      }
    }

    return to_subarray();
  });
}

}  // namespace rc

/**
 * Generates an arbitrary expected-to-not-error input to
 * `instance_dense_global_order` of an appropriate size for the given
 * `dimensions`.
 *
 * "Appropriate size" means tiles with at most `1024 * 128` cells, and a write
 * domain with at most `1024 * 1024 * 4` cells (see
 * `make_tile_aligned_subarray`). We expect that this should allow inputs which
 * are large enough to be interesting but not so large that each instance takes
 * a long time.
 *
 * Inputs generated by this test function are expected to successfully write
 * fragments within the generated max fragment size.  The maximum fragment size
 * is a number of bytes which represents between 1 and 8 hyperrows.
 */
template <sm::Datatype DTYPE>
void rapidcheck_dense_array(
    Context& ctx,
    const std::string& array_name,
    const std::vector<templates::Dimension<DTYPE>>& dimensions) {
  uint64_t num_cells_per_tile = 1;
  for (const auto& dim : dimensions) {
    num_cells_per_tile *= dim.extent;
  }
  RC_PRE(num_cells_per_tile <= 1024 * 128);

  const tiledb_layout_t tile_order =
      *rc::gen::element(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  const tiledb_layout_t cell_order =
      *rc::gen::element(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

  const uint64_t tile_size = num_cells_per_tile * sizeof(int);
  const uint64_t filter_chunk_size =
      sm::WriterTile::compute_chunk_size(tile_size, sizeof(int));
  const uint64_t num_filter_chunks_per_tile =
      (tile_size + filter_chunk_size - 1) / filter_chunk_size;

  const uint64_t estimate_single_tile_fragment_size =
      num_cells_per_tile * sizeof(int)  // data
      + sizeof(uint64_t)  // prefix containing the number of chunks
      + num_filter_chunks_per_tile * 3 * sizeof(uint32_t);  // chunk sizes

  const auto subarray =
      *rc::make_tile_aligned_subarray<sm::Datatype::UINT64>(dimensions);

  uint64_t num_tiles_per_hyperrow = 1;
  for (uint64_t i = 0; i < dimensions.size() - 1; i++) {
    const uint64_t dim =
        (tile_order == TILEDB_ROW_MAJOR ? i + 1 : dimensions.size() - i - 2);
    num_tiles_per_hyperrow *= dimensions[dim].num_tiles(subarray[dim]);
  }

  const uint64_t num_tiles_total =
      num_tiles_per_hyperrow *
      (tile_order == TILEDB_ROW_MAJOR ?
           (dimensions[0].num_tiles(subarray[0])) :
           (dimensions.back().num_tiles(subarray.back())));

  auto gen_fragment_size = rc::gen::inRange(
      estimate_single_tile_fragment_size,
      num_tiles_per_hyperrow * estimate_single_tile_fragment_size * 8);
  const uint64_t max_fragment_size = *gen_fragment_size;

  auto gen_write_unit_num_cells =
      rc::gen::inRange<uint64_t>(1, num_tiles_total * num_cells_per_tile);
  const uint64_t write_unit_num_cells = *gen_write_unit_num_cells;

  instance_dense_global_order<AsserterRapidcheck>(
      ctx,
      array_name,
      tile_order,
      cell_order,
      max_fragment_size,
      dimensions,
      subarray,
      write_unit_num_cells);
}

TEST_CASE(
    "C++ API: Max fragment size dense array rapidcheck 1d",
    "[cppapi][max-frag-size][rapidcheck]") {
  static constexpr auto DT = sm::Datatype::UINT64;
  using Dim64 = templates::Dimension<DT>;
  using Dom64 = Dim64::domain_type;

  VFSTestSetup vfs;
  Context ctx(vfs.ctx());

  const std::string array_name =
      vfs.array_uri("max_fragment_size_dense_global_order_rapidcheck_1d");

  SECTION("Shrinking") {
    instance_dense_global_order<AsserterCatch>(
        ctx,
        array_name,
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        2396,
        {Dim64(0, 8929, 594)},
        {Dom64(0, 2969)});
  }

  rc::prop("max fragment size dense 1d", [&]() {
    Dim64 d1 = *rc::make_dimension<DT>(8192);

    rapidcheck_dense_array<DT>(ctx, array_name, {d1});
  });
}

TEST_CASE(
    "C++ API: Max fragment size dense array rapidcheck 2d",
    "[cppapi][max-frag-size][rapidcheck]") {
  static constexpr auto DT = sm::Datatype::UINT64;
  using Dim64 = templates::Dimension<DT>;
  using Dom64 = Dim64::domain_type;

  VFSTestSetup vfs;
  Context ctx(vfs.ctx());

  const std::string array_name =
      vfs.array_uri("max_fragment_size_dense_global_order_rapidcheck_2d");

  SECTION("Shrinking") {
    SECTION("Example 1") {
      instance_dense_global_order<AsserterCatch>(
          ctx,
          array_name,
          TILEDB_ROW_MAJOR,
          TILEDB_COL_MAJOR,
          48,
          {Dim64(0, 116, 1), Dim64(0, 0, 1)},
          {Dom64(2, 20), Dom64(0, 0)});
    }

    SECTION("Example 2") {
      instance_dense_global_order<AsserterCatch>(
          ctx,
          array_name,
          TILEDB_COL_MAJOR,
          TILEDB_ROW_MAJOR,
          24,
          {Dim64(0, 60, 1), Dim64(0, 20, 1)},
          {Dom64(0, 1), Dom64(0, 1)});
    }

    SECTION("Example 3") {
      instance_dense_global_order<AsserterCatch>(
          ctx,
          array_name,
          TILEDB_ROW_MAJOR,
          TILEDB_ROW_MAJOR,
          48,
          {Dim64(0, 35, 1), Dim64(0, 420, 1)},
          {Dom64(0, 1), Dom64(0, 4)},
          1);
    }

    SECTION("Example 4") {
      /*
       * In this example we end up with a fragment which fills all but one tile
       * of a single row. The last tile in the row has to be its own fragment.
       */
      auto fragments = instance_dense_global_order<AsserterCatch>(
          ctx,
          array_name,
          TILEDB_ROW_MAJOR,
          TILEDB_ROW_MAJOR,
          924,
          {Dim64(0, 304, 8), Dim64(0, 147, 2)},
          {Dom64(0, 31), Dom64(0, 23)},
          41);
    }
  }

  rc::prop("max fragment size dense 2d", [&]() {
    Dim64 d1 = *rc::make_dimension<DT>(128);
    Dim64 d2 = *rc::make_dimension<DT>(128);

    rapidcheck_dense_array<DT>(ctx, array_name, {d1, d2});
  });
}

TEST_CASE(
    "C++ API: Max fragment size dense array rapidcheck 3d",
    "[cppapi][max-frag-size][rapidcheck]") {
  static constexpr auto DT = sm::Datatype::UINT64;
  using Dim64 = templates::Dimension<DT>;
  using Dom64 = Dim64::domain_type;

  VFSTestSetup vfs;
  Context ctx(vfs.ctx());

  const std::string array_name =
      vfs.array_uri("max_fragment_size_dense_global_order_rapidcheck_3d");

  SECTION("Shrinking") {
    instance_dense_global_order<AsserterCatch>(
        ctx,
        array_name,
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        2160,
        {Dim64(0, 85, 5), Dim64(0, 102, 2), Dim64(0, 37, 1)},
        {Dom64(5, 19), Dom64(4, 15), Dom64(1, 6)});
  }

  rc::prop("max fragment size dense 3d", [&]() {
    Dim64 d1 = *rc::make_dimension<DT>(32);
    Dim64 d2 = *rc::make_dimension<DT>(32);
    Dim64 d3 = *rc::make_dimension<DT>(32);

    rapidcheck_dense_array<DT>(ctx, array_name, {d1, d2, d3});
  });
}

/**
 * Test some edge cases induced by variable-length tiles
 */
TEST_CASE(
    "C++ API: Max fragment size dense array var size tiles",
    "[cppapi][max-frag-size]") {
  VFSTestSetup vfs;
  Context ctx(vfs.ctx());
  const std::string array_name =
      vfs.array_uri("max_fragment_size_dense_global_order_var");

  using Dim64 = templates::Dimension<Datatype::UINT64>;
  using Dom64 = Dim64::domain_type;

  using F = templates::Fragment<std::tuple<>, std::tuple<std::vector<char>>>;

  const tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  const tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;

  SECTION("Rectangle tiles") {
    const uint64_t d1_extent = 8;
    const uint64_t d2_span = 10000;
    REQUIRE(d2_span % d1_extent == 0);

    const uint64_t d2_extent = d2_span / d1_extent;

    const Dim64 row(0, std::numeric_limits<uint64_t>::max() - 1, d1_extent);
    const Dim64 col(0, d2_span - 1, d2_extent);

    const Dom64 subrow(0, 2 * d1_extent - 1);
    const Dom64 subcol = col.domain;

    auto make_subcol = [&](uint64_t start_tile, uint64_t end_tile) -> Dom64 {
      const uint64_t tile_span = d2_extent * d1_extent / 8;
      return Dom64(tile_span * start_tile, tile_span * end_tile - 1);
    };

    const Dom64 subrow_0(0, d1_extent - 1);
    const Dom64 subrow_1(d1_extent, 2 * d1_extent - 1);

    const std::optional<uint64_t> num_cells =
        subarray_num_cells(std::vector<Dom64>{subrow, subcol});
    REQUIRE(num_cells.has_value());

    const uint64_t approx_tiles_per_fragment = GENERATE(4, 9);
    const uint64_t max_fragment_size = approx_tiles_per_fragment * 64 * 1024;

    F attributes;
    attributes.reserve(num_cells.value());

    const std::optional<uint64_t> write_unit_num_cells = GENERATE_COPY(
        std::optional<uint64_t>{},
        64,
        1024,
        1024 * 1024,
        num_cells.value() - 1);

    const uint64_t num_cells_per_tile = d1_extent * d2_extent;

    DYNAMIC_SECTION(
        "approx_tiles_per_fragment = " << approx_tiles_per_fragment) {
      DYNAMIC_SECTION(
          "write_unit_num_cells = "
          << (write_unit_num_cells.has_value() ?
                  std::to_string(write_unit_num_cells.value()) :
                  "unlimited")) {
        SECTION("Even") {
          for (uint64_t c = 0; c < num_cells.value(); c++) {
            const std::string str = std::to_string(c);
            std::get<0>(attributes.attributes())
                .push_back(std::span<const char>(str.begin(), str.end()));
          }

          const auto actual = instance_dense_global_order<F>(
              ctx,
              array_name,
              tile_order,
              cell_order,
              max_fragment_size,
              {row, col},
              {subrow, subcol},
              attributes,
              write_unit_num_cells);

          std::vector<std::vector<Dom64>> expect;
          if (approx_tiles_per_fragment == 4) {
            expect.push_back({subrow_0, make_subcol(0, 4)});
            expect.push_back({subrow_0, make_subcol(4, 8)});
            expect.push_back({subrow_1, make_subcol(0, 3)});
            expect.push_back({subrow_1, make_subcol(3, 6)});
            expect.push_back({subrow_1, make_subcol(6, 8)});
          } else {
            expect.push_back({subrow_0, subcol});
            expect.push_back({subrow_1, subcol});
          }
          CHECK(expect == actual);
        }

        SECTION("Skew first tile") {
          // inflate all the records of the first tile
          for (uint64_t c = 0; c < num_cells.value(); c++) {
            const std::string str =
                (c < num_cells_per_tile ? "foobargubquux" + std::to_string(c) :
                                          std::to_string(c));
            std::get<0>(attributes.attributes())
                .push_back(std::span<const char>(str.begin(), str.end()));
          }

          const auto actual = instance_dense_global_order<F>(
              ctx,
              array_name,
              tile_order,
              cell_order,
              max_fragment_size,
              {row, col},
              {subrow, subcol},
              attributes,
              write_unit_num_cells);

          std::vector<std::vector<Dom64>> expect;
          if (approx_tiles_per_fragment == 4) {
            expect.push_back({subrow_0, make_subcol(0, 2)});
            expect.push_back({subrow_0, make_subcol(2, 6)});
            expect.push_back({subrow_0, make_subcol(6, 8)});
            expect.push_back({subrow_1, make_subcol(0, 3)});
            expect.push_back({subrow_1, make_subcol(3, 6)});
            expect.push_back({subrow_1, make_subcol(6, 8)});
          } else {
            expect.push_back({subrow_0, make_subcol(0, 7)});
            expect.push_back({subrow_0, make_subcol(7, 8)});
            expect.push_back({subrow_1, subcol});
          }
          CHECK(expect == actual);
        }

        SECTION("Skew second tile") {
          // inflate all the records of the second tile
          for (uint64_t c = 0; c < num_cells.value(); c++) {
            const std::string str =
                (num_cells_per_tile <= c && c < 2 * num_cells_per_tile ?
                     "foobargubquux" + std::to_string(c) :
                     std::to_string(c));
            std::get<0>(attributes.attributes())
                .push_back(std::span<const char>(str.begin(), str.end()));
          }

          const auto actual = instance_dense_global_order<F>(
              ctx,
              array_name,
              tile_order,
              cell_order,
              max_fragment_size,
              {row, col},
              {subrow, subcol},
              attributes,
              write_unit_num_cells);

          std::vector<std::vector<Dom64>> expect;
          if (approx_tiles_per_fragment == 4) {
            expect.push_back({subrow_0, make_subcol(0, 2)});
            expect.push_back({subrow_0, make_subcol(2, 6)});
            expect.push_back({subrow_0, make_subcol(6, 8)});
            expect.push_back({subrow_1, make_subcol(0, 3)});
            expect.push_back({subrow_1, make_subcol(3, 6)});
            expect.push_back({subrow_1, make_subcol(6, 8)});
          } else {
            expect.push_back({subrow_0, make_subcol(0, 7)});
            expect.push_back({subrow_0, make_subcol(7, 8)});
            expect.push_back({subrow_1, subcol});
          }
          CHECK(expect == actual);
        }

        SECTION("Skew last tile") {
          // inflate all the records of the last tile
          for (uint64_t c = 0; c < num_cells.value(); c++) {
            const std::string str =
                (num_cells.value() - num_cells_per_tile <= c ?
                     "foobargubquux" + std::to_string(c) :
                     std::to_string(c));
            std::get<0>(attributes.attributes())
                .push_back(std::span<const char>(str.begin(), str.end()));
          }

          const auto actual = instance_dense_global_order<F>(
              ctx,
              array_name,
              tile_order,
              cell_order,
              max_fragment_size,
              {row, col},
              {subrow, subcol},
              attributes,
              write_unit_num_cells);

          std::vector<std::vector<Dom64>> expect;
          if (approx_tiles_per_fragment == 4) {
            expect.push_back({subrow_0, make_subcol(0, 4)});
            expect.push_back({subrow_0, make_subcol(4, 8)});
            expect.push_back({subrow_1, make_subcol(0, 3)});
            expect.push_back({subrow_1, make_subcol(3, 6)});
            expect.push_back({subrow_1, make_subcol(6, 7)});
            expect.push_back({subrow_1, make_subcol(7, 8)});
          } else {
            expect.push_back({subrow_0, subcol});
            expect.push_back({subrow_1, make_subcol(0, 7)});
            expect.push_back({subrow_1, make_subcol(7, 8)});
          }
          CHECK(expect == actual);
        }
      }
    }
  }
}

TEST_CASE(
    "C++ API: Max fragment size dense unsupported on REST", "[cppapi][rest]") {
  VFSTestSetup vfs;
  if (!vfs.is_rest()) {
    SKIP("Test is only applicable to REST client");
  }

  const std::string array_name =
      vfs.array_uri("max_fragment_size_dense_global_order_rest_support");

  Context ctx(vfs.ctx());

  using Dim = templates::Dimension<sm::Datatype::UINT64>;
  using Dom = Dim::domain_type;

  Dim d1(0, 0, 1);
  Dim d2(0, 0, 1);
  Dom s1(0, 0);
  Dom s2(0, 0);
  const uint64_t max_fragment_size = 24;

  const auto expect = Catch::Matchers::ContainsSubstring(
      "Fragment size is not supported for remote global order writes to dense "
      "arrays.");

  REQUIRE_THROWS(
      instance_dense_global_order<AsserterCatch>(
          ctx,
          array_name,
          TILEDB_ROW_MAJOR,
          TILEDB_ROW_MAJOR,
          max_fragment_size,
          {d1, d2},
          {s1, s2}),
      expect);
}
