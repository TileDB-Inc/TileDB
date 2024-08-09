/**
 * @file   unit-cppapi-consolidation-with-timestamps.cc
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
 * Tests the CPP API for consolidation with timestamps.
 */

/*
 * WARNING:
 *   [2024/05/13] This file has succumbed to bit rot. It used to be a copy of
 *   a file by the same name within `tiledb_unit`, but it no longer is. It
 *   should not be expected to compile.
 */

#include <catch2/catch_test_macros.hpp>
#include "test/support/src/helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;
using namespace tiledb::test;

unsigned int read_sparse_iters = 1;
unsigned int perform_query = 1;
unsigned int consolidate_sparse_iters = 1;

/** Tests for C API consolidation with timestamps. */
struct ConsolidationWithTimestampsFx {
  // Constants.
  const char* SPARSE_ARRAY_NAME = "test_consolidate_sparse_array";
  const char* SPARSE_ARRAY_FRAG_DIR =
      "test_consolidate_sparse_array/__fragments";

  // TileDB context.
  Context ctx_;
  VFS vfs_;
  sm::StorageManager* sm_;

  // Constructors/destructors.
  ConsolidationWithTimestampsFx();
  ~ConsolidationWithTimestampsFx();

  // Functions.
  void set_legacy();
  void create_sparse_array(bool allows_dups = false);
  void create_sparse_array_v11();
  void write_sparse(
      std::vector<int> a1,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp);
  void write_sparse_v11(uint64_t timestamp);
  void consolidate_sparse(bool vacuum = false);
  void consolidate_sparse(
      uint64_t start_time, uint64_t end_time, bool vacuum = false);
  void check_timestamps_file(std::vector<uint64_t> expected);
  void read_sparse(
      std::vector<int>& a1,
      std::vector<uint64_t>& dim1,
      std::vector<uint64_t>& dim2,
      std::string& stats,
      tiledb_layout_t layout,
      uint64_t timestamp,
      std::vector<uint64_t>* timestamps_ptr = nullptr,
      bool skip_query = false);
  void reopen_sparse(
      std::vector<int>& a1,
      std::vector<uint64_t>& dim1,
      std::vector<uint64_t>& dim2,
      std::string& stats,
      tiledb_layout_t layout,
      uint64_t start_time,
      uint64_t end_time,
      std::vector<uint64_t>* timestamps_ptr = nullptr);

  void remove_sparse_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
};

ConsolidationWithTimestampsFx::ConsolidationWithTimestampsFx()
    : vfs_(ctx_) {
  Config config;
  config.set("sm.consolidation.buffer_size", "1000");
  ctx_ = Context(config);
  sm_ = ctx_.ptr().get()->storage_manager();
  vfs_ = VFS(ctx_);
}

ConsolidationWithTimestampsFx::~ConsolidationWithTimestampsFx() {
}

void ConsolidationWithTimestampsFx::set_legacy() {
  Config config;
  config.set("sm.consolidation.buffer_size", "1000");
  config.set("sm.query.sparse_global_order.reader", "legacy");
  config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");

  ctx_ = Context(config);
  sm_ = ctx_.ptr().get()->storage_manager();
  vfs_ = VFS(ctx_);
}

void ConsolidationWithTimestampsFx::create_sparse_array(bool allows_dups) {
  // Create dimensions.
  auto d1 = Dimension::create<uint64_t>(ctx_, "d1", {{1, 4}}, 2);
  auto d2 = Dimension::create<uint64_t>(ctx_, "d2", {{1, 4}}, 2);

  // Create domain.
  Domain domain(ctx_);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create attributes.
  auto a1 = Attribute::create<int32_t>(ctx_, "a1");

  // Create array schema.
  ArraySchema schema(ctx_, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.set_capacity(20);
  schema.add_attributes(a1);

  if (allows_dups) {
    schema.set_allows_dups(true);
  }

  // Set up filters.
  Filter filter(ctx_, TILEDB_FILTER_NONE);
  FilterList filter_list(ctx_);
  filter_list.add_filter(filter);
  schema.set_coords_filter_list(filter_list);

  Array::create(SPARSE_ARRAY_NAME, schema);
}

void ConsolidationWithTimestampsFx::create_sparse_array_v11() {
  // Get the v11 sparse array.
  std::string v11_arrays_dir =
      std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays/sparse_array_v11";
  REQUIRE(
      tiledb_vfs_copy_dir(
          ctx_.ptr().get(),
          vfs_.ptr().get(),
          v11_arrays_dir.c_str(),
          SPARSE_ARRAY_NAME) == TILEDB_OK);
}

void ConsolidationWithTimestampsFx::write_sparse(
    std::vector<int> a1,
    std::vector<uint64_t> dim1,
    std::vector<uint64_t> dim2,
    uint64_t timestamp) {
  // Open array.
  Array array(ctx_, SPARSE_ARRAY_NAME, TILEDB_WRITE, timestamp);

  // Create query.
  Query query(ctx_, array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_data_buffer("a1", a1);
  query.set_data_buffer("d1", dim1);
  query.set_data_buffer("d2", dim2);

  // Submit/finalize the query.
  query.submit();
  query.finalize();

  // Close array.
  array.close();
}

void ConsolidationWithTimestampsFx::write_sparse_v11(uint64_t timestamp) {
  // Prepare cell buffers.
  std::vector<int> buffer_a1{0, 1, 2, 3};
  std::vector<uint64_t> buffer_a2{0, 1, 3, 6};
  std::string buffer_var_a2("abbcccdddd");
  std::vector<float> buffer_a3{0.1f, 0.2f, 1.1f, 1.2f, 2.1f, 2.2f, 3.1f, 3.2f};
  std::vector<uint64_t> buffer_coords_dim1{1, 1, 1, 2};
  std::vector<uint64_t> buffer_coords_dim2{1, 2, 4, 3};

  // Open array.
  Array array(ctx_, SPARSE_ARRAY_NAME, TILEDB_WRITE, timestamp);

  // Create query.
  Query query(ctx_, array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_data_buffer("a1", buffer_a1);
  query.set_data_buffer(
      "a2", (void*)buffer_var_a2.c_str(), buffer_var_a2.size());
  query.set_offsets_buffer("a2", buffer_a2);
  query.set_data_buffer("a3", buffer_a3);
  query.set_data_buffer("d1", buffer_coords_dim1);
  query.set_data_buffer("d2", buffer_coords_dim2);

  // Submit/finalize the query.
  query.submit();
  query.finalize();

  // Close array.
  array.close();
}

void ConsolidationWithTimestampsFx::consolidate_sparse(bool vacuum) {
  auto config = ctx_.config();
  Array::consolidate(ctx_, SPARSE_ARRAY_NAME, &config);

  if (vacuum) {
    REQUIRE_NOTHROW(Array::vacuum(ctx_, SPARSE_ARRAY_NAME, &config));
  }
}

void ConsolidationWithTimestampsFx::consolidate_sparse(
    uint64_t start_time, uint64_t end_time, bool vacuum) {
  auto config = ctx_.config();
  config.set("sm.consolidation.timestamp_start", std::to_string(start_time));
  config.set("sm.consolidation.timestamp_end", std::to_string(end_time));
  Array::consolidate(ctx_, SPARSE_ARRAY_NAME, &config);

  if (vacuum) {
    REQUIRE_NOTHROW(Array::vacuum(ctx_, SPARSE_ARRAY_NAME, &config));
  }
}

void ConsolidationWithTimestampsFx::check_timestamps_file(
    std::vector<uint64_t> expected) {
  // Find consolidated fragment URI.
  std::string consolidated_fragment_uri;
  auto uris = vfs_.ls(SPARSE_ARRAY_FRAG_DIR);
  for (auto& uri : uris) {
    if (uri.find("__1_2_") != std::string::npos) {
      consolidated_fragment_uri = uri;
      break;
    }
  }

  auto timestamps_file = consolidated_fragment_uri + "/t.tdb";

  VFS::filebuf buf(vfs_);
  buf.open(timestamps_file, std::ios::in);
  std::istream is(&buf);
  REQUIRE(is.good());

  uint64_t num_tiles;
  is.read((char*)&num_tiles, sizeof(uint64_t));
  REQUIRE(num_tiles == 1);

  uint32_t filtered_size;
  is.read((char*)&filtered_size, sizeof(uint32_t));
  REQUIRE(filtered_size == expected.size() * sizeof(uint64_t));

  uint32_t unfiltered_size;
  is.read((char*)&unfiltered_size, sizeof(uint32_t));
  REQUIRE(unfiltered_size == expected.size() * sizeof(uint64_t));

  uint32_t md_size;
  is.read((char*)&md_size, sizeof(uint32_t));
  REQUIRE(md_size == 0);

  std::vector<uint64_t> written(unfiltered_size / sizeof(uint64_t));
  is.read((char*)written.data(), unfiltered_size);

  for (uint64_t i = 0; i < expected.size(); i++) {
    if (expected[i] == std::numeric_limits<uint64_t>::max()) {
      CHECK((written[i] == 1 || written[i] == 2));
    } else {
      CHECK(expected[i] == written[i]);
    }
  }
}

void ConsolidationWithTimestampsFx::read_sparse(
    std::vector<int>& a1,
    std::vector<uint64_t>& dim1,
    std::vector<uint64_t>& dim2,
    std::string& stats,
    tiledb_layout_t layout,
    uint64_t timestamp,
    std::vector<uint64_t>* timestamps_ptr,
    bool skip_query) {
  // Open array.
  Array array(ctx_, SPARSE_ARRAY_NAME, TILEDB_READ, timestamp);

  if (!skip_query) {
    // Create query.
    Query query(ctx_, array, TILEDB_READ);
    query.set_layout(layout);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("d1", dim1);
    query.set_data_buffer("d2", dim2);
    if (timestamps_ptr != nullptr) {
      query.set_data_buffer(tiledb_timestamps(), *timestamps_ptr);
    }

    // Submit the query.
    query.submit();
    CHECK(query.query_status() == Query::Status::COMPLETE);

    // Get the query stats.
    stats = query.stats();
  }

  // Close array.
  array.close();
}

void ConsolidationWithTimestampsFx::reopen_sparse(
    std::vector<int>& a1,
    std::vector<uint64_t>& dim1,
    std::vector<uint64_t>& dim2,
    std::string& stats,
    tiledb_layout_t layout,
    uint64_t start_time,
    uint64_t end_time,
    std::vector<uint64_t>* timestamps_ptr) {
  // Re-open array.
  Array array(ctx_, SPARSE_ARRAY_NAME, TILEDB_READ);
  array.set_open_timestamp_start(start_time);
  array.set_open_timestamp_end(end_time);
  array.reopen();

  // Create query.
  Query query(ctx_, array, TILEDB_READ);
  query.set_layout(layout);
  query.set_data_buffer("a1", a1);
  query.set_data_buffer("d1", dim1);
  query.set_data_buffer("d2", dim2);
  if (timestamps_ptr != nullptr) {
    query.set_data_buffer(tiledb_timestamps(), *timestamps_ptr);
  }

  // Submit the query.
  query.submit();
  CHECK(query.query_status() == Query::Status::COMPLETE);

  // Get the query stats.
  stats = query.stats();

  // Close array.
  array.close();
}

void ConsolidationWithTimestampsFx::remove_array(
    const std::string& array_name) {
  if (!is_array(array_name))
    return;

  vfs_.remove_dir(array_name);
}

void ConsolidationWithTimestampsFx::remove_sparse_array() {
  remove_array(SPARSE_ARRAY_NAME);
}

bool ConsolidationWithTimestampsFx::is_array(const std::string& array_name) {
  return vfs_.is_dir(array_name);
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, full and partial read with "
    "dups",
    "[cppapi][consolidation-with-timestamps][partial-read][dups]") {
  remove_sparse_array();
  // Enable duplicates.
  create_sparse_array(true);

  // Write first fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);
  // Write second fragment.
  write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 4, 2, 3}, 3);

  // Consolidate.
  bool vacuum = GENERATE(true, false);
  consolidate_sparse(vacuum);

  // Write third fragment.
  write_sparse({8, 9, 10, 11}, {2, 1, 3, 4}, {1, 3, 1, 1}, 4);
  // Write fourth fragment.
  write_sparse({12, 13, 14, 15}, {4, 3, 3, 4}, {2, 3, 4, 4}, 6);

  for (auto ux = 0u; ux < consolidate_sparse_iters; ++ux) {
    // Consolidate.
    consolidate_sparse(3, 7, vacuum);
  }

  tiledb_layout_t layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);

  // Test read for both refactored and legacy.
  bool legacy = GENERATE(true, false);
  if (legacy) {
    set_legacy();
  }

  std::string stats;
  std::vector<int> a(16);
  std::vector<uint64_t> dim1(16);
  std::vector<uint64_t> dim2(16);
  std::vector<uint64_t> timestamps(16);
  auto timestamps_ptr =
      GENERATE_REF(as<std::vector<uint64_t>*>{}, nullptr, &timestamps);

  uint64_t tstamp = 7;
  SECTION("Read after all writes") {
    // Read after both writes - should see everything.
    tstamp = 7;
    // read_sparse(a, dim1, dim2, stats, layout, 7, timestamps_ptr);
  }

  // Read with full coverage on first 2 consolidated, partial coverage on second
  // 2 consolidated.
  SECTION("Read between the 2 writes") {
    tstamp = 5;
    // read_sparse(a, dim1, dim2, stats, layout, 5, timestamps_ptr);
  }
  for (auto ux = 0u; ux < read_sparse_iters; ++ux)
    read_sparse(
        a,
        dim1,
        dim2,
        stats,
        layout,
        tstamp,
        timestamps_ptr,
        perform_query == 0);

  remove_sparse_array();
}
