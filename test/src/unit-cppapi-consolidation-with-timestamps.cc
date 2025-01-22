/**
 * @file   unit-cppapi-consolidation-with-timestamps.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc.
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

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/query/readers/sparse_global_order_reader.h"

using namespace tiledb;
using namespace tiledb::test;

/** Tests for CPP API consolidation with timestamps. */
struct ConsolidationWithTimestampsFx {
  // Constants.
  const char* SPARSE_ARRAY_NAME = "test_consolidate_sparse_array";
  const char* SPARSE_ARRAY_FRAG_DIR =
      "test_consolidate_sparse_array/__fragments";

  // TileDB context.
  Context ctx_;
  VFS vfs_;
  sm::ContextResources* resources_;

  // Constructors/destructors.
  ConsolidationWithTimestampsFx();
  ~ConsolidationWithTimestampsFx();

  // Functions.
  void set_legacy();
  void create_sparse_array(bool allows_dups = false);
  void write_sparse(
      std::vector<int> a1,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp);
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
      std::vector<uint64_t>* timestamps_ptr = nullptr);
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
  resources_ = &ctx_.ptr().get()->resources();
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
  resources_ = &ctx_.ptr().get()->resources();
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

void ConsolidationWithTimestampsFx::write_sparse(
    std::vector<int> a1,
    std::vector<uint64_t> dim1,
    std::vector<uint64_t> dim2,
    uint64_t timestamp) {
  // Open array.
  Array array(
      ctx_,
      SPARSE_ARRAY_NAME,
      TILEDB_WRITE,
      tiledb::TemporalPolicy(tiledb::TimeTravel, timestamp));

  // Create query.
  Query query(ctx_, array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_data_buffer("a1", a1);
  query.set_data_buffer("d1", dim1);
  query.set_data_buffer("d2", dim2);

  // Submit query
  query.submit_and_finalize();

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
    std::vector<uint64_t>* timestamps_ptr) {
  // Open array.
  Array array(
      ctx_,
      SPARSE_ARRAY_NAME,
      TILEDB_READ,
      tiledb::TemporalPolicy(tiledb::TimeTravel, timestamp));

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
    "CPP API: Test consolidation with timestamps",
    "[cppapi][consolidation-with-timestamps][write-check][non-rest]") {
  remove_sparse_array();
  create_sparse_array();

  // Write first fragment.
  write_sparse(
      {0, 1, 2, 3, 4, 5, 6, 7},
      {1, 1, 1, 2, 3, 4, 3, 3},
      {1, 2, 4, 3, 1, 2, 3, 4},
      1);

  // Write second fragment.
  write_sparse({8, 9, 10, 11}, {2, 2, 3, 3}, {2, 3, 2, 3}, 2);

  // Consolidate.
  consolidate_sparse();

  // Check t.tdb file.
  const uint64_t X = std::numeric_limits<uint64_t>::max();
  check_timestamps_file({1, 1, 2, 1, X, X, 1, 2, 1, X, X, 1});

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, check directory contents",
    "[cppapi][consolidation-with-timestamps][sparse-unordered-with-dups][non-"
    "rest]") {
  remove_sparse_array();
  create_sparse_array(true);

  // Write first fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  // Write second fragment.
  write_sparse({8, 9, 10, 11}, {2, 2, 3, 3}, {2, 3, 2, 3}, 3);

  // Consolidate.
  consolidate_sparse();

  sm::URI array_uri(SPARSE_ARRAY_NAME);

  std::vector<std::array<uint64_t, 4>> test_timestamps = {
      {0, 2, 1, 3},   // Partial coverage of lower timestamp.
      {2, 10, 1, 3},  // Partial coverage of upper timestamp.
      {0, 5, 1, 3},   // Full coverage.
      {3, 5, 1, 3}    // Boundary case.
  };

  for (auto& ts : test_timestamps) {
    sm::ArrayDirectory array_dir(*resources_, array_uri, ts[0], ts[1]);

    // Check that only the consolidated fragment is visible.
    auto filtered_fragment_uris = array_dir.filtered_fragment_uris(false);
    auto fragments = filtered_fragment_uris.fragment_uris();
    CHECK(fragments.size() == 1);

    auto ts_range = fragments[0].timestamp_range_;
    CHECK(ts_range.first == ts[2]);
    CHECK(ts_range.second == ts[3]);
  }

  test_timestamps = {
      {4, 5, 0, 0},  // No coverage - later read.
      {0, 0, 0, 0}   // No coverage - earlier read.
  };

  for (auto& ts : test_timestamps) {
    sm::ArrayDirectory array_dir(*resources_, array_uri, ts[0], ts[1]);

    // Check that no fragment is visible
    auto filtered_fragment_uris = array_dir.filtered_fragment_uris(false);
    auto fragments = filtered_fragment_uris.fragment_uris();
    CHECK(fragments.size() == 0);
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, global read",
    "[cppapi][consolidation-with-timestamps][global-read][non-rest]") {
  remove_sparse_array();
  create_sparse_array();

  // Write first fragment.
  write_sparse(
      {0, 1, 2, 3, 4, 5, 6, 7},
      {1, 1, 1, 2, 3, 4, 3, 3},
      {1, 2, 4, 3, 1, 2, 3, 4},
      1);

  // Write second fragment.
  write_sparse({8, 9, 10, 11}, {2, 2, 3, 3}, {2, 3, 2, 3}, 2);

  // Consolidate.
  consolidate_sparse();

  // Test read for both refactored and legacy.
  bool legacy = GENERATE(true, false);
  uint64_t buffer_size = 10;
  if (legacy) {
    buffer_size = 100;
    set_legacy();
  }

  std::string stats;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, dim1, dim2, stats, TILEDB_GLOBAL_ORDER, 3);

  std::vector<int> c_a1 = {0, 1, 8, 2, 9, 4, 10, 5, 11, 7};
  std::vector<uint64_t> c_dim1 = {1, 1, 2, 1, 2, 3, 3, 4, 3, 3};
  std::vector<uint64_t> c_dim2 = {1, 2, 2, 4, 3, 1, 2, 2, 3, 4};
  CHECK(!memcmp(c_a1.data(), a1.data(), c_a1.size() * sizeof(int)));
  CHECK(!memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
  CHECK(!memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));

  remove_sparse_array();
}

// TODO: remove once tiledb_vfs_copy_dir is implemented for windows.
#ifndef _WIN32
TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, check directory contents of "
    "old array",
    "[cppapi][consolidation-with-timestamps][sparse-unordered-with-dups][non-"
    "rest]") {
  if constexpr (is_experimental_build) {
    return;
  }

  remove_sparse_array();
  test::create_sparse_array_v11(ctx_.ptr().get(), SPARSE_ARRAY_NAME);
  // Write first fragment.
  test::write_sparse_v11(ctx_.ptr().get(), SPARSE_ARRAY_NAME, 1);

  // Write second fragment.
  test::write_sparse_v11(ctx_.ptr().get(), SPARSE_ARRAY_NAME, 3);

  // Consolidate.
  consolidate_sparse();

  sm::URI array_uri(SPARSE_ARRAY_NAME);

  std::vector<std::array<uint64_t, 4>> test_timestamps = {
      {0, 2, 1, 1},   // Partial coverage of lower timestamp.
      {2, 10, 3, 3},  // Partial coverage of upper timestamp.
      {0, 4, 1, 3},   // Full coverage
      {3, 5, 3, 3}    // Boundary case.
  };

  for (auto& ts : test_timestamps) {
    sm::ArrayDirectory array_dir(*resources_, array_uri, ts[0], ts[1]);

    // Check that only the first fragment is visible on an old array.
    auto filtered_fragment_uris = array_dir.filtered_fragment_uris(false);
    auto fragments = filtered_fragment_uris.fragment_uris();
    CHECK(fragments.size() == 1);

    auto ts_range = fragments[0].timestamp_range_;
    CHECK(ts_range.first == ts[2]);
    CHECK(ts_range.second == ts[3]);
  }

  test_timestamps = {
      {4, 5, 0, 0},  // No coverage - later read.
      {0, 0, 0, 0}   // No coverage - earlier read.
  };

  for (auto& ts : test_timestamps) {
    sm::ArrayDirectory array_dir(*resources_, array_uri, ts[0], ts[1]);

    // Check that no fragment is visible
    auto filtered_fragment_uris = array_dir.filtered_fragment_uris(false);
    auto fragments = filtered_fragment_uris.fragment_uris();
    CHECK(fragments.size() == 0);
  }

  remove_sparse_array();
}
#endif

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, global read, all cells same "
    "coords",
    "[cppapi][consolidation-with-timestamps][global-read][same-coords][non-"
    "rest]") {
  remove_sparse_array();
  create_sparse_array();

  // Write fragments.
  for (uint64_t i = 0; i < 50; i++) {
    std::vector<int> a(1);
    a[0] = i + 1;
    write_sparse(a, {1}, {1}, i + 1);
  }

  // Consolidate.
  consolidate_sparse();

  // Test read for both refactored and legacy.
  bool legacy = GENERATE(true, false);
  uint64_t buffer_size = 1;
  if (legacy) {
    set_legacy();
    buffer_size = 100;
  }

  std::string stats;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, dim1, dim2, stats, TILEDB_GLOBAL_ORDER, 51);

  CHECK(a1[0] == 50);
  CHECK(dim1[0] == 1);
  CHECK(dim2[0] == 1);

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, global read, same coords "
    "across tiles",
    "[cppapi][consolidation-with-timestamps][global-read][across-tiles][non-"
    "rest]") {
  remove_sparse_array();
  bool allow_dups = GENERATE(true, false);
  create_sparse_array(allow_dups);

  // Write fragments.
  // We write 8 cells per fragments for 6 fragments. Then it gets consolidated
  // into one. So we'll get in order 6xcell1, 6xcell2... total 48 cells. Tile
  // capacity is 20 so we'll end up with 3 tiles. First break in the tiles will
  // be in the middle of cell3, second will be in the middle of the cells7.
  for (uint64_t i = 0; i < 6; i++) {
    write_sparse(
        {1, 2, 3, 4, 5, 6, 7, 8},
        {1, 1, 2, 2, 1, 1, 2, 2},
        {1, 2, 1, 2, 3, 4, 3, 4},
        i + 1);
  }

  // Consolidate.
  consolidate_sparse();

  // Test read for both refactored and legacy.
  bool legacy = GENERATE(true, false);
  uint64_t buffer_size = allow_dups ? 48 : 8;
  if (legacy) {
    set_legacy();
    buffer_size = 100;
  }

  std::string stats;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, dim1, dim2, stats, TILEDB_GLOBAL_ORDER, 7);

  std::vector<int> c_a1 = {1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<uint64_t> c_dim1 = {1, 1, 2, 2, 1, 1, 2, 2};
  std::vector<uint64_t> c_dim2 = {1, 2, 1, 2, 3, 4, 3, 4};
  for (uint64_t i = 0; i < 8; i++) {
    uint64_t max_j = allow_dups ? 6 : 1;
    for (uint64_t j = 0; j < max_j; j++) {
      uint64_t idx = i * max_j + j;
      CHECK(a1[idx] == c_a1[i]);
      CHECK(dim1[idx] == c_dim1[i]);
      CHECK(dim2[idx] == c_dim2[i]);
    }
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, global read, all cells same "
    "coords, with memory budget",
    "[cppapi][consolidation-with-timestamps][global-read][same-coords][mem-"
    "budget][non-rest]") {
  remove_sparse_array();
  create_sparse_array();

  // Write fragments.
  for (uint64_t i = 0; i < 50; i++) {
    std::vector<int> a(1);
    a[0] = i + 1;
    write_sparse(a, {1}, {1}, i + 1);
  }

  // Consolidate.
  consolidate_sparse();

  // Will only allow to load two tiles out of 3.
  Config cfg;
  cfg.set("sm.mem.total_budget", "50000");
  cfg.set("sm.mem.reader.sparse_global_order.ratio_coords", "0.15");
  ctx_ = Context(cfg);

  std::string stats;
  std::vector<int> a1(1);
  std::vector<uint64_t> dim1(1);
  std::vector<uint64_t> dim2(1);
  read_sparse(a1, dim1, dim2, stats, TILEDB_GLOBAL_ORDER, 51);

  CHECK(a1[0] == 50);
  CHECK(dim1[0] == 1);
  CHECK(dim2[0] == 1);

  // Make sure there was an internal loop on the reader.
  CHECK(
      stats.find("\"Context.Query.Reader.internal_loop_num\": 2") !=
      std::string::npos);

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, global read, same cells "
    "across tiles, with memory budget",
    "[cppapi][consolidation-with-timestamps][global-read][across-tiles][mem-"
    "budget][non-rest]") {
  remove_sparse_array();
  create_sparse_array();

  // Write fragments.
  // We write 8 cells per fragment for 6 fragments. Then it gets consolidated
  // into one. So we'll get in order 6xcell1, 6xcell2... total 48 cells. Tile
  // capacity is 20 so we'll end up with 3 tiles. First break in the tiles will
  // be in the middle of cell3, second will be in the middle of the cells7.
  for (uint64_t i = 0; i < 6; i++) {
    write_sparse(
        {1, 2, 3, 4, 5, 6, 7, 8},
        {1, 1, 2, 2, 1, 1, 2, 2},
        {1, 2, 1, 2, 3, 4, 3, 4},
        i + 1);
  }

  // Consolidate.
  consolidate_sparse();

  // Will only allow to load two tiles out of 3.
  Config cfg;
  cfg.set("sm.mem.total_budget", "60000");
  cfg.set("sm.mem.reader.sparse_global_order.ratio_coords", "0.15");
  ctx_ = Context(cfg);

  std::string stats;
  std::vector<int> a1(9);
  std::vector<uint64_t> dim1(9);
  std::vector<uint64_t> dim2(9);
  read_sparse(a1, dim1, dim2, stats, TILEDB_GLOBAL_ORDER, 7);

  std::vector<int> c_a1 = {1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<uint64_t> c_dim1 = {1, 1, 2, 2, 1, 1, 2, 2};
  std::vector<uint64_t> c_dim2 = {1, 2, 1, 2, 3, 4, 3, 4};
  CHECK(!memcmp(c_a1.data(), a1.data(), c_a1.size() * sizeof(int)));
  CHECK(!memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
  CHECK(!memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));

  // Make sure there was an internal loop on the reader.
  CHECK(
      stats.find("\"Context.Query.Reader.internal_loop_num\": 2") !=
      std::string::npos);

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, partial read with dups",
    "[cppapi][consolidation-with-timestamps][partial-read][dups][non-rest]") {
  remove_sparse_array();
  // Enable duplicates.
  create_sparse_array(true);

  // Write first fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  // Write second fragment.
  write_sparse({8, 9, 10, 11}, {2, 2, 3, 3}, {2, 3, 2, 3}, 3);

  // Consolidate.
  bool vacuum = GENERATE(true, false);
  consolidate_sparse(vacuum);

  tiledb_layout_t layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);

  // Test read for both refactored and legacy.
  bool legacy = GENERATE(true, false);
  if (legacy) {
    set_legacy();
  }

  std::string stats;
  std::vector<int> a(10);
  std::vector<uint64_t> dim1(10);
  std::vector<uint64_t> dim2(10);
  std::vector<uint64_t> timestamps(10);
  auto timestamps_ptr =
      GENERATE_REF(as<std::vector<uint64_t>*>{}, nullptr, &timestamps);

  SECTION("Read after all writes") {
    // Read after both writes - should see everything.
    read_sparse(a, dim1, dim2, stats, layout, 4, timestamps_ptr);

    std::vector<int> c_a_opt1 = {0, 1, 8, 2, 3, 9, 10, 11};
    std::vector<int> c_a_opt2 = {0, 1, 8, 2, 9, 3, 10, 11};
    std::vector<uint64_t> c_dim1 = {1, 1, 2, 1, 2, 2, 3, 3};
    std::vector<uint64_t> c_dim2 = {1, 2, 2, 4, 3, 3, 2, 3};
    CHECK(
        (!memcmp(c_a_opt1.data(), a.data(), c_a_opt1.size() * sizeof(int)) ||
         !memcmp(c_a_opt2.data(), a.data(), c_a_opt2.size() * sizeof(int))));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));

    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts_opt1 = {1, 1, 3, 1, 1, 3, 3, 3};
      std::vector<uint64_t> exp_ts_opt2 = {1, 1, 3, 1, 3, 1, 3, 3};
      CHECK(
          (!memcmp(
               exp_ts_opt1.data(),
               timestamps.data(),
               exp_ts_opt1.size() * sizeof(uint64_t)) ||
           !memcmp(
               exp_ts_opt2.data(),
               timestamps.data(),
               exp_ts_opt2.size() * sizeof(uint64_t))));
    }
  }

  SECTION("Read between the 2 writes") {
    // Should see only first write.
    read_sparse(a, dim1, dim2, stats, layout, 2, timestamps_ptr);

    std::vector<int> c_a = {0, 1, 2, 3};
    std::vector<uint64_t> c_dim1 = {1, 1, 1, 2};
    std::vector<uint64_t> c_dim2 = {1, 2, 4, 3};
    CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts = {1, 1, 1, 1};
      CHECK(!memcmp(
          exp_ts.data(), timestamps.data(), exp_ts.size() * sizeof(uint64_t)));
    }
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, partial read without dups",
    "[cppapi][consolidation-with-timestamps][partial-read][no-dups][non-"
    "rest]") {
  remove_sparse_array();
  // No duplicates.
  create_sparse_array();

  // Write first fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  // Write second fragment.
  write_sparse({8, 9, 10, 11}, {2, 2, 3, 3}, {2, 3, 2, 3}, 3);

  // Consolidate.
  bool vacuum = GENERATE(true, false);
  consolidate_sparse(vacuum);

  // Test read for both refactored and legacy.
  bool legacy = GENERATE(true, false);
  if (legacy) {
    set_legacy();
  }

  std::string stats;
  std::vector<int> a(10);
  std::vector<uint64_t> dim1(10);
  std::vector<uint64_t> dim2(10);
  std::vector<uint64_t> timestamps(10);
  auto timestamps_ptr =
      GENERATE_REF(as<std::vector<uint64_t>*>{}, nullptr, &timestamps);

  SECTION("Read after all writes") {
    read_sparse(a, dim1, dim2, stats, TILEDB_GLOBAL_ORDER, 4, timestamps_ptr);

    std::vector<int> c_a = {0, 1, 8, 2, 9, 10, 11};
    std::vector<uint64_t> c_dim1 = {1, 1, 2, 1, 2, 3, 3};
    std::vector<uint64_t> c_dim2 = {1, 2, 2, 4, 3, 2, 3};
    CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts = {1, 1, 3, 1, 3, 3, 3};
      CHECK(!memcmp(
          exp_ts.data(), timestamps.data(), exp_ts.size() * sizeof(uint64_t)));
    }
  }

  SECTION("Read between the 2 writes") {
    read_sparse(a, dim1, dim2, stats, TILEDB_GLOBAL_ORDER, 2, timestamps_ptr);

    std::vector<int> c_a = {0, 1, 2, 3};
    std::vector<uint64_t> c_dim1 = {1, 1, 1, 2};
    std::vector<uint64_t> c_dim2 = {1, 2, 4, 3};
    CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts = {1, 1, 1, 1};
      CHECK(!memcmp(
          exp_ts.data(), timestamps.data(), exp_ts.size() * sizeof(uint64_t)));
    }
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, full and partial read with "
    "dups",
    "[cppapi][consolidation-with-timestamps][partial-read][dups][non-rest]") {
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

  // Consolidate.
  consolidate_sparse(3, 7, vacuum);

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

  SECTION("Read after all writes") {
    // Read after both writes - should see everything.
    read_sparse(a, dim1, dim2, stats, layout, 7, timestamps_ptr);

    if (layout == TILEDB_UNORDERED) {
      // Results in global order of fragments.
      std::vector<int> c_a = {
          0, 1, 4, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
      std::vector<uint64_t> c_dim1 = {
          1, 1, 2, 1, 2, 2, 3, 3, 2, 1, 3, 4, 4, 3, 3, 4};
      std::vector<uint64_t> c_dim2 = {
          1, 2, 2, 4, 3, 4, 2, 3, 1, 3, 1, 1, 2, 3, 4, 4};
      CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
      CHECK(!memcmp(
          c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
      CHECK(!memcmp(
          c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
      if (timestamps_ptr != nullptr) {
        std::vector<uint64_t> exp_ts = {
            1, 1, 3, 1, 1, 3, 3, 3, 4, 4, 4, 4, 6, 6, 6, 6};
        CHECK(!memcmp(
            exp_ts.data(),
            timestamps.data(),
            exp_ts.size() * sizeof(uint64_t)));
      }
    } else {
      // result in cell global order.
      std::vector<int> c_a_opt1 = {
          0, 1, 8, 4, 9, 2, 3, 5, 10, 6, 11, 12, 7, 13, 14, 15};
      std::vector<int> c_a_opt2 = {
          0, 1, 8, 4, 9, 2, 3, 5, 10, 6, 11, 12, 13, 7, 14, 15};
      std::vector<uint64_t> c_dim1 = {
          1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 3, 4};
      std::vector<uint64_t> c_dim2 = {
          1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 3, 4, 4};
      CHECK(
          (!memcmp(c_a_opt1.data(), a.data(), c_a_opt1.size() * sizeof(int)) ||
           !memcmp(c_a_opt2.data(), a.data(), c_a_opt2.size() * sizeof(int))));
      CHECK(!memcmp(
          c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
      CHECK(!memcmp(
          c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
      if (timestamps_ptr != nullptr) {
        std::vector<uint64_t> exp_ts_opt1 = {
            1, 1, 4, 3, 4, 1, 1, 3, 4, 3, 4, 6, 3, 6, 6, 6};
        std::vector<uint64_t> exp_ts_opt2 = {
            1, 1, 4, 3, 4, 1, 1, 3, 4, 3, 4, 6, 6, 3, 6, 6};
        CHECK(
            (!memcmp(
                 exp_ts_opt1.data(),
                 timestamps.data(),
                 exp_ts_opt1.size() * sizeof(uint64_t)) ||
             !memcmp(
                 exp_ts_opt2.data(),
                 timestamps.data(),
                 exp_ts_opt2.size() * sizeof(uint64_t))));
      }
    }
  }

  // Read with full coverage on first 2 consolidated, partial coverage on second
  // 2 consolidated.
  SECTION("Read between the 2 writes") {
    read_sparse(a, dim1, dim2, stats, layout, 5, timestamps_ptr);

    if (layout == TILEDB_UNORDERED) {
      // results in global order of fragments
      std::vector<int> c_a = {0, 1, 4, 2, 3, 5, 6, 7, 8, 9, 10, 11};
      std::vector<uint64_t> c_dim1 = {1, 1, 2, 1, 2, 2, 3, 3, 2, 1, 3, 4};
      std::vector<uint64_t> c_dim2 = {1, 2, 2, 4, 3, 4, 2, 3, 1, 3, 1, 1};
      CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
      CHECK(!memcmp(
          c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
      CHECK(!memcmp(
          c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
      if (timestamps_ptr != nullptr) {
        std::vector<uint64_t> exp_ts = {1, 1, 3, 1, 1, 3, 3, 3, 4, 4, 4, 4};
        CHECK(!memcmp(
            exp_ts.data(),
            timestamps.data(),
            exp_ts.size() * sizeof(uint64_t)));
      }
    } else {
      // Result in cell global order.
      std::vector<int> c_a = {0, 1, 8, 4, 9, 2, 3, 5, 10, 6, 11, 7};
      std::vector<uint64_t> c_dim1 = {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 3};
      std::vector<uint64_t> c_dim2 = {1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 3};
      CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
      CHECK(!memcmp(
          c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
      CHECK(!memcmp(
          c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
      if (timestamps_ptr != nullptr) {
        std::vector<uint64_t> exp_ts = {1, 1, 4, 3, 4, 1, 1, 3, 4, 3, 4, 3};
        CHECK(!memcmp(
            exp_ts.data(),
            timestamps.data(),
            exp_ts.size() * sizeof(uint64_t)));
      }
    }
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, reopen",
    "[cppapi][consolidation-with-timestamps][partial-read][dups][non-rest]") {
  remove_sparse_array();
  // Enable duplicates.
  create_sparse_array(true);

  // Write first fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);
  // Write second fragment.
  write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 3, 2, 3}, 3);
  // Write third  fragment.
  write_sparse({8, 9, 10, 11}, {1, 2, 3, 4}, {3, 4, 1, 1}, 5);

  // Consolidate.
  bool vacuum = GENERATE(true, false);
  consolidate_sparse(vacuum);

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

  SECTION("Read in the middle") {
    // Read between 2 and 4.
    reopen_sparse(a, dim1, dim2, stats, layout, 2, 4, timestamps_ptr);

    // Expect to read only what was written at time 3.
    std::vector<int> c_a = {4, 5, 6, 7};
    std::vector<uint64_t> c_dim1 = {2, 2, 3, 3};
    std::vector<uint64_t> c_dim2 = {2, 3, 2, 3};
    CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts = {3, 3, 3, 3};
      CHECK(!memcmp(
          exp_ts.data(), timestamps.data(), exp_ts.size() * sizeof(uint64_t)));
    }
  }

  SECTION("Read the last 2 writes only") {
    reopen_sparse(a, dim1, dim2, stats, layout, 2, 6, timestamps_ptr);

    // Expect to read what the last 2 writes wrote.
    std::vector<int> c_a = {4, 8, 5, 9, 10, 6, 11, 7};
    std::vector<uint64_t> c_dim1 = {2, 1, 2, 2, 3, 3, 4, 3};
    std::vector<uint64_t> c_dim2 = {2, 3, 3, 4, 1, 2, 1, 3};
    CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts = {3, 5, 3, 5, 5, 3, 5, 3};
      CHECK(!memcmp(
          exp_ts.data(), timestamps.data(), exp_ts.size() * sizeof(uint64_t)));
    }
  }

  SECTION("Read the first 2 writes only") {
    reopen_sparse(a, dim1, dim2, stats, layout, 0, 4, timestamps_ptr);

    // Expect to read what the first 2 writes wrote.
    std::vector<int> c_a_opt1 = {0, 1, 4, 2, 5, 3, 6, 7};
    std::vector<int> c_a_opt2 = {0, 1, 4, 2, 3, 5, 6, 7};
    std::vector<uint64_t> c_dim1 = {1, 1, 2, 1, 2, 2, 3, 3};
    std::vector<uint64_t> c_dim2 = {1, 2, 2, 4, 3, 3, 2, 3};
    CHECK(
        (!memcmp(c_a_opt1.data(), a.data(), c_a_opt1.size() * sizeof(int)) ||
         !memcmp(c_a_opt2.data(), a.data(), c_a_opt2.size() * sizeof(int))));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts_opt1 = {1, 1, 3, 1, 3, 1, 3, 3};
      std::vector<uint64_t> exp_ts_opt2 = {1, 1, 3, 1, 1, 3, 3, 3};
      CHECK(
          (!memcmp(
               exp_ts_opt1.data(),
               timestamps.data(),
               exp_ts_opt1.size() * sizeof(uint64_t)) ||
           !memcmp(
               exp_ts_opt2.data(),
               timestamps.data(),
               exp_ts_opt2.size() * sizeof(uint64_t))));
    }
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test read timestamps of unconsolidated fragments, unordered "
    "reader",
    "[cppapi][consolidation-with-timestamps][read-timestamps][unordered-"
    "reader][non-rest]") {
  remove_sparse_array();
  // Enable duplicates.
  create_sparse_array(true);

  // Write first fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);
  // Write second fragment.
  write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 3, 2, 3}, 3);
  // Write third  fragment.
  write_sparse({8, 9, 10, 11}, {1, 2, 3, 4}, {3, 4, 1, 1}, 5);

  tiledb_layout_t layout = TILEDB_UNORDERED;

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

  SECTION("Read in the middle") {
    // Read between 2 and 4
    reopen_sparse(a, dim1, dim2, stats, layout, 2, 4, timestamps_ptr);

    // Expect to read only what was written at time 3.
    std::vector<int> c_a = {4, 5, 6, 7};
    std::vector<uint64_t> c_dim1 = {2, 2, 3, 3};
    std::vector<uint64_t> c_dim2 = {2, 3, 2, 3};
    CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts = {3, 3, 3, 3};
      CHECK(!memcmp(
          exp_ts.data(), timestamps.data(), exp_ts.size() * sizeof(uint64_t)));
    }
  }

  SECTION("Read the last 2 writes only") {
    reopen_sparse(a, dim1, dim2, stats, layout, 2, 6, timestamps_ptr);

    // Expect to read what the last 2 writes wrote.
    std::vector<int> c_a = {4, 5, 6, 7, 8, 9, 10, 11};
    std::vector<uint64_t> c_dim1 = {2, 2, 3, 3, 1, 2, 3, 4};
    std::vector<uint64_t> c_dim2 = {2, 3, 2, 3, 3, 4, 1, 1};
    CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts = {3, 3, 3, 3, 5, 5, 5, 5};
      CHECK(!memcmp(
          exp_ts.data(), timestamps.data(), exp_ts.size() * sizeof(uint64_t)));
    }
  }

  SECTION("Read the first 2 writes only") {
    reopen_sparse(a, dim1, dim2, stats, layout, 0, 4, timestamps_ptr);

    // Expect to read what the first 2 writes wrote.
    std::vector<int> c_a = {0, 1, 2, 3, 4, 5, 6, 7};
    std::vector<uint64_t> c_dim1 = {1, 1, 1, 2, 2, 2, 3, 3};
    std::vector<uint64_t> c_dim2 = {1, 2, 4, 3, 2, 3, 2, 3};
    CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts = {1, 1, 1, 1, 3, 3, 3, 3};
      CHECK(!memcmp(
          exp_ts.data(), timestamps.data(), exp_ts.size() * sizeof(uint64_t)));
    }
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test read timestamps, unordered reader, overlapping ranges",
    "[cppapi][consolidation-with-timestamps][read-timestamps][unordered-"
    "reader][overlapping-ranges][SC-18541][non-rest]") {
  remove_sparse_array();
  // Enable duplicates.
  create_sparse_array(true);

  // Disable merge overlapping sparse ranges.
  // Support for returning multiplicities for overlapping ranges will be
  // deprecated in a few releases. Turning off this setting allows to still
  // test that the feature functions properly until we do so. Once support is
  // fully removed for overlapping ranges, this test case can be deleted.
  tiledb::Config cfg;
  cfg.set("sm.merge_overlapping_ranges_experimental", "false");
  cfg["sm.mem.consolidation.buffers_weight"] = "1";
  cfg["sm.mem.consolidation.reader_weight"] = "5000";
  cfg["sm.mem.consolidation.writer_weight"] = "5000";
  ctx_ = Context(cfg);
  resources_ = &ctx_.ptr().get()->resources();
  vfs_ = VFS(ctx_);

  // Write first fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);
  // Write second fragment.
  write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 3, 2, 3}, 3);
  // Write third  fragment.
  write_sparse({8, 9, 10, 11}, {1, 2, 3, 4}, {3, 4, 1, 1}, 5);

  // bool consolidate = GENERATE(true, false);
  bool consolidate = true;
  if (consolidate) {
    consolidate_sparse();
  }

  std::string stats;
  std::vector<int> a(16);
  std::vector<uint64_t> dim1(16);
  std::vector<uint64_t> dim2(16);
  std::vector<uint64_t> timestamps(16);

  // Open array
  Array array(ctx_, SPARSE_ARRAY_NAME, TILEDB_READ);
  array.set_open_timestamp_start(0);
  array.set_open_timestamp_end(4);
  array.reopen();

  // Create query
  Query query(ctx_, array, TILEDB_READ);
  query.set_layout(TILEDB_UNORDERED);
  query.set_data_buffer("a1", a);
  query.set_data_buffer("d1", dim1);
  query.set_data_buffer("d2", dim2);
  query.set_data_buffer(tiledb_timestamps(), timestamps);

  // Add overlapping ranges
  Subarray subarray(ctx_, array);
  subarray.add_range<uint64_t>(1, 2, 3);
  subarray.add_range<uint64_t>(1, 2, 3);
  subarray.set_config(cfg);
  query.set_subarray(subarray);

  // Submit/finalize the query
  query.submit();
  CHECK(query.query_status() == Query::Status::COMPLETE);

  // Expect to read what the first 2 writes wrote: each element will
  // appear twice because of the identical overlapping ranges. The order
  // of results varies if consolidated or not.
  if (!consolidate) {
    // Results in global order of fragments
    std::vector<int> c_a = {1, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7};
    std::vector<uint64_t> c_dim1 = {1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3};
    std::vector<uint64_t> c_dim2 = {2, 2, 3, 3, 2, 2, 3, 3, 2, 2, 3, 3};
    std::vector<uint64_t> exp_ts = {1, 1, 1, 1, 3, 3, 3, 3, 3, 3, 3, 3};
    CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    CHECK(!memcmp(
        exp_ts.data(), timestamps.data(), exp_ts.size() * sizeof(uint64_t)));
  } else {
    // Result in cell global order in consolidated fragment.
    std::vector<int> c_a_opt1 = {1, 1, 4, 4, 3, 3, 5, 5, 6, 6, 7, 7};
    std::vector<int> c_a_opt2 = {1, 1, 4, 4, 5, 5, 3, 3, 6, 6, 7, 7};
    std::vector<uint64_t> c_dim1 = {1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3};
    std::vector<uint64_t> c_dim2 = {2, 2, 2, 2, 3, 3, 3, 3, 2, 2, 3, 3};
    std::vector<uint64_t> exp_ts_opt1 = {1, 1, 3, 3, 3, 3, 1, 1, 3, 3, 3, 3};
    std::vector<uint64_t> exp_ts_opt2 = {1, 1, 3, 3, 1, 1, 3, 3, 3, 3, 3, 3};
    CHECK(
        (!memcmp(c_a_opt1.data(), a.data(), c_a_opt1.size() * sizeof(int)) ||
         !memcmp(c_a_opt2.data(), a.data(), c_a_opt2.size() * sizeof(int))));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    CHECK(
        (!memcmp(
             exp_ts_opt1.data(),
             timestamps.data(),
             exp_ts_opt1.size() * sizeof(uint64_t)) ||
         !memcmp(
             exp_ts_opt2.data(),
             timestamps.data(),
             exp_ts_opt2.size() * sizeof(uint64_t))));
  }

  // Write first fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);
  // Write second fragment.
  write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 3, 2, 3}, 3);

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test read timestamps of unconsolidated fragments, global order "
    "reader",
    "[cppapi][consolidation-with-timestamps][read-timestamps][global-order-"
    "reader][non-rest]") {
  remove_sparse_array();
  // Enable duplicates.
  create_sparse_array(true);

  // Write first fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);
  // Write second fragment.
  write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 3, 2, 3}, 3);
  // Write third  fragment.
  write_sparse({8, 9, 10, 11}, {1, 2, 3, 4}, {3, 4, 1, 1}, 5);

  tiledb_layout_t layout = TILEDB_GLOBAL_ORDER;

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

  SECTION("Read in the middle") {
    // Read between 2 and 4.
    reopen_sparse(a, dim1, dim2, stats, layout, 2, 4, timestamps_ptr);

    // Expect to read only what was written at time 3.
    std::vector<int> c_a = {4, 5, 6, 7};
    std::vector<uint64_t> c_dim1 = {2, 2, 3, 3};
    std::vector<uint64_t> c_dim2 = {2, 3, 2, 3};
    CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts = {3, 3, 3, 3};
      CHECK(!memcmp(
          exp_ts.data(), timestamps.data(), exp_ts.size() * sizeof(uint64_t)));
    }
  }

  SECTION("Read the last 2 writes only") {
    reopen_sparse(a, dim1, dim2, stats, layout, 2, 6, timestamps_ptr);

    // Expect to read what the last 2 writes wrote.
    std::vector<int> c_a = {4, 8, 5, 9, 10, 6, 11, 7};
    std::vector<uint64_t> c_dim1 = {2, 1, 2, 2, 3, 3, 4, 3};
    std::vector<uint64_t> c_dim2 = {2, 3, 3, 4, 1, 2, 1, 3};
    CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts = {3, 5, 3, 5, 5, 3, 5, 3};
      CHECK(!memcmp(
          exp_ts.data(), timestamps.data(), exp_ts.size() * sizeof(uint64_t)));
    }
  }

  SECTION("Read the first 2 writes only") {
    reopen_sparse(a, dim1, dim2, stats, layout, 0, 4, timestamps_ptr);

    // Expect to read what the first 2 writes wrote.
    std::vector<int> c_a_opt1 = {0, 1, 4, 2, 5, 3, 6, 7};
    std::vector<int> c_a_opt2 = {0, 1, 4, 2, 3, 5, 6, 7};
    std::vector<uint64_t> c_dim1 = {1, 1, 2, 1, 2, 2, 3, 3};
    std::vector<uint64_t> c_dim2 = {1, 2, 2, 4, 3, 3, 2, 3};
    CHECK(
        (!memcmp(c_a_opt1.data(), a.data(), c_a_opt1.size() * sizeof(int)) ||
         !memcmp(c_a_opt2.data(), a.data(), c_a_opt2.size() * sizeof(int))));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    if (timestamps_ptr != nullptr) {
      std::vector<uint64_t> exp_ts_opt1 = {1, 1, 3, 1, 3, 1, 3, 3};
      std::vector<uint64_t> exp_ts_opt2 = {1, 1, 3, 1, 1, 3, 3, 3};
      CHECK(
          (!memcmp(
               exp_ts_opt1.data(),
               timestamps.data(),
               exp_ts_opt1.size() * sizeof(uint64_t)) ||
           !memcmp(
               exp_ts_opt2.data(),
               timestamps.data(),
               exp_ts_opt2.size() * sizeof(uint64_t))));
    }
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, check number of tiles read",
    "[cppapi][consolidation-with-timestamps][partial-read][stats][non-rest]") {
  remove_sparse_array();
  // Enable duplicates.
  create_sparse_array(true);

  // Write first fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);
  // Write second fragment.
  write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 4, 2, 3}, 3);

  // Consolidate first 2 fragments into 1:3
  bool vacuum = GENERATE(true, false);
  consolidate_sparse(vacuum);

  // Write third fragment.
  write_sparse({8, 9, 10, 11}, {2, 1, 3, 4}, {1, 3, 1, 1}, 5);
  // Write fourth fragment.
  write_sparse({12, 13, 14, 15}, {4, 3, 3, 4}, {2, 3, 4, 4}, 7);

  // Consolidate last 2 fragments into 5:7.
  consolidate_sparse(4, 7, vacuum);

  std::string stats;
  std::vector<int> a(16);
  std::vector<uint64_t> dim1(16);
  std::vector<uint64_t> dim2(16);
  tiledb_layout_t layout = TILEDB_UNORDERED;

  SECTION("Partial overlap or not") {
    // Read one tile for dim1, dim2, attr a1 and timestamp = 4, since 0:2 array
    // partially overlaps with the first 1:3 consolidated fragment.
    read_sparse(a, dim1, dim2, stats, layout, 2);
    CHECK(
        stats.find("\"Context.Query.Reader.num_tiles_read\": 4") !=
        std::string::npos);

    // Same but skip timestamps: 4 - 1 = 3, since 0:4 array fully overlaps with
    // the first 1:3 consolidated fragment, so no need to check timestamps for
    // cells.
    read_sparse(a, dim1, dim2, stats, layout, 4);
    CHECK(
        stats.find("\"Context.Query.Reader.num_tiles_read\": 3") !=
        std::string::npos);

    // Read 2 tiles for dim1, dim2, attr a1 and 1 for timestamps: 3 * 2 + 1 = 7
    // since 0:6 array fully overlaps with the first 1:3, but only partially.
    // overlaps with the second 5:7 consolidated fragment.
    read_sparse(a, dim1, dim2, stats, layout, 6);
    CHECK(
        stats.find("\"Context.Query.Reader.num_tiles_read\": 7") !=
        std::string::npos);

    // Read 2 tiles for dim1, dim2, attr a1 and skip timestamps: 3 * 2, since
    // 0:8 array fully overlaps with both consolidateded fragments 1:3 and 5:7.
    read_sparse(a, dim1, dim2, stats, layout, 8);
    CHECK(
        stats.find("\"Context.Query.Reader.num_tiles_read\": 6") !=
        std::string::npos);

    // Read 2 tiles for dim1, dim2, attr a1 and timestamps: 4 * 2, since 3:5
    // array partially overlaps with both consolidateded fragments 1:3 and 5:7.
    reopen_sparse(a, dim1, dim2, stats, layout, 3, 5);
    CHECK(
        stats.find("\"Context.Query.Reader.num_tiles_read\": 8") !=
        std::string::npos);
  }

  SECTION("User requested timestamps") {
    std::vector<uint64_t> timestamps(16);
    read_sparse(a, dim1, dim2, stats, layout, 4, &timestamps);
    CHECK(
        stats.find("\"Context.Query.Reader.num_tiles_read\": 4") !=
        std::string::npos);
  }

  SECTION("No duplicates") {
    // Set no dups.
    remove_sparse_array();
    create_sparse_array(false);

    // Write first fragment.
    write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);
    // Write second fragment.
    write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 4, 2, 3}, 3);

    // Consolidate first 2 fragments into 1:3.
    consolidate_sparse(vacuum);
    read_sparse(a, dim1, dim2, stats, layout, 4);
    CHECK(
        stats.find("\"Context.Query.Reader.num_tiles_read\": 4") !=
        std::string::npos);
  }

  SECTION("Fragment without timestamps") {
    // Set no dups.
    remove_sparse_array();
    create_sparse_array(false);

    // Write first fragment.
    write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);
    // Write second fragment.
    write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 4, 2, 3}, 3);

    // Read with partial overlap - expect no timestamps since fragment doesn't
    // have any.
    read_sparse(a, dim1, dim2, stats, layout, 2);
    CHECK(
        stats.find("\"Context.Query.Reader.num_tiles_read\": 3") !=
        std::string::npos);

    // Request timestamps - expect no timestamps getting read since fragment
    // doesn't have any.
    std::vector<uint64_t> timestamps(16);
    read_sparse(a, dim1, dim2, stats, layout, 4, &timestamps);
    CHECK(
        stats.find("\"Context.Query.Reader.num_tiles_read\": 6") !=
        std::string::npos);
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, consolidate consolidated"
    " fragment with timestamps",
    "[cppapi][consolidation-with-timestamps][frag-w-timestamps][non-rest]") {
  remove_sparse_array();

  bool dups = GENERATE(true, false);

  // Enable duplicates.
  create_sparse_array(dups);

  // Write first fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);
  // Write second fragment.
  write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 4, 2, 3}, 3);

  // Consolidate first 2 fragments into 1:3
  bool vacuum = GENERATE(true, false);
  consolidate_sparse(vacuum);

  // Write third fragment.
  write_sparse({8, 9, 10, 11}, {2, 1, 3, 4}, {1, 3, 1, 1}, 5);
  // Write fourth fragment.
  write_sparse({12, 13, 14, 15}, {4, 3, 3, 4}, {2, 3, 4, 4}, 7);

  // Consolidate last all fragments into 1:7.
  consolidate_sparse(1, 7, vacuum);

  std::string stats;
  std::vector<int> a(16);
  std::vector<uint64_t> dim1(16);
  std::vector<uint64_t> dim2(16);
  std::vector<uint64_t> timestamps(16);
  tiledb_layout_t layout = TILEDB_UNORDERED;

  // Read and validate.
  read_sparse(a, dim1, dim2, stats, layout, 8, &timestamps);

  // Cell [3, 3] is duplicated for values of a 7 and 13.
  if (dups) {
    std::vector<int> c_a_1 = {
        0, 1, 8, 4, 9, 2, 3, 5, 10, 6, 11, 12, 7, 13, 14, 15};
    std::vector<int> c_a_2 = {
        0, 1, 8, 4, 9, 2, 3, 5, 10, 6, 11, 12, 13, 7, 14, 15};
    std::vector<uint64_t> c_dim1 = {
        1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 3, 4};
    std::vector<uint64_t> c_dim2 = {
        1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 3, 4, 4};
    std::vector<uint64_t> c_ts = {
        1, 1, 5, 3, 5, 1, 1, 3, 5, 3, 5, 7, 7, 3, 7, 7};
    CHECK(
        (!memcmp(c_a_1.data(), a.data(), c_a_1.size() * sizeof(int)) ||
         !memcmp(c_a_2.data(), a.data(), c_a_2.size() * sizeof(int))));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    CHECK(!memcmp(
        c_ts.data(), timestamps.data(), c_ts.size() * sizeof(uint64_t)));
  } else {
    std::vector<int> c_a = {0, 1, 8, 4, 9, 2, 3, 5, 10, 6, 11, 12, 13, 14, 15};
    std::vector<uint64_t> c_dim1 = {
        1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4};
    std::vector<uint64_t> c_dim2 = {
        1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 4};
    std::vector<uint64_t> c_ts = {1, 1, 5, 3, 5, 1, 1, 3, 5, 3, 5, 7, 7, 7, 7};
    CHECK(!memcmp(c_a.data(), a.data(), c_a.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
    CHECK(!memcmp(
        c_ts.data(), timestamps.data(), c_ts.size() * sizeof(uint64_t)));
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "CPP API: Test consolidation with timestamps, ts tiles kept in memory",
    "[cppapi][consolidation-with-timestamps][ts-tile-in-memory][non-rest]") {
  remove_sparse_array();

  // Enable duplicates.
  create_sparse_array(true);

  // Write first fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);
  // Write second fragment.
  write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 4, 2, 3}, 3);

  // Consolidate first 2 fragments into 1:3
  consolidate_sparse(true);

  std::vector<int> a1(4);
  std::vector<uint64_t> dim1(4);
  std::vector<uint64_t> dim2(4);
  std::vector<uint64_t> timestamps(4);
  tiledb_layout_t layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);

  // Open array.
  Array array(ctx_, SPARSE_ARRAY_NAME, TILEDB_READ);

  // Create query.
  Query query(ctx_, array, TILEDB_READ);
  query.set_layout(layout);
  query.set_data_buffer("a1", a1);
  query.set_data_buffer("d1", dim1);
  query.set_data_buffer("d2", dim2);
  query.set_data_buffer(tiledb_timestamps(), timestamps);

  // Submit the query.
  query.submit();
  CHECK(query.query_status() == Query::Status::INCOMPLETE);

  // Validate.
  std::vector<int> c_a = {0, 1, 4, 2};
  std::vector<uint64_t> c_dim1 = {1, 1, 2, 1};
  std::vector<uint64_t> c_dim2 = {1, 2, 2, 4};
  std::vector<uint64_t> c_ts = {1, 1, 3, 1};
  CHECK(!memcmp(c_a.data(), a1.data(), c_a.size() * sizeof(int)));
  CHECK(!memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
  CHECK(!memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
  CHECK(
      !memcmp(c_ts.data(), timestamps.data(), c_ts.size() * sizeof(uint64_t)));

  // Submit again.
  query.submit();
  CHECK(query.query_status() == Query::Status::COMPLETE);

  // Validate.
  std::vector<int> c_a_2 = {3, 5, 6, 7};
  std::vector<uint64_t> c_dim1_2 = {2, 2, 3, 3};
  std::vector<uint64_t> c_dim2_2 = {3, 4, 2, 3};
  std::vector<uint64_t> c_ts_2 = {1, 3, 3, 3};
  CHECK(!memcmp(c_a_2.data(), a1.data(), c_a_2.size() * sizeof(int)));
  CHECK(!memcmp(
      c_dim1_2.data(), dim1.data(), c_dim1_2.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_dim2_2.data(), dim2.data(), c_dim2_2.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_ts_2.data(), timestamps.data(), c_ts_2.size() * sizeof(uint64_t)));

  // Close array.
  array.close();

  remove_sparse_array();
}
