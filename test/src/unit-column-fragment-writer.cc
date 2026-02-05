/**
 * @file   unit-column-fragment-writer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2026 TileDB, Inc.
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
 * Tests the ColumnFragmentWriter class.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/fragment/column_fragment_writer.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/storage_manager/context.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/sm/tile/tile_metadata_generator.h"
#include "tiledb/sm/tile/writer_tile_tuple.h"
#include "tiledb/storage_format/uri/generate_uri.h"

using namespace tiledb::sm;

namespace {

const std::string ARRAY_NAME = "column_fragment_writer_test_array";

/**
 * Test fixture for ColumnFragmentWriter tests.
 */
struct ColumnFragmentWriterFx {
  tiledb::Context ctx_;
  tiledb::VFS vfs_;

  ColumnFragmentWriterFx()
      : ctx_()
      , vfs_(ctx_) {
    if (vfs_.is_dir(ARRAY_NAME)) {
      vfs_.remove_dir(ARRAY_NAME);
    }
  }

  ~ColumnFragmentWriterFx() {
    if (vfs_.is_dir(ARRAY_NAME)) {
      vfs_.remove_dir(ARRAY_NAME);
    }
  }

  /**
   * Creates a simple dense array with a single int32 attribute.
   */
  void create_dense_array() {
    auto dim = tiledb::Dimension::create<int32_t>(ctx_, "d", {{0, 99}}, 10);
    tiledb::Domain dom(ctx_);
    dom.add_dimension(dim);

    auto attr = tiledb::Attribute::create<int32_t>(ctx_, "a");

    tiledb::ArraySchema schema(ctx_, TILEDB_DENSE);
    schema.set_domain(dom);
    schema.add_attribute(attr);
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_ROW_MAJOR);

    tiledb::Array::create(ARRAY_NAME, schema);
  }

  /**
   * Creates a sparse array with a single int32 dimension and attribute.
   */
  void create_sparse_array() {
    auto dim = tiledb::Dimension::create<int32_t>(ctx_, "d", {{0, 999}}, 100);
    tiledb::Domain dom(ctx_);
    dom.add_dimension(dim);

    auto attr = tiledb::Attribute::create<int32_t>(ctx_, "a");

    tiledb::ArraySchema schema(ctx_, TILEDB_SPARSE);
    schema.set_domain(dom);
    schema.add_attribute(attr);
    schema.set_capacity(10);

    tiledb::Array::create(ARRAY_NAME, schema);
  }

  /**
   * Creates a sparse array with a fixed-size dimension, a var-size dimension,
   * and a variable-size string attribute.
   */
  void create_varsize_array() {
    auto dim1 = tiledb::Dimension::create<int32_t>(ctx_, "d1", {{0, 999}}, 100);
    auto dim2 = tiledb::Dimension::create(
        ctx_, "d2", TILEDB_STRING_ASCII, nullptr, nullptr);
    tiledb::Domain dom(ctx_);
    dom.add_dimension(dim1);
    dom.add_dimension(dim2);

    auto attr = tiledb::Attribute::create<std::string>(ctx_, "a");
    attr.set_nullable(true);

    tiledb::ArraySchema schema(ctx_, TILEDB_SPARSE);
    schema.set_domain(dom);
    schema.add_attribute(attr);
    schema.set_capacity(10);

    tiledb::Array::create(ARRAY_NAME, schema);
  }

  /**
   * Creates a sparse array with a nullable int32 attribute.
   */
  void create_nullable_array() {
    auto dim = tiledb::Dimension::create<int32_t>(ctx_, "d", {{0, 999}}, 100);
    tiledb::Domain dom(ctx_);
    dom.add_dimension(dim);

    auto attr = tiledb::Attribute::create<int32_t>(ctx_, "a");
    attr.set_nullable(true);

    tiledb::ArraySchema schema(ctx_, TILEDB_SPARSE);
    schema.set_domain(dom);
    schema.add_attribute(attr);
    schema.set_capacity(10);

    tiledb::Array::create(ARRAY_NAME, schema);
  }

  /**
   * Creates a dense array with multiple tiles (domain 0-99, tile extent 10).
   */
  void create_multi_tile_dense_array() {
    auto dim = tiledb::Dimension::create<int32_t>(ctx_, "d", {{0, 99}}, 10);
    tiledb::Domain dom(ctx_);
    dom.add_dimension(dim);

    auto attr = tiledb::Attribute::create<int32_t>(ctx_, "a");

    tiledb::ArraySchema schema(ctx_, TILEDB_DENSE);
    schema.set_domain(dom);
    schema.add_attribute(attr);
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_ROW_MAJOR);

    tiledb::Array::create(ARRAY_NAME, schema);
  }

  /**
   * Gets context resources from the C++ API context.
   */
  ContextResources& get_resources() {
    return ctx_.ptr().get()->context().resources();
  }

  /**
   * Gets the array schema from an open array.
   */
  shared_ptr<const tiledb::sm::ArraySchema> get_array_schema() {
    tiledb::Array array(ctx_, ARRAY_NAME, TILEDB_READ);
    auto schema = array.ptr().get()->array()->array_schema_latest_ptr();
    array.close();
    return schema;
  }

  /**
   * Generates a fragment URI for testing.
   */
  URI generate_fragment_uri(uint64_t timestamp = 1) const {
    auto fragment_name = tiledb::storage_format::generate_timestamped_name(
        timestamp, constants::format_version);
    return URI(ARRAY_NAME)
        .join_path(constants::array_fragments_dir_name)
        .join_path(fragment_name);
  }
};

/**
 * Helper to filter a tile for testing purposes.
 */
void filter_tile_for_test(
    const std::string& name,
    WriterTileTuple& tile,
    const ArraySchema& schema,
    ContextResources& resources) {
  const bool var_size = schema.var_size(name);
  const bool nullable = schema.is_nullable(name);
  EncryptionKey enc_key;

  auto filter_single_tile = [&](WriterTile* t,
                                WriterTile* offsets_tile,
                                bool is_offsets,
                                bool is_validity) {
    FilterPipeline filters;
    if (is_offsets) {
      filters = schema.cell_var_offsets_filters();
    } else if (is_validity) {
      filters = schema.cell_validity_filters();
    } else {
      filters = schema.filters(name);
    }

    if (is_offsets &&
        filters.skip_offsets_filtering(schema.type(name), schema.version())) {
      t->filtered_buffer().expand(sizeof(uint64_t));
      uint64_t nchunks = 0;
      memcpy(t->filtered_buffer().data(), &nchunks, sizeof(uint64_t));
      t->clear_data();
      return;
    }

    throw_if_not_ok(
        FilterPipeline::append_encryption_filter(&filters, enc_key));
    bool use_chunking = filters.use_tile_chunking(
        schema.var_size(name), schema.version(), t->type());
    filters.run_forward(
        &resources.stats(),
        t,
        offsets_tile,
        &resources.compute_tp(),
        use_chunking);
  };

  if (var_size) {
    filter_single_tile(&tile.var_tile(), &tile.offset_tile(), false, false);
    filter_single_tile(&tile.offset_tile(), nullptr, true, false);
  } else {
    filter_single_tile(&tile.fixed_tile(), nullptr, false, false);
  }

  if (nullable) {
    filter_single_tile(&tile.validity_tile(), nullptr, false, true);
  }
}

}  // namespace

TEST_CASE_METHOD(
    ColumnFragmentWriterFx,
    "ColumnFragmentWriter: field lifecycle errors",
    "[column-fragment-writer]") {
  create_dense_array();

  auto array_schema = get_array_schema();
  auto fragment_uri = generate_fragment_uri();

  // Create domain for constructor
  int32_t domain_start = 0;
  int32_t domain_end = 9;
  NDRange non_empty_domain;
  non_empty_domain.emplace_back(
      Range(&domain_start, &domain_end, sizeof(int32_t)));

  ColumnFragmentWriter writer(
      &get_resources(),
      array_schema,
      fragment_uri,
      non_empty_domain,
      1);  // tile_count

  SECTION("Error: write_tile without open_field") {
    auto memory_tracker = tiledb::test::get_test_memory_tracker();
    auto tile = WriterTileTuple(
        *array_schema,
        10,
        false,
        false,
        sizeof(int32_t),
        Datatype::INT32,
        memory_tracker);

    REQUIRE_THROWS_WITH(
        writer.write_tile(tile),
        Catch::Matchers::ContainsSubstring("no field is currently open"));
  }

  SECTION("Error: close_field without open_field") {
    REQUIRE_THROWS_WITH(
        writer.close_field(),
        Catch::Matchers::ContainsSubstring("no field is currently open"));
  }

  SECTION("Error: open non-existent field") {
    REQUIRE_THROWS_WITH(
        writer.open_field("nonexistent"),
        Catch::Matchers::ContainsSubstring("does not exist in array schema"));
  }

  SECTION("Error: open field while another is open") {
    writer.open_field("a");
    REQUIRE_THROWS_WITH(
        writer.open_field("d"),
        Catch::Matchers::ContainsSubstring("is already open"));
  }

  SECTION("Error: finalize with field open") {
    writer.open_field("a");
    EncryptionKey enc_key;
    REQUIRE_THROWS_WITH(
        writer.finalize(enc_key),
        Catch::Matchers::ContainsSubstring("is still open"));
    // Don't close - the writer will be destroyed with the field still open
  }
}

TEST_CASE_METHOD(
    ColumnFragmentWriterFx,
    "ColumnFragmentWriter: write_tile validates input",
    "[column-fragment-writer]") {
  create_dense_array();

  auto array_schema = get_array_schema();
  auto fragment_uri = generate_fragment_uri();

  int32_t domain_start = 0;
  int32_t domain_end = 9;
  NDRange non_empty_domain;
  non_empty_domain.emplace_back(
      Range(&domain_start, &domain_end, sizeof(int32_t)));

  ColumnFragmentWriter writer(
      &get_resources(),
      array_schema,
      fragment_uri,
      non_empty_domain,
      1);  // tile_count

  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  auto tile = WriterTileTuple(
      *array_schema,
      10,
      false,
      false,
      sizeof(int32_t),
      Datatype::INT32,
      memory_tracker);

  writer.open_field("a");
  REQUIRE_THROWS_WITH(
      writer.write_tile(tile),
      Catch::Matchers::ContainsSubstring("tile is not filtered"));
}

TEST_CASE_METHOD(
    ColumnFragmentWriterFx,
    "ColumnFragmentWriter: accessors",
    "[column-fragment-writer]") {
  create_dense_array();

  auto array_schema = get_array_schema();
  auto fragment_uri = generate_fragment_uri();

  int32_t domain_start = 0;
  int32_t domain_end = 9;
  NDRange non_empty_domain;
  non_empty_domain.emplace_back(
      Range(&domain_start, &domain_end, sizeof(int32_t)));

  ColumnFragmentWriter writer(
      &get_resources(),
      array_schema,
      fragment_uri,
      non_empty_domain,
      1);  // tile_count

  // Test accessors
  CHECK(writer.fragment_uri() == fragment_uri);
  CHECK(writer.fragment_metadata() != nullptr);
}

TEST_CASE_METHOD(
    ColumnFragmentWriterFx,
    "ColumnFragmentWriter: sparse array requires MBRs",
    "[column-fragment-writer]") {
  create_sparse_array();

  auto array_schema = get_array_schema();
  auto fragment_uri = generate_fragment_uri();

  int32_t domain_start = 0;
  int32_t domain_end = 99;
  NDRange non_empty_domain;
  non_empty_domain.emplace_back(
      Range(&domain_start, &domain_end, sizeof(int32_t)));

  ColumnFragmentWriter writer(
      &get_resources(), array_schema, fragment_uri, non_empty_domain, 1);

  EncryptionKey enc_key;
  REQUIRE_THROWS_WITH(
      writer.finalize(enc_key),
      Catch::Matchers::ContainsSubstring("Call set_mbrs() first"));
}

TEST_CASE_METHOD(
    ColumnFragmentWriterFx,
    "ColumnFragmentWriter: dense array rejects set_mbrs",
    "[column-fragment-writer]") {
  create_dense_array();

  auto array_schema = get_array_schema();
  auto fragment_uri = generate_fragment_uri();

  int32_t domain_start = 0;
  int32_t domain_end = 9;
  NDRange non_empty_domain;
  non_empty_domain.emplace_back(
      Range(&domain_start, &domain_end, sizeof(int32_t)));

  ColumnFragmentWriter writer(
      &get_resources(), array_schema, fragment_uri, non_empty_domain);

  std::vector<NDRange> mbrs;
  REQUIRE_THROWS_WITH(
      writer.set_mbrs(std::move(mbrs)),
      Catch::Matchers::ContainsSubstring(
          "Dense arrays should not provide MBRs"));
}

TEST_CASE_METHOD(
    ColumnFragmentWriterFx,
    "ColumnFragmentWriter: verify standard API read roundtrip",
    "[column-fragment-writer]") {
  // First write data using the standard TileDB API
  create_dense_array();

  {
    tiledb::Array array(ctx_, ARRAY_NAME, TILEDB_WRITE);
    tiledb::Query query(ctx_, array, TILEDB_WRITE);
    query.set_layout(TILEDB_ROW_MAJOR);

    std::vector<int32_t> data(100);
    for (int i = 0; i < 100; i++) {
      data[i] = i * 10;
    }
    query.set_data_buffer("a", data);
    query.submit();
    array.close();
  }

  // Read back and verify using standard API
  // This confirms that fragments written by standard writers can be read,
  // which validates the patterns we're using in ColumnFragmentWriter
  {
    tiledb::Array read_array(ctx_, ARRAY_NAME, TILEDB_READ);
    tiledb::Query read_query(ctx_, read_array, TILEDB_READ);
    read_query.set_layout(TILEDB_ROW_MAJOR);

    // Set subarray for dense read
    tiledb::Subarray subarray(ctx_, read_array);
    subarray.add_range(0, 0, 99);
    read_query.set_subarray(subarray);

    std::vector<int32_t> data(100);
    read_query.set_data_buffer("a", data);
    read_query.submit();
    read_array.close();

    for (int i = 0; i < 100; i++) {
      CHECK(data[i] == i * 10);
    }
  }
}

TEST_CASE_METHOD(
    ColumnFragmentWriterFx,
    "ColumnFragmentWriter: sparse array with multiple tiles and MBRs",
    "[column-fragment-writer]") {
  create_sparse_array();

  auto array_schema = get_array_schema();
  auto fragment_uri = generate_fragment_uri(200);

  // Non-empty domain covering all coordinates across 3 tiles
  int32_t domain_start = 10;
  int32_t domain_end = 540;
  NDRange non_empty_domain;
  non_empty_domain.emplace_back(
      Range(&domain_start, &domain_end, sizeof(int32_t)));

  // Create writer for sparse array (tile_count=0 for dynamic growth)
  ColumnFragmentWriter writer(
      &get_resources(),
      array_schema,
      fragment_uri,
      non_empty_domain,
      0);  // dynamic tile count

  EncryptionKey enc_key;
  auto memory_tracker = tiledb::test::get_test_memory_tracker();

  // 3 tiles: first two full (10 cells), last partial (5 cells)
  std::vector<std::vector<int32_t>> tile_coords = {
      {10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
      {200, 210, 220, 230, 240, 250, 260, 270, 280, 290},
      {500, 510, 520, 530, 540}};

  // Write dimension "d" - 3 tiles
  {
    writer.open_field("d");

    for (int t = 0; t < 3; t++) {
      const uint64_t cell_num = tile_coords[t].size();
      auto tile = WriterTileTuple(
          *array_schema,
          cell_num,
          false,
          false,
          sizeof(int32_t),
          Datatype::INT32,
          memory_tracker);

      tile.fixed_tile().write(
          tile_coords[t].data(), 0, cell_num * sizeof(int32_t));
      tile.set_final_size(cell_num);

      TileMetadataGenerator md_gen(
          Datatype::INT32, true, false, sizeof(int32_t), 1);
      md_gen.process_full_tile(tile);
      md_gen.set_tile_metadata(tile);

      filter_tile_for_test("d", tile, *array_schema, get_resources());
      writer.write_tile(tile);
    }

    writer.close_field();
  }

  // Set MBRs after processing dimensions
  std::vector<NDRange> mbrs(3);
  for (int t = 0; t < 3; t++) {
    int32_t min_coord = tile_coords[t].front();
    int32_t max_coord = tile_coords[t].back();
    mbrs[t].emplace_back(Range(&min_coord, &max_coord, sizeof(int32_t)));
  }
  writer.set_mbrs(std::move(mbrs));

  // Write attribute "a" - 3 tiles
  {
    writer.open_field("a");

    for (int t = 0; t < 3; t++) {
      const uint64_t cell_num = tile_coords[t].size();
      auto tile = WriterTileTuple(
          *array_schema,
          cell_num,
          false,
          false,
          sizeof(int32_t),
          Datatype::INT32,
          memory_tracker);

      std::vector<int32_t> data(cell_num);
      for (uint64_t i = 0; i < cell_num; i++) {
        data[i] = t * 1000 + i;
      }
      tile.fixed_tile().write(data.data(), 0, data.size() * sizeof(int32_t));
      tile.set_final_size(cell_num);

      TileMetadataGenerator md_gen(
          Datatype::INT32, false, false, sizeof(int32_t), 1);
      md_gen.process_full_tile(tile);
      md_gen.set_tile_metadata(tile);

      filter_tile_for_test("a", tile, *array_schema, get_resources());
      writer.write_tile(tile);
    }

    writer.close_field();
  }

  // Finalize sparse fragment
  writer.finalize(enc_key);

  // Verify last_tile_cell_num matches the partial last tile
  CHECK(writer.fragment_metadata()->last_tile_cell_num() == 5);

  // Read back and verify using standard API
  {
    tiledb::Array read_array(ctx_, ARRAY_NAME, TILEDB_READ);
    tiledb::Query read_query(ctx_, read_array, TILEDB_READ);
    read_query.set_layout(TILEDB_UNORDERED);

    std::vector<int32_t> coords(25);
    std::vector<int32_t> data(25);
    read_query.set_data_buffer("d", coords);
    read_query.set_data_buffer("a", data);
    read_query.submit();
    read_array.close();

    // Verify all 3 tiles (10 + 10 + 5 cells)
    uint64_t offset = 0;
    for (int t = 0; t < 3; t++) {
      for (size_t i = 0; i < tile_coords[t].size(); i++) {
        CHECK(coords[offset + i] == tile_coords[t][i]);
        CHECK(data[offset + i] == t * 1000 + (int)i);
      }
      offset += tile_coords[t].size();
    }
  }

  // Verify global order bounds via FragmentInfo
  {
    tiledb::FragmentInfo fragment_info(ctx_, ARRAY_NAME);
    fragment_info.load();

    CHECK(fragment_info.fragment_num() == 1);
    CHECK(fragment_info.mbr_num(0) == 3);

    for (uint32_t t = 0; t < 3; t++) {
      auto lower = fragment_info.global_order_lower_bound(0, t);
      auto upper = fragment_info.global_order_upper_bound(0, t);
      CHECK(lower.size() == 1);  // 1 dimension
      CHECK(upper.size() == 1);

      int32_t expected_min = tile_coords[t].front();
      int32_t expected_max = tile_coords[t].back();
      int32_t actual_min, actual_max;
      memcpy(&actual_min, lower[0].data(), sizeof(int32_t));
      memcpy(&actual_max, upper[0].data(), sizeof(int32_t));
      CHECK(actual_min == expected_min);
      CHECK(actual_max == expected_max);
    }
  }
}

TEST_CASE_METHOD(
    ColumnFragmentWriterFx,
    "ColumnFragmentWriter: var-size attribute roundtrip",
    "[column-fragment-writer]") {
  create_varsize_array();

  auto array_schema = get_array_schema();
  auto fragment_uri = generate_fragment_uri(300);
  auto memory_tracker = tiledb::test::get_test_memory_tracker();

  CHECK(array_schema->var_size("d2") == true);
  CHECK(array_schema->var_size("a") == true);
  CHECK(array_schema->is_nullable("a") == true);

  // Non-empty domain: d1=[0,19], d2=["aa","zz"]
  int32_t d1_start = 0, d1_end = 19;
  std::string d2_min = "aa", d2_max = "zz";
  NDRange non_empty_domain;
  non_empty_domain.emplace_back(Range(&d1_start, &d1_end, sizeof(int32_t)));
  non_empty_domain.emplace_back(
      Range(d2_min.data(), d2_min.size(), d2_max.data(), d2_max.size()));

  ColumnFragmentWriter writer(
      &get_resources(), array_schema, fragment_uri, non_empty_domain);

  EncryptionKey enc_key;
  const uint64_t cell_num = 10;  // matches sparse capacity

  // Var-size dimension data for 2 tiles
  std::vector<std::vector<std::string>> d2_strings = {
      {"aa", "ab", "ac", "ad", "ae", "af", "ag", "ah", "ai", "aj"},
      {"ba", "bb", "bc", "bd", "be", "bf", "bg", "bh", "bi", "bj"}};

  // Write fixed-size dimension d1 - 2 tiles
  {
    writer.open_field("d1");

    for (int t = 0; t < 2; t++) {
      auto tile = WriterTileTuple(
          *array_schema,
          cell_num,
          false,
          false,
          sizeof(int32_t),
          Datatype::INT32,
          memory_tracker);

      std::vector<int32_t> dim_data(cell_num);
      for (uint64_t i = 0; i < cell_num; i++) {
        dim_data[i] = t * 10 + i;
      }
      tile.fixed_tile().write(
          dim_data.data(), 0, dim_data.size() * sizeof(int32_t));
      tile.set_final_size(cell_num);

      TileMetadataGenerator md_gen(
          Datatype::INT32, true, false, sizeof(int32_t), 1);
      md_gen.process_full_tile(tile);
      md_gen.set_tile_metadata(tile);

      filter_tile_for_test("d1", tile, *array_schema, get_resources());
      writer.write_tile(tile);
    }

    writer.close_field();
  }

  // Write var-size dimension d2 - 2 tiles
  {
    writer.open_field("d2");

    for (int t = 0; t < 2; t++) {
      auto tile = WriterTileTuple(
          *array_schema,
          cell_num,
          true,
          false,
          1,
          Datatype::STRING_ASCII,
          memory_tracker);

      std::string var_data;
      std::vector<uint64_t> offsets;
      for (const auto& s : d2_strings[t]) {
        offsets.push_back(var_data.size());
        var_data += s;
      }

      tile.offset_tile().write(
          offsets.data(), 0, offsets.size() * sizeof(uint64_t));
      tile.var_tile().write_var(var_data.c_str(), 0, var_data.size());
      tile.var_tile().set_size(var_data.size());
      tile.set_final_size(cell_num);

      TileMetadataGenerator md_gen(
          Datatype::STRING_ASCII, true, true, constants::var_num, 1);
      md_gen.process_full_tile(tile);
      md_gen.set_tile_metadata(tile);

      filter_tile_for_test("d2", tile, *array_schema, get_resources());
      writer.write_tile(tile);
    }

    writer.close_field();
  }

  // Set MBRs after processing dimensions (2 ranges per MBR: d1 and d2)
  std::vector<NDRange> mbrs(2);
  for (int t = 0; t < 2; t++) {
    int32_t lo = t * 10, hi = t * 10 + 9;
    mbrs[t].emplace_back(Range(&lo, &hi, sizeof(int32_t)));
    mbrs[t].emplace_back(Range(
        d2_strings[t].front().data(),
        d2_strings[t].front().size(),
        d2_strings[t].back().data(),
        d2_strings[t].back().size()));
  }
  writer.set_mbrs(std::move(mbrs));

  // Write var-size nullable attribute - 2 tiles
  // Tile 0: all null. Tile 1: valid strings.
  {
    writer.open_field("a");

    // Tile 0: all null
    {
      auto tile = WriterTileTuple(
          *array_schema,
          cell_num,
          true,
          true,
          1,
          Datatype::CHAR,
          memory_tracker);

      std::vector<uint64_t> offsets(cell_num, 0);
      tile.offset_tile().write(
          offsets.data(), 0, offsets.size() * sizeof(uint64_t));
      tile.var_tile().set_size(0);
      std::vector<uint8_t> validity(cell_num, 0);
      tile.validity_tile().write(validity.data(), 0, validity.size());
      tile.set_final_size(cell_num);

      TileMetadataGenerator md_gen(
          Datatype::STRING_ASCII, false, true, constants::var_num, 1);
      md_gen.process_full_tile(tile);
      md_gen.set_tile_metadata(tile);

      filter_tile_for_test("a", tile, *array_schema, get_resources());
      writer.write_tile(tile);
    }

    // Tile 1: strings with all-valid
    {
      auto tile = WriterTileTuple(
          *array_schema,
          cell_num,
          true,
          true,
          1,
          Datatype::CHAR,
          memory_tracker);

      std::vector<std::string> strings = {
          "hello",
          "world",
          "foo",
          "bar",
          "test",
          "alpha",
          "beta",
          "gamma",
          "delta",
          "epsilon"};
      std::string var_data;
      std::vector<uint64_t> offsets;
      for (const auto& s : strings) {
        offsets.push_back(var_data.size());
        var_data += s;
      }

      tile.offset_tile().write(
          offsets.data(), 0, offsets.size() * sizeof(uint64_t));
      tile.var_tile().write_var(var_data.c_str(), 0, var_data.size());
      tile.var_tile().set_size(var_data.size());
      std::vector<uint8_t> validity(cell_num, 1);
      tile.validity_tile().write(validity.data(), 0, validity.size());
      tile.set_final_size(cell_num);

      TileMetadataGenerator md_gen(
          Datatype::STRING_ASCII, false, true, constants::var_num, 1);
      md_gen.process_full_tile(tile);
      md_gen.set_tile_metadata(tile);

      filter_tile_for_test("a", tile, *array_schema, get_resources());
      writer.write_tile(tile);
    }

    writer.close_field();
  }

  writer.finalize(enc_key);

  // Verify var-size min/max metadata for tile 1 (the valid tile).
  // With the all-null tile first, an index mismatch bug would cause
  // tile 1's min/max to never be written to the metadata buffer.
  {
    auto frag_meta = writer.fragment_metadata();
    auto* loaded = frag_meta->loaded_metadata();
    auto& lm = loaded->loaded_metadata();
    for (size_t i = 0; i < lm.tile_min_.size(); i++) {
      lm.tile_min_[i] = true;
      lm.tile_max_[i] = true;
    }
    auto min_val = frag_meta->get_tile_min_as<std::string_view>("a", 1);
    auto max_val = frag_meta->get_tile_max_as<std::string_view>("a", 1);
    CHECK(min_val == "alpha");
    CHECK(max_val == "world");
  }

  // Verify var-size dimension d2 global order bounds via FragmentInfo
  {
    tiledb::FragmentInfo fragment_info(ctx_, ARRAY_NAME);
    fragment_info.load();
    // Check tile 0's d2 bounds: "aa" to "aj"
    auto lower = fragment_info.global_order_lower_bound(0, 0);
    auto upper = fragment_info.global_order_upper_bound(0, 0);
    std::string lower_d2(
        reinterpret_cast<const char*>(lower[1].data()), lower[1].size());
    std::string upper_d2(
        reinterpret_cast<const char*>(upper[1].data()), upper[1].size());
    CHECK(lower_d2 == "aa");
    CHECK(upper_d2 == "aj");
  }

  // Read back and verify
  {
    tiledb::Array read_array(ctx_, ARRAY_NAME, TILEDB_READ);
    tiledb::Query read_query(ctx_, read_array, TILEDB_READ);
    read_query.set_layout(TILEDB_UNORDERED);

    const uint64_t total_cells = 20;
    std::vector<int32_t> d1_result(total_cells);
    std::vector<uint64_t> d2_offsets(total_cells);
    std::string d2_data;
    d2_data.resize(100);
    std::vector<uint64_t> a_offsets(total_cells);
    std::string a_data;
    a_data.resize(200);
    std::vector<uint8_t> validity_result(total_cells);

    read_query.set_data_buffer("d1", d1_result);
    read_query.set_data_buffer("d2", d2_data);
    read_query.set_offsets_buffer("d2", d2_offsets);
    read_query.set_data_buffer("a", a_data);
    read_query.set_offsets_buffer("a", a_offsets);
    read_query.set_validity_buffer("a", validity_result);
    read_query.submit();
    read_array.close();

    auto result_num = read_query.result_buffer_elements()["a"];
    CHECK(result_num.first == total_cells);
    // Tile 0: all null attribute
    CHECK(d1_result[0] == 0);
    CHECK(validity_result[0] == 0);
    // Tile 1: valid strings
    CHECK(d1_result[10] == 10);
    CHECK(validity_result[10] == 1);
    CHECK(a_data.substr(a_offsets[10], 5) == "hello");
  }
}

TEST_CASE_METHOD(
    ColumnFragmentWriterFx,
    "ColumnFragmentWriter: nullable attribute roundtrip",
    "[column-fragment-writer]") {
  create_nullable_array();

  auto array_schema = get_array_schema();
  auto fragment_uri = generate_fragment_uri(400);
  auto memory_tracker = tiledb::test::get_test_memory_tracker();

  CHECK(array_schema->is_nullable("a") == true);

  int32_t domain_start = 0;
  int32_t domain_end = 9;
  NDRange non_empty_domain;
  non_empty_domain.emplace_back(
      Range(&domain_start, &domain_end, sizeof(int32_t)));

  ColumnFragmentWriter writer(
      &get_resources(), array_schema, fragment_uri, non_empty_domain, 1);

  EncryptionKey enc_key;
  const uint64_t cell_num = 10;  // matches sparse capacity

  // Write dimension
  {
    writer.open_field("d");

    auto tile = WriterTileTuple(
        *array_schema,
        cell_num,
        false,
        false,
        sizeof(int32_t),
        Datatype::INT32,
        memory_tracker);

    std::vector<int32_t> dim_data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    tile.fixed_tile().write(
        dim_data.data(), 0, dim_data.size() * sizeof(int32_t));
    tile.set_final_size(cell_num);

    TileMetadataGenerator md_gen(
        Datatype::INT32, true, false, sizeof(int32_t), 1);
    md_gen.process_full_tile(tile);
    md_gen.set_tile_metadata(tile);

    filter_tile_for_test("d", tile, *array_schema, get_resources());
    writer.write_tile(tile);
    writer.close_field();
  }

  // Set MBRs after processing dimensions
  std::vector<NDRange> mbrs(1);
  mbrs[0].emplace_back(Range(&domain_start, &domain_end, sizeof(int32_t)));
  writer.set_mbrs(std::move(mbrs));

  // Write nullable attribute (values at odd indices are null)
  {
    writer.open_field("a");

    auto tile = WriterTileTuple(
        *array_schema,
        cell_num,
        false,
        true,
        sizeof(int32_t),
        Datatype::INT32,
        memory_tracker);

    std::vector<int32_t> data = {100, 0, 300, 0, 500, 600, 0, 800, 0, 1000};
    std::vector<uint8_t> validity = {1, 0, 1, 0, 1, 1, 0, 1, 0, 1};

    tile.fixed_tile().write(data.data(), 0, data.size() * sizeof(int32_t));
    tile.validity_tile().write(validity.data(), 0, validity.size());
    tile.set_final_size(cell_num);

    TileMetadataGenerator md_gen(
        Datatype::INT32, false, false, sizeof(int32_t), 1);
    md_gen.process_full_tile(tile);
    md_gen.set_tile_metadata(tile);

    filter_tile_for_test("a", tile, *array_schema, get_resources());
    writer.write_tile(tile);
    writer.close_field();
  }

  writer.finalize(enc_key);

  // Read back and verify
  {
    tiledb::Array read_array(ctx_, ARRAY_NAME, TILEDB_READ);
    tiledb::Query read_query(ctx_, read_array, TILEDB_READ);
    read_query.set_layout(TILEDB_UNORDERED);

    std::vector<int32_t> dim_result(cell_num);
    std::vector<int32_t> data_result(cell_num);
    std::vector<uint8_t> validity_result(cell_num);

    read_query.set_data_buffer("d", dim_result);
    read_query.set_data_buffer("a", data_result);
    read_query.set_validity_buffer("a", validity_result);
    read_query.submit();
    read_array.close();

    CHECK(dim_result[0] == 0);
    CHECK(dim_result[9] == 9);

    // Check validity and values
    CHECK(validity_result[0] == 1);
    CHECK(validity_result[1] == 0);
    CHECK(validity_result[5] == 1);
    CHECK(validity_result[6] == 0);
    CHECK(data_result[0] == 100);
    CHECK(data_result[5] == 600);
    CHECK(data_result[9] == 1000);
  }
}

TEST_CASE_METHOD(
    ColumnFragmentWriterFx,
    "ColumnFragmentWriter: multiple tiles per field",
    "[column-fragment-writer]") {
  create_multi_tile_dense_array();

  auto array_schema = get_array_schema();
  auto fragment_uri = generate_fragment_uri(500);

  // Initialize with domain [0, 29] covering 3 tiles (tile extent is 10)
  int32_t domain_start = 0;
  int32_t domain_end = 29;
  NDRange non_empty_domain;
  non_empty_domain.emplace_back(
      Range(&domain_start, &domain_end, sizeof(int32_t)));

  ColumnFragmentWriter writer(
      &get_resources(),
      array_schema,
      fragment_uri,
      non_empty_domain,
      3);  // 3 tiles

  EncryptionKey enc_key;
  auto memory_tracker = tiledb::test::get_test_memory_tracker();

  // Write attribute "a" - 3 tiles
  {
    writer.open_field("a");

    for (int tile_idx = 0; tile_idx < 3; tile_idx++) {
      auto tile = WriterTileTuple(
          *array_schema,
          10,
          false,
          false,
          sizeof(int32_t),
          Datatype::INT32,
          memory_tracker);

      // Each tile has values tile_idx*1000 + i for i in 0..9
      std::vector<int32_t> data(10);
      for (int i = 0; i < 10; i++) {
        data[i] = tile_idx * 1000 + i;
      }
      tile.fixed_tile().write(data.data(), 0, data.size() * sizeof(int32_t));
      tile.set_final_size(10);

      TileMetadataGenerator md_gen(
          Datatype::INT32, false, false, sizeof(int32_t), 1);
      md_gen.process_full_tile(tile);
      md_gen.set_tile_metadata(tile);

      filter_tile_for_test("a", tile, *array_schema, get_resources());
      writer.write_tile(tile);
    }

    writer.close_field();
  }

  writer.finalize(enc_key);

  // Read back and verify
  {
    tiledb::Array read_array(ctx_, ARRAY_NAME, TILEDB_READ);
    tiledb::Query read_query(ctx_, read_array, TILEDB_READ);
    read_query.set_layout(TILEDB_ROW_MAJOR);

    tiledb::Subarray subarray(ctx_, read_array);
    subarray.add_range(0, 0, 29);
    read_query.set_subarray(subarray);

    std::vector<int32_t> result(30);
    read_query.set_data_buffer("a", result);
    read_query.submit();
    read_array.close();

    // Verify all 30 values across 3 tiles
    for (int tile_idx = 0; tile_idx < 3; tile_idx++) {
      for (int i = 0; i < 10; i++) {
        CHECK(result[tile_idx * 10 + i] == tile_idx * 1000 + i);
      }
    }
  }
}

TEST_CASE_METHOD(
    ColumnFragmentWriterFx,
    "ColumnFragmentWriter: tile count overflow error",
    "[column-fragment-writer]") {
  create_dense_array();

  auto array_schema = get_array_schema();
  auto fragment_uri = generate_fragment_uri(600);

  int32_t domain_start = 0;
  int32_t domain_end = 9;
  NDRange non_empty_domain;
  non_empty_domain.emplace_back(
      Range(&domain_start, &domain_end, sizeof(int32_t)));

  ColumnFragmentWriter writer(
      &get_resources(),
      array_schema,
      fragment_uri,
      non_empty_domain,
      1);  // Only 1 tile allowed

  writer.open_field("a");
  auto memory_tracker = tiledb::test::get_test_memory_tracker();

  // Write first tile - should succeed
  {
    auto tile = WriterTileTuple(
        *array_schema,
        10,
        false,
        false,
        sizeof(int32_t),
        Datatype::INT32,
        memory_tracker);

    std::vector<int32_t> data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    tile.fixed_tile().write(data.data(), 0, data.size() * sizeof(int32_t));
    tile.set_final_size(10);

    TileMetadataGenerator md_gen(
        Datatype::INT32, false, false, sizeof(int32_t), 1);
    md_gen.process_full_tile(tile);
    md_gen.set_tile_metadata(tile);

    filter_tile_for_test("a", tile, *array_schema, get_resources());
    writer.write_tile(tile);
  }

  // Write second tile - should fail (tile count limit reached)
  {
    auto tile = WriterTileTuple(
        *array_schema,
        10,
        false,
        false,
        sizeof(int32_t),
        Datatype::INT32,
        memory_tracker);

    std::vector<int32_t> data = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    tile.fixed_tile().write(data.data(), 0, data.size() * sizeof(int32_t));
    tile.set_final_size(10);

    TileMetadataGenerator md_gen(
        Datatype::INT32, false, false, sizeof(int32_t), 1);
    md_gen.process_full_tile(tile);
    md_gen.set_tile_metadata(tile);

    filter_tile_for_test("a", tile, *array_schema, get_resources());
    REQUIRE_THROWS_WITH(
        writer.write_tile(tile),
        Catch::Matchers::ContainsSubstring("tile count limit"));
  }
}
