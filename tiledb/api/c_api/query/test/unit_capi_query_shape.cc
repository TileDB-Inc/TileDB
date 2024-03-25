/**
 * @file tiledb/api/c_api/query/test/unit_capi_query_shape.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB Inc.
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
 * Tests the Query C API for setting shapes on dimensions.
 */

#include <test/support/src/vfs_helpers.h>
#include <test/support/tdb_catch.h>
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/query/query_api_external.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

// TODO: Convert internal calls to Query::set_shape to tiledb_query_set_shape.
TEST_CASE("Test set shape API validation", "[query][set_shape]") {
  tiledb_ctx_t* ctx;
  auto rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  tiledb::test::SupportedFsLocal local;
  tiledb::test::create_dir(local.temp_dir(), ctx, vfs);
  auto uri = local.file_prefix() + local.temp_dir() + "set_shape";
  int d1_domain[] = {1, 100};
  float d2_domain[] = {1.0f, 100.0f};
  int tile_extent = 10;
  tiledb::test::create_array(
      ctx,
      uri,
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_INT32, TILEDB_FLOAT32},
      {d1_domain, d2_domain},
      {&tile_extent, &tile_extent},
      {"a1"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      1);

  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx, uri.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  uint64_t min(1), max(2);
  float minf(1), maxf(2);

  std::vector<int32_t> d1 = {1, 2, 3, 4};
  uint64_t d1_size = d1.size() * sizeof(int32_t);
  std::vector<float> d2 = {1.0f, 2.0f, 3.0f, 4.0f};
  uint64_t d2_size = d2.size() * sizeof(float);
  std::vector<int32_t> a1 = {1, 2, 3, 4};
  uint64_t a1_size = a1.size() * sizeof(int32_t);
  rc = tiledb_query_set_data_buffer(ctx, query, "d1", d1.data(), &d1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx, query, "d2", d2.data(), &d2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx, query, "a1", a1.data(), &a1_size);
  REQUIRE(rc == TILEDB_OK);

  bool shape_set = false;
  auto expected = TILEDB_OK;
  SECTION("Setting the shape with null ranges fails") {
    // Attempt to set a shape with null ranges.
    // The query submit will not fail, we never successfully set any shape.
    CHECK_THROWS_WITH(
        query->query_->set_shape(0, nullptr, nullptr),
        Catch::Matchers::ContainsSubstring(
            "Input range is null for dimension index 0."));
    CHECK_THROWS_WITH(
        query->query_->set_shape(0, &min, nullptr),
        Catch::Matchers::ContainsSubstring(
            "Input range is null for dimension index 0."));
    CHECK_THROWS_WITH(
        query->query_->set_shape(0, nullptr, &max),
        Catch::Matchers::ContainsSubstring(
            "Input range is null for dimension index 0."));
  }
  SECTION("Setting shape on OOB dimension fails") {
    // Attempt to set a shape on a dimension that does not exist.
    // The query submit will not fail, we never successfully set any shape.
    CHECK_THROWS_WITH(
        query->query_->set_shape(2, &min, &max),
        Catch::Matchers::ContainsSubstring(
            "Input dimension index 2 does not exist."));
  }
  SECTION("Setting shape on one dimension fails") {
    // Shape must be set on all dimensions.
    query->query_->set_shape(0, &min, &max);
    expected = TILEDB_ERR;
  }
  SECTION("Setting shape on all dimensions succeeds") {
    // Shape must be set on all dimensions.
    query->query_->set_shape(0, &min, &max);
    query->query_->set_shape(1, &minf, &maxf);
    shape_set = true;
  }

  rc = tiledb_query_submit(ctx, query);
  REQUIRE(rc == expected);
  if (expected == TILEDB_OK && shape_set) {
    // Validate the written fragments contain shape data.
    auto dim_num = array->array_->array_schema_latest().domain().dim_num();
    auto written_frags = query->query_->get_written_fragment_info();
    for (const auto& written_frag : written_frags) {
      std::unordered_map<std::string, std::pair<tiledb::sm::Tile*, uint64_t>>
          offsets;
      auto loaded_fragments = tiledb::sm::FragmentMetadata::load(
          ctx->resources(),
          tiledb::test::get_test_memory_tracker(),
          query->query_->array()->opened_array()->array_schema_latest_ptr(),
          query->query_->array()->array_schemas_all(),
          *query->query_->array()->encryption_key(),
          {tiledb::sm::TimestampedURI(
              written_frag.uri_, written_frag.timestamp_range_)},
          offsets);
      for (const auto& frag : loaded_fragments) {
        auto frag_shape_data = frag->shape_data();
        CHECK(frag_shape_data.size() == dim_num);
        for (size_t i = 0; i < dim_num; i++) {
          auto shape_data_set = query->query_->get_shape(i);
          CHECK(shape_data_set == frag_shape_data[i]);
        }
      }
    }
    rc = tiledb_array_close(ctx, array);
    REQUIRE(rc == TILEDB_OK);

    // Open the array for reads to test loading shape data.
    tiledb_array_t* array_read;
    rc = tiledb_array_alloc(ctx, uri.c_str(), &array_read);
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_array_open(ctx, array_read, TILEDB_READ);
    REQUIRE(rc == TILEDB_OK);

    // Check we can read the shape data from an array opened for reads.
    auto shapes = array_read->array_->shape_data();
    for (size_t i = 0; i < dim_num; i++) {
      auto query_shape = query->query_->get_shape(i);
      CHECK(shapes[i] == query_shape);
    }
  }
}

// TODO: Convert internal calls to Query::set_shape to tiledb_query_set_shape.
TEST_CASE("Test set shape API errors on invalid type", "[query][set_shape]") {
  tiledb_ctx_t* ctx;
  auto rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs;
  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  tiledb::test::SupportedFsLocal local;
  tiledb::test::create_dir(local.temp_dir(), ctx, vfs);
  auto uri = local.file_prefix() + local.temp_dir() + "set_shape_data";
  int d1_domain[] = {1, 100};
  int tile_extent = 10;
  tiledb::test::create_array(
      ctx,
      uri,
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_INT32, TILEDB_STRING_ASCII},
      {d1_domain, nullptr},
      {&tile_extent, nullptr},
      {"a1"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      1);

  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx, uri.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  uint64_t min(1), max(2);

  std::vector<int32_t> d1 = {1, 2, 3, 4};
  uint64_t d1_size = d1.size() * sizeof(int32_t);
  std::string d2("aabbccdd");
  uint64_t d2_size = d2.size();
  std::vector<uint64_t> d2_off = {0, 2, 4, 6};
  uint64_t d2_off_size = d2_off.size() * sizeof(uint64_t);
  std::vector<int32_t> a1 = {1, 2, 3, 4};
  uint64_t a1_size = a1.size() * sizeof(int32_t);
  rc = tiledb_query_set_data_buffer(ctx, query, "d1", d1.data(), &d1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx, query, "d2", d2.data(), &d2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "d2", d2_off.data(), &d2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx, query, "a1", a1.data(), &a1_size);
  REQUIRE(rc == TILEDB_OK);

  SECTION("Setting shape on non-arithmetic dimension fails") {
    // Attempt to set a shape on a dimension using a non-arithmetic type.
    CHECK_THROWS_WITH(
        query->query_->set_shape(1, &min, &max),
        Catch::Matchers::ContainsSubstring(
            "All dimension types for the array must be arithmetic types."));
  }
  SECTION("Setting shape on array using non-arithmetic dimensions fails") {
    // Attempt to set a shape on a dimension using an arithmetic type.
    // This should still fail, because the array contains other dimensions that
    // are using non-arithmetic types.
    CHECK_THROWS_WITH(
        query->query_->set_shape(0, &min, &max),
        Catch::Matchers::ContainsSubstring(
            "All dimension types for the array must be arithmetic types."));
  }
}

// TODO: Set shape should fail on a dense array(?)
// TODO: Set shape should fail on a read query.
