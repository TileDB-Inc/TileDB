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
struct QueryShapeTest {
  QueryShapeTest(
      tiledb_array_type_t array_type,
      const std::vector<std::string>& dim_names,
      const std::vector<tiledb_datatype_t>& dim_types,
      const std::vector<void*>& dim_domains,
      const std::vector<void*>& tile_extents) {
    auto rc = tiledb_ctx_alloc(nullptr, &ctx_);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_alloc(ctx_, nullptr, &vfs_);
    REQUIRE(rc == TILEDB_OK);
    tiledb::test::create_dir(local.temp_dir(), ctx_, vfs_);
    array_uri_ = local.file_prefix() + local.temp_dir() + "shape_api";

    tiledb::test::create_array(
        ctx_,
        array_uri_,
        array_type,
        dim_names,
        dim_types,
        dim_domains,
        tile_extents,
        {"a1"},
        {TILEDB_INT32},
        {1},
        {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        1);
  }

  tiledb::test::SupportedFsLocal local;
  std::string array_uri_;
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
};

TEST_CASE(
    "Set shape API validation sparse arrays", "[query][set_shape][sparse]") {
  int domain[] = {1, 100};
  int tile_extent = 10;
  QueryShapeTest test(
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_INT32, TILEDB_INT32},
      {domain, domain},
      {&tile_extent, &tile_extent});

  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(test.ctx_, test.array_uri_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_array_open(test.ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_alloc(test.ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  int32_t min(1), max(2);

  std::vector<int32_t> data = {1, 2, 3, 4};
  uint64_t data_size = data.size() * sizeof(int32_t);
  rc = tiledb_query_set_data_buffer(
      test.ctx_, query, "d1", data.data(), &data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      test.ctx_, query, "d2", data.data(), &data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      test.ctx_, query, "a1", data.data(), &data_size);
  REQUIRE(rc == TILEDB_OK);

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
  SECTION("Setting shape on read query fails") {
    // Attempt to set a shape on a read query.
    tiledb_query_free(&query);
    rc = tiledb_array_close(test.ctx_, array);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(test.ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_alloc(test.ctx_, array, TILEDB_READ, &query);
    REQUIRE(rc == TILEDB_OK);
    CHECK_THROWS_WITH(
        query->query_->set_shape(0, &min, &max),
        Catch::Matchers::ContainsSubstring(
            "Cannot set shape on a READ query."));
  }
  SECTION("Setting shape on one dimension fails") {
    // Shape must be set on all dimensions.
    query->query_->set_shape(0, &min, &max);
    CHECK(tiledb_query_submit(test.ctx_, query) == TILEDB_ERR);
    CHECK_THAT(
        test.ctx_->last_error().value(),
        Catch::Matchers::ContainsSubstring(
            "Shape data must be set on all dimensions."));
  }
}

TEST_CASE(
    "Set shape API validation dense arrays", "[query][set_shape][dense]") {
  int64_t domain[] = {1, 100};
  int64_t tile_extent = 10;
  QueryShapeTest test(
      TILEDB_DENSE, {"d1"}, {TILEDB_INT64}, {domain}, {&tile_extent});

  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(test.ctx_, test.array_uri_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_array_open(test.ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_alloc(test.ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  int64_t min(1), max(2);

  SECTION("Setting shape on dense query fails") {
    CHECK_THROWS_WITH(
        query->query_->set_shape(0, &min, &max),
        Catch::Matchers::ContainsSubstring(
            "Cannot set shape on a dense array."));
  }
}

TEST_CASE("Test set shape API errors on invalid type", "[query][set_shape]") {
  int d1_domain[] = {1, 100};
  int32_t tile_extent = 10;
  QueryShapeTest test(
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_INT32, TILEDB_STRING_ASCII},
      {d1_domain, nullptr},
      {&tile_extent, nullptr});

  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(test.ctx_, test.array_uri_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_array_open(test.ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_alloc(test.ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  uint64_t min(1), max(2);

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

TEST_CASE(
    "Test set shape API written fragments",
    "[query][set_shape][fragment-metadata]") {
  int d1_domain[] = {1, 100};
  float d2_domain[] = {1.0f, 100.0f};
  int32_t tile_extent = 10;
  QueryShapeTest test(
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_INT32, TILEDB_FLOAT32},
      {d1_domain, d2_domain},
      {&tile_extent, &tile_extent});

  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(test.ctx_, test.array_uri_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_array_open(test.ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_alloc(test.ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  uint64_t min(1), max(2);
  float minf(1), maxf(2);

  size_t frag_count = GENERATE(1, 2, 10, 25);
  std::vector<tiledb::sm::WrittenFragmentInfo> written_frags;
  written_frags.reserve(frag_count);
  for (size_t i = 0; i < frag_count; i++) {
    query->query_->set_shape(0, &min, &max);
    query->query_->set_shape(1, &minf, &maxf);

    int mod = i * 4;
    std::vector<int32_t> d1 = {1 + mod, 2 + mod, 3 + mod, 4 + mod};
    uint64_t d1_size = d1.size() * sizeof(int32_t);
    std::vector<float> d2 = {1.0f + mod, 2.0f + mod, 3.0f + mod, 4.0f + mod};
    uint64_t d2_size = d2.size() * sizeof(float);
    std::vector<int32_t> a1 = {1 + mod, 2 + mod, 3 + mod, 4 + mod};
    uint64_t a1_size = a1.size() * sizeof(int32_t);
    rc = tiledb_query_set_data_buffer(
        test.ctx_, query, "d1", d1.data(), &d1_size);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        test.ctx_, query, "d2", d2.data(), &d2_size);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        test.ctx_, query, "a1", a1.data(), &a1_size);
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_query_submit(test.ctx_, query);
    REQUIRE(rc == TILEDB_OK);

    written_frags.push_back(query->query_->get_written_fragment_info().back());
    if (i < frag_count - 1) {
      // Reset the query for the next fragment.
      tiledb_query_free(&query);
      rc = tiledb_query_alloc(test.ctx_, array, TILEDB_WRITE, &query);
      REQUIRE(rc == TILEDB_OK);
      rc = tiledb_query_alloc(test.ctx_, array, TILEDB_WRITE, &query);
      REQUIRE(rc == TILEDB_OK);
    }
  }

  // Validate the written fragments contain shape data.
  auto dim_num = array->array_->array_schema_latest().domain().dim_num();
  DYNAMIC_SECTION(
      "Testing with " << written_frags.size() << " written fragments") {
    for (const auto& written_frag : written_frags) {
      std::unordered_map<std::string, std::pair<tiledb::sm::Tile*, uint64_t>>
          offsets;
      auto loaded_fragments = tiledb::sm::FragmentMetadata::load(
          test.ctx_->resources(),
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
    // Open the array for reads to test loading shape data.
    rc = tiledb_array_close(test.ctx_, array);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(test.ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_OK);

    // Check we can read the shape data from an array opened for reads.
    auto shapes = array->array_->shape_data();
    for (size_t i = 0; i < dim_num; i++) {
      auto query_shape = query->query_->get_shape(i);
      CHECK(shapes[i] == query_shape);
    }
  }
}
