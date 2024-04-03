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
#include "tiledb/type/range/range.h"

// TODO: Convert internal calls to Query::set_shape to tiledb_query_set_shape.
struct QueryShapeTest {
  using dimension_size_type = tiledb::sm::Domain::dimension_size_type;

  QueryShapeTest(
      tiledb_array_type_t array_type,
      const std::vector<std::string>& dim_names,
      const std::vector<tiledb_datatype_t>& dim_types,
      const std::vector<void*>& dim_domains,
      const std::vector<void*>& tile_extents)
      : ctx_(nullptr)
      , vfs_(nullptr) {
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

  /**
   * Checks that the shape data set on the query matches the shape data in the
   * array and its written fragments. The array will be reopened in read mode to
   * ensure the data is current.
   *
   * The query should have already been sumbitted with a status of
   * TILEDB_COMPLETED.
   *
   * @param query The query used to write shape data to the array.
   * @param array The array to validate the shape data.
   */
  void validate_shape_data(tiledb_query_t* query, tiledb_array_t* array) const {
    tiledb_query_status_t status;
    auto rc = tiledb_query_get_status(ctx_, query, &status);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(status == TILEDB_COMPLETED);

    int32_t is_open = false;
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    if (is_open) {
      rc = tiledb_array_close(ctx_, array);
      REQUIRE(rc == TILEDB_OK);
    }
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_OK);

    // Validate the written fragments contain shape data.
    auto array_frags = array->array_->fragment_metadata();
    auto dim_num = array->array_->array_schema_latest().domain().dim_num();
    for (size_t i = 0; i < array_frags.size(); i++) {
      auto frag = array_frags[i];
      auto frag_shape_data = frag->shape_data();
      REQUIRE(frag_shape_data.size() == dim_num);
      for (size_t j = 0; j < dim_num; j++) {
        auto shape_data_set = frag_shapes_[i][j];
        CHECK(shape_data_set == frag_shape_data[j]);
      }
    }

    // Check we can read the shape data from an array opened for reads.
    auto array_shapes = array->array_->shape_data();
    CHECK(array_shapes.size() == dim_num);
    CHECK(array_shapes.size() == array_shapes_.size());
    CHECK(array_shapes == array_shapes_);
  }

  /**
   * Sets the shape data on the query for the given dimension index.
   * The shape data is stored in array_shapes_ member variable for validating
   * the written fragments and array shape data.
   *
   * @param query The query to set the shape data.
   * @param idx The dimension index to set the shape data on.
   * @param start The start of the shape data range.
   * @param end The end of the shape data range.
   */
  void set_shape(
      tiledb_query_t* query,
      dimension_size_type idx,
      const void* start,
      const void* end) {
    CHECK_NOTHROW(query->query_->set_shape(idx, start, end));
    auto dim = query->query_->array_schema().dimension_ptr(idx);
    if (array_shapes_.size() < idx + 1) {
      array_shapes_.emplace_back(start, end, dim->coord_size());
    } else {
      const auto& set_range = array_shapes_[idx];
      auto new_range = tiledb::type::Range(start, end, dim->coord_size());
      dim->expand_range(set_range, &new_range);
      array_shapes_[idx] = new_range;
    }
  }

  /**
   * Writes the fragment using the query and buffers provided.
   * The shape data set on the query for this fragment is stored in
   * frag_shapes_.
   *
   * @param query The query to write the fragment.
   * @param buffers The data buffers to write.
   */
  void write_fragment(
      tiledb_query_t* query, tiledb::test::QueryBuffers& buffers) {
    for (auto& buffer : buffers) {
      auto rc = tiledb_query_set_data_buffer(
          ctx_,
          query,
          buffer.first.c_str(),
          buffer.second.fixed_,
          &buffer.second.fixed_size_);
      REQUIRE(rc == TILEDB_OK);
    }
    auto rc = tiledb_query_submit(ctx_, query);
    REQUIRE(rc == TILEDB_OK);

    // For global order finalize the query.
    if (query->query_->layout() == tiledb::sm::Layout::GLOBAL_ORDER) {
      rc = tiledb_query_finalize(ctx_, query);
      REQUIRE(rc == TILEDB_OK);
    }

    // Store the shape data set on the query for this fragment.
    std::vector<Range> shapes;
    for (size_t i = 0; i < query->query_->array_schema().dim_num(); i++) {
      shapes.emplace_back(query->query_->get_shape(i));
    }
    // We will use these shapes to validate each fragment in the array.
    frag_shapes_.push_back(shapes);
  }

  /**
   * Consolidates the array and vacuums to remove the consolidated fragments.
   *
   * @param array The array to consolidate and vacuum.
   */
  void consolidate_and_vacuum(tiledb_array_t* array) {
    auto rc = tiledb_array_close(ctx_, array);
    REQUIRE(rc == TILEDB_OK);
    auto frag_count = tiledb::test::num_fragments(array_uri_);
    rc = tiledb_array_consolidate(ctx_, array_uri_.c_str(), nullptr);
    REQUIRE(rc == TILEDB_OK);
    // Account for the consolidated fragment created above.
    CHECK(tiledb::test::num_fragments(array_uri_) == (int32_t)frag_count + 1);

    // Vacuum to remove the consolidated fragments.
    rc = tiledb_array_vacuum(ctx_, array_uri_.c_str(), nullptr);
    REQUIRE(rc == TILEDB_OK);
    CHECK(tiledb::test::num_fragments(array_uri_) == 1);

    // After vacuuming, the fragment shape should match the loaded array shape.
    frag_shapes_.clear();
    frag_shapes_.emplace_back(array_shapes_);
  }

  /// Shape data set on the query for each written fragment.
  std::vector<tiledb::sm::NDRange> frag_shapes_;

  /// Shape data set on the array, expanded across fragments.
  tiledb::sm::NDRange array_shapes_;

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

  tiledb_layout_t layout = GENERATE(TILEDB_GLOBAL_ORDER, TILEDB_UNORDERED);
  DYNAMIC_SECTION(
      tiledb::sm::layout_str((tiledb::sm::Layout)layout) << " layout") {
    size_t frag_count = GENERATE(1, 2, 10, 25);
    for (size_t i = 0; i < frag_count; i++) {
      rc = tiledb_query_set_layout(test.ctx_, query, layout);
      REQUIRE(rc == TILEDB_OK);
      test.set_shape(query, 0, &min, &max);
      test.set_shape(query, 1, &minf, &maxf);

      tiledb::test::QueryBuffers buffers;
      int mod = i * 4;
      std::vector<int32_t> d1 = {1 + mod, 2 + mod, 3 + mod, 4 + mod};
      uint64_t d1_size = d1.size() * sizeof(int32_t);
      buffers["d1"] = tiledb::test::QueryBuffer(d1.data(), d1_size);
      std::vector<float> d2 = {1.0f + mod, 2.0f + mod, 3.0f + mod, 4.0f + mod};
      uint64_t d2_size = d2.size() * sizeof(float);
      buffers["d2"] = tiledb::test::QueryBuffer(d2.data(), d2_size);
      std::vector<int32_t> a1 = {1 + mod, 2 + mod, 3 + mod, 4 + mod};
      uint64_t a1_size = a1.size() * sizeof(int32_t);
      buffers["a1"] = tiledb::test::QueryBuffer(a1.data(), a1_size);
      test.write_fragment(query, buffers);

      if (i < frag_count - 1) {
        // Reset the query to prepare to write the next fragment.
        tiledb_query_free(&query);
        rc = tiledb_query_alloc(test.ctx_, array, TILEDB_WRITE, &query);
        REQUIRE(rc == TILEDB_OK);
      }
    }

    // Validate the written fragments contain shape data.
    DYNAMIC_SECTION(frag_count << " written fragments.") {
      test.validate_shape_data(query, array);
    }

    // If we wrote more than one fragment test with consolidation and vacuum.
    if (frag_count > 1) {
      DYNAMIC_SECTION(
          frag_count << " written fragments consolidate and vacuum.") {
        test.consolidate_and_vacuum(array);
        test.validate_shape_data(query, array);
      }
    }
  }
}

TEST_CASE(
    "Test shape API array get shape", "[array][get_shape][fragment-metadata]") {
  int d1_domain[] = {1, 100};
  float d2_domain[] = {1.0f, 100.0f};
  int32_t tile_extent = 10;
  QueryShapeTest test(
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_INT32, TILEDB_INT32},
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

  uint64_t d1_min(1), d1_max(4);
  uint64_t d2_min(1), d2_max(4);

  tiledb_layout_t layout = GENERATE(TILEDB_GLOBAL_ORDER, TILEDB_UNORDERED);
  DYNAMIC_SECTION(
      tiledb::sm::layout_str((tiledb::sm::Layout)layout) << " layout") {
    size_t frag_count = GENERATE(1, 2, 10, 25);
    for (size_t i = 0; i < frag_count; i++) {
      rc = tiledb_query_set_layout(test.ctx_, query, layout);
      REQUIRE(rc == TILEDB_OK);
      int mod = i * 4;

      // Increase the shape data for each fragment.
      // [1, 4], [5, 8], [9, 12], ...
      d1_min += mod;
      d1_max += mod;
      test.set_shape(query, 0, &d1_min, &d1_max);

      // The shape for d2 will be set with a depth of 4 and stride of 10.
      // The union of these ranges will not be contiguous.
      // [1, 4], [11, 14], [21, 24], ...
      d2_min += i * 10;
      d2_max += i * 10;
      test.set_shape(query, 1, &d2_min, &d2_max);

      // Prepare buffers.
      tiledb::test::QueryBuffers buffers;
      std::vector<int32_t> d1 = {1 + mod, 2 + mod, 3 + mod, 4 + mod};
      uint64_t d1_size = d1.size() * sizeof(int32_t);
      buffers["d1"] = tiledb::test::QueryBuffer(d1.data(), d1_size);
      std::vector<float> d2 = {1.0f + mod, 2.0f + mod, 3.0f + mod, 4.0f + mod};
      uint64_t d2_size = d2.size() * sizeof(float);
      buffers["d2"] = tiledb::test::QueryBuffer(d2.data(), d2_size);
      std::vector<int32_t> a1 = {1 + mod, 2 + mod, 3 + mod, 4 + mod};
      uint64_t a1_size = a1.size() * sizeof(int32_t);
      buffers["a1"] = tiledb::test::QueryBuffer(a1.data(), a1_size);

      // Write the fragment.
      test.write_fragment(query, buffers);

      if (i < frag_count - 1) {
        // Reset the query to prepare to write the next fragment.
        tiledb_query_free(&query);
        rc = tiledb_query_alloc(test.ctx_, array, TILEDB_WRITE, &query);
        REQUIRE(rc == TILEDB_OK);
      }
    }

    // Validate the written fragments contain shape data.
    DYNAMIC_SECTION(frag_count << " written fragments.") {
      test.validate_shape_data(query, array);
    }

    // If we wrote more than one fragment test with consolidation and vacuum.
    if (frag_count > 1) {
      DYNAMIC_SECTION(
          frag_count << " written fragments consolidate and vacuum.") {
        test.consolidate_and_vacuum(array);
        test.validate_shape_data(query, array);
      }
    }
  }
}
