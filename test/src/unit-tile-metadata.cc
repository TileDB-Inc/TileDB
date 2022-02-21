/**
 * @file   unit-tile-metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * Tests the min/max/sum/null count values written to disk by using the
 * load_tile_*_values and get_tile_* apis of fragment metadata.
 */

#include <catch.hpp>

#include "catch.hpp"
#include "helpers.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/tile/tile_metadata_generator.h"

using namespace tiledb;

template <typename TestType>
struct CPPFixedTileMetadataFx {
  CPPFixedTileMetadataFx()
      : vfs_(ctx_) {
    if (vfs_.is_dir(ARRAY_NAME))
      vfs_.remove_dir(ARRAY_NAME);
  }

  ~CPPFixedTileMetadataFx() {
    if (vfs_.is_dir(ARRAY_NAME))
      vfs_.remove_dir(ARRAY_NAME);
  }

  void create_array(
      tiledb_layout_t layout, bool nullable, uint64_t cell_val_num) {
    Domain domain(ctx_);
    auto d = Dimension::create<uint32_t>(ctx_, "d", {{0, 999}}, tile_extent_);
    domain.add_dimension(d);

    auto a = Attribute::create<TestType>(ctx_, "a");
    a.set_nullable(nullable);
    a.set_cell_val_num(cell_val_num);

    ArraySchema schema(
        ctx_, layout == TILEDB_ROW_MAJOR ? TILEDB_DENSE : TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.add_attribute(a);

    if (layout != TILEDB_ROW_MAJOR) {
      schema.set_capacity(tile_extent_);
    }

    Array::create(ARRAY_NAME, schema);
  }

  void write_fragment(
      uint64_t f,
      tiledb_layout_t layout,
      bool nullable,
      bool all_null,
      uint64_t cell_val_num) {
    std::default_random_engine random_engine;

    // Generate random, sorted strings for the string ascii type.
    if (f == 0) {
      string_ascii_.clear();
      string_ascii_.reserve(256);
      if constexpr (std::is_same<TestType, char>::value) {
        for (uint64_t i = 0; i < 256; i++) {
          string_ascii_.emplace_back(tiledb::test::random_string(10));
        }
        std::sort(string_ascii_.begin(), string_ascii_.end());
      }
    }

    // Ensure correct vectors are big enough for this fragment.
    correct_mins_.resize(f + 1);
    correct_maxs_.resize(f + 1);
    correct_sums_int_.resize(f + 1);
    correct_sums_double_.resize(f + 1);
    correct_null_counts_.resize(f + 1);

    // Compute correct values as the tile is filled with data.
    correct_mins_[f].clear();
    correct_maxs_[f].clear();
    correct_sums_int_[f].clear();
    correct_sums_double_[f].clear();
    correct_null_counts_[f].clear();
    correct_mins_[f].resize(num_tiles_, std::numeric_limits<TestType>::max());
    correct_maxs_[f].resize(
        num_tiles_, std::numeric_limits<TestType>::lowest());
    correct_sums_int_[f].resize(num_tiles_, 0);
    correct_sums_double_[f].resize(num_tiles_, 0);
    correct_null_counts_[f].resize(num_tiles_, 0);

    // Fill the tiles with data.
    std::vector<uint32_t> d(num_cells_);
    std::vector<TestType> a(num_cells_ * cell_val_num);
    std::vector<uint8_t> a_val(num_cells_);
    for (uint64_t i = 0; i < num_cells_; i++) {
      auto tile_idx = i / tile_extent_;

      // Generate a random value depending on the data type, also compute
      // correct sums as we go.
      TestType val;
      a_val[i] =
          all_null ? 0 : (nullable && (i % tile_extent_ != 0) ? rand() % 2 : 1);
      if constexpr (std::is_integral_v<TestType>) {
        if constexpr (std::is_signed_v<TestType>) {
          if constexpr (std::is_same<TestType, int64_t>::value) {
            std::uniform_int_distribution<int64_t> dist(
                std::numeric_limits<int32_t>::lowest(),
                std::numeric_limits<int32_t>::max());
            val = dist(random_engine);
          } else {
            std::uniform_int_distribution<int64_t> dist(
                std::numeric_limits<TestType>::lowest(),
                std::numeric_limits<TestType>::max());
            val = (TestType)dist(random_engine);
          }
        } else {
          if constexpr (std::is_same<TestType, uint64_t>::value) {
            std::uniform_int_distribution<uint64_t> dist(
                std::numeric_limits<uint32_t>::lowest(),
                std::numeric_limits<uint32_t>::max());
            val = dist(random_engine);
          } else {
            std::uniform_int_distribution<uint64_t> dist(
                std::numeric_limits<TestType>::lowest(),
                std::numeric_limits<TestType>::max());
            val = (TestType)dist(random_engine);
          }
        }

        if constexpr (!std::is_same<TestType, char>::value) {
          if (a_val[i] == 1) {
            correct_sums_int_[f][tile_idx] += (int64_t)val;
          }
        }
      } else {
        std::uniform_real_distribution<TestType> dist(-10000, 10000);
        val = dist(random_engine);
        if (a_val[i] == 1) {
          correct_sums_double_[f][tile_idx] += (double)val;
        }
      }

      // Compute correct min/max.
      if (a_val[i] == 1) {
        correct_mins_[f][tile_idx] = std::min(correct_mins_[f][tile_idx], val);
        correct_maxs_[f][tile_idx] = std::max(correct_maxs_[f][tile_idx], val);
      }

      // Compute correct null count.
      correct_null_counts_[f][tile_idx] += uint64_t(a_val[i] == 0);

      // Set the value.
      if constexpr (std::is_same<TestType, char>::value) {
        // For strings, the index is stored in a signed value, switch to
        // the index to unsigned.
        int64_t idx = (int64_t)val - (int64_t)std::numeric_limits<char>::min();

        // Copy the string.
        memcpy(&a[i * cell_val_num], string_ascii_[idx].c_str(), cell_val_num);
      } else {
        a[i] = val;
      }

      // Set the coordinate value.
      d[i] = i;
    }

    // Write to the array.
    auto array = tiledb::Array(ctx_, ARRAY_NAME, TILEDB_WRITE);
    auto query = tiledb::Query(ctx_, array, TILEDB_WRITE);

    query.set_layout(layout);

    if (layout != TILEDB_ROW_MAJOR) {
      query.set_data_buffer("d", d);
    }
    query.set_data_buffer("a", a);

    if (nullable) {
      query.set_validity_buffer("a", a_val);
    }

    query.submit();
    query.finalize();
    array.close();
  }

  void check_metadata(
      uint64_t f,
      tiledb_layout_t layout,
      bool nullable,
      bool all_null,
      uint64_t cell_val_num) {
    // Open array.
    tiledb_ctx_t* ctx;
    tiledb_ctx_alloc(NULL, &ctx);
    tiledb_array_t* array;
    int rc = tiledb_array_alloc(ctx, ARRAY_NAME, &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Load the metadata and validate coords netadata.
    auto frag_meta = array->array_->fragment_metadata();
    auto& enc_key = array->array_->get_encryption_key();
    bool has_coords = layout != TILEDB_ROW_MAJOR;
    if (has_coords) {
      std::vector<std::string> names_min{"d"};
      auto st =
          frag_meta[f]->load_tile_min_values(enc_key, std::move(names_min));
      CHECK(st.ok());

      std::vector<std::string> names_max{"d"};
      st = frag_meta[f]->load_tile_max_values(enc_key, std::move(names_max));
      CHECK(st.ok());

      std::vector<std::string> names_sum{"d"};
      st = frag_meta[f]->load_tile_sum_values(enc_key, std::move(names_sum));
      CHECK(st.ok());

      std::vector<std::string> names_null_count{"d"};
      st = frag_meta[f]->load_tile_null_count_values(
          enc_key, std::move(names_null_count));
      CHECK(st.ok());

      // Validation.
      // Min/max/sum for all null tile are invalid.
      if (!all_null) {
        for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
          uint32_t correct_min = tile_idx * tile_extent_;
          uint32_t correct_max = tile_idx * tile_extent_ + tile_extent_ - 1;
          int64_t correct_sum =
              (tile_extent_) * (correct_min + correct_max) / 2;

          // Validate no min.
          auto&& [st_min, min, min_size] =
              frag_meta[f]->get_tile_min("d", tile_idx);
          CHECK(!st_min.ok());
          CHECK(!min.has_value());
          CHECK(!min_size.has_value());

          // Validate no max.
          auto&& [st_max, max, max_size] =
              frag_meta[f]->get_tile_max("d", tile_idx);
          CHECK(!st_max.ok());
          CHECK(!max.has_value());
          CHECK(!max_size.has_value());

          // Validate sum.
          auto&& [st_sum, sum] = frag_meta[f]->get_tile_sum("d", tile_idx);
          CHECK(st_sum.ok());
          if (st_sum.ok()) {
            CHECK(*(int64_t*)*sum == correct_sum);
          }
        }
      }
    }

    // Load attribute metadata.
    std::vector<std::string> names_min{"a"};
    auto st = frag_meta[f]->load_tile_min_values(enc_key, std::move(names_min));
    CHECK(st.ok());

    std::vector<std::string> names_max{"a"};
    st = frag_meta[f]->load_tile_max_values(enc_key, std::move(names_max));
    CHECK(st.ok());

    std::vector<std::string> names_sum{"a"};
    st = frag_meta[f]->load_tile_sum_values(enc_key, std::move(names_sum));
    CHECK(st.ok());

    std::vector<std::string> names_null_count{"a"};
    st = frag_meta[f]->load_tile_null_count_values(
        enc_key, std::move(names_null_count));
    CHECK(st.ok());

    // Validate attribute metadta.
    // Min/max/sum for all null tile are invalid.
    if (!all_null) {
      if constexpr (std::is_same<TestType, char>::value) {
        for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
          // Validate min.
          auto&& [st_min, min, min_size] =
              frag_meta[f]->get_tile_min("a", tile_idx);
          CHECK(st_min.ok());
          if (st_min.ok()) {
            CHECK(*min_size == cell_val_num);

            // For strings, the index is stored in a signed value, switch to
            // the index to unsigned.
            int64_t idx = (int64_t)correct_mins_[f][tile_idx] -
                          (int64_t)std::numeric_limits<char>::min();
            CHECK(
                0 == strncmp(
                         (const char*)*min,
                         string_ascii_[idx].c_str(),
                         cell_val_num));
          }

          // Validate max.
          auto&& [st_max, max, max_size] =
              frag_meta[f]->get_tile_max("a", tile_idx);
          CHECK(st_max.ok());
          if (st_max.ok()) {
            CHECK(*max_size == cell_val_num);

            // For strings, the index is stored in a signed value, switch to
            // the index to unsigned.
            int64_t idx = (int64_t)correct_maxs_[f][tile_idx] -
                          (int64_t)std::numeric_limits<char>::min();
            CHECK(
                0 == strncmp(
                         (const char*)*max,
                         string_ascii_[idx].c_str(),
                         cell_val_num));
          }

          // Validate no sum.
          auto&& [st_sum, sum] = frag_meta[f]->get_tile_sum("a", tile_idx);
          CHECK(!st_sum.ok());
        }
      } else {
        (void)cell_val_num;
        for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
          // Validate min.
          auto&& [st_min, min, min_size] =
              frag_meta[f]->get_tile_min("a", tile_idx);
          CHECK(st_min.ok());
          if (st_min.ok()) {
            CHECK(*min_size == sizeof(TestType));
            CHECK(0 == memcmp(*min, &correct_mins_[f][tile_idx], *min_size));
          }

          // Validate max.
          auto&& [st_max, max, max_size] =
              frag_meta[f]->get_tile_max("a", tile_idx);
          CHECK(st_max.ok());
          if (st_max.ok()) {
            CHECK(*max_size == sizeof(TestType));
            CHECK(0 == memcmp(*max, &correct_maxs_[f][tile_idx], *max_size));
          }

          // Validate sum.
          auto&& [st_sum, sum] = frag_meta[f]->get_tile_sum("a", tile_idx);
          CHECK(st_sum.ok());
          if (st_sum.ok()) {
            if constexpr (std::is_integral_v<TestType>) {
              CHECK(*(int64_t*)*sum == correct_sums_int_[f][tile_idx]);
            } else {
              CHECK(*(double*)*sum == correct_sums_double_[f][tile_idx]);
            }
          }
        }
      }
    }

    // Check null count.
    for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
      // Null count.
      auto&& [st_nc, nc] = frag_meta[f]->get_tile_null_count("a", tile_idx);
      CHECK(st_nc.ok() == nullable);
      if (st_nc.ok()) {
        CHECK(*nc == correct_null_counts_[f][tile_idx]);
      }
    }

    // Close array.
    rc = tiledb_array_close(ctx, array);
    CHECK(rc == TILEDB_OK);

    // Clean up.
    tiledb_array_free(&array);
    tiledb_ctx_free(&ctx);
  }

  std::vector<std::vector<TestType>> correct_mins_;
  std::vector<std::vector<TestType>> correct_maxs_;
  std::vector<std::vector<int64_t>> correct_sums_int_;
  std::vector<std::vector<double>> correct_sums_double_;
  std::vector<std::vector<uint64_t>> correct_null_counts_;
  std::vector<std::string> string_ascii_;

  const char* ARRAY_NAME = "tile_metadata_unit_array";
  const uint64_t tile_extent_ = 100;
  const uint64_t num_cells_ = 1000;
  const uint64_t num_tiles_ = num_cells_ / tile_extent_;
  Context ctx_;
  VFS vfs_;
};

typedef tuple<
    char,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    float,
    double>
    FixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE_METHOD(
    CPPFixedTileMetadataFx,
    "TileMetadata: fixed data type tile",
    "[tile-metadata][fixed-data]",
    FixedTypesUnderTest) {
  typedef TestType T;
  std::string test = GENERATE("nullable", "all null", "non nullable");

  tiledb_layout_t layout =
      GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER, TILEDB_ROW_MAJOR);

  uint64_t num_frag = GENERATE(1, 2);

  bool nullable = test == "nullable" || test == "all null";
  bool all_null = test == "all null";

  uint64_t cell_val_num = std::is_same<T, char>::value ? 10 : 1;

  // Create the array.
  CPPFixedTileMetadataFx<T>::create_array(layout, nullable, cell_val_num);

  for (uint64_t f = 0; f < num_frag; f++) {
    // Write a fragment.
    CPPFixedTileMetadataFx<T>::write_fragment(
        f, layout, nullable, all_null, cell_val_num);
  }

  for (uint64_t f = 0; f < num_frag; f++) {
    // Check metadata.
    CPPFixedTileMetadataFx<T>::check_metadata(
        f, layout, nullable, all_null, cell_val_num);
  }
}

struct CPPVarTileMetadataFx {
  CPPVarTileMetadataFx()
      : vfs_(ctx_) {
    if (vfs_.is_dir(ARRAY_NAME))
      vfs_.remove_dir(ARRAY_NAME);
  }

  ~CPPVarTileMetadataFx() {
    if (vfs_.is_dir(ARRAY_NAME))
      vfs_.remove_dir(ARRAY_NAME);
  }

  void create_array(tiledb_layout_t layout, bool nullable) {
    Domain domain(ctx_);
    auto d = Dimension::create<uint32_t>(ctx_, "d", {{0, 999}}, tile_extent_);
    domain.add_dimension(d);

    auto a = Attribute::create<std::string>(ctx_, "a");
    a.set_nullable(nullable);
    a.set_cell_val_num(TILEDB_VAR_NUM);

    ArraySchema schema(
        ctx_, layout == TILEDB_ROW_MAJOR ? TILEDB_DENSE : TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.add_attribute(a);

    if (layout != TILEDB_ROW_MAJOR) {
      schema.set_capacity(tile_extent_);
    }

    Array::create(ARRAY_NAME, schema);
  }

  void write_fragment(
      uint64_t f, tiledb_layout_t layout, bool nullable, bool all_null) {
    std::default_random_engine random_engine;

    uint64_t max_string_size = 100;
    uint64_t num_strings = 2000;

    if (f == 0) {
      // Generate random, sorted strings for the string ascii type.
      strings_.reserve(num_strings);
      for (uint64_t i = 0; i < num_strings; i++) {
        strings_.emplace_back(
            tiledb::test::random_string(rand() % max_string_size));
      }
      std::sort(strings_.begin(), strings_.end());
    }

    // Choose strings randomly.
    std::vector<int> values;
    values.reserve(num_cells_);
    uint64_t var_size = 0;
    for (uint64_t i = 0; i < num_cells_; i++) {
      values.emplace_back(rand() % num_strings);
      var_size += strings_[values.back()].size();
    }

    // Ensure correct vectors are big enough for this fragment.
    correct_mins_.resize(f + 1);
    correct_maxs_.resize(f + 1);
    correct_null_counts_.resize(f + 1);

    // Compute correct values as the tile is filled with data.
    correct_mins_[f].clear();
    correct_maxs_[f].clear();
    correct_null_counts_[f].clear();
    correct_mins_[f].resize(num_tiles_, std::numeric_limits<int>::max());
    correct_maxs_[f].resize(num_tiles_, std::numeric_limits<int>::lowest());
    correct_null_counts_[f].resize(num_tiles_, 0);

    // Fill the tiles with data.
    uint64_t offset = 0;
    std::vector<uint32_t> d(num_cells_);
    std::vector<uint64_t> a_offsets(num_cells_);
    std::vector<char> a_var(var_size);
    std::vector<uint8_t> a_val(num_cells_);
    for (uint64_t i = 0; i < num_cells_; i++) {
      auto tile_idx = i / tile_extent_;

      a_val[i] =
          all_null ? 0 : (nullable && (i % tile_extent_ != 0) ? rand() % 2 : 1);

      // Compute correct min/max.
      if (a_val[i] == 1) {
        correct_mins_[f][tile_idx] =
            std::min(correct_mins_[f][tile_idx], values[i]);
        correct_maxs_[f][tile_idx] =
            std::max(correct_maxs_[f][tile_idx], values[i]);
      }

      // Compute correct null count.
      correct_null_counts_[f][tile_idx] += uint64_t(a_val[i] == 0);

      // Copy the string.
      a_offsets[i] = offset;
      auto idx = values[i];
      memcpy(&a_var[offset], strings_[idx].c_str(), strings_[idx].size());
      offset += strings_[idx].size();

      // Set the coordinate value.
      d[i] = i;
    }

    // Write to the array.
    auto array = tiledb::Array(ctx_, ARRAY_NAME, TILEDB_WRITE);
    auto query = tiledb::Query(ctx_, array, TILEDB_WRITE);

    query.set_layout(layout);

    if (layout != TILEDB_ROW_MAJOR) {
      query.set_data_buffer("d", d);
    }
    query.set_offsets_buffer("a", a_offsets);
    query.set_buffer("a", a_var);

    if (nullable) {
      query.set_validity_buffer("a", a_val);
    }

    query.submit();
    query.finalize();
    array.close();
  }

  void check_metadata(
      uint64_t f, tiledb_layout_t layout, bool nullable, bool all_null) {
    // Open array.
    tiledb_ctx_t* ctx;
    tiledb_ctx_alloc(NULL, &ctx);
    tiledb_array_t* array;
    int rc = tiledb_array_alloc(ctx, ARRAY_NAME, &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Load the metadata and validate coords netadata.
    auto frag_meta = array->array_->fragment_metadata();
    auto& enc_key = array->array_->get_encryption_key();
    bool has_coords = layout != TILEDB_ROW_MAJOR;
    if (has_coords) {
      std::vector<std::string> names_min{"d"};
      auto st =
          frag_meta[f]->load_tile_min_values(enc_key, std::move(names_min));
      CHECK(st.ok());

      std::vector<std::string> names_max{"d"};
      st = frag_meta[f]->load_tile_max_values(enc_key, std::move(names_max));
      CHECK(st.ok());

      std::vector<std::string> names_sum{"d"};
      st = frag_meta[f]->load_tile_sum_values(enc_key, std::move(names_sum));
      CHECK(st.ok());

      std::vector<std::string> names_null_count{"d"};
      st = frag_meta[f]->load_tile_null_count_values(
          enc_key, std::move(names_null_count));
      CHECK(st.ok());

      // Validation.
      // Min/max/sum for all null tile are invalid.
      if (!all_null) {
        for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
          uint32_t correct_min = tile_idx * tile_extent_;
          uint32_t correct_max = tile_idx * tile_extent_ + tile_extent_ - 1;
          int64_t correct_sum =
              (tile_extent_) * (correct_min + correct_max) / 2;

          // Validate no min.
          auto&& [st_min, min, min_size] =
              frag_meta[f]->get_tile_min("d", tile_idx);
          CHECK(!st_min.ok());
          CHECK(!min.has_value());
          CHECK(!min_size.has_value());

          // Validate no max.
          auto&& [st_max, max, max_size] =
              frag_meta[f]->get_tile_max("d", tile_idx);
          CHECK(!st_max.ok());
          CHECK(!max.has_value());
          CHECK(!max_size.has_value());

          // Validate sum.
          auto&& [st_sum, sum] = frag_meta[f]->get_tile_sum("d", tile_idx);
          CHECK(st_sum.ok());
          if (st_sum.ok()) {
            CHECK(*(int64_t*)*sum == correct_sum);
          }
        }
      }
    }

    // Load attribute metadata.
    std::vector<std::string> names_min{"a"};
    auto st = frag_meta[f]->load_tile_min_values(enc_key, std::move(names_min));
    CHECK(st.ok());

    std::vector<std::string> names_max{"a"};
    st = frag_meta[f]->load_tile_max_values(enc_key, std::move(names_max));
    CHECK(st.ok());

    std::vector<std::string> names_sum{"a"};
    st = frag_meta[f]->load_tile_sum_values(enc_key, std::move(names_sum));
    CHECK(st.ok());

    std::vector<std::string> names_null_count{"a"};
    st = frag_meta[f]->load_tile_null_count_values(
        enc_key, std::move(names_null_count));
    CHECK(st.ok());

    // Validate attribute metadata.
    // Min/max/sum for all null tile are invalid.
    if (!all_null) {
      for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
        // Validate min.
        auto&& [st_min, min, min_size] =
            frag_meta[f]->get_tile_min("a", tile_idx);
        CHECK(st_min.ok());
        if (st_min.ok()) {
          int idx = correct_mins_[f][tile_idx];
          CHECK(*min_size == strings_[idx].size());
          CHECK(
              0 == strncmp(
                       (const char*)*min,
                       strings_[idx].c_str(),
                       strings_[idx].size()));
        }

        // Validate max.
        auto&& [st_max, max, max_size] =
            frag_meta[f]->get_tile_max("a", tile_idx);
        CHECK(st_max.ok());
        if (st_max.ok()) {
          int idx = correct_maxs_[f][tile_idx];
          CHECK(*max_size == strings_[idx].size());
          CHECK(
              0 == strncmp(
                       (const char*)*max,
                       strings_[idx].c_str(),
                       strings_[idx].size()));
        }

        // Validate no sum.
        auto&& [st_sum, sum] = frag_meta[f]->get_tile_sum("a", tile_idx);
        CHECK(!st_sum.ok());
      }
    }

    // Check null count.
    for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
      // Null count.
      auto&& [st_nc, nc] = frag_meta[f]->get_tile_null_count("a", tile_idx);
      CHECK(st_nc.ok() == nullable);
      if (st_nc.ok()) {
        CHECK(*nc == correct_null_counts_[f][tile_idx]);
      }
    }

    // Close array.
    rc = tiledb_array_close(ctx, array);
    CHECK(rc == TILEDB_OK);

    // Clean up.
    tiledb_array_free(&array);
    tiledb_ctx_free(&ctx);
  }

  std::vector<std::vector<int>> correct_mins_;
  std::vector<std::vector<int>> correct_maxs_;
  std::vector<std::vector<uint64_t>> correct_null_counts_;
  std::vector<std::string> strings_;

  const char* ARRAY_NAME = "tile_metadata_unit_array";
  const uint64_t tile_extent_ = 10;
  const uint64_t num_cells_ = 1000;
  const uint64_t num_tiles_ = num_cells_ / tile_extent_;
  Context ctx_;
  VFS vfs_;
};

TEST_CASE_METHOD(
    CPPVarTileMetadataFx,
    "TileMetadata: var data type tile",
    "[tile-metadata][var-data]") {
  std::string test = GENERATE("nullable", "all null", "non nullable");

  tiledb_layout_t layout =
      GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER, TILEDB_ROW_MAJOR);

  uint64_t num_frag = GENERATE(1, 2);

  bool nullable = test == "nullable" || test == "all null";
  bool all_null = test == "all null";

  // Create the array.
  CPPVarTileMetadataFx::create_array(layout, nullable);

  for (uint64_t f = 0; f < num_frag; f++) {
    // Write a fragment.
    CPPVarTileMetadataFx::write_fragment(f, layout, nullable, all_null);
  }

  for (uint64_t f = 0; f < num_frag; f++) {
    // Check metadata.
    CPPVarTileMetadataFx::check_metadata(f, layout, nullable, all_null);
  }
}