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

#include <random>

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/query/readers/aggregators/tile_metadata.h"
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
    auto tiledb_type = TILEDB_CHAR;
    if constexpr (!std::is_same<TestType, char>::value) {
      auto type = tiledb::impl::type_to_tiledb<TestType>();
      tiledb_type = type.tiledb_type;
    }

    // Create TileDB context
    tiledb_ctx_t* ctx;
    tiledb_ctx_alloc(NULL, &ctx);

    // The array will be one dimension "d", with domain [0,999].
    uint32_t dim_domain[]{0, 999};
    tiledb_dimension_t* d;
    tiledb_dimension_alloc(
        ctx, "d", TILEDB_UINT32, &dim_domain[0], &tile_extent_, &d);

    // Create domain
    tiledb_domain_t* domain;
    tiledb_domain_alloc(ctx, &domain);
    tiledb_domain_add_dimension(ctx, domain, d);

    // Create a single attribute "a" so each (i,j) cell can store an integer
    tiledb_attribute_t* a;
    tiledb_attribute_alloc(ctx, "a", tiledb_type, &a);
    tiledb_attribute_set_nullable(ctx, a, nullable);
    tiledb_attribute_set_cell_val_num(ctx, a, cell_val_num);

    // Create array schema
    tiledb_array_schema_t* array_schema;
    tiledb_array_schema_alloc(
        ctx,
        layout == TILEDB_ROW_MAJOR ? TILEDB_DENSE : TILEDB_SPARSE,
        &array_schema);
    tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
    tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
    tiledb_array_schema_set_domain(ctx, array_schema, domain);
    tiledb_array_schema_add_attribute(ctx, array_schema, a);

    if (layout != TILEDB_ROW_MAJOR) {
      tiledb_array_schema_set_capacity(ctx, array_schema, tile_extent_);
    }

    // Create array
    tiledb_array_create(ctx, ARRAY_NAME, array_schema);

    // Clean up
    tiledb_attribute_free(&a);
    tiledb_dimension_free(&d);
    tiledb_domain_free(&domain);
    tiledb_array_schema_free(&array_schema);
    tiledb_ctx_free(&ctx);
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
    correct_tile_mins_.resize(f + 1);
    correct_tile_maxs_.resize(f + 1);
    correct_tile_sums_int_.resize(f + 1);
    correct_tile_sums_double_.resize(f + 1);
    correct_tile_null_counts_.resize(f + 1);
    correct_mins_.resize(f + 1, std::numeric_limits<TestType>::max());
    correct_maxs_.resize(f + 1);
    correct_sums_double_.resize(f + 1);
    correct_sums_int_.resize(f + 1);
    correct_null_counts_.resize(f + 1);

    // Compute correct values as the tile is filled with data.
    correct_tile_mins_[f].clear();
    correct_tile_maxs_[f].clear();
    correct_tile_sums_int_[f].clear();
    correct_tile_sums_double_[f].clear();
    correct_tile_null_counts_[f].clear();
    correct_tile_mins_[f].resize(
        num_tiles_, std::numeric_limits<TestType>::max());
    correct_tile_maxs_[f].resize(
        num_tiles_, std::numeric_limits<TestType>::lowest());
    correct_tile_sums_int_[f].resize(num_tiles_, 0);
    correct_tile_sums_double_[f].resize(num_tiles_, 0);
    correct_tile_null_counts_[f].resize(num_tiles_, 0);

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
      if constexpr (std::is_same<TestType, std::byte>::value) {
        val = std::byte(0);
      } else if constexpr (std::is_integral_v<TestType>) {
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
            correct_tile_sums_int_[f][tile_idx] += (int64_t)val;
            correct_sums_int_[f] += (int64_t)val;
          }
        }
      } else {
        std::uniform_real_distribution<TestType> dist(-10000, 10000);
        val = dist(random_engine);
        if (a_val[i] == 1) {
          correct_tile_sums_double_[f][tile_idx] += (double)val;
          correct_sums_double_[f] += (double)val;
        }
      }

      // Compute correct min/max.
      if (a_val[i] == 1) {
        correct_tile_mins_[f][tile_idx] =
            std::min(correct_tile_mins_[f][tile_idx], val);
        correct_tile_maxs_[f][tile_idx] =
            std::max(correct_tile_maxs_[f][tile_idx], val);
        correct_mins_[f] = std::min(correct_mins_[f], val);
        correct_maxs_[f] = std::max(correct_maxs_[f], val);
      }

      // Compute correct null count.
      correct_tile_null_counts_[f][tile_idx] += uint64_t(a_val[i] == 0);
      correct_null_counts_[f] += uint64_t(a_val[i] == 0);

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

    // Submit query
    if (layout == TILEDB_GLOBAL_ORDER) {
      query.submit_and_finalize();
    } else {
      query.submit();
    }

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

    // Load fragment metadata.
    auto frag_meta = array->array_->fragment_metadata();
    auto& enc_key = array->array_->get_encryption_key();
    frag_meta[f]->loaded_metadata()->load_fragment_min_max_sum_null_count(
        enc_key);

    // Load the metadata and validate coords netadata.
    bool has_coords = layout != TILEDB_ROW_MAJOR;
    if (has_coords) {
      std::vector<std::string> names{"d"};
      frag_meta[f]->loaded_metadata()->load_rtree(enc_key);
      frag_meta[f]->loaded_metadata()->load_tile_min_values(enc_key, names);
      frag_meta[f]->loaded_metadata()->load_tile_max_values(enc_key, names);
      frag_meta[f]->loaded_metadata()->load_tile_sum_values(enc_key, names);
      frag_meta[f]->loaded_metadata()->load_tile_null_count_values(
          enc_key, names);

      // Validation.
      // Min/max/sum for all null tile are invalid.
      if (!all_null) {
        // Do fragment metadata first.
        {
          int64_t correct_sum =
              (num_tiles_ * tile_extent_) * (num_tiles_ * tile_extent_ - 1) / 2;

          // Validate no min.
          CHECK_THROWS_WITH(
              frag_meta[f]->loaded_metadata()->get_min("d"),
              "FragmentMetadata: Trying to access fragment min metadata that's "
              "not present");

          // Validate no max.
          CHECK_THROWS_WITH(
              frag_meta[f]->loaded_metadata()->get_max("d"),
              "FragmentMetadata: Trying to access fragment max metadata that's "
              "not present");

          // Validate sum.
          auto sum = frag_meta[f]->loaded_metadata()->get_sum("d");
          CHECK(*(int64_t*)sum == correct_sum);
        }

        for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
          uint32_t correct_min = tile_idx * tile_extent_;
          uint32_t correct_max = tile_idx * tile_extent_ + tile_extent_ - 1;
          int64_t correct_sum =
              (tile_extent_) * (correct_min + correct_max) / 2;

          // Validate no min.
          CHECK_THROWS_WITH(
              frag_meta[f]->get_tile_min_as<TestType>("d", tile_idx),
              "FragmentMetadata: Trying to access tile min metadata that's not "
              "present");

          // Validate no max.
          CHECK_THROWS_WITH(
              frag_meta[f]->get_tile_max_as<TestType>("d", tile_idx),
              "FragmentMetadata: Trying to access tile max metadata that's not "
              "present");

          // Validate sum.
          auto sum =
              frag_meta[f]->loaded_metadata()->get_tile_sum("d", tile_idx);
          CHECK(*(int64_t*)sum == correct_sum);

          // Validate the tile metadata structure.
          auto full_tile_data = frag_meta[f]->get_tile_metadata("d", tile_idx);
          CHECK(correct_min == full_tile_data.min_as<uint32_t>());
          CHECK(correct_max == full_tile_data.max_as<uint32_t>());
          CHECK(correct_sum == full_tile_data.sum_as<int64_t>());
        }
      }
    }

    // Do fragment metadata first for attribute.
    {
      // Min/max/sum for byte don't exist.
      if constexpr (std::is_same<TestType, std::byte>::value) {
        // Validate no min.
        CHECK_THROWS_WITH(
            frag_meta[f]->loaded_metadata()->get_min("a"),
            "FragmentMetadata: Trying to access fragment min metadata that's "
            "not present");

        // Validate no max.
        CHECK_THROWS_WITH(
            frag_meta[f]->loaded_metadata()->get_max("a"),
            "FragmentMetadata: Trying to access fragment max metadata that's "
            "not present");

        // Validate no sum.
        CHECK_THROWS_WITH(
            frag_meta[f]->loaded_metadata()->get_sum("a"),
            "FragmentMetadata: Trying to access fragment sum metadata that's "
            "not present");
      } else {
        // Min/max/sum for all null tile are invalid.
        if (!all_null) {
          if constexpr (std::is_same<TestType, char>::value) {
            // Validate min.
            auto& min = frag_meta[f]->loaded_metadata()->get_min("a");
            CHECK(min.size() == cell_val_num);

            // For strings, the index is stored in a signed value, switch to
            // the index to unsigned.
            int64_t idx = (int64_t)correct_mins_[f] -
                          (int64_t)std::numeric_limits<char>::min();
            CHECK(
                0 == strncmp(
                         (const char*)min.data(),
                         string_ascii_[idx].c_str(),
                         cell_val_num));

            // Validate max.
            auto& max = frag_meta[f]->loaded_metadata()->get_max("a");
            CHECK(max.size() == cell_val_num);

            // For strings, the index is stored in a signed value, switch to
            // the index to unsigned.
            idx = (int64_t)correct_maxs_[f] -
                  (int64_t)std::numeric_limits<char>::min();
            CHECK(
                0 == strncmp(
                         (const char*)max.data(),
                         string_ascii_[idx].c_str(),
                         cell_val_num));

            // Validate no sum.
            CHECK_THROWS_WITH(
                frag_meta[f]->loaded_metadata()->get_sum("a"),
                "FragmentMetadata: Trying to access fragment sum metadata "
                "that's not present");
          } else {
            // Validate min.
            auto& min = frag_meta[f]->loaded_metadata()->get_min("a");
            CHECK(min.size() == sizeof(TestType));
            CHECK(0 == memcmp(min.data(), &correct_mins_[f], min.size()));

            // Validate max.
            auto& max = frag_meta[f]->loaded_metadata()->get_max("a");
            CHECK(max.size() == sizeof(TestType));
            CHECK(0 == memcmp(max.data(), &correct_maxs_[f], max.size()));

            if constexpr (!std::is_same<TestType, unsigned char>::value) {
              // Validate sum.
              auto sum = frag_meta[f]->loaded_metadata()->get_sum("a");
              if constexpr (std::is_integral_v<TestType>) {
                CHECK(*(int64_t*)sum == correct_sums_int_[f]);
              } else {
                CHECK(*(double*)sum - correct_sums_double_[f] < 0.0001);
              }
            }
          }
        }
      }

      // Check null count.
      if (nullable) {
        auto nc = frag_meta[f]->loaded_metadata()->get_null_count("a");
        CHECK(nc == correct_null_counts_[f]);
      } else {
        CHECK_THROWS_WITH(
            frag_meta[f]->loaded_metadata()->get_null_count("a"),
            "FragmentMetadata: Trying to access fragment null count metadata "
            "that's not present");
      }
    }

    // Load attribute metadata.
    std::vector<std::string> names{"a"};
    frag_meta[f]->loaded_metadata()->load_tile_min_values(enc_key, names);
    frag_meta[f]->loaded_metadata()->load_tile_max_values(enc_key, names);
    frag_meta[f]->loaded_metadata()->load_tile_sum_values(enc_key, names);
    frag_meta[f]->loaded_metadata()->load_tile_null_count_values(
        enc_key, names);

    // Validate attribute metadta.
    // Min/max/sum for all null tile are invalid.
    if constexpr (std::is_same<TestType, std::byte>::value) {
      // Validate no min.
      CHECK_THROWS_WITH(
          frag_meta[f]->get_tile_min_as<TestType>("a", 0),
          "FragmentMetadata: Trying to access tile min metadata that's not "
          "present");

      // Validate no max.
      CHECK_THROWS_WITH(
          frag_meta[f]->get_tile_max_as<TestType>("a", 0),
          "FragmentMetadata: Trying to access tile max metadata that's not "
          "present");

      // Validate no sum.
      CHECK_THROWS_WITH(
          frag_meta[f]->loaded_metadata()->get_tile_sum("a", 0),
          "FragmentMetadata: Trying to access tile sum metadata that's not "
          "present");
    } else {
      if (!all_null) {
        if constexpr (std::is_same<TestType, char>::value) {
          for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
            // Validate min.
            const auto min =
                frag_meta[f]->get_tile_min_as<std::string_view>("a", tile_idx);
            CHECK(min.size() == cell_val_num);

            // For strings, the index is stored in a signed value, switch to
            // the index to unsigned.
            int64_t min_idx = (int64_t)correct_tile_mins_[f][tile_idx] -
                              (int64_t)std::numeric_limits<char>::min();
            CHECK(
                0 ==
                strncmp(
                    min.data(), string_ascii_[min_idx].c_str(), cell_val_num));

            // Validate max.
            const auto max =
                frag_meta[f]->get_tile_max_as<std::string_view>("a", tile_idx);
            CHECK(max.size() == cell_val_num);

            // For strings, the index is stored in a signed value, switch to
            // the index to unsigned.
            int64_t max_idx = (int64_t)correct_tile_maxs_[f][tile_idx] -
                              (int64_t)std::numeric_limits<char>::min();
            CHECK(
                0 ==
                strncmp(
                    max.data(), string_ascii_[max_idx].c_str(), cell_val_num));

            // Validate no sum.
            CHECK_THROWS_WITH(
                frag_meta[f]->loaded_metadata()->get_tile_sum("a", tile_idx),
                "FragmentMetadata: Trying to access tile sum metadata that's "
                "not "
                "present");

            // Validate the tile metadata structure.
            auto full_tile_data =
                frag_meta[f]->get_tile_metadata("a", tile_idx);
            CHECK(
                string_ascii_[min_idx] ==
                full_tile_data.min_as<std::string_view>());
            CHECK(
                string_ascii_[max_idx] ==
                full_tile_data.max_as<std::string_view>());
          }
        } else {
          (void)cell_val_num;
          for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
            // Validate min.
            const auto min =
                frag_meta[f]->get_tile_min_as<TestType>("a", tile_idx);
            CHECK(
                0 ==
                memcmp(
                    &min, &correct_tile_mins_[f][tile_idx], sizeof(TestType)));

            // Validate max.
            const auto max =
                frag_meta[f]->get_tile_max_as<TestType>("a", tile_idx);
            CHECK(
                0 ==
                memcmp(
                    &max, &correct_tile_maxs_[f][tile_idx], sizeof(TestType)));

            // Validate the tile metadata structure.
            auto full_tile_data =
                frag_meta[f]->get_tile_metadata("a", tile_idx);
            CHECK(
                correct_tile_mins_[f][tile_idx] ==
                full_tile_data.min_as<TestType>());
            CHECK(
                correct_tile_maxs_[f][tile_idx] ==
                full_tile_data.max_as<TestType>());

            if constexpr (!std::is_same<TestType, unsigned char>::value) {
              // Validate sum.
              auto sum =
                  frag_meta[f]->loaded_metadata()->get_tile_sum("a", tile_idx);
              if constexpr (std::is_integral_v<TestType>) {
                CHECK(*(int64_t*)sum == correct_tile_sums_int_[f][tile_idx]);
                CHECK(
                    correct_tile_sums_int_[f][tile_idx] ==
                    full_tile_data.sum_as<int64_t>());
              } else {
                CHECK(*(double*)sum == correct_tile_sums_double_[f][tile_idx]);
                CHECK(
                    correct_tile_sums_double_[f][tile_idx] ==
                    full_tile_data.sum_as<double>());
              }
            }
          }
        }
      }
    }

    // Check null count.
    for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
      if (nullable) {
        auto nc =
            frag_meta[f]->loaded_metadata()->get_tile_null_count("a", tile_idx);
        CHECK(nc == correct_tile_null_counts_[f][tile_idx]);
      } else {
        CHECK_THROWS_WITH(
            frag_meta[f]->loaded_metadata()->get_tile_null_count("a", tile_idx),
            "FragmentMetadata: Trying to access tile null count metadata "
            "that's not present");
      }
    }

    if constexpr (!std::is_same<TestType, std::byte>::value) {
      // Validate the full tile data structure for null count
      for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
        auto full_tile_data = frag_meta[f]->get_tile_metadata("a", tile_idx);
        if (nullable) {
          CHECK(
              full_tile_data.null_count() ==
              correct_tile_null_counts_[f][tile_idx]);
        } else {
          CHECK(full_tile_data.null_count() == 0);
        }
      }
    }

    // Close array.
    rc = tiledb_array_close(ctx, array);
    CHECK(rc == TILEDB_OK);

    // Clean up.
    tiledb_array_free(&array);
    tiledb_ctx_free(&ctx);
  }

  std::vector<std::vector<TestType>> correct_tile_mins_;
  std::vector<std::vector<TestType>> correct_tile_maxs_;
  std::vector<std::vector<int64_t>> correct_tile_sums_int_;
  std::vector<std::vector<double>> correct_tile_sums_double_;
  std::vector<std::vector<uint64_t>> correct_tile_null_counts_;
  std::vector<TestType> correct_mins_;
  std::vector<TestType> correct_maxs_;
  std::vector<int64_t> correct_sums_int_;
  std::vector<double> correct_sums_double_;
  std::vector<uint64_t> correct_null_counts_;
  std::vector<std::string> string_ascii_;

  const char* ARRAY_NAME = "tile_metadata_unit_array";
  const uint64_t tile_extent_ = 100;
  const uint64_t num_cells_ = 1000;
  const uint64_t num_tiles_ = num_cells_ / tile_extent_;
  tiledb::Context ctx_;
  tiledb::VFS vfs_;
};

typedef tuple<
    std::byte,
    unsigned char,
    char,  // Used for TILEDB_CHAR.
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
    tiledb::Domain domain(ctx_);
    auto d = tiledb::Dimension::create<uint32_t>(
        ctx_, "d", {{0, 999}}, tile_extent_);
    domain.add_dimension(d);

    auto a = tiledb::Attribute::create<std::string>(ctx_, "a");
    a.set_nullable(nullable);
    a.set_cell_val_num(TILEDB_VAR_NUM);

    tiledb::ArraySchema schema(
        ctx_, layout == TILEDB_ROW_MAJOR ? TILEDB_DENSE : TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.add_attribute(a);

    if (layout != TILEDB_ROW_MAJOR) {
      schema.set_capacity(tile_extent_);
    }

    tiledb::Array::create(ARRAY_NAME, schema);
  }

  void write_fragment(
      uint64_t f, tiledb_layout_t layout, bool nullable, bool all_null) {
    std::default_random_engine random_engine;

    uint64_t max_string_size = 100;
    int num_strings = 2000;

    if (f == 0) {
      // Generate random, sorted strings for the string ascii type.
      strings_.reserve(num_strings);
      for (int i = 0; i < num_strings; i++) {
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
    correct_tile_mins_.resize(f + 1);
    correct_tile_maxs_.resize(f + 1);
    correct_tile_null_counts_.resize(f + 1);
    correct_mins_.resize(f + 1, std::numeric_limits<int>::max());
    correct_maxs_.resize(f + 1, std::numeric_limits<int>::lowest());
    correct_null_counts_.resize(f + 1);

    // Compute correct values as the tile is filled with data.
    correct_tile_mins_[f].clear();
    correct_tile_maxs_[f].clear();
    correct_tile_null_counts_[f].clear();
    correct_tile_mins_[f].resize(num_tiles_, std::numeric_limits<int>::max());
    correct_tile_maxs_[f].resize(
        num_tiles_, std::numeric_limits<int>::lowest());
    correct_tile_null_counts_[f].resize(num_tiles_, 0);

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
        correct_tile_mins_[f][tile_idx] =
            std::min(correct_tile_mins_[f][tile_idx], values[i]);
        correct_tile_maxs_[f][tile_idx] =
            std::max(correct_tile_maxs_[f][tile_idx], values[i]);
        correct_mins_[f] = std::min(correct_mins_[f], values[i]);
        correct_maxs_[f] = std::max(correct_maxs_[f], values[i]);
      }

      // Compute correct null count.
      correct_tile_null_counts_[f][tile_idx] += uint64_t(a_val[i] == 0);
      correct_null_counts_[f] += uint64_t(a_val[i] == 0);

      // Copy the string.
      a_offsets[i] = offset;
      auto idx = values[i];
      memcpy(
          a_var.data() + offset, strings_[idx].c_str(), strings_[idx].size());
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
    query.set_data_buffer("a", a_var);

    if (nullable) {
      query.set_validity_buffer("a", a_val);
    }

    // Submit query
    if (layout == TILEDB_GLOBAL_ORDER) {
      query.submit_and_finalize();
    } else {
      query.submit();
    }

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

    // Load fragment metadata.
    auto frag_meta = array->array_->fragment_metadata();
    auto& enc_key = array->array_->get_encryption_key();
    frag_meta[f]->loaded_metadata()->load_fragment_min_max_sum_null_count(
        enc_key);

    // Load the metadata and validate coords netadata.
    bool has_coords = layout != TILEDB_ROW_MAJOR;
    if (has_coords) {
      std::vector<std::string> names{"d"};
      frag_meta[f]->loaded_metadata()->load_rtree(enc_key);
      frag_meta[f]->loaded_metadata()->load_tile_min_values(enc_key, names);
      frag_meta[f]->loaded_metadata()->load_tile_max_values(enc_key, names);
      frag_meta[f]->loaded_metadata()->load_tile_sum_values(enc_key, names);
      frag_meta[f]->loaded_metadata()->load_tile_null_count_values(
          enc_key, names);

      // Validation.
      // Min/max/sum for all null tile are invalid.
      if (!all_null) {
        // Do fragment metadata first.
        {
          int64_t correct_sum =
              (num_tiles_ * tile_extent_) * (num_tiles_ * tile_extent_ - 1) / 2;

          // Validate no min.
          CHECK_THROWS_WITH(
              frag_meta[f]->loaded_metadata()->get_min("d"),
              "FragmentMetadata: Trying to access fragment min metadata that's "
              "not present");

          // Validate no max.
          CHECK_THROWS_WITH(
              frag_meta[f]->loaded_metadata()->get_max("d"),
              "FragmentMetadata: Trying to access fragment max metadata that's "
              "not present");

          // Validate sum.
          auto sum = frag_meta[f]->loaded_metadata()->get_sum("d");
          CHECK(*(int64_t*)sum == correct_sum);
        }

        for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
          uint32_t correct_min = tile_idx * tile_extent_;
          uint32_t correct_max = tile_idx * tile_extent_ + tile_extent_ - 1;
          int64_t correct_sum =
              (tile_extent_) * (correct_min + correct_max) / 2;

          // Validate no min.
          CHECK_THROWS_WITH(
              frag_meta[f]->get_tile_min_as<uint32_t>("d", tile_idx),
              "FragmentMetadata: Trying to access tile min metadata that's not "
              "present");

          // Validate no max.
          CHECK_THROWS_WITH(
              frag_meta[f]->get_tile_max_as<uint32_t>("d", tile_idx),
              "FragmentMetadata: Trying to access tile max metadata that's not "
              "present");

          // Validate sum.
          auto sum =
              frag_meta[f]->loaded_metadata()->get_tile_sum("d", tile_idx);
          CHECK(*(int64_t*)sum == correct_sum);

          // Validate the tile metadata structure.
          auto full_tile_data = frag_meta[f]->get_tile_metadata("d", tile_idx);
          CHECK(correct_min == full_tile_data.min_as<uint32_t>());
          CHECK(correct_max == full_tile_data.max_as<uint32_t>());
          CHECK(correct_sum == full_tile_data.sum_as<int64_t>());
        }
      }
    }

    // Do fragment metadata first for attribute.
    {
      // Min/max/sum for all null tile are invalid.
      if (!all_null) {
        // Validate min.
        auto& min = frag_meta[f]->loaded_metadata()->get_min("a");
        CHECK(min.size() == strings_[correct_mins_[f]].size());
        CHECK(
            0 == strncmp(
                     (const char*)min.data(),
                     strings_[correct_mins_[f]].c_str(),
                     strings_[correct_mins_[f]].size()));

        // Validate max.
        auto& max = frag_meta[f]->loaded_metadata()->get_max("a");
        CHECK(max.size() == strings_[correct_maxs_[f]].size());
        CHECK(
            0 == strncmp(
                     (const char*)max.data(),
                     strings_[correct_maxs_[f]].c_str(),
                     strings_[correct_maxs_[f]].size()));

        // Validate no sum.
        CHECK_THROWS_WITH(
            frag_meta[f]->loaded_metadata()->get_sum("a"),
            "FragmentMetadata: Trying to access fragment sum metadata that's "
            "not present");
      }

      // Check null count.
      if (nullable) {
        auto nc = frag_meta[f]->loaded_metadata()->get_null_count("a");
        CHECK(nc == correct_null_counts_[f]);
      } else {
        CHECK_THROWS_WITH(
            frag_meta[f]->loaded_metadata()->get_null_count("a"),
            "FragmentMetadata: Trying to access fragment null count metadata "
            "that's not present");
      }
    }

    // Load attribute metadata.
    std::vector<std::string> names{"a"};
    frag_meta[f]->loaded_metadata()->load_tile_min_values(enc_key, names);
    frag_meta[f]->loaded_metadata()->load_tile_max_values(enc_key, names);
    frag_meta[f]->loaded_metadata()->load_tile_sum_values(enc_key, names);
    frag_meta[f]->loaded_metadata()->load_tile_null_count_values(
        enc_key, names);

    // Validate attribute metadata.
    // Min/max/sum for all null tile are invalid.
    if (!all_null) {
      for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
        // Validate min.
        const auto min =
            frag_meta[f]->get_tile_min_as<std::string_view>("a", tile_idx);
        int min_idx = correct_tile_mins_[f][tile_idx];
        CHECK(min.size() == strings_[min_idx].size());
        CHECK(
            0 == strncmp(
                     min.data(),
                     strings_[min_idx].c_str(),
                     strings_[min_idx].size()));

        // Validate max.
        const auto max =
            frag_meta[f]->get_tile_max_as<std::string_view>("a", tile_idx);
        int max_idx = correct_tile_maxs_[f][tile_idx];
        CHECK(max.size() == strings_[max_idx].size());
        CHECK(
            0 == strncmp(
                     max.data(),
                     strings_[max_idx].c_str(),
                     strings_[max_idx].size()));

        // Validate no sum.
        CHECK_THROWS_WITH(
            frag_meta[f]->loaded_metadata()->get_tile_sum("a", tile_idx),
            "FragmentMetadata: Trying to access tile sum metadata that's not "
            "present");

        // Validate the tile metadata structure.
        auto full_tile_data = frag_meta[f]->get_tile_metadata("a", tile_idx);
        CHECK(strings_[min_idx] == full_tile_data.min_as<std::string_view>());
        CHECK(strings_[max_idx] == full_tile_data.max_as<std::string_view>());
      }
    }

    // Check null count.
    for (uint64_t tile_idx = 0; tile_idx < num_tiles_; tile_idx++) {
      if (nullable) {
        auto nc =
            frag_meta[f]->loaded_metadata()->get_tile_null_count("a", tile_idx);
        CHECK(nc == correct_tile_null_counts_[f][tile_idx]);
      } else {
        CHECK_THROWS_WITH(
            frag_meta[f]->loaded_metadata()->get_tile_null_count("a", tile_idx),
            "FragmentMetadata: Trying to access tile null count metadata "
            "that's not present");
      }
    }

    // Close array.
    rc = tiledb_array_close(ctx, array);
    CHECK(rc == TILEDB_OK);

    // Clean up.
    tiledb_array_free(&array);
    tiledb_ctx_free(&ctx);
  }

  std::vector<std::vector<int>> correct_tile_mins_;
  std::vector<std::vector<int>> correct_tile_maxs_;
  std::vector<std::vector<uint64_t>> correct_tile_null_counts_;
  std::vector<int> correct_mins_;
  std::vector<int> correct_maxs_;
  std::vector<uint64_t> correct_null_counts_;
  std::vector<std::string> strings_;

  const char* ARRAY_NAME = "tile_metadata_unit_array";
  const uint64_t tile_extent_ = 10;
  const uint64_t num_cells_ = 1000;
  const uint64_t num_tiles_ = num_cells_ / tile_extent_;
  tiledb::Context ctx_;
  tiledb::VFS vfs_;
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

struct CPPFixedTileMetadataPartialFx {
  CPPFixedTileMetadataPartialFx()
      : vfs_(ctx_) {
    if (vfs_.is_dir(ARRAY_NAME))
      vfs_.remove_dir(ARRAY_NAME);
  }

  ~CPPFixedTileMetadataPartialFx() {
    if (vfs_.is_dir(ARRAY_NAME))
      vfs_.remove_dir(ARRAY_NAME);
  }

  void create_array() {
    // Create TileDB context
    tiledb_ctx_t* ctx;
    tiledb_ctx_alloc(NULL, &ctx);

    // The array will be two dimension "d1" and "d2", with domain [1,8].
    uint32_t dim_domain[]{1, 8};
    tiledb_dimension_t* d1;
    tiledb_dimension_alloc(
        ctx, "d1", TILEDB_UINT32, &dim_domain[0], &tile_extent_, &d1);
    tiledb_dimension_t* d2;
    tiledb_dimension_alloc(
        ctx, "d2", TILEDB_UINT32, &dim_domain[0], &tile_extent_, &d2);

    // Create domain
    tiledb_domain_t* domain;
    tiledb_domain_alloc(ctx, &domain);
    tiledb_domain_add_dimension(ctx, domain, d1);
    tiledb_domain_add_dimension(ctx, domain, d2);

    // Create a single attribute "a" so each (i,j) cell can store a double.
    tiledb_attribute_t* a;
    tiledb_attribute_alloc(ctx, "a", TILEDB_FLOAT64, &a);

    // Create array schema
    tiledb_array_schema_t* array_schema;
    tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
    tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
    tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
    tiledb_array_schema_set_domain(ctx, array_schema, domain);
    tiledb_array_schema_add_attribute(ctx, array_schema, a);

    // Create array
    tiledb_array_create(ctx, ARRAY_NAME, array_schema);

    // Clean up
    tiledb_attribute_free(&a);
    tiledb_dimension_free(&d1);
    tiledb_dimension_free(&d2);
    tiledb_domain_free(&domain);
    tiledb_array_schema_free(&array_schema);
    tiledb_ctx_free(&ctx);
  }

  void write_fragment() {
    // Write to the array.
    auto array = tiledb::Array(ctx_, ARRAY_NAME, TILEDB_WRITE);
    auto query = tiledb::Query(ctx_, array, TILEDB_WRITE);

    // Write a 4x4 square intersecting the 4 tiles of the array. Writting a 2x2
    // square to each tile.
    Subarray subarray(ctx_, array);
    uint32_t r[2]{3, 6};
    subarray.add_range(0, r[0], r[1]);
    subarray.add_range(1, r[0], r[1]);
    query.set_subarray(subarray);

    std::vector<double> a{
        1.7,
        1.1,
        2.2,
        2.5,
        1.5,
        1.3,
        2.1,
        2.6,
        3.4,
        3.8,
        4.1,
        4.2,
        3.5,
        3.2,
        4.9,
        4.6};
    query.set_layout(TILEDB_ROW_MAJOR);
    query.set_data_buffer("a", a);

    query.submit();
    query.finalize();
    array.close();
  }

  void check_metadata() {
    // Open array.
    tiledb_ctx_t* ctx;
    tiledb_ctx_alloc(NULL, &ctx);
    tiledb_array_t* array;
    int rc = tiledb_array_alloc(ctx, ARRAY_NAME, &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Load fragment metadata.
    auto frag_meta = array->array_->fragment_metadata();
    auto& enc_key = array->array_->get_encryption_key();
    frag_meta[0]->loaded_metadata()->load_fragment_min_max_sum_null_count(
        enc_key);

    // Do fragment metadata first for attribute.
    {
      // Validate min.
      auto& min = frag_meta[0]->loaded_metadata()->get_min("a");
      CHECK(min.size() == sizeof(double));

      double correct_min = 1.1;
      CHECK(0 == memcmp(min.data(), &correct_min, min.size()));

      // Validate max.
      auto& max = frag_meta[0]->loaded_metadata()->get_max("a");
      CHECK(max.size() == sizeof(double));

      double correct_max = 4.9;
      CHECK(0 == memcmp(max.data(), &correct_max, max.size()));

      // Validate sum.
      auto sum = frag_meta[0]->loaded_metadata()->get_sum("a");
      double correct_sum = 46.7;
      CHECK(*(double*)sum - correct_sum < 0.0001);
    }

    // Load attribute metadata.
    std::vector<std::string> names{"a"};
    frag_meta[0]->loaded_metadata()->load_tile_min_values(enc_key, names);
    frag_meta[0]->loaded_metadata()->load_tile_max_values(enc_key, names);
    frag_meta[0]->loaded_metadata()->load_tile_sum_values(enc_key, names);
    frag_meta[0]->loaded_metadata()->load_tile_null_count_values(
        enc_key, names);

    std::vector<double> correct_tile_mins{1.1, 2.1, 3.2, 4.1};
    std::vector<double> correct_tile_maxs{1.7, 2.6, 3.8, 4.9};
    std::vector<double> correct_tile_sums{5.6, 9.4, 13.9, 17.8};

    // Validate attribute metadta.
    for (uint64_t tile_idx = 0; tile_idx < 4; tile_idx++) {
      // Validate min.
      const auto min = frag_meta[0]->get_tile_min_as<double>("a", tile_idx);
      CHECK(0 == memcmp(&min, &correct_tile_mins[tile_idx], sizeof(double)));

      // Validate max.
      const auto max = frag_meta[0]->get_tile_max_as<double>("a", tile_idx);
      CHECK(0 == memcmp(&max, &correct_tile_maxs[tile_idx], sizeof(double)));

      // Validate sum.
      auto sum = frag_meta[0]->loaded_metadata()->get_tile_sum("a", tile_idx);
      CHECK(*(double*)sum - correct_tile_sums[tile_idx] < 0.0001);

      // Validate the tile metadata structure.
      auto full_tile_data = frag_meta[0]->get_tile_metadata("a", tile_idx);
      CHECK(correct_tile_mins[tile_idx] == full_tile_data.min_as<double>());
      CHECK(correct_tile_maxs[tile_idx] == full_tile_data.max_as<double>());
      CHECK(
          std::abs(
              correct_tile_sums[tile_idx] - full_tile_data.sum_as<double>()) <
          0.0001);
    }

    // Close array.
    rc = tiledb_array_close(ctx, array);
    CHECK(rc == TILEDB_OK);

    // Clean up.
    tiledb_array_free(&array);
    tiledb_ctx_free(&ctx);
  }

  const char* ARRAY_NAME = "tile_metadata_unit_array";
  const uint64_t tile_extent_ = 4;
  tiledb::Context ctx_;
  tiledb::VFS vfs_;
};

TEST_CASE_METHOD(
    CPPFixedTileMetadataPartialFx,
    "TileMetadata: partial tile fixed",
    "[tile-metadata][fixed][partial]") {
  // Create the array.
  create_array();
  write_fragment();
  check_metadata();
}

struct CPPVarTileMetadataPartialFx {
  CPPVarTileMetadataPartialFx()
      : vfs_(ctx_) {
    if (vfs_.is_dir(ARRAY_NAME))
      vfs_.remove_dir(ARRAY_NAME);
  }

  ~CPPVarTileMetadataPartialFx() {
    if (vfs_.is_dir(ARRAY_NAME))
      vfs_.remove_dir(ARRAY_NAME);
  }

  void create_array() {
    // Create TileDB context
    tiledb_ctx_t* ctx;
    tiledb_ctx_alloc(NULL, &ctx);

    // The array will be two dimension "d1" and "d2", with domain [1,8].
    uint32_t dim_domain[]{1, 8};
    tiledb_dimension_t* d1;
    tiledb_dimension_alloc(
        ctx, "d1", TILEDB_UINT32, &dim_domain[0], &tile_extent_, &d1);
    tiledb_dimension_t* d2;
    tiledb_dimension_alloc(
        ctx, "d2", TILEDB_UINT32, &dim_domain[0], &tile_extent_, &d2);

    // Create domain
    tiledb_domain_t* domain;
    tiledb_domain_alloc(ctx, &domain);
    tiledb_domain_add_dimension(ctx, domain, d1);
    tiledb_domain_add_dimension(ctx, domain, d2);

    // Create a single attribute "a" so each (i,j) cell can store a string
    tiledb_attribute_t* a;
    tiledb_attribute_alloc(ctx, "a", TILEDB_STRING_ASCII, &a);
    tiledb_attribute_set_cell_val_num(ctx, a, TILEDB_VAR_NUM);

    // Create array schema
    tiledb_array_schema_t* array_schema;
    tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
    tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
    tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
    tiledb_array_schema_set_domain(ctx, array_schema, domain);
    tiledb_array_schema_add_attribute(ctx, array_schema, a);

    // Create array
    tiledb_array_create(ctx, ARRAY_NAME, array_schema);

    // Clean up
    tiledb_attribute_free(&a);
    tiledb_dimension_free(&d1);
    tiledb_dimension_free(&d2);
    tiledb_domain_free(&domain);
    tiledb_array_schema_free(&array_schema);
    tiledb_ctx_free(&ctx);
  }

  void write_fragment() {
    // Write to the array.
    auto array = tiledb::Array(ctx_, ARRAY_NAME, TILEDB_WRITE);
    auto query = tiledb::Query(ctx_, array, TILEDB_WRITE);

    // Write a 4x4 square intersecting the 4 tiles of the array. Writting a 2x2
    // square to each tile.
    Subarray subarray(ctx_, array);
    uint32_t r[2]{3, 6};
    subarray.add_range(0, r[0], r[1]);
    subarray.add_range(1, r[0], r[1]);
    query.set_subarray(subarray);

    std::string a = "1.71.12.22.51.51.32.12.63.43.84.14.23.53.24.94.6";
    std::vector<uint64_t> offsets{
        0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45};
    query.set_layout(TILEDB_ROW_MAJOR);
    query.set_data_buffer("a", a).set_offsets_buffer("a", offsets);

    query.submit();
    query.finalize();
    array.close();
  }

  void check_metadata() {
    // Open array.
    tiledb_ctx_t* ctx;
    tiledb_ctx_alloc(NULL, &ctx);
    tiledb_array_t* array;
    int rc = tiledb_array_alloc(ctx, ARRAY_NAME, &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Load fragment metadata.
    auto frag_meta = array->array_->fragment_metadata();
    auto& enc_key = array->array_->get_encryption_key();
    frag_meta[0]->loaded_metadata()->load_fragment_min_max_sum_null_count(
        enc_key);

    // Do fragment metadata first for attribute.
    {
      // Validate min.
      auto& min = frag_meta[0]->loaded_metadata()->get_min("a");
      std::string correct_min = "1.1";
      CHECK(min.size() == correct_min.size());
      CHECK(0 == memcmp(min.data(), correct_min.data(), min.size()));

      // Validate max.
      auto& max = frag_meta[0]->loaded_metadata()->get_max("a");
      std::string correct_max = "4.9";
      CHECK(max.size() == correct_max.size());
      CHECK(0 == memcmp(max.data(), correct_max.data(), max.size()));
    }

    // Load attribute metadata.
    std::vector<std::string> names{"a"};
    frag_meta[0]->loaded_metadata()->load_tile_min_values(enc_key, names);
    frag_meta[0]->loaded_metadata()->load_tile_max_values(enc_key, names);
    frag_meta[0]->loaded_metadata()->load_tile_sum_values(enc_key, names);
    frag_meta[0]->loaded_metadata()->load_tile_null_count_values(
        enc_key, names);

    std::vector<std::string> correct_tile_mins{"1.1", "2.1", "3.2", "4.1"};
    std::vector<std::string> correct_tile_maxs{"1.7", "2.6", "3.8", "4.9"};

    // Validate attribute metadta.
    for (uint64_t tile_idx = 0; tile_idx < 4; tile_idx++) {
      // Validate min.
      const auto min =
          frag_meta[0]->get_tile_min_as<std::string_view>("a", tile_idx);
      CHECK(min.size() == correct_tile_mins[tile_idx].size());
      CHECK(
          0 ==
          memcmp(min.data(), correct_tile_mins[tile_idx].data(), min.size()));

      // Validate max.
      const auto max =
          frag_meta[0]->get_tile_max_as<std::string_view>("a", tile_idx);
      CHECK(max.size() == correct_tile_maxs[tile_idx].size());
      CHECK(
          0 ==
          memcmp(max.data(), correct_tile_maxs[tile_idx].data(), max.size()));

      // Validate the tile metadata structure.
      auto full_tile_data = frag_meta[0]->get_tile_metadata("a", tile_idx);
      CHECK(
          correct_tile_mins[tile_idx] ==
          full_tile_data.min_as<std::string_view>());
      CHECK(
          correct_tile_maxs[tile_idx] ==
          full_tile_data.max_as<std::string_view>());
    }

    // Close array.
    rc = tiledb_array_close(ctx, array);
    CHECK(rc == TILEDB_OK);

    // Clean up.
    tiledb_array_free(&array);
    tiledb_ctx_free(&ctx);
  }

  const char* ARRAY_NAME = "tile_metadata_unit_array";
  const uint64_t tile_extent_ = 4;
  tiledb::Context ctx_;
  tiledb::VFS vfs_;
};

TEST_CASE_METHOD(
    CPPVarTileMetadataPartialFx,
    "TileMetadata: partial tile var",
    "[tile-metadata][var][partial]") {
  // Create the array.
  create_array();
  write_fragment();
  check_metadata();
}

struct CPPTileMetadataStringDimFx {
  CPPTileMetadataStringDimFx()
      : vfs_(ctx_) {
    if (vfs_.is_dir(ARRAY_NAME))
      vfs_.remove_dir(ARRAY_NAME);
  }

  ~CPPTileMetadataStringDimFx() {
    if (vfs_.is_dir(ARRAY_NAME))
      vfs_.remove_dir(ARRAY_NAME);
  }

  void create_array() {
    // Create TileDB context
    tiledb_ctx_t* ctx;
    tiledb_ctx_alloc(NULL, &ctx);

    // The array will be two string dimension "d1" and "d2".
    tiledb_dimension_t* d1;
    tiledb_dimension_alloc(ctx, "d1", TILEDB_STRING_ASCII, 0, 0, &d1);
    tiledb_dimension_t* d2;
    tiledb_dimension_alloc(ctx, "d2", TILEDB_STRING_ASCII, 0, 0, &d2);

    // Create domain
    tiledb_domain_t* domain;
    tiledb_domain_alloc(ctx, &domain);
    tiledb_domain_add_dimension(ctx, domain, d1);
    tiledb_domain_add_dimension(ctx, domain, d2);

    // Create a single attribute "a" so each (i,j) cell can store a double
    tiledb_attribute_t* a;
    tiledb_attribute_alloc(ctx, "a", TILEDB_FLOAT64, &a);

    // Create array schema
    tiledb_array_schema_t* array_schema;
    tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
    tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
    tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
    tiledb_array_schema_set_domain(ctx, array_schema, domain);
    tiledb_array_schema_add_attribute(ctx, array_schema, a);

    // Create array
    tiledb_array_create(ctx, ARRAY_NAME, array_schema);

    // Clean up
    tiledb_attribute_free(&a);
    tiledb_dimension_free(&d1);
    tiledb_dimension_free(&d2);
    tiledb_domain_free(&domain);
    tiledb_array_schema_free(&array_schema);
    tiledb_ctx_free(&ctx);
  }

  void write_fragment() {
    // Write to the array.
    auto array = tiledb::Array(ctx_, ARRAY_NAME, TILEDB_WRITE);
    auto query = tiledb::Query(ctx_, array, TILEDB_WRITE);

    std::string d1 = "abbcccdddd";
    std::vector<uint64_t> d1_offsets{0, 1, 3, 6};
    std::string d2 = "abcd";
    std::vector<uint64_t> d2_offsets{0, 1, 2, 3};
    std::vector<double> a{4, 5, 6, 7};
    query.set_layout(TILEDB_UNORDERED);
    query.set_data_buffer("d1", d1).set_offsets_buffer("d1", d1_offsets);
    query.set_data_buffer("d2", d2).set_offsets_buffer("d2", d2_offsets);
    query.set_data_buffer("a", a);

    query.submit();
    query.finalize();
    array.close();
  }

  void check_metadata() {
    // Open array.
    tiledb_ctx_t* ctx;
    tiledb_ctx_alloc(NULL, &ctx);
    tiledb_array_t* array;
    int rc = tiledb_array_alloc(ctx, ARRAY_NAME, &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Load fragment metadata.
    auto frag_meta = array->array_->fragment_metadata();
    auto& enc_key = array->array_->get_encryption_key();
    frag_meta[0]->loaded_metadata()->load_fragment_min_max_sum_null_count(
        enc_key);

    // Do fragment metadata first.
    {
      // Validate mins.
      auto& min = frag_meta[0]->loaded_metadata()->get_min("a");
      CHECK(min.size() == sizeof(double));
      CHECK(*static_cast<double*>(static_cast<void*>(min.data())) == 4);

      CHECK_THROWS_WITH(
          frag_meta[0]->loaded_metadata()->get_min("d1"),
          "FragmentMetadata: Trying to access fragment min metadata that's "
          "not present");

      CHECK_THROWS_WITH(
          frag_meta[0]->loaded_metadata()->get_min("d2"),
          "FragmentMetadata: Trying to access fragment min metadata that's "
          "not present");

      // Validate maxs.
      auto& max = frag_meta[0]->loaded_metadata()->get_max("a");
      CHECK(max.size() == sizeof(double));
      CHECK(*static_cast<double*>(static_cast<void*>(max.data())) == 7);

      CHECK_THROWS_WITH(
          frag_meta[0]->loaded_metadata()->get_max("d1"),
          "FragmentMetadata: Trying to access fragment max metadata that's "
          "not present");

      CHECK_THROWS_WITH(
          frag_meta[0]->loaded_metadata()->get_max("d2"),
          "FragmentMetadata: Trying to access fragment max metadata that's "
          "not present");
    }

    // Load metadata.
    std::vector<std::string> names{"a", "d1", "d2"};
    frag_meta[0]->loaded_metadata()->load_rtree(enc_key);
    frag_meta[0]->loaded_metadata()->load_tile_min_values(enc_key, names);
    frag_meta[0]->loaded_metadata()->load_tile_max_values(enc_key, names);
    frag_meta[0]->loaded_metadata()->load_tile_sum_values(enc_key, names);
    frag_meta[0]->loaded_metadata()->load_tile_null_count_values(
        enc_key, names);

    // Validate min.
    CHECK(frag_meta[0]->get_tile_min_as<double>("a", 0) == 4);
    CHECK_THROWS_WITH(
        frag_meta[0]->get_tile_min_as<std::string_view>("d1", 0),
        "FragmentMetadata: Trying to access tile min metadata that's not "
        "present");
    CHECK_THROWS_WITH(
        frag_meta[0]->get_tile_min_as<std::string_view>("d2", 0),
        "FragmentMetadata: Trying to access tile min metadata that's not "
        "present");

    // Validate max.
    CHECK(frag_meta[0]->get_tile_max_as<double>("a", 0) == 7);
    CHECK_THROWS_WITH(
        frag_meta[0]->get_tile_max_as<std::string_view>("d1", 0),
        "FragmentMetadata: Trying to access tile max metadata that's not "
        "present");
    CHECK_THROWS_WITH(
        frag_meta[0]->get_tile_max_as<std::string_view>("d2", 0),
        "FragmentMetadata: Trying to access tile max metadata that's not "
        "present");

    // Validate sum.
    CHECK(
        *(double*)frag_meta[0]->loaded_metadata()->get_tile_sum("a", 0) == 22);

    // Validate the tile metadata structure.
    auto full_tile_data_a = frag_meta[0]->get_tile_metadata("a", 0);
    CHECK(4 == full_tile_data_a.min_as<double>());
    CHECK(7 == full_tile_data_a.max_as<double>());
    CHECK(22 == full_tile_data_a.sum_as<double>());

    auto full_tile_data_d1 = frag_meta[0]->get_tile_metadata("d1", 0);
    CHECK("a" == full_tile_data_d1.min_as<std::string_view>());
    CHECK("dddd" == full_tile_data_d1.max_as<std::string_view>());

    auto full_tile_data_d2 = frag_meta[0]->get_tile_metadata("d2", 0);
    CHECK("a" == full_tile_data_d2.min_as<std::string_view>());
    CHECK("d" == full_tile_data_d2.max_as<std::string_view>());

    // Close array.
    rc = tiledb_array_close(ctx, array);
    CHECK(rc == TILEDB_OK);

    // Clean up.
    tiledb_array_free(&array);
    tiledb_ctx_free(&ctx);
  }

  const char* ARRAY_NAME = "tile_metadata_unit_array";
  const uint64_t tile_extent_ = 4;
  tiledb::Context ctx_;
  tiledb::VFS vfs_;
};

TEST_CASE_METHOD(
    CPPTileMetadataStringDimFx,
    "TileMetadata: string dims",
    "[tile-metadata][string-dims]") {
  // Create the array.
  create_array();
  write_fragment();
  check_metadata();
}
