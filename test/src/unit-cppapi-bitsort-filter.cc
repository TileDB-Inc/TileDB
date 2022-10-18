/**
 * @file   unit-cppapi-bitsort-filter.cc
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
 * Tests the C++ API for bitsort filter related functions.
 */

#include <optional>
#include <random>
#include <tuple>
#include <vector>

#include <test/support/tdb_catch.h>
#include "tiledb/common/common.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

const int BITSORT_DIM_LO = 1;
const int BITSORT_DIM_HI = 10;
const int TILE_EXTENT = 4;
const int CAPACITY = 32;
const int SEED = 0xADA65ED6;

// Defining distribution parameters.
typedef typename std::uniform_int_distribution<uint64_t>
    UnsignedIntDistribution;
typedef typename std::uniform_int_distribution<int64_t> IntDistribution;
typedef typename std::uniform_real_distribution<double> FloatDistribution;

// Defining index-dimension map type.
struct DimIdxValue {
  DimIdxValue(
      std::optional<int> x,
      std::optional<int> y,
      std::optional<int> z,
      int read_idx)
      : x_(x)
      , y_(y)
      , z_(z)
      , read_idx_(read_idx) {
  }

  std::optional<int> x_;
  std::optional<int> y_;
  std::optional<int> z_;
  int read_idx_;
};

/**
 * @brief This is the type that defines the metadata used in the CPP API bitsort
 * tests. It includes the constructed dimension data vectors to use for writing,
 * along with a global index -> dimension data map. The dimension data includes
 * the correct x, y, and z coordinates for that global index, along with the
 * index that corresponds with the read-in attribute data (this is read_index_).
 * For any layout, we should note that for a global array for attribute array
 * (global_attribute_data), and a read-in array with the specified layout in the
 * test case using this metadata, (read_in_data), and a map called dim_idx_map,
 * global_attribute_data[i] == read_in_data[dim_idx_map[i]] should be true.
 *
 * @tparam DimType The type of the dimension.
 */
template <typename DimType>
using DimensionDataMetadata = std::tuple<
    std::vector<DimType>,
    std::vector<DimType>,
    std::vector<DimType>,
    std::vector<DimIdxValue>>;

/**
 * @brief Set the buffer with the appropriate dimension for a 1D array.
 *
 * @tparam DimType The type of the dimension.
 * @returns The dimension storage buffer and the dimension index map.
 */
template <typename DimType>
DimensionDataMetadata<DimType> set_1d_dim_buffers() {
  std::vector<DimType> x_dims_data;
  x_dims_data.reserve(BITSORT_DIM_HI - BITSORT_DIM_LO + 1);
  std::vector<DimIdxValue> dim_idx_map;
  dim_idx_map.reserve(BITSORT_DIM_HI - BITSORT_DIM_LO + 1);

  for (int x = BITSORT_DIM_LO; x <= BITSORT_DIM_HI; ++x) {
    x_dims_data.emplace_back(static_cast<DimType>(x));
    dim_idx_map.emplace_back(
        std::optional(x), std::nullopt, std::nullopt, x - BITSORT_DIM_LO);
  }
  return make_tuple(
      x_dims_data, std::vector<DimType>(), std::vector<DimType>(), dim_idx_map);
}

/**
 * @brief Set the buffer with the appropriate dimensions for a 2D array.
 *
 * @tparam DimType The type of the dimension.
 * @param read_layout The read layout of the read query.
 * @returns The dimension storage buffers and the dimension index map.
 */
template <typename DimType>
DimensionDataMetadata<DimType> set_2d_dim_buffers(tiledb_layout_t read_layout) {
  std::vector<DimType> x_dims_data;
  std::vector<DimType> y_dims_data;
  std::vector<DimIdxValue> dim_idx_map;

  int element_size = (BITSORT_DIM_HI - BITSORT_DIM_LO + 1);
  x_dims_data.reserve(element_size * element_size);
  y_dims_data.reserve(element_size * element_size);
  dim_idx_map.reserve(element_size * element_size);

  int global_read_index = 0;
  for (int tile_idx_x = BITSORT_DIM_LO; tile_idx_x <= BITSORT_DIM_HI;
       tile_idx_x += TILE_EXTENT) {
    for (int tile_idx_y = BITSORT_DIM_LO; tile_idx_y <= BITSORT_DIM_HI;
         tile_idx_y += TILE_EXTENT) {
      for (int x = tile_idx_x; x < tile_idx_x + TILE_EXTENT; ++x) {
        if (x > BITSORT_DIM_HI) {
          break;
        }

        for (int y = tile_idx_y; y < tile_idx_y + TILE_EXTENT; ++y) {
          if (y > BITSORT_DIM_HI) {
            break;
          }

          // Find read index based on the read layout.
          int read_index = global_read_index;
          if (read_layout == TILEDB_ROW_MAJOR) {
            read_index =
                ((x - BITSORT_DIM_LO) * element_size) + (y - BITSORT_DIM_LO);
          } else if (read_layout == TILEDB_COL_MAJOR) {
            read_index =
                ((y - BITSORT_DIM_LO) * element_size) + (x - BITSORT_DIM_LO);
          }

          x_dims_data.emplace_back(static_cast<DimType>(x));
          y_dims_data.emplace_back(static_cast<DimType>(y));
          dim_idx_map.emplace_back(
              std::optional(x), std::optional(y), std::nullopt, read_index);

          global_read_index += 1;
        }
      }
    }
  }

  return make_tuple(
      x_dims_data, y_dims_data, std::vector<DimType>(), dim_idx_map);
}

/**
 * @brief Set the buffer with the appropriate dimensions for a 3D array.
 *
 * @tparam DimType The type of the dimension.
 * @param read_layout The read layout of the read query.
 * @returns The dimension storage buffers and the dimension index map.
 */
template <typename DimType>
DimensionDataMetadata<DimType> set_3d_dim_buffers(tiledb_layout_t read_layout) {
  std::vector<DimType> x_dims_data;
  std::vector<DimType> y_dims_data;
  std::vector<DimType> z_dims_data;
  std::vector<DimIdxValue> dim_idx_map;

  int element_size = (BITSORT_DIM_HI - BITSORT_DIM_LO + 1);
  x_dims_data.reserve(element_size * element_size * element_size);
  y_dims_data.reserve(element_size * element_size * element_size);
  z_dims_data.reserve(element_size * element_size * element_size);
  dim_idx_map.reserve(element_size * element_size * element_size);

  int global_read_index = 0;
  for (int tile_idx_x = BITSORT_DIM_LO; tile_idx_x <= BITSORT_DIM_HI;
       tile_idx_x += TILE_EXTENT) {
    for (int tile_idx_y = BITSORT_DIM_LO; tile_idx_y <= BITSORT_DIM_HI;
         tile_idx_y += TILE_EXTENT) {
      for (int tile_idx_z = BITSORT_DIM_LO; tile_idx_z <= BITSORT_DIM_HI;
           tile_idx_z += TILE_EXTENT) {
        for (int x = tile_idx_x; x < tile_idx_x + TILE_EXTENT; ++x) {
          if (x > BITSORT_DIM_HI) {
            break;
          }

          for (int y = tile_idx_y; y < tile_idx_y + TILE_EXTENT; ++y) {
            if (y > BITSORT_DIM_HI) {
              break;
            }

            for (int z = tile_idx_z; z < tile_idx_z + TILE_EXTENT; ++z) {
              if (z > BITSORT_DIM_HI) {
                break;
              }

              // Find read index based on the layout.
              int read_index = global_read_index;
              if (read_layout == TILEDB_ROW_MAJOR) {
                read_index = ((((x - BITSORT_DIM_LO) * element_size) +
                               (y - BITSORT_DIM_LO)) *
                              element_size) +
                             (z - BITSORT_DIM_LO);
              } else if (read_layout == TILEDB_COL_MAJOR) {
                read_index = ((((z - BITSORT_DIM_LO) * element_size) +
                               (y - BITSORT_DIM_LO)) *
                              element_size) +
                             (x - BITSORT_DIM_LO);
              }

              x_dims_data.emplace_back(static_cast<DimType>(x));
              y_dims_data.emplace_back(static_cast<DimType>(y));
              z_dims_data.emplace_back(static_cast<DimType>(z));
              dim_idx_map.emplace_back(
                  std::optional(x),
                  std::optional(y),
                  std::optional(z),
                  read_index);

              global_read_index += 1;
            }
          }
        }
      }
    }
  }

  return std::make_tuple(x_dims_data, y_dims_data, z_dims_data, dim_idx_map);
}

/**
 * @brief Checks that a read query returns the correct results for an array
 * with the specifications indicated by the parameters.
 *
 * @tparam AttrType The type of the attribute data in the array.
 * @tparam DimType The type of the dimensions of the array.
 * @param num_dims The number of dimensions.
 * @param global_a The attribute data in global order.
 * @param a_data_read The read-in attribute data.
 * @param dim_idx_map The map that maps global indexes to dimension data/read
 * index.
 * @param has_dimensions Whether the read included reading in the dimensions.
 * @param x_dims_data The first buffer that the function checks.
 * @param y_dims_data The second buffer that the function checks.
 * @param z_dims_data The third buffer that the function checks.
 */
template <typename AttrType, typename DimType>
void check_read(
    uint64_t num_dims,
    const std::vector<AttrType>& global_a,
    const std::vector<AttrType>& a_data_read,
    const std::vector<DimIdxValue>& dim_idx_map,
    bool has_dimensions,
    const std::vector<DimType>& x_dim_data = {},
    const std::vector<DimType>& y_dim_data = {},
    const std::vector<DimType>& z_dim_data = {}) {
  uint64_t num_cells = global_a.size();
  REQUIRE(a_data_read.size() == num_cells);
  if (has_dimensions) {
    REQUIRE(x_dim_data.size() == num_cells);

    if (num_dims >= 2) {
      REQUIRE(y_dim_data.size() == num_cells);
    }

    if (num_dims == 3) {
      REQUIRE(z_dim_data.size() == num_cells);
    }
  }

  for (uint64_t i = 0; i < num_cells; ++i) {
    auto& dim_value = dim_idx_map[i];
    int read_idx = dim_value.read_idx_;
    CHECK(global_a[i] == a_data_read[read_idx]);

    if (has_dimensions) {
      // Check x dimension.
      REQUIRE(dim_value.x_.has_value());
      CHECK(x_dim_data[read_idx] == static_cast<DimType>(dim_value.x_.value()));

      if (num_dims == 2) {
        // Check y dimension.
        REQUIRE(dim_value.y_.has_value());
        CHECK(
            y_dim_data[read_idx] == static_cast<DimType>(dim_value.y_.value()));
      }

      if (num_dims == 3) {
        // Check z dimension.
        REQUIRE(dim_value.z_.has_value());
        CHECK(
            z_dim_data[read_idx] == static_cast<DimType>(dim_value.z_.value()));
      }
    }
  }
}

/**
 * @brief Sets the subarrays to the entire array for a read query.
 * Including this so we can test different code paths.
 *
 * @tparam DimType The type of the dimension of the array.
 * @param read_query The read query.
 * @param num_dims The number of dimensions.
 */
template <typename DimType>
void read_query_set_subarray(Query& read_query, int num_dims) {
  read_query.add_range<DimType>("x", BITSORT_DIM_LO, BITSORT_DIM_HI);

  if (num_dims >= 2) {
    read_query.add_range<DimType>("y", BITSORT_DIM_LO, BITSORT_DIM_HI);
  }

  if (num_dims == 3) {
    read_query.add_range<DimType>("z", BITSORT_DIM_LO, BITSORT_DIM_HI);
  }
}

/**
 * @brief Tests the bitsort filter, given the parameters passed in.
 * This function creates an array with num_dims dimensions and
 * the bitsort filter. Then, it writes into this array with randomly
 * generated attribute data, and then reads from the array to test
 * the bitsort filter.
 *
 * @tparam AttrType The type of the attribute data in the array.
 * @tparam DimType The type of the dimensions of the array.
 * @tparam AttributeDistribution The type of the random distribution used to
 * generate the attribute data.
 * @param bitsort_array_name The array name.
 * @param num_dims The number of dimensions.
 * @param write_layout The layout of the write query.
 * @param read_layout The layout of the read query.
 * @param set_subarray Whether the read query is called with a subarray
 * encompassing the whole array.
 * @param set_capacity Set whether the array has a custom capacity.
 */
template <typename AttrType, typename DimType, typename AttributeDistribution>
void bitsort_filter_api_test(
    const std::string& bitsort_array_name,
    uint64_t num_dims,
    tiledb_layout_t write_layout,
    tiledb_layout_t read_layout,
    bool set_subarray,
    bool set_capacity) {
  // Setup.
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(bitsort_array_name)) {
    vfs.remove_dir(bitsort_array_name);
  }

  Domain domain(ctx);

  // Add first dimension.
  REQUIRE(num_dims >= 1);
  REQUIRE(num_dims <= 3);
  size_t num_element_per_dim = (BITSORT_DIM_HI - BITSORT_DIM_LO + 1);
  size_t number_elements = num_element_per_dim;
  domain.add_dimension(Dimension::create<DimType>(
      ctx,
      "x",
      {{static_cast<DimType>(BITSORT_DIM_LO),
        static_cast<DimType>(BITSORT_DIM_HI)}},
      static_cast<DimType>(TILE_EXTENT)));

  if (num_dims >= 2) {
    domain.add_dimension(Dimension::create<DimType>(
        ctx,
        "y",
        {{static_cast<DimType>(BITSORT_DIM_LO),
          static_cast<DimType>(BITSORT_DIM_HI)}},
        static_cast<DimType>(TILE_EXTENT)));
    number_elements *= num_element_per_dim;
  }
  if (num_dims == 3) {
    domain.add_dimension(Dimension::create<DimType>(
        ctx,
        "z",
        {{static_cast<DimType>(BITSORT_DIM_LO),
          static_cast<DimType>(BITSORT_DIM_HI)}},
        static_cast<DimType>(TILE_EXTENT)));
    number_elements *= num_element_per_dim;
  }

  Filter f(ctx, TILEDB_FILTER_BITSORT);
  FilterList filters(ctx);
  filters.add_filter(f);

  auto a = Attribute::create<AttrType>(ctx, "a");
  a.set_filter_list(filters);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a);
  if (set_capacity) {
    schema.set_capacity(CAPACITY);
  }

  Array::create(bitsort_array_name, schema);

  // Setting up the random number generator for the bitsort filter testing.
  std::mt19937_64 gen(SEED);
  AttributeDistribution dis(
      std::numeric_limits<AttrType>::lowest(),
      std::numeric_limits<AttrType>::max());

  std::vector<AttrType> a_write;
  std::vector<AttrType> global_a;
  a_write.reserve(number_elements);
  global_a.reserve(number_elements);
  for (size_t i = 0; i < number_elements; ++i) {
    AttrType f = static_cast<AttrType>(dis(gen));
    a_write.emplace_back(f);
    global_a.emplace_back(f);
  }

  Array array_w(ctx, bitsort_array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(write_layout).set_data_buffer("a", a_write);
  // Set dimension buffers and the dimension index map.
  auto&& [x_dims_data, y_dims_data, z_dims_data, dim_idx_map]{
      (num_dims == 1) ?
          set_1d_dim_buffers<DimType>() :
          ((num_dims == 2) ? set_2d_dim_buffers<DimType>(read_layout) :
                             set_3d_dim_buffers<DimType>(read_layout))};

  // Setting data buffers.
  query_w.set_data_buffer("x", x_dims_data);

  if (num_dims >= 2) {
    query_w.set_data_buffer("y", y_dims_data);
  }
  if (num_dims == 3) {
    query_w.set_data_buffer("z", z_dims_data);
  }

  query_w.submit();
  query_w.finalize();
  array_w.close();

  // Open and read the entire array.
  std::vector<AttrType> a_data_read(number_elements, 0);
  Array array_r(ctx, bitsort_array_name, TILEDB_READ);
  Query query_r(ctx, array_r);
  query_r.set_data_buffer("a", a_data_read);
  query_r.set_layout(read_layout);

  if (set_subarray) {
    read_query_set_subarray<DimType>(query_r, num_dims);
  }

  query_r.submit();

  // Check for results.
  auto table = query_r.result_buffer_elements();
  REQUIRE(table.size() == 1);
  REQUIRE(table["a"].first == 0);
  REQUIRE(table["a"].second == number_elements);

  check_read<AttrType, DimType>(
      num_dims, global_a, a_data_read, dim_idx_map, false);

  query_r.finalize();
  array_r.close();

  // Set a query where we read the dimensions back.
  std::vector<AttrType> a_data_read_dims(number_elements, 0);
  std::vector<DimType> x_dims_data_read(number_elements, 0);
  std::vector<DimType> y_dims_data_read(number_elements, 0);
  std::vector<DimType> z_dims_data_read(number_elements, 0);

  Array array_r_dims(ctx, bitsort_array_name, TILEDB_READ);
  Query query_r_dims(ctx, array_r_dims);
  query_r_dims.set_data_buffer("a", a_data_read_dims);
  query_r_dims.set_layout(read_layout);

  query_r_dims.set_data_buffer("x", x_dims_data_read);

  if (num_dims >= 2) {
    query_r_dims.set_data_buffer("y", y_dims_data_read);
  }
  if (num_dims == 3) {
    query_r_dims.set_data_buffer("z", z_dims_data_read);
  }

  if (set_subarray) {
    read_query_set_subarray<DimType>(query_r_dims, num_dims);
  }

  query_r_dims.submit();

  // Check for results.
  auto table_dims = query_r_dims.result_buffer_elements();
  REQUIRE(table_dims.size() == 1 + num_dims);
  REQUIRE(table_dims["a"].first == 0);
  REQUIRE(table_dims["a"].second == number_elements);

  REQUIRE(table_dims["x"].first == 0);
  REQUIRE(table_dims["x"].second == number_elements);

  if (num_dims >= 2) {
    REQUIRE(table_dims["y"].first == 0);
    REQUIRE(table_dims["y"].second == number_elements);
  }
  if (num_dims == 3) {
    REQUIRE(table_dims["z"].first == 0);
    REQUIRE(table_dims["z"].second == number_elements);
  }

  check_read<AttrType, DimType>(
      num_dims,
      global_a,
      a_data_read_dims,
      dim_idx_map,
      true,
      x_dims_data_read,
      y_dims_data_read,
      z_dims_data_read);

  query_r_dims.finalize();
  array_r_dims.close();

  // Teardown.
  if (vfs.is_dir(bitsort_array_name)) {
    vfs.remove_dir(bitsort_array_name);
  }
}

/**
 * @brief Tests the bitsort filter for all dimension types.
 *
 * @tparam AttrType The type of the attribute data in the array.
 * @tparam AttributeDistribution The type of the random distribution used to
 * generate the attribute data.
 * @param bitsort_array_name The array name.
 * @param num_dims The number of dimensions.
 * @param write_layout The layout of the write query.
 * @param read_layout The layout of the read query.
 * @param set_subarray Whether the read query is called with a subarray
 * encompassing the whole array.
 * @param set_capacity Set whether the array has a custom capacity.
 */
template <typename AttrType, typename AttributeDistribution>
void bitsort_filter_api_test(
    const std::string& bitsort_array_name,
    uint64_t num_dims,
    tiledb_layout_t write_layout,
    tiledb_layout_t read_layout,
    bool set_subarray,
    bool set_capacity) {
  bitsort_filter_api_test<AttrType, int16_t, AttributeDistribution>(
      bitsort_array_name,
      num_dims,
      write_layout,
      read_layout,
      set_subarray,
      set_capacity);
  bitsort_filter_api_test<AttrType, int8_t, AttributeDistribution>(
      bitsort_array_name,
      num_dims,
      write_layout,
      read_layout,
      set_subarray,
      set_capacity);
  bitsort_filter_api_test<AttrType, int32_t, AttributeDistribution>(
      bitsort_array_name,
      num_dims,
      write_layout,
      read_layout,
      set_subarray,
      set_capacity);
  bitsort_filter_api_test<AttrType, int64_t, AttributeDistribution>(
      bitsort_array_name,
      num_dims,
      write_layout,
      read_layout,
      set_subarray,
      set_capacity);
  bitsort_filter_api_test<AttrType, uint8_t, AttributeDistribution>(
      bitsort_array_name,
      num_dims,
      write_layout,
      read_layout,
      set_subarray,
      set_capacity);
  bitsort_filter_api_test<AttrType, uint16_t, AttributeDistribution>(
      bitsort_array_name,
      num_dims,
      write_layout,
      read_layout,
      set_subarray,
      set_capacity);
  bitsort_filter_api_test<AttrType, uint32_t, AttributeDistribution>(
      bitsort_array_name,
      num_dims,
      write_layout,
      read_layout,
      set_subarray,
      set_capacity);
  bitsort_filter_api_test<AttrType, uint64_t, AttributeDistribution>(
      bitsort_array_name,
      num_dims,
      write_layout,
      read_layout,
      set_subarray,
      set_capacity);
  bitsort_filter_api_test<AttrType, float, AttributeDistribution>(
      bitsort_array_name,
      num_dims,
      write_layout,
      read_layout,
      set_subarray,
      set_capacity);
  bitsort_filter_api_test<AttrType, double, AttributeDistribution>(
      bitsort_array_name,
      num_dims,
      write_layout,
      read_layout,
      set_subarray,
      set_capacity);
}

TEMPLATE_TEST_CASE(
    "C++ API: Bitsort Filter Read on Array",
    "[cppapi][filter][bitsort][whee]",
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    float,
    double) {
  // Generate parameters.
  std::string array_name = "cpp_unit_bitsort_array";
  uint64_t num_dims = GENERATE(1, 2, 3);
  tiledb_layout_t write_layout =
      GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);
  tiledb_layout_t read_layout = GENERATE(
      TILEDB_UNORDERED,
      TILEDB_GLOBAL_ORDER,
      TILEDB_ROW_MAJOR,
      TILEDB_COL_MAJOR);
  bool set_subarray = GENERATE(true, false);
  bool set_capacity = false;  // TODO: set to generate.

  // Run tests.
  if constexpr (std::is_floating_point<TestType>::value) {
    bitsort_filter_api_test<TestType, FloatDistribution>(
        array_name,
        num_dims,
        write_layout,
        read_layout,
        set_subarray,
        set_capacity);
  } else if constexpr (std::is_unsigned<TestType>::value) {
    bitsort_filter_api_test<TestType, UnsignedIntDistribution>(
        array_name,
        num_dims,
        write_layout,
        read_layout,
        set_subarray,
        set_capacity);
  } else if constexpr (std::is_signed<TestType>::value) {
    bitsort_filter_api_test<TestType, IntDistribution>(
        array_name,
        num_dims,
        write_layout,
        read_layout,
        set_subarray,
        set_capacity);
  }
}

/*TEST_CASE("bitsort filter debugging test (set capacity)",
"[cppapi][filter][bitsort][capacity][!mayfail]") { uint64_t num_dims = 2;
  std::string array_name = "cpp_unit_bitsort_array";
  tiledb_layout_t write_layout = TILEDB_GLOBAL_ORDER;
  tiledb_layout_t read_layout = TILEDB_ROW_MAJOR;
  bool set_subarray = false;
  bool set_capacity = true;

  bitsort_filter_api_test<int32_t, int16_t, IntDistribution>(
        array_name, num_dims, write_layout, read_layout, set_subarray,
set_capacity);
}*/
