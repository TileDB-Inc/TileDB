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

#include <random>
#include <tuple>
#include <vector>

#include "catch.hpp"
#include "tiledb/common/common.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

const int BITSORT_DIM_LO = 1;
const int BITSORT_DIM_HI = 10;
const int TILE_EXTENT = 4;
const int SEED = 0xADA65ED6;

// Defining distribution parameters.
typedef typename std::uniform_int_distribution<uint64_t>
    UnsignedIntDistribution;
typedef typename std::uniform_int_distribution<int64_t> IntDistribution;
typedef typename std::uniform_real_distribution<double> FloatDistribution;

// Defining dimension vector storage type.
template<typename DimType>
using DimensionVectors = std::tuple<std::vector<DimType>, std::vector<DimType>, std::vector<DimType>>;

/**
 * @brief Set the buffer with the appropriate dimension for a 1D array.
 *
 * @tparam DimType The type of the dimension.
 * @returns The dimension storage buffer.
 */
template <typename DimType>
DimensionVectors<DimType> set_1d_dim_buffers() {
  std::vector<DimType> x_dims_data;
  for (int x = BITSORT_DIM_LO; x <= BITSORT_DIM_HI; ++x) {
    x_dims_data.push_back(static_cast<DimType>(x));
  }
  return make_tuple(x_dims_data, std::vector<DimType>(), std::vector<DimType>());
}

/**
 * @brief Set the buffer with the appropriate dimensions for a 2D array.
 *
 * @tparam DimType The type of the dimension.
 * @returns The dimension storage buffers.
 */
template <typename DimType>
DimensionVectors<DimType> set_2d_dim_buffers() {
  std::vector<DimType> x_dims_data;
  std::vector<DimType> y_dims_data;

  for (int tile_idx_x = BITSORT_DIM_LO; tile_idx_x <= BITSORT_DIM_HI; tile_idx_x += TILE_EXTENT) {
    for (int tile_idx_y = BITSORT_DIM_LO; tile_idx_y <= BITSORT_DIM_HI; tile_idx_y += TILE_EXTENT) {
      for (int x = tile_idx_x; x < tile_idx_x + TILE_EXTENT; ++x) {
        if (x > BITSORT_DIM_HI) {
          break;
        }

        for (int y = tile_idx_y; y < tile_idx_y + TILE_EXTENT; ++y) {
          if (y > BITSORT_DIM_HI) {
            break;
          }

          x_dims_data.push_back(static_cast<DimType>(x));
          y_dims_data.push_back(static_cast<DimType>(y));
        }
      }
    }
  }

  return make_tuple(x_dims_data, y_dims_data, std::vector<DimType>());
}

/**
 * @brief Set the buffer with the appropriate dimensions for a 3D array.
 *
 * @tparam DimType The type of the dimension.
 * @returns The dimension storage buffers.
 */
template <typename DimType>
DimensionVectors<DimType> set_3d_dim_buffers() {
  std::vector<DimType> x_dims_data;
  std::vector<DimType> y_dims_data;
  std::vector<DimType> z_dims_data;

  for (int tile_idx_x = BITSORT_DIM_LO; tile_idx_x <= BITSORT_DIM_HI; tile_idx_x += TILE_EXTENT) {
    for (int tile_idx_y = BITSORT_DIM_LO; tile_idx_y <= BITSORT_DIM_HI; tile_idx_y += TILE_EXTENT) {
      for (int tile_idx_z = BITSORT_DIM_LO; tile_idx_z <= BITSORT_DIM_HI; tile_idx_z += TILE_EXTENT) {
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

              x_dims_data.push_back(static_cast<DimType>(x));
              y_dims_data.push_back(static_cast<DimType>(y));
              z_dims_data.push_back(static_cast<DimType>(z));
            }
          }
        }
      }
    }
  }

  return std::make_tuple(x_dims_data, y_dims_data, z_dims_data);
}

/**
 * @brief Check the dimension buffer to ensure it has the appropriate data
 * for a 1D array.
 *
 * @tparam DimType The type of the dimension.
 * @param x_dims_data The buffer that this function checks.
 */
template <typename DimType>
void check_1d_dim_buffer(const std::vector<DimType>& x_dims_data) {
  size_t idx = 0;
  for (int x = BITSORT_DIM_LO; x <= BITSORT_DIM_HI; ++x) {
    CHECK(x_dims_data[idx++] == static_cast<DimType>(x));
  }
}

/**
 * @brief  Check the dimension buffer to ensure it has the appropriate
 * data for a 2D array read in global order.
 *
 * @tparam DimType The type of the dimension.
 * @param x_dims_data The first buffer that the function checks.
 * @param y_dims_data The second buffer that the function checks.
 */
template <typename DimType>
void check_2d_dim_buffers_global_read(
    const std::vector<DimType>& x_dims_data, const std::vector<DimType>& y_dims_data) {
  size_t idx = 0;
  for (int tile_idx_x = BITSORT_DIM_LO; tile_idx_x <= BITSORT_DIM_HI; tile_idx_x += TILE_EXTENT) {
    for (int tile_idx_y = BITSORT_DIM_LO; tile_idx_y <= BITSORT_DIM_HI; tile_idx_y += TILE_EXTENT) {
      for (int x = tile_idx_x; x < tile_idx_x + TILE_EXTENT; ++x) {
        if (x > BITSORT_DIM_HI) {
          break;
        }

        for (int y = tile_idx_y; y < tile_idx_y + TILE_EXTENT; ++y) {
          if (y > BITSORT_DIM_HI) {
            break;
          }

          CHECK(x_dims_data[idx] == static_cast<DimType>(x));
          CHECK(y_dims_data[idx] == static_cast<DimType>(y));
          idx += 1;
        }
      }
    }
  }
}

/**
 * @brief  Check the dimension buffer to ensure it has the appropriate
 * data for a 2D array read in row major order.
 *
 * @tparam DimType The type of the dimension.
 * @param x_dims_data The first buffer that the function checks.
 * @param y_dims_data The second buffer that the function checks.
 */
template <typename DimType>
void check_2d_dim_buffers_row_read(
    const std::vector<DimType>& x_dims_data, const std::vector<DimType>& y_dims_data) {
  size_t idx = 0;
  for (int x = BITSORT_DIM_LO; x <= BITSORT_DIM_HI; ++x) {
    for (int y = BITSORT_DIM_LO; y <= BITSORT_DIM_HI; ++y) {
      CHECK(x_dims_data[idx] == static_cast<DimType>(x));
      CHECK(y_dims_data[idx] == static_cast<DimType>(y));
      idx += 1;
    }
  }
}

/**
 * @brief  Check the dimension buffer to ensure it has the appropriate
 * data for a 2D array read in column major order.
 *
 * @tparam DimType The type of the dimension.
 * @param x_dims_data The first buffer that the function checks.
 * @param y_dims_data The second buffer that the function checks.
 */
template <typename DimType>
void check_2d_dim_buffers_col_read(
    const std::vector<DimType>& x_dims_data, const std::vector<DimType>& y_dims_data) {
  size_t idx = 0;
  for (int y = BITSORT_DIM_LO; y <= BITSORT_DIM_HI; ++y) {
    for (int x = BITSORT_DIM_LO; x <= BITSORT_DIM_HI; ++x) {
      CHECK(x_dims_data[idx] == static_cast<DimType>(x));
      CHECK(y_dims_data[idx] == static_cast<DimType>(y));
      idx += 1;
    }
  }
}


/**
 * @brief Check the dimension buffer to ensure it has the appropriate data for a 3D array.
 *
 * @tparam DimType The type of the dimension.
 * @param x_dims_data The first buffer that the function checks.
 * @param y_dims_data The second buffer that the function checks.
 * @param z_dims_data The third buffer that the function checks.
 */
template <typename DimType>
void check_3d_dim_buffers(
    const std::vector<DimType>& x_dims_data,
    const std::vector<DimType>& y_dims_data,
    const std::vector<DimType>& z_dims_data) {
  size_t idx = 0;
  for (int tile_idx_x = BITSORT_DIM_LO; tile_idx_x <= BITSORT_DIM_HI; tile_idx_x += TILE_EXTENT) {
    for (int tile_idx_y = BITSORT_DIM_LO; tile_idx_y <= BITSORT_DIM_HI; tile_idx_y += TILE_EXTENT) {
      for (int tile_idx_z = BITSORT_DIM_LO; tile_idx_z <= BITSORT_DIM_HI; tile_idx_z += TILE_EXTENT) {
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

              CHECK(x_dims_data[idx] == static_cast<DimType>(x));
              CHECK(y_dims_data[idx] == static_cast<DimType>(y));
              CHECK(z_dims_data[idx] == static_cast<DimType>(z));
              idx += 1;
            }
          }
        }
      }
    }
  }
}

/**
 * @brief Checks that the read query with row major layout returns the correct
 * results for a 2D array.
 *
 * @tparam AttrType The type of the attribute of the array.
 * @param global_a The attribute data in global order.
 * @param a_data_read The attribute data in row-major order.
 */
template <typename AttrType>
void check_2d_row_major(std::vector<AttrType>& global_a, std::vector<AttrType>& a_data_read) {
  REQUIRE(global_a.size() == a_data_read.size());

  size_t idx = 0;
  for (int tile_idx_x = BITSORT_DIM_LO; tile_idx_x <= BITSORT_DIM_HI; tile_idx_x += TILE_EXTENT) {
    for (int tile_idx_y = BITSORT_DIM_LO; tile_idx_y <= BITSORT_DIM_HI; tile_idx_y += TILE_EXTENT) {
      for (int x = tile_idx_x; x < tile_idx_x + TILE_EXTENT; ++x) {
        if (x > BITSORT_DIM_HI) {
          break;
        }

        for (int y = tile_idx_y; y < tile_idx_y + TILE_EXTENT; ++y) {
          if (y > BITSORT_DIM_HI) {
            break;
          }

          int row_index = ((x - BITSORT_DIM_LO) * (BITSORT_DIM_HI - BITSORT_DIM_LO + 1)) + (y - BITSORT_DIM_LO);
          CHECK(global_a[idx] == a_data_read[row_index]);
          idx += 1;
        }
      }
    }
  }
}

/**
 * @brief Checks that the read query with column major layout returns the correct
 * results for a 2D array.
 *
 * @tparam AttrType The type of the attribute of the array.
 * @param global_a The attribute data in global order.
 * @param a_data_read The attribute data in row-major order.
 */
template <typename AttrType>
void check_2d_col_major(std::vector<AttrType>& global_a, std::vector<AttrType>& a_data_read) {
  REQUIRE(global_a.size() == a_data_read.size());
}



/**
 * @brief Checks that the read query with row major layout returns the correct
 * results for a 3D array.
 *
 * @tparam AttrType The type of the attribute of the array.
 * @param global_a The attribute data in global order.
 * @param a_data_read The attribute data in row-major order.
 */
template <typename AttrType>
void check_3d_row_major(std::vector<AttrType>& global_a, std::vector<AttrType>& a_data_read) {
  REQUIRE(global_a.size() == a_data_read.size());
}

/**
 * @brief Checks that the read query with column major layout returns the correct
 * results for a 3D array.
 *
 * @tparam AttrType The type of the attribute of the array.
 * @param global_a The attribute data in global order.
 * @param a_data_read The attribute data in row-major order.
 */
template <typename AttrType>
void check_3d_col_major(std::vector<AttrType>& global_a, std::vector<AttrType>& a_data_read) {
  REQUIRE(global_a.size() == a_data_read.size());
}

/**
 * @brief Sets the subarrays to the entire array for a read query.
 * Including this so we can test different code paths.
 * 
 * @tparam DimType The type of the dimension of the array.
 * @param read_query The read query.
 * @param num_dims The number of dimensions.
 */
template<typename DimType>
void read_query_set_subarray(Query &read_query, int num_dims) {
  DimType dim_lo = static_cast<DimType>(BITSORT_DIM_LO);
  DimType dim_hi = static_cast<DimType>(BITSORT_DIM_HI);
  read_query.add_range("x", dim_lo, dim_hi);

  if (num_dims >= 2) {
    read_query.add_range("y", dim_lo, dim_hi);
  }

  if (num_dims == 3) {
     read_query.add_range("z", dim_lo, dim_hi);
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
 * @param set_subarray Whether the read query is called with a subarray encompassing the whole array.
 */
template <typename AttrType, typename DimType, typename AttributeDistribution>
void bitsort_filter_api_test(
    const std::string& bitsort_array_name,
    uint64_t num_dims,
    tiledb_layout_t write_layout,
    tiledb_layout_t read_layout,
    bool set_subarray) {
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
      {{static_cast<DimType>(BITSORT_DIM_LO), static_cast<DimType>(BITSORT_DIM_HI)}},
      static_cast<DimType>(TILE_EXTENT)));

  if (num_dims >= 2) {
    domain.add_dimension(Dimension::create<DimType>(
        ctx,
        "y",
        {{static_cast<DimType>(BITSORT_DIM_LO), static_cast<DimType>(BITSORT_DIM_HI)}},
        static_cast<DimType>(TILE_EXTENT)));
    number_elements *= num_element_per_dim;
  }
  if (num_dims == 3) {
    domain.add_dimension(Dimension::create<DimType>(
        ctx,
        "z",
        {{static_cast<DimType>(BITSORT_DIM_LO), static_cast<DimType>(BITSORT_DIM_HI)}},
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
  Array::create(bitsort_array_name, schema);

  // Setting up the random number generator for the bitsort filter testing.
  std::mt19937_64 gen(SEED);
  AttrType low = 0;
  if constexpr (std::is_signed<AttrType>::value) {
    low = std::numeric_limits<AttrType>::min();
  }
  AttributeDistribution dis(low, std::numeric_limits<AttrType>::max());

  std::vector<AttrType> a_write;
  std::vector<AttrType> global_a;
  for (size_t i = 0; i < number_elements; ++i) {
    AttrType f = static_cast<AttrType>(dis(gen));
    a_write.push_back(f);
    global_a.push_back(f);
  }

  Array array_w(ctx, bitsort_array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(write_layout).set_data_buffer("a", a_write);
  // Dimension buffers.
  auto&& [x_dims_data, y_dims_data, z_dims_data]{(num_dims == 1) ? set_1d_dim_buffers<DimType>() : ((num_dims == 2) ? set_2d_dim_buffers<DimType>() : set_3d_dim_buffers<DimType>())};

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

  if (read_layout == TILEDB_GLOBAL_ORDER || read_layout == TILEDB_UNORDERED || num_dims == 1) {
    for (size_t i = 0; i < number_elements; ++i) {
      CHECK(a_data_read[i] == global_a[i]);
    }
  } else if (read_layout == TILEDB_ROW_MAJOR) {
    // TODO: case on the number of dims
    check_2d_row_major(global_a, a_data_read);
  } else if (read_layout == TILEDB_COL_MAJOR) {
    // TODO: case on the number of dims
    check_2d_col_major(global_a, a_data_read);
  }

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

  // Check the attribute data.
  if (read_layout == TILEDB_GLOBAL_ORDER || read_layout == TILEDB_UNORDERED || num_dims == 1) {
    for (size_t i = 0; i < number_elements; ++i) {
      CHECK(a_data_read_dims[i] == global_a[i]);
    }
  } else if (read_layout == TILEDB_ROW_MAJOR) {
    // TODO: case on the number of dims
    check_2d_row_major(global_a, a_data_read_dims);
  } else if (read_layout == TILEDB_COL_MAJOR) {
    // TODO: case on the number of dims
    check_2d_col_major(global_a, a_data_read_dims);
  }

  // Check the dimension data.
  if (num_dims == 1) {
    check_1d_dim_buffer(x_dims_data_read);
  } else if (num_dims == 2) {
    if (read_layout == TILEDB_UNORDERED || read_layout == TILEDB_GLOBAL_ORDER) {
      check_2d_dim_buffers_global_read(x_dims_data_read, y_dims_data_read);
    } else if (read_layout == TILEDB_ROW_MAJOR) {
      check_2d_dim_buffers_row_read(x_dims_data_read, y_dims_data_read);
    } else if (read_layout == TILEDB_COL_MAJOR) {
      check_2d_dim_buffers_col_read(x_dims_data_read, y_dims_data_read);
    }
  } else if (num_dims == 3) {
    check_3d_dim_buffers(x_dims_data_read, y_dims_data_read, z_dims_data_read);
  }

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
 * @param set_subarray Whether the read query is called with a subarray encompassing the whole array.
 */
template <typename AttrType, typename AttributeDistribution>
void bitsort_filter_api_test(
    const std::string& bitsort_array_name,
    uint64_t num_dims,
    tiledb_layout_t write_layout,
    tiledb_layout_t read_layout,
    bool set_subarray) {
  bitsort_filter_api_test<AttrType, int16_t, AttributeDistribution>(
      bitsort_array_name, num_dims, write_layout, read_layout, set_subarray);
  bitsort_filter_api_test<AttrType, int8_t, AttributeDistribution>(
      bitsort_array_name, num_dims, write_layout, read_layout, set_subarray);
  bitsort_filter_api_test<AttrType, int32_t, AttributeDistribution>(
      bitsort_array_name, num_dims, write_layout, read_layout, set_subarray);
  bitsort_filter_api_test<AttrType, int64_t, AttributeDistribution>(
      bitsort_array_name, num_dims, write_layout, read_layout, set_subarray);
  bitsort_filter_api_test<AttrType, uint8_t, AttributeDistribution>(
      bitsort_array_name, num_dims, write_layout, read_layout, set_subarray);
  bitsort_filter_api_test<AttrType, uint16_t, AttributeDistribution>(
      bitsort_array_name, num_dims, write_layout, read_layout, set_subarray);
  bitsort_filter_api_test<AttrType, uint32_t, AttributeDistribution>(
      bitsort_array_name, num_dims, write_layout, read_layout, set_subarray);
  bitsort_filter_api_test<AttrType, uint64_t, AttributeDistribution>(
      bitsort_array_name, num_dims, write_layout, read_layout, set_subarray);
  bitsort_filter_api_test<AttrType, float, AttributeDistribution>(
      bitsort_array_name, num_dims, write_layout, read_layout, set_subarray);
  bitsort_filter_api_test<AttrType, double, AttributeDistribution>(
      bitsort_array_name, num_dims, write_layout, read_layout, set_subarray);
}

TEMPLATE_TEST_CASE(
    "Bitsort Filter CPP API Tests",
    "[cppapi][filter][bitsort][global]",
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
  // TODO: after debugging the other test, test row and column major here.
  tiledb_layout_t read_layout = GENERATE(TILEDB_GLOBAL_ORDER, TILEDB_UNORDERED);
  bool set_subarray = GENERATE(true, false);

  // Run tests.
  if constexpr (std::is_floating_point<TestType>::value) {
    bitsort_filter_api_test<TestType, FloatDistribution>(
        array_name, num_dims, write_layout, read_layout, set_subarray);
  } else if constexpr (std::is_unsigned<TestType>::value) {
    bitsort_filter_api_test<TestType, UnsignedIntDistribution>(
        array_name, num_dims, write_layout, read_layout, set_subarray);
  } else if constexpr (std::is_signed<TestType>::value) {
    bitsort_filter_api_test<TestType, IntDistribution>(
        array_name, num_dims, write_layout, read_layout, set_subarray);
  }
}