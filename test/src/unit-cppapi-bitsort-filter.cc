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
#include <vector>

#include "catch.hpp"
#include "tiledb/common/common.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

std::string bitsort_array_name = "cpp_unit_array";
int bitsort_dim_hi = 10;
int tile_extent = 4;

// Defining distribution parameters.
typedef typename std::uniform_int_distribution<uint64_t> UnsignedIntDistribution;
typedef typename std::uniform_int_distribution<int64_t> IntDistribution;
typedef typename std::uniform_real_distribution<double> FloatDistribution;

/**
 * @brief Set the buffer with the appropriate dimensions for a 1D (10x) array.
 * 
 * @tparam T The type of the dimension.
 * @param x_dims The buffer that the function sets. 
 */
template<typename T>
void set_1d_dim_buffers(std::vector<T> &x_dims) {
  for (int x = 0; x < bitsort_dim_hi; ++x) {
    x_dims.push_back(static_cast<T>(x + 1));
  }
}

/**
 * @brief  Set the buffer with the appropriate dimensions for a 2D (10x10) array.
 * 
 * @tparam T The type of the dimension.
 * @param x_dims The first buffer that the function sets. 
 * @param y_dims The second buffer that the function sets.
 */
template<typename T>
void set_2d_dim_buffers(std::vector<T> &x_dims, std::vector<T> &y_dims) {
  for (int x_tile = 0; x_tile < bitsort_dim_hi; x_tile += tile_extent) {
    for (int y_tile = 0; y_tile < bitsort_dim_hi; y_tile += tile_extent) {
        for (int x = x_tile; x < x_tile + tile_extent; ++x) {
          if (x >= bitsort_dim_hi) {
            break;
          }
          for (int y = y_tile; y < y_tile + tile_extent; ++y) {
            if (y >= bitsort_dim_hi) {
              break;
            }
              x_dims.push_back(static_cast<T>(x + 1));
              y_dims.push_back(static_cast<T>(y + 1));
          }
        }
    }
  }
}

/**
 * @brief Set the buffer with the appropriate dimensions for a 3D (10x10x10) array.
 * 
 * @tparam T The type of the dimension.
 * @param x_dims The first buffer that the function sets. 
 * @param y_dims The second buffer that the function sets.
 * @param z_dims The third buffer that the function sets.
 */
template<typename T>
void set_3d_dim_buffers(std::vector<T> &x_dims, std::vector<T> &y_dims, std::vector<T> &z_dims) {
  for (int x_tile = 0; x_tile < bitsort_dim_hi; x_tile += tile_extent) {
    for (int y_tile = 0; y_tile < bitsort_dim_hi; y_tile += tile_extent) {
      for (int z_tile = 0; z_tile < bitsort_dim_hi; z_tile += tile_extent) {
        for (int x = x_tile; x < x_tile + tile_extent; ++x) {
          if (x >= bitsort_dim_hi) {
            break;
          }
          for (int y = y_tile; y < y_tile + tile_extent; ++y) {
            if (y >= bitsort_dim_hi) {
              break;
            }
            for (int z = z_tile; z < z_tile + tile_extent; ++z) {
              if (z >= bitsort_dim_hi) {
                break;
              }

              x_dims.push_back(static_cast<T>(x + 1));
              y_dims.push_back(static_cast<T>(y + 1));
              z_dims.push_back(static_cast<T>(z + 1));
            }
          }
        }
      }
    }
  }
}

/**
 * @brief Check the dimension buffer to ensure it has the appropriate dimensions for a 1D (10x) array.
 * 
 * @tparam T The type of the dimension.
 * @param x_dims The buffer that this function checks.
 */
template<typename T>
void check_1d_dim_buffers(const std::vector<T> &x_dims) {
  for (int x = 0; x < bitsort_dim_hi; ++x) {
   CHECK(x_dims[x] == static_cast<T>(x+1));
  }
}

/**
 * @brief  Check the dimension buffer to ensure it has the appropriate dimensions for a 2D (10x10) array.
 * 
 * @tparam T The type of the dimension.
 * @param x_dims The first buffer that the function checks. 
 * @param y_dims The second buffer that the function checks.
 */
template<typename T>
void check_2d_dim_buffers(const std::vector<T> &x_dims, const std::vector<T> &y_dims) {
  size_t x_index = 0;
  size_t y_index = 0;
  for (int x_tile = 0; x_tile < bitsort_dim_hi; x_tile += tile_extent) {
    for (int y_tile = 0; y_tile < bitsort_dim_hi; y_tile += tile_extent) {
        for (int x = x_tile; x < x_tile + tile_extent; ++x) {
          if (x >= bitsort_dim_hi) {
            break;
          }
          for (int y = y_tile; y < y_tile + tile_extent; ++y) {
            if (y >= bitsort_dim_hi) {
              break;
            }

            CHECK(x_dims[x_index++] == static_cast<T>(x+1));
              CHECK(y_dims[y_index++] == static_cast<T>(y+1));
          }
        }
    }
  }
}

/**
 * @brief Check the dimension buffer to ensure it has the appropriate dimensions for a 3D (10x10x10) array.
 * 
 * @tparam T The type of the dimension.
 * @param x_dims The first buffer that the function sets. 
 * @param y_dims The second buffer that the function sets.
 * @param z_dims The third buffer that the function sets.
 */
template<typename T>
void check_3d_dim_buffers(const std::vector<T> &x_dims, const std::vector<T> &y_dims, const std::vector<T> &z_dims) {
  size_t x_index = 0;
  size_t y_index = 0;
  size_t z_index = 0;
  for (int x_tile = 0; x_tile < bitsort_dim_hi; x_tile += tile_extent) {
    for (int y_tile = 0; y_tile < bitsort_dim_hi; y_tile += tile_extent) {
      for (int z_tile = 0; z_tile < bitsort_dim_hi; z_tile += tile_extent) {
        for (int x = x_tile; x < x_tile + tile_extent; ++x) {
          if (x >= bitsort_dim_hi) {
            break;
          }
          for (int y = y_tile; y < y_tile + tile_extent; ++y) {
            if (y >= bitsort_dim_hi) {
              break;
            }
            for (int z = z_tile; z < z_tile + tile_extent; ++z) {
              if (z >= bitsort_dim_hi) {
                break;
              }

              CHECK(x_dims[x_index++] == static_cast<T>(x+1));
              CHECK(y_dims[y_index++] == static_cast<T>(y+1));
              CHECK(z_dims[z_index++] == static_cast<T>(z+1));
            }
          }
        }
      }
    }
  }
}

/**
 * @brief Checks that the read query with row major layout returns the correct
 * results for a 2D (10x10) array.
 * 
 * @tparam T The type of the attribute of the array.
 * @param global_a The attribute data in global order.
 * @param a_data_read The attribute data in row-major order.
 */
template<typename T>
void check_2d_row_major(std::vector<T> &global_a, std::vector<T> &a_data_read) {
  REQUIRE(global_a.size() == a_data_read.size());
  size_t global_a_index = 0;
  for (int x_tile = 0; x_tile < bitsort_dim_hi; x_tile += tile_extent) {
    for (int y_tile = 0; y_tile < bitsort_dim_hi; y_tile += tile_extent) {
        for (int x = x_tile; x < x_tile + tile_extent; ++x) {
          if (x >= bitsort_dim_hi) {
            break;
          }
          for (int y = y_tile; y < y_tile + tile_extent; ++y) {
            if (y >= bitsort_dim_hi) {
              break;
            }

            int index = (x * bitsort_dim_hi) + y;
            CHECK(global_a[global_a_index] == a_data_read[index]);
            global_a_index += 1;
          }
        }
    }
  }
}

/**
 * @brief Checks that the read query with row major layout returns the correct
 * results for a 3D (10x10x10) array.
 * 
 * @tparam T The type of the attribute of the array.
 * @param global_a The attribute data in global order.
 * @param a_data_read The attribute data in row-major order.
 */
template<typename T>
void check_3d_row_major(std::vector<T> &global_a, std::vector<T> &a_data_read) {
  size_t global_a_index = 0;
  for (int x_tile = 0; x_tile < bitsort_dim_hi; x_tile += tile_extent) {
    for (int y_tile = 0; y_tile < bitsort_dim_hi; y_tile += tile_extent) {
      for (int z_tile = 0; z_tile < bitsort_dim_hi; z_tile += tile_extent) {
        for (int x = x_tile; x < x_tile + tile_extent; ++x) {
          if (x >= bitsort_dim_hi) {
            break;
          }
          for (int y = y_tile; y < y_tile + tile_extent; ++y) {
            if (y >= bitsort_dim_hi) {
              break;
            }
            for (int z = z_tile; z < z_tile + tile_extent; ++z) {
              if (z >= bitsort_dim_hi) {
                break;
              }

              int index = ((x * bitsort_dim_hi + y) * bitsort_dim_hi) + z;
              CHECK(global_a[global_a_index] == a_data_read[index]);
              global_a_index += 1;
            }
          }
        }
      }
    }
  }
}

/**
 * @brief Tests the bitsort filter, given the parameters passed in.
 * 
 * @tparam T The type of the attribute data in the array.
 * @tparam W The type of the dimensions of the array.
 * @tparam AttributeDistribution The type of the random distribution used to generate the attribute data.
 * @param ctx The context object.
 * @param vfs The VFS object.
 * @param num_dims The number of dimensions.
 * @param write_layout The layout of the write query.
 * @param read_layout The layout of the read query.
 */
template <typename T, typename W, typename AttributeDistribution>
void bitsort_filter_api_test(Context& ctx, VFS &vfs, uint64_t num_dims, tiledb_layout_t write_layout, tiledb_layout_t read_layout) {
  // Setup.
  if (vfs.is_dir(bitsort_array_name))
    vfs.remove_dir(bitsort_array_name);

  Domain domain(ctx);

  // Add first dimension.
  REQUIRE(num_dims >= 1);
  REQUIRE(num_dims <= 3);
  size_t number_elements = bitsort_dim_hi;
  domain.add_dimension(Dimension::create<W>(ctx, "x", {{static_cast<W>(1), static_cast<W>(bitsort_dim_hi)}}, static_cast<W>(4)));

  if (num_dims >= 2) {
    domain.add_dimension(Dimension::create<W>(ctx, "y", {{static_cast<W>(1), static_cast<W>(bitsort_dim_hi)}}, static_cast<W>(4)));
    number_elements *= bitsort_dim_hi;
  }
  if (num_dims == 3) {
    domain.add_dimension(Dimension::create<W>(ctx, "z", {{static_cast<W>(1), static_cast<W>(bitsort_dim_hi)}}, static_cast<W>(4)));
    number_elements *= bitsort_dim_hi;
  }

  Filter f(ctx, TILEDB_FILTER_BITSORT);
  FilterList filters(ctx);
  filters.add_filter(f);

  auto a = Attribute::create<T>(ctx, "a");
  a.set_filter_list(filters);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a);
  Array::create(bitsort_array_name, schema);

  // Setting up the random number generator for the bitsort filter testing.
  std::mt19937_64 gen(0xADA65ED6);
  T low = 0;
  if constexpr (std::is_signed<T>::value) {
    low = std::numeric_limits<T>::min();
  }
  AttributeDistribution dis(low, std::numeric_limits<T>::max());

  std::vector<T> a_write;
  std::vector<T> global_a;
  for (size_t i = 0; i < number_elements; ++i) {
    T f = static_cast<T>(dis(gen));
    a_write.push_back(f);
    global_a.push_back(f);
  }

  Array array_w(ctx, bitsort_array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(write_layout).set_data_buffer("a", a_write);
  // Dimension buffers.
  std::vector<W> x_dims;
  std::vector<W> y_dims;
  std::vector<W> z_dims;

  if (num_dims == 1) {
    set_1d_dim_buffers(x_dims);
  } else if (num_dims == 2) {
    set_2d_dim_buffers(x_dims, y_dims);
  } else {
    set_3d_dim_buffers(x_dims, y_dims, z_dims);
  }

  // Setting data buffers.
  query_w.set_data_buffer("x", x_dims);
  
  if (num_dims >= 2) {
    query_w.set_data_buffer("y", y_dims);
  } 
  if (num_dims == 3) {
    query_w.set_data_buffer("z", z_dims);
  }

  query_w.submit();
  query_w.finalize();
  array_w.close();

  // Open and read the entire array.
  std::vector<T> a_data_read(number_elements, 0);
  Array array_r(ctx, bitsort_array_name, TILEDB_READ);
  Query query_r(ctx, array_r);
  query_r.set_data_buffer("a", a_data_read);
  query_r.set_layout(read_layout);
  query_r.submit();

  // Check for results.
  auto table = query_r.result_buffer_elements();
  REQUIRE(table.size() == 1);
  REQUIRE(table["a"].first == 0);
  REQUIRE(table["a"].second == number_elements);
  
  for (size_t i = 0; i < number_elements; ++i) {
    CHECK(a_data_read[i] == global_a[i]);
  }

  query_r.finalize();
  array_r.close();

#ifdef BITSORT_DIMS_READ
  // Set a query where we read the dimensions back.
  std::vector<T> a_data_read_dims(number_elements, 0);
  std::vector<W> x_dims_read(number_elements, 0);
  std::vector<W> y_dims_read(number_elements, 0);
  std::vector<W> z_dims_read(number_elements, 0);

  Array array_r_dims(ctx, bitsort_array_name, TILEDB_READ);
  Query query_r_dims(ctx, array_r_dims);
  query_r_dims.set_data_buffer("a", a_data_read_dims);
  query_r_dims.set_layout(read_layout);

  query_r_dims.set_data_buffer("x", x_dims_read);
  
  if (num_dims >= 2) {
    query_r_dims.set_data_buffer("y", y_dims_read);
  } 
  if (num_dims == 3) {
    query_r_dims.set_data_buffer("z", z_dims_read);
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
  
  for (size_t i = 0; i < number_elements; ++i) {
    CHECK(a_data_read_dims[i] == global_a[i]);
  }

  // Check the dimension data.
  if (num_dims == 1) {
    check_1d_dim_buffers(x_dims_read);
  } else if (num_dims == 2) {
    check_2d_dim_buffers(x_dims_read, y_dims_read);
  } else if (num_dims == 3) {
    check_3d_dim_buffers(x_dims_read, y_dims_read, z_dims_read);
  }

  /// TODO: Add tests for row and column major reads.

  query_r_dims.finalize();
  array_r_dims.close();
#endif

  // Teardown.
  if (vfs.is_dir(bitsort_array_name))
    vfs.remove_dir(bitsort_array_name);
}

TEMPLATE_TEST_CASE("Seeing if templated dims works", "[cppapi][filter][bitsort][whee]", int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t, float, double) {
  // Setup.
  Context ctx;
  VFS vfs(ctx);

  // Generate parameters.
  uint64_t num_dims = GENERATE(1, 2, 3);
  tiledb_layout_t write_layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);
  tiledb_layout_t read_layout = TILEDB_GLOBAL_ORDER;

  // Run tests.
  if constexpr (std::is_floating_point<TestType>::value) {
    bitsort_filter_api_test<TestType, int16_t, FloatDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, int8_t, FloatDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, int32_t, FloatDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, int64_t, FloatDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, uint8_t, FloatDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, uint16_t, FloatDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, uint32_t, FloatDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, uint64_t, FloatDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, float, FloatDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, double, FloatDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
  } else if constexpr (std::is_unsigned<TestType>::value) {
    bitsort_filter_api_test<TestType, int8_t, UnsignedIntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, int16_t, UnsignedIntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, int32_t, UnsignedIntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, int64_t, UnsignedIntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, uint8_t, UnsignedIntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, uint16_t, UnsignedIntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, uint32_t, UnsignedIntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, uint64_t, UnsignedIntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, float, UnsignedIntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, double, UnsignedIntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
  } else if constexpr (std::is_signed<TestType>::value) {
    bitsort_filter_api_test<TestType, int8_t, IntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, int16_t, IntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, int32_t, IntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, int64_t, IntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, uint8_t, IntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, uint16_t, IntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, uint32_t, IntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, uint64_t, IntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, float, IntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
    bitsort_filter_api_test<TestType, double, IntDistribution>(ctx, vfs, num_dims, write_layout, read_layout);
  }
}