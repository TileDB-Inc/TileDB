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

#include <iostream>
#include <random>
#include <vector>

#include "catch.hpp"
#include "tiledb/common/common.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

std::string bitsort_array_name = "cpp_unit_array";
int bitsort_dim_hi = 10;
int tile_extent = 4;

typedef typename std::uniform_int_distribution<uint64_t> UnsignedIntDistribution;
typedef typename std::uniform_int_distribution<int64_t> IntDistribution;
typedef typename std::uniform_real_distribution<double> FloatDistribution;

template<typename T>
void set_1d_dim_buffers(std::vector<T> &x_dims) {
  for (int x = 0; x < bitsort_dim_hi; ++x) {
    x_dims.push_back(static_cast<T>(x + 1));
  }
}

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

// t is the attribute type, w is the dimension type (TODO: add into comment)
template <typename T, typename W, typename AttributeDistribution>
void bitsort_filter_api_test(Context& ctx, uint64_t num_dims) {
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
  AttributeDistribution dis(
      std::numeric_limits<T>::min(), std::numeric_limits<T>::max());

  std::vector<T> a_write;
  std::vector<T> global_a;
  for (size_t i = 0; i < number_elements; ++i) {
    T f = static_cast<T>(dis(gen));
    a_write.push_back(f);
    global_a.push_back(f);
  }

  tiledb_layout_t layout_type = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);

  Array array_w(ctx, bitsort_array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(layout_type).set_data_buffer("a", a_write);
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

/*
  SECTION("Global order") {
    auto read_layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);
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
  }
  */

  SECTION("Row major order") {
    query_r.set_layout(TILEDB_ROW_MAJOR);
    query_r.submit();

    // Check for results.
    auto table = query_r.result_buffer_elements();
    REQUIRE(table.size() == 1);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == number_elements);

    if (num_dims == 1) {
      for (size_t i = 0; i < number_elements; ++i) {
        CHECK(a_data_read[i] == global_a[i]);
      }
    } else if (num_dims == 2) {
        check_2d_row_major(global_a, a_data_read);
    }
  }  

  /// TODO: column major.

  query_r.finalize();
  array_r.close();
}

TEST_CASE("Seeing if templated dims works", "[cppapi][filter][bitsort][whee]") {
  // Setup.
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(bitsort_array_name))
    vfs.remove_dir(bitsort_array_name);

  SECTION("2D") {
    bitsort_filter_api_test<int, float, IntDistribution>(ctx, 2);
  }

  /// TODO: add templating + 1d 3d.

  // Teardown.
  if (vfs.is_dir(bitsort_array_name))
    vfs.remove_dir(bitsort_array_name);
}