/**
 * @file   tiledb_sparse_write.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Loads a CSV file FILENAME into the 3d array ARRAYNAME. Each line in the
 * CSV file must be of the form:
 *
 * <coord_1> <coord_2> <coord_3> <attribute_value>
 *
 * where each value in the line is space-delimited. Note that this examples
 * is pretty basic and no correctness checks are performed.
 *
 * The CSV file can be arbitrarily large, but only TILEDB_CELL_NUM cells
 * are loaded at a time, creating each time a new fragment. No particular
 * cell ordering is assumed (the cells can be random).
 */

#include "tiledb.h"

#include <fstream>
#include <iostream>
#include <sstream>

#define CELL_NUM 10000000
#define DIM_NUM 3
#define FILENAME "file.csv"
#define ARRAYNAME "3d_sparse_array"

// Populates buffers CELL_NUM at a time and returns the number of cells read
uint64_t populate_buffers(std::ifstream& file, int64_t* coords, int* a1);

int main() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Prepare buffers
  auto coords = new int64_t[CELL_NUM * DIM_NUM];
  auto a1 = new int[CELL_NUM];

  // Prepare file
  std::ifstream file(FILENAME);

  // Populate buffers and submit query until CSV file is read
  uint64_t cell_num;
  while ((cell_num = populate_buffers(file, coords, a1)) != 0) {
    // Prepare input buffers
    void* buffers[] = {a1, coords};
    uint64_t buffer_sizes[] = {cell_num * sizeof(int),
                               cell_num * DIM_NUM * sizeof(int64_t)};

    // Create query
    tiledb_query_t* query;
    tiledb_query_create(
        ctx,
        &query,
        ARRAYNAME,
        TILEDB_WRITE,
        TILEDB_UNORDERED,
        nullptr,
        nullptr,
        0,
        buffers,
        buffer_sizes);

    // Submit query
    tiledb_query_submit(ctx, query);
    tiledb_query_free(ctx, query);
  }

  // Clean up
  tiledb_ctx_free(ctx);
  file.close();
  delete[] coords;
  delete[] a1;

  return 0;
}

uint64_t populate_buffers(std::ifstream& file, int64_t* coords, int* a1) {
  std::string line_str;
  uint64_t cell_num = 0;

  while (!file.eof() && cell_num <= CELL_NUM) {
    std::getline(file, line_str);
    if (line_str.empty())
      break;
    std::stringstream line(line_str);
    for (int i = 0; i < 3; ++i)
      line >> coords[3 * cell_num + i];
    line >> a1[cell_num];
    ++cell_num;
  }

  return cell_num;
}
