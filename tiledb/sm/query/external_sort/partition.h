/**
 * @file   tiledb/sm/query/external_sort/partition.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 */

#ifndef TILEDB_PARTITION_H
#define TILEDB_PARTITION_H

#include <cassert>
#include <list>
#include <tuple>
#include <vector>

/**
 * @brief Partition a list of sizes into bins that are less than or equal to a
 * given number of bytes.  The sizes are the number of elements in each cell,
 * which are assumed to be of type `char`.
 * @param bin_size The maximum number of bytes in a bin.
 * @param num_cells The total number of cells to be partitioned.
 * @param fixed_bytes_per_cell The number of fixed bytes per cell.  This
 * includes all of the non varlength elements in each cell, including the
 * elements that specify the sizes.
 * @param sizes The number of varlength elements in each cell.  These are
 * assumed to correspond to chars, so the number of bytes in each cell is
 * the same as the number of elements in the cell.
 * @return
 */
auto bin_partition(
    size_t bin_size,
    size_t num_cells,
    size_t fixed_bytes_per_cell,
    std::list<std::vector<uint64_t>::iterator>& sizes) {
  assert(bin_size > 0);
  assert(num_cells > 0);
  assert(fixed_bytes_per_cell > 0);

  size_t current_index{0};
  // size_t next_index{0};
  size_t current_size{0};
  size_t next_size{0};

  auto offsets_begin = sizes.begin();
  auto offsets_end = sizes.end();

  std::vector<uint64_t> bins{0};
  std::vector<uint64_t> bin_sizes;

  while (true) {
    next_size = current_size + fixed_bytes_per_cell;
    for (auto o = offsets_begin; o != offsets_end; ++o) {
      next_size += (*o)[current_index] * sizeof(char);
    }
    if (next_size > bin_size) {
      bins.push_back(current_index);
      bin_sizes.push_back(current_size);

      next_size = current_size = 0;
      continue;
    } else {
      current_size = next_size;
    }
    if (++current_index == num_cells) {
      bins.push_back(num_cells);
      bin_sizes.push_back(current_size);
      break;
    }
  }

  return std::make_tuple(std::move(bins), std::move(bin_sizes));
}

#endif  // TILEDB_PARTITION_H
