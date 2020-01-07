/**
 * @file   parallel_functions.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 TileDB, Inc.
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
 * This file defines several parallelized utility functions.
 */

#ifndef TILEDB_PARALLEL_FUNCTIONS_H
#define TILEDB_PARALLEL_FUNCTIONS_H

#include <algorithm>
#include <cassert>

#ifdef HAVE_TBB
#include <tbb/blocked_range2d.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_for_each.h>
#include <tbb/parallel_sort.h>
#endif

namespace tiledb {
namespace sm {

/**
 * Sort the given iterator range, possibly in parallel.
 *
 * @tparam IterT Iterator type
 * @tparam CmpT Comparator type
 * @param begin Beginning of range to sort (inclusive).
 * @param end End of range to sort (exclusive).
 * @param cmp Comparator.
 */
template <typename IterT, typename CmpT>
void parallel_sort(IterT begin, IterT end, CmpT cmp) {
#ifdef HAVE_TBB
  tbb::parallel_sort(begin, end, cmp);
#else
  std::sort(begin, end, cmp);
#endif
}

/**
 * Sort the given iterator range, possibly in parallel.
 *
 * @tparam IterT Iterator type
 * @param begin Beginning of range to sort (inclusive).
 * @param end End of range to sort (exclusive).
 */
template <typename IterT>
void parallel_sort(IterT begin, IterT end) {
#ifdef HAVE_TBB
  tbb::parallel_sort(begin, end);
#else
  std::sort(begin, end);
#endif
}

/**
 * Call the given function on each element in the given iterator range,
 * possibly in parallel.
 *
 * @tparam IterT Iterator type
 * @tparam FuncT Function type (returning Status).
 * @param begin Beginning of range to sort (inclusive).
 * @param end End of range to sort (exclusive).
 * @param F Function to call on each item
 * @return Vector of Status objects, one for each function invocation.
 */
template <typename IterT, typename FuncT>
std::vector<Status> parallel_for_each(IterT begin, IterT end, const FuncT& F) {
  auto niters = static_cast<uint64_t>(std::distance(begin, end));
  std::vector<Status> result(niters);
#ifdef HAVE_TBB
  tbb::parallel_for(uint64_t(0), niters, [begin, &result, &F](uint64_t i) {
    auto it = std::next(begin, i);
    result[i] = F(*it);
  });
#else
  for (uint64_t i = 0; i < niters; i++) {
    auto it = std::next(begin, i);
    result[i] = F(*it);
  }
#endif
  return result;
}

template <typename FuncT>
std::vector<Status> parallel_for(uint64_t begin, uint64_t end, const FuncT& F) {
  assert(begin <= end);
  uint64_t num_iters = end - begin + 1;
  std::vector<Status> result(num_iters);
#ifdef HAVE_TBB
  tbb::parallel_for(begin, end, [begin, &result, &F](uint64_t i) {
    result[i - begin] = F(i);
  });
#else
  for (uint64_t i = begin; i < end; i++) {
    result[i - begin] = F(i);
  }
#endif
  return result;
}

/**
 * Call the given function on every pair (i, j) in the given i and j ranges,
 * possibly in parallel.
 *
 * @tparam FuncT Function type (returning Status).
 * @param i0 Inclusive start of outer (rows) range.
 * @param i1 Exclusive end of outer range.
 * @param j0 Inclusive start of inner (cols) range.
 * @param j1 Exclusive end of inner range.
 * @param F Function to call on each (i, j) pair.
 * @return Vector of Status objects, one for each function invocation.
 */
template <typename FuncT>
std::vector<Status> parallel_for_2d(
    uint64_t i0, uint64_t i1, uint64_t j0, uint64_t j1, const FuncT& F) {
  assert(i0 <= i1);
  assert(j0 <= j1);
  const uint64_t num_i_iters = i1 - i0 + 1, num_j_iters = j1 - j0 + 1;
  const uint64_t num_iters = num_i_iters * num_j_iters;
  std::vector<Status> result(num_iters);
#ifdef HAVE_TBB
  auto range = tbb::blocked_range2d<uint64_t>(i0, i1, j0, j1);
  tbb::parallel_for(
      range,
      [i0, j0, num_j_iters, &result, &F](
          const tbb::blocked_range2d<uint64_t>& r) {
        const auto& rows = r.rows();
        const auto& cols = r.cols();
        for (uint64_t i = rows.begin(); i < rows.end(); i++) {
          for (uint64_t j = cols.begin(); j < cols.end(); j++) {
            uint64_t idx = (i - i0) * num_j_iters + (j - j0);
            result[idx] = F(i, j);
          }
        }
      });
#else
  for (uint64_t i = i0; i < i1; i++) {
    for (uint64_t j = j0; j < j1; j++) {
      uint64_t idx = (i - i0) * num_j_iters + (j - j0);
      result[idx] = F(i, j);
    }
  }
#endif
  return result;
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_PARALLEL_FUNCTIONS_H
