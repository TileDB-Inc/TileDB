/**
 * @file   parallel_functions.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_PARALLEL_FUNCTIONS_H
