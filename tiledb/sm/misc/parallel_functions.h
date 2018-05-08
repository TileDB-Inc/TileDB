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

#ifdef HAVE_TBB
#include <tbb/concurrent_vector.h>
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
  std::vector<Status> result;
#ifdef HAVE_TBB
  tbb::concurrent_vector<Status> tbb_result;
  tbb::parallel_for_each(
      begin,
      end,
      [&tbb_result,
       &F](const typename std::iterator_traits<IterT>::value_type& elem) {
        tbb_result.push_back(F(elem));
      });
  result.insert(result.end(), tbb_result.begin(), tbb_result.end());
#else
  std::for_each(
      begin,
      end,
      [&result,
       &F](const typename std::iterator_traits<IterT>::value_type& elem) {
        result.push_back(F(elem));
      });
#endif
  return result;
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_PARALLEL_FUNCTIONS_H