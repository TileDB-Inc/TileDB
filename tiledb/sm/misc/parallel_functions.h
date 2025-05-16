/**
 * @file   parallel_functions.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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

#include "tiledb/common/assert.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/global_state/global_state.h"

#include <algorithm>
#include <cassert>

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * Sort the given iterator range, possibly in parallel.
 *
 * @tparam IterT Iterator type
 * @tparam CmpT Comparator type
 * @param tp The threadpool to use.
 * @param begin Beginning of range to sort (inclusive).
 * @param end End of range to sort (exclusive).
 * @param cmp Comparator.
 */
template <
    typename IterT,
    typename CmpT = std::less<typename std::iterator_traits<IterT>::value_type>>
void parallel_sort(
    ThreadPool* const tp, IterT begin, IterT end, const CmpT& cmp = CmpT()) {
  // Sort the range using a quicksort. The algorithm is:
  // 1. Pick a pivot value in the range.
  // 2. Re-order the range so that all values less than the
  //    pivot value are ordered left of the pivot's index.
  // 3. Recursively invoke step 1.
  //
  // To parallelize the algorithm, step #3 is modified to execute
  // the recursion on the thread pool.
  passert(tp);

  // Calculate the maximum height of the recursive call stack tree
  // where each leaf node can be assigned to a single level of
  // concurrency on the thread pool. The motiviation is to stop the
  // recursion when all concurrency levels are utilized. When all
  // concurrency levels have their own subrange to operate on, we
  // will stop the quicksort and invoke std::sort() to sort the
  // their subrange.
  uint64_t height = 1;
  uint64_t width = 1;
  while (width <= tp->concurrency_level()) {
    ++height;
    width = 2 * width;
  }

  // To fully utilize the all concurrency levels, increment
  // the target height of the call stack tree if there would
  // be idle concurrency levels.
  if (width > tp->concurrency_level()) {
    ++height;
  }

  // Define a work routine that encapsulates steps #1 - #3 in the
  // algorithm.
  std::function<Status(uint64_t, IterT, IterT)> quick_sort;
  quick_sort = [&](const uint64_t depth, IterT begin, IterT end) -> Status {
    const size_t elements = std::distance(begin, end);

    // Stop the recursion if this subrange does not contain
    // any elements to sort.
    if (elements <= 1) {
      return Status::Ok();
    }

    // If there are only two elements remaining, directly sort them.
    if (elements <= 2) {
      std::sort(begin, end, cmp);
      return Status::Ok();
    }

    // If we have reached the target height of the call stack tree,
    // we will defer sorting to std::sort() to finish sorting the
    // subrange. This is an optimization because all concurrency
    // levels in the threadpool should be utilized if the work was
    // evenly distributed among them.
    if (depth + 1 == height) {
      std::sort(begin, end, cmp);
      return Status::Ok();
    }

    // Step #1: Pick a pivot value in the range.
    auto pivot_iter = begin + (elements / 2);
    const auto pivot_value = *pivot_iter;
    if ((end - 1) != pivot_iter)
      std::iter_swap((end - 1), pivot_iter);

    // Step #2: Partition the subrange.
    auto middle = begin;
    for (auto iter = begin; iter != end - 1; ++iter) {
      if (cmp(*iter, pivot_value)) {
        std::iter_swap(middle, iter);
        ++middle;
      }
    }
    std::iter_swap(middle, (end - 1));

    // Step #3: Recursively sort the left and right partitions.
    std::vector<ThreadPool::Task> tasks;
    if (begin != middle) {
      std::function<Status()> quick_sort_left =
          std::bind(quick_sort, depth + 1, begin, middle);
      ThreadPool::Task left_task = tp->execute(std::move(quick_sort_left));
      tasks.emplace_back(std::move(left_task));
    }
    if (middle != end) {
      std::function<Status()> quick_sort_right =
          std::bind(quick_sort, depth + 1, middle + 1, end);
      ThreadPool::Task right_task = tp->execute(std::move(quick_sort_right));
      tasks.emplace_back(std::move(right_task));
    }

    // Wait for the sorted partitions.
    return tp->wait_all(tasks);
  };

  // Start the quicksort from the entire range.
  throw_if_not_ok(quick_sort(0, begin, end));
}

/**
 * Call the given function on each element in the given iterator range.
 *
 * @tparam IterT Iterator type
 * @tparam FuncT Function type (returning Status).
 * @param tp The threadpool to use.
 * @param begin Beginning of range (inclusive).
 * @param end End of range (exclusive).
 * @param F Function to call on each item
 * @return Status
 */
template <typename FuncT>
Status parallel_for(
    ThreadPool* const tp, uint64_t begin, uint64_t end, const FuncT& F) {
  iassert(begin <= end, "begin = {}, end = {}", begin, end);

  const uint64_t range_len = end - begin;
  if (range_len == 0)
    return Status::Ok();

  passert(tp);

  /*
   * Mutex protects atomicity of `failed*` local variables together. The first
   * subrange to fail determines the return or exception value.
   */
  std::mutex failed_mutex;
  /*
   * If we were checking this variable inside either the main loop or the one in
   * `execute_subrange`, then it would be better to use `atomic_bool` to lessen
   * the lock overhead on the mutex. As it is, we do not prematurely stop any
   * loop that is not the first to fail.
   */
  bool failed = false;
  optional<std::exception_ptr> failed_exception{nullopt};
  optional<Status> failed_status{nullopt};

  /*
   * Executes subrange [subrange_start, subrange_end) that exists within the
   * range [begin, end).
   *
   * If all the functions `F` return OK, then so does this function. If a
   * function fails but is not the first do so in this `parallel_for`
   * invocation, then this function returns OK. If a first function to fail
   * returns not OK, then this function returns that value. If a first function
   * to fail throws, then this function throws that value.
   */
  std::function<Status(uint64_t, uint64_t)> execute_subrange =
      [&failed, &failed_exception, &failed_status, &failed_mutex, &F](
          const uint64_t subrange_start,
          const uint64_t subrange_end) -> Status {
    for (uint64_t i = subrange_start; i < subrange_end; ++i) {
      Status st;
      try {
        st = F(i);
        if (st.ok()) {
          continue;
        }
        std::lock_guard<std::mutex> lock(failed_mutex);
        if (!failed) {
          failed_status = st;
          failed = true;
          return st;
        }
      } catch (...) {
        std::lock_guard<std::mutex> lock(failed_mutex);
        if (!failed) {
          auto ce{std::current_exception()};
          failed_exception = ce;
          failed = true;
          std::rethrow_exception(ce);
        }
      }
      /*
       * If we reach this line, then either the status was not OK or `F` threw.
       * Now you'd think that we'd do something other than continue the loop in
       * this case, like `break` and end the function. Nope. That's not the
       * legacy behavior of this function. Nor is checking failure status in the
       * loop that kicks off this function. Regardless, we are leaving the
       * behavior exactly the same for now.
       */
    }
    return Status{};
  };

  // Calculate the length of the subrange that each thread will
  // be responsible for.
  const uint64_t concurrency_level = tp->concurrency_level();
  const uint64_t subrange_len = range_len / concurrency_level;
  const uint64_t subrange_len_carry = range_len % concurrency_level;

  // Execute a bound instance of `execute_subrange` for each
  // subrange on the global thread pool.
  uint64_t fn_iter = 0;
  std::vector<ThreadPool::Task> tasks;
  tasks.reserve(concurrency_level);
  for (size_t i = 0; i < concurrency_level; ++i) {
    const uint64_t task_subrange_len =
        subrange_len + ((i < subrange_len_carry) ? 1 : 0);

    if (task_subrange_len == 0)
      break;

    const uint64_t subrange_start = begin + fn_iter;
    const uint64_t subrange_end = begin + fn_iter + task_subrange_len;
    std::function<Status()> bound_fn =
        std::bind(execute_subrange, subrange_start, subrange_end);
    tasks.emplace_back(tp->execute(std::move(bound_fn)));

    fn_iter += task_subrange_len;
  }

  // Wait for all instances of `execute_subrange` to complete.
  // This is ignoring the wait status as we use failed_exception for propagating
  // the tasks exceptions.
  (void)tp->wait_all(tasks);

  if (failed_exception.has_value()) {
    std::rethrow_exception(failed_exception.value());
  }
  if (failed_status.has_value()) {
    return failed_status.value();
  }
  return Status{};  // otherwise return OK
}

/**
 * Call the given function on every pair (i, j) in the given i and j ranges,
 * possibly in parallel.
 *
 * @tparam FuncT Function type (returning Status).
 * @param tp The threadpool to use.
 * @param i0 Inclusive start of outer (rows) range.
 * @param i1 Exclusive end of outer range.
 * @param j0 Inclusive start of inner (cols) range.
 * @param j1 Exclusive end of inner range.
 * @param F Function to call on each (i, j) pair.
 * @return Status
 */
template <typename FuncT>
Status parallel_for_2d(
    ThreadPool* const tp,
    uint64_t i0,
    uint64_t i1,
    uint64_t j0,
    uint64_t j1,
    const FuncT& F) {
  passert(i0 <= i1);
  passert(j0 <= j1);

  passert(tp);

  const uint64_t range_len_i = i1 - i0;
  const uint64_t range_len_j = j1 - j0;

  if (range_len_i == 0 || range_len_j == 0)
    return Status::Ok();

  bool failed = false;
  Status return_st = Status::Ok();
  std::mutex return_st_mutex;

  // Calculate the length of the subrange-i and subrange-j that
  // each thread will be responsible for.
  const uint64_t concurrency_level = tp->concurrency_level();
  const uint64_t subrange_len_i = range_len_i / concurrency_level;
  const uint64_t subrange_len_i_carry = range_len_i % concurrency_level;
  const uint64_t subrange_len_j = range_len_j / concurrency_level;
  const uint64_t subrange_len_j_carry = range_len_j % concurrency_level;

  // Executes subarray [begin_i, end_i) x [start_j, end_j) within the
  // array [i0, i1) x [j0, j1).
  std::function<Status(uint64_t, uint64_t, uint64_t, uint64_t)>
      execute_subrange_ij = [&failed, &return_st, &return_st_mutex, &F](
                                const uint64_t begin_i,
                                const uint64_t end_i,
                                const uint64_t start_j,
                                const uint64_t end_j) -> Status {
    for (uint64_t i = begin_i; i < end_i; ++i) {
      for (uint64_t j = start_j; j < end_j; ++j) {
        const Status st = F(i, j);
        if (!st.ok() && !failed) {
          failed = true;
          std::lock_guard<std::mutex> lock(return_st_mutex);
          return_st = st;
        }
      }
    }

    return Status::Ok();
  };

  // Calculate the subranges for each dimension, i and j.
  std::vector<std::pair<uint64_t, uint64_t>> subranges_i;
  std::vector<std::pair<uint64_t, uint64_t>> subranges_j;
  uint64_t iter_i = 0;
  uint64_t iter_j = 0;
  for (size_t t = 0; t < concurrency_level; ++t) {
    const uint64_t task_subrange_len_i =
        subrange_len_i + ((t < subrange_len_i_carry) ? 1 : 0);
    const uint64_t task_subrange_len_j =
        subrange_len_j + ((t < subrange_len_j_carry) ? 1 : 0);

    if (task_subrange_len_i == 0 && task_subrange_len_j == 0)
      break;

    if (task_subrange_len_i > 0) {
      const uint64_t begin_i = i0 + iter_i;
      const uint64_t end_i = i0 + iter_i + task_subrange_len_i;
      subranges_i.emplace_back(begin_i, end_i);
      iter_i += task_subrange_len_i;
    }

    if (task_subrange_len_j > 0) {
      const uint64_t start_j = j0 + iter_j;
      const uint64_t end_j = j0 + iter_j + task_subrange_len_j;
      subranges_j.emplace_back(start_j, end_j);
      iter_j += task_subrange_len_j;
    }
  }

  // Execute a bound instance of `execute_subrange_ij` for each
  // 2D subarray on the global thread pool.
  std::vector<ThreadPool::Task> tasks;
  tasks.reserve(concurrency_level * concurrency_level);
  for (const auto& subrange_i : subranges_i) {
    for (const auto& subrange_j : subranges_j) {
      std::function<Status()> bound_fn = std::bind(
          execute_subrange_ij,
          subrange_i.first,
          subrange_i.second,
          subrange_j.first,
          subrange_j.second);
      tasks.emplace_back(tp->execute(std::move(bound_fn)));
    }
  }

  // Wait for all instances of `execute_subrange` to complete.
  auto wait_status = tp->wait_all(tasks);
  if (!wait_status.ok())
    return wait_status;
  return return_st;
}

}  // namespace tiledb::sm

#endif  // TILEDB_PARALLEL_FUNCTIONS_H
