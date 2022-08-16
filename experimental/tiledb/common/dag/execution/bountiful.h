/**
 * @file bountiful.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file defines the "bountiful" thread pool for dag. It launches every
 * scheduled job on its own thread, using `std::async`.  The retuen futures are
 * saved.  When the scheduler destructor is called, all tasks are waited on
 * (using future::get until they complete).
 *
 * This scheduler is assumed to be used in conjunction with an AsyncPolicy (a
 * policy that does its own synchronization).
 */

#ifndef TILEDB_DAG_BOUNTIFUL_H
#define TILEDB_DAG_BOUNTIFUL_H

#include <future>
#include <thread>
#include <vector>

namespace tiledb::common {

class BountifulScheduler {
 public:
  BountifulScheduler() = default;
  ~BountifulScheduler() = default;

  template <class Fn>
  auto async(Fn&& f) {
    static_assert(std::is_invocable_v<Fn>);
    return std::async(std::launch::async, std::forward<Fn>(f));
  }

  template <class Fn>
  auto async(std::vector<Fn>&& f) {
    std::vector<std::future<void>> futures;
    futures.reserve(size(f));
    for (auto&& t : f) {
      futures.emplace_back(std::async(std::launch::async, std::forward<Fn>(t)));
    }
    return futures;
  }
};

template <class Fn>
auto async(BountifulScheduler& sch, Fn&& f) {
  return sch.async(std::forward<Fn>(f));
}

template <class R>
auto sync_wait(std::future<R>& task) {
  return task.get();
}

template <class R>
auto sync_wait(std::future<R>&& task) {
  return std::forward<std::future<R>>(task).get();
}

template <class R>
auto sync_wait_all(std::vector<std::future<R>>&& tasks) {
  std::vector<R> results;
  for (auto&& t : tasks) {
    results.emplace_back(t);
  }
  return results;
}

}  // namespace tiledb::common

#endif  // TILEDB_DAG_BOUNTIFUL_H
