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
#include <vector>

namespace tiledb::common {

class BountifulScheduler {
  std::vector<std::future<void>> futures;

  /**
   * Destructor.  Waits for all tasks to finish.
   */
 public:
  ~BountifulScheduler() {
    for (auto&& f : futures) {
      f.get();
    }
  }

  /**
   * You get a thread and you get a thread!  Every task gets a thread!
   *
   * Operates in conjunction with an asynchronous state machine policy (i.e., a
   * policy that does its own wait and notify).
   *
   * @tparam Schedulable The `Schedulable` is assumed to have a `start` function
   * that takes a `void` and returns a `void`.
   *
   * @param node The thing to be scheduled.
   *
   * @return A reference to the argument is returned.
   */
  template <class Schedulable>
  Schedulable* schedule(Schedulable& node) {
    futures.emplace_back(
        std::async(std::launch::async, [&node]() { node.start(); }));
    return &node;
  }

  template <class Schedulable>
  Schedulable* schedule(Schedulable&& node) {
    futures.emplace_back(
        std::async(std::launch::async, [&node]() { node.start(); }));
    return &node;
  }
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_BOUNTIFUL_H
