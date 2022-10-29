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
#include <iostream>
#include <thread>
#include <vector>

namespace tiledb::common {



template <class Node>
class BountifulScheduler {
 public:
  BountifulScheduler() = default;
  ~BountifulScheduler() = default;

  std::vector<std::future<void>> futures_{};
  std::vector<std::function<void()>> tasks_{};

  template <class C = Node>
  void submit(Node& n, std::enable_if_t<std::is_same_v<decltype(n.resume()), void>, void*> = nullptr)  {
    tasks_.emplace_back([&n]() {

    auto mover = n.get_mover();
    if (mover->debug_enabled()) {
      std::cout << "producer starting run on " << n.get_mover()
                << std::endl;
    }

    while (!mover->is_stopping()) {
      n.resume();
    }
    });
  }

  template <class C = Node>
  void submit(Node&& n, std::enable_if_t<std::is_same_v<decltype(n.resume()), void>, void*> = nullptr) {
    futures_.emplace_back(std::async(std::launch::async, [&n]() { n.resume(); }));    
  }

  template <class C = Node>
  void submit(C& n, std::enable_if_t<(!std::is_same_v<decltype(n.resume()), void> && std::is_same_v<decltype(n.run()), void>), void*> = nullptr)  {
    futures_.emplace_back(std::async(std::launch::async, [&n]() { n.run(); }));
  }

  template <class C = Node>
  void submit(C& n, std::enable_if_t<(std::is_same_v<decltype(n.resume()), void> && std::is_same_v<decltype(n.run()), void>), void*> = nullptr)  {
    futures_.emplace_back(std::async(std::launch::async, [&n]() { n.run(); }));
  }


  auto sync_wait_all() {
    for (auto&& t : tasks_) {
      futures_.emplace_back(std::async(std::launch::async, t));
    }
    for (auto&& t : futures_) {
       t.get();
    }
  }
};






}  // namespace tiledb::common

#endif  // TILEDB_DAG_BOUNTIFUL_H
