/**
 * @file   thread_pool.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file declares the ThreadPool class.
 */

#ifndef TILEDB_THREAD_POOL_H
#define TILEDB_THREAD_POOL_H

#include "producer_consumer_queue.hpp"

#include <functional>
#include <future>

#include "tiledb/common/macros.h"
#include "tiledb/common/status.h"

namespace tiledb::common {

class ThreadPool {

public:
  using Task = std::future<Status>;

  ThreadPool(size_t n = std::thread::hardware_concurrency()) {
    threads_.reserve(n);
    
    for (size_t i = 0; i < n; ++i) {
      std::thread tmp;
      
      size_t tries = 3;
      while (tries--) {
	try {
	  tmp = std::thread(&ThreadPool::worker, this);
	}
	catch (const std::system_error& e) {
	  if (e.code() != std::errc::resource_unavailable_try_again) {
	    throw;
	  }
	  continue;
	}
	break;
      }

      threads_.emplace_back(std::move(tmp));
    }
  }

  ~ThreadPool() { shutdown(); }

  template <class Fn, class... Args, class R = std::invoke_result_t<std::decay_t<Fn>, std::decay_t<Args>...>>
  std::shared_future<R> execute(const Fn& task, const Args&... args) {
    return async(task, args...);
  }

  template <class Fn, class... Args, class R = std::invoke_result_t<std::decay_t<Fn>, std::decay_t<Args>...>>
  std::shared_future<R> async(const Fn& task, const Args&... args) {

    std::shared_ptr<std::promise<R>> task_promise(new std::promise<R>);
    std::shared_future<R>            future = task_promise->get_future();

    task_queue_.push([task, args..., task_promise] {
      try {
        if constexpr (std::is_void_v<R>) {
          task(args...);
          task_promise->set_value();
        } else {
          task_promise->set_value(task(args...));
        }
      } catch (...) {
        try {
          task_promise->set_exception(std::current_exception());
        } catch (...) {
        }
      }
    });

    return future;
  }

  void worker() {
    while (true) {
      auto val = task_queue_.pop();
      if (val) {
        (*val)();
      } else {
        break;
      }
    }
  }

  void shutdown() {
    task_queue_.drain();
    for (auto&& t : threads_) {
      t.join();
    }
    threads_.clear();
  }

  


private:
  producer_consumer_queue<std::function<void()>> task_queue_;
  std::vector<std::thread>                       threads_;
  //  std::atomic<size_t>                            concurrency_level_;
};




}


#endif  // TILEDB_THREAD_POOL_H
