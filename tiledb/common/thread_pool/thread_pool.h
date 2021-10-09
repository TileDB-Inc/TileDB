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

#include "tiledb/common/logger.h"
#include "tiledb/common/macros.h"
#include "tiledb/common/status.h"


namespace tiledb::common {

class ThreadPool {

public:
  using Task = std::shared_future<Status>;

  ThreadPool() : concurrency_level_(0) {}
  
  // There shouldn't be an uninitalized threadpool (IMO)
  explicit ThreadPool(size_t n) {
    init(n);
  }

  Status init(size_t n = std::thread::hardware_concurrency()) {
    if (n == 0) {
      return Status::ThreadPoolError("Unable to initialize a thread pool with a concurrency level of 0.");
    }
    
    Status st = Status::Ok();

    threads_.reserve(n);
    
    for (size_t i = 0; i < n; ++i) {
      std::thread tmp;
      
      // Three shall be the maximum number of retries and the maximum number of retries shall be three.
      // Try to launch a thread running the worker() function
      // If we get resources_unvailable_try_again error, then try again
      size_t tries = 3;
      while (tries--) {
	try {
	  tmp = std::thread(&ThreadPool::worker, this);
	}
	catch (const std::system_error& e) {
	  if (e.code() != std::errc::resource_unavailable_try_again || tries == 0) {
	    st = Status::ThreadPoolError("Error initializing thread pool of concurrency level " +
					 std::to_string(concurrency_level_) + "; " + e.what());
	    LOG_STATUS(st);
	    break;
	  }
	  continue;
	}
	break;
      }
      if (!st.ok()) {
	shutdown();
	return st;
      }
      
      threads_.emplace_back(std::move(tmp));
    }
    concurrency_level_ = n;

    return st;
  }

  ~ThreadPool() { shutdown(); }

  // Alias for async
  template <class Fn, class... Args, class R = std::invoke_result_t<std::decay_t<Fn>, std::decay_t<Args>...>>
  std::shared_future<R> execute(const Fn& task, const Args&... args) {
    return async(task, args...);
  }


  // Launch an async task, returning a future
  template <class Fn, class... Args, class R = std::invoke_result_t<std::decay_t<Fn>, std::decay_t<Args>...>>
  std::shared_future<R> async(const Fn& task, const Args&... args) {
    
    if (concurrency_level_ == 0) {
      Task invalid_future;
      LOG_ERROR("Cannot execute task; thread pool uninitialized.");
      return invalid_future;
    }
    
    std::shared_ptr<std::promise<R>> task_promise(new std::promise<R>);
    std::shared_future<R>            future = task_promise->get_future();
    
    task_queue_.push([task, args..., task_promise] {
      try {

	// Separately handle the case of future<void>
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

  Status wait_all(std::vector<Task>& tasks) {
    auto statuses = wait_all_status(tasks);
    for (auto& st : statuses) {
      if (!st.ok()) {
	return st;
      }
    }
    return Status::Ok();
  }

  std::vector<Status> wait_all_status(std::vector<Task>& tasks) {
    std::vector<Status> statuses;

    std::queue<Task> pending_tasks;

    // enqueue all valid tasks that have not completed
    for (auto& task : tasks) {
      if (!task.valid()) {
	LOG_ERROR("Waiting on invalid task future.");
	statuses.push_back(Status::ThreadPoolError("Invalid task future"));
      } else {
	if (task.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
	  statuses.push_back(task.get());
	} else {
	  pending_tasks.push(task);	  
	}
      }
    }
    
    // While still waiting for some tasks
    while (!pending_tasks.empty()) {
      auto task = pending_tasks.front(); pending_tasks.pop();
      
      // Check if task has completed
      if (task.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
	statuses.push_back(task.get());
      } else {

	// If not completed, try again later
	pending_tasks.push(task);
	
	// In the meantime, try to do something useful
	if (auto val = task_queue_.try_pop()) {
	  (*val)();
	} else {

	  // If nothing useful to do, pause
	  task.wait_for(std::chrono::milliseconds(10));
	}
      }
    }

    return statuses;
  }
  
  size_t concurrency_level() { return concurrency_level_; }

private:
  producer_consumer_queue<std::function<void()>> task_queue_;
  std::vector<std::thread>                       threads_;
  std::atomic<size_t>                            concurrency_level_;
};


}


#endif  // TILEDB_THREAD_POOL_H
