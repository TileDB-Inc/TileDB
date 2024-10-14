/**
 * @file   thread_pool.cc
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
 * This file defines the ThreadPool class.
 */

#include <cassert>
#include <memory>
#include <queue>
#include <thread>

#include "tiledb/common/logger.h"
#include "tiledb/common/thread_pool/thread_pool.h"

namespace tiledb::common {

// Constructor.  May throw an exception on error.  No logging is done as the
// logger may not yet be initialized.
ThreadPool::ThreadPool(size_t n)
    : concurrency_level_(n) {
  // If concurrency_level_ is set to zero, construct the thread pool in shutdown
  // state.  Explicitly shut down the task queue as well.
  if (concurrency_level_ == 0) {
    task_queue_.drain();
    return;
  }

  // Set an upper limit on number of threads per core.  One use for this is in
  // testing error conditions in creating a context.
  if (concurrency_level_ >= 256 * std::thread::hardware_concurrency()) {
    std::string msg = "Error initializing thread pool of concurrency level " +
                      std::to_string(concurrency_level_) +
                      "; Requested size too large";
    auto st = Status_ThreadPoolError(msg);
    throw std::runtime_error(msg);
  }

  threads_.reserve(concurrency_level_);

  for (size_t i = 0; i < concurrency_level_; ++i) {
    std::thread tmp;

    // Try to launch a thread running the worker() function. If we get
    // resources_unvailable_try_again error, then try again. Three shall be the
    // maximum number of retries and the maximum number of retries shall be
    // three.
    size_t tries = 3;
    while (tries--) {
      try {
        tmp = std::thread(&ThreadPool::worker, this);
      } catch (const std::system_error& e) {
        if (e.code() != std::errc::resource_unavailable_try_again ||
            tries == 0) {
          auto st = Status_ThreadPoolError(
              "Error initializing thread pool of concurrency level " +
              std::to_string(concurrency_level_) + "; " + e.what());
          shutdown();
          throw std::runtime_error(
              "Error initializing thread pool of concurrency level " +
              std::to_string(concurrency_level_) + "; " + e.what());
        }
        continue;
      }
      break;
    }

    try {
      threads_.emplace_back(std::move(tmp));
    } catch (...) {
      shutdown();
      throw;
    }
  }
}

void ThreadPool::worker() {
  while (true) {
    auto val = task_queue_.pop();
    if (val) {
      (*(*val))();
    } else {
      break;
    }
  }
}

// shutdown is private and only called by constructor and destructor (RAII), so
// shutdown won't be called from multiple threads.
void ThreadPool::shutdown() {
  concurrency_level_.store(0);
  task_queue_.drain();
  for (auto&& t : threads_) {
    t.join();
  }
  threads_.clear();
}

// Return a vector of Status.  If any task returns an error value or throws an
// exception, we save an error code in the corresponding location in the Status
// vector.  All tasks are waited on before return.  Multiple error statuses may
// be saved.  We may call logger here because thread pool will not be used until
// context is fully constructed (which will include logger).
// Unfortunately, C++ does not have the notion of an aggregate exception, so we
// don't throw in the case of errors/exceptions.
void ThreadPool::wait_all(std::vector<Task>& tasks) {
  std::queue<size_t> pending_tasks;

  // Create queue of ids of all the pending tasks for processing
  for (size_t i = 0; i < tasks.size(); ++i) {
    pending_tasks.push(i);
  }

  // Process tasks in the pending queue
  while (!pending_tasks.empty()) {
    auto task_id = pending_tasks.front();
    pending_tasks.pop();
    auto& task = tasks[task_id];

    if (!task.valid()) {
      throw TaskException("Invalid task future");
    } else if (
        task.wait_for(std::chrono::milliseconds(0)) ==
        std::future_status::ready) {
      // Task is completed, throw possible exception
      task.get();
    } else {
      // If the task is not completed, try again later
      pending_tasks.push(task_id);

      // In the meantime, try to do something useful to make progress (and avoid
      // deadlock)
      if (auto val = task_queue_.try_pop()) {
        (*(*val))();
      } else {
        // If nothing useful to do, yield so we don't burn cycles
        // going through the task list over and over (thereby slowing down other
        // threads).
        std::this_thread::yield();

        // (An alternative would be to wait some amount of time)
        // task.wait_for(std::chrono::milliseconds(10));
      }
    }
  }
}

void ThreadPool::wait(Task& task) {
  while (true) {
    if (!task.valid()) {
      throw TaskException("Invalid task future");
    } else if (
        task.wait_for(std::chrono::milliseconds(0)) ==
        std::future_status::ready) {
      // Task is completed, throw possible exception
      task.get();
      return;
    } else {
      // In the meantime, try to do something useful to make progress (and avoid
      // deadlock)
      if (auto val = task_queue_.try_pop()) {
        (*(*val))();
      } else {
        std::this_thread::yield();
      }
    }
  }
}

}  // namespace tiledb::common
