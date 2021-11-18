/**
 * @file   thread_pool.cc
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
 * This file defines the ThreadPool class.
 */

#include <cassert>
#include <queue>

#include "tiledb/common/logger.h"
#include "tiledb/common/thread_pool.h"

namespace tiledb::common {

Status ThreadPool::init(size_t n) {
  if (n == 0) {
    return Status::ThreadPoolError(
        "Unable to initialize a thread pool with a concurrency level of 0.");
  }
  Status st = Status::Ok();

  concurrency_level_ = n;
  threads_.reserve(n);

  for (size_t i = 0; i < concurrency_level_; ++i) {
    std::thread tmp;

    // Try to launch a thread running the worker()
    // function. If we get resources_unvailable_try_again error, then try
    // again. Three shall be the maximum number of retries and the maximum
    // number of retries shall be three.
    size_t tries = 3;
    while (tries--) {
      try {
        tmp = std::thread(&ThreadPool::worker, this);
      } catch (const std::system_error& e) {
        if (e.code() != std::errc::resource_unavailable_try_again ||
            tries == 0) {
          st = Status::ThreadPoolError(
              "Error initializing thread pool of concurrency level " +
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

  return st;
}

void ThreadPool::worker() {
  while (true) {
    auto val = task_queue_.pop();
    if (val) {
      (*val)();
    } else {
      break;
    }
  }
}

void ThreadPool::shutdown() {
  task_queue_.drain();
  for (auto&& t : threads_) {
    t.join();
  }
  threads_.clear();
}

Status ThreadPool::wait_all(std::vector<Task>& tasks) {
  auto statuses = wait_all_status(tasks);
  for (auto& st : statuses) {
    if (!st.ok()) {
      return st;
    }
  }
  return Status::Ok();
}

std::vector<Status> ThreadPool::wait_all_status(std::vector<Task>& tasks) {
  std::vector<Status> statuses;

  std::queue<Task> pending_tasks;

  // Enqueue all the tasks for processing
  for (auto& task : tasks) {
    pending_tasks.push(task);
  }

  // Process tasks in the pending queue
  while (!pending_tasks.empty()) {
    auto task = pending_tasks.front();
    pending_tasks.pop();

    if (!task.valid()) {
      LOG_ERROR("Waiting on invalid task future.");
      statuses.push_back(Status::ThreadPoolError("Invalid task future"));
    } else if (
        task.wait_for(std::chrono::milliseconds(0)) ==
	    std::future_status::ready) {

      // Task is completed, get result, handling possible exceptions

      Status st = [&task] {
        try {
          return task.get();
        } catch (const std::string& msg) {
          return Status::TaskError("Caught " + msg);
        } catch (const Status& stat) {
          return stat;
        } catch (...) {
          return Status::TaskError("Caught ... ");
        }
      }();

      statuses.emplace_back(st);

    } else {
      // If the task is not completed, try again later
      pending_tasks.push(task);
      
      // In the meantime, try to do something useful to make progress (and avoid
      // deadlock)

      if (auto val = task_queue_.try_pop()) {
        (*val)();
      } else {
        // If nothing useful to do, take a short pause so we don't burn cycles
        // going through the task list over and over (thereby slowing down other
	// threads). 

	task.wait_for(std::chrono::milliseconds(10));
	
	// (Or, we could simply yield -- this seems to degrade performance.)
	// std::this_thread::yield();
      }
    }
  }

  return statuses;
}

}  // namespace tiledb::common
