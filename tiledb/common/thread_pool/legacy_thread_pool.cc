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

#include "tiledb/common/logger.h"
#include "tiledb/common/thread_pool.h"

namespace tiledb {
namespace common {

// Define the static ThreadPool member variables.
std::unordered_map<std::thread::id, ThreadPool*> ThreadPool::tp_index_;
std::mutex ThreadPool::tp_index_lock_;
std::unordered_map<std::thread::id, shared_ptr<ThreadPool::PackagedTask>>
    ThreadPool::task_index_;
std::mutex ThreadPool::task_index_lock_;

ThreadPool::ThreadPool()
    : concurrency_level_(0)
    , task_stack_clock_(0)
    , idle_threads_(0)
    , should_terminate_(false) {
}

ThreadPool::~ThreadPool() {
  terminate();
}

Status ThreadPool::init(const uint64_t concurrency_level) {
  if (concurrency_level == 0) {
    return Status_ThreadPoolError(
        "Unable to initialize a thread pool with a concurrency level of 0.");
  }

  Status st = Status::Ok();

  // We allocate one less thread than `concurrency_level` because
  // the `wait_all*()` routines may service tasks concurrently with
  // the worker threads.
  const uint64_t num_threads = concurrency_level - 1;
  for (uint64_t i = 0; i < num_threads; i++) {
    try {
      threads_.emplace_back([this]() { worker(*this); });
    } catch (const std::exception& e) {
      st = Status_ThreadPoolError(
          "Error initializing thread pool of concurrency level " +
          std::to_string(concurrency_level) + "; " + e.what());
      LOG_STATUS(st);
      break;
    }
  }

  if (!st.ok()) {
    // Join any created threads on error.
    terminate();
    return st;
  }

  // Save the concurrency level.
  concurrency_level_ = concurrency_level;

  // Add indexes from this ThreadPool instance from all of its thread ids.
  add_tp_index();

  // Add task indexes for each thread in this thread pool.
  add_task_index();

  return st;
}

ThreadPool::Task ThreadPool::execute(std::function<Status()>&& function) {
  if (concurrency_level_ == 0) {
    Task invalid_future;
    LOG_ERROR("Cannot execute task; thread pool uninitialized.");
    return invalid_future;
  }

  if (!function) {
    Task invalid_future;
    LOG_ERROR("Cannot execute task; invalid function.");
    return invalid_future;
  }

  std::unique_lock<std::mutex> ul(task_stack_mutex_);

  if (should_terminate_) {
    Task invalid_future;
    LOG_ERROR("Cannot execute task; thread pool has terminated.");
    return invalid_future;
  }

  // Locate the currently executing task, which may be null.
  const std::thread::id tid = std::this_thread::get_id();
  shared_ptr<PackagedTask> parent_task = lookup_task(tid);

  // Create the packaged task.
  auto task =
      make_shared<PackagedTask>(HERE(), move(function), move(parent_task));

  // Fetch the future from the packaged task.
  ThreadPool::Task future = task->get_future();

  // When we have a concurrency level > 1, we will have at least
  // one thread available to pick up the task. For a concurrency
  // level == 1, we have no worker threads available. When no
  // worker threads are available, execute the task on this
  // thread.
  if (concurrency_level_ == 1) {
    ul.unlock();
    exec_packaged_task(task);
  } else {
    // Lookup the thread pool that this thread belongs to. If it
    // does not belong to a thread pool, `lookup_tp` will return
    // `nullptr`.
    ThreadPool* const tp = lookup_tp(tid);

    // As both an optimization and a means of breaking deadlock,
    // execute the task if this thread belongs to `this`. Otherwise,
    // queue it for a worker thread.
    if (tp == this && idle_threads_ == 0) {
      ul.unlock();
      exec_packaged_task(task);
    } else {
      // Add `task` to the stack of pending tasks.
      task_stack_.emplace_back(std::move(task));
      task_stack_cv_.notify_one();

      // Increment the logical clock to indicate that the
      // `task_stack_` has been modified.
      ++task_stack_clock_;

      // The `ul` protects `task_stack_`, `task_stack_clock_`, and
      // `idle_threads_`. Save a copy of `idle_threads_` before releasing
      // the lock.
      const uint64_t idle_threads_cp = idle_threads_;
      ul.unlock();

      // If all threads are busy, signal a thread in `this` that is
      // blocked waiting on another task. This wakes up one of those
      // threads to service the `task` that we just added to `task_stack_`.
      // There is a race here on `idle_threads_` because we just released
      // the lock. If a thread became idle and picks up `task`, we have
      // spuriously unlocked a thread in the `wait` path. It will find
      // that the `task_stack_` is empty and re-enter its wait.
      if (idle_threads_cp == 0) {
        blocked_tasks_mutex_.lock();
        if (!blocked_tasks_.empty()) {
          // Signal the first blocked task to wake up and check the task
          // stack for a task to execute.
          shared_ptr<TaskState> blocked_task = *blocked_tasks_.begin();
          {
            std::lock_guard<std::mutex> lg(blocked_task->return_st_mutex_);
            blocked_task->check_task_stack_ = true;
          }
          blocked_task->cv_.notify_all();
          blocked_tasks_.erase(blocked_task);
        }
        blocked_tasks_mutex_.unlock();
      }
    }
  }

  assert(future.valid());
  return future;
}

uint64_t ThreadPool::concurrency_level() const {
  return concurrency_level_;
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
  for (auto& task : tasks) {
    if (!task.valid()) {
      LOG_ERROR("Waiting on invalid task future.");
      statuses.push_back(Status_ThreadPoolError("Invalid task future"));
    } else {
      Status status = wait_or_work(std::move(task));
      if (!status.ok()) {
        LOG_STATUS(status);
      }
      statuses.push_back(status);
    }
  }
  return statuses;
}

Status ThreadPool::wait_or_work(Task&& task) {
  // Records the last read value from `task_stack_clock_`.
  uint64_t last_task_stack_clock = 0;

  do {
    if (task.done())
      break;

    // Lock the `task_stack_` to receive the next task to work on.
    task_stack_mutex_.lock();

    // Determine if `task_stack_` has been modified since our
    // last loop. This is always true for the first iteration
    // in this loop. Note that `task_stack_clock_` may overflow,
    // producing a false-positive. In that scenario, we will
    // perform one spurious loop but will not affect the
    // correctness of this routine.
    const bool task_stack_modified = last_task_stack_clock == 0 ||
                                     last_task_stack_clock != task_stack_clock_;

    // If there are no pending tasks or the stack of pending tasks has
    // not changed since our last inspection, we will wait for `task`
    // to complete.
    if (task_stack_.empty() || !task_stack_modified) {
      task_stack_mutex_.unlock();

      // Add `task` to `blocked_tasks_` so that the `execute()` path can
      // signal it when a new pending task is available.
      blocked_tasks_mutex_.lock();
      blocked_tasks_.insert(task.task_state_);
      blocked_tasks_mutex_.unlock();

      // Block until the task is signaled. It will be signaled when it
      // has completed or when there is new work to execute on `task_stack_`.
      task.wait();

      // Remove `task` from `blocked_tasks_`.
      blocked_tasks_mutex_.lock();
      if (blocked_tasks_.count(task.task_state_) > 0)
        blocked_tasks_.erase(task.task_state_);
      blocked_tasks_mutex_.unlock();

      // After the task has been woken up, check to see if it has completed.
      if (task.done()) {
        break;
      }

      // The task did not complete. This task has been signaled because a new
      // pending task was added to `task_stack_`. Reset the `check_task_stack_`
      // flag.
      {
        std::lock_guard<std::mutex> lg(task.task_state_->return_st_mutex_);
        task.task_state_->check_task_stack_ = false;
      }

      // Lock the `task_stack_` again before checking for the next pending task.
      task_stack_mutex_.lock();
    }

    // We may have released and re-aquired the `task_stack_mutex_`. We must
    // check if it is still non-empty.
    if (!task_stack_.empty()) {
      // Pull the next task off of the task stack. We specifically use a LIFO
      // ordering to prevent overflowing the call stack. We will skip tasks
      // that are not descendents of the task we are currently executing in.
      const std::thread::id tid = std::this_thread::get_id();
      shared_ptr<PackagedTask> current_task = lookup_task(tid);
      shared_ptr<PackagedTask> descendent_task = nullptr;
      if (current_task == nullptr) {
        // We are not executing in the context of a threadpool task, we do
        // not have any restriction on which task we can execute. Select
        // the next one.
        descendent_task = task_stack_.back();
        task_stack_.pop_back();
      } else {
        // Find the next pending task that is a descendent of `current_task`.
        for (auto riter = task_stack_.rbegin(); riter != task_stack_.rend();
             ++riter) {
          // Determine if the task pointed to by `riter` is a descendent
          // of `current_task`.
          const PackagedTask* tmp_task = riter->get();
          while (tmp_task != nullptr) {
            const PackagedTask* const tmp_task_parent = tmp_task->get_parent();
            if (tmp_task_parent == current_task.get()) {
              descendent_task = *riter;
              break;
            }
            tmp_task = tmp_task_parent;
          }

          // If we found a descendent task, erase it from the task stack
          // and break.
          if (descendent_task != nullptr) {
            task_stack_.erase(std::next(riter).base());
            break;
          }
        }
      }

      // Save the current state of `task_stack_clock_`. We may mutate it below.
      last_task_stack_clock = task_stack_clock_;

      // If `descendent_task` is non-null, we must have removed it from
      // the `task_stack_`. In this scenario, increment the logical clock
      // to indicate that the `task_stack_` has been modified.
      if (descendent_task != nullptr)
        ++task_stack_clock_;

      // We're done mutating `task_stack_` and `task_stack_clock_`.
      task_stack_mutex_.unlock();

      // Execute the descendent task if we found one.
      if (descendent_task != nullptr)
        exec_packaged_task(descendent_task);
    } else {
      // The task stack is now empty, retry.
      task_stack_mutex_.unlock();
    }
  } while (true);

  // The task has completed and will not block.
  assert(task.done());
  return task.get();
}

void ThreadPool::terminate() {
  {
    std::unique_lock<std::mutex> ul(task_stack_mutex_);
    should_terminate_ = true;
    task_stack_cv_.notify_all();
  }

  remove_tp_index();

  for (auto& t : threads_) {
    t.join();
  }

  remove_task_index();

  threads_.clear();
}

void ThreadPool::worker(ThreadPool& pool) {
  while (true) {
    shared_ptr<PackagedTask> task = nullptr;

    {
      // Wait until there's work to do.
      std::unique_lock<std::mutex> ul(pool.task_stack_mutex_);
      ++pool.idle_threads_;
      pool.task_stack_cv_.wait(ul, [&pool]() {
        return pool.should_terminate_ || !pool.task_stack_.empty();
      });

      if (!pool.task_stack_.empty()) {
        task = std::move(pool.task_stack_.back());
        pool.task_stack_.pop_back();
        --pool.idle_threads_;
      } else {
        // The task stack was empty, ensure `task` is invalid.
        task = nullptr;
      }
    }

    if (task != nullptr)
      exec_packaged_task(task);

    if (pool.should_terminate_)
      break;
  }
}

void ThreadPool::add_tp_index() {
  std::lock_guard<std::mutex> lock(tp_index_lock_);
  for (const auto& thread : threads_)
    tp_index_[thread.get_id()] = this;
}

void ThreadPool::remove_tp_index() {
  std::lock_guard<std::mutex> lock(tp_index_lock_);
  for (const auto& thread : threads_)
    tp_index_.erase(thread.get_id());
}

ThreadPool* ThreadPool::lookup_tp(const std::thread::id tid) {
  std::lock_guard<std::mutex> lock(tp_index_lock_);
  if (tp_index_.count(tid) == 1)
    return tp_index_[tid];
  return nullptr;
}

void ThreadPool::add_task_index() {
  std::lock_guard<std::mutex> lock(task_index_lock_);
  for (const auto& thread : threads_)
    task_index_[thread.get_id()] = nullptr;
}

void ThreadPool::remove_task_index() {
  std::lock_guard<std::mutex> lock(task_index_lock_);
  for (const auto& thread : threads_)
    task_index_.erase(thread.get_id());
}

shared_ptr<ThreadPool::PackagedTask> ThreadPool::lookup_task(
    const std::thread::id tid) {
  std::lock_guard<std::mutex> lock(task_index_lock_);
  if (task_index_.count(tid) == 1)
    return task_index_[tid];
  return nullptr;
}

void ThreadPool::exec_packaged_task(shared_ptr<PackagedTask> const task) {
  const std::thread::id tid = std::this_thread::get_id();

  // Before we execute `task`, we must update `task_index_` to map
  // this thread-id to the executing task. Note that we only lock
  // `task_index_` to protect the container itself. The elements
  // on the map are only ever accessed by the thread that the
  // element is keyed on, making it implicitly safe.
  shared_ptr<PackagedTask> tmp_task = nullptr;
  std::unique_lock<std::mutex> ul(task_index_lock_);
  if (task_index_.count(tid) == 1)
    tmp_task = task_index_[tid];
  task_index_[tid] = task;
  ul.unlock();

  // Execute `task`.
  (*task)();

  // Restore `task_index_` to the task that it was previously
  // executing, which may be null.
  ul.lock();
  task_index_[tid] = tmp_task;
  ul.unlock();
}

}  // namespace common
}  // namespace tiledb
