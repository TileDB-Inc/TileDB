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
 * This file declares the (legacy) ThreadPool class.
 */

#ifndef TILEDB_LEGACY_THREAD_POOL_H
#define TILEDB_LEGACY_THREAD_POOL_H

#include <condition_variable>
#include <functional>
#include <mutex>
#include <stack>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tiledb/common/common.h"
#include "tiledb/common/macros.h"
#include "tiledb/common/status.h"

namespace tiledb {
namespace common {

/**
 * A recusive-safe thread pool.
 */
class ThreadPool {
 private:
  /* ********************************* */
  /*          PRIVATE DATATYPES        */
  /* ********************************* */

  // Forward-declaration.
  struct TaskState;

  // Forward-declaration.
  class PackagedTask;

 public:
  /* ********************************* */
  /*          PUBLIC DATATYPES         */
  /* ********************************* */

  class Task {
   public:
    /** Constructor. */
    Task()
        : task_state_(nullptr) {
    }

    /** Move constructor. */
    Task(Task&& rhs) {
      task_state_ = std::move(rhs.task_state_);
    }

    /** Move-assign operator. */
    Task& operator=(Task&& rhs) {
      task_state_ = std::move(rhs.task_state_);
      return *this;
    }

    /** Returns true if this instance is associated with a valid task. */
    bool valid() {
      return task_state_ != nullptr;
    }

   private:
    /** Value constructor. */
    Task(const shared_ptr<TaskState>& task_state)
        : task_state_(std::move(task_state)) {
    }

    DISABLE_COPY_AND_COPY_ASSIGN(Task);

    /** Blocks until the task has completed or there are other tasks to service.
     */
    void wait() {
      std::unique_lock<std::mutex> ul(task_state_->return_st_mutex_);
      if (!task_state_->return_st_set_ && !task_state_->check_task_stack_)
        task_state_->cv_.wait(ul);
    }

    /** Returns true if the associated task has completed. */
    bool done() {
      std::lock_guard<std::mutex> lg(task_state_->return_st_mutex_);
      return task_state_->return_st_set_;
    }

    /**
     * Returns the result value from the task. If the task
     * has not completed, it will wait.
     */
    Status get() {
      wait();
      std::lock_guard<std::mutex> lg(task_state_->return_st_mutex_);
      return task_state_->return_st_;
    }

    /** The shared task state between futures and their associated task. */
    shared_ptr<TaskState> task_state_;

    friend ThreadPool;
    friend PackagedTask;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ThreadPool();

  /** Destructor. */
  ~ThreadPool();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Initialize the thread pool.
   *
   * @param concurrency_level Maximum level of concurrency.
   * @return Status
   */
  Status init(uint64_t concurrency_level = 1);

  /**
   * Schedule a new task to be executed. If the returned `Task` object
   * is valid, `function` is guaranteed to execute. The 'function' may
   * execute immediately on the calling thread. To avoid deadlock, `function`
   * should not aquire non-recursive locks held by the calling thread.
   *
   * @param function Task function to execute.
   * @return Task for the return status of the task.
   */
  Task execute(std::function<Status()>&& function);

  /** Return the maximum level of concurrency. */
  uint64_t concurrency_level() const;

  /**
   * Wait on all the given tasks to complete. This is safe to call recusively
   * and may execute pending tasks on the calling thread while waiting.
   *
   * @param tasks Task list to wait on.
   * @return Status::Ok if all tasks returned Status::Ok, otherwise the first
   * error status is returned
   */
  Status wait_all(std::vector<Task>& tasks);

  /**
   * Wait on all the given tasks to complete, return a vector of their return
   * Status. This is safe to call recusively and may execute pending tasks
   * on the calling thread while waiting.
   *
   * @param tasks Task list to wait on
   * @return Vector of each task's Status.
   */
  std::vector<Status> wait_all_status(std::vector<Task>& tasks);

 private:
  /* ********************************* */
  /*          PRIVATE DATATYPES        */
  /* ********************************* */

  struct TaskState {
    /** Constructor. */
    TaskState()
        : return_st_()
        , check_task_stack_(false)
        , return_st_set_(false) {
    }

    DISABLE_COPY_AND_COPY_ASSIGN(TaskState);
    DISABLE_MOVE_AND_MOVE_ASSIGN(TaskState);

    /** The return status from an executed task. */
    Status return_st_;

    bool check_task_stack_;

    /** True if the `return_st_` has been set. */
    bool return_st_set_;

    /** Waits for a task to complete. */
    std::condition_variable cv_;

    /** Protects `return_st_`, `return_st_set_`, and `cv_`. */
    std::mutex return_st_mutex_;
  };

  class PackagedTask {
   public:
    /** Constructor. */
    PackagedTask()
        : fn_(nullptr)
        , task_state_(nullptr)
        , parent_(nullptr) {
    }

    /** Value constructor. */
    template <class Fn_T>
    explicit PackagedTask(Fn_T&& fn, shared_ptr<PackagedTask>&& parent) {
      fn_ = std::move(fn);
      task_state_ = make_shared<TaskState>(HERE());
      parent_ = std::move(parent);
    }

    /** Function-call operator. */
    void operator()() {
      const Status r = fn_();
      {
        std::lock_guard<std::mutex> lg(task_state_->return_st_mutex_);
        task_state_->return_st_set_ = true;
        task_state_->return_st_ = r;
      }
      task_state_->cv_.notify_all();

      fn_ = std::function<Status()>();
      task_state_ = nullptr;
    }

    /** Returns the future associated with this task. */
    ThreadPool::Task get_future() const {
      return Task(task_state_);
    }

    PackagedTask* get_parent() const {
      return parent_.get();
    }

   private:
    DISABLE_COPY_AND_COPY_ASSIGN(PackagedTask);
    DISABLE_MOVE_AND_MOVE_ASSIGN(PackagedTask);

    /** The packaged function. */
    std::function<Status()> fn_;

    /** The task state to share with futures. */
    shared_ptr<TaskState> task_state_;

    /** The parent task that executed this task. */
    shared_ptr<PackagedTask> parent_;
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The maximum level of concurrency among a single waiter and all
   * of the the `threads_`.
   */
  uint64_t concurrency_level_;

  /** Protects `task_stack_`, `idle_threads_`, and `task_stack_clock_`. */
  std::mutex task_stack_mutex_;

  /** Notifies work threads to check `task_stack_` for work. */
  std::condition_variable task_stack_cv_;

  /** Pending tasks in LIFO ordering. */
  std::vector<shared_ptr<PackagedTask>> task_stack_;

  /*
   * A logical, monotonically increasing clock that is incremented
   * when a task is either added or removed from `task_stack_`. This
   * is used by threads to determine if `task_stack_` has been modified
   * between two points in time.
   */
  uint64_t task_stack_clock_;

  /**
   * The number of threads waiting for the `task_stack_` to
   * become non-empty.
   */
  uint64_t idle_threads_;

  /** The worker threads. */
  std::vector<std::thread> threads_;

  /** When true, all pending tasks will remain unscheduled. */
  bool should_terminate_;

  /** All tasks that threads in this instance are waiting on. */
  struct BlockedTasksHasher {
    size_t operator()(const shared_ptr<TaskState>& task) const {
      return reinterpret_cast<size_t>(task.get());
    }
  };
  std::unordered_set<shared_ptr<TaskState>, BlockedTasksHasher> blocked_tasks_;

  /** Protects `blocked_tasks_`. */
  std::mutex blocked_tasks_mutex_;

  /** Indexes thread ids to the ThreadPool instance they belong to. */
  static std::unordered_map<std::thread::id, ThreadPool*> tp_index_;

  /** Protects 'tp_index_'. */
  static std::mutex tp_index_lock_;

  /** Indexes thread ids to the task it is currently executing. */
  static std::unordered_map<std::thread::id, shared_ptr<PackagedTask>>
      task_index_;

  /** Protects 'task_index_'. */
  static std::mutex task_index_lock_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Waits for `task`, but will execute other tasks from `task_stack_`
   * while waiting. While this may be an performance optimization
   * to perform work on this thread rather than waiting, the primary
   * motiviation is to prevent deadlock when tasks are enqueued recursively.
   */
  Status wait_or_work(Task&& task);

  /** Terminate the threads in the thread pool. */
  void terminate();

  /** The worker thread routine. */
  static void worker(ThreadPool& pool);

  // Add indexes from each thread to this instance.
  void add_tp_index();

  // Remove indexes from each thread to this instance.
  void remove_tp_index();

  // Lookup the thread pool instance that contains `tid`.
  static ThreadPool* lookup_tp(std::thread::id tid);

  // Add indexes for each thread on the `task_index_`.
  void add_task_index();

  // Remove indexes for each thread on the `task_index_`.
  void remove_task_index();

  // Lookup the task executing on `tid`.
  static shared_ptr<PackagedTask> lookup_task(std::thread::id tid);

  // Wrapper to update `task_index_` and execute `task`.
  static void exec_packaged_task(shared_ptr<PackagedTask> task);
};

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_LEGACY_THREAD_POOL_H
