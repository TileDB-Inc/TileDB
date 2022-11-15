/**
 * @file   throw_catch.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file declares a throw_catch (static threadpool) scheduler for dag.
 *
 * This scheduler has a fixed number of threads (determined when the scheduler
 * is constructed).  Each thread runs the `worker` method of the scheduler.
 * The `worker` method implements the scheduling of tasks.  A task is an
 * executable entity with a `resume` method.  The `worker` manages the state
 * of each task, in conjunction with a scheduler policy task and the scheduler
 * state machine (defined in task_state_machine.h).
 *
 * Tasks are submitted to the scheduler with the `submit` method.  The
 * `submit` method is variadic, so an arbitrary number of tasks can be
 * submitted.  Task execution is lazy; tasks do not start executing when
 * submit is called.  Rather, after `submit` has been called, a "wait"
 * scheduler function is called, which will begin execution of the submitted
 * tasks.  In the case of `sync_wait_all`, the scheduler will start execution
 * of all tasks and block until they are all complete.
 *
 * Tasks are maintained on a "runnable" queue and are executed in order from
 * the queue.  When a task is executing, it is placed in the running set and
 * when it is waiting, it is placed in the waiting set.  When a task yields,
 * it is moved from the running set to the back of the runnable queue.  When a
 * task is notified, it is moved from the waiting set to the runnable queue.
 *
 * When a task has completed execution, it is moved to the finished queue.
 *
 * @todo Factor scheduler, task, and policy so they are more orthogonal and
 * can be mixed and matched.
 */

#ifndef TILEDB_DAG_throw_catch_H
#define TILEDB_DAG_throw_catch_H

#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/item_mover.h"
#include "experimental/tiledb/common/dag/utils/bounded_buffer.h"
#include "experimental/tiledb/common/dag/utils/concurrent_map.h"
#include "experimental/tiledb/common/dag/utils/concurrent_set.h"
#include "experimental/tiledb/common/dag/utils/print_types.h"

namespace tiledb::common {

namespace detail {

enum class throw_catch_target { self, source, sink, last };

class throw_catch_exception : public std::exception {
  throw_catch_target target_{throw_catch_target::self};

 public:
  explicit throw_catch_exception(
      throw_catch_target target = throw_catch_target::self)
      : target_{target} {
  }

  [[nodiscard]] throw_catch_target target() const {
    return target_;
  }
};

class throw_catch_exit : public throw_catch_exception {
  using throw_catch_exception::throw_catch_exception;
};

class throw_catch_wait : public throw_catch_exception {
  using throw_catch_exception::throw_catch_exception;
};

class throw_catch_notify : public throw_catch_exception {
  using throw_catch_exception::throw_catch_exception;
};

}  // namespace detail

const detail::throw_catch_exit throw_catch_exit;
const detail::throw_catch_wait throw_catch_sink_wait{
    detail::throw_catch_target::sink};
const detail::throw_catch_wait throw_catch_source_wait{
    detail::throw_catch_target::source};
const detail::throw_catch_notify throw_catch_notify_sink{
    detail::throw_catch_target::sink};
const detail::throw_catch_notify throw_catch_notify_source{
    detail::throw_catch_target::source};

template <class Mover, class PortState = typename Mover::PortState>
class ThrowCatchPortPolicy : public PortFiniteStateMachine<
                                 ThrowCatchPortPolicy<Mover, PortState>,
                                 PortState> {
  using state_machine_type =
      PortFiniteStateMachine<ThrowCatchPortPolicy<Mover, PortState>, PortState>;
  using lock_type = typename state_machine_type::lock_type;

  using mover_type = Mover;

 public:
  ThrowCatchPortPolicy() {
    if constexpr (std::is_same_v<PortState, two_stage>) {
      assert(static_cast<Mover*>(this)->state() == PortState::st_00);
    } else if constexpr (std::is_same_v<PortState, three_stage>) {
      assert(static_cast<Mover*>(this)->state() == PortState::st_000);
    }
  }

  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action return");
  }

  inline void on_source_move(lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(event);
  }

  inline void on_sink_move(lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(event);
  }

  inline void on_notify_source(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action notify source");
    throw(detail::throw_catch_notify{detail::throw_catch_target::source});
  }

  inline void on_notify_sink(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action notify sink");
    throw(detail::throw_catch_notify{detail::throw_catch_target::sink});
  }

  inline void on_source_wait(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action source wait");
    throw(detail::throw_catch_wait{detail::throw_catch_target::source});
  }

  inline void on_sink_wait(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action sink wait");
    throw(detail::throw_catch_wait{detail::throw_catch_target::sink});
  }

  inline void on_term_source(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action source done");
    throw(detail::throw_catch_exit{detail::throw_catch_target::source});
  }

  inline void on_term_sink(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action sink done");
    throw(detail::throw_catch_exit{detail::throw_catch_target::sink});
  }

 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << "\n";
    }
  }
};

template <class T>
using ThrowCatchMover3 = ItemMover<ThrowCatchPortPolicy, three_stage, T>;
template <class T>
using ThrowCatchMover2 = ItemMover<ThrowCatchPortPolicy, two_stage, T>;

template <class Node>
class ThrowCatchScheduler;

template <class Task>
class ThrowCatchSchedulerPolicy;

template <typename T>
struct SchedulerTraits<ThrowCatchSchedulerPolicy<T>> {
  using task_type = T;
  using task_handle_type = T;
};

template <class Task>
class ThrowCatchSchedulerPolicy
    : public SchedulerStateMachine<ThrowCatchSchedulerPolicy<Task>> {
  using state_machine_type =
      SchedulerStateMachine<ThrowCatchSchedulerPolicy<Task>>;
  using lock_type = typename state_machine_type::lock_type;

 public:
  using task_type =
      typename SchedulerTraits<ThrowCatchSchedulerPolicy<Task>>::task_type;
  using task_handle_type = typename SchedulerTraits<
      ThrowCatchSchedulerPolicy<Task>>::task_handle_type;

  ~ThrowCatchSchedulerPolicy() {
    if (this->debug())
      std::cout << "policy destructor\n";

    waiting_set_.clear();
    runnable_queue_.drain();
    running_set_.clear();
    finished_queue_.drain();
  }

  void on_create(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_create"
                << "\n";
    this->submission_queue_.push(task);
  }

  void on_stop_create(task_handle_type&) {
    if (this->debug())
      std::cout << "calling on_stop_create"
                << "\n";
  }

  void on_make_runnable(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_make_runnable"
                << "\n";
    this->runnable_queue_.push(task);
  }

  void on_stop_runnable(task_handle_type&) {
    if (this->debug())
      std::cout << "calling on_stop_runnable"
                << "\n";
  }

  void on_make_running(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_make_running"
                << "\n";
    this->running_set_.insert(task);
  }

  void on_stop_running(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_stop_running"
                << "\n";
    auto n = this->running_set_.extract(task);

    assert(!n.empty());
  }

  void on_make_waiting(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_make_waiting"
                << "\n";
    this->waiting_set_.insert(task);
  }

  void on_stop_waiting(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_stop_waiting"
                << "\n";

    auto n = this->waiting_set_.extract(task);
    assert(!n.empty());

    // Try waited-on condition again
    // @todo Is this the best place to be doing this?
    n.value()->decrement_program_counter();
  }

  void on_terminate(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_terminate"
                << "\n";
    this->finished_queue_.push(task);
  }

  void launch() {
    if (this->debug()) {
      this->dump_queue_state("Launching start");
    }
    while (true) {
      auto s = submission_queue_.try_pop();
      if (!s)
        break;
      if (this->debug())
        (*s)->dump_task_state("Admitting");

      this->task_admit(*s);
    }
    if (this->debug()) {
      this->dump_queue_state("Launching end");
    }
  }

  auto get_runnable_task() {
#if 1
    auto val = runnable_queue_.try_pop();
    while (val && ((*val)->task_state() == TaskState::terminated)) {
      val = runnable_queue_.try_pop();
    }
    return val;
#else
    auto val = runnable_queue_.pop();
    return val;
#endif
  }

  void p_shutdown() {
#if 0
    if (this->debug())
      std::cout << "policy shutdown\n";

    size_t loops = waiting_set_.size();

    for (size_t i = 0; i < loops; ++i) {
      std::vector<Task> notified_waiters_;
      notified_waiters_.reserve(waiting_set_.size());

      for (auto t : waiting_set_) {

  // Notify can change the waiting_set_
  // But it should be empty by here?
        this->task_notify(t);
      }

      for (auto&& t : notified_waiters_) {
        this->task_dispatch(t);
        waiting_set_.erase(t);
        runnable_queue_.push(t);
      }
      notified_waiters_.clear();
    }

    // all waiters must have been notified
    assert(waiting_set_.empty());

    runnable_queue_.drain();
    finished_queue_.drain();

    waiting_set_.clear();
    running_set_.clear();
#endif
  }

  void dump_queue_state(const std::string& msg = "") {
    std::string preface = (!msg.empty() ? msg + "\n" : "");

    std::cout << preface + "    runnable_queue_.size() = " +
                     std::to_string(runnable_queue_.size()) + "\n" +
                     "    running_set_.size() = " +
                     std::to_string(running_set_.size()) + "\n" +
                     "    waiting_set_.size() = " +
                     std::to_string(waiting_set_.size()) + "\n" +
                     "    finished_queue_.size() = " +
                     std::to_string(finished_queue_.size()) + "\n" + "\n";
  }

 private:
#if 1
  ConcurrentSet<Task> waiting_set_;
  ConcurrentSet<Task> running_set_;
  BoundedBufferQ<Task, std::queue<Task>, false> submission_queue_;
  BoundedBufferQ<Task, std::queue<Task>, false> runnable_queue_;
  BoundedBufferQ<Task, std::queue<Task>, false> finished_queue_;
#else
  ConcurrentSet<std::shared_ptr<Task>> waiting_set_;
  ConcurrentSet<std::shared_ptr<Task>> running_set_;
  BoundedBufferQ<
      std::shared_ptr<Task>,
      std::queue<std::shared_ptr<Task>>,
      false>
      submission_queue_;
  BoundedBufferQ<
      std::shared_ptr<Task>,
      std::queue<std::shared_ptr<Task>>,
      false>
      runnable_queue_;
  BoundedBufferQ<
      std::shared_ptr<Task>,
      std::queue<std::shared_ptr<Task>>,
      false>
      finished_queue_;
#endif
};

#if 0
template <class Node>
class ThrowCatchTaskImpl {
  using scheduler = ThrowCatchScheduler<Node>;
  scheduler* scheduler_{nullptr};
  TaskState state_{TaskState::created};

  /** @todo Is there a way to derive from Node to be able to use statements like
   * `using Node::resume` even though it is a `shared_ptr`? */
  Node node_;

 public:
  ThrowCatchTaskImpl(const Node& n)
      : node_{n} {
  }

  ThrowCatchTaskImpl() = default;
  ThrowCatchTaskImpl(const ThrowCatchTaskImpl&) = default;
  ThrowCatchTaskImpl(ThrowCatchTaskImpl&&) = default;
  ThrowCatchTaskImpl& operator=(const ThrowCatchTaskImpl&) = default;
  ThrowCatchTaskImpl& operator=(ThrowCatchTaskImpl&&) = default;

  void set_scheduler(scheduler* sched) {
    scheduler_ = sched;
  }

  TaskState& task_state() {
    return state_;
  }

  void resume() {
    node_->resume();
  }

  void decrement_program_counter() {
    node_->decrement_program_counter();
  }

  Node* sink_correspondent() const {
    return node_->sink_correspondent();
  }

  Node* source_correspondent() const {
    return node_->source_correspondent();
  }

  std::string name() const {
    return node_->name() + " task";
  }

  auto id() const {
    return node_->id();
  }

  virtual ~ThrowCatchTaskImpl() = default;

  void dump_task_state(const std::string msg = "") {
    std::string preface = (msg != "" ? msg + "\n" : "");

    std::cout << preface + "    " + this->name() + " with id " +
                     std::to_string(this->id()) + "\n" +
                     "    state = " + str(this->task_state()) + "\n";
  }
};

#else

template <class Node>
class ThrowCatchTaskImpl : Node {
  using scheduler = ThrowCatchScheduler<Node>;
  scheduler* scheduler_{nullptr};
  TaskState state_{TaskState::created};

  /** @todo Is there a way to derive from Node to be able to use statements like
   * `using Node::resume` even though it is a `shared_ptr`? */
  //  Node node_;
  using node_ = Node;

 public:
  explicit ThrowCatchTaskImpl(const Node& n)
      : node_{n} {
  }

  ThrowCatchTaskImpl() = default;

  ThrowCatchTaskImpl(const ThrowCatchTaskImpl&) = default;

  ThrowCatchTaskImpl(ThrowCatchTaskImpl&&) noexcept = default;

  ThrowCatchTaskImpl& operator=(const ThrowCatchTaskImpl&) = default;

  ThrowCatchTaskImpl& operator=(ThrowCatchTaskImpl&&) noexcept = default;

  void set_scheduler(scheduler* sched) {
    scheduler_ = sched;
  }

  TaskState& task_state() {
    return state_;
  }

  void resume() {
    (*this)->resume();
  }

  void decrement_program_counter() {
    (*this)->decrement_program_counter();
  }

  Node& sink_correspondent() const {
    return (*this)->sink_correspondent();
  }

  Node& source_correspondent() const {
    return (*this)->source_correspondent();
  }

  [[nodiscard]] std::string name() const {
    return (*this)->name() + " task";
  }

  [[nodiscard]] size_t id() const {
    return (*this)->id();
  }

  virtual ~ThrowCatchTaskImpl() = default;

  void dump_task_state(const std::string& msg = "") {
    std::string preface = (!msg.empty() ? msg + "\n" : "");

    std::cout << preface + "    " + this->name() + " with id " +
                     std::to_string(this->id()) + "\n" +
                     "    state = " + str(this->task_state()) + "\n";
  }
};

#endif

template <class Node>
class ThrowCatchTask : public std::shared_ptr<ThrowCatchTaskImpl<Node>> {
  using Base = std::shared_ptr<ThrowCatchTaskImpl<Node>>;

 public:
  explicit ThrowCatchTask(const Node& n)
      : Base{std::make_shared<ThrowCatchTaskImpl<Node>>(n)} {
  }

  ThrowCatchTask() = default;

  ThrowCatchTask(const ThrowCatchTask& rhs) = default;

  ThrowCatchTask(ThrowCatchTask&& rhs) noexcept = default;

  ThrowCatchTask& operator=(const ThrowCatchTask& rhs) = default;

  ThrowCatchTask& operator=(ThrowCatchTask&& rhs) noexcept = default;

  [[nodiscard]] TaskState& task_state() const {
    return (*this)->task_state();
  }
};

template <class Node>
class ThrowCatchScheduler
    : public ThrowCatchSchedulerPolicy<ThrowCatchTask<Node>> {
  using Scheduler = ThrowCatchScheduler<Node>;
  using Policy = ThrowCatchSchedulerPolicy<ThrowCatchTask<Node>>;

 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param n The number of threads to be spawned for the thread pool.  This
   * should be a value between 1 and 256 * hardware_concurrency.  A value of
   * zero will construct the thread pool in its shutdown state--constructed
   * but not accepting nor executing any tasks.  A value of
   * 256*hardware_concurrency or larger is an error.
   */
  explicit ThrowCatchScheduler(size_t n)
      : concurrency_level_(n) {
    // If concurrency_level_ is set to zero, construct the thread pool in
    // shutdown state.  Explicitly shut down the task queue as well.
    if (concurrency_level_ == 0) {
      return;
    }

    // Set an upper limit on number of threads per core.  One use for this is
    // in testing error conditions in creating a context.
    if (concurrency_level_ >= 256 * std::thread::hardware_concurrency()) {
      std::string msg =
          "Error initializing throw_catch scheduler of concurrency level " +
          std::to_string(concurrency_level_) + "; Requested size too large";
      throw std::runtime_error(msg);
    }

    threads_.reserve(concurrency_level_);

    for (size_t i = 0; i < concurrency_level_; ++i) {
      std::thread tmp;

      // Try to launch a thread running the worker() function. If we get
      // resources_unavailable_try_again error, then try again. Three shall be
      // the maximum number of retries and the maximum number of retries shall
      // be three.
      size_t tries = 3;
      while (tries--) {
        try {
          tmp = std::thread(&ThrowCatchScheduler::worker, this);
        } catch (const std::system_error& e) {
          if (e.code() != std::errc::resource_unavailable_try_again ||
              tries == 0) {
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

  /** Deleted default constructor */
  ThrowCatchScheduler() = delete;

  ThrowCatchScheduler(const ThrowCatchScheduler&) = delete;

  /** Destructor. */
  ~ThrowCatchScheduler() {
    if (this->debug())
      std::cout << "scheduler destructor\n";
    shutdown();
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

 public:
  size_t concurrency_level() {
    return concurrency_level_;
  }

  std::atomic<bool> debug_{false};

  void enable_debug() {
    debug_.store(true);
  }

  void disable_debug() {
    debug_.store(false);
  }

  bool debug() {
    return debug_.load();
  }

 private:
  /** @todo Need to make ConcurrentMap */
  ConcurrentMap<Node, ThrowCatchTask<Node>> node_to_task;
  //  ConcurrentMap<Node, ThrowCatchTask<Node>> port_to_task;

 public:
  void submit(Node&& n) {
    ++num_submitted_tasks_;
    ++num_tasks_;

    auto t = ThrowCatchTask{n};

    t->set_scheduler(this);

    node_to_task[n] = t;

    this->task_create(t);

    if (this->debug()) {
      t->dump_task_state("Submitting");
    }
  }

  /**
   * Wait on all the given tasks to complete. Since tasks are started lazily,
   * they are not actually started on submit().  So, we first start all the
   * submitted jobs and then wait on them to all be started before
   * transferring them from the submitted queue to the runnable queue.
   */
  void sync_wait_all() {
    std::cout << "Starting sync_wait_all()\n";

    /*
     * Swap the submission queue (where all the submitted tasks are) with the
     * runnable queue, making all the tasks runnable.
     */
    Policy::launch();

    this->make_ready_to_run();

    std::cout << "About to release worker threads\n";

    /*
     * Once we have put all tasks into the runnable queue, we release the
     * worker threads.
     */
    {
      std::unique_lock _(mutex_);
      start_cv_.notify_all();
    }

    /*
     * Then we wait for the worker threads to complete.
     *
     * @todo switch to using tasks with `std::async` so we can catch
     * exceptions.
     */
    for (auto&& t : threads_) {
      t.join();
    }
    threads_.clear();
  }

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

 private:
  std::atomic<bool> ready_to_run_{false};

  void make_ready_to_run() {
    ready_to_run_.store(true);
  }

  bool ready_to_run() {
    return ready_to_run_.load();
  }

  /** The worker thread routine */
  void worker() {
    [[maybe_unused]] thread_local static auto scheduler = this;
    thread_local ThrowCatchTask<Node> this_task{nullptr};

    /*
     * The worker threads wait on a condition variable until they are released
     * by a call to `sync_wait_all` (e.g.)
     */
    {
      std::unique_lock _(mutex_);
      start_cv_.wait(_, [this]() { return this->ready_to_run(); });
    }

    if (num_submitted_tasks_ == 0) {
      if (this->debug())
        std::cout << "No submissions, returning\n";
      return;
    }

    while (true) {
      {
        std::unique_lock _(mutex_);

        if (num_exited_tasks_ == num_submitted_tasks_) {
          if (this->debug()) {
            std::cout << "Breaking at top of while true with " +
                             std::to_string(num_exited_tasks_) +
                             " exited tasks and " +
                             std::to_string(num_submitted_tasks_) +
                             " submitted tasks\n";
            this->dump_queue_state("Breaking at top of while true");
          }
          break;
        }

        if (this->debug())
          this->dump_queue_state("About to get task");

        /*
         * Get a runnable task.
         * This is a blocking call, unless the queue is finished
         */
        auto val = this->get_runnable_task();

        if (!val) {
          if (this->debug())
            this->dump_queue_state("breaking with empty val");
          break;
        }

        auto task_to_run = *val;

        if (this->debug()) {
          this->dump_queue_state("Pre dispatch");
          task_to_run->dump_task_state();
        }

        /*
         * Transition task from runnable to running
         */
        this->task_dispatch(task_to_run);

        if (this->debug()) {
          this->dump_queue_state("Post dispatch");
          task_to_run->dump_task_state();
        }

        /*
         * Experimental throw-catch mechanism for cooperative multitasking.
         *
         * Executing tasks throw on
         */

        try {
          if (this->debug())
            task_to_run->dump_task_state("About to resume");

          _.unlock();
          task_to_run->resume();
          _.lock();

          if (this->debug())
            task_to_run->dump_task_state("Returning from resume");

        } catch (const detail::throw_catch_wait& w) {
          _.lock();

          assert(
              w.target() == detail::throw_catch_target::sink ||
              w.target() == detail::throw_catch_target::source);

          if (this->debug())
            task_to_run->dump_task_state("Caught wait");

          this->task_wait(task_to_run);

          if (this->debug())
            task_to_run->dump_task_state("Post wait");

        } catch (const detail::throw_catch_notify& n) {
          _.lock();

          assert(
              n.target() == detail::throw_catch_target::sink ||
              n.target() == detail::throw_catch_target::source);

          if (this->debug())
            task_to_run->dump_task_state("Caught notify");

          /** @note Notification goes to correspondent task of task_to_run */
          // auto task_to_notify = node_to_task[task_to_run->correspondent()];

          if (n.target() == detail::throw_catch_target::sink) {
            auto task_to_notify =
                node_to_task[task_to_run->sink_correspondent()];
            this->task_notify(task_to_notify);

            if (this->debug()) {
              task_to_run->dump_task_state("Post notify");
              task_to_notify->dump_task_state("Correspondent task");
            }

          } else {
            auto task_to_notify =
                node_to_task[task_to_run->source_correspondent()];
            this->task_notify(task_to_notify);

            if (this->debug()) {
              task_to_run->dump_task_state("Post notify");
              task_to_notify->dump_task_state("Correspondent task");
            }
          }

        } catch (const detail::throw_catch_exit& ex) {
          _.lock();

          if (true || ex.target() == detail::throw_catch_target::source) {
            if (this->debug()) {
              task_to_run->dump_task_state("Caught exit");
              this->dump_queue_state("Caught exit");
            }

            --num_tasks_;
            ++num_exited_tasks_;
            this->task_exit(task_to_run);

            if (this->debug()) {
              task_to_run->dump_task_state("Post exit");
              this->dump_queue_state("Post exit");
            }

            /* Slight optimization to skip call to yield when exiting */
            continue;
          } else {
            std::cout << "*** Caught sink exit\n";
          }

          // break; // Don't do this
        } catch (...) {
          throw;
        }

        if (this->debug()) {
          this->dump_queue_state("Pre yield");
          task_to_run->dump_task_state();
        }

        /*
         * @todo Perhaps deal with everything in yield?  Or dispatch?  Or both?
         */
        this->task_yield(task_to_run);

        if (this->debug()) {
          this->dump_queue_state("Post yield");
          task_to_run->dump_task_state();
        }
        if (num_exited_tasks_ == num_submitted_tasks_) {
          if (this->debug()) {
            std::cout << "Breaking at bottom of while true with " +
                             std::to_string(num_exited_tasks_) +
                             " exited tasks and " +
                             std::to_string(num_submitted_tasks_) +
                             " submitted tasks\n";
            this->dump_queue_state("Breaking at bottom of while true");
            task_to_run->dump_task_state();
          }

          break;
        }
      }  // End lock
    }    // end while(true);
    if (this->debug())
      std::cout << "Finished while (true)\n";

  }  // worker()

  /** Terminate threads in the thread pool */
  void shutdown() {
    if (this->debug())
      std::cout << "scheduler shutdown\n";

    /** Clear out any submitted tasks that haven't been put into the scheduler
     */
    this->make_ready_to_run();
    start_cv_.notify_all();

    // This doesn't seem to be necessary
    // All queues should be empty by the time we come to shutdown

    // Policy::p_shutdown();

    concurrency_level_.store(0);

    for (auto&& t : threads_) {
      t.join();
    }
    threads_.clear();
  }

  /** The worker threads */
  std::vector<std::thread> threads_;

  /** The maximum level of concurrency among all of the worker threads */
  std::atomic<size_t> concurrency_level_;

  /** Track number of tasks submited to scheduler */
  std::atomic<size_t> num_submitted_tasks_{0};

  /** Track number of tasks in the scheduler */
  std::atomic<size_t> num_tasks_{0};

  /** Track number of tasks that have exited the scheduler */
  std::atomic<size_t> num_exited_tasks_{0};

  /** Synchronization variables */
  std::mutex mutex_;
  std::condition_variable start_cv_;
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_throw_catch_H
