/**
 * @file   frugal.h
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
 * Implementation of a "frugal" scheduler.  This scheduler is similar to the
 * bountiful scheduler, but uses a fixed-size thread pool.
 *
 * This mostly a proof of concept, as there are issues related to starvation
 * deadlock and synchronization.
 */
#ifndef TILEDB_DAG_FRUGAL_H
#define TILEDB_DAG_FRUGAL_H

#include <future>
#include <iostream>
#include <thread>
#include <vector>

#include "experimental/tiledb/common/dag/execution/task_state_machine.h"
#include "experimental/tiledb/common/dag/execution/throw_catch_types.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/item_mover.h"

namespace tiledb::common {

template <class Policy>
struct PortPolicyTraits;

template <class Mover, class PortState>
class FrugalPortPolicy;

template <class Mover, class PortState>
struct PortPolicyTraits<FrugalPortPolicy<Mover, PortState>> {
  constexpr static bool wait_returns_{true};
};

/**
 * @brief A "frugal" scheduler.  This scheduler is similar to the bountiful
 * scheduler, but uses a fixed-size thread pool.
 *
 * @tparam Mover The type of data mover used to move data between nodes ports.
 * @tparam PortState The type of port state, either `PortState::two_stage` or
 * `PortState::three_stage`.
 */
template <class Mover, class PortState = typename Mover::PortState>
class FrugalPortPolicy : public PortFiniteStateMachine<
                             FrugalPortPolicy<Mover, PortState>,
                             PortState> {
  using mover_type = Mover;
  using scheduler_event_type = SchedulerAction;

  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;

  std::array<size_t, 2> moves_{0, 0};

 public:
  using state_machine_type =
      PortFiniteStateMachine<FrugalPortPolicy<Mover, PortState>, PortState>;
  using lock_type = typename state_machine_type::lock_type;

  FrugalPortPolicy() = default;
  FrugalPortPolicy(const FrugalPortPolicy&) {
  }
  FrugalPortPolicy(FrugalPortPolicy&&) noexcept = default;

  /**
   * Function for handling `ac_return` action.
   */
  inline scheduler_event_type on_ac_return(lock_type&, std::atomic<int>&) {
    return scheduler_event_type::noop;
  }

  /**
   * Function for handling `source_move` action.
   */
  inline scheduler_event_type on_source_move(
      lock_type&, std::atomic<int>& event) {
    moves_[0]++;
    static_cast<Mover*>(this)->on_move(event);
    return scheduler_event_type::noop;
  }

  /**
   * Function for handling `sink_move` action.
   */
  inline scheduler_event_type on_sink_move(
      [[maybe_unused]] lock_type& lock, std::atomic<int>& event) {
    assert(lock.owns_lock());
    moves_[1]++;
    static_cast<Mover*>(this)->on_move(event);
    return scheduler_event_type::noop;
  }

  /* Utility for testing, counts number of data transfers at source end. */
  [[nodiscard]] size_t source_swaps() const {
    return moves_[0];
  }

  /* Utility for testing, counts number of data transfers at source end. */
  [[nodiscard]] size_t sink_swaps() const {
    return moves_[1];
  }

  /**
   * Function for handling `notify_source` action.
   */
  inline scheduler_event_type on_notify_source(
      [[maybe_unused]] lock_type& lock, std::atomic<int>&) {
    assert(lock.owns_lock());
    source_cv_.notify_one();
    return scheduler_event_type::notify_source;
  }

  /**
   * Function for handling `notify_sink` action.
   */
  inline scheduler_event_type on_notify_sink(
      [[maybe_unused]] lock_type& lock, std::atomic<int>&) {
    assert(lock.owns_lock());

    // This assertion will fail when state machine is stopping, so check
    if (!static_cast<Mover*>(this)->is_stopping()) {
      assert(is_source_full(this->state()) == "");
    }
    sink_cv_.notify_one();
    return scheduler_event_type::notify_sink;
  }

  /**
   * Function for handling `source_wait` action.  Waits on cv.
   */
  inline scheduler_event_type on_source_wait(
      lock_type& lock, std::atomic<int>&) {
    assert(lock.owns_lock());
    if constexpr (std::is_same_v<PortState, two_stage>) {
      // CHECK(str(this->state()) == "st_11");
      assert(str(this->state()) == "st_11");
    } else if constexpr (std::is_same_v<PortState, three_stage>) {
      // CHECK(str(this->state()) == "st_111");
      assert(str(this->state()) == "st_111");
    }
    source_cv_.wait(lock);

    assert(is_source_post_move(this->state()) == "");

    return scheduler_event_type::source_wait;
  }

  /**
   * Function for handling `sink_wait` action.  Waits on cv.
   */
  inline scheduler_event_type on_sink_wait(lock_type& lock, std::atomic<int>&) {
    assert(lock.owns_lock());

    sink_cv_.wait(lock, [this]() {
      return done(this->state()) || terminating(this->state()) ||
             full_sink(this->state());
    });

    // assert(is_sink_post_move(this->state()) == "");

    return scheduler_event_type::sink_wait;
  }

  /**
   * Function for handling `term_source` action.  Here, we throw as in the
   * throw_catch scheduler. Since exit is an infrequent event, this should have
   * no impact on performance.
   */
  inline scheduler_event_type on_term_source(
      lock_type& lock, std::atomic<int>& event) {
    assert(lock.owns_lock());

    on_notify_sink(lock, event);
    throw throw_catch_source_exit;
    return scheduler_event_type::source_exit;
  }

  /**
   * Function for handling `term_sink` action.  Here, we throw as in the
   * throw_catch scheduler. Since exit is an infrequent event, this should have
   * no impact on performance.
   */
  inline scheduler_event_type on_term_sink(
      [[maybe_unused]] lock_type& lock, std::atomic<int>&) {
    assert(lock.owns_lock());
    // throw throw_catch_sink_exit;
    return scheduler_event_type::noop;
  }

 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << std::endl;
    }
  }
};

/*
 * Some traits for the frugal scheduler, necessary so that the `FrugalScheduler`
 * will compile.
 */
template <class T>
using FrugalMover3 = ItemMover<FrugalPortPolicy, three_stage, T>;
template <class T>
using FrugalMover2 = ItemMover<FrugalPortPolicy, two_stage, T>;

template <class Node>
class FrugalScheduler;

/*
 * Since we are just doing "bountiful" things but with fixed-size thread pool,
 * we don't have a policy as such.
 */
template <class Task>
class FrugalSchedulerPolicy;

template <typename T>
struct SchedulerTraits<FrugalSchedulerPolicy<T>> {
  using task_type = T;
  using task_handle_type = T;
};

/**
 * A scheduler that uses a fixed-size thread pool to execute tasks.
 * @tparam Node the type of node to be scheduled.
 */
template <class Node>
class FrugalScheduler {
  BoundedBufferQ<Node, std::queue<Node>, false> task_queue_;
  std::atomic<size_t> concurrency_level_;
  std::vector<std::thread> threads_;
  std::mutex mutex_;
  std::condition_variable start_cv_;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

 public:
  std::atomic<bool> debug_{false};

  /**
   * @brief Constructor
   *
   * @param concurrency_level The number of threads to use
   */
  explicit FrugalScheduler(size_t num_threads)
      : concurrency_level_(num_threads) {
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

    /*
     * Create the threads
     * */
    for (size_t i = 0; i < concurrency_level_; ++i) {
      std::thread tmp;

      size_t tries = 3;
      while (tries--) {
        try {
          tmp = std::thread(&FrugalScheduler::worker, this);
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

  ~FrugalScheduler() {
    shutdown();
  }

 private:
  std::atomic<bool> ready_to_run_{false};

  void make_ready_to_run() {
    ready_to_run_.store(true);
  }

  inline bool ready_to_run() const {
    return ready_to_run_.load();
  }

  std::atomic<size_t> num_tasks_{0};
  /**
   * @brief Main loop for the worker thread
   */
  void worker() {
    /*
     * Wait for the start signal
     */
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (this->debug_enabled())
        std::cout << "Waiting for start signal: ready to run = "
                  << this->ready_to_run() << "\n";
      start_cv_.wait(lock, [this]() { return this->ready_to_run(); });
      if (this->debug_enabled())
        std::cout << "Got start signal\n";
    }
    while (true) {
      std::unique_lock lock(mutex_);

      /* Get a task from the task queue. */
      lock.unlock();
      auto val = task_queue_.pop();
      lock.lock();

      /* If the queue has been shutdown, we are done. */
      if (!val) {
        break;
      }

      try {
        lock.unlock();
        (*val)->resume();
        lock.lock();
      } catch (const detail::throw_catch_exit& n) {
        lock.lock();

        /* Check if all tasks have completed. */
        --num_tasks_;
        if (num_tasks_ == 0) {
          lock.unlock();
          task_queue_.drain();
          break;
        }
        continue;
      }
      lock.unlock();
      task_queue_.push(*val);
    }
  }

  /**
   * @brief Finish running all tasks and shutdown the scheduler
   */
  void shutdown() {
    task_queue_.drain();

    this->make_ready_to_run();
    start_cv_.notify_all();

    for (auto&& t : threads_) {
      t.join();
    }
    concurrency_level_.store(0);
    threads_.clear();
  }

 public:
  /**
   * @brief Turn on debug mode.
   */
  void enable_debug() {
    debug_.store(true);
  }

  /**
   * @brief Turn off debug mode.
   */
  void disable_debug() {
    debug_.store(false);
  }

  /**
   * @brief Get state of debug mode.
   */
  bool debug_enabled() {
    return debug_.load();
  }

  /**
   * @brief Submit a task to the scheduler.
   *
   * @param node The task to submit.
   */
  void submit(Node&& n) {
    if (debug_enabled()) {
      std::cout << "Submitting node " << n->id() << std::endl;
    }
    task_queue_.push(n);
  }

  /**
   * @brief Launch tasks that have been submitted.
   */
  void sync_wait_all() {
    num_tasks_.store(task_queue_.size());

    this->make_ready_to_run();
    {
      std::unique_lock<std::mutex> lock(mutex_);
      start_cv_.notify_all();
    }

    for (auto&& t : threads_) {
      t.join();
    }
    threads_.clear();
  }
};
}  // namespace tiledb::common

#endif  // TILEDB_DAG_FRUGAL_H
