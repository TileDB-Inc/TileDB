/**
 * @file   bounded_buffer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2022 TileDB, Inc.
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
 * This file declares a classic/basic generic bounded-buffer producer-consumer
 * queue. The class supports a purely unbounded option.
 */

#ifndef TILEDB_BOUNDED_BUFFER_H
#define TILEDB_BOUNDED_BUFFER_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <type_traits>

#include <deque>
#include <queue>

namespace tiledb::common {

template <class Item, class Container, bool Bounded>
class BoundedBufferQ {
 public:
  template <bool B = Bounded>
  explicit BoundedBufferQ(size_t max_size, std::enable_if_t<B, void*> = nullptr)
      : max_size_(max_size) {
  }

  /**
   * Constructor for bounded buffer
   */
  template <bool B = Bounded>
  explicit BoundedBufferQ(std::enable_if_t<!B, void*> = nullptr)
      : max_size_(0) {
  }

  BoundedBufferQ(const BoundedBufferQ&) = delete;
  BoundedBufferQ& operator=(const BoundedBufferQ&) = delete;

  /**
   * Copy Constructor
   */
  BoundedBufferQ(BoundedBufferQ&& rhs) noexcept
      : max_size_(rhs.max_size_)
      , queue_(std::move(rhs.queue_))
      , draining_{rhs.draining_.load()}
      , shutdown_{rhs.shutdown_.load()} {
  }

  /**
   * Push an item onto the producer-consumer queue.
   *
   * @param item Item to be pushed onto the queue.
   * @return bool indicating whether the item was successfully pushed or not.
   */
  bool push(const Item& item) {
    std::unique_lock lock{mutex_};

    if constexpr (Bounded) {
      if (queue_.size() >= max_size_) {
        full_cv_.wait(lock, [this]() {
          return queue_.size() < max_size_ || draining_ || shutdown_;
        });
      }
    }
    if (draining_ || shutdown_) {
      return false;
    }

    if constexpr (std::is_same_v<Container, std::queue<Item>>) {
      queue_.push(item);
    } else if constexpr (std::is_same_v<Container, std::deque<Item>>) {
      queue_.push_front(item);
    } else {
      // Compile-time error if neither std::queue nor std::deque
      queue_.no_push(item);
    }

    empty_cv_.notify_one();
    return true;
  }

  /**
   * Try to push an item onto the producer-consumer queue.
   *
   * @param item Item to be pushed onto the queue.
   * @return bool indicating whether the item was successfully pushed or not.
   */
  bool try_push(const Item& item) {
    std::unique_lock lock{mutex_};

    if constexpr (Bounded) {
      if (queue_.size() >= max_size_) {
        return false;
      }
    }
    if (draining_ || shutdown_) {
      return false;
    }

    /*
     * Bounded queue_ has space for a new item.
     * Or, there is no upper bound so push will always succeed.
     */
    if constexpr (std::is_same_v<Container, std::queue<Item>>) {
      queue_.push(item);
    } else if constexpr (std::is_same_v<Container, std::deque<Item>>) {
      queue_.push_front(item);
    } else {
      // Compile-time error if neither std::queue nor std::deque
      queue_.no_push(item);
    }

    empty_cv_.notify_one();
    return true;
  }

  /**
   * Try to pop an item from the queue.  If no item is available
   * the function returns nothing.  It does not sleep.
   *
   * @returns Item from the queue, if available, otherwise nothing.
   */
  std::optional<Item> try_pop() {
    std::scoped_lock lock{mutex_};

    if (queue_.empty() || draining_ || shutdown_) {
      return {};
    }
    Item item = queue_.front();

    if constexpr (std::is_same_v<Container, std::queue<Item>>) {
      queue_.pop();
    } else if constexpr (std::is_same_v<Container, std::deque<Item>>) {
      queue_.pop_front();
    } else {
      // Compile-time error if neither std::queue nor std::deque
      queue_.no_pop(item);
    }

    full_cv_.notify_one();
    return item;
  }

  /**
   * Pop an item from the queue.  If the queue is empty, the calling
   * thread will wait on a condition variable until an item becomes
   * available.  If the queue is empty and the queue is closed
   * (shutting down), nothing is returned.  If the queue is not
   * empty and the queue is closed, an item will be returned.
   *
   * @returns Item from the queue, if available, otherwise nothing.
   */
  std::optional<Item> pop() {
    std::unique_lock lock{mutex_};

    empty_cv_.wait(
        lock, [this]() { return !queue_.empty() || draining_ || shutdown_; });

    if ((draining_ && queue_.empty()) || shutdown_) {
      return {};
    }
    Item item = queue_.front();

    if constexpr (std::is_same_v<Container, std::queue<Item>>) {
      queue_.pop();
    } else if constexpr (std::is_same_v<Container, std::deque<Item>>) {
      queue_.pop_front();
    } else {
      // Compile-time error if neither std::queue nor std::deque
      queue_.no_pop(item);
    }

    full_cv_.notify_one();
    return item;
  }

  /**
   * Pop an item from the back of the queue (if using a deque).
   * If the queue is empty, the calling
   * thread will wait on a condition variable until an item becomes
   * available.  If the queue is empty and the queue is closed
   * (shutting down), nothing is returned.  If the queue is not
   * empty and the queue is closed, an item will be returned.
   *
   * @returns Item from the queue, if available, otherwise nothing.
   */
  template <class Q = Item>
  typename std::enable_if<
      std::is_same_v<Container, std::deque<Q>>,
      std::optional<Q>>::type
  pop_back() {
    std::unique_lock lock{mutex_};

    empty_cv_.wait(
        lock, [this]() { return !queue_.empty() || draining_ || shutdown_; });

    if ((draining_ && queue_.empty()) || shutdown_) {
      return {};
    }
    Item item = queue_.back();
    queue_.pop_back();

    full_cv_.notify_one();
    return item;
  }

  /**
   * Try to pop an item from the back of the queue (if using a deque).
   * If the queue is empty or if it is closed (shutting down), return
   * nothing.
   *
   * @returns Item from the queue, if available, otherwise nothing.
   */
  template <class Q = Item>
  typename std::enable_if<
      std::is_same_v<Container, std::deque<Q>>,
      std::optional<Q>>::type
  try_pop_back() {
    std::unique_lock lock{mutex_};

    if (queue_.empty() || draining_ || shutdown_) {
      return {};
    }
    Item item = queue_.back();
    queue_.pop_back();

    full_cv_.notify_one();
    return item;
  }

  inline size_t size() const {
    return queue_.size();
  }

  /**
   * Swap only data contents.  Synchronization variables must all be empty.
   */
  void swap_data(BoundedBufferQ& rhs) {
    std::scoped_lock lock{mutex_};
    std::swap(max_size_, rhs.max_size_);
    std::swap(queue_, rhs.queue_);
  }

  /**
   * Soft shutdown of the queue.  The queue is closed and all threads waiting on
   * items are notified.  Any threads waiting on pop() will then return nothing.
   */
  void drain() {
    std::scoped_lock lock{mutex_};
    draining_ = true;
    empty_cv_.notify_all();
    full_cv_.notify_all();
  }

  /**
   * Hard shutdown of the queue.  The queue is closed and all threads waiting on
   * items are notified.  Any threads waiting on pop() will then return nothing.
   */
  void shutdown() {
    std::scoped_lock lock{mutex_};
    shutdown_ = true;
    empty_cv_.notify_all();
    full_cv_.notify_all();
  }

  /**
   * @todo: Conditionally include max_size and full_cv.
   */
 private:
  size_t max_size_;
  Container queue_;
  std::condition_variable empty_cv_;
  std::condition_variable full_cv_;
  mutable std::mutex mutex_;
  std::atomic<bool> draining_{false};
  std::atomic<bool> shutdown_{false};
};

template <class Item, class Container = std::deque<Item>>
using ProducerConsumerQueue = BoundedBufferQ<Item, Container, false>;

template <class Item, class Container = std::deque<Item>>
using BoundedBuffer = BoundedBufferQ<Item, Container, true>;

}  // namespace tiledb::common

#endif  // TILEDB_BOUNDED_BUFFER_H
