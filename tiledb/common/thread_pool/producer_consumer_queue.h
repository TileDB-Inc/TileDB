/**
 * @file   producer_consumer_queue.h
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
 * This file declares a classic/basic generic producer-consumer queue.
 */

#ifndef TILEDB_PRODUCER_CONSUMER_QUEUE_H
#define TILEDB_PRODUCER_CONSUMER_QUEUE_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <type_traits>

#include <deque>
#include <queue>

namespace tiledb::common {

template <class Item, class Container = std::deque<Item>>
class ProducerConsumerQueue {
 public:
  ProducerConsumerQueue() = default;
  ProducerConsumerQueue(const ProducerConsumerQueue<Item>&) = delete;
  ProducerConsumerQueue& operator=(const ProducerConsumerQueue<Item>&) = delete;

  /**
   * Push an item onto the producer-consumer queue.  This producer-consumer
   * queue is unbounded; there is no risk of the caller being put to sleep.  If
   * the queue is closed, the item is not pushed and false is returned.
   *
   * @param item Item to be pushed onto the queue.
   * @return bool indicating whether the item was successfully pushed or not.
   */
  bool push(const Item& item) {
    std::lock_guard lock{mutex_};
    if (closed_) {
      return false;
    }

    if constexpr (std::is_same<Container, std::queue<Item>>::value) {
      queue_.push(item);
    } else if constexpr (std::is_same<Container, std::deque<Item>>::value) {
      queue_.push_front(item);
    } else {
      // Compile-time error if neither std::queue nor std::deque
      queue_.no_push(item);
    }

    cv_.notify_one();
    return true;
  }

  /**
   * Push an item onto the producer-consumer queue.  This producer-consumer
   * queue is unbounded; there is no risk of the caller being put to sleep.  If
   * the queue is closed, the item is not pushed and false is returned.
   *
   * @param item Item to be pushed onto the queue.
   * @return bool indicating whether the item was successfully pushed or not.
   */
  bool push(Item&& item) {
    std::lock_guard lock{mutex_};
    if (closed_) {
      return false;
    }

    if constexpr (std::is_same<Container, std::queue<Item>>::value) {
      queue_.push(std::move(item));
    } else if constexpr (std::is_same<Container, std::deque<Item>>::value) {
      queue_.push_front(std::move(item));
    } else {
      // Compile-time error if neither std::queue nor std::deque
      queue_.no_push(item);
    }

    cv_.notify_one();
    return true;
  }

  /**
   * Try to pop an item from the queue.  If no item is available
   * the function returns nothing.  It does not sleep.
   *
   * @returns Item from the queue, if available, otherwise nothing.
   */
  std::optional<Item> try_pop() {
    std::lock_guard lock{mutex_};

    if (queue_.empty() || closed_) {
      return {};
    }
    Item item = queue_.front();

    if constexpr (std::is_same<Container, std::queue<Item>>::value) {
      queue_.pop(item);
    } else if constexpr (std::is_same<Container, std::deque<Item>>::value) {
      queue_.pop_front();
    } else {
      // Compile-time error if neither std::queue nor std::deque
      queue_.no_pop(item);
    }

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

    cv_.wait(lock, [this]() { return closed_ || !queue_.empty(); });

    if (closed_ && queue_.empty()) {
      return {};
    }
    Item item = std::move(queue_.front());

    if constexpr (std::is_same<Container, std::queue<Item>>::value) {
      queue_.pop(item);
    } else if constexpr (std::is_same<Container, std::deque<Item>>::value) {
      queue_.pop_front();
    } else {
      // Compile-time error if neither std::queue nor std::deque
      queue_.no_pop(item);
    }

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

    cv_.wait(lock, [this]() { return closed_ || !queue_.empty(); });

    if (closed_ && queue_.empty()) {
      return {};
    }
    Item item = queue_.back();
    queue_.pop_back();

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
    std::lock_guard lock{mutex_};

    if (queue_.empty() || closed_) {
      return {};
    }
    Item item = queue_.back();
    queue_.pop_back();

    return item;
  }

  /**
   * Shut down the queue.  The queue is closed and all threads waiting on items
   * are notified.  Any threads waiting on pop() will then return nothing.
   */
  void drain() {
    std::lock_guard lock{mutex_};
    closed_ = true;
    cv_.notify_all();
  }

 private:
  Container queue_;
  std::condition_variable cv_;
  mutable std::mutex mutex_;
  std::atomic<bool> closed_{false};
};

}  // namespace tiledb::common

#endif  // TILEDB_PRODUCER_CONSUMER_QUEUE_H
