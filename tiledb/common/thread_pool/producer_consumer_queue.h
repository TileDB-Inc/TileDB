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
  auto push(const Item& item) {
    std::unique_lock lock{mutex_};
    if (!is_open_) {
      return false;
    }

    if constexpr (std::is_same<Container, std::queue<Item>>::value) {
      queue_.push(item);
    } else if constexpr (std::is_same<Container, std::deque<Item>>::value) {
      queue_.push_front(item);
    } else {
      queue_.no_push(item);
    }

    // lock.unlock();
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
    std::scoped_lock lock{mutex_};

    if (queue_.empty()) {
      return {};
    }
    Item item = queue_.front();

    if constexpr (std::is_same<Container, std::queue<Item>>::value) {
      queue_.pop(item);
    } else if constexpr (std::is_same<Container, std::deque<Item>>::value) {
      queue_.pop_front();
    } else {
      queue_.no_pop(item);
    }

    return item;
  }

  /**
   * Pop an item from the queue.  If the queue is closed (shutting down), return
   * nothing.  Otherwise, if no item is available, the calling hread is put to
   * sleep until an item is available.
   *
   * @returns Item from the queue, if available, otherwise nothing.
   */
  std::optional<Item> pop() {
    std::unique_lock lock{mutex_};

    cv_.wait(lock, [this]() { return !is_open_ || !queue_.empty(); });

    if (!is_open_ && queue_.empty()) {
      return {};
    }
    Item item = queue_.front();

    if constexpr (std::is_same<Container, std::queue<Item>>::value) {
      queue_.pop(item);
    } else if constexpr (std::is_same<Container, std::deque<Item>>::value) {
      queue_.pop_front();
    } else {
      queue_.no_pop(item);
    }

    return item;
  }

  /**
   * Shut down the queue.  The queue is closed and all threads waiting on items
   * are notified.  Any threads waiting on pop() will then return nothing.
   */
  void drain() {
    std::scoped_lock lock{mutex_};
    is_open_ = false;
    // lock.unlock();
    cv_.notify_all();
  }

 private:
  Container queue_;
  std::condition_variable cv_;
  mutable std::mutex mutex_;
  std::atomic<bool> is_open_{true};
};

}  // namespace tiledb::common

#endif  // TILEDB_PRODUCER_CONSUMER_QUEUE_H
