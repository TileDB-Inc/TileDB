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

template <class Item>
class PriorityItem {
 public:
  PriorityItem(uint64_t depth, Item item) : depth_(depth), item_(item) {
  }

  bool operator<(const PriorityItem<Item>& r) const {
    return depth_ < r.depth_;
  }

  uint64_t depth_;
  Item item_;
};


template <class Item>
class ProducerConsumerQueue {
 public:
  ProducerConsumerQueue() = default;
  ProducerConsumerQueue(const ProducerConsumerQueue<Item>&) = delete;
  ProducerConsumerQueue& operator=(const ProducerConsumerQueue<Item>&) = delete;

  size_t size() {
    std::lock_guard lock{mutex_};
    return queue_.size();
  }

  /**
   * Push an item onto the producer-consumer queue.  This producer-consumer
   * queue is unbounded; there is no risk of the caller being put to sleep.  If
   * the queue is closed, the item is not pushed and false is returned.
   *
   * @param item Item to be pushed onto the queue.
   * @return bool indicating whether the item was successfully pushed or not.
   */
  bool push(const Item& item, uint64_t depth) {
    std::lock_guard lock{mutex_};
    if (closed_) {
      return false;
    }

    queue_.push(PriorityItem<Item>(depth, item));

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
  bool push(Item&& item, uint64_t depth) {
    std::lock_guard lock{mutex_};
    if (closed_) {
      return false;
    }

    queue_.push(PriorityItem<Item>(depth, std::move(item)));

    cv_.notify_one();
    return true;
  }

  /**
   * Try to pop an item from the queue.  If no item is available
   * the function returns nothing.  It does not sleep.
   *
   * @returns Item from the queue, if available, otherwise nothing.
   */
  std::optional<Item> try_pop(uint64_t depth) {
    std::lock_guard lock{mutex_};

    if (queue_.empty() || closed_) {
      return {};
    }

    auto pitem = queue_.top();
    if (pitem.depth_ > depth) {
      queue_.pop();
      return pitem.item_;
    }

    return {};
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
  std::optional<Item> pop(uint64_t depth) {
    std::unique_lock lock{mutex_};

    cv_.wait(lock, [this, depth]() {
      if (closed_) {
        return true;
      }

      if (queue_.empty()) {
        return false;
      }

      auto pitem = queue_.top();
      if (pitem.depth_ > depth) {
        return true;
      }

      return false;
    });

    if (closed_ && queue_.empty()) {
      return {};
    }

    auto pitem = queue_.top();
    queue_.pop();

    return pitem.item_;
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
  std::priority_queue<PriorityItem<Item>> queue_;
  std::condition_variable cv_;
  mutable std::mutex mutex_;
  std::atomic<bool> closed_{false};
};

}  // namespace tiledb::common

#endif  // TILEDB_PRODUCER_CONSUMER_QUEUE_H
