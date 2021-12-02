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

#define USE_DEQUE

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>

#ifdef USE_DEQUE
#include <deque>
#else
#include <queue>
#endif


namespace tiledb::common {

template <class Item>
class ProducerConsumerQueue {
 public:
  ProducerConsumerQueue() = default;
  ProducerConsumerQueue(const ProducerConsumerQueue<Item>&) = delete;
  ProducerConsumerQueue& operator=(const ProducerConsumerQueue<Item>&) =
      delete;

  auto push(const Item& item) {
    std::unique_lock lock{mutex_};
    if (!is_open_) {
      return false;
    }

#ifdef USE_DEQUE
    queue_.push_front(item);
#else
    queue_.push(item);
#endif

    // lock.unlock();
    cv_.notify_one();
    return true;
  }

  std::optional<Item> try_pop() {
    std::scoped_lock lock{mutex_};

    if (queue_.empty()) {
      return {};
    }
    Item item = queue_.front();
#ifdef USE_DEQUE
    queue_.pop_front();
#else
    queue_.pop();
#endif

    return item;
  }

  std::optional<Item> pop() {
    std::unique_lock lock{mutex_};

    cv_.wait(lock, [this]() { return !is_open_ || !queue_.empty(); });

    if (!is_open_ && queue_.empty()) {
      return {};
    }
    Item item = queue_.front();
#ifdef USE_DEQUE
    queue_.pop_front();
#else
    queue_.pop();
#endif
    return item;
  }

  bool is_open() const {
    std::scoped_lock lock{mutex_};
    return is_open_;
  }

  void drain() {
    std::scoped_lock lock{mutex_};
    is_open_ = false;
    // lock.unlock();
    cv_.notify_all();
  }

  void signal_one() {
    cv_.notify_one();
  }

  void signal_all() {
    cv_.notify_all();
  }

 private:
  bool empty() const {
    std::scoped_lock lock{mutex_};
    return queue_.empty();
  }
  size_t size() const {
    std::scoped_lock lock{mutex_};
    return queue_.size();
  }

 private:
#ifdef USE_DEQUE
  std::deque<Item> queue_;
#else
  std::queue<Item> queue_;
#endif
 std::condition_variable cv_;
  mutable std::mutex mutex_;
  std::atomic<bool> is_open_{true};
};

}  // namespace tiledb::common

#endif // TILEDB_PRODUCER_CONSUMER_QUEUE_H
