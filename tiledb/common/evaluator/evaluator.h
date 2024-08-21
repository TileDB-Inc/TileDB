/**
 * @file   tiledb/common/evaluator/evaluator.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines a function evaluator class and a set of caching policies
 * that can be used to configure the evaluator's behavior.
 */

#ifndef TILEDB_COMMON_EVALUATOR_H
#define TILEDB_COMMON_EVALUATOR_H

#include <future>
#include <list>

namespace tiledb::common {

/**
 * Abstract class for caching policies.
 * This contains most of the logic of the caching mechanism
 * except budgeting which is policy specific.
 *
 * @tparam Key The type of key
 * @tparam Value The type of value
 */
template <class Key, class Value>
class CachePolicyBase {
 public:
  using key_type = Key;
  using value_type = Value;
  using return_type = std::shared_ptr<Value>;
  using List = std::list<Key>;
  using OndemandValue = std::shared_future<return_type>;

  /**
   * Try to get an entry from the cache given a key or
   * execute the callback to fetch the value if the key is not present
   * and cache the value for future readers.
   * If the entry exists, this operation moves this entry
   * at the end of the internal LRU list.
   *
   * @tparam F The callback function type.
   * @param f The callback function to evaluate.
   * @param key The key to evaluate the callback on.
   */
  template <class F>
  return_type operator()(F f, Key key) {
    std::promise<return_type> promise;
    std::pair<typename decltype(inprogress_)::iterator, bool> emplace_pair;
    {
      std::unique_lock<std::mutex> lock(mutex_);

      auto it = entries_.find(key);
      // Cache hit
      if (it != entries_.end()) {
        lru_.erase(it->second.lru_it);
        lru_.push_back(key);
        it->second.lru_it = std::prev(lru_.end());
        return it->second.value;
      }

      // Cache miss, but result is already in progress
      auto inprogress_it = inprogress_.find(key);
      if (inprogress_it != inprogress_.end()) {
        // Unlock the mutex before waiting
        auto future_val = inprogress_it->second;
        lock.unlock();

        // Wait for the value to be ready
        return future_val.get();
      }

      // Eval the callback and cache the result
      // First let others know we're working on it
      emplace_pair = inprogress_.emplace(key, promise.get_future().share());
    }

    // Then compute the value
    auto value = make_shared<value_type>(f(key));

    // Aquire again the mutex to update the cache
    {
      std::lock_guard<std::mutex> lock(mutex_);

      // Enforce caching policy
      enforce_policy(*value);

      // Update the LRU list and the cache
      lru_.emplace_back(key);
      entries_.emplace(key, Cached_Entry(std::prev(lru_.end()), value));
      promise.set_value(value);

      // Remove the inprogress entry
      inprogress_.erase(emplace_pair.first);
    }

    return value;
  }

 protected:
  /**
   * Pops the head of the LRU list and removes the corresponding
   * entry from the cache.
   * This operation assumes the bookkeeping mutex is already locked.
   *
   * The evicted value is returned to the caller so that it can be
   * kept alive for as long as necessary.
   *
   * @return The evicted value.
   */
  return_type evict() {
    if (lru_.empty()) {
      throw std::runtime_error("Cannot evict from an empty cache.");
    }

    auto key = lru_.front();
    auto it = entries_.find(key);
    auto value = it->second.value;
    entries_.erase(it);
    lru_.pop_front();
    return value;
  }

  /**
   * Entry in the cache.
   * It stores an iterator to the LRU list and the value.
   */
  struct Cached_Entry {
    typename List::iterator lru_it;
    return_type value;
    Cached_Entry() = delete;
    Cached_Entry(typename List::iterator it, return_type v)
        : lru_it(it)
        , value(v) {
    }
  };

  /**
   * Enforces the cache policy by evicting entries if necessary.
   * This operation assumes the bookkeeping mutex is already locked.
   */
  virtual void enforce_policy(const Value& v) = 0;

  /**
   * Double linked list where the head is the least
   * recently used element from the cache.
   */
  List lru_;

  /**
   * Each entry stores a promised value which is in the process of
   * being fetched. Readers will wait on it to become available.
   */
  std::unordered_map<Key, OndemandValue> inprogress_;

  /**
   * Map of cached entries by key. Each entry stores and iterator
   * within the LRU list and a shared_ptr to the value.
   * If an entry is in this map, it is guaranteed that the value is
   * already available to serve readers.
   */
  std::unordered_map<Key, Cached_Entry> entries_;

 private:
  /** Bookkeeping lock */
  std::mutex mutex_;
};

/**
 * Passing this policy to the evaluator will configure it to
 * always evaluate the callback function and never cache the results.
 *
 * @tparam Key The type of key
 * @tparam Value The type of value
 */
template <class Key, class Value>
class ImmediateEvaluation {
 public:
  using key_type = Key;
  using value_type = Value;
  using return_type = std::shared_ptr<Value>;

  /**
   * Evaluates the function and constructs a shared_ptr with the prduced value.
   *
   * @tparam F The callback function type.
   * @param f The callback function to evaluate.
   * @param key The key to evaluate the callback on.
   */
  template <class F>
  return_type operator()(F f, Key key) {
    return make_shared<value_type>(f(key));
  }
};

/**
 * Policy that enforces a maximum number of entries in the cache.
 * Once the threshold is reached, the LRY entry is evicted making space
 * for a new value to evaluate.
 *
 * This policy is useful for testing purposes.
 *
 * @tparam Key The type of key
 * @tparam Value The type of value
 */
template <class Key, class Value, size_t N = 0>
class MaxEntriesCache : public virtual CachePolicyBase<Key, Value> {
 public:
  using return_type = std::shared_ptr<Value>;

  MaxEntriesCache()
      : num_entries_(0) {
    if (N == 0) {
      throw std::logic_error(
          "The maximum number of entries must be greater than zero.");
    }
  }

  virtual void enforce_policy([[maybe_unused]] const Value& v) override {
    if (num_entries_ == max_entries_) {
      auto evicted_value = this->evict();
      --num_entries_;
      return;
    }
    ++num_entries_;
  }

 private:
  size_t num_entries_;
  static const size_t max_entries_ = N;
};

/**
 * Policy that enforces a memory budget for the cache.
 * This is considered to be the production policy which should
 * be used in all real use cases.
 *
 * The memory consumption is accounted using a user provided function
 * which returns how much a value uses plus the overhead of a shared_ptr.
 * In the near future, the evaluator will be extended with orthogonal
 * memory allocation policies for the values so the accounting for
 * shared_ptr overhead might be conditioned by these policies.
 *
 * The budget is enforced by evicting LRU entries from the cache.
 *
 * @tparam Key The type of key
 * @tparam Value The type of value
 */
template <class Key, class Value>
class MemoryBudgetedCache : public virtual CachePolicyBase<Key, Value> {
 public:
  using return_type = std::shared_ptr<Value>;
  using SizeFn = std::function<size_t(const Value&)>;

  MemoryBudgetedCache(SizeFn size_fn, size_t budget)
      : size_fn_(size_fn)
      , memory_budget_{budget}
      , memory_consumed_(0) {
    if (budget == 0) {
      throw std::logic_error("The memory budget must be greater than zero.");
    }
  }
  /**
   * Enforces the cache policy by evicting entries if necessary.
   * This function takes the value produced by the callback
   * to account its memory usage against the budget.
   *
   * @param v The value produced by the callback
   */
  virtual void enforce_policy(const Value& v) override {
    auto mem_usage = sizeof(return_type) + size_fn_(v);
    while (memory_consumed_ + mem_usage > memory_budget_) {
      if (CachePolicyBase<Key, Value>::entries_.empty()) {
        throw std::logic_error(
            "The memory consumed by this value exceeds the budget of the "
            "cache.");
      }

      auto evicted_value = this->evict();
      memory_consumed_ -= (size_fn_(*evicted_value) + sizeof(return_type));
    }

    // acount for the new value added in the cache
    memory_consumed_ += mem_usage;
  }

 protected:
  /** User provided function for calculating the size of a value */
  SizeFn size_fn_;

  /** The maximum amount of bytes managed by this cache */
  size_t memory_budget_;

  /** The maximum amount of bytes currently consumed by this cache */
  size_t memory_consumed_;
};

/**
 * This class evaluates the result of a callback function on a key,
 * caching might happen according to the policy specified.
 *
 * Any function that takes a key and returns a value can be passed here
 * provided that the function is reentrant and does RVO so that the caching
 * policy can construct efficiently the results it produces.
 *
 * @tparam Policy The caching policy to use.
 * @tparam F The callback function type.
 */
template <class Policy, class F>
class Evaluator {
 public:
  using Key = typename Policy::key_type;
  using Value = typename Policy::value_type;
  using return_type = typename Policy::return_type;

  /**
   * Constructor.
   *
   * @tparam Args Argument types for the policy constructor.
   * @param f The callback function to evaluate.
   * @param args Arguments to pass to the policy constructor.
   */
  template <class... Args>
  Evaluator(F f, Args&&... args)
      : caching_policy_{std::forward<Args>(args)...}
      , func_{f} {
  }

  /**
   * Evaluate callback on key and return the value.
   * According to the policy specified, the value might be fetched
   * directly from the cache or the callback might be executed to fetch it.
   *
   * @param key The key to evaluate the callback on.
   * @return The value of the callback on the key.
   */
  return_type operator()(Key key) {
    return caching_policy_.template operator()<F>(func_, key);
  }

 protected:
  Policy caching_policy_;
  F func_;
};

}  // namespace tiledb::common

#endif  // TILEDB_COMMON_EVALUATOR_H
