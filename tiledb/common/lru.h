/*
  TODO:
  - document shared_ptr is not necessary for the cache, it's part of the
  initial design, might go away in the future.

 Usage:
 auto f = [](std::string key) {
    return {sizeof(uint64_t), std::make_shared<uint64_t>(new uint64_t)};
 };
 Evaluator<MaxEntriesCache<std::string, uint64_t, 100>, decltype(f)> eval;
 eval(f, "key");

 Evaluator<MemoryBudgetedCache<std::string, uint64_t, 2048>, decltype(f)> eval;
 eval(f, "key");
*/
template <class Key, class Value>
class CachePolicyBase {
 public:
  using key_type = Key;
  using value_type = Value;
  using List = std::list<Key>;

  using OndemandValue = std::shared_future<std::shared_ptr<Value>>;
  using Cached_Entry = std::tuple<List::iterator, std::shared_ptr<Value>>;

  /**
   * Try to get an entry from the cache given a key or
   * execute the callback to fetch the value if the key is not present
   * and cache the value for future readers.
   * If the entry exists, this operation moves this entry
   * at the end of the internal LRU list.
   */
  template <class F>
  shared_ptr<Value> operator()(F f, Key key) {
    std::unique_lock<std::mutex> lock(mutex_);

    auto it = entries_.find(key);
    // Cache hit
    if (it != entries_.end()) {
      lru_.erase(std::get<0>(it->second));
      lru_.push_back(key);
      return std::get<1>(it->second);
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
    std::promise<std::shared_ptr<Value>> promise;
    auto emplace_pair = inprogress_.emplace(key, promise.get_future().share());

    // Unlock the mutex before evaluating the callback
    lock.unlock();

    // Then compute the value
    auto [mem_usage, value] = f(key);

    // Aquire again the lock to update the cache
    lock.lock();

    // Enforce caching policy
    shared_ptr<Value> evicted_value = enforce_policy(mem_usage);

    // Update the LRU list and the cache
    auto lru_it = lru_.emplace_back(key);
    entries_.emplace(key, std::make_tuple(lru_it, value));
    promise.set_value(value);

    // Remove the inprogress entry
    inprogress_.erase(emplace_pair.first);

    // Unlock the mutex before going out of scope so that the evicted value
    // gets destroyed and deallocated ouside critical sections and we don't
    // keep the lock longer than necessary.
    lock.unlock();

    return value;
  }

 private:
  /**
   * Enforces the cache policy by evicting entries if necessary.
   * This operation assumes the bookkeeping mutex is already locked.
   */
  virtual shared_ptr<Value> enforce_policy(size_t mem_usage) = 0;

  /**
   * Pops the head of the LRU list and removes the corresponding
   * entry from the cache.
   * This operation assumes the bookkeeping mutex is already locked.
   *
   * The evicted value is returned to the caller so that it can be
   * kept alive for as long as necessary.
   */
  shared_ptr<Value> evict() {
    auto key = lru_.front();
    auto it = entries_.find(key);
    auto value = std::get<1>(it->second);
    entries_.erase(it);
    lru_.pop_front();
    return value;
  }

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

  /** Bookkeeping lock */
  std::mutex mutex_;
};

template <class Key, class Value>
class NoCachePolicy : public CachePolicyBase<Key, Value> {
 public:
  NoCachePolicy() = default;

  template <class F>
  virtual shared_ptr<Value> operator()(F f, Key key) override {
    return f(key);
  }

  virtual shared_ptr<Value> enforce_policy(size_t mem_usage) override {
    return nullptr;
  }
};

template <class Key, class Value, size_t N = 0>
class MaxEntriesCache : public CachePolicyBase<Key, Value> {
 public:
  MaxEntriesCache()
      : num_entries_(0) {
  }

  virtual shared_ptr<Value> enforce_policy(
      [[maybe_unused]] size_t mem_usage) override {
    if (num_entries_ == max_entries_) {
      auto evicted_value = evict();
      --num_entries_;
      return evicted_value;
    }
    ++num_entries_;
    return nullptr;
  }

 private:
  size_t num_entries_;
  static size_t max_entries_ = N;
};

template <class Key, class Value, class N = 1024>
class MemoryBudgetedCache : public CachePolicyBase<Key, Value> {
 public:
  MemoryBudgetedCache()
      : memory_consumed_(0) {
  }

  virtual shared_ptr<Value> enforce_policy(size_t mem_usage) override {
    if (memory_consumed_ + mem_usage > memory_budget_) {
      auto evicted_value = evict();
      memory_consumed_ -= mem_usage;
      return evicted_value;
    }

    memory_consumed_ += mem_usage;
    return nullptr;
  }

 private:
  /** The maximum amount of bytes managed by this cache */
  static size_t memory_budget_ = N;

  /** The maximum amount of bytes currently consumed by this cache */
  size_t memory_consumed_;
};

template <class Policy, class F>
class Evaluator {
 public:
  using Key = Policy::key_type;
  using Value = Policy::value_type;

  Evaluator()
      : caching_policy_{} {
  }

  /**
   * Evaluate callback on key and return the value.
   * According to the policy specified, the value might be fetched
   * directly from the cache or the callback might be executed to fetch it.
   */
  std::shared_ptr<Value> operator()(F f, Key key) {
    return caching_policy_.template operator()<F>(f, key);
  }

 private:
  Policy caching_policy_;
};
