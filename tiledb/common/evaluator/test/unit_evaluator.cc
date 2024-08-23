/**
 * @file   tiledb/common/evaluator/test/unit_evaluator.cc
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
 */

#include "test/support/tdb_catch.h"
#include "tiledb/common/dynamic_memory/dynamic_memory.h"
#include "tiledb/common/evaluator/evaluator.h"

using namespace tiledb::common;

template <class Key, class Value>
class WhiteBoxPolicy : public virtual CachePolicyBase<Key, Value> {
  using base = CachePolicyBase<Key, Value>;

 public:
  [[nodiscard]] bool has_entry(const Key& key) {
    std::lock_guard<std::mutex> lock(base::mutex_);
    return base::entries_.find(key) != base::entries_.end();
  }
  [[nodiscard]] bool entry_in_progress(const Key& key) {
    std::lock_guard<std::mutex> lock(base::mutex_);
    return base::inprogress_.find(key) != base::inprogress_.end();
  }
  [[nodiscard]] size_t entries_size() {
    std::lock_guard<std::mutex> lock(base::mutex_);
    return base::entries_.size();
  }
  [[nodiscard]] size_t inprogress_size() {
    std::lock_guard<std::mutex> lock(base::mutex_);
    return base::inprogress_.size();
  }
  [[nodiscard]] bool is_mru(const Key& key) {
    std::lock_guard<std::mutex> lock(base::mutex_);
    return base::lru_.back() == key;
  }
  [[nodiscard]] bool lru_contains(const Key& key) {
    std::lock_guard<std::mutex> lock(base::mutex_);
    return std::find(base::lru_.begin(), base::lru_.end(), key) !=
           base::lru_.end();
  }

  void wait_till_numreaders(const Key& key, size_t num) {
    while (1) {
      {
        std::lock_guard<std::mutex> lock(base::mutex_);
        auto it = base::inprogress_.find(key);
        if (it == base::inprogress_.end()) {
          throw std::runtime_error("Key not found in inprogress list");
        }

        if (it->second.num_readers == num) {
          return;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
};

template <class Key, class Value, size_t N = 0>
class WhiteBoxMaxEntriesPolicy : public WhiteBoxPolicy<Key, Value>,
                                 public MaxEntriesCache<Key, Value, N> {
  using base = MaxEntriesCache<Key, Value, N>;

 public:
  [[nodiscard]] size_t num_entries() {
    std::lock_guard<std::mutex> lock(base::mutex_);
    return base::num_entries_;
  }
  void evict_lru() {
    std::lock_guard<std::mutex> lock(base::mutex_);
    base::enforce_policy(Value{});
  }
};

template <class Key, class Value, class SizeFn>
class WhiteBoxMemoryBudgetPolicy
    : public WhiteBoxPolicy<Key, Value>,
      public MemoryBudgetedCache<Key, Value, SizeFn> {
  using base = MemoryBudgetedCache<Key, Value, SizeFn>;

 public:
  WhiteBoxMemoryBudgetPolicy(SizeFn size_fn, size_t budget)
      : base(size_fn, budget) {
  }

  [[nodiscard]] size_t memory_consumed() {
    std::lock_guard<std::mutex> lock(base::mutex_);
    return base::memory_consumed_;
  }
  void evict_lru() {
    std::lock_guard<std::mutex> lock(base::mutex_);
    base::enforce_policy(Value{});
  }
};

template <class Key, class Value>
class WhiteBoxImmediateEvaluationPolicy
    : public ImmediateEvaluation<Key, Value> {
  using base = ImmediateEvaluation<Key, Value>;

 public:
  using return_type = typename base::return_type;

  WhiteBoxImmediateEvaluationPolicy()
      : num_executions_(0) {
  }

  template <class F>
  return_type operator()(F f, Key key) {
    ++num_executions_;
    return base::operator()(f, key);
  }

  uint64_t num_executions() const {
    return num_executions_;
  }

 private:
  uint64_t num_executions_;
};

template <class Policy, class F>
class WhiteBoxEvaluator : public Evaluator<Policy, F> {
  using base = Evaluator<Policy, F>;
  using return_type = typename Policy::return_type;
  using Key = typename Policy::key_type;

 public:
  template <class... Args>
  WhiteBoxEvaluator(F f, Args&&... args)
      : base(std::forward<F>(f), std::forward<Args>(args)...) {
  }

  return_type operator()(const Key& key) {
    return base::operator()(key);
  }

  [[nodiscard]] Policy& policy() {
    return base::caching_policy_;
  }
};

TEST_CASE("Testing immediate evaluation", "[evaluator][immediate]") {
  auto f = [](const std::string& key) {
    static uint64_t counter = 0;  // safe only in this testing scenario
    return "test-" + key + std::to_string(counter++);
  };

  WhiteBoxEvaluator<
      WhiteBoxImmediateEvaluationPolicy<std::string, std::string>,
      decltype(f)>
      no_cache_eval(f);
  REQUIRE(no_cache_eval.policy().num_executions() == 0);
  auto v1 = no_cache_eval("key");
  CHECK(no_cache_eval.policy().num_executions() == 1);
  CHECK(*v1 == "test-key0");
  auto v2 = no_cache_eval("key");
  CHECK(no_cache_eval.policy().num_executions() == 2);
  CHECK(*v2 == "test-key1");
}

TEST_CASE("Ready cached items are stored properly", "[evaluator][entries]") {
  auto f = [](std::string) { return std::string("test"); };
  WhiteBoxEvaluator<
      WhiteBoxMaxEntriesPolicy<std::string, std::string, 2>,
      decltype(f)>
      max_entries_eval(f);

  max_entries_eval("key");
  CHECK(max_entries_eval.policy().entries_size() == 1);
  CHECK(max_entries_eval.policy().has_entry("key"));

  max_entries_eval("key2");
  CHECK(max_entries_eval.policy().entries_size() == 2);
  CHECK(max_entries_eval.policy().has_entry("key"));
  CHECK(max_entries_eval.policy().has_entry("key2"));

  // A cache hit doesn't change the content of the entries map
  max_entries_eval("key");
  CHECK(max_entries_eval.policy().entries_size() == 2);
  CHECK(max_entries_eval.policy().has_entry("key"));
  CHECK(max_entries_eval.policy().has_entry("key2"));

  // Capacity reached, key should be evicted
  max_entries_eval("key3");
  CHECK(max_entries_eval.policy().entries_size() == 2);
  CHECK(max_entries_eval.policy().has_entry("key"));
  CHECK(max_entries_eval.policy().has_entry("key3"));
}

TEST_CASE("Testing LRU list is accurate", "[evaluator][lru]") {
  auto f = [](std::string) { return std::string("test"); };
  WhiteBoxEvaluator<
      WhiteBoxMaxEntriesPolicy<std::string, std::string, 2>,
      decltype(f)>
      max_entries_eval(f);

  max_entries_eval("key");
  CHECK(max_entries_eval.policy().entries_size() == 1);
  CHECK(max_entries_eval.policy().has_entry("key"));
  CHECK(max_entries_eval.policy().is_mru("key"));
  CHECK(max_entries_eval.policy().inprogress_size() == 0);

  max_entries_eval("key2");
  CHECK(max_entries_eval.policy().is_mru("key2"));

  max_entries_eval("key");
  CHECK(max_entries_eval.policy().is_mru("key"));

  // Evict, key is now MRU
  CHECK(max_entries_eval.policy().entries_size() == 2);
  max_entries_eval.policy().evict_lru();
  CHECK(max_entries_eval.policy().entries_size() == 1);
  CHECK(max_entries_eval.policy().is_mru("key"));

  max_entries_eval("key3");
  CHECK(max_entries_eval.policy().is_mru("key3"));

  // Max entries reached, key should be evicted
  CHECK(max_entries_eval.policy().lru_contains("key"));
  max_entries_eval("key4");
  CHECK(max_entries_eval.policy().lru_contains("key") == false);
  CHECK(max_entries_eval.policy().is_mru("key4"));
  CHECK(max_entries_eval.policy().entries_size() == 2);
}

TEST_CASE(
    "Inprogress list contains items that are in the process of being evaluated",
    "[evaluator][inprogress]") {
  std::promise<void> finish_I_promise;
  std::promise<void> ready_promise;
  auto f = [&finish_I_promise, &ready_promise](std::string) {
    ready_promise.set_value();
    finish_I_promise.get_future().wait();
    return std::string("test");
  };
  WhiteBoxEvaluator<
      WhiteBoxMaxEntriesPolicy<std::string, std::string, 2>,
      decltype(f)>
      eval(f);

  std::thread t([&eval]() {
    // This blocks until f(key) is evaluated
    eval("key");
  });

  // Give the thread some heads up
  ready_promise.get_future().wait();

  // Now that key is in the process of evaluation, let's do some checks
  CHECK(eval.policy().inprogress_size() == 1);
  CHECK(eval.policy().entry_in_progress("key"));
  CHECK(eval.policy().entries_size() == 0);

  // Unblock the callback so that the evaluation can finish and the entry gets
  // cached
  finish_I_promise.set_value();

  t.join();

  // The item moved from inprogress to ready
  CHECK(eval.policy().entries_size() == 1);
  CHECK(eval.policy().inprogress_size() == 0);
}

TEST_CASE(
    "Readers that come once an entry is in progress are blocked until the "
    "entry is ready",
    "[evaluator][inprogress]") {
  std::promise<void> finish_I_promise;
  std::promise<void> t1_ready_promise;
  bool first_thread = true;
  bool second_exec = false;
  auto f = [&](std::string) {
    if (first_thread) {
      t1_ready_promise.set_value();
      finish_I_promise.get_future().wait();
    } else {
      second_exec = true;
    }
    return std::string("test");
  };

  WhiteBoxEvaluator<
      WhiteBoxMaxEntriesPolicy<std::string, std::string, 2>,
      decltype(f)>
      eval(f);

  std::thread t1([&eval]() {
    // This blocks until f(key) is evaluated
    eval("key");
  });

  // Give t1 some heads up
  t1_ready_promise.get_future().wait();

  first_thread = false;
  std::thread t2([&eval]() {
    // This should wait within the evaluator until the value becomes available
    eval("key");
  });

  eval.policy().wait_till_numreaders("key", 1);

  // Check a single entry is in progress and no ready entries are present
  CHECK(eval.policy().entries_size() == 0);
  CHECK(eval.policy().inprogress_size() == 1);
  CHECK(eval.policy().entry_in_progress("key"));

  // Unblock the first thread so that the evaluation can finish and f(key) gets
  // cached, second reader gets the value as well and becomes unblocked
  finish_I_promise.set_value();

  t1.join();
  t2.join();

  // Make sure the second reader got a cached value and didn't just execute the
  // callback again.
  CHECK(second_exec == false);
}

TEST_CASE("Memory budgeting policy is enforced", "[evaluator][memory_budget]") {
  size_t budget = 4096;
  auto f = [](const std::string& key) { return key; };
  auto sizefn = [](const std::string& val) noexcept {
    return sizeof(std::string) + val.size();
  };

  WhiteBoxEvaluator<
      WhiteBoxMemoryBudgetPolicy<std::string, std::string, decltype(sizefn)>,
      decltype(f)>
      mem_budgeted_eval(f, sizefn, budget);

  CHECK(mem_budgeted_eval.policy().memory_consumed() == 0);

  size_t num_entries = 0;
  size_t c = 0;
  std::string str = "key0";
  size_t unit_size = sizeof(std::shared_ptr<std::string>) + sizefn(str);
  while (c < budget - unit_size) {
    str = "key" + std::to_string(c);
    unit_size = sizeof(std::shared_ptr<std::string>) + sizefn(str);

    mem_budgeted_eval(str);
    CHECK(mem_budgeted_eval.policy().memory_consumed() == c + unit_size);

    ++num_entries;
    c += unit_size;
  }

  // No eviction happened
  CHECK(mem_budgeted_eval.policy().entries_size() == num_entries);

  // str should be over budget, eviction should happen
  str = "key" + std::to_string(c);
  unit_size = sizeof(std::shared_ptr<std::string>) + sizefn(str);
  std::string evicted_val = "key0";
  size_t evicted_size =
      sizeof(std::shared_ptr<std::string>) + sizefn(evicted_val);
  mem_budgeted_eval(str);
  CHECK(mem_budgeted_eval.policy().entries_size() == num_entries);
  CHECK(
      mem_budgeted_eval.policy().memory_consumed() ==
      c + unit_size - evicted_size);

  // The policy evicts multiple entries until the new value fits
  str = std::string(unit_size, 'a');
  mem_budgeted_eval(str);  // evicts 2 lru entries
  CHECK(mem_budgeted_eval.policy().entries_size() == num_entries - 1);
  CHECK(mem_budgeted_eval.policy().memory_consumed() <= budget);
}

TEST_CASE(
    "Test evaluator invalid memory budget", "[evaluator][invalid_budget]") {
  size_t budget = 0;
  auto f = [](const std::string& key) { return key; };
  auto sizefn = [](const std::string& val) noexcept {
    return sizeof(std::string) + val.size();
  };

  REQUIRE_THROWS(static_cast<void>(WhiteBoxEvaluator<
                                   WhiteBoxMemoryBudgetPolicy<
                                       std::string,
                                       std::string,
                                       decltype(sizefn)>,
                                   decltype(f)>(f, sizefn, budget)));
}

TEST_CASE(
    "Test evaluator budget too small to hold a single value",
    "[evaluator][too_small_budget]") {
  auto f = [](const std::string& key) { return key; };
  auto sizefn = [](const std::string& val) noexcept {
    return sizeof(std::string) + val.size();
  };
  size_t budget = sizeof(std::shared_ptr<std::string>) + 1;

  WhiteBoxEvaluator<
      WhiteBoxMemoryBudgetPolicy<std::string, std::string, decltype(sizefn)>,
      decltype(f)>
      mem_budgeted_eval(f, sizefn, budget);
  REQUIRE_THROWS(mem_budgeted_eval("key"));
}

TEST_CASE(
    "Test max entries policy invalid argument",
    "[evaluator][invalid_maxentries]") {
  auto f = [](const std::string& key) { return key; };

  REQUIRE_THROWS(
      static_cast<void>(WhiteBoxEvaluator<
                        WhiteBoxMaxEntriesPolicy<std::string, std::string, 0>,
                        decltype(f)>(f)));
}
