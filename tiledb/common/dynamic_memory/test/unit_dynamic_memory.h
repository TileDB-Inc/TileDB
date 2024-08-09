/**
 * @file tiledb/common/dynamic_memory/test/unit_dynamic_memory.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 */

#include "../dynamic_memory.h"
#include "tiledb/common/common-std.h"

#include <catch2/catch_test_macros.hpp>
#include <sstream>

#ifndef TILEDB_COMMON_DYNAMIC_MEMORY_TEST_DYNAMIC_MEMORY_H
#define TILEDB_COMMON_DYNAMIC_MEMORY_TEST_DYNAMIC_MEMORY_H

using namespace tiledb::common;

/**
 * NullGovernor supports minimal constructor tests.
 */
struct NullGovernor {
  static void memory_panic() {
  }
};

/**
 * TestGovernor records whether memory_panic has been called.
 */
struct TestGovernor {
  static bool memory_panicked_;
  static void memory_panic() {
    memory_panicked_ = true;
  }
};

/*
 * Fixture class for Catch2.
 */
struct GoverningFixture {
  GoverningFixture() {
    TestGovernor::memory_panicked_ = false;
  }
};

struct NullTracer {
  static void allocate(void*, size_t, size_t, const TracingLabel&) {
  }
  static void deallocate(void*, size_t, size_t, const TracingLabel&) {
  }
};

struct TestTraceEntry {
  std::string_view event_;
  void* p_;
  size_t type_size_;
  size_t n_elements_;
  TracingLabel label_;
};

struct TestTracer {
  static void allocate(
      void* p, size_t type_size, size_t n_elements, const TracingLabel& label) {
    log_.push_back({"allocate", p, type_size, n_elements, label});
  }

  static void deallocate(
      void* p, size_t type_size, size_t n_elements, const TracingLabel& label) {
    log_.push_back({"deallocate", p, type_size, n_elements, label});
  }

  static std::vector<TestTraceEntry> log_;

  static std::string dump() {
    std::ostringstream ss;
    if (log_.empty()) {
      ss << "Log is empty" << std::endl;
    } else {
      ss << "-- begin dump --" << std::endl;
      for (auto entry : log_) {
        ss << entry.event_ << ":" << entry.p_ << ":";
        ss << entry.n_elements_ << "x" << entry.type_size_ << ":";
        ss << "/" << entry.label_.origin_ << "/" << std::endl;
      }
      ss << "--- end dump ---" << std::endl;
    }
    return ss.str();
  }
};

/*
 * Fixture class for Catch2.
 */
struct TracingFixture {
  TracingFixture() {
    TestTracer::log_.clear();
  }
};

namespace tiledb::common::detail {
template <class T, template <class U> class Alloc, class Tracer>
class WhiteboxTracedAllocator : public TracedAllocator<T, Alloc, Tracer> {
  using base = TracedAllocator<T, Alloc, Tracer>;

 public:
  [[nodiscard]] const TracingLabel& label() const {
    return base::label_;
  }
  [[nodiscard]] const std::string_view& origin() const {
    return base::label_.origin_;
  }

  template <typename... Args>
  explicit WhiteboxTracedAllocator(const TracingLabel& label, Args&&... args)
      : base(label, forward<Args>(args)...) {
  }

  template <size_t n, typename... Args>
  explicit WhiteboxTracedAllocator(const char (&origin)[n], Args&&... args)
      : base(
            TracingLabel(std::string_view(origin, n - 1)),
            forward<Args>(args)...) {
  }
};
}  // namespace tiledb::common::detail

template <class T>
using TestingAllocator =
    detail::WhiteboxTracedAllocator<T, std::allocator, TestTracer>;

/*
 * Whitebox duplicates of tiledb::common::make_shared
 */
template <class T, class... Args>
std::shared_ptr<T> make_shared_whitebox(const TracingLabel& x, Args&&... args) {
  return std::allocate_shared<T>(
      TestingAllocator<T>(x), std::forward<Args>(args)...);
}

template <class T, int n, class... Args>
std::shared_ptr<T> make_shared_whitebox(
    const char (&origin)[n], Args&&... args) {
  return make_shared_whitebox<T>(
      TracingLabel(std::string_view(origin, n - 1)),
      std::forward<Args>(args)...);
}

#endif  // TILEDB_COMMON_DYNAMIC_MEMORY_TEST_DYNAMIC_MEMORY_H
