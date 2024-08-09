/**
 * @file synchronized_optional/test/unit_synchronized_optional_swap.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#include <future>
#include "../synchronized_optional.h"
#include <catch2/catch_test_macros.hpp>

using stdx::synchronized_optional;

namespace stdx::detail {

/**
 * Whitebox version of synchronized_optional exposes `swap` for testing.
 */
template <class T>
class whitebox_synchronized_optional : public synchronized_optional<T> {
  using base_type = synchronized_optional<T>;

 public:
  using rt = typename base_type::CR_read_traits;
  template <class... Args>
  inline explicit whitebox_synchronized_optional(
      std::in_place_t, Args&&... args)
      : base_type(std::in_place, std::forward<Args>(args)...) {
  }
  void swap(whitebox_synchronized_optional& x) {
    base_type::swap(static_cast<base_type&>(x));
  }
};

}  // namespace stdx::detail
using soint = stdx::detail::whitebox_synchronized_optional<int>;

/*
 * `swap()` is currently disabled.
 *
 * The original version of this code used `scoped_lock` in the implementation of
 * `swap` to avoid deadlocks. `scoped_lock` is supposed to avoid deadlock when
 * called with multiple locks. It does seem to avoid deadlock when only other
 * instance of `scoped_lock` are used. It does not seem to be compatible with
 * other locks, particularly with `lock_guard`; when these are mixed deadlock
 * can occur.
 *
 * These tests are currently disabled by default using the Catch2 behavior of
 * tags with initial period character. They can be run explicitly by name.
 */

/**
 * Data held in common between multiple swapper threads.
 */
struct data {
  /**
   * Two objects to swap back and forth
   */
  soint x_, y_;

  /**
   * Initial values of the two operands
   */
  int a_, b_;

  data() = delete;

  data(int a, int b)
      : x_(std::in_place, a)
      , y_(std::in_place, b)
      , a_(a)
      , b_(b) {
  }
};

/**
 * Active class that runs an independent thread to exercise swapping behavior.
 *
 * @tparam C value-check policy class
 */
template <class C>
struct swapper {
  /**
   * Reference to a common data object
   */
  data& d_;

  /**
   * Thread body swaps and calls the value-check routine
   */
  bool operator()() {
    for (size_t i = 0; i < 10'000; ++i) {
      d_.x_.swap(d_.y_);
      {
        /*
         * `C::check` return false if values can be determined to be invalid
         */
        if (!C::check(*this)) {
          return false;
        };
      }
    }
    return true;
  };

  /**
   * Non-implemented function substitutes for `this` in the decltype below.
   */
  static swapper* one_of_this_type();

  /**
   * Type of the future to start off at construction
   */
  using future_type = decltype(std::async(
      std::launch::async, &swapper::operator(), one_of_this_type()));

  /**
   * The future is launched at construction
   */
  future_type fut;

  swapper(data& d)
      : d_(d)
      , fut(std::async(std::launch::async, &swapper::operator(), this)) {
  }
  bool get() {
    return fut.get();
  }
};

struct checker {
  static inline bool check(swapper<checker>& sw) {
    const int t{*sw.d_.x_};
    return t == sw.d_.a_ || t == sw.d_.b_;
  }
};

struct checker2 {
  static inline bool check(swapper<checker2>& sw) {
    soint& x{sw.d_.x_};
    const auto xref{*x};
    if (!((x == sw.d_.a_) || (x == sw.d_.b_))) {
      return false;
    }
    return true;
  }
};

struct checker3 {
  static inline bool check(swapper<checker3>& sw) {
    soint& x{sw.d_.x_};
    const auto xref{*x};
    if (!((x.value() == sw.d_.a_) || (x.value() == sw.d_.b_))) {
      return false;
    }
    return true;
  }
};

TEST_CASE("std::synchronized_optional -- swap 1", "[.swap]") {
  data d{5, 7};
  swapper<checker> s1{d};
  swapper<checker> s2{d};
  CHECK(s1.get() == true);
  CHECK(s2.get() == true);
}

TEST_CASE("std::synchronized_optional -- swap 2", "[.swap]") {
  data d{5, 7};
  swapper<checker2> s1{d};
  swapper<checker2> s2{d};
  CHECK(s1.get() == true);
  CHECK(s2.get() == true);
}

TEST_CASE("std::synchronized_optional -- swap 3", "[.swap]") {
  data d{5, 7};
  swapper<checker3> s1{d};
  swapper<checker3> s2{d};
  CHECK(s1.get() == true);
  CHECK(s2.get() == true);
}
