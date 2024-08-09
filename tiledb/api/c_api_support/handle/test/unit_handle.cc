/**
 * @file tiledb/api/proxy/test/unit_handle.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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

#include <catch2/catch_test_macros.hpp>
#include "../handle.h"

namespace tiledb::api {
namespace detail {
/**
 * Whitebox testing version of CAPIHandle
 */
template <class T>
class WhiteboxCAPIHandle : public CAPIHandle<T> {
  using base = CAPIHandle<T>;

 protected:
  WhiteboxCAPIHandle() = default;

 public:
  using weak_ptr_type = std::weak_ptr<WhiteboxCAPIHandle>;
  /**
   * Return a weak pointer to the private `self_` member of the CAPIHandle base
   * class.
   */
  weak_ptr_type weak_self() {
    return {base::self_};
  }

  typename base::shared_ptr_type self() {
    return {base::self_};
  }
};
}  // namespace detail

/**
 * Test handle object is just the handle; nothing else. The class keeps a count,
 * though, of all objects in existence.
 */
class TestHandle : public detail::WhiteboxCAPIHandle<TestHandle> {
  /*
   * `counter_type` is signed to allow underflow so that error detection works
   * correctly
   */
  using counter_type = int;
  static counter_type count_;

 public:
  TestHandle() {
    ++count_;
  }
  ~TestHandle() {
    --count_;
  }

  [[nodiscard]] static counter_type count() {
    return count_;
  }
};
TestHandle::counter_type TestHandle::count_{0};

}  // namespace tiledb::api

using THandle = tiledb::api::TestHandle;

/**
 * Ensure default constructor makes an empty `self_`
 */
TEST_CASE("CAPIHandle: constructor") {
  REQUIRE(THandle::count() == 0);
  THandle x;
  auto y{x.weak_self()};
  CHECK(y.expired());
}

/**
 * Factory and lifespan variation 1: weak_self in scope of creation
 */
TEST_CASE("CAPIHandle: factory and lifespan 1") {
  REQUIRE(THandle::count() == 0);
  THandle* y{};
  {
    y = THandle::make_handle();
    CHECK(THandle::count() == 1);
    REQUIRE(y != nullptr);
    REQUIRE(y == &y->get());
    auto z{y->weak_self()};
    CHECK(!z.expired());
  }
  // Now that we're out of scope, make sure we're still not expired
  THandle::break_handle(y);
  CHECK(THandle::count() == 0);
  CHECK(y == nullptr);
}

/**
 * Factory and lifespan variation 2: weak_self outside scope of creation
 */
TEST_CASE("CAPIHandle: factory and lifespan 2") {
  REQUIRE(THandle::count() == 0);
  THandle* y{};
  THandle::weak_ptr_type z{};
  {
    y = THandle::make_handle();
    CHECK(THandle::count() == 1);
    REQUIRE(y != nullptr);
    REQUIRE(y == &y->get());
    z = y->weak_self();
    CHECK(!z.expired());
  }
  // Now that we're out of scope, make sure we're still not expired
  CHECK(!z.expired());
  CHECK(z.use_count() == 1);
  THandle::break_handle(y);
  CHECK(THandle::count() == 0);
  CHECK(y == nullptr);
  CHECK(z.expired());
}

/**
 * Factory and lifespan variation 3: (strong) self in scope of creation
 */
TEST_CASE("CAPIHandle: factory and lifespan 3") {
  REQUIRE(THandle::count() == 0);
  THandle* y{};
  {
    y = THandle::make_handle();
    CHECK(THandle::count() == 1);
    REQUIRE(y != nullptr);
    REQUIRE(y == &y->get());
    auto z{y->self()};
    CHECK(THandle::count() == 1);
    CHECK(bool(z));
  }
  // Now that we're out of scope, make sure we're still not expired
  THandle::break_handle(y);
  CHECK(THandle::count() == 0);
  CHECK(y == nullptr);
}

/**
 * Factory and lifespan variation 4: weak_self outside scope of creation and
 * (strong) self inside
 */
TEST_CASE("CAPIHandle: factory and lifespan 4") {
  REQUIRE(THandle::count() == 0);
  THandle* y{};
  THandle::weak_ptr_type z{};
  {
    y = THandle::make_handle();
    CHECK(THandle::count() == 1);
    REQUIRE(y != nullptr);
    REQUIRE(y == &y->get());
    z = y->weak_self();
    CHECK(!z.expired());
    CHECK(z.use_count() == 1);
    auto w{y->self()};
    CHECK(THandle::count() == 1);
    CHECK(bool(w));
    CHECK(z.use_count() == 2);
  }
  // Now that we're out of scope, make sure we're still not expired
  CHECK(!z.expired());
  CHECK(z.use_count() == 1);
  THandle::break_handle(y);
  CHECK(THandle::count() == 0);
  CHECK(y == nullptr);
  CHECK(z.expired());
}
