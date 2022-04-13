/**
 * @file tiledb/api/proxy/test/unit_proxy.cc
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
 *
 * This file tests the Dimension class
 */

#include <catch.hpp>
#include "../proxy.h"

using namespace tiledb::api;

/**
 * Arguments class to construct `TestT`
 */
class TestArgs {
 public:
  TestArgs()
      : a_(-1)
      , b_(-2){};
  int a_;
  int b_;
  bool validate() {
    return a_ > 0 && b_ > 0;
  }
};

/**
 * Underlying type for a proxy
 */
class TestT {
 public:
  static int instance_count;
  int a_;
  int b_;
  TestT(int a, int b)
      : a_(a)
      , b_(b) {
    ++instance_count;
  }
  TestT(TestArgs& x)
      : TestT(x.a_, x.b_) {
  }
  TestT(const TestT&) = delete;
  TestT(TestT&& x) = delete;
  ~TestT() {
    a_ = 0;
    b_ = 0;
    --instance_count;
  }
};
int TestT::instance_count{0};

/**
 * Life cycle class for `TestT`
 */
class TestLC {
 public:
  using arguments_type = TestArgs;
  static constexpr bool has_shutdown = false;
};

TEST_CASE("Proxy::Proxy") {
  Proxy<TestT, TestLC> x{};
}

TEST_CASE("Proxy - ordinary life cycle 1") {
  Proxy<TestT, TestLC> x{};
  REQUIRE(TestT::instance_count == 0);
  auto args_p{x.args()};
  auto& args{*args_p};
  args.a_ = 1;
  args.b_ = 2;
  REQUIRE(TestT::instance_count == 0);
  x.construct();
  REQUIRE(TestT::instance_count == 1);
  {
    auto y{x.access()};
    auto& z{y.value()};
    CHECK(z.a_ == 1);
    CHECK(z.b_ == 2);
  }
  REQUIRE(TestT::instance_count == 1);
  x.destroy();
  REQUIRE(TestT::instance_count == 0);
}
