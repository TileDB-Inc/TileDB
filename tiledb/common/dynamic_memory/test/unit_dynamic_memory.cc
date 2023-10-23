/**
 * @file   tiledb/common/dynamic_memory/test/unit_dynamic_memory.cc
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
 */

#include "unit_dynamic_memory.h"
#include "testing_allocators.h"

using tiledb::common::make_shared;

TEST_CASE("GovernedAllocator - constructor, default arguments") {
  GovernedAllocator<int, std::allocator, NullGovernor> a;
}

TEST_CASE("GovernedAllocator - allocate throw") {
  GovernedAllocator<int, ThrowingAllocator, NullGovernor> a;
  REQUIRE_THROWS(a.allocate(1));
}

TEST_CASE_METHOD(
    GoverningFixture, "GovernedAllocator - regular allocator does not panic") {
  GovernedAllocator<int, std::allocator, TestGovernor> a;
  a.allocate(1);
  REQUIRE(TestGovernor::memory_panicked_ == false);
}

TEST_CASE_METHOD(
    GoverningFixture, "GovernedAllocator - throwing allocator panics") {
  GovernedAllocator<int, ThrowingAllocator, TestGovernor> a;
  REQUIRE_THROWS(a.allocate(1));
  REQUIRE(TestGovernor::memory_panicked_ == true);
}

/**
 * Tests that the NullTracer default class correctly instantiates.
 */
TEST_CASE(
    "TracedAllocator - default null tracing, constructor, default arguments") {
  TracingLabel label{std::string_view("", 0)};
  TracedAllocator<int, std::allocator, NullTracer> a(label);
}

TEST_CASE("TracedAllocator - with tracing, allocate throw") {
  TracingLabel label{std::string_view("", 0)};
  TracedAllocator<int, ThrowingAllocator, NullTracer> a(label);
  REQUIRE_THROWS(a.allocate(1));
}

TEST_CASE("make_shared - TracingLabel") {
  auto label = TracingLabel(std::string_view("foo", 3));
  auto x = make_shared<unsigned short>(label, 5);
}

#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ < 13
// Temporarily disabled due to compile error on GCC 13
// - https://github.com/TileDB-Inc/TileDB/issues/4191
TEST_CASE("make_shared_whitebox - TracingLabel") {
  auto label = TracingLabel(std::string_view("foo", 3));
  auto x = make_shared_whitebox<unsigned short>(label, 5);
}

TEST_CASE("make_shared_whitebox - string constant") {
  auto x = make_shared_whitebox<unsigned short>("foo", 5);
}

TEST_CASE_METHOD(TracingFixture, "Tracer - trace make_shared") {
  auto label = TracingLabel(std::string_view("bar", 3));
  {
    auto x = make_shared_whitebox<unsigned short>(label, 5);
    // deallocation at end of block
  }
  const auto& log = TestTracer::log_;
  REQUIRE(log.size() == 2);
  CHECK(log[0].event_ == "allocate");
  CHECK(log[1].event_ == "deallocate");
  WARN(TestTracer::dump());
}

#endif

TEST_CASE("make_shared - string constant") {
  auto x = make_shared<unsigned short>("foo", 5);
}

TEST_CASE_METHOD(TracingFixture, "Tracer - trace bad_alloc") {
  TracingLabel label{std::string_view(HERE(), std::string(HERE()).length())};
  TracedAllocator<int, ThrowingAllocator, TestTracer> a(label);
  try {
    a.allocate(1);
  } catch (const std::bad_alloc&) {
    // do nothing
  }
  const auto& log = TestTracer::log_;
  REQUIRE(log.size() == 1);
  CHECK(log[0].event_ == "allocate");
  CHECK(log[0].p_ == nullptr);
  WARN(TestTracer::dump());
}
