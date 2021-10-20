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

TEST_CASE("GovernedAllocator - constructor, default arguments")
{
  GovernedAllocator<int, std::allocator, NullGovernor> a;
}

TEST_CASE("GovernedAllocator - allocate throw")
{
  GovernedAllocator<int, ThrowingAllocator, NullGovernor> a;
  REQUIRE_THROWS(a.allocate(1));
}

TEST_CASE_METHOD(GoverningFixture, "GovernedAllocator - regular allocator does not panic") {
  GovernedAllocator<int, std::allocator, TestGovernor> a;
  a.allocate(1);
  REQUIRE(TestGovernor::memory_panicked_==false);
}

TEST_CASE_METHOD(GoverningFixture, "GovernedAllocator - throwing allocator panics") {
  GovernedAllocator<int, ThrowingAllocator, TestGovernor> a;
  REQUIRE_THROWS(a.allocate(1));
  REQUIRE(TestGovernor::memory_panicked_==true);
}

/**
 * Tests that the NullTracer default class correctly instantiates.
 */
TEST_CASE("TracedAllocator - default null tracing, constructor, default arguments")
{
  TracedAllocator<int, std::allocator, NullTracer> a("");
}

TEST_CASE("TracedAllocator - with tracing, allocate throw")
{
  TracedAllocator<int, ThrowingAllocator, NullTracer> a("");
  REQUIRE_THROWS(a.allocate(1));
}

TEST_CASE("make_shared - TracingLabel")
{
  auto label = TracingLabel(std::string_view("foo",3));
  auto x = make_shared<unsigned short>(label, 5);
}

TEST_CASE("make_shared - string_view")
{
  std::string_view sv("foo",3);
  auto x = tiledb::common::make_shared<unsigned short>(sv, 5);

  // MSVC 14.29.30037 (2019) faults when the explicit namespace specification is
  // absent, as follows:
  //     auto y = make_shared<unsigned short>(sv, 5);
  // The error messages show that it's trying to instantiate std::make_shared,
  // even though there's no `using`-statement to warrant looking in `std`, and
  // despite the explicit `using`-statement at the top of the file.
}

TEST_CASE("make_shared - string constant")
{
  auto x = make_shared<unsigned short>("foo", 5);
}

TEST_CASE_METHOD(TracingFixture, "Tracer - trace make_shared")
{
  auto label = TracingLabel(std::string_view("bar",3));
  {
    auto x = make_shared_whitebox<unsigned short>(label, 5);
    // deallocation at end of block
  }
  const auto& log = TestTracer::log_;
  REQUIRE(log.size()==2);
  CHECK(log[0].event_=="allocate");
  CHECK(log[1].event_=="deallocate");
  WARN(TestTracer::dump());
}

TEST_CASE_METHOD(TracingFixture, "Tracer - trace bad_alloc")
{
  TracedAllocator<int, ThrowingAllocator, TestTracer> a(HERE());
  try {
    a.allocate(1);
  } catch (const std::bad_alloc&) {
    // do nothing
  }
  const auto& log = TestTracer::log_;
  REQUIRE(log.size()==1);
  CHECK(log[0].event_=="allocate");
  CHECK(log[0].p_==nullptr);
  WARN(TestTracer::dump());
}
