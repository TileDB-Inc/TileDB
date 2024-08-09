/**
 * @file tiledb/common/dynamic_memory/test/main.cc
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
 *
 * This file defines a test `main()`
 */

#include "unit_dynamic_memory.h"

using namespace tiledb::common::detail;

bool TestGovernor::memory_panicked_ = false;
std::vector<TestTraceEntry> TestTracer::log_;

TEST_CASE("White-box constructor, label argument") {
  auto label = TracingLabel(std::string_view("foo", 3));
  WhiteboxTracedAllocator<int, std::allocator, TestTracer> x{label};
  CHECK(x.origin() == "foo");
}

TEST_CASE("White-box constructor, const-char argument") {
  WhiteboxTracedAllocator<int, std::allocator, TestTracer> x{"foo"};
  CHECK(x.origin() == "foo");
}
