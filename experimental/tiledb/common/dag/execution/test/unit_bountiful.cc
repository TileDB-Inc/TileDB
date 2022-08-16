/**
 * @file unit_bountiful.cc
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
 * Tests the bountiful scheduler class.
 *
 */

#include "unit_bountiful.h"
#include <atomic>
#include <memory>
#include <vector>
#include "experimental/tiledb/common/dag/execution/bountiful.h"

using namespace tiledb::common;

std::atomic<size_t> i_;

struct super_simple {
  explicit super_simple() = default;
  super_simple(const super_simple&) = delete;
  super_simple(super_simple&&) = default;

  void operator()() {
    i_++;
  }
};

TEST_CASE("BountifulScheduler: Test construct one", "[bountiful_scheduler]") {
  i_ = 0;
  super_simple a;

  BountifulScheduler sch;
  auto b = async(sch, a);
  sync_wait(b);
  CHECK(i_ == 1);
}

TEST_CASE(
    "BountifulScheduler: Test construct several", "[bountiful_scheduler]") {
  for (size_t i = 1; i <= 16; ++i) {
    std::vector<super_simple> v;
    std::vector<std::future<void>> w;
    v.clear();
    CHECK(v.size() == 0);

    BountifulScheduler sch;

    for (size_t j = 0; j < i; ++j) {
      v.emplace_back(super_simple{});
    }
    CHECK(v.size() == i);
    for (size_t j = 0; j < i; ++j) {
      w[i] = async(sch, v[j]);
    }
    CHECK(v.size() == i);
    CHECK(w.size() == i);
    for (size_t j = 0; j < i; ++j) {
      sync_wait(w[j]);
    }
    CHECK(i_ == i);
  }
}
