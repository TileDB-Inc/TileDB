/**
 * @file unit_stats.cc
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
 * Tests the `DurationInstrument` class.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/stats/duration_instrument.h"

#include <catch2/catch_test_macros.hpp>
#include <iostream>

using namespace tiledb::sm;

class TestStats {
 public:
  void report_duration(
      const std::string& stat, const std::chrono::duration<double> duration) {
    CHECK(!reported_);
    reported_ = true;
    reported_stat_ = stat;
    duration_ = duration.count();
  }

  void check_reported_stat_and_duration(std::string expected) {
    CHECK(reported_);
    CHECK(reported_stat_ == expected);
    CHECK((duration_ >= 0 && duration_ < 1));
  }

 private:
  bool reported_ = false;
  std::string reported_stat_ = "";
  double duration_ = 1000;
};

TEST_CASE("DurationInstrument: basic test", "[stats][duration_instrument]") {
  TestStats stats;
  { stats::DurationInstrument<TestStats> temp(stats, "test_stat"); }
  stats.check_reported_stat_and_duration("test_stat");
}
