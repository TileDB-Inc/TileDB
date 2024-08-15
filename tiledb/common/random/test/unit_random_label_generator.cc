/**
 * @file tiledb/common/random/test/unit_random_label_generator.cc
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
 *
 * Tests for the random label generator.
 */

#include <ranges>
#include <string>

#include <math.h>

#include <test/support/tdb_catch.h>
#include "../random_label.h"

using namespace tiledb::common;
using namespace tiledb::sm;

class TestRandomLabelGenerator : public RandomLabelGenerator {
 public:
  TestRandomLabelGenerator() = default;
  ~TestRandomLabelGenerator() = default;

  RandomLabelWithTimestamp generate_at(uint64_t now) {
    return generate(now);
  }
};

uint32_t prefix_as_uint32(std::string& label) {
  return std::stoul(label.substr(0, 8), nullptr, 16);
}

TEST_CASE(
    "RandomLabelGenerator: serial generation",
    "[RandomLabelGenerator][serial]") {
  TestRandomLabelGenerator trlg;

  // Check that we can generate a label
  auto label0 = trlg.generate_at(0);
  REQUIRE(label0.timestamp_ == 0);
  REQUIRE(label0.random_label_.size() == 32);

  // Check that a label at a different timestamp is correct.
  auto label1 = trlg.generate_at(1);
  REQUIRE(label1.timestamp_ == 1);
  REQUIRE(label1.random_label_.size() == 32);
  REQUIRE(label1.random_label_ != label0.random_label_);

  // Check that prefixes aren't off by one to show that the prefix was
  // regenerated after the time change.
  auto prefix0 = prefix_as_uint32(label0.random_label_);
  auto prefix1 = prefix_as_uint32(label1.random_label_);
  REQUIRE(std::max(prefix0, prefix1) - std::min(prefix0, prefix1) > 1);

  // Check that the label prefix is random by going backwards in time and
  // generating another label at time 0.
  auto label0_2 = trlg.generate_at(0);
  REQUIRE(label0_2.timestamp_ == 0);
  REQUIRE(label0_2.random_label_.size() == 32);
  REQUIRE(label0_2.random_label_ != label1.random_label_);
  REQUIRE(label0_2.random_label_ != label0.random_label_);

  // Check that label0_2 had a different prefix generated.
  auto prefix0_2 = prefix_as_uint32(label0_2.random_label_);
  REQUIRE(std::max(prefix0_2, prefix0) - std::min(prefix0_2, prefix0) > 1);

  // Check that generating a large number of labels at the same time results
  // in a common prefix for all labels, with all labels sorted, while having
  // no duplicates generated.
  std::vector<std::string> labels(25000);
  for (auto idx : std::views::iota(0, 25000)) {
    labels[idx] = trlg.generate_at(5).random_label_;
  }

  auto prefix = prefix_as_uint32(labels[0]);
  auto prev = labels[0];

  // Every generated label should have the same prefix and be strictly
  // greater than the previous label. Strictly greater assures that there are
  // no duplicates given that these strings are strictly ordered.
  for (auto idx : std::views::iota(1, 25000)) {
    auto curr_prefix = prefix_as_uint32(labels[idx]);
    REQUIRE(curr_prefix - prefix == 1);
    REQUIRE(labels[idx] > prev);

    prefix = curr_prefix;
    prev = labels[idx];
  }
}

TEST_CASE(
    "RandomLabelGenerator: parallel generation",
    "[RandomLabelGenerator][parallel]") {
  TestRandomLabelGenerator trlg;
  const unsigned nthreads = 20;
  const unsigned labels_per_thread = 25000;
  std::vector<std::thread> threads;
  std::vector<std::vector<std::string>> labels{nthreads};

  // Pre-allocate our buffers so we're getting as much contention as possible
  for (size_t i = 0; i < nthreads; i++) {
    labels[i].resize(labels_per_thread);
  }

  // Generate labels simultaneously in multiple threads all at the same time
  for (size_t i = 0; i < nthreads; i++) {
    auto vec_ptr = &labels[i];
    threads.emplace_back([&trlg, vec_ptr]() {
      for (size_t idx = 0; idx < labels_per_thread; idx++) {
        (*vec_ptr)[idx] = trlg.generate_at(3).random_label_;
      }
    });
  }

  // Wait for all of our threads to finish.
  for (auto& t : threads) {
    t.join();
  }

  // Sort and validate the parallel threads as if they were serially generated.
  std::vector<std::string> all_labels{labels_per_thread * nthreads};
  size_t idx = 0;
  for (auto thrlabels : labels) {
    for (auto label : thrlabels) {
      all_labels[idx++] = label;
    }
  }
  std::sort(all_labels.begin(), all_labels.end());

  // Verify a common prefix and unique suffix amongst all generated labels.
  REQUIRE(all_labels[0].size() == 32);
  auto prefix = prefix_as_uint32(all_labels[0]);
  auto prev = all_labels[0];
  for (size_t idx = 1; idx < labels_per_thread * nthreads; idx++) {
    auto curr_prefix = prefix_as_uint32(all_labels[idx]);
    REQUIRE(curr_prefix - prefix == 1);
    REQUIRE(all_labels[idx].size() == 32);
    REQUIRE(all_labels[idx] > prev);

    prefix = curr_prefix;
    prev = all_labels[idx];
  }
}
