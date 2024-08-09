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

#include <math.h>

#include <test/support/tdb_catch.h>
#include "../random_label.h"

using namespace tiledb::common;
using namespace tiledb::sm;

size_t generate_labels(std::vector<std::string>& labels) {
  size_t labels_size = labels.size();
  size_t idx = 0;
  auto now = utils::time::timestamp_now_ms();
  while ((utils::time::timestamp_now_ms()) < now + 100 && idx < labels_size) {
    labels[idx++] = random_label();
  }

  return idx;
}

void validate_labels(std::vector<std::string>& labels, size_t num_labels) {
  /**
   * When creating a random label, ordering is determined by the first 8 bytes.
   * In this test, we are assuming the buffer overflow check works as expected,
   * and no more than 16^8 labels are generated within a single millisecond.
   *
   * Given the label randomness, and the fact that we're racing the processor,
   * the best we can do here (for now) is assert that there's at least 100
   * ordered groups, as `generate_labels` generates approximately 100 labels per
   * timestamp.
   *
   * A group is defined as sharing the first _n_ bytes, where n is calculated
   * as the lowest number of bytes within the 8-byte counter which can contain
   * the given num_labels.
   */

  size_t group_delimeter = ceil(log(num_labels) / log(16));
  uint64_t num_groups = 0;
  uint64_t this_group = 0;
  for (size_t i = 1; i < num_labels; i++) {
    bool match = true;
    for (size_t j = 0; j < group_delimeter; j++) {
      if (labels[i - 1][j] != labels[i][j]) {
        match = false;
        break;
      }
    }
    if (!match) {
      if (this_group > 100) {
        num_groups += 1;
      }
      this_group = 0;
      continue;
    }

    // We share a prefix so assert that they're ordered.
    REQUIRE(labels[i] > labels[i - 1]);
    this_group += 1;
  }

  REQUIRE(num_groups > 100);
}

TEST_CASE(
    "RandomLabelGenerator: serial generation",
    "[RandomLabelGenerator][serial]") {
  // Generate a random label to validate initialization.
  auto label = random_label();
  REQUIRE(label.size() == 32);

  // Test one million strings. Let's assume the buffer overflow check works.
  std::vector<std::string> labels{1000000};
  auto num_labels = generate_labels(labels);
  validate_labels(labels, num_labels);
}

TEST_CASE(
    "RandomLabelGenerator: parallel generation",
    "[RandomLabelGenerator][parallel]") {
  const unsigned nthreads = 20;
  std::vector<std::thread> threads;
  std::vector<std::vector<std::string>> labels{nthreads};
  size_t num_labels[nthreads];

  // Pre-allocate our buffers so we're getting as much contention as possible
  for (size_t i = 0; i < nthreads; i++) {
    labels[i].resize(1000000);
  }

  // Generate labels simultaneously in multiple threads.
  for (size_t i = 0; i < nthreads; i++) {
    auto num_ptr = &num_labels[i];
    auto vec_ptr = &labels[i];
    threads.emplace_back([num_ptr, vec_ptr]() {
      auto num = generate_labels(*vec_ptr);
      *num_ptr = num;
    });
  }

  // Wait for all of our threads to finish.
  for (auto& t : threads) {
    t.join();
  }

  // Check that we've generated the correct number of random labels.
  std::unordered_set<std::string> label_set;
  size_t total_labels = 0;
  for (size_t i = 0; i < nthreads; i++) {
    total_labels += num_labels[i];
    for (size_t j = 0; j < num_labels[i]; j++) {
      label_set.insert(labels[i][j]);
    }
  }
  REQUIRE(label_set.size() == total_labels);

  // Sort and validate the parallel threads as if they were serially generated.
  std::vector<std::string> all_labels{total_labels};
  size_t idx = 0;
  for (auto label : label_set) {
    all_labels[idx++] = label;
  }
  std::sort(all_labels.begin(), all_labels.end());
  validate_labels(all_labels, total_labels);
}
