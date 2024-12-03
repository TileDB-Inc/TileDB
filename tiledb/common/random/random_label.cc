/**
 * @file   random_label.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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
 * This file defines a random label generator.
 */

#include "tiledb/common/random/random_label.h"

namespace tiledb::common {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */
RandomLabelGenerator::RandomLabelGenerator()
    : prev_time_(tiledb::sm::utils::time::timestamp_now_ms()) {
}

/* ********************************* */
/*                API                */
/* ********************************* */
RandomLabelWithTimestamp RandomLabelGenerator::generate() {
  auto now = tiledb::sm::utils::time::timestamp_now_ms();
  return generate(now);
}

RandomLabelWithTimestamp RandomLabelGenerator::generate(uint64_t timestamp) {
  PRNG& prng = PRNG::get();
  std::lock_guard<std::mutex> lock(mtx_);

  // If no label has been generated this millisecond, generate a new one.
  if (timestamp != prev_time_) {
    prev_time_ = timestamp;
    counter_ = static_cast<uint32_t>(prng());
    // Clear the top bit of the counter such that a full 2 billion values
    // could be generated within a single millisecond.
    counter_ &= 0x7FFFFFFF;
  } else {
    counter_ += 1;
    if (counter_ == 0) {
      throw RandomLabelException("Maximum generation frequency exceeded.");
    }
  }

  // Generate and format a 128-bit, 32-digit hexadecimal random number
  std::stringstream ss;
  ss << std::hex << std::setw(8) << std::setfill('0') << counter_;
  ss << std::hex << std::setw(8) << std::setfill('0')
     << static_cast<uint32_t>(prng());
  ss << std::hex << std::setw(16) << std::setfill('0') << prng();
  return {ss.str(), timestamp};
}

RandomLabelWithTimestamp RandomLabelGenerator::generate_random_label() {
  static RandomLabelGenerator generator;
  return generator.generate();
}

std::string random_label() {
  return RandomLabelGenerator::generate_random_label().random_label_;
}

RandomLabelWithTimestamp random_label_with_timestamp() {
  return RandomLabelGenerator::generate_random_label();
}

}  // namespace tiledb::common
