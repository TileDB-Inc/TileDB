/**
 * @file   random_label.h
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
 * This file declares a random label generator.
 */

#ifndef TILEDB_HELPERS_H
#define TILEDB_HELPERS_H

#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>

#include "tiledb/common/exception/exception.h"
#include "tiledb/common/random/prng.h"
#include "tiledb/sm/misc/tdb_time.h"

namespace tiledb::common {

class RandomLabelException : public StatusException {
 public:
  explicit RandomLabelException(const std::string& message)
      : StatusException("RandomLabel", message) {
  }
};

class RandomLabelGenerator {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  DISABLE_COPY_AND_COPY_ASSIGN(RandomLabelGenerator);
  DISABLE_MOVE_AND_MOVE_ASSIGN(RandomLabelGenerator);

  /** Default destructor. */
  ~RandomLabelGenerator() = default;

 protected:
  /** Protected constructor, abstracted by public-facing accessor. */
  RandomLabelGenerator();

  /* ********************************* */
  /*                API                */
  /* ********************************* */
  /** Generate a random label. */
  std::string generate() {
    PRNG& prng = PRNG::get();
    std::lock_guard<std::mutex> lock(mtx_);
    auto now = tiledb::sm::utils::time::timestamp_now_ms();

    // If no label has been generated this millisecond, generate a new one.
    if (now != prev_time_) {
      prev_time_ = now;
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
    return ss.str();
  }

 public:
  /** Generate a random label. */
  static std::string generate_random_label();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Mutex which protects against simultaneous random label generation. */
  std::mutex mtx_;

  /** The time (in milliseconds) of the last label creation. */
  uint64_t prev_time_;

  /** The submillsecond counter portion of the random label. */
  uint32_t counter_;
};

/**
 * Returns a PRNG-generated label as a 32-digit hexadecimal random number.
 * (Ex. f258d22d4db9139204eef2b4b5d860cc).
 *
 * Note: the random number is actually the combination of two 16-digit numbers.
 * The values are 0-padded to ensure exactly a 128-bit, 32-digit length.
 *
 * @return A random label.
 */
std::string random_label();

}  // namespace tiledb::common

#endif  // TILEDB_HELPERS_H
