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

/** Simple struct to return a label with timestamp. */
struct RandomLabelWithTimestamp {
  /**
   * Construct a new random label with timestamp object
   *
   * @param random_label Random label.
   * @param timestamp Timestamp the random label was created.
   */
  RandomLabelWithTimestamp(std::string random_label, uint64_t timestamp)
      : random_label_(random_label)
      , timestamp_(timestamp) {
  }

  /** Random label. */
  std::string random_label_;

  /** Timestamp the label was created. */
  uint64_t timestamp_;
};

/**
 * Generates a pseudeo-random label, formatted as a 32-digit hexadecimal number.
 * (Ex. f258d22d4db9139204eef2b4b5d860cc).
 *
 * @pre If multiple labels are generated within the same millisecond, they will
 * be sorted using a counter on the most significant 4 bytes.
 * @note Use of wrapper `random_label()` is encouraged in production code.
 */
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
  /** Generate a random label with a timestamp. */
  RandomLabelWithTimestamp generate();

  /** Generate a random label at the specified timestamp. */
  RandomLabelWithTimestamp generate(uint64_t now);

 public:
  /** Generate a random label. */
  static RandomLabelWithTimestamp generate_random_label();

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
 * Wrapper function for `generate_random_label`, which returns a PRNG-generated
 * label as a 32-digit hexadecimal random number.
 * (Ex. f258d22d4db9139204eef2b4b5d860cc).
 *
 * @pre If multiple labels are generated within the same millisecond, they will
 * be sorted using a counter on the most significant 4 bytes.
 * @note Labels may be 0-padded to ensure exactly a 128-bit, 32-digit length.
 *
 * @return A random label.
 */
std::string random_label();

/**
 * Wrapper function for `generate_random_label`, which returns a PRNG-generated
 * label as a 32-digit hexadecimal random number.
 * (Ex. f258d22d4db9139204eef2b4b5d860cc).
 *
 * @pre If multiple labels are generated within the same millisecond, they will
 * be sorted using a counter on the most significant 4 bytes.
 * @note Labels may be 0-padded to ensure exactly a 128-bit, 32-digit length.
 *
 * @return A random label with timestamp.
 */
RandomLabelWithTimestamp random_label_with_timestamp();

}  // namespace tiledb::common

#endif  // TILEDB_HELPERS_H
