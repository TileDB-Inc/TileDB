/**
 * @file   duration_instrument.h
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
 * This file defines class DurationInstrument.
 */

#ifndef TILEDB_DURATION_INSTRUMENT_H
#define TILEDB_DURATION_INSTRUMENT_H

#include "tiledb/common/common.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace stats {

class Stats;

/**
 * This class contains a simple duration instrument.
 */
class DurationInstrument {
 public:
  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /** Constructs a duration instrument object. */
  DurationInstrument(Stats* parent_stats, const std::string stat_name)
      : parent_stats_(parent_stats)
      , stat_name_(stat_name)
      , start_time_(std::chrono::high_resolution_clock::now()) {
    if (parent_stats_ == nullptr) {
      throw std::logic_error("Null stats object");
    }
  }

  /** Destructor, reports duration to the stats. */
  ~DurationInstrument();

 private:
  /* ****************************** */
  /*       PRIVATE ATTRIBUTES       */
  /* ****************************** */

  /** Pointer to the parent stats. */
  Stats* parent_stats_;

  /** Stat to report duration for. */
  const std::string stat_name_;

  /** Start time of the duration instrument. */
  std::chrono::high_resolution_clock::time_point start_time_;
};

}  // namespace stats
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DURATION_INSTRUMENT_H