/**
 * @file   stats.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 TileDB, Inc.
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
 * This file contains declarations of statistics-related code.
 */

#ifndef TILEDB_STATS_H
#define TILEDB_STATS_H

#define __STDC_FORMAT_MACROS

#include <inttypes.h>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace tiledb {
namespace sm {
namespace stats {

/* ********************************* */
/*          TYPE DEFINITIONS         */
/* ********************************* */

/**
 * Class that defines stats counters and methods to manipulate them.
 */
class Stats {
 public:
  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /** Constructor. */
  Stats();

  /** Destructor. */
  ~Stats() = default;

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  /** Reset all counters to zero. */
  void reset();

  /** Dump the current counter values to the given file. */
  void dump(FILE* out) const;

  /** Dump the current counter values to the given string. */
  void dump(std::string* out) const;

 private:
  /* ****************************** */
  /*       PRIVATE FUNCTIONS        */
  /* ****************************** */

  /**
   * Helper function to pretty-print a ratio of integers as a "times" value.
   *
   * @param out Output file
   * @param msg Message to print at the beginning
   * @param unit Units for the numbers
   * @param numerator Numerator
   * @param denominator Denominator
   */
  void report_ratio(
      FILE* out,
      const char* msg,
      const char* unit,
      uint64_t numerator,
      uint64_t denominator) const;

  /**
   * Helper function to pretty-print a ratio of integers as a percentage.
   *
   * @param out Output file
   * @param msg Message to print at the beginning
   * @param unit Units for the numbers
   * @param numerator Numerator
   * @param denominator Denominator
   */
  void report_ratio_pct(
      FILE* out,
      const char* msg,
      const char* unit,
      uint64_t numerator,
      uint64_t denominator) const;
};

/**
 * The singleton instance holding all global stats counters. The report will
 * be automatically made when this object is destroyed (at program termination).
 */
extern Stats all_stats;

}  // namespace stats
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_STATS_H
