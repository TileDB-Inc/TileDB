/**
 * @file   stats.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * C++ API for TileDB stats.
 */

#ifndef TILEDB_CPP_API_STATS_H
#define TILEDB_CPP_API_STATS_H

#include "tiledb.h"

namespace tiledb {

/**
 * Encapsulates functionality related to internal TileDB statistics.
 *
 * **Example:**
 * @code{.cpp}
 * // Enable stats, submit a query, then dump to stdout.
 * tiledb::Stats::enable();
 * query.submit();
 * tiledb::Stats::dump();
 *
 * // Dump to a string instead.
 * std::string str;
 * tiledb::Stats::dump(&str);
 * @endcode
 */
class Stats {
 public:
  /** Enables internal TileDB statistics gathering. */
  static void enable() {
    check_error(tiledb_stats_enable(), "error enabling stats");
  }

  /** Disables internal TileDB statistics gathering. */
  static void disable() {
    check_error(tiledb_stats_disable(), "error disabling stats");
  }

  /** Reset all internal statistics counters to 0. */
  static void reset() {
    check_error(tiledb_stats_reset(), "error resetting stats");
  }

  /**
   * Dump all statistics counters to some output (e.g., file or stdout).
   *
   * @param out The output.
   */
  static void dump(FILE* out = nullptr) {
    check_error(tiledb_stats_dump(out), "error dumping stats");
  }

  /**
   * Dump all statistics counters to a string.
   *
   * @param out The output.
   */
  static void dump(std::string* out) {
    char* c_str = nullptr;
    check_error(tiledb_stats_dump_str(&c_str), "error dumping stats");
    *out = std::string(c_str);
    check_error(tiledb_stats_free_str(&c_str), "error freeing stats string");
  }

  /**
   * Dump all raw statistics counters to some output (e.g., file or stdout)
   * as a JSON.
   *
   * @param out The output.
   */
  static void raw_dump(FILE* out = nullptr) {
    check_error(tiledb_stats_raw_dump(out), "error dumping stats");
  }

  /**
   * Dump all raw statistics counters to a string.
   *
   * @param out The output.
   */
  static void raw_dump(std::string* out) {
    char* c_str = nullptr;
    check_error(tiledb_stats_raw_dump_str(&c_str), "error dumping stats");
    *out = std::string(c_str);
    check_error(tiledb_stats_free_str(&c_str), "error freeing stats string");
  }

 private:
  /**
   * Checks the return code for TILEDB_OK and throws an exception if not.
   *
   * @param rc Return code to check
   * @param msg Exception message string
   */
  static inline void check_error(int rc, const std::string& msg) {
    if (rc != TILEDB_OK)
      throw TileDBError("Stats Error: " + msg);
  }
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_STATS_H
