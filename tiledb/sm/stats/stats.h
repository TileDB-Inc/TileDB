/**
 * @file   stats.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file declares class Stats.
 */

#ifndef TILEDB_STATS_H
#define TILEDB_STATS_H

#include <inttypes.h>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>

namespace tiledb {
namespace sm {
namespace stats {

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

  /** Constructor. */
  Stats(const std::string& prefix);

  /** Destructor. */
  ~Stats() = default;

  /** Copy constructor. */
  Stats(const Stats& stats);

  /** Move constructor. */
  Stats(Stats&& stats);

  /** Copy-assign operator. */
  Stats& operator=(const Stats& stats);

  /** Move-assign operator. */
  Stats& operator=(Stats&& stats);

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  /** Starts a timer for the input timer stat. */
  void start_timer(const std::string& stat);

  /** Ends a timer for the input timer stat. */
  void end_timer(const std::string& stat);

  /** Adds `count` to the input counter stat. */
  void add_counter(const std::string& stat, uint64_t count);

  /** Returns true if statistics are currently enabled. */
  bool enabled() const;

  /** Enable or disable statistics gathering. */
  void set_enabled(bool enabled);

  /** Reset all counters to zero. */
  void reset();

  /** Dumps the stats in ASCII format. */
  std::string dump() const;

  /** Appends the input stats to the object stats. */
  void append(const Stats& stats);

 private:
  /* ****************************** */
  /*       PRIVATE ATTRIBUTES       */
  /* ****************************** */

  /** Mutex. */
  std::mutex mtx_;

  /** True if stats are being gathered. */
  bool enabled_;

  /** A map of timer stats measuring time in seconds. */
  std::map<std::string, double> timers_;

  /** A map of counter stats. */
  std::map<std::string, uint64_t> counters_;

  typedef std::unordered_map<
      std::thread::id,
      std::chrono::high_resolution_clock::time_point>
      ThreadTimer;

  /** The start time for all pending timers. */
  std::map<std::string, ThreadTimer> start_times_;

  /** Prefix used for the various timers and counters. */
  std::string prefix_;

  /* ****************************** */
  /*       PRIVATE FUNCTIONS        */
  /* ****************************** */

  /** Clones the object and returns it. */
  Stats clone() const;

  /** Swaps the contents (all field values) of this object with the given
   * object. */
  void swap(Stats& stats);
};

}  // namespace stats
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_STATS_H
