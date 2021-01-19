/**
 * @file   timer_stat.h
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
 * This file contains the TimerStat definition.
 *
 * This allows starting and stopping concurrent timers on different threads,
 * but does not count overlapping time periods toward the recorded duration.
 *
 * Thread-safety: none.
 */

#ifndef TILEDB_TIMER_STAT_H
#define TILEDB_TIMER_STAT_H

#include <thread>
#include <unordered_map>

namespace tiledb {
namespace sm {
namespace stats {

class TimerStat {
 public:
  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /** Constructor. */
  TimerStat();

  /** Destructor. */
  ~TimerStat() = default;

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  /**
   * Starts a timer for `tid`. This may not be called again
   * until a subsequent call to `end_timer`.
   */
  void start_timer(std::thread::id tid);

  /**
   * Ends a timer for `tid`. This may not be called again
   * until a subsequent call to `start_timer`.
   */
  void end_timer(std::thread::id tid);

  /**
   * Resets the total recorded durations.
   */
  void reset();

  /**
   * Returns the sum of all recorded durations among threads.
   */
  double duration() const;

 private:
  /* ****************************** */
  /*       PRIVATE ATTRIBUTES       */
  /* ****************************** */

  /** The total duration of all recorded timers. */
  double duration_;

  /** The start time for all pending timers. */
  static std::unordered_map<
      std::thread::id,
      std::chrono::high_resolution_clock::time_point>
      start_times_;
};

}  // namespace stats
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TIMER_STAT_H
