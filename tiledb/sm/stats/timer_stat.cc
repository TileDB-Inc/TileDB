/**
 * @file   timer_stat.cc
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
 * This file contains the TimerStat implementation.
 */

#include <assert.h>

#include "tiledb/sm/stats/timer_stat.h"

namespace tiledb {
namespace sm {
namespace stats {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

TimerStat::TimerStat()
    : duration_(0.0f) {
}

/* ****************************** */
/*              API               */
/* ****************************** */

void TimerStat::start_timer(const std::thread::id tid) {
  start_times_[tid] = std::chrono::high_resolution_clock::now();
}

void TimerStat::end_timer(const std::thread::id tid) {
  std::chrono::high_resolution_clock::time_point* const start_time =
      &start_times_.at(tid);

  // Check for intersections with pending timers on other
  // threads.
  std::chrono::high_resolution_clock::time_point end_time;
  for (const auto& start_times_kv : start_times_) {
    const std::thread::id other_tid = start_times_kv.first;
    const std::chrono::high_resolution_clock::time_point* const
        other_start_time = &start_times_kv.second;

    // Do not compare this state with itself.
    if (other_tid == tid)
      continue;

    // Check if the other state has a pending timer.
    if (other_start_time->time_since_epoch().count() > 0) {
      // If the other thread's timer started at an earlier or identical
      // time than this thread's timer, the other thread will capture
      // the entire duration of this timer.
      if (*other_start_time <= *start_time) {
        // Discard this entire duration.
        *start_time = std::chrono::high_resolution_clock::time_point();
        return;
      }

      // If the other thread's timer started at a later time than this
      // thread's timer, only record this thread's timer to an end time
      // equal to the other thread's timer.
      assert(*other_start_time > *start_time);
      end_time = *other_start_time;
    }
  }

  // If we did not set `end_time`, set it as the current time.
  if (end_time.time_since_epoch().count() == 0)
    end_time = std::chrono::high_resolution_clock::now();

  // Calculate and record the duration.
  const std::chrono::duration<double> duration = end_time - *start_time;
  duration_ += duration.count();

  // Reset the start time.
  *start_time = std::chrono::high_resolution_clock::time_point();
}

void TimerStat::reset() {
  duration_ = 0.0f;
}

double TimerStat::duration() const {
  return duration_;
}

}  // namespace stats
}  // namespace sm
}  // namespace tiledb
