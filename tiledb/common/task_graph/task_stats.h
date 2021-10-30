/**
 * @file   task_stats.h
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
 This file defines class TaskStats.
 */

#ifndef TILEDB_TASK_STATS_H
#define TILEDB_TASK_STATS_H

#include <chrono>

namespace tiledb {
namespace common {

/** Maintains statistics about a task. */
class TaskStats {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  TaskStats();

  /** Destructor. */
  ~TaskStats();

  /** Copy constructor. */
  TaskStats(const TaskStats&) = default;

  /** Move constructor. */
  TaskStats(TaskStats&&) = default;

  /** Copy assignment. */
  TaskStats& operator=(const TaskStats&) = default;

  /** Move assignment. */
  TaskStats& operator=(TaskStats&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the task start time. */
  std::chrono::time_point<std::chrono::steady_clock> start_time() const;

  /** Returns the task end time. */
  std::chrono::time_point<std::chrono::steady_clock> end_time() const;

  /** Returns the task duration time. */
  std::chrono::duration<double> duration() const;

  /** Sets the task start time. */
  void set_start_time(
      const std::chrono::time_point<std::chrono::steady_clock>& start_time);

  /** Sets the task end time. */
  void set_end_time(
      const std::chrono::time_point<std::chrono::steady_clock>& end_time);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The start time of the task. */
  std::chrono::time_point<std::chrono::steady_clock> start_time_;

  /** The end time of the task. */
  std::chrono::time_point<std::chrono::steady_clock> end_time_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_TASK_STATS_H
