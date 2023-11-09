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

#include "duration_instrument.h"

#include <inttypes.h>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <list>
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
  /**
   * Friends with DurationInstrument so it can call `report_duration`.
   */
  friend class DurationInstrument<Stats>;

 public:
  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /**
   * Value constructor.
   *
   * @param prefix The stat name prefix.
   */
  Stats(const std::string& prefix);

  /** Destructor. */
  ~Stats() = default;

/* ****************************** */
/*              API               */
/* ****************************** */

/**
 * Starts a timer for the input timer stat. The timer ends when the returned
 * `DurationInstrument` object is destroyed.
 */
#ifdef TILEDB_STATS
  DurationInstrument<Stats> start_timer(const std::string& stat);
#else
  int start_timer(const std::string& stat);
#endif

  /** Adds `count` to the input counter stat. */
  void add_counter(const std::string& stat, uint64_t count);

  /** Subtracts `count` from the input counter stat. */
  void sub_counter(const std::string& stat, uint64_t count);

  /** Set a counter to a value if it is larger than the current stat value. */
  void set_max_counter(const std::string& stat, uint64_t count);

  /** Set a counter to a specific value. */
  void set_counter(const std::string& stat, uint64_t value);

  /** Returns true if statistics are currently enabled. */
  bool enabled() const;

  /** Enable or disable statistics gathering. */
  void set_enabled(bool enabled);

  /** Reset all stats. */
  void reset();

  /**
   * Dumps the stats for this instance as a JSON dictionary of
   * timers and stats.
   *
   * @param indent_size The number of spaces in an indentation.
   * @param num_indents The number of leading indentations.
   */
  std::string dump(uint64_t indent_size, uint64_t num_indents) const;

  /** Returns the parent that manages this instance. */
  Stats* parent();

  /** Creates a child instance, managed by this instance. */
  Stats* create_child(const std::string& prefix);

  /** Return pointer to timers map, used for serialization only. */
  std::unordered_map<std::string, double>* timers();

  /** Return pointer to conters map, used for serialization only. */
  std::unordered_map<std::string, uint64_t>* counters();

 private:
  /* ****************************** */
  /*       PRIVATE ATTRIBUTES       */
  /* ****************************** */

  /** Mutex. */
  mutable std::mutex mtx_;

  /** True if stats are being gathered. */
  bool enabled_;

  /** A map of timer stats measuring time in seconds. */
  std::unordered_map<std::string, double> timers_;

  /** A map of counter stats. */
  std::unordered_map<std::string, uint64_t> counters_;

  /** Prefix used for the various timers and counters. */
  const std::string prefix_;

  /**
   * A pointer to the parent instance that manages this
   * lifetime of this instance.
   */
  Stats* parent_;

  /** All child instances created with the `create_child` API. */
  std::list<Stats> children_;

  /* ****************************** */
  /*       PRIVATE FUNCTIONS        */
  /* ****************************** */

  /** Reports a duration. Called from a `DurationInstrument` object. */
  void report_duration(
      const std::string& stat, const std::chrono::duration<double> duration);

  /**
   * Populates the input stats with the instance stats. This is a
   * recursive work routine that `dump()` uses to aggregate all stats
   * from the children instances. The `mtx_` must be unlocked when
   * entering this routine.
   *
   * @param flattened_timers Timers to append to.
   * @param flattened_counters Counters to append to.
   */
  void populate_flattened_stats(
      std::unordered_map<std::string, double>* const flattened_timers,
      std::unordered_map<std::string, uint64_t>* const flattened_counters)
      const;
};

}  // namespace stats
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_STATS_H
