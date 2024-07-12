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
 * Class that holds measurement data that Stats objects can be
 * initialized with.
 */
class StatsData {
 public:
  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /* Default constructor */
  StatsData() = default;

  /**
   * Value constructor.
   *
   * @param counters A map of counters
   * @param timers A map of timers
   */
  StatsData(
      std::unordered_map<std::string, uint64_t>& counters,
      std::unordered_map<std::string, double>& timers)
      : counters_(counters)
      , timers_(timers) {
  }

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  /** Get a reference to internal counters */
  const std::unordered_map<std::string, uint64_t>& counters() const {
    return counters_;
  }

  /** Get a reference to internal timers */
  const std::unordered_map<std::string, double>& timers() const {
    return timers_;
  }

 private:
  /* ****************************** */
  /*       PRIVATE ATTRIBUTES       */
  /* ****************************** */

  /** Map of counters and values */
  std::unordered_map<std::string, uint64_t> counters_;

  /** Map of timers and values */
  std::unordered_map<std::string, double> timers_;
};

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

  /**
   * Value constructor.
   *
   * @param prefix The stat name prefix.
   * @param data Initial data to populate the Stats object with.
   */
  Stats(const std::string& prefix, const StatsData& data);

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

  /**
   * Creates a child instance, managed by this instance, the instance is
   * constructed with initial data.
   *
   * @param prefix The stat name prefix.
   * @param data Initial data to populate the Stats object with.
   */
  Stats* create_child(const std::string& prefix, const StatsData& data);

  /** Return pointer to timers map, used for serialization only. */
  const std::unordered_map<std::string, double>* timers() const;

  /** Return pointer to conters map, used for serialization only. */
  const std::unordered_map<std::string, uint64_t>* counters() const;

  /**
   * Populate the counters and timers internal maps from a StatsData object
   * Please be aware that the data is not being added up, it will override the
   * existing data on the Stats object.
   *
   * @param data Data to populate the stats with.
   */
  void populate_with_data(const StatsData& data);

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
