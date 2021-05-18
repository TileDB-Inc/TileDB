/**
 * @file   global_stats.h
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
 * This file contains declarations of statistics-related code.
 */

#ifndef TILEDB_GLOBAL_STATS_H
#define TILEDB_GLOBAL_STATS_H

#include <inttypes.h>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <list>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>

#include "tiledb/common/heap_memory.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/stats/timer_stat.h"

namespace tiledb {
namespace sm {
namespace stats {

/* ********************************* */
/*          TYPE DEFINITIONS         */
/* ********************************* */

/**
 * Class that defines stats counters and methods to manipulate them.
 */
class GlobalStats {
 public:
  /* ****************************** */
  /*            CONSTANTS           */
  /* ****************************** */

  /** Used in byte to GB conversion. */
  const uint64_t GB_BYTES = 1024 * 1024 * 1024;

  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /** Constructor. */
  GlobalStats();

  /** Destructor. */
  ~GlobalStats() = default;

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  /** Returns true if statistics are currently enabled. */
  bool enabled() const;

  /** Enable or disable statistics gathering. */
  void set_enabled(bool enabled);

  /**
   * Registers a `Stats` instance. Stats in this instance
   * will be aggregated and dumped with the other registered
   * stats.
   */
  void register_stats(const tdb_shared_ptr<Stats>& stats);

  /** Dump the current stats to the given file. */
  void dump(FILE* out) const;

  /** Dump the current stats to the given string. */
  void dump(std::string* out) const;

  /** Dump the current raw stats to the given file as a JSON. */
  void raw_dump(FILE* out) const;

  /** Dump the current raw stats to the given string as a JSON. */
  void raw_dump(std::string* out) const;

 private:
  /* ****************************** */
  /*       PRIVATE DATATYPES        */
  /* ****************************** */

  /** An STL hasher for enum definitions. */
  struct EnumHasher {
    template <typename T>
    std::size_t operator()(T t) const {
      return static_cast<std::size_t>(t);
    }
  };

  /* ****************************** */
  /*       PRIVATE ATTRIBUTES       */
  /* ****************************** */

  /** True if stats are being gathered. */
  bool enabled_;

  /** Mutex to protext in multi-threading scenarios. */
  std::mutex mtx_;

  /** The aggregated stats. */
  std::list<tdb_shared_ptr<stats::Stats>> registered_stats_;

  /* ****************************** */
  /*       PRIVATE FUNCTIONS        */
  /* ****************************** */

  /** Dump the current registered stats. */
  std::string dump_registered_stats() const;
};

/* ********************************* */
/*               GLOBAL              */
/* ********************************* */

/**
 * The singleton instance holding all global stats counters. The report will
 * be automatically made when this object is destroyed (at program termination).
 */
extern GlobalStats all_stats;

}  // namespace stats
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_GLOBAL_STATS_H
