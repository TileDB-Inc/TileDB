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

#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/sm/stats/stats.h"

/**
 * Documenting the current stats behavior and architecture as close as
 * possible to the code so it's helpful next time someone tries to refactor.
 *
 * Statistics collection is done at the top level via the GlobalStats class
 * defined in this file.
 * We maintain a global object called `all_stats` which is used to register
 * Stats objects, enable/disable collection, reset or dumping the collected
 * stats.
 *
 * The TileDB C API uses the `all_stats` object directly to execute the
 * actions iterated above.
 *
 * The GlobalStats class owns a list called `registered_stats` that has one
 * Stats object registered for each Context used. In consequence,
 * ContextResources register a Stats object for each Context created, this
 * object serves as the root for the tree of all children Stats used in a
 * Context.
 *
 * As mentioned above, the Stats objects used under a Context form a tree.
 * Each Stats object mentains a list of children Stats and a pointer to the
 * parent Stats object.
 * The Stats object created by ContextResources(named "Context.StorageManager")
 * is the only Stats constructed in a standalone fashion using the Stats
 * constructor, all the other objects under this root Stats are created via
 * the Stats::create_child API.
 *
 * The (current, please update if not accurate anymore) exhaustive list of
 * Stats we maintain under a Context is:
 * ---------------------------
 * ContextResources
 *    - Query
 *    - Reader
 *    - Writer
 *        - DenseTiler
 *        - Subarray
 *    - Deletes
 *    - Subarray
 *    - subSubarray
 *        - SubarrayPartitioner
 *    - VFS
 *        - S3
 *        - ArrayDirectory
 *    - RestClient
 *    - Consolidator
 * ---------------------------
 * Please visualize this as a tree, it was much easier to write
 * like this, the tree is too wide.
 *
 *
 * Observed issues:
 * - Stats objects currently are created via Stats::create_child API from a
 *   parent stats object. Child objects such as e.g. Subarray only hold a
 *   pointer to the Stats object, this means that the Stats objects outlive
 *   the objects they represent and are kept alive by the tree like structure
 *   defined by the Stats class.
 *   In theory, a Context running for a long time would OOM the machine with
 *   Stats objects.
 *
 * - Stats::populate_flattened_stats aggregate the collected statistic via
 *   summing. But we also collect ".min", ".max" statistics as well,
 *   sum-aggregating those is incorrect. Currently the dump function just
 *   doesn't print those statistics.
 */

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

  /** Reset all registered stats. */
  void reset();

  /**
   * Registers a `Stats` instance. Stats in this instance
   * will be aggregated and dumped with the other registered
   * stats.
   */
  void register_stats(const shared_ptr<Stats>& stats);

  /** Dumps the current stats to the given file. */
  void dump(FILE* out) const;

  /** Dumps the current stats to the given string. */
  void dump(std::string* out) const;

  /** Dumps the current raw stats to the given file as a JSON. */
  void raw_dump(FILE* out) const;

  /** Dumps the current raw stats to the given string as a JSON. */
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
  mutable std::mutex mtx_;

  /** The aggregated stats. */
  std::list<std::weak_ptr<stats::Stats>> registered_stats_;

  /* ****************************** */
  /*       PRIVATE FUNCTIONS        */
  /* ****************************** */

  /** Dumps the current registered stats in ASCII format. */
  std::string dump_registered_stats() const;

  /** iterate over raw stats calling f() */
  template <class FuncT>
  void iterate(const FuncT&);

  /** iterate over raw stats calling f() */
  template <class FuncT>
  void iterate(const FuncT&) const;
};

/* ********************************* */
/*               GLOBAL              */
/* ********************************* */

/**
 * The singleton instance holding all global stats counters and timers.
 */
extern GlobalStats all_stats;

}  // namespace stats
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_GLOBAL_STATS_H
