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
 * This file contains declarations of statistics-related code.
 */

#ifndef TILEDB_STATS_H
#define TILEDB_STATS_H

#include <inttypes.h>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>

#include "tiledb/sm/stats/timer_stat.h"

// Define an enum named <TypeName> and a comma-separated
// string named <TypeName>Names_ containing the stat names.
#define DEFINE_TILEDB_STATS(TypeName, ...)           \
 private:                                            \
  const std::string TypeName##Names_ = #__VA_ARGS__; \
                                                     \
 public:                                             \
  enum class TypeName { __VA_ARGS__, __SIZE }

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
  /*            CONSTANTS           */
  /* ****************************** */

  /** Used in byte to GB conversion. */
  const uint64_t GB_BYTES = 1024 * 1024 * 1024;

  /* ****************************** */
  /*         TYPE DEFINITIONS       */
  /* ****************************** */

  /** Enumerates the stat timer types. */
  DEFINE_TILEDB_STATS(
      TimerType,
      CONSOLIDATE_CREATE_BUFFERS,
      CONSOLIDATE_CREATE_QUERIES,
      CONSOLIDATE_COPY_ARRAY,
      CONSOLIDATE_FRAGS,
      CONSOLIDATE_MAIN,
      CONSOLIDATE_COMPUTE_NEXT,
      CONSOLIDATE_ARRAY_META,
      CONSOLIDATE_FRAG_META,
      READ_ARRAY_OPEN,
      READ_ARRAY_OPEN_WITHOUT_FRAGMENTS,
      READ_FILL_DENSE_COORDS,
      READ_GET_FRAGMENT_URIS,
      READ_LOAD_ARRAY_SCHEMA,
      READ_LOAD_ARRAY_META,
      READ_LOAD_FRAG_META,
      READ_LOAD_CONSOLIDATED_FRAG_META,
      READ_ATTR_TILES,
      READ_COORD_TILES,
      READ_UNFILTER_ATTR_TILES,
      READ_UNFILTER_COORD_TILES,
      READ_COPY_ATTR_VALUES,
      READ_COPY_COORDS,
      READ_COPY_FIXED_ATTR_VALUES,
      READ_COPY_FIXED_COORDS,
      READ_COPY_VAR_ATTR_VALUES,
      READ_COPY_VAR_COORDS,
      READ_COMPUTE_EST_RESULT_SIZE,
      READ_COMPUTE_RESULT_COORDS,
      READ_COMPUTE_RANGE_RESULT_COORDS,
      READ_COMPUTE_SPARSE_RESULT_CELL_SLABS_SPARSE,
      READ_COMPUTE_SPARSE_RESULT_CELL_SLABS_DENSE,
      READ_COMPUTE_SPARSE_RESULT_TILES,
      READ_COMPUTE_TILE_OVERLAP,
      READ_COMPUTE_TILE_COORDS,
      READ_COMPUTE_RELEVANT_TILE_OVERLAP,
      READ_LOAD_RELEVANT_RTREES,
      READ_COMPUTE_RELEVANT_FRAGS,
      READ_INIT_STATE,
      READ_NEXT_PARTITION,
      READ_SPLIT_CURRENT_PARTITION,
      READ,
      DBG,
      WRITE,
      WRITE_SPLIT_COORDS_BUFF,
      WRITE_CHECK_COORD_OOB,
      WRITE_SORT_COORDS,
      WRITE_CHECK_COORD_DUPS,
      WRITE_COMPUTE_COORD_DUPS,
      WRITE_PREPARE_TILES,
      WRITE_COMPUTE_COORD_META,
      WRITE_FILTER_TILES,
      WRITE_TILES,
      WRITE_STORE_FRAG_META,
      WRITE_FINALIZE,
      WRITE_CHECK_GLOBAL_ORDER,
      WRITE_INIT_TILE_ITS,
      WRITE_COMPUTE_CELL_RANGES,
      WRITE_PREPARE_AND_FILTER_TILES,
      WRITE_ARRAY_META);

  /** Enumerates the stat counter types. */
  DEFINE_TILEDB_STATS(
      CounterType,
      READ_ARRAY_SCHEMA_SIZE,
      CONSOLIDATED_FRAG_META_SIZE,
      READ_FRAG_META_SIZE,
      READ_RTREE_SIZE,
      READ_TILE_OFFSETS_SIZE,
      READ_TILE_VAR_OFFSETS_SIZE,
      READ_TILE_VAR_SIZES_SIZE,
      READ_TILE_VALIDITY_OFFSETS_SIZE,
      READ_ARRAY_META_SIZE,
      READ_NUM,
      READ_BYTE_NUM,
      READ_UNFILTERED_BYTE_NUM,
      READ_ATTR_FIXED_NUM,
      READ_ATTR_VAR_NUM,
      READ_ATTR_NULLABLE_NUM,
      READ_DIM_FIXED_NUM,
      READ_DIM_VAR_NUM,
      READ_DIM_ZIPPED_NUM,
      READ_OVERLAP_TILE_NUM,
      READ_RESULT_NUM,
      READ_CELL_NUM,
      READ_LOOP_NUM,
      READ_OPS_NUM,
      WRITE_NUM,
      WRITE_ATTR_NUM,
      WRITE_ATTR_FIXED_NUM,
      WRITE_ATTR_VAR_NUM,
      WRITE_ATTR_NULLABLE_NUM,
      WRITE_DIM_NUM,
      WRITE_DIM_FIXED_NUM,
      WRITE_DIM_VAR_NUM,
      WRITE_DIM_ZIPPED_NUM,
      WRITE_BYTE_NUM,
      WRITE_FILTERED_BYTE_NUM,
      WRITE_RTREE_SIZE,
      WRITE_TILE_OFFSETS_SIZE,
      WRITE_TILE_VAR_OFFSETS_SIZE,
      WRITE_TILE_VAR_SIZES_SIZE,
      WRITE_TILE_VALIDITY_OFFSETS_SIZE,
      WRITE_FRAG_META_FOOTER_SIZE,
      WRITE_ARRAY_SCHEMA_SIZE,
      WRITE_TILE_NUM,
      WRITE_CELL_NUM,
      WRITE_ARRAY_META_SIZE,
      WRITE_OPS_NUM,
      CONSOLIDATE_STEP_NUM,
      VFS_S3_SLOW_DOWN_RETRIES);

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

  /** Starts a timer for the input timer stat. */
  void start_timer(TimerType stat);

  /** Ends a timer for the input timer stat. */
  void end_timer(TimerType stat);

  /** Adds `count` to the input counter stat. */
  void add_counter(CounterType stat, uint64_t count);

  /** Returns true if statistics are currently enabled. */
  bool enabled() const;

  /** Enable or disable statistics gathering. */
  void set_enabled(bool enabled);

  /** Reset all counters to zero. */
  void reset();

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

  /** A map of timer stats types to TimerStat instances. */
  std::unordered_map<TimerType, TimerStat, EnumHasher> timer_stats_;

  /** A map of counter stats. */
  std::unordered_map<CounterType, uint64_t, EnumHasher> counter_stats_;

  /** Mutex to protext in multi-threading scenarios. */
  std::mutex mtx_;

  /* ****************************** */
  /*       PRIVATE FUNCTIONS        */
  /* ****************************** */

  /** Dump the current consolidation stats. */
  std::string dump_consolidate() const;

  /** Dump the current write stats. */
  std::string dump_write() const;

  /** Dump the current read stats. */
  std::string dump_read() const;

  /** Dump the current vfs stats. */
  std::string dump_vfs() const;

  /** Parse a comma-separated string of enum names. */
  std::vector<std::string> parse_enum_names(
      const std::string& enum_names) const;

  /**
   * Writes the input message and seconds to the string stream, only if
   * it is not zero.
   */
  void write(std::stringstream* ss, const std::string& msg, double secs) const;

  /**
   * Writes the input message and count to the string stream, only if
   * it is not zero.
   */
  void write(
      std::stringstream* ss, const std::string& msg, uint64_t count) const;

  /**
   * Writes to the string stream the input message and the ratio
   * factor between the first count over the second, only if the counts
   * are not zero.
   */
  void write_factor(
      std::stringstream* ss,
      const std::string& msg,
      uint64_t count_a,
      uint64_t count_b) const;

  /**
   * Writes the input message and count to the string stream, only if
   * it is not zero. The input is written in both bytes and GBs format.
   */
  void write_bytes(
      std::stringstream* ss, const std::string& msg, uint64_t count) const;

  /**
   * Writes the input message and the ratio between the two input counts as a
   * percentage to the string stream, only if it is not zero.
   */
  void write_ratio(
      std::stringstream* ss,
      const std::string& msg,
      uint64_t count_a,
      uint64_t count_b) const;

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

/* ********************************* */
/*               GLOBAL              */
/* ********************************* */

/**
 * The singleton instance holding all global stats counters. The report will
 * be automatically made when this object is destroyed (at program termination).
 */
extern Stats all_stats;

/* ********************************* */
/*               MACROS              */
/* ********************************* */

#ifdef TILEDB_STATS

/** Marks the beginning of a stats-enabled function. This should come before the
 * first statement where you want the function timer to start. */
#define STATS_START_TIMER(stat)       \
  stats::all_stats.start_timer(stat); \
  auto __retval = [&]() {
/** Marks the end of a stats-enabled function. This should come after the last
 * statement in the function. Note that a function can have multiple exit paths
 * (i.e. multiple returns), but you should still put this macro after the very
 * last statement in the function. */
#define STATS_END_TIMER(stat)         \
  }                                   \
  ();                                 \
  if (stats::all_stats.enabled())     \
    stats::all_stats.end_timer(stat); \
  return __retval;

#define STATS_ADD_COUNTER(stat, c) \
  if (stats::all_stats.enabled())  \
    stats::all_stats.add_counter(stat, c);

#else

#define STATS_START_TIMER(stat) (void)stat;
#define STATS_END_TIMER(stat) (void)stat;
#define STATS_ADD_COUNTER(stat, c) \
  (void)stat;                      \
  (void)c;

#endif

}  // namespace stats
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_STATS_H
