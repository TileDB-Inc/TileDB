/**
 * @file   heap_profiler.h
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
 * This file defines the HeapProfiler.
 */

#ifndef TILEDB_HEAP_PROFILER_H
#define TILEDB_HEAP_PROFILER_H

#include <cassert>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "tiledb/common/macros.h"

namespace tiledb {
namespace common {

class HeapProfiler {
 public:
  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /** Constructor. */
  HeapProfiler();

  /** Destructor. */
  ~HeapProfiler();

  DISABLE_COPY(HeapProfiler);

  DISABLE_MOVE(HeapProfiler);

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  DISABLE_COPY_ASSIGN(HeapProfiler);

  DISABLE_MOVE_ASSIGN(HeapProfiler);

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  /** Returns true if the heap profiler is enabled. */
  inline bool enabled() const {
    // We know that this instance has been initalized
    // if `reserved_memory_` has been set.
    return reserved_memory_ != nullptr;
  }

  /**
   * Initialize and start the profiler.
   *
   * @param file_name_prefix If empty, stats are dumped to stdout.
   *   If non-empty, this specifies the file_name prefix to
   *   write to. For example, value "tiledb_mem_stats" will write
   *   to "tiledb_mem_stats__1611170501", where the postfix is
   *   determined by the current epoch.
   * @param dump_interval_ms If non-zero, this spawns a dedicated
   *   thread to dump on this time interval.
   * @param dump_interval_bytes If non-zero, a dump will occur when
   *   the total number of lifetime allocated bytes is increased by
   *   more than this amount.
   * @param dump_threshold_bytes If non-zero, labeled allocations with
   *   a number of bytes lower than this threshold will not be reported
   *   in the dump.
   */
  void enable(
      const std::string& file_name_prefix,
      uint64_t dump_interval_ms,
      uint64_t dump_interval_bytes,
      uint64_t dump_threshold_bytes);

  /**
   * Records a successful allocation.
   *
   * @param p The pointer to the allocated memory.
   * @param size The allocation byte size of `p`.
   * @param label An optional label to associate with this
   *   allocation.
   */
  void record_alloc(void* p, size_t size, const std::string& label = "");

  /**
   * Records a deallocation, where `p` has been recorded
   * in `record_alloc()`.
   *
   * @param p The pointer to the deallocated memory.
   */
  void record_dealloc(const uint64_t addr);

  /** Dumps the current stats and terminates the process. */
  void dump_and_terminate();

  /** Dumps the current stats. */
  void dump();

  /* ****************************** */
  /*       PRIVATE ATTRIBUTES       */
  /* ****************************** */

  /**
   * When non-empty, stats are dumped to this file instead of
   * stdout.
   */
  std::string file_name_;

  /**
   * When non-zero, stats are asynchronously dumped on
   * this time interval.
   */
  uint64_t dump_interval_ms_;

  /**
   * When non-zero, stats are synchronously dumped at the
   * time of an allocation that increases the total number
   * of allocated bytes to the next interval defiend by
   * this value.
   */
  uint64_t dump_interval_bytes_;

  /**
   * When non-zero, the dump will not report labeled allocations
   * with a byte count lower than this number.
   */
  uint64_t dump_threshold_bytes_;

  /**
   * When `dump_interval_ms_` is non-zero, this thread is
   * responsible for periodically dumping the stats.
   */
  std::unique_ptr<std::thread> periodic_dump_thread_;

  /** Protects all below member variables. */
  std::mutex mutex_;

  /**
   * Reserve a fixed size of memory at initialization. When we
   * are out of memory, we will free this reserved memory to
   * in an attempt to ensure that the final
   */
  void* reserved_memory_;

  /**
   * Maps an address to a pair, where the pair contains the current
   * number of allocated bytes and a pointer to a label. The
   * labels also exist on `labels_cache_` as a way to de-duplicate
   * identical labels.
   */
  std::unordered_map<uint64_t, std::pair<size_t, const std::string*>>
      addr_to_alloc_;

  /**
   * The `record_alloc` routine allows an optional, free-form string
   * label to associate with the allocation. For example, this could be
   * an attribute name, a file name, a line number, etc. This map is
   * used to deduplicate identical strings among separate
   * allocations.
   *
   * The key is a shared, heap-allocated string. The associated value
   * is a reference counter. When the reference counter reaches zero,
   * the label is removed from this cache.
   */
  std::unordered_map<std::string, uint64_t> labels_cache_;

  /** The total number of allocation operations. */
  uint64_t num_allocs_;

  /** The total number of deallocation operations. */
  uint64_t num_deallocs_;

  /** The total number of allocated bytes. */
  uint64_t num_alloc_bytes_;

  /** The total number of deallocated bytes. */
  uint64_t num_dealloc_bytes_;

  /**
   * This is not a stat, but to track the allocation size
   * of the last dump triggered by `dump_interval_bytes_`.
   */
  uint64_t last_interval_dump_alloc_bytes_;

  /* ****************************** */
  /*       PRIVATE FUNCTIONS        */
  /* ****************************** */

  /** Initializes and starts `periodic_dump_thread_`. */
  void start_periodic_dump();

  /** Performs an interval dump if necessary. */
  void try_interval_dump();

  /** Creates a dump file and returns the file name. */
  std::string create_dump_file(const std::string& file_name_prefix);

  /** Fetches the cached string matching the value of `label`. */
  const std::string* fetch_label_ptr(const std::string& label);

  /** Returns the cache string fetched from `fetch_label_ptr`. */
  void release_label_ptr(const std::string* label);

  /**
   * Dumps the current stats and terminates the process without
   * locking on `mutex_`.
   */
  void dump_and_terminate_internal();

  /* Dumps the current stats without locking on `mutex_`. */
  void dump_internal();
};

/* ********************************* */
/*               GLOBAL              */
/* ********************************* */

/** The singleton instance holding all heap stats. */
extern HeapProfiler heap_profiler;

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_HEAP_PROFILER_H
