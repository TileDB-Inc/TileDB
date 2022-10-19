/**
 * @file   context.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines class Context.
 */

#ifndef TILEDB_CONTEXT_H
#define TILEDB_CONTEXT_H

#include "tiledb/common/exception/exception.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <mutex>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * This class manages the context for the C API, wrapping a
 * storage manager object.
 * */
class Context {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Context(const Config& = Config());

  /** Destructor. */
  ~Context() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the last error status. */
  optional<std::string> last_error();

  /**
   * Saves a `Status` as the last error.
   */
  void save_error(const Status& st);

  /**
   * Saves a `StatusException` as the last error.
   */
  void save_error(const StatusException& st);

  /** Returns a pointer to the underlying storage manager. */
  StorageManager* storage_manager() const;

  /** Returns the thread pool for compute-bound tasks. */
  ThreadPool* compute_tp() const;

  /** Returns the thread pool for io-bound tasks. */
  ThreadPool* io_tp() const;

  /** Returns the internal stats object. */
  stats::Stats* stats() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The last error occurred. */
  optional<std::string> last_error_{nullopt};

  /** A mutex for thread-safety. */
  std::mutex mtx_;

  /** The class logger. */
  shared_ptr<Logger> logger_;

  /** The class unique logger prefix */
  inline static std::string logger_prefix_ =
      std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count()) +
      "-Context: ";

  /**
   * Counter for generating unique identifiers for `Logger` objects.
   */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /** The thread pool for compute-bound tasks. */
  mutable ThreadPool compute_tp_;

  /** The thread pool for io-bound tasks. */
  mutable ThreadPool io_tp_;

  /** The class stats. */
  shared_ptr<stats::Stats> stats_;

  /** The storage manager. */
  tdb_unique_ptr<StorageManager> storage_manager_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /**
   * Initializes the context with the input config. Called only from
   * constructor.
   */
  Status init(const Config& config);

  /**
   * Get maximum number of threads to use in thread pools, based on config
   * parameters.
   *
   * @param config The Config to look up max thread information from.
   * @param max_thread_count (out) Variable to store max thread count.
   * @return Status of request.
   */
  Status get_config_thread_count(
      const Config& config, uint64_t& max_thread_count);

  /**
   * Get number of threads to use in compute thread pool, based on config
   * parameters.  Will return the max of the configured value and the max thread
   * count returned by get_max_thread_count()
   *
   * @param config The Config to look up the compute thread information from.
   * @return Compute thread count.
   */
  size_t get_compute_thread_count(const Config& config);

  /**
   * Get number of threads to use in io thread pool, based on config
   * parameters.  Will return the max of the configured value and the max thread
   * count returned by get_max_thread_count()
   *
   * @param config The Config to look up the io thread information from.
   * @return IO thread count.
   */
  size_t get_io_thread_count(const Config& config);

  /**
   * Initializes global and local logger.
   *
   * @param config The configuration parameters.
   * @return Status
   */
  Status init_loggers(const Config& config);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CONTEXT_H
