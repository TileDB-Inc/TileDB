/**
 * @file   context.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/cancellation_source.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <mutex>

namespace tiledb::common {
class Logger;
}

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * This class manages the context for the C API, wrapping a
 * storage manager object.
 */
class Context {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Default constructor is deleted.
   */
  Context() = delete;

  /** Constructor. */
  explicit Context(const Config&);

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
   * Saves a `std::string` as the last error.
   */
  void save_error(const std::string& msg);

  /**
   * Saves a `StatusException` as the last error.
   */
  void save_error(const StatusException& st);

  /** Pointer to the underlying storage manager. */
  inline StorageManager* storage_manager() {
    return &storage_manager_;
  }

  /** Pointer to the underlying storage manager. */
  inline const StorageManager* storage_manager() const {
    return &storage_manager_;
  }

  inline CancellationSource cancellation_source() const {
    return CancellationSource(storage_manager());
  }

  [[nodiscard]] inline ContextResources& resources() const {
    return resources_;
  }

  /** Returns the thread pool for compute-bound tasks. */
  [[nodiscard]] inline ThreadPool* compute_tp() const {
    return &(resources_.compute_tp());
  }

  /** Returns the thread pool for io-bound tasks. */
  [[nodiscard]] inline ThreadPool* io_tp() const {
    return &(resources_.io_tp());
  }

  [[nodiscard]] inline RestClient& rest_client() const {
    auto x = resources_.rest_client();
    if (!x) {
      throw std::runtime_error(
          "Failed to retrieve RestClient; the underlying instance is null and "
          "may not have been configured.");
    }
    return *(x.get());
  }

  [[nodiscard]] inline bool has_rest_client() const {
    auto x = resources_.rest_client();
    return bool(x);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The last error occurred. */
  optional<std::string> last_error_{nullopt};

  /**
   * Mutex protects access to `last_error_`.
   */
  std::mutex mtx_;

  /** The class logger. */
  shared_ptr<Logger> logger_;

  /**
   * Returns the shared logger prefix string (constant for all Context
   * instances).
   */
  static const std::string& logger_prefix_();

  /**
   * Counter for generating unique identifiers for `Logger` objects.
   */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /** The class resources. */
  mutable ContextResources resources_;

  /**
   * The storage manager.
   */
  StorageManager storage_manager_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

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
   */
  void init_loggers(const Config& config);
};

}  // namespace tiledb::sm

#endif  // TILEDB_CONTEXT_H
