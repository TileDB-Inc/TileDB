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

#include "context_registry.h"
#include "job.h"
#include "tiledb/common/exception/exception.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <mutex>

namespace tiledb::common {
class Logger;
}

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * Base class for `Context`
 *
 * This class exists to deal with order-of-initialization between
 * `StorageManager` and `CancellationSource`. A cancellation source is required
 * for the base class `JobRoot`, but currently the constructor for
 * `CancellationSource` requires a pointer to a storage manager. This class
 * breaks what would otherwise be a cycle by moving the `StorageManager` member
 * variable into a base class.
 *
 * This class won't need to exist once `StorageManager` is gone.
 */
struct ContextBase {
  /** The class logger. */
  shared_ptr<Logger> logger_;

  /**
   * Initializer for `logger_prefix_` cannot throw during static initialization.
   */
  static std::string build_logger_prefix() noexcept {
    try {
      return std::to_string(
                 std::chrono::duration_cast<std::chrono::nanoseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count()) +
             "-Context: ";
    } catch (...) {
      try {
        return {"TimeError-Context:"};
      } catch (...) {
        return {};
      }
    }
  }

  /** The class unique logger prefix */
  inline static std::string logger_prefix_{build_logger_prefix()};

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

  explicit ContextBase(const Config& config);
};

/**
 * This class manages the context for the C API, wrapping a
 * storage manager object.
 */
class Context : public ContextBase, public JobRoot {
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
  ~Context() override = default;

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

  /**
   * Cancel all free-running activity under this context.
   *
   * This function is synchronous. It does not return until all activity under
   * the context has ended.
   *
   * @section Maturity
   *
   * At the present time, not all activities that can operate under a context
   * are interruptible by the context. They'll all eventually end, but it may
   * not be promptly.
   */
  void cancel_all_tasks();

  /**
   * Derived from `JobRoot`
   */
  [[nodiscard]] ContextResources& resources() const override {
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

  /**
   * The handle of this context within the context registry.
   */
  ContextRegistry::handle_type context_handle_;

  /** The last error occurred. */
  optional<std::string> last_error_{nullopt};

  /**
   * Mutex protects access to `last_error_`.
   */
  std::mutex mtx_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /**
   * Initializes global and local logger.
   *
   * @param config The configuration parameters.
   */
  void init_loggers(const Config& config);
};

}  // namespace tiledb::sm

#endif  // TILEDB_CONTEXT_H
