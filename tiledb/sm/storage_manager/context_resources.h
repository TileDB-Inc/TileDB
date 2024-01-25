/**
 * @file context_resources.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2022 TileDB, Inc.
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
 * This file defines class ContextResources.
 */

#ifndef TILEDB_CONTEXT_RESOURCES_H
#define TILEDB_CONTEXT_RESOURCES_H

#include "tiledb/common/exception/exception.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/stats/global_stats.h"

using namespace tiledb::common;

namespace tiledb::sm {

class RestClient;

/**
 * This class manages the context for the C API, wrapping a
 * storage manager object.
 */
class ContextResources {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Default constructor is deleted.
   */
  ContextResources() = delete;

  /** Constructor. */
  explicit ContextResources(
      const Config& config,
      shared_ptr<Logger> logger,
      size_t compute_thread_count,
      size_t io_thread_count,
      std::string stats_name);

  DISABLE_COPY_AND_COPY_ASSIGN(ContextResources);
  DISABLE_MOVE_AND_MOVE_ASSIGN(ContextResources);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the config object. */
  [[nodiscard]] inline Config& config() const {
    return config_;
  }

  /** Returns the internal logger object. */
  [[nodiscard]] inline shared_ptr<Logger> logger() const {
    return logger_;
  }

  /** Returns the thread pool for compute-bound tasks. */
  [[nodiscard]] inline ThreadPool& compute_tp() const {
    return compute_tp_;
  }

  /** Returns the thread pool for io-bound tasks. */
  [[nodiscard]] inline ThreadPool& io_tp() const {
    return io_tp_;
  }

  /** Returns the internal stats object. */
  [[nodiscard]] inline stats::Stats& stats() const {
    return *(stats_.get());
  }

  [[nodiscard]] inline VFS& vfs() const {
    return vfs_;
  }

  [[nodiscard]] inline shared_ptr<RestClient> rest_client() const {
    return rest_client_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The configuration for this ContextResources */
  mutable Config config_;

  /** The class logger. */
  shared_ptr<Logger> logger_;

  /** The thread pool for compute-bound tasks. */
  mutable ThreadPool compute_tp_;

  /** The thread pool for io-bound tasks. */
  mutable ThreadPool io_tp_;

  /** The class stats. */
  shared_ptr<stats::Stats> stats_;

  /**
   * Virtual filesystem handler. It directs queries to the appropriate
   * filesystem backend. Note that this is stateful.
   */
  mutable VFS vfs_;

  /** The rest client (may be null if none was configured). */
  shared_ptr<RestClient> rest_client_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_CONTEXT_RESOURCES_H
