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
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/stats/global_stats.h"

using namespace tiledb::common;

namespace tiledb::sm {

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
      size_t compute_thread_count, size_t io_thread_count, stats::Stats* stats);

  DISABLE_COPY_AND_COPY_ASSIGN(ContextResources);
  DISABLE_MOVE_AND_MOVE_ASSIGN(ContextResources);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

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

  /** The thread pool for compute-bound tasks. */
  mutable ThreadPool compute_tp_;

  /** The thread pool for io-bound tasks. */
  mutable ThreadPool io_tp_;

  /** The class stats. */
  stats::Stats* stats_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_CONTEXT_RESOURCES_H
