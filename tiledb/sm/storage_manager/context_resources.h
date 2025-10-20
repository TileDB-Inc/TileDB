/**
 * @file context_resources.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2025 TileDB, Inc.
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

class MemoryTracker;
class MemoryTrackerManager;
class MemoryTrackerReporter;
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

  [[nodiscard]] inline shared_ptr<VFS> vfs() const {
    return vfs_;
  }

  [[nodiscard]] inline shared_ptr<RestClient> rest_client() const {
    return rest_client_;
  }

  [[nodiscard]] inline MemoryTrackerManager& memory_tracker_manager() const {
    return *memory_tracker_manager_;
  }

  /**
   * Create a new MemoryTracker
   *
   * @return The created MemoryTracker.
   */
  shared_ptr<MemoryTracker> create_memory_tracker() const;

  /**
   * Return the ephemeral memory tracker.
   *
   * Use this tracker when you have a case where you need a memory tracker
   * temporarily, without access to a more appropriate tracker. For instance,
   * when using GenericTileIO when deserializing various objects we can use
   * this for the GenericTileIO. Make sure to not confuse this with the
   * memory tracker that might exists on what's being deserialized.
   *
   * @return The ephemeral MemoryTracker.
   */
  shared_ptr<MemoryTracker> ephemeral_memory_tracker() const;

  /**
   * Return the serialization memory tracker.
   *
   * Use this tracker on serialization buffers.
   *
   * @return The serialization MemoryTracker.
   */
  shared_ptr<MemoryTracker> serialization_memory_tracker() const;

  /**
   * Return the vector of supported filesystems.
   *
   * @param stats The parent stats to inherit from.
   * @param io_tp Thread pool for io-bound tasks.
   * @param config Configuration parameters.
   *
   * @return The vector of supported filesystems.
   */
  static std::vector<std::unique_ptr<FilesystemBase>> make_filesystems(
      [[maybe_unused]] stats::Stats* parent_stats,
      [[maybe_unused]] ThreadPool* io_tp,
      const Config& config);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The MemoryTrackerManager for this context. */
  mutable shared_ptr<MemoryTrackerManager> memory_tracker_manager_;

  /** The ephemeral MemoryTracker. */
  mutable shared_ptr<MemoryTracker> ephemeral_memory_tracker_;

  /** The MemoryTracker for serialization operations. */
  mutable shared_ptr<MemoryTracker> serialization_memory_tracker_;

  /** The MemoryTrackerReporter for this context. */
  mutable shared_ptr<MemoryTrackerReporter> memory_tracker_reporter_;

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
  shared_ptr<VFS> vfs_;

  /** The rest client (may be null if none was configured). */
  shared_ptr<RestClient> rest_client_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_CONTEXT_RESOURCES_H
