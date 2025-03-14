/**
 * @file   storage_manager_canonical.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines class StorageManagerCanonical.
 */

#ifndef TILEDB_STORAGE_MANAGER_CANONICAL_H
#define TILEDB_STORAGE_MANAGER_CANONICAL_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <thread>

#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/enums/walk_order.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/group/group.h"
#include "tiledb/sm/misc/cancelable_tasks.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/storage_manager/context_resources.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Query;

enum class EncryptionType : uint8_t;

/** The storage manager that manages pretty much nothing in TileDB. */
class StorageManagerCanonical {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Complete, C.41-compliant constructor
   *
   * The `resources` argument is only used for its `vfs()` member function. This
   * is the VFS instance that's waited on in `cancel_all_tasks`.
   *
   *
   * @param resources Resource object from associated context
   * @param config The configuration parameters.
   */
  StorageManagerCanonical(
      ContextResources& resources,
      const shared_ptr<Logger>& logger,
      const Config& config);

 public:
  /** Destructor. */
  ~StorageManagerCanonical();

  DISABLE_COPY_AND_COPY_ASSIGN(StorageManagerCanonical);
  DISABLE_MOVE_AND_MOVE_ASSIGN(StorageManagerCanonical);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Cancels all background tasks. */
  Status cancel_all_tasks();

  /** Returns true while all tasks are being cancelled. */
  bool cancellation_in_progress() const;

  /** Submits a query for (sync) execution. */
  Status query_submit(Query* query);

 private:
  /* ********************************* */
  /*        PRIVATE DATATYPES          */
  /* ********************************* */

  /**
   * Helper RAII struct that increments 'queries_in_progress' in the constructor
   * and decrements in the destructor, on the given StorageManagerCanonical
   * instance.
   *
   * This ensures that the counter is decremented even in the case of
   * exceptions.
   */
  struct QueryInProgress {
    /** The StorageManagerCanonical instance. */
    StorageManagerCanonical* sm;

    /**
     * Constructor. Calls increment_in_progress() on given
     * StorageManagerCanonical.
     */
    QueryInProgress(StorageManagerCanonical* sm)
        : sm(sm) {
      sm->increment_in_progress();
    }

    /**
     * Destructor. Calls decrement_in_progress() on given
     * StorageManagerCanonical.
     */
    ~QueryInProgress() {
      sm->decrement_in_progress();
    }
  };

  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The GlobalState to use for this StorageManager. */
  shared_ptr<global_state::GlobalState> global_state_;

  /**
   * VFS instance used in `cancel_all_tasks`.
   */
  VFS& vfs_;

  /** Set to true when tasks are being cancelled. */
  bool cancellation_in_progress_;

  /** Mutex protecting cancellation_in_progress_. */
  mutable std::mutex cancellation_in_progress_mtx_;

  /** Stores the TileDB configuration parameters. */
  Config config_;

  /** Count of the number of queries currently in progress. */
  uint64_t queries_in_progress_;

  /** Guards queries_in_progress_ counter. */
  std::mutex queries_in_progress_mtx_;

  /** Guards queries_in_progress_ counter. */
  std::condition_variable queries_in_progress_cv_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /** Decrement the count of in-progress queries. */
  void decrement_in_progress();

  /** Increment the count of in-progress queries. */
  void increment_in_progress();

  /** Block until there are zero in-progress queries. */
  void wait_for_zero_in_progress();
};

}  // namespace tiledb::sm

#endif  // TILEDB_STORAGE_MANAGER_CANONICAL_H
