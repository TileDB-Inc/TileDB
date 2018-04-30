/**
 * @file   global_state.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 This file declares the GlobalState class.
 */

#ifndef TILEDB_GLOBAL_STATE_H
#define TILEDB_GLOBAL_STATE_H

#include <set>

#include "tiledb/sm/storage_manager/storage_manager.h"

namespace tiledb {
namespace sm {
namespace global_state {

/**
 * Singleton class containing various global TileDB state.
 */
class GlobalState {
 public:
  GlobalState(const GlobalState&) = delete;
  GlobalState(const GlobalState&&) = delete;
  GlobalState& operator=(const GlobalState&) = delete;
  GlobalState& operator=(const GlobalState&&) = delete;

  /** Returns a reference to the singleton GlobalState instance. */
  static GlobalState& GetGlobalState();

  /**
   * Initializes all TileDB global state in an idempotent and threadsafe way.
   *
   * @param config The TileDB configuration parameters (or nullptr).
   * @return Status
   */
  Status initialize(Config* config);

  /**
   * Register the given StorageManger instance.
   * @param sm The StorageManager
   */
  void register_storage_manager(StorageManager* sm);

  /**
   * Unregister the given StorageManger instance.
   * @param sm The StorageManager
   */
  void unregister_storage_manager(StorageManager* sm);

  /**
   * Returns a copy of the set of registered StorageManager instances.
   */
  std::set<StorageManager*> storage_managers();

 private:
  /** The TileDB configuration parameters. */
  Config config_;

  /** True if global state has been initialized. */
  bool initialized_;

  /** Protects the initialized flag. */
  std::mutex init_mtx_;

  /** Set of currently active StorageManager instances. */
  std::set<StorageManager*> storage_managers_;

  /** Mutex protecting list of StorageManagers. */
  std::mutex storage_managers_mtx_;

  /** Constructor. */
  GlobalState();
};

}  // namespace global_state
}  // namespace sm
}  // namespace tiledb

#endif
