/**
 * @file   global_state.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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
 This file defines the GlobalState class.
 */

#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/global_state/signal_handlers.h"
#include "tiledb/sm/global_state/watchdog.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#ifdef __linux__
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <cassert>

using namespace tiledb::common;

namespace tiledb::sm::global_state {

shared_ptr<GlobalState> GlobalState::GetGlobalState() {
  // This is thread-safe in C++11.
  static shared_ptr<GlobalState> globalState = make_shared<GlobalState>(HERE());
  return globalState;
}

GlobalState::GlobalState() {
  initialized_ = false;
}

void GlobalState::init(const Config& config) {
  std::unique_lock<std::mutex> lck(init_mtx_);

  // Get config params
  bool enable_signal_handlers =
      config.get<bool>("sm.enable_signal_handlers", Config::must_find);

  // run these operations once
  if (!initialized_) {
    config_ = config;

    if (enable_signal_handlers) {
      SignalHandlers::GetSignalHandlers().initialize();
    }
    Watchdog::GetWatchdog().initialize();

    initialized_ = true;
  }
}

void GlobalState::register_storage_manager(StorageManager* sm) {
  std::unique_lock<std::mutex> lck(storage_managers_mtx_);
  storage_managers_.insert(sm);
}

void GlobalState::unregister_storage_manager(StorageManager* sm) {
  std::unique_lock<std::mutex> lck(storage_managers_mtx_);
  storage_managers_.erase(sm);
}

std::set<StorageManager*> GlobalState::storage_managers() {
  std::unique_lock<std::mutex> lck(storage_managers_mtx_);
  return storage_managers_;
}

}  // namespace tiledb::sm::global_state
