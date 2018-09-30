/**
 * @file   global_state.cc
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
 This file defines the GlobalState class.
 */

#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/global_state/openssl_state.h"
#include "tiledb/sm/global_state/signal_handlers.h"
#include "tiledb/sm/global_state/tbb_state.h"
#include "tiledb/sm/global_state/watchdog.h"
#include "tiledb/sm/misc/constants.h"

namespace tiledb {
namespace sm {
namespace global_state {

GlobalState& GlobalState::GetGlobalState() {
  // This is thread-safe in C++11.
  static GlobalState globalState;
  return globalState;
}

GlobalState::GlobalState() {
  initialized_ = false;
}

Status GlobalState::initialize(Config* config) {
  std::unique_lock<std::mutex> lck(init_mtx_);

  // initialize tbb with configured number of threads
  RETURN_NOT_OK(init_tbb(config));

  // run these operations once
  if (!initialized_) {
    if (config != nullptr) {
      config_ = *config;
    }
    if (config_.sm_params().enable_signal_handlers_) {
      RETURN_NOT_OK(SignalHandlers::GetSignalHandlers().initialize());
    }
    RETURN_NOT_OK(Watchdog::GetWatchdog().initialize());
    RETURN_NOT_OK(init_openssl());
    initialized_ = true;
  }

  return Status::Ok();
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
}  // namespace global_state
}  // namespace sm
}  // namespace tiledb