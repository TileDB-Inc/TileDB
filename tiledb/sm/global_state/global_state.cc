/**
 * @file   global_state.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
#include "tiledb/sm/global_state/libcurl_state.h"
#include "tiledb/sm/global_state/openssl_state.h"
#include "tiledb/sm/global_state/signal_handlers.h"
#include "tiledb/sm/global_state/watchdog.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#ifdef __linux__
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/filesystem/posix.h"
#include "tiledb/sm/misc/utils.h"
#endif

#include <cassert>

using namespace tiledb::common;

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

Status GlobalState::init(const Config* config) {
  std::unique_lock<std::mutex> lck(init_mtx_);

  // Get config params
  bool found;
  bool enable_signal_handlers = false;
  RETURN_NOT_OK(config_.get<bool>(
      "sm.enable_signal_handlers", &enable_signal_handlers, &found));
  assert(found);

  // run these operations once
  if (!initialized_) {
    if (config != nullptr) {
      config_ = *config;
    }
    if (enable_signal_handlers) {
      RETURN_NOT_OK(SignalHandlers::GetSignalHandlers().initialize());
    }
    RETURN_NOT_OK(Watchdog::GetWatchdog().initialize());
    RETURN_NOT_OK(init_openssl());
    RETURN_NOT_OK(init_libcurl());

#ifdef __linux__
    // We attempt to find the linux ca cert bundle
    // This only needs to happen one time, and then we will use the file found
    // for each s3/rest call as appropriate
    Posix posix;
    ThreadPool tp;
    tp.init();
    posix.init(config_, &tp);
    cert_file_ = utils::https::find_ca_certs_linux(posix);
#endif

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

OpenArrayMemoryTracker* GlobalState::array_memory_tracker(
    const URI& array_uri, StorageManager* caller) {
  std::unique_lock<std::mutex> lck(storage_managers_mtx_);
  for (auto& storage_manager : storage_managers_) {
    if (storage_manager == caller)
      continue;

    auto tracker = storage_manager->array_memory_tracker(array_uri, false);
    if (tracker != nullptr)
      return tracker;
  }

  return nullptr;
}

const std::string& GlobalState::cert_file() {
  return cert_file_;
}

}  // namespace global_state
}  // namespace sm
}  // namespace tiledb
