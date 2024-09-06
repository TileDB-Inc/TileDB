/**
 * @file   watchdog.cc
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
 This file defines the Watchdog thread class.
 */

#include "tiledb/sm/global_state/watchdog.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/global_state/signal_handlers.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace global_state {

Watchdog& Watchdog::GetWatchdog() {
  // This is thread-safe in C++11.
  static Watchdog watchdog;
  return watchdog;
}

Watchdog::Watchdog() {
  should_exit_ = false;
}

Watchdog::~Watchdog() {
  {
    std::unique_lock<std::mutex> lck(mtx_);
    should_exit_ = true;
#ifndef __MINGW32__
    cv_.notify_one();
#endif
  }
  thread_.join();
}

void Watchdog::initialize() {
  thread_ = std::thread([this]() { watchdog_thread(this); });
}

void Watchdog::watchdog_thread(Watchdog* watchdog) {
  if (watchdog == nullptr) {
    return;
  }

  while (true) {
    std::unique_lock<std::mutex> lck(watchdog->mtx_);
    watchdog->cv_.wait_for(
        lck, std::chrono::milliseconds(constants::watchdog_thread_sleep_ms));

    if (SignalHandlers::signal_received()) {
      for (auto* sm : GlobalState::GetGlobalState().storage_managers()) {
        sm->cancel_all_tasks();
      }
    }

    if (watchdog->should_exit_) {
      break;
    }
  }
}

}  // namespace global_state
}  // namespace sm
}  // namespace tiledb
