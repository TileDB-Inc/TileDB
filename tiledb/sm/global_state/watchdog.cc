#include "tiledb/sm/global_state/watchdog.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/global_state/signal_handlers.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/logger.h"

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
    cv_.notify_one();
  }
  thread_.join();
}

Status Watchdog::initialize() {
  try {
    thread_ = std::thread([this]() { watchdog_thread(this); });
  } catch (const std::exception& e) {
    return Status::Error(
        std::string("Could not initialize watchdog thread; ") + e.what());
  }
  return Status::Ok();
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
