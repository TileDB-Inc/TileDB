/**
 * @file   watchdog.h
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
 This file declares the Watchdog thread class.
 */

#ifndef TILEDB_WATCHDOG_H
#define TILEDB_WATCHDOG_H

#include <condition_variable>
#include <mutex>
#include <thread>

#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {
namespace global_state {

/**
 * Singleton class that watches for global events and performs actions based on
 * them (e.g. actions taken on receiving process signals).
 */
class Watchdog {
 public:
  Watchdog(const Watchdog&) = delete;
  Watchdog(const Watchdog&&) = delete;
  Watchdog& operator=(const Watchdog&) = delete;
  Watchdog& operator=(const Watchdog&&) = delete;

  /** Returns a reference to the singleton Watchdog instance. */
  static Watchdog& GetWatchdog();

  /**
   * Initializes the Watchdog thread.
   *
   * @return Status
   */
  Status initialize();

 private:
  /** Condition variable for coordinating with the watchdog thread. */
  std::condition_variable cv_;

  /** Mutex for coordinating with the watchdog thread. */
  std::mutex mtx_;

  /** True when the watchdog thread should terminate. */
  bool should_exit_;

  /** Watchdog thread handle. */
  std::thread thread_;

  /** Constructor. */
  Watchdog();

  /** Destructor. */
  ~Watchdog();

  /** Watchdog thread function. */
  static void watchdog_thread(Watchdog* watchdog);
};

}  // namespace global_state
}  // namespace sm
}  // namespace tiledb

#endif
