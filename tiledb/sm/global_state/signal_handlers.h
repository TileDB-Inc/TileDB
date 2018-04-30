/**
 * @file   signal_handlers.h
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
 This file declares SignalHandlers.
 */

#ifndef TILEDB_SIGNAL_HANDLERS_H
#define TILEDB_SIGNAL_HANDLERS_H

#include <atomic>

#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {
namespace global_state {

/**
 * Singleton class that manages process-level signals and signal handlers.
 */
class SignalHandlers {
 public:
  SignalHandlers(const SignalHandlers&) = delete;
  SignalHandlers(const SignalHandlers&&) = delete;
  SignalHandlers& operator=(const SignalHandlers&) = delete;
  SignalHandlers& operator=(const SignalHandlers&&) = delete;

  /** Returns a reference to the singleton SignalHandlers instance. */
  static SignalHandlers& GetSignalHandlers();

  /**
   * Initialize the signal handlers.
   *
   * @return Status
   */
  Status initialize();

  /**
   * Returns true if a signal has been received. This will only return true on
   * the first call after a signal was received, and then false until another
   * signal is received. This is a thread-safe operation (only one thread will
   * get a true return value).
   */
  static bool signal_received();

  /**
   * Safely write the given message to stderr, ignoring errors.
   * This can be called from a signal handler.
   *
   * @param msg Message text
   * @param msg_len Number of chars in message
   */
  static void safe_stderr(const char* msg, size_t msg_len);

 private:
  /** Constructor. */
  SignalHandlers(){};
};

}  // namespace global_state
}  // namespace sm
}  // namespace tiledb

#endif