/**
 * @file tiledb/common/coroutine/schedule_on.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * Awaitable that resumes a coroutine on a specified ThreadPool.
 *
 * Usage:
 *   co_await ScheduleOn{compute_tp};  // resume on compute thread pool
 *   // now running on a compute_tp thread
 */

#ifndef TILEDB_COROUTINE_SCHEDULE_ON_H
#define TILEDB_COROUTINE_SCHEDULE_ON_H

#include <coroutine>

#include "tiledb/common/thread_pool/thread_pool.h"

namespace tiledb::common {

/**
 * Awaitable that, when co_awaited, suspends the current coroutine and
 * schedules its resumption on the given ThreadPool.
 */
struct ScheduleOn {
  ThreadPool& tp;

  bool await_ready() const noexcept {
    return false;
  }

  void await_suspend(std::coroutine_handle<> h) const {
    tp.schedule_resume(h);
  }

  void await_resume() const noexcept {
  }
};

}  // namespace tiledb::common

#endif  // TILEDB_COROUTINE_SCHEDULE_ON_H
