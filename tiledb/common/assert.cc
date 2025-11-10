/**
 * @file tiledb/common/assert.cc
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
 */

#include "tiledb/common/assert.h"

#include <mutex>
#include <vector>

namespace tiledb::common {

AssertFailure::AssertFailure(const std::string& what)
    : what_(what) {
}

[[noreturn]] void iassert_failure(
    const char* file, uint64_t line, const char* expr) {
  throw AssertFailure(file, line, expr);
}

static std::mutex passertFailureCallbackMutex;
static std::list<std::function<void()>> passertFailureCallbacks;

void passert_failure_run_callbacks(void) {
  std::unique_lock<std::mutex> lk(passertFailureCallbackMutex);
  for (auto& callback : passertFailureCallbacks) {
    callback();
  }
}

[[noreturn]] void passert_failure_abort(void) {
  passert_failure_run_callbacks();
  std::abort();
}

[[noreturn]] void passert_failure(
    const char* file, uint64_t line, const char* expr) {
  std::cerr << "FATAL TileDB core library internal error: " << expr
            << std::endl;
  std::cerr << "  " << file << ":" << line << std::endl;

  passert_failure_abort();
}

#ifdef TILEDB_ASSERTIONS

PAssertFailureCallbackRegistration::PAssertFailureCallbackRegistration(
    std::function<void()>&& callback) {
  std::unique_lock<std::mutex> lk(passertFailureCallbackMutex);

  passertFailureCallbacks.push_front(
      std::forward<std::function<void()>>(callback));

  callback_node_ = passertFailureCallbacks.begin();
}

PAssertFailureCallbackRegistration::~PAssertFailureCallbackRegistration() {
  std::unique_lock<std::mutex> lk(passertFailureCallbackMutex);
  passertFailureCallbacks.erase(callback_node_);
}

#endif

}  // namespace tiledb::common
