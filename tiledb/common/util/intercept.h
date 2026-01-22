/**
 * @file   intercept.h
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
 * This file provides declarations for interception, which allows us
 * to run arbitrary code internally at pre-defined "interception points".
 * This can be used to verify that a test case causes a specific event
 * to occur, or it can be used to pause and resume tasks so as
 * to simulate particular patterns of concurrent execution, or it can
 * be used to simulate exceptions being thrown from particular blocks
 * of code, etc. etc.
 */

#ifndef TILEDB_TEST_INTERCEPTION_H
#define TILEDB_TEST_INTERCEPTION_H

#if defined(TILEDB_INTERCEPTS)

#include "tiledb/common/macros.h"

#include <functional>
#include <list>
#include <type_traits>

namespace tiledb::intercept {

/**
 * Describes a type which can be passed to the INTERCEPT macro.
 */
template <typename T>
concept interceptible = std::copyable<T> || std::is_reference_v<T>;

/**
 * A set of actions to perform at a logical interception point.
 *
 * An `InterceptionPoint` may capture values from an `INTERCEPT` (see below)
 * and runs an arbitrary set of callbacks over those values.
 *
 * Do not use this directly; instead use the macros `DECLARE_INTERCEPT`,
 * `DEFINE_INTERCEPT`, and `INTERCEPT` to create functions which
 * respectively create and invoke callbacks of `InterceptionPoint`.
 */
template <interceptible... T>
class InterceptionPoint {
  using Self = InterceptionPoint<T...>;

 public:
  class CallbackRegistration {
    Self& intercept_;

    using iter = std::list<std::function<void(T&&...)>>::const_iterator;
    iter callback_node_;

   public:
    CallbackRegistration(InterceptionPoint& intercept, iter node)
        : intercept_(intercept)
        , callback_node_(node) {
    }

    ~CallbackRegistration() {
      intercept_.callbacks_.erase(callback_node_);
    }

    DISABLE_COPY_AND_COPY_ASSIGN(CallbackRegistration);
  };

  void event(T... args) {
    for (auto& callback : callbacks_) {
      callback(std::forward<T>(args)...);
    }
  }

  CallbackRegistration and_also(std::function<void(T...)>&& callback) {
    callbacks_.push_back(std::move(callback));
    return CallbackRegistration(*this, std::prev(callbacks_.end()));
  }

 private:
  friend class CallbackRegistration;

  std::list<std::function<void(T&&...)>> callbacks_;
};

template <typename... Args>
void forward(auto& intercept, Args&&... args) {
  intercept.event(std::forward<Args>(args)...);
}

}  // namespace tiledb::intercept

#define DECLARE_INTERCEPT(name, ...) \
  extern tiledb::intercept::InterceptionPoint<__VA_ARGS__>& name()

#define DEFINE_INTERCEPT(name, ...)                                \
  tiledb::intercept::InterceptionPoint<__VA_ARGS__>& name() {      \
    static tiledb::intercept::InterceptionPoint<__VA_ARGS__> impl; \
    return impl;                                                   \
  }

#define INTERCEPT(name, ...) \
  tiledb::intercept::forward(name() __VA_OPT__(, ) __VA_ARGS__)

#else  // not defined(TILEDB_INTERCEPTS)

/**
 * Similar to `assert`, expand to nothing if TILEDB_INTERCEPTS is not enabled,
 * so that we can leave the intercepts in the code and have them optimized
 * out for production builds.
 */
#define DECLARE_INTERCEPT(...)
#define DEFINE_INTERCEPT(...)
#define INTERCEPT(...)

#endif

#endif
