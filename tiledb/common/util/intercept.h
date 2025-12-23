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

#include <list>

namespace tiledb::intercept {

template <std::copyable... T>
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

  void event(T&&... args) {
    for (auto& callback : callbacks_) {
      callback(std::forward<T>(args)...);
    }
  }

  CallbackRegistration and_also(std::function<void(T&&...)>&& callback) {
    callbacks_.push_back(std::move(callback));
    return CallbackRegistration(*this, std::next(callbacks_.end(), -1));
  }

 private:
  friend class CallbackRegistration;

  std::list<std::function<void(T&&...)>> callbacks_;
};

}  // namespace tiledb::intercept

#define DECLARE_INTERCEPT(name, ...) \
  extern tiledb::intercept::InterceptionPoint<__VA_ARGS__>& name()

#define DEFINE_INTERCEPT(name, ...)                                \
  tiledb::intercept::InterceptionPoint<__VA_ARGS__>& name() {      \
    static tiledb::intercept::InterceptionPoint<__VA_ARGS__> impl; \
    return impl;                                                   \
  }

#define INTERCEPT(name, ...)   \
  do {                         \
    name().event(__VA_ARGS__); \
  } while (0)

#else  // not defined(TILEDB_INTERCEPTS)

#define DECLARE_INTERCEPT(...)
#define DEFINE_INTERCEPT(...)
#define INTEREPT(...)

#endif

#endif
