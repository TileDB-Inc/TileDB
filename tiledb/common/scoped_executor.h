/**
 * @file   scoped_executor.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file defines and implements the `ScopedExecutor` class.
 */

#ifndef TILEDB_SCOPED_EXECUTOR_H
#define TILEDB_SCOPED_EXECUTOR_H

#include <functional>

#include "tiledb/common/macros.h"

namespace tiledb {
namespace common {

class ScopedExecutor final {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  ScopedExecutor() = default;

  /**
   * Value constructor. Executes `fn` when this instance is
   * destructed.
   *
   * @param fn The function to execute on destruction.
   */
  explicit ScopedExecutor(std::function<void()>&& fn)
      : fn_(std::move(fn)) {
  }

  /** Move constructor. */
  ScopedExecutor(ScopedExecutor&& rhs) {
    fn_.swap(rhs.fn_);
  }

  /** Destructor. Executes `fn_`. */
  ~ScopedExecutor() {
    if (fn_) {
      fn_();
    }
  }

  DISABLE_COPY_AND_COPY_ASSIGN(ScopedExecutor);
  DISABLE_MOVE_ASSIGN(ScopedExecutor);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The wrapped function to execute on destruction. */
  std::function<void()> fn_;
};

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_SCOPED_EXECUTOR_H
