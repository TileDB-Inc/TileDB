/**
 * @file   observable.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines base class Observable that centralizes observability tools
 * (logging, metrics, tracing etc) for core classes.
 */

#ifndef TILEDB_OBSERVABLE_H
#define TILEDB_OBSERVABLE_H

#include <atomic>
#include "logger_distinct.h"

namespace tiledb {

namespace common {

/**
 * To be extended by classes that want to use tools that enhance
 * debuggability and visibility during runtime.
 */
template <typename T>
class Observable {
 public:
  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /** Constructor. */
  Observable(const std::string& name)
      : name_(name)
      , log_(tdb::make_shared<LoggerDistinct<T>>(HERE(), name)) {
  }

  /** Constructor. */
  Observable(const std::string& name, const shared_ptr<Logger>& parent_logger)
      : name_(name)
      , log_(tdb::make_shared<LoggerDistinct<T>>(HERE(), name, parent_logger)) {
  }

  /** Constructor. */
  Observable(const shared_ptr<Logger>& log)
      : log_(log) {
  }

  /** Destructor. */
  ~Observable() = default;

  /** Copy constructor. */
  DISABLE_COPY(Observable);

  /** Move constructor. */
  DISABLE_MOVE(Observable);

  /* ****************************** */
  /*           OPERATORS            */
  /* ****************************** */

  /** Copy-assignment. */
  DISABLE_COPY_ASSIGN(Observable);

  /** Move-assignment. */
  DISABLE_MOVE_ASSIGN(Observable);

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /** Returns the internal logger object. */
  shared_ptr<Logger> logger() const {
    return log_;
  }

 protected:
  /* ********************************* */
  /*        PROTECTED ATTRIBUTES       */
  /* ********************************* */

  /** The name to use for observing this class */
  const std::string name_;

  /** The class logger. */
  shared_ptr<Logger> log_;
};

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_OBSERVABLE_H