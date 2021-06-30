/**
 * @file   iquery_strategy.h
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
 * This file defines class IQueryStrategy.
 */

#ifndef TILEDB_IQUERY_STRATEGY_H
#define TILEDB_IQUERY_STRATEGY_H

#include "tiledb/common/status.h"
#include "tiledb/sm/enums/layout.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class IQueryStrategy {
 public:
  /** Destructor. */
  virtual ~IQueryStrategy() = default;

  /** Initializes the strategy. */
  virtual Status init() = 0;

  /** Performs a query using its set members. */
  virtual Status dowork() = 0;

  /** Finalizes the strategy. */
  virtual Status finalize() = 0;

  /** Returns of the query is incomplete. */
  virtual bool incomplete() const = 0;

  /** Resets the object */
  virtual void reset() = 0;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_IQUERY_STRATEGY_H
