/**
 * @file   scheduler.h
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
 * This file declares scheduler(s) for dag.
 */

#ifndef TILEDB_DAG_SCHEDULER_H
#define TILEDB_DAG_SCHEDULER_H

#include "experimental/tiledb/common/dag/execution/threadpool.h"

namespace tiledb::common {

/**
 * Scheduler for the graph.
 */
template <class Task>
class Scheduler {
  /** Debug flag */
  bool debug_{false};

 public:
  virtual void submit(Task* t) = 0;
  virtual void sync_wait_all() = 0;

  void enable_debug() {
    debug_ = true;
  }

  void disable_debug() {
    debug_ = false;
  }

  bool debug() {
    return debug_;
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_SCHEDULER_H
