/**
 * @file context_resources.cc
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
 * This file implements class ContextResources.
 */

#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/storage_manager/context_resources.h"

using namespace tiledb::common;

namespace tiledb::sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

// PJD: "StorageManager" is not a typo here. I don't want to suddenly
// change naming conventions in the stats hierarchy. Though I am
// 100% making the assumption that this label is visible to users.
ContextResources::ContextResources(
      size_t compute_thread_count,
      size_t io_thread_count,
      stats::Stats* stats)
    : compute_tp_(compute_thread_count)
    , io_tp_(io_thread_count)
    , stats_(stats->create_child("StorageManager")) {
}

/* ****************************** */
/*                API             */
/* ****************************** */

ThreadPool* ContextResources::compute_tp() const {
  return &compute_tp_;
}

ThreadPool* ContextResources::io_tp() const {
  return &io_tp_;
}

stats::Stats* ContextResources::stats() const {
  return stats_;
}

}  // namespace tiledb::sm
