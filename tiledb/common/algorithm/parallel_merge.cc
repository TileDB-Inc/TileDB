/**
 * @file   parallel_merge.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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
 * This file contains non-template definition used in the parallel merge
 * algorithm.
 */

#include "tiledb/common/algorithm/parallel_merge.h"

using namespace tiledb::common;

namespace tiledb::algorithm {

ParallelMergeFuture::ParallelMergeFuture(
    ParallelMergeMemoryResources& memory, size_t parallel_factor)
    : memory_(memory)
    , merge_bounds_(&memory.control)
    , merge_cursor_(0) {
  merge_bounds_.reserve(parallel_factor);
  for (size_t p = 0; p < parallel_factor; p++) {
    merge_bounds_.push_back(MergeUnit(memory.control));
  }
}

std::optional<uint64_t> ParallelMergeFuture::await() {
  auto maybe_task = merge_tasks_.pop();
  if (maybe_task.has_value()) {
    maybe_task->wait();
    return merge_bounds_[merge_cursor_++].output_end();
  } else {
    return std::nullopt;
  }
}

void ParallelMergeFuture::block() {
  while (await().has_value())
    ;
}

}  // namespace tiledb::algorithm
