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

ParallelMergeFuture::~ParallelMergeFuture() {
  // make sure all threads are finished because they reference
  // data which is contractually expected to outlive `this`,
  // but makes no guarantee after that
  while (true) {
    const auto m = merge_cursor_;
    try {
      if (!await().has_value()) {
        break;
      }
    } catch (...) {
      // Swallow it. Yes, really.
      // 1) exceptions cannot propagate out of destructors.
      // 2) the tasks only throw on internal error, i.e. we wrote bad code.
      // 3) the user did not wait for all the tasks to finish, so we had better
      //    do so else we risk undefined behavior.
      // 4) the user did not wait for all the tasks to finish, ergo they don't
      //    care about the result, ergo they don't care if there is an error
      //    here.  Most likely we are tearing down the stack to propagate
      //    a different exception anyway, we don't want to mask that up
      //    by signaling.
      //
      // If this is happening *not* due to an exception, then either:
      // 1) the developer is responsible for blocking prior to destruction
      //    so as to correctly handle any errors
      // 2) the developer is responsible for not caring about any
      //    results beyond what was previously consumed
    }

    // however we definitely do want to avoid an infinite loop here,
    // so we had better have made progress.
    assert(merge_cursor_ > m);
  }
}

bool ParallelMergeFuture::finished() const {
  return merge_cursor_ == merge_bounds_.size();
}

std::optional<uint64_t> ParallelMergeFuture::valid_output_bound() const {
  if (merge_cursor_ > 0) {
    return merge_bounds_[merge_cursor_ - 1].output_end();
  } else {
    return std::nullopt;
  }
}

std::optional<uint64_t> ParallelMergeFuture::await() {
  auto maybe_task = merge_tasks_.pop();
  if (maybe_task.has_value()) {
    const auto m = merge_cursor_++;
    throw_if_not_ok(maybe_task->wait());
    return merge_bounds_[m].output_end();
  } else {
    return std::nullopt;
  }
}

void ParallelMergeFuture::block() {
  while (await().has_value())
    ;
}

}  // namespace tiledb::algorithm
