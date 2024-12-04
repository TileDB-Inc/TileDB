/**
 * @file   parallel_merge.h
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
 * This file defines an implementation of a parallel merge algorithm for
 * in-memory data.
 *
 * TODO: explain algorithm
 */

#ifndef TILEDB_PARALLEL_MERGE_H
#define TILEDB_PARALLEL_MERGE_H

#include "tiledb/common/thread_pool/producer_consumer_queue.h"
#include "tiledb/common/thread_pool/thread_pool.h"

namespace tiledb::algorithm {

struct ParallelMergeOptions {
  uint64_t parallel_factor;
  uint64_t min_merge_items;
};

struct ParallelMergeFuture {
  std::vector<uint64_t> merge_bounds_;
  tiledb::common::ProducerConsumerQueue<tiledb::common::ThreadPool::Task>
      merge_units_;

  /**
   * Wait for more data to finish merging.
   *
   * @return the bound in the output stream up to which the merge has completed
   */
  uint64_t await();

  /**
   * Wait for all data to finish merging.
   */
  void block();
};

std::unique_ptr<ParallelMergeFuture> parallel_merge(
    tiledb::common::ThreadPool& pool,
    const ParallelMergeOptions& options,
    std::span<std::span<uint64_t>> streams,
    uint64_t* output);

}  // namespace tiledb::algorithm

#endif
