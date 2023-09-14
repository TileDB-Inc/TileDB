/**
 * @file parallel_reader.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares the ParallelReader class.
 */

template<class Wrapped>
class ParallelReader {
 public:
  ParallelReader(ContextResources& resources)
    : Wrapped(resources)
    , min_parallel_size_(resources.config().get<uint64_t>("vfs.min_parallel_size").value()) {
  }

  void read(
      const URI& uri,
      const uint64_t offset,
      void* const buffer,
      const uint64_t nbytes) {
    resources_.stats().add_counter("parallel_read_byte_num", nbytes);

    // Calculate the number of tasks we're willing to run at once.
    uint64_t max_tasks = get_max_tasks(uri);
    uint64_t num_tasks = 1;

    // Only create more than one task if we have more than
    // min_parallel_size_ bytes to read.
    if (nbytes > min_parallel_size_) {
      num_tasks = nbytes / min_parallel_size_;
    }

    if (num_tasks > max_tasks) {
      num_tasks = max_tasks;
    }

    // If we only have one task, just execute it and be done with it.
    if (num_tasks == 1) {
      Wrapped::read(uri, offset, buffer, nbytes);
      return;
    }

    // The number of bytes to read per thread. The last task created may read
    // fewer bytes due to rounding.
    uint64_t nbytes_per_thread = utils::math::ceil(nbytes, num_tasks);

    // Create our read tasks.
    std::vector<ThreadPool::Task> results;
    for (uint64_t i = 0; i < num_tasks; i++) {
      // Calculate our offset and number of bytes to read.
      uint64_t thread_offset = offset + (i * nbytes_per_thread);
      uint64_t thread_nbytes = nbytes_per_thread;

      // Make sure the last task doesn't attempt to read more bytes than
      // were requested.
      if ((thread_offset + thread_nbytes) > nbytes) {
        thread_nbytes = nbytes - thread_offset;
      }

      // Don't queue tasks that have nothing to read. This could theoretically
      // happen with some pathological configuration options.
      if (thread_nbytes <= 0) {
        continue;
      }

      auto thread_buffer = reinterpret_cast<char*>(buffer) + begin;
      auto task = cancelable_tasks_.execute(io_tp_, [&]() {
        return Wrapped::read(uri, thread_offset, thread_buffer, thread_nbytes);
      });
      results.push_back(std::move(task));
    }

    auto st = io_tp_->wait_all(results);
    if (!st.ok()) {
      std::stringstream errmsg;
      errmsg << ;
      throw ParallelReadFSException("Error performing parallel read on '" + uri.to_string()
      +  "'; " + st.message())
    }
  }

 protected:
  /** The minimum number of bytes in a parallel operation. */
  uint64_t min_parallel_size_;

  /** Wrapper for tracking and canceling certain tasks on 'thread_pool' */
  CancelableTasks cancelable_tasks_;

  uint64_t get_max_tasks(const URI& uri) const {
    std::string key;

    if (uri.is_azure()) {
      key = "vfs.azure.max_parallel_ops";
    } else if (uri.is_file()) {
      key = "vfs.file.max_parallel_ops";
    } else if (uri.is_gcs()) {
      key = "vfs.gcs.max_parallel_ops";
    } else if (uri.is_s3()) {
      key = "vfs.s3.max_parallel_ops";
    } else {
      return 1;
    }

    return resources_.config().get<uint64_t>(key).value();
  }
};
