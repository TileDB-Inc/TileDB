/**
 * @file   context.cc
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
 * This file implements class Context.
 */

#include "tiledb/sm/storage_manager/context.h"
#include "tiledb/common/logger.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Context::Context()
    : last_error_(Status::Ok())
    , storage_manager_(nullptr) {
}

Context::~Context() {
  // Delete the `storage_manager_` to ensure it is destructed
  // before the `compute_tp_` and `io_tp_` objects that it
  // references.
  delete storage_manager_;
}

/* ****************************** */
/*                API             */
/* ****************************** */

Status Context::init(Config* const config) {
  if (storage_manager_ != nullptr)
    return LOG_STATUS(Status::ContextError(
        "Cannot initialize context; Context already initialized"));

  // Initialize `compute_tp_` and `io_tp_`.
  RETURN_NOT_OK(init_thread_pools(config));

  // Create storage manager
  storage_manager_ =
      new (std::nothrow) tiledb::sm::StorageManager(&compute_tp_, &io_tp_);
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::ContextError(
        "Cannot initialize contextl Storage manager allocation failed"));

  // Initialize storage manager
  return storage_manager_->init(config);
}

Status Context::last_error() {
  std::lock_guard<std::mutex> lock(mtx_);
  return last_error_;
}

void Context::save_error(const Status& st) {
  std::lock_guard<std::mutex> lock(mtx_);
  last_error_ = st;
}

StorageManager* Context::storage_manager() const {
  return storage_manager_;
}

ThreadPool* Context::compute_tp() const {
  return &compute_tp_;
}

ThreadPool* Context::io_tp() const {
  return &io_tp_;
}

Status Context::init_thread_pools(Config* const config) {
  // If `config` is null, use a default-constructed config.
  Config tmp_config;
  if (config != nullptr)
    tmp_config = *config;

  // The "sm.num_async_threads", "sm.num_reader_threads",
  // "sm.num_writer_threads", and "sm.num_vfs_threads" have
  // been removed. If they are set, we will log an error message.
  // To error on the side of maintaining high-performance for
  // existing users, we will take the maximum thread count
  // among all of these configurations and set them to the new
  // "sm.compute_concurrency_level" and "sm.io_concurrency_level".
  uint64_t max_thread_count = 0;

  bool found;
  uint64_t num_async_threads = 0;
  RETURN_NOT_OK(tmp_config.get<uint64_t>(
      "sm.num_async_threads", &num_async_threads, &found));
  if (found) {
    max_thread_count = std::max(max_thread_count, num_async_threads);
    LOG_STATUS(Status::StorageManagerError(
        "Config parameter \"sm.num_async_threads\" has been removed; use "
        "config parameter \"sm.compute_concurrency_level\"."));
  }

  uint64_t num_reader_threads = 0;
  RETURN_NOT_OK(tmp_config.get<uint64_t>(
      "sm.num_reader_threads", &num_reader_threads, &found));
  if (found) {
    max_thread_count = std::max(max_thread_count, num_reader_threads);
    LOG_STATUS(Status::StorageManagerError(
        "Config parameter \"sm.num_reader_threads\" has been removed; use "
        "config parameter \"sm.compute_concurrency_level\"."));
  }

  uint64_t num_writer_threads = 0;
  RETURN_NOT_OK(tmp_config.get<uint64_t>(
      "sm.num_writer_threads", &num_writer_threads, &found));
  if (found) {
    max_thread_count = std::max(max_thread_count, num_writer_threads);
    LOG_STATUS(Status::StorageManagerError(
        "Config parameter \"sm.num_writer_threads\" has been removed; use "
        "config parameter \"sm.compute_concurrency_level\"."));
  }

  uint64_t num_vfs_threads = 0;
  RETURN_NOT_OK(
      tmp_config.get<uint64_t>("sm.num_vfs_threads", &num_vfs_threads, &found));
  if (found) {
    max_thread_count = std::max(max_thread_count, num_vfs_threads);
    LOG_STATUS(Status::StorageManagerError(
        "Config parameter \"sm.num_vfs_threads\" has been removed; use "
        "config parameter \"sm.io_concurrency_level\"."));
  }

  // Fetch the compute and io concurrency levels.
  uint64_t compute_concurrency_level = 0;
  RETURN_NOT_OK(tmp_config.get<uint64_t>(
      "sm.compute_concurrency_level", &compute_concurrency_level, &found));
  assert(found);
  uint64_t io_concurrency_level = 0;
  RETURN_NOT_OK(tmp_config.get<uint64_t>(
      "sm.io_concurrency_level", &io_concurrency_level, &found));
  assert(found);

#ifndef HAVE_TBB
  // The "sm.num_tbb_threads" has been deprecated when TBB is disabled.
  // Now that TBB is disabled by default, users may still be setting this
  // configuration parameter larger than the default value. In this scenario,
  // we will override the compute and io concurrency levels if the configured
  // tbb threads are greater. We will do this silently because the
  // "sm.num_tbb_threads" still has a default value in the config. When we
  // remove TBB, we will also remove this configuration parameter.
  int num_tbb_threads = 0;
  RETURN_NOT_OK(
      tmp_config.get<int>("sm.num_tbb_threads", &num_tbb_threads, &found));
  assert(found);
  if (num_tbb_threads > 0)
    max_thread_count =
        std::max(max_thread_count, static_cast<uint64_t>(num_tbb_threads));
#endif

  // If 'max_thread_count' is larger than the compute or io concurrency levels,
  // use that instead.
  compute_concurrency_level =
      std::max(max_thread_count, compute_concurrency_level);
  io_concurrency_level = std::max(max_thread_count, io_concurrency_level);

  // Initialize the thread pools.
  RETURN_NOT_OK(compute_tp_.init(compute_concurrency_level));
  RETURN_NOT_OK(io_tp_.init(io_concurrency_level));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
