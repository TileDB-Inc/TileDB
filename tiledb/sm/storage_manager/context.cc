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

#include "tiledb/common/common.h"

#include "tiledb/common/logger.h"
#include "tiledb/sm/storage_manager/context.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

// Constructor.  Note order of construction:  storage_manager depends on the
// preceding members to be initialized for its initialization.
Context::Context(const Config& config)
    : last_error_(nullopt)
    , logger_(make_shared<Logger>(
          HERE(), "Context: " + std::to_string(++logger_id_)))
    , compute_tp_(get_compute_thread_count(config))
    , io_tp_(get_io_thread_count(config))
    , stats_(make_shared<stats::Stats>(HERE(), "Context"))
    , storage_manager_{} {
  if (!init(config).ok()) {
    logger_->status(
        Status_ContextError("Failed to initialize Context in constructor."));
    throw std::logic_error("Could not initialize Context in constructor");
  }
}

/* ****************************** */
/*                API             */
/* ****************************** */

optional<std::string> Context::last_error() {
  std::lock_guard<std::mutex> lock(mtx_);
  return last_error_;
}

void Context::save_error(const Status& st) {
  std::lock_guard<std::mutex> lock(mtx_);
  last_error_ = st.to_string();
}

void Context::save_error(const StatusException& st) {
  std::lock_guard<std::mutex> lock(mtx_);
  last_error_ = st.what();
}

// Return pointer to underlying storage manager.
StorageManager* Context::storage_manager() const {
  return &(*storage_manager_);
}

ThreadPool* Context::compute_tp() const {
  return &compute_tp_;
}

ThreadPool* Context::io_tp() const {
  return &io_tp_;
}

stats::Stats* Context::stats() const {
  return stats_.get();
}

Status Context::init(const Config& config) {
  RETURN_NOT_OK(init_loggers(config));

  // Register stats.
  stats::all_stats.register_stats(stats_);

  // Create storage manager
  storage_manager_ = tdb_unique_ptr<tiledb::sm::StorageManager>{
      new (std::nothrow) tiledb::sm::StorageManager(
          &compute_tp_, &io_tp_, stats_.get(), logger_)};
  if (storage_manager_ == nullptr)
    return logger_->status(Status_ContextError(
        "Cannot initialize context Storage manager allocation failed"));

  // Initialize storage manager
  auto sm = storage_manager_->init(config);

  return sm;
}

Status Context::get_config_thread_count(
    const Config& config, uint64_t& config_thread_count_ret) {
  // The "sm.num_async_threads", "sm.num_reader_threads",
  // "sm.num_tbb_threads", "sm.num_writer_threads", and "sm.num_vfs_threads"
  // have been removed. If they are set, we will log an error message.
  // To error on the side of maintaining high-performance for
  // existing users, we will take the maximum thread count
  // among all of these configurations and set them to the new
  // "sm.compute_concurrency_level" and "sm.io_concurrency_level".
  uint64_t config_thread_count{0};

  bool found{false};
  uint64_t num_async_threads{0};
  RETURN_NOT_OK(
      config.get<uint64_t>("sm.num_async_threads", &num_async_threads, &found));
  if (found) {
    config_thread_count = std::max(config_thread_count, num_async_threads);
    logger_->status(Status_StorageManagerError(
        "Config parameter \"sm.num_async_threads\" has been removed; use "
        "config parameter \"sm.compute_concurrency_level\"."));
  }

  uint64_t num_reader_threads{0};
  RETURN_NOT_OK(config.get<uint64_t>(
      "sm.num_reader_threads", &num_reader_threads, &found));
  if (found) {
    config_thread_count = std::max(config_thread_count, num_reader_threads);
    logger_->status(Status_StorageManagerError(
        "Config parameter \"sm.num_reader_threads\" has been removed; use "
        "config parameter \"sm.compute_concurrency_level\"."));
  }

  uint64_t num_writer_threads{0};
  RETURN_NOT_OK(config.get<uint64_t>(
      "sm.num_writer_threads", &num_writer_threads, &found));
  if (found) {
    config_thread_count = std::max(config_thread_count, num_writer_threads);
    logger_->status(Status_StorageManagerError(
        "Config parameter \"sm.num_writer_threads\" has been removed; use "
        "config parameter \"sm.compute_concurrency_level\"."));
  }

  uint64_t num_vfs_threads{0};
  RETURN_NOT_OK(
      config.get<uint64_t>("sm.num_vfs_threads", &num_vfs_threads, &found));
  if (found) {
    config_thread_count = std::max(config_thread_count, num_vfs_threads);
    logger_->status(Status_StorageManagerError(
        "Config parameter \"sm.num_vfs_threads\" has been removed; use "
        "config parameter \"sm.io_concurrency_level\"."));
  }

  // The "sm.num_tbb_threads" has been deprecated. Users may still be setting
  // this configuration parameter. In this scenario, we will override the
  // compute and io concurrency levels if the configured tbb threads are
  // greater.
  int num_tbb_threads{0};
  RETURN_NOT_OK(
      config.get<int>("sm.num_tbb_threads", &num_tbb_threads, &found));
  if (found) {
    config_thread_count =
        std::max(config_thread_count, static_cast<uint64_t>(num_tbb_threads));
    logger_->status(Status_StorageManagerError(
        "Config parameter \"sm.num_tbb_threads\" has been removed; use "
        "config parameter \"sm.io_concurrency_level\"."));
  }

  config_thread_count_ret = static_cast<size_t>(config_thread_count);

  return Status::Ok();
}

size_t Context::get_compute_thread_count(const Config& config) {
  uint64_t config_thread_count{0};
  if (!get_config_thread_count(config, config_thread_count).ok()) {
    throw std::logic_error("Cannot get compute thread count");
  }

  bool found{false};
  uint64_t compute_concurrency_level{0};
  if (!config
           .get<uint64_t>(
               "sm.compute_concurrency_level",
               &compute_concurrency_level,
               &found)
           .ok()) {
    throw std::logic_error("Cannot get compute concurrency level");
  }
  assert(found);

  return static_cast<size_t>(
      std::max(config_thread_count, compute_concurrency_level));
}

size_t Context::get_io_thread_count(const Config& config) {
  uint64_t config_thread_count{0};
  if (!get_config_thread_count(config, config_thread_count).ok()) {
    throw std::logic_error("Cannot get config thread count");
  }

  bool found = false;
  uint64_t io_concurrency_level{0};
  if (!config
           .get<uint64_t>(
               "sm.io_concurrency_level", &io_concurrency_level, &found)
           .ok()) {
    throw std::logic_error("Cannot get io concurrency level");
  }
  assert(found);

  io_concurrency_level = std::max(config_thread_count, io_concurrency_level);

  return static_cast<size_t>(
      std::max(config_thread_count, io_concurrency_level));
}

Status Context::init_loggers(const Config& config) {
  // temporarily set level to error so that possible errors reading
  // configuration are visible to the user
  logger_->set_level(Logger::Level::ERR);

  const char* format_conf;
  RETURN_NOT_OK(config.get("config.logging_format", &format_conf));
  assert(format_conf != nullptr);
  Logger::Format format = Logger::Format::DEFAULT;
  RETURN_NOT_OK(logger_format_from_string(format_conf, &format));

  global_logger(format);
  logger_->set_format(static_cast<Logger::Format>(format));

  // set logging level from config
  bool found = false;
  uint32_t level = static_cast<unsigned int>(Logger::Level::ERR);
  RETURN_NOT_OK(config.get<uint32_t>("config.logging_level", &level, &found));
  assert(found);
  if (level > static_cast<unsigned int>(Logger::Level::TRACE)) {
    return logger_->status(Status_StorageManagerError(
        "Cannot set logger level; Unsupported level:" + std::to_string(level) +
        "set in configuration"));
  }

  global_logger().set_level(static_cast<Logger::Level>(level));
  logger_->set_level(static_cast<Logger::Level>(level));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
