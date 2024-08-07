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

#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/rest/rest_client.h"

using namespace tiledb::common;

namespace tiledb::sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ContextResources::ContextResources(
    const Config& config,
    shared_ptr<Logger> logger,
    size_t compute_thread_count,
    size_t io_thread_count,
    std::string stats_name)
    : memory_tracker_manager_(make_shared<MemoryTrackerManager>(HERE()))
    , ephemeral_memory_tracker_(memory_tracker_manager_->create_tracker())
    , serialization_memory_tracker_(memory_tracker_manager_->create_tracker())
    , memory_tracker_reporter_(make_shared<MemoryTrackerReporter>(
          HERE(), config, memory_tracker_manager_))
    , config_(config)
    , logger_(logger)
    , compute_tp_(compute_thread_count)
    , io_tp_(io_thread_count)
    , stats_(make_shared<stats::Stats>(HERE(), stats_name))
    , vfs_(stats_.get(), &compute_tp_, &io_tp_, config)
    , rest_client_{RestClientFactory::make(
          *(stats_.get()),
          config_,
          compute_tp(),
          *logger_.get(),
          create_memory_tracker())} {
  ephemeral_memory_tracker_->set_type(MemoryTrackerType::EPHEMERAL);
  serialization_memory_tracker_->set_type(MemoryTrackerType::SERIALIZATION);

  /*
   * Explicitly register our `stats` object with the global.
   */
  stats::all_stats.register_stats(stats_);

  if (!logger_) {
    throw std::logic_error("Logger must not be nullptr");
  }

  memory_tracker_reporter_->start();
}

shared_ptr<MemoryTracker> ContextResources::create_memory_tracker() const {
  return memory_tracker_manager_->create_tracker();
}

shared_ptr<MemoryTracker> ContextResources::ephemeral_memory_tracker() const {
  return ephemeral_memory_tracker_;
}

shared_ptr<MemoryTracker> ContextResources::serialization_memory_tracker()
    const {
  return serialization_memory_tracker_;
}

}  // namespace tiledb::sm
