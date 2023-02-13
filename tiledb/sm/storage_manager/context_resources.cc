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
#include "tiledb/sm/rest/rest_client.h"

using namespace tiledb::common;

namespace tiledb::sm {

#ifdef TILEDB_SERIALIZATION
constexpr bool TILEDB_SERIALIZATION_ENABLED = true;
#else
constexpr bool TILEDB_SERIALIZATION_ENABLED = false;
#endif

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ContextResources::ContextResources(
    const Config& config,
    shared_ptr<Logger> logger,
    size_t compute_thread_count,
    size_t io_thread_count,
    std::string stats_name)
    : config_(config)
    , logger_(logger)
    , compute_tp_(compute_thread_count)
    , io_tp_(io_thread_count)
    , stats_(make_shared<stats::Stats>(HERE(), stats_name))
    , vfs_(stats_.get(), &compute_tp_, &io_tp_, config) {
  /*
   * Explicitly register our `stats` object with the global.
   */
  stats::all_stats.register_stats(stats_);

  if (!logger_) {
    throw std::logic_error("Logger must not be nullptr");
  }

  if constexpr (TILEDB_SERIALIZATION_ENABLED) {
    auto server_address = config_.get<std::string>("rest.server_address");
    if (server_address) {
      auto client = tdb::make_shared<RestClient>(HERE());
      auto st = client->init(&stats(), &config_, &compute_tp(), logger_);
      throw_if_not_ok(st);
      rest_client_ = client;
    }
  }
}

}  // namespace tiledb::sm
