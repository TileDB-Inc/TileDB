/**
 * @file query_plan.h
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
 * This file implements serialization functions for Stats.
 */

// clang-format off
#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif
// clang-format on

#include "tiledb/sm/serialization/stats.h"

using namespace tiledb::common;

namespace tiledb::sm::serialization {

Status stats_to_capnp(Stats& stats, capnp::Stats::Builder* stats_builder) {
  // Build counters
  const auto counters = stats.counters();
  if (counters != nullptr && !counters->empty()) {
    auto counters_builder = stats_builder->initCounters();
    auto entries_builder = counters_builder.initEntries(counters->size());
    uint64_t index = 0;
    for (const auto& entry : *counters) {
      entries_builder[index].setKey(entry.first);
      entries_builder[index].setValue(entry.second);
      ++index;
    }
  }

  // Build timers
  const auto timers = stats.timers();
  if (timers != nullptr && !timers->empty()) {
    auto timers_builder = stats_builder->initTimers();
    auto entries_builder = timers_builder.initEntries(timers->size());
    uint64_t index = 0;
    for (const auto& entry : *timers) {
      entries_builder[index].setKey(entry.first);
      entries_builder[index].setValue(entry.second);
      ++index;
    }
  }

  return Status::Ok();
}

Status stats_from_capnp(
    const capnp::Stats::Reader& stats_reader, Stats* stats) {
  if (stats_reader.hasCounters()) {
    auto counters = stats->counters();
    auto counters_reader = stats_reader.getCounters();
    for (const auto entry : counters_reader.getEntries()) {
      auto key = std::string_view{entry.getKey().cStr(), entry.getKey().size()};
      (*counters)[std::string{key}] = entry.getValue();
    }
  }

  if (stats_reader.hasTimers()) {
    auto timers = stats->timers();
    auto timers_reader = stats_reader.getTimers();
    for (const auto entry : timers_reader.getEntries()) {
      auto key = std::string_view{entry.getKey().cStr(), entry.getKey().size()};
      (*timers)[std::string{key}] = entry.getValue();
    }
  }

  return Status::Ok();
}

}  // namespace tiledb::sm::serialization