/**
 * @file   global_stats.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file contains definitions of statistics-related code.
 */

#include "tiledb/sm/stats/global_stats.h"

#include <cassert>
#include <sstream>

namespace tiledb {
namespace sm {
namespace stats {

GlobalStats all_stats;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

GlobalStats::GlobalStats()
    : enabled_(false) {
}

/* ****************************** */
/*              API               */
/* ****************************** */

bool GlobalStats::enabled() const {
  return enabled_;
}

void GlobalStats::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  std::string output;
  GlobalStats::dump(&output);
  fprintf(out, "%s", output.c_str());
}

void GlobalStats::dump(std::string* out) const {
  *out = dump_registered_stats();
}

void GlobalStats::raw_dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  std::string output;
  GlobalStats::raw_dump(&output);
  fprintf(out, "%s", output.c_str());
}

void GlobalStats::raw_dump(std::string* out) const {
  *out = dump_registered_stats();
}

void GlobalStats::set_enabled(bool enabled) {
  enabled_ = enabled;
}

void GlobalStats::register_stats(const tdb_shared_ptr<Stats>& stats) {
  registered_stats_.emplace_back(stats);
}

/* ****************************** */
/*       PRIVATE FUNCTIONS        */
/* ****************************** */

std::string GlobalStats::dump_registered_stats() const {
  std::stringstream ss;

  for (const auto& stats : registered_stats_) {
    const std::string stats_dump = stats->dump();
    if (!stats_dump.empty())
      ss << stats_dump;
  }

  return ss.str();
}

}  // namespace stats
}  // namespace sm
}  // namespace tiledb
