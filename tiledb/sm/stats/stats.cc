/**
 * @file   stats.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 TileDB, Inc.
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

#include <cassert>
#include <sstream>

#include "tiledb/sm/stats/stats.h"

namespace tiledb {
namespace sm {
namespace stats {

Stats all_stats;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Stats::Stats() {
  reset();
}

/* ****************************** */
/*              API               */
/* ****************************** */

void Stats::start_timer(StatType stat) {
  assert(timers_.find(stat) != timers_.end());
  assert(time_stats_.find(stat) != time_stats_.end());

  timers_[stat] = std::chrono::high_resolution_clock::now();
}

std::chrono::duration<double> Stats::end_timer(StatType stat) {
  auto it = timers_.find(stat);
  assert(it != timers_.end());
  assert(time_stats_.find(stat) != time_stats_.end());

  auto dur = std::chrono::high_resolution_clock::now() - it->second;
  time_stats_[stat] += dur;
  timers_[stat] = std::chrono::high_resolution_clock::now();
  return dur;
}

void Stats::reset() {
  std::unique_lock<std::mutex> lck(mtx_);

  timers_.clear();
  time_stats_.clear();

  timers_[StatType::COMPUTE_TILE_OVERLAP] =
      std::chrono::high_resolution_clock::now();
  time_stats_[StatType::COMPUTE_TILE_OVERLAP] =
      std::chrono::duration<double>::zero();

  timers_[StatType::COMPUTE_EST_RESULT_SIZE] =
      std::chrono::high_resolution_clock::now();
  time_stats_[StatType::COMPUTE_EST_RESULT_SIZE] =
      std::chrono::duration<double>::zero();

  timers_[StatType::DBG] = std::chrono::high_resolution_clock::now();
  time_stats_[StatType::DBG] = std::chrono::duration<double>::zero();
}

void Stats::dump(FILE* out) const {
  // TODO
  (void)out;
}

void Stats::dump(std::string* out) const {
  std::stringstream ss;
  ss << "==== READS ====\n\n";
  ss << "- Time to compute estimated result size: "
     << secs(StatType::COMPUTE_EST_RESULT_SIZE) << " secs\n";
  ss << "  * Time to compute tile overlap: "
     << secs(StatType::COMPUTE_TILE_OVERLAP) << " secs\n";
  ss << "  * Others: "
     << secs(StatType::COMPUTE_EST_RESULT_SIZE) -
            secs(StatType::COMPUTE_TILE_OVERLAP)
     << " secs\n";

  ss << "\n DEBUG: " << secs(StatType::DBG) << "\n";
  *out = ss.str();
}

double Stats::secs(StatType stat) const {
  auto it = time_stats_.find(stat);
  return it->second.count();
}

/* ****************************** */
/*       PRIVATE FUNCTIONS        */
/* ****************************** */

void Stats::report_ratio(
    FILE* out,
    const char* msg,
    const char* unit,
    uint64_t numerator,
    uint64_t denominator) const {
  fprintf(
      out,
      "%s: %" PRIu64 " / %" PRIu64 " %s",
      msg,
      numerator,
      denominator,
      unit);
  if (denominator > 0) {
    fprintf(out, " (%.1fx)", double(numerator) / double(denominator));
  }
  fprintf(out, "\n");
}

void Stats::report_ratio_pct(
    FILE* out,
    const char* msg,
    const char* unit,
    uint64_t numerator,
    uint64_t denominator) const {
  fprintf(
      out,
      "%s: %" PRIu64 " / %" PRIu64 " %s",
      msg,
      numerator,
      denominator,
      unit);
  if (denominator > 0) {
    fprintf(out, " (%.1f%%)", 100.0 * double(numerator) / double(denominator));
  }
  fprintf(out, "\n");
}

}  // namespace stats
}  // namespace sm
}  // namespace tiledb
