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

#include <thread>

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

void Stats::reset() {
  // TODO
}

void Stats::dump(FILE* out) const {
  // TODO
  (void)out;
}

void Stats::dump(std::string* out) const {
  // TODO
  (void)out;
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
