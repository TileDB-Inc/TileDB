/**
 * @file   stats.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file defines class Stats.
 */

#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/misc/utils.h"

#include <cassert>
#include <sstream>

namespace tiledb {
namespace sm {
namespace stats {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Stats::Stats() {
  enabled_ = true;
}

Stats::Stats(const std::string& prefix)
    : Stats() {
  prefix_ = prefix;
}

Stats::Stats(const Stats& stats)
    : Stats() {
  auto clone = stats.clone();
  swap(clone);
}

Stats::Stats(Stats&& stats)
    : Stats() {
  swap(stats);
}

Stats& Stats::operator=(const Stats& stats) {
  auto clone = stats.clone();
  swap(clone);

  return *this;
}

Stats& Stats::operator=(Stats&& stats) {
  swap(stats);

  return *this;
}

/* ****************************** */
/*              API               */
/* ****************************** */

bool Stats::enabled() const {
  return enabled_;
}

void Stats::reset() {
  timers_.clear();
  counters_.clear();
  start_times_.clear();
}

void Stats::set_enabled(bool enabled) {
  enabled_ = enabled;
}

std::string Stats::dump() const {
  std::stringstream ss;
  ss << "--- Timers ---\n\n";
  for (const auto& timer : timers_) {
    ss << timer.first << ": " << timer.second << "\n";
    if (utils::parse::ends_with(timer.first, ".sum")) {
      auto stat = timer.first.substr(
          0, timer.first.size() - std::string(".sec.sum").size());
      auto it = counters_.find(stat + ".count");
      assert(it != counters_.end());
      auto avg = timer.second / it->second;
      ss << stat + ".sec.avg"
         << ": " << avg << "\n";
    }
  }
  ss << "\n";
  ss << "--- Counters ---\n\n";
  for (const auto& counter : counters_)
    ss << counter.first << ": " << counter.second << "\n";
  ss << "\n";

  return ss.str();
}

#ifdef TILEDB_STATS

void Stats::add_counter(const std::string& stat, uint64_t count) {
  if (!enabled_)
    return;

  std::string new_stat = prefix_ + "." + stat;
  std::unique_lock<std::mutex> lck(mtx_);
  auto it = counters_.find(new_stat);
  if (it == counters_.end()) {  // Counter not found
    counters_[new_stat] = count;
  } else {  // Counter found
    it->second += count;
  }
}

void Stats::start_timer(const std::string& stat) {
  if (!enabled_)
    return;

  std::string new_stat = prefix_ + "." + stat;
  std::unique_lock<std::mutex> lck(mtx_);
  const std::thread::id tid = std::this_thread::get_id();
  start_times_[new_stat][tid] = std::chrono::high_resolution_clock::now();
}

void Stats::end_timer(const std::string& stat) {
  if (!enabled_)
    return;

  std::string new_stat = prefix_ + "." + stat;
  std::unique_lock<std::mutex> lck(mtx_);

  // Calculate duration
  const std::thread::id tid = std::this_thread::get_id();
  auto it = start_times_.find(new_stat);
  assert(it != start_times_.end());
  auto tit = it->second.find(tid);
  assert(tit != it->second.end());
  auto start_time = tit->second;
  auto end_time = std::chrono::high_resolution_clock::now();
  const std::chrono::duration<double> duration = end_time - start_time;

  // Add duration to timer total
  auto it2 = timers_.find(new_stat + ".sum");
  if (it2 == timers_.end()) {  // Timer not found
    timers_[new_stat + ".sum"] = duration.count();
  } else {  // Timer found
    it2->second += duration.count();
  }

  // Add duration to timer max
  auto it3 = timers_.find(new_stat + ".max");
  if (it3 == timers_.end()) {  // Timer not found
    timers_[new_stat + ".max"] = duration.count();
  } else {  // Timer found
    if (duration.count() > it3->second)
      it3->second = duration.count();
  }
}

#else

void Stats::add_counter(const std::string& stat, uint64_t count) {
  (void)stat;
  (void)count;
}

void Stats::start_timer(const std::string& stat) {
  (void)stat;
}

void Stats::end_timer(const std::string& stat) {
  (void)stat;
}

#endif

void Stats::append(const Stats& stats) {
  for (const auto& timer : stats.timers_)
    timers_[timer.first] = timer.second;
  for (const auto& counter : stats.counters_)
    counters_[counter.first] = counter.second;
}

/* ****************************** */
/*       PRIVATE FUNCTIONS        */
/* ****************************** */

Stats Stats::clone() const {
  Stats clone;
  clone.prefix_ = prefix_;
  clone.enabled_ = enabled_;
  clone.timers_ = timers_;
  clone.counters_ = counters_;
  clone.start_times_ = start_times_;
  return clone;
}

void Stats::swap(Stats& stats) {
  std::swap(prefix_, stats.prefix_);
  std::swap(enabled_, stats.enabled_);
  std::swap(timers_, stats.timers_);
  std::swap(counters_, stats.counters_);
  std::swap(start_times_, stats.start_times_);
}

}  // namespace stats
}  // namespace sm
}  // namespace tiledb
