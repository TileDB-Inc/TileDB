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
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/misc/utils.h"

#include <algorithm>
#include <cassert>
#include <sstream>
#include <vector>

namespace tiledb {
namespace sm {
namespace stats {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Stats::Stats(const std::string& prefix)
    : enabled_(true)
    , prefix_(prefix + ".")
    , parent_(nullptr) {
}

/* ****************************** */
/*              API               */
/* ****************************** */

bool Stats::enabled() const {
  return enabled_;
}

void Stats::set_enabled(bool enabled) {
  enabled_ = enabled;
}

void Stats::reset() {
  // We will acquire the locks top-down in the tree and hold
  // until the recursion terminates.
  std::unique_lock<std::mutex> lck(mtx_);

  timers_.clear();
  counters_.clear();

  for (auto& child : children_) {
    child.reset();
  }
}

std::string Stats::dump(
    const uint64_t indent_size, const uint64_t num_indents) const {
  std::unordered_map<std::string, double> flattened_timers;
  std::unordered_map<std::string, uint64_t> flattened_counters;

  // Recursively populate the flattened stats with the stats from
  // this instance and all of its children.
  populate_flattened_stats(&flattened_timers, &flattened_counters);

  // Return an empty string if there are no stats.
  if (flattened_timers.empty() && flattened_counters.empty()) {
    return "";
  }

  // We store timers and counters on an `unordered_map` for quicker
  // access times. However, we want to sort the keys before dumping
  // them. Copy the elements into `vector` objects and sort them.
  std::vector<std::pair<std::string, double>> sorted_timers(
      flattened_timers.begin(), flattened_timers.end());
  std::vector<std::pair<std::string, uint64_t>> sorted_counters(
      flattened_counters.begin(), flattened_counters.end());
  std::sort(
      sorted_timers.begin(),
      sorted_timers.end(),
      [](const std::pair<std::string, double>& a,
         const std::pair<std::string, double>& b) -> bool {
        return a.first > b.first;
      });
  std::sort(
      sorted_counters.begin(),
      sorted_counters.end(),
      [](const std::pair<std::string, uint64_t>& a,
         const std::pair<std::string, uint64_t>& b) -> bool {
        return a.first > b.first;
      });

  // Build the indentation literal and the leading indentation literal.
  const std::string indent(indent_size, ' ');
  const std::string l_indent(indent_size * num_indents, ' ');

  std::stringstream ss;
  ss << l_indent << "{\n";

  ss << l_indent << indent << "\"timers\": {\n";
  bool printed_first_timer = false;
  for (const auto& timer : sorted_timers) {
    if (utils::parse::ends_with(timer.first, ".sum")) {
      if (printed_first_timer) {
        ss << ",\n";
      }
      ss << l_indent << indent << indent << "\"" << timer.first
         << "\": " << timer.second << ",\n";
      auto stat = timer.first.substr(
          0, timer.first.size() - std::string(".sum").size());
      auto it = flattened_counters.find(stat + ".timer_count");
      assert(it != flattened_counters.end());
      auto avg = timer.second / it->second;
      ss << l_indent << indent << indent << "\"" << stat + ".avg"
         << "\": " << avg;
      printed_first_timer = true;
    }
  }
  if (printed_first_timer) {
    ss << "\n";
  }

  ss << l_indent << indent << "},\n";
  ss << l_indent << indent << "\"counters\": {\n";
  bool printed_first_counter = false;
  for (const auto& counter : sorted_counters) {
    // Ignore the reserved "timer_count" counters.
    if (utils::parse::ends_with(counter.first, ".timer_count")) {
      continue;
    }

    if (printed_first_counter) {
      ss << ",\n";
    }
    ss << l_indent << indent << indent << "\"" << counter.first
       << "\": " << counter.second;
    printed_first_counter = true;
  }
  if (printed_first_counter) {
    ss << "\n";
  }
  ss << l_indent << indent << "}\n";

  ss << l_indent << "}";

  return ss.str();
}

#ifdef TILEDB_STATS

void Stats::add_counter(const std::string& stat, uint64_t count) {
  if (!enabled_)
    return;

  std::string new_stat = prefix_ + stat;
  std::unique_lock<std::mutex> lck(mtx_);
  auto it = counters_.find(new_stat);
  if (it == counters_.end()) {  // Counter not found
    counters_[new_stat] = count;
  } else {  // Counter found
    it->second += count;
  }
}

ScopedExecutor Stats::start_timer(const std::string& stat) {
  if (!enabled_)
    return ScopedExecutor();

  std::string new_stat = prefix_ + stat;
  std::unique_lock<std::mutex> lck(mtx_);
  const std::thread::id tid = std::this_thread::get_id();
  start_times_[new_stat][tid] = std::chrono::high_resolution_clock::now();

  std::function<void()> end_timer_fn = std::bind(&Stats::end_timer, this, stat);
  return ScopedExecutor(std::move(end_timer_fn));
}

void Stats::end_timer(const std::string& stat) {
  if (!enabled_)
    return;

  std::string new_stat = prefix_ + stat;
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

  // Increment the timer counter
  auto it4 = counters_.find(new_stat + ".timer_count");
  if (it4 == counters_.end()) {  // Timer not found
    counters_[new_stat + ".timer_count"] = 1;
  } else {  // Timer found
    it4->second += 1;
  }
}

#else

void Stats::add_counter(const std::string& stat, uint64_t count) {
  (void)stat;
  (void)count;
}
ScopedExecutor Stats::start_timer(const std::string& stat) {
  (void)stat;
  return ScopedExecutor();
}

void Stats::end_timer(const std::string& stat) {
  (void)stat;
}

#endif

Stats* Stats::parent() {
  return parent_;
}

Stats* Stats::create_child(const std::string& prefix) {
  std::unique_lock<std::mutex> lck(mtx_);
  children_.emplace_back(prefix_ + prefix);
  Stats* const child = &children_.back();
  child->parent_ = this;
  return child;
}

void Stats::populate_flattened_stats(
    std::unordered_map<std::string, double>* const flattened_timers,
    std::unordered_map<std::string, uint64_t>* const flattened_counters) const {
  // We will acquire the locks top-down in the tree and hold
  // until the recursion terminates.
  std::unique_lock<std::mutex> lck(mtx_);

  // Append the stats from this instance.
  for (const auto& timer : timers_)
    (*flattened_timers)[timer.first] += timer.second;
  for (const auto& counter : counters_)
    (*flattened_counters)[counter.first] += counter.second;

  // Populate the stats from all of the children.
  for (const auto& child : children_) {
    child.populate_flattened_stats(flattened_timers, flattened_counters);
  }
}

std::unordered_map<std::string, double>* Stats::timers() {
  return &timers_;
}

/** Return pointer to conters map, used for serialization only. */
std::unordered_map<std::string, uint64_t>* Stats::counters() {
  return &counters_;
}

}  // namespace stats
}  // namespace sm
}  // namespace tiledb
