/**
 * @file   heap_profiler.cc
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
 * This file contains the implementation of the HeapProfiler class.
 */

#include <chrono>
#include <fstream>
#include <iostream>

#include "tiledb/common/heap_profiler.h"

namespace tiledb::common {

HeapProfiler heap_profiler;

// A callback to dump stats and terminate when the C++
// allocation APIs fail (e.g. ::operator new).
void failed_cpp_alloc_cb() {
  heap_profiler.dump_and_terminate_internal();
}

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

HeapProfiler::HeapProfiler()
    : dump_interval_ms_(0)
    , dump_interval_bytes_(0)
    , reserved_memory_(nullptr)
    , num_allocs_(0)
    , num_deallocs_(0)
    , num_alloc_bytes_(0)
    , num_dealloc_bytes_(0)
    , last_interval_dump_alloc_bytes_(0) {
}

HeapProfiler::~HeapProfiler() {
  if (periodic_dump_thread_ != nullptr) {
    // Set `dump_interval_ms_` to 0 to signal `periodic_dump_thread_`
    // to stop. We intentionally leave `dump_interval_ms_` unprotected
    // because we only mutate it one time (here) after initialization.
    dump_interval_ms_ = 0;
    periodic_dump_thread_->join();
  }
}

/* ****************************** */
/*              API               */
/* ****************************** */

void HeapProfiler::enable(
    const std::string& file_name_prefix,
    const uint64_t dump_interval_ms,
    const uint64_t dump_interval_bytes,
    const uint64_t dump_threshold_bytes) {
  std::unique_lock<std::mutex> ul(mutex_);

  if (enabled())
    return;

  dump_interval_ms_ = dump_interval_ms;
  dump_interval_bytes_ = dump_interval_bytes;
  dump_threshold_bytes_ = dump_threshold_bytes;

  // Reserve 50MiB to free when we encounter an out-of-memory
  // scenario. This attempts to ensure there is enough memory
  // available to dump stats.
  reserved_memory_ = std::malloc(50 * 1024 * 1024);

  if (!file_name_prefix.empty())
    file_name_ = create_dump_file(file_name_prefix);

  if (dump_interval_ms_ > 0)
    start_periodic_dump();

  // Attach a callback for when the C++ allocation APIs fail.
  std::set_new_handler(failed_cpp_alloc_cb);
}

void HeapProfiler::record_alloc(
    void* const p, const size_t size, const std::string& label) {
  std::unique_lock<std::mutex> ul(mutex_);

  try {
    const uint64_t addr = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(p));
    // Until we have replaced all dynamic memory APIs, we will silently
    // ignore mismatched `record_alloc` and `record_dealloc` calls.
    // assert(addr_to_alloc_.count(addr) == 0);
    if (addr_to_alloc_.count(addr) > 0) {
      // std::cerr << "TileDB:: duplicate alloc on " << std::hex << addr
      //          << std::endl;
      return;
    }

    // Increment the total number of allocation operations.
    ++num_allocs_;

    // Record a mapping from `addr` to the bytes allocated and the user-provided
    // string label (if provided). Note that labels are cached/deduped on
    // `labels_cache_`.
    addr_to_alloc_[addr] = std::make_pair(size, fetch_label_ptr(label));

    // Increase the total number of allocated bytes.
    num_alloc_bytes_ += size;

    // Perform an interval dump if necessary.
    try_interval_dump();
  } catch (const std::bad_alloc&) {
    dump_and_terminate_internal();
  }
}

void HeapProfiler::record_dealloc(const void* const p) {
  std::unique_lock<std::mutex> ul(mutex_);

  try {
    const uint64_t addr = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(p));

    // Until we have replaced all dynamic memory APIs, we will silently
    // ignore mismatched `record_alloc` and `record_dealloc` calls.
    // assert(addr_to_alloc_.count(addr) > 0);
    if (addr_to_alloc_.count(addr) == 0) {
      // std::cerr << "TileDB:: unmatched dealloc on " << std::hex << addr
      //          << std::endl;
      return;
    }

    // Increment the total number of deallocation operations.
    ++num_deallocs_;

    // Increase the total number of allocated bytes.
    num_dealloc_bytes_ += addr_to_alloc_[addr].first;

    // Release the label to the `label_cache_`.
    release_label_ptr(addr_to_alloc_[addr].second);
    addr_to_alloc_.erase(addr);
  } catch (const std::bad_alloc&) {
    dump_and_terminate_internal();
  }
}

void HeapProfiler::dump_and_terminate() {
  std::unique_lock<std::mutex> ul(mutex_);
  dump_and_terminate_internal();
}

void HeapProfiler::dump() {
  std::unique_lock<std::mutex> ul(mutex_);
  dump_internal();
}

/* ****************************** */
/*       PRIVATE FUNCTIONS        */
/* ****************************** */

void HeapProfiler::start_periodic_dump() {
  // Spawn a thread to dump every `dump_interval_ms_` milliseconds.
  periodic_dump_thread_ =
      std::unique_ptr<std::thread>(new std::thread([this]() {
        // Loop until `dump_interval_ms_` is set to `0` in the
        // HeapProfiler destructor.
        while (dump_interval_ms_ > 0) {
          std::this_thread::sleep_for(
              std::chrono::milliseconds(dump_interval_ms_));
          dump();
        }
      }));
}

void HeapProfiler::try_interval_dump() {
  if (dump_interval_bytes_ == 0)
    return;

  // If the total allocation bytes have increased by more than
  // `dump_interval_bytes_` since the last interval dump, perform
  // a dump.
  if (num_alloc_bytes_ - last_interval_dump_alloc_bytes_ >=
      dump_interval_bytes_) {
    dump_internal();
    last_interval_dump_alloc_bytes_ = num_alloc_bytes_;
  }
}

std::string HeapProfiler::create_dump_file(
    const std::string& file_name_prefix) {
  const uint64_t epoch_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  const std::string file_name =
      file_name_prefix + "__" + std::to_string(epoch_ms);

  std::ofstream file_stream;
  file_stream.open(file_name, std::ofstream::out);
  if (!file_stream) {
    std::cerr << "TileDB:: failed to create dump file " << file_name
              << std::endl;
    exit(EXIT_FAILURE);
  }

  file_stream.close();

  return file_name;
}

const std::string* HeapProfiler::fetch_label_ptr(const std::string& label) {
  if (label.empty())
    return nullptr;

  // If we have an entry in `labels_cache_` with a value matching
  // `label`, return a pointer to the cached string. Note that pointers
  // and references to elements on `labels_cache_` are not invalidated
  // in the event of an internal rehash.
  auto iter = labels_cache_.find(label);
  if (iter != labels_cache_.end()) {
    ++iter->second;
    return &iter->first;
  }

  std::pair<std::unordered_map<std::string, uint64_t>::iterator, bool> ret =
      labels_cache_.emplace(label, 1);
  assert(ret.second);

  return &ret.first->first;
}

void HeapProfiler::release_label_ptr(const std::string* label) {
  if (!label)
    return;

  // Fetch the cache entry.
  assert(!label->empty());
  auto iter = labels_cache_.find(*label);
  assert(iter != labels_cache_.end());

  // Decrement the reference counter. If the referene count
  // reaches 0, free the label and delete its entry in the
  // cache.
  if (--iter->second == 0)
    labels_cache_.erase(iter);
}

void HeapProfiler::dump_and_terminate_internal() {
  // Free our reserved heap memory in an attempt to
  // ensure there is enough memory to dump the stats.
  free(reserved_memory_);

  std::cerr << "TileDB: HeapProfiler terminating" << std::endl;

  // Dump the stats and exit the process.
  dump_internal();
  exit(EXIT_FAILURE);
}

void HeapProfiler::dump_internal() {
  std::ofstream file_stream;
  if (!file_name_.empty()) {
    file_stream.open(file_name_, std::ofstream::out | std::ofstream::app);
    if (!file_stream) {
      std::cerr << "TileDB:: failed to open dump file " << file_name_
                << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  std::ostream* const out_stream =
      (file_name_.empty() ? &std::cout : &file_stream);

  *out_stream << "TileDB: HeapProfiler dump" << std::endl;
  *out_stream << "  num_allocs_ " << num_allocs_ << std::endl;
  *out_stream << "  num_deallocs_ " << num_deallocs_ << std::endl;
  *out_stream << "  num_alloc_bytes_ " << num_alloc_bytes_ << std::endl;
  *out_stream << "  num_dealloc_bytes_ " << num_dealloc_bytes_ << std::endl;

  // Build a map between labels and their total number of outstanding bytes.
  std::unordered_map<const std::string*, uint64_t> label_to_alloc;
  for (const auto& kv : addr_to_alloc_) {
    const uint64_t bytes = kv.second.first;
    const std::string* const label = kv.second.second;
    if (labels_cache_[*label] == 1) {
      // If this label is only used once, we immediately know that
      // there are no other elements in `addr_to_alloc_` to sum.
      // Print this now to avoid consuming more memory within
      // `label_to_alloc`.
      if (dump_threshold_bytes_ == 0 || bytes >= dump_threshold_bytes_)
        *out_stream << "[" << *label << "]"
                    << " " << bytes << std::endl;
    } else {
      if (label_to_alloc.count(label) == 0)
        label_to_alloc[label] = bytes;
      else
        label_to_alloc[label] += bytes;
    }
  }

  for (const auto& kv : label_to_alloc) {
    const std::string* const label = kv.first;
    const uint64_t bytes = kv.second;
    if (dump_threshold_bytes_ == 0 || bytes >= dump_threshold_bytes_)
      *out_stream << "  [" << *label << "]"
                  << " " << bytes << std::endl;
  }

  if (!file_name_.empty())
    file_stream.close();
}

}  // namespace tiledb::common
