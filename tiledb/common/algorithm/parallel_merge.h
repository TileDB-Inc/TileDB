/**
 * @file   parallel_merge.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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
 * This file defines an implementation of a parallel merge algorithm for
 * in-memory data.
 *
 * TODO: explain algorithm
 */

#ifndef TILEDB_PARALLEL_MERGE_H
#define TILEDB_PARALLEL_MERGE_H

#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/pmr.h"
#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool/producer_consumer_queue.h"
#include "tiledb/common/thread_pool/thread_pool.h"

#include <memory>
#include <queue>

namespace tiledb::algorithm {

class ParallelMergeException : public tiledb::common::StatusException {
 public:
  explicit ParallelMergeException(const std::string& detail)
      : tiledb::common::StatusException("ParallelMerge", detail) {
  }
};

/**
 * Options for running the parallel merge.
 */
struct ParallelMergeOptions {
  // Maximum number of parallel tasks to submit.
  uint64_t parallel_factor;

  // Minimum number of items to merge in each parallel task.
  uint64_t min_merge_items;
};

struct ParallelMergeMemoryResources {
  // Memory resource for allocating parallel merge control structures.
  tdb::pmr::memory_resource& control;

  ParallelMergeMemoryResources(tiledb::sm::MemoryTracker& memory_tracker)
      : control(*memory_tracker.get_resource(
            tiledb::sm::MemoryType::PARALLEL_MERGE_CONTROL)) {
  }
};

struct MergeUnit;

/**
 * The output future of the parallel merge.
 *
 * Provides methods for waiting on the incremental asynchronous output
 * of the merge operation.
 *
 * FIXME: need to move the comparator here somehow so that way
 * it doesn't have to have a longer lifetime from the caller
 */
struct ParallelMergeFuture {
  ParallelMergeFuture(
      ParallelMergeMemoryResources& memory, size_t parallel_factor);

  ~ParallelMergeFuture();

  /**
   * @return memory resource used for parallel merge control structures
   */
  tdb::pmr::memory_resource& control_memory() const {
    return memory_.control;
  }

  /**
   * @return true if the merge has completed
   */
  bool finished() const;

  /**
   * @return the position in the output up to which the merge has completed
   */
  std::optional<uint64_t> valid_output_bound() const;

  /**
   * Wait for more data to finish merging.
   *
   * @return The bound in the output buffer up to which the merge has completed
   * @throws If a task finished with error status.
   *         If this happens then this future is left in an invalid state
   *         and should not be used.
   */
  std::optional<uint64_t> await();

  /**
   * Wait for all data to finish merging.
   *
   * @throws If a task finished with error status.
   *         If this happens then this future is left in an invalid state
   *         and should not be used.
   */
  void block();

 private:
  ParallelMergeMemoryResources memory_;

  tdb::pmr::vector<MergeUnit> merge_bounds_;
  tiledb::common::ProducerConsumerQueue<tiledb::common::ThreadPool::Task>
      merge_tasks_;

  // index of the next expected item in `merge_bounds_`
  uint64_t merge_cursor_;

  template <typename T, typename Cmp>
  friend class ParallelMerge;
};

template <typename T, class Compare = std::less<T>>
tdb::pmr::unique_ptr<ParallelMergeFuture> parallel_merge(
    tiledb::common::ThreadPool& pool,
    const ParallelMergeOptions& options,
    std::span<std::span<T>> streams,
    T* output);

/**
 * Represents one sequential unit of the parallel merge.
 *
 * Merges values in the ranges for each stream `s`, [starts[s], ends[s]].
 * This unit writes to the output in the range [sum(starts), sum(ends)].
 */
struct MergeUnit {
  tdb::pmr::vector<uint64_t> starts;
  tdb::pmr::vector<uint64_t> ends;

  MergeUnit(tdb::pmr::memory_resource& resource)
      : starts(&resource)
      , ends(&resource) {
  }

  uint64_t num_items() const {
    uint64_t total_items = 0;
    for (size_t i = 0; i < starts.size(); i++) {
      total_items += (ends[i] - starts[i]);
    }
    return total_items;
  }

  uint64_t output_start() const {
    uint64_t total_bound = 0;
    for (size_t i = 0; i < ends.size(); i++) {
      total_bound += starts[i];
    }
    return total_bound;
  }

  uint64_t output_end() const {
    uint64_t total_bound = 0;
    for (size_t i = 0; i < ends.size(); i++) {
      total_bound += ends[i];
    }
    return total_bound;
  }

  bool operator==(const MergeUnit& other) const {
    return (starts == other.starts) && (ends == other.ends);
  }
};

// forward declarations of friend classes for testing
template <typename T>
struct VerifySplitPointStream;
template <typename T>
struct VerifyIdentifyMergeUnit;
template <typename T>
struct VerifyTournamentMerge;

template <typename T, class Compare = std::less<T>>
class ParallelMerge {
 public:
  typedef std::span<std::span<T>> Streams;

 private:
  struct span_greater {
    span_greater(Compare& cmp)
        : cmp_(cmp) {
    }

    bool operator()(const std::span<T> l, const std::span<T> r) const {
      return !(cmp_(l.front(), r.front()));
    }

    Compare cmp_;
  };

  static Status tournament_merge(
      Streams streams, Compare* cmp, const MergeUnit& unit, T* output) {
    std::vector<std::span<T>> container;
    container.reserve(streams.size());

    // NB: we can definitely make a more optimized implementation
    // which does a bunch of buffering for each battle in the tournament
    // but this is straightforward
    std::priority_queue<std::span<T>, std::vector<std::span<T>>, span_greater>
        tournament(span_greater(*cmp), container);

    for (size_t i = 0; i < streams.size(); i++) {
      if (unit.starts[i] != unit.ends[i]) {
        tournament.push(
            streams[i].subspan(unit.starts[i], unit.ends[i] - unit.starts[i]));
      }
    }

    size_t o = unit.output_start();

    while (!tournament.empty()) {
      auto stream = tournament.top();
      tournament.pop();

      // empty streams are not put on the priority queue
      assert(!stream.empty());

      output[o++] = stream.front();

      if (stream.size() > 1) {
        tournament.push(stream.subspan(1));
      }
    }

    if (o == unit.output_end()) {
      return tiledb::common::Status::Ok();
    } else {
      return tiledb::common::Status_Error("Internal error in parallel merge");
    }
  }

  static MergeUnit split_point_stream_bounds(
      Streams streams,
      Compare& cmp,
      tdb::pmr::memory_resource& memory,
      uint64_t which,
      const MergeUnit& search_bounds) {
    const T& split_point = streams[which][search_bounds.ends[which] - 1];

    MergeUnit output(memory);
    output.starts = search_bounds.starts;
    output.ends.reserve(streams.size());

    for (uint64_t i = 0; i < streams.size(); i++) {
      if (i == which) {
        output.starts[i] = search_bounds.starts[i];
        output.ends.push_back(search_bounds.ends[i]);
      } else {
        std::span<T> substream(
            streams[i].begin() + search_bounds.starts[i],
            streams[i].begin() + search_bounds.ends[i]);

        auto lower_bound =
            std::lower_bound<decltype(substream.begin()), T, Compare>(
                substream.begin(), substream.end(), split_point, cmp);
        output.ends.push_back(
            output.starts[i] + std::distance(substream.begin(), lower_bound));
      }
    }

    return output;
  }

  enum class SearchStep { Stalled, MadeProgress, Converged };

  struct SearchMergeBoundary {
    Streams streams_;
    Compare& cmp_;
    tdb::pmr::memory_resource& memory_;
    uint64_t split_point_stream_;
    uint64_t remaining_items_;
    MergeUnit search_bounds_;

    SearchMergeBoundary(
        Streams streams,
        Compare& cmp,
        tdb::pmr::memory_resource& memory,
        uint64_t target_items)
        : streams_(streams)
        , cmp_(cmp)
        , memory_(memory)
        , split_point_stream_(0)
        , remaining_items_(target_items)
        , search_bounds_(memory) {
      search_bounds_.starts.reserve(streams.size());
      search_bounds_.ends.reserve(streams.size());
      for (const auto& stream : streams) {
        search_bounds_.starts.push_back(0);
        search_bounds_.ends.push_back(stream.size());
      }
    }

    MergeUnit current() const {
      MergeUnit m(memory_);
      m.starts.resize(search_bounds_.starts.size(), 0);
      m.ends = search_bounds_.ends;
      return m;
    }

    SearchStep step() {
      if (remaining_items_ == 0) {
        return SearchStep::Converged;
      }

      advance_split_point_stream();

      MergeUnit split_point_bounds(memory_);
      {
        // temporarily shrink the split point stream bounds to indicate the
        // split point
        auto original_end = search_bounds_.ends[split_point_stream_];
        if (search_bounds_.starts[split_point_stream_] >=
            search_bounds_.ends[split_point_stream_]) {
          throw ParallelMergeException("Internal error: invalid split point");
        }

        search_bounds_.ends[split_point_stream_] =
            (search_bounds_.starts[split_point_stream_] +
             search_bounds_.ends[split_point_stream_] + 1) /
            2;

        split_point_bounds = split_point_stream_bounds(
            streams_, cmp_, memory_, split_point_stream_, search_bounds_);

        search_bounds_.ends[split_point_stream_] = original_end;
      }

      const uint64_t num_split_point_items = split_point_bounds.num_items();
      if (num_split_point_items == remaining_items_) {
        search_bounds_ = split_point_bounds;
        remaining_items_ = 0;
        return SearchStep::Converged;
      } else if (num_split_point_items < remaining_items_) {
        // the split point has too few tuples
        // we will include everything we found and advance
        assert(search_bounds_.num_items() > 0);

        if (search_bounds_.num_items() == 0) {
          throw ParallelMergeException(
              "Internal error: split point found zero tuples");
        }

        remaining_items_ -= num_split_point_items;
        search_bounds_.starts = split_point_bounds.ends;
        return SearchStep::MadeProgress;
      } else {
        // this split point has too many tuples
        // discard the items greater than the split point, and advance to a new
        // split point
        if (split_point_bounds == search_bounds_) {
          return SearchStep::Stalled;
        } else {
          search_bounds_.ends = split_point_bounds.ends;
          return SearchStep::MadeProgress;
        }
      }
    }

   private:
    uint64_t next_split_point_stream() const {
      return (split_point_stream_ + 1) % streams_.size();
    }

    void advance_split_point_stream() {
      for (unsigned i = 0; i < streams_.size(); i++) {
        split_point_stream_ = next_split_point_stream();
        if (search_bounds_.starts[split_point_stream_] ==
            search_bounds_.ends[split_point_stream_]) {
          continue;
        } else {
          return;
        }
      }
      throw ParallelMergeException(
          "Internal error: advance_split_point_stream");
    }
  };

  static MergeUnit identify_merge_unit(
      Streams streams,
      Compare* cmp,
      tdb::pmr::memory_resource& memory,
      uint64_t target_items) {
    SearchMergeBoundary search(streams, *cmp, memory, target_items);
    uint64_t stalled = 0;

    while (true) {
      auto step = search.step();
      switch (step) {
        case SearchStep::Stalled:
          stalled++;
          if (stalled >= streams.size()) {
            throw ParallelMergeException(
                "Internal error: no split point shrinks bounds");
          }
          continue;
        case SearchStep::MadeProgress:
          stalled = 0;
          continue;
        case SearchStep::Converged:
          return search.current();
      }
    }
  }

  static Status spawn_next_merge_unit(
      tiledb::common::ThreadPool* pool,
      Streams streams,
      Compare* cmp,
      uint64_t parallel_factor,
      uint64_t total_items,
      uint64_t target_unit_size,
      uint64_t p,
      T* output,
      ParallelMergeFuture* future) {
    const uint64_t output_end =
        std::min(total_items, (p + 1) * target_unit_size);

    auto accumulated_stream_bounds =
        identify_merge_unit(streams, cmp, future->control_memory(), output_end);

    if (p == 0) {
      future->merge_bounds_[p] = accumulated_stream_bounds;
      auto unit_future = pool->execute(
          tournament_merge, streams, cmp, future->merge_bounds_[p], output);
      future->merge_tasks_.push(std::move(unit_future));
    } else {
      future->merge_bounds_[p].starts = future->merge_bounds_[p - 1].ends;
      future->merge_bounds_[p].ends = accumulated_stream_bounds.ends;

      auto unit_future = pool->execute(
          tournament_merge, streams, cmp, future->merge_bounds_[p], output);
      future->merge_tasks_.push(std::move(unit_future));
    }

    if (p < parallel_factor - 1) {
      pool->execute(
          spawn_next_merge_unit,
          pool,
          streams,
          cmp,
          parallel_factor,
          total_items,
          target_unit_size,
          p + 1,
          output,
          future);
    } else {
      future->merge_tasks_.drain();
    }

    return tiledb::common::Status::Ok();
  }

  static void spawn_merge_units(
      tiledb::common::ThreadPool& pool,
      size_t parallel_factor,
      uint64_t total_items,
      Streams streams,
      Compare& cmp,
      T* output,
      ParallelMergeFuture& future) {
    // NB: round up, if there is a shorter merge unit it will be the last one.
    const uint64_t target_unit_size =
        (total_items + (parallel_factor - 1)) / parallel_factor;

    pool.execute(
        spawn_next_merge_unit,
        &pool,
        streams,
        &cmp,
        parallel_factor,
        total_items,
        target_unit_size,
        static_cast<uint64_t>(0),
        output,
        &future);
  }

  // friend declarations for testing
  friend struct VerifySplitPointStream<T>;
  friend struct VerifyIdentifyMergeUnit<T>;
  friend struct VerifyTournamentMerge<T>;

 public:
  static tdb::pmr::unique_ptr<ParallelMergeFuture> start(
      tiledb::common::ThreadPool& pool,
      ParallelMergeMemoryResources& memory,
      const ParallelMergeOptions& options,
      Streams streams,
      Compare& cmp,
      T* output) {
    uint64_t total_items = 0;
    for (const auto& stream : streams) {
      total_items += stream.size();
    }

    const uint64_t parallel_factor = std::clamp(
        total_items / options.min_merge_items,
        static_cast<uint64_t>(1),
        options.parallel_factor);

    tdb::pmr::unique_ptr<ParallelMergeFuture> future =
        tdb::pmr::emplace_unique<ParallelMergeFuture>(
            &memory.control, memory, parallel_factor);
    ParallelMerge::spawn_merge_units(
        pool, parallel_factor, total_items, streams, cmp, output, *future);
    return future;
  }
};

template <typename T, class Compare>
tdb::pmr::unique_ptr<ParallelMergeFuture> parallel_merge(
    tiledb::common::ThreadPool& pool,
    ParallelMergeMemoryResources& memory,
    const ParallelMergeOptions& options,
    std::span<std::span<T>> streams,
    Compare& cmp,
    T* output) {
  return ParallelMerge<T, Compare>::start(
      pool, memory, options, streams, cmp, output);
}

}  // namespace tiledb::algorithm

#endif
