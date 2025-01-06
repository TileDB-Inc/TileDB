/**
 * @file   unit_parallel_merge.cc
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
 */

#include "tiledb/common/algorithm/parallel_merge.h"

#include <test/support/src/mem_helpers.h>
#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

using namespace tiledb::algorithm;
using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::test;

namespace tiledb::algorithm {

template <typename T>
using Streams = std::vector<std::vector<T>>;

/**
 * An instance of an input to the `split_point_stream_bounds`
 * function.
 *
 * The `verify` method checks correctness of `split_point_stream_bounds`
 * with respect to the input.
 */
template <typename T>
struct VerifySplitPointStream {
  std::shared_ptr<MemoryTracker> memory;

  Streams<T> streams;
  uint64_t which;
  MergeUnit search_bounds;

  void verify() {
    RC_ASSERT(streams.size() == search_bounds.starts.size());
    RC_ASSERT(search_bounds.starts.size() == search_bounds.ends.size());

    RC_ASSERT(search_bounds.starts[which] < search_bounds.ends[which]);

    std::vector<std::span<T>> spans;
    for (auto& stream : streams) {
      spans.push_back(std::span(stream));
    }

    auto cmp = std::less<T>{};
    auto result = ParallelMerge<T>::split_point_stream_bounds(
        spans,
        cmp,
        *memory->get_resource(MemoryType::PARALLEL_MERGE_CONTROL),
        which,
        search_bounds);

    RC_ASSERT(result.ends[which] > 0);

    const auto& split_point = streams[which][search_bounds.ends[which] - 1];

    for (size_t s = 0; s < streams.size(); s++) {
      RC_ASSERT(result.starts[s] == search_bounds.starts[s]);
      RC_ASSERT(result.ends[s] <= search_bounds.ends[s]);

      for (size_t i = result.starts[s]; i < result.ends[s]; i++) {
        RC_ASSERT(streams[s][i] <= split_point);
      }
    }
  }
};

template <typename T>
struct VerifyIdentifyMergeUnit {
  Streams<T> streams;
  uint64_t target_items;

  void verify() {
    std::vector<std::span<T>> spans;
    for (auto& stream : streams) {
      spans.push_back(std::span(stream));
    }

    auto memory_tracker = get_test_memory_tracker();
    auto& memory =
        *memory_tracker->get_resource(MemoryType::PARALLEL_MERGE_CONTROL);

    auto cmp = std::less<T>{};
    auto result = ParallelMerge<T>::identify_merge_unit(
        spans, &cmp, memory, target_items);
    RC_ASSERT(target_items == result.num_items());

    for (size_t s = 0; s < streams.size(); s++) {
      if (streams[s].empty()) {
        RC_ASSERT(result.starts[s] == 0);
        RC_ASSERT(result.ends[s] == 0);
      } else if (result.starts[s] == result.ends[s]) {
        // this stream is not used at all, its first item must exceed
        // what lies at the boundary for all other streams
        for (size_t s2 = 0; s2 < streams.size(); s2++) {
          if (result.starts[s2] != result.ends[s2]) {
            RC_ASSERT(streams[s][0] >= streams[s2][result.ends[s2] - 1]);
          }
        }
      } else {
        // items from this stream in range `[starts[s], ends[s])` are included
        // in the merge unit, so everything outside of the merge unit must
        // exceed it
        auto& bound_item = streams[s][result.ends[s] - 1];
        for (size_t s2 = 0; s2 < streams.size(); s2++) {
          if (result.ends[s2] == streams[s2].size()) {
            // the entirety of `s2` is included in the merge unit,
            // we can infer no relation to `bound_item`
            continue;
          } else {
            RC_ASSERT(bound_item <= streams[s2][result.ends[s2]]);
          }
        }
      }
    }
  }
};

template <typename T>
struct VerifyTournamentMerge {
  std::shared_ptr<MemoryTracker> memory_tracker;
  // appears unused but is needed for `MergeUnit` lifetime

  Streams<T> streams;
  MergeUnit unit;

  void verify() {
    auto cmp = std::less<T>{};

    std::vector<T> output(unit.num_items());

    // SAFETY: the merge unit will begin writing at index `unit.output_start()`
    T* output_ptr = &output[-unit.output_start()];

    std::vector<std::span<T>> spans;
    for (size_t s = 0; s < streams.size(); s++) {
      auto& stream = streams[s];
      spans.push_back(std::span(stream));
    }

    auto result =
        ParallelMerge<T>::tournament_merge(spans, &cmp, unit, output_ptr);
    RC_ASSERT(result.ok());

    // compare against a naive and slow merge
    std::vector<T> inputcmp;
    inputcmp.reserve(output.size());
    for (size_t s = 0; s < streams.size(); s++) {
      inputcmp.insert(
          inputcmp.end(),
          streams[s].begin() + unit.starts[s],
          streams[s].begin() + unit.ends[s]);
    }
    std::sort(inputcmp.begin(), inputcmp.end());

    RC_ASSERT(inputcmp == output);
  }
};

template <typename T>
struct VerifyParallelMerge {
  Streams<T> streams;
  ParallelMergeOptions options;
  size_t pool_concurrency;

  void verify() {
    auto cmp = std::less<T>{};

    uint64_t total_items = 0;
    for (const auto& stream : streams) {
      total_items += stream.size();
    }

    std::vector<std::span<T>> spans;
    for (auto& stream : streams) {
      spans.push_back(std::span(stream));
    }

    // compare against a naive and slow merge
    std::vector<T> inputcmp;
    {
      inputcmp.reserve(total_items);
      for (size_t s = 0; s < streams.size(); s++) {
        inputcmp.insert(inputcmp.end(), streams[s].begin(), streams[s].end());
      }
      std::sort(inputcmp.begin(), inputcmp.end());
    }

    std::vector<T> output(total_items);

    auto memory_tracker = get_test_memory_tracker();
    ParallelMergeMemoryResources resources(*memory_tracker.get());

    ThreadPool pool(pool_concurrency);
    auto future = parallel_merge<T, decltype(cmp)>(
        pool, resources, options, spans, cmp, &output[0]);

    std::optional<uint64_t> prev_bound;
    std::optional<uint64_t> bound;
    while ((bound = future->await()).has_value()) {
      if (prev_bound.has_value()) {
        RC_ASSERT(*prev_bound < *bound);
        RC_ASSERT(std::equal(
            inputcmp.begin() + *prev_bound,
            inputcmp.begin() + *bound,
            output.begin() + *prev_bound,
            output.begin() + *bound));
      } else {
        RC_ASSERT(std::equal(
            inputcmp.begin(),
            inputcmp.begin() + *bound,
            output.begin(),
            output.begin() + *bound));
      }
      prev_bound = bound;
    }

    RC_ASSERT(inputcmp == output);
  }
};

}  // namespace tiledb::algorithm

namespace rc {

/**
 * @return a generator which generates a list of streams each sorted in
 * ascending order
 */
template <typename T>
Gen<Streams<T>> streams() {
  auto stream =
      gen::map(gen::arbitrary<std::vector<T>>(), [](std::vector<T> elts) {
        std::sort(elts.begin(), elts.end());
        return elts;
      });
  return gen::nonEmpty(gen::container<std::vector<std::vector<T>>>(stream));
}

template <typename T>
Gen<Streams<T>> streams_non_empty() {
  return gen::suchThat(streams<T>(), [](const Streams<T>& streams) {
    return std::any_of(
        streams.begin(), streams.end(), [](const std::vector<T>& stream) {
          return !stream.empty();
        });
  });
}

template <typename T>
Gen<MergeUnit> merge_unit(
    std::shared_ptr<MemoryTracker> memory_tracker, const Streams<T>& streams) {
  std::vector<Gen<std::pair<uint64_t, uint64_t>>> all_stream_bounds;
  for (const auto& stream : streams) {
    Gen<std::pair<uint64_t, uint64_t>> bounds = gen::apply(
        [](uint64_t a, uint64_t b) {
          return std::make_pair(std::min(a, b), std::max(a, b));
        },
        gen::inRange<uint64_t>(0, stream.size() + 1),
        gen::inRange<uint64_t>(0, stream.size() + 1));
    all_stream_bounds.push_back(bounds);
  }

  Gen<std::vector<std::pair<uint64_t, uint64_t>>> gen_stream_bounds =
      //   gen::arbitrary<std::vector<std::pair<uint64_t, uint64_t>>>();
      gen::exec([all_stream_bounds] {
        std::vector<std::pair<uint64_t, uint64_t>> bounds;
        for (const auto& stream_bound : all_stream_bounds) {
          bounds.push_back(*stream_bound);
        }
        return bounds;
      });

  auto memory =
      memory_tracker->get_resource(MemoryType::PARALLEL_MERGE_CONTROL);

  return gen::map(
      gen_stream_bounds,
      [memory](std::vector<std::pair<uint64_t, uint64_t>> bounds) {
        MergeUnit unit(*memory);
        unit.starts.reserve(bounds.size());
        unit.ends.reserve(bounds.size());
        for (const auto& bound : bounds) {
          unit.starts.push_back(bound.first);
          unit.ends.push_back(bound.second);
        }
        return unit;
      });
}

template <>
struct Arbitrary<ParallelMergeOptions> {
  static Gen<ParallelMergeOptions> arbitrary() {
    auto parallel_factor = gen::inRange(1, 16);
    auto min_merge_items = gen::inRange(1, 16 * 1024);
    return gen::apply(
        [](uint64_t parallel_factor, uint64_t min_merge_items) {
          return ParallelMergeOptions{
              .parallel_factor = parallel_factor,
              .min_merge_items = min_merge_items};
        },
        parallel_factor,
        min_merge_items);
  }
};

/**
 * Arbitrary `VerifySplitPointStream` input.
 *
 * Values generated by this satisfy the following properties:
 * 1) At least one of the generated `streams` is non-empty.
 * 2) `which` is an index of a non-empty stream.
 * 3) `search_bounds` has a non-empty range for stream `which`.
 */
template <typename T>
struct Arbitrary<VerifySplitPointStream<T>> {
  static Gen<VerifySplitPointStream<T>> arbitrary() {
    return gen::mapcat(streams_non_empty<T>(), [](Streams<T> streams) {
      std::vector<uint64_t> which_candidates;
      for (size_t s = 0; s < streams.size(); s++) {
        if (!streams[s].empty()) {
          which_candidates.push_back(s);
        }
      }

      auto memory_tracker = get_test_memory_tracker();

      auto which = gen::elementOf(which_candidates);
      auto search_bounds = merge_unit(memory_tracker, streams);

      return gen::apply(
          [](std::shared_ptr<MemoryTracker> memory_tracker,
             Streams<T> streams,
             uint64_t which,
             MergeUnit search_bounds) {
            for (size_t s = 0; s < streams.size(); s++) {
              if (s == which &&
                  search_bounds.starts[s] == search_bounds.ends[s]) {
                // tweak to ensure that the split point is valid
                if (search_bounds.starts[s] == 0) {
                  search_bounds.ends[s] = 1;
                } else {
                  search_bounds.starts[s] -= 1;
                }
              }
            }
            return VerifySplitPointStream<T>{
                .memory = memory_tracker,
                .streams = streams,
                .which = which,
                .search_bounds = search_bounds};
          },
          gen::just(memory_tracker),
          gen::just(streams),
          which,
          search_bounds);
    });
  }
};

template <typename T>
struct Arbitrary<VerifyIdentifyMergeUnit<T>> {
  static Gen<VerifyIdentifyMergeUnit<T>> arbitrary() {
    auto fields =
        gen::mapcat(streams_non_empty<T>(), [](const Streams<T> streams) {
          uint64_t total_items = 0;
          for (const auto& stream : streams) {
            total_items += stream.size();
          }
          return gen::pair(
              gen::just(streams), gen::inRange<uint64_t>(1, total_items + 1));
        });
    return gen::map(fields, [](std::pair<Streams<T>, uint64_t> fields) {
      return VerifyIdentifyMergeUnit<T>{
          .streams = fields.first, .target_items = fields.second};
    });
  }
};

template <typename T>
struct Arbitrary<VerifyTournamentMerge<T>> {
  static Gen<VerifyTournamentMerge<T>> arbitrary() {
    return gen::mapcat(streams<T>(), [](Streams<T> streams) {
      auto memory_tracker = get_test_memory_tracker();
      auto unit = merge_unit(memory_tracker, streams);
      return gen::apply(
          [memory_tracker](Streams<T> streams, MergeUnit unit) {
            return VerifyTournamentMerge<T>{
                .memory_tracker = memory_tracker,
                .streams = streams,
                .unit = unit};
          },
          gen::just(streams),
          unit);
    });
  }
};

template <typename T>
struct Arbitrary<VerifyParallelMerge<T>> {
  static Gen<VerifyParallelMerge<T>> arbitrary() {
    return gen::apply(
        [](Streams<T> streams,
           ParallelMergeOptions options,
           size_t pool_concurrency) {
          return VerifyParallelMerge<T>{
              .streams = streams,
              .options = options,
              .pool_concurrency = pool_concurrency};
        },
        streams<T>(),
        gen::arbitrary<ParallelMergeOptions>(),
        gen::inRange(1, 32));
  }
};

template <>
void show<VerifyIdentifyMergeUnit<uint64_t>>(
    const VerifyIdentifyMergeUnit<uint64_t>& instance, std::ostream& os) {
  os << "{" << std::endl;
  os << "\t\"streams\": ";
  show<decltype(instance.streams)>(instance.streams, os);
  os << "," << std::endl;
  os << "\t\"target_items\": " << instance.target_items << std::endl;
  os << "}";
}

template <>
void show<VerifyTournamentMerge<uint64_t>>(
    const VerifyTournamentMerge<uint64_t>& instance, std::ostream& os) {
  os << "{" << std::endl;
  os << "\t\"streams\": ";
  show<decltype(instance.streams)>(instance.streams, os);
  os << "," << std::endl;
  os << "\t\"starts\": [";
  show(instance.unit.starts, os);
  os << "]" << std::endl;
  os << "," << std::endl;
  os << "\t\"ends\": [";
  show(instance.unit.ends, os);
  os << "]" << std::endl;
  os << "}";
}

template <>
void show<VerifyParallelMerge<uint64_t>>(
    const VerifyParallelMerge<uint64_t>& instance, std::ostream& os) {
  os << "{" << std::endl;
  os << "\t\"streams\": ";
  show<decltype(instance.streams)>(instance.streams, os);
  os << "," << std::endl;
  os << "\t\"options\": {" << std::endl;
  os << "\t\t\"parallel_factor\": " << instance.options.parallel_factor
     << std::endl;
  os << "\t\t\"min_merge_items\": " << instance.options.min_merge_items
     << std::endl;
  os << "\t}," << std::endl;
  os << "\t\"pool_concurrency\": " << instance.pool_concurrency << std::endl;
  os << "}";
}

}  // namespace rc

TEST_CASE(
    "parallel merge rapidcheck uint64_t VerifySplitPointStream",
    "[algorithm][parallel_merge]") {
  rc::prop(
      "verify_split_point_stream_bounds",
      [](VerifySplitPointStream<uint64_t> input) { input.verify(); });
}

TEST_CASE(
    "parallel merge rapidcheck uint64_t VerifyIdentifyMergeUnit",
    "[algorithm][parallel_merge]") {
  rc::prop(
      "verify_identify_merge_unit",
      [](VerifyIdentifyMergeUnit<uint64_t> input) { input.verify(); });
}

TEST_CASE(
    "parallel merge rapidcheck uint64_t VerifyTournamentMerge",
    "[algorithm][parallel_merge]") {
  rc::prop(
      "verify_tournament_merge",
      [](VerifyTournamentMerge<uint64_t> input) { input.verify(); });
}

TEST_CASE(
    "parallel merge rapidcheck uint64_t VerifyParallelMerge",
    "[algorithm][parallel_merge]") {
  rc::prop("verify_parallel_merge", [](VerifyParallelMerge<uint64_t> input) {
    input.verify();
  });
}

TEST_CASE(
    "parallel merge rapidcheck int8_t VerifySplitPointStream",
    "[algorithm][parallel_merge]") {
  rc::prop(
      "verify_split_point_stream_bounds",
      [](VerifySplitPointStream<int8_t> input) { input.verify(); });
}

TEST_CASE(
    "parallel merge rapidcheck int8_t VerifyIdentifyMergeUnit",
    "[algorithm][parallel_merge]") {
  rc::prop(
      "verify_identify_merge_unit",
      [](VerifyIdentifyMergeUnit<int8_t> input) { input.verify(); });
}

TEST_CASE(
    "parallel merge rapidcheck int8_t VerifyTournamentMerge",
    "[algorithm][parallel_merge]") {
  rc::prop("verify_tournament_merge", [](VerifyTournamentMerge<int8_t> input) {
    input.verify();
  });
}

TEST_CASE(
    "parallel merge rapidcheck int8_t VerifyParallelMerge",
    "[algorithm][parallel_merge]") {
  rc::prop("verify_parallel_merge", [](VerifyParallelMerge<int8_t> input) {
    input.verify();
  });
}

struct OneDigit {
  uint8_t value_;

  operator uint8_t() const {
    return value_;
  }
};

namespace rc {
template <>
struct Arbitrary<OneDigit> {
  static Gen<OneDigit> arbitrary() {
    return gen::map(gen::inRange(0, 10), [](uint8_t value) {
      RC_PRE(0 <= value && value < 10);
      return OneDigit{.value_ = value};
    });
  }
};
}  // namespace rc

TEST_CASE(
    "parallel merge rapidcheck OneDigit VerifySplitPointStream",
    "[algorithm][parallel_merge]") {
  rc::prop(
      "verify_split_point_stream_bounds",
      [](VerifySplitPointStream<OneDigit> input) { input.verify(); });
}

TEST_CASE(
    "parallel merge rapidcheck OneDigit VerifyIdentifyMergeUnit",
    "[algorithm][parallel_merge]") {
  rc::prop(
      "verify_identify_merge_unit",
      [](VerifyIdentifyMergeUnit<OneDigit> input) { input.verify(); });
}

TEST_CASE(
    "parallel merge rapidcheck OneDigit VerifyTournamentMerge",
    "[algorithm][parallel_merge]") {
  rc::prop(
      "verify_tournament_merge",
      [](VerifyTournamentMerge<OneDigit> input) { input.verify(); });
}

TEST_CASE(
    "parallel merge rapidcheck OneDigit VerifyParallelMerge",
    "[algorithm][parallel_merge]") {
  rc::prop("verify_parallel_merge", [](VerifyParallelMerge<OneDigit> input) {
    input.verify();
  });
}

TEST_CASE("parallel merge example", "[algorithm][parallel_merge]") {
  std::vector<uint64_t> output(20);

  ParallelMergeOptions options = {.parallel_factor = 4, .min_merge_items = 4};

  auto memory_tracker = get_test_memory_tracker();
  ParallelMergeMemoryResources resources(*memory_tracker.get());

  ThreadPool pool(4);

  SECTION("less") {
    auto cmp = std::less<uint64_t>{};

    std::vector<std::vector<uint64_t>> streambufs = {
        {123, 456, 789, 890, 901},
        {135, 357, 579, 791, 913},
        {24, 246, 468, 680, 802},
        {100, 200, 300, 400, 500}};
    const std::vector<uint64_t> expect = {24,  100, 123, 135, 200, 246, 300,
                                          357, 400, 456, 468, 500, 579, 680,
                                          789, 791, 802, 890, 901, 913};
    std::vector<std::span<uint64_t>> streams = {
        std::span(streambufs[0]),
        std::span(streambufs[1]),
        std::span(streambufs[2]),
        std::span(streambufs[3]),
    };

    auto future = parallel_merge<uint64_t, decltype(cmp)>(
        pool, resources, options, streams, cmp, &output[0]);

    future->block();

    CHECK(output.size() == expect.size());
    CHECK(output == expect);
  }

  SECTION("greater") {
    auto cmp = std::greater<uint64_t>{};

    std::vector<std::vector<uint64_t>> streambufs = {
        {901, 890, 789, 456, 123},
        {913, 791, 579, 357, 135},
        {802, 680, 468, 246, 24},
        {500, 400, 300, 200, 100}};
    const std::vector<uint64_t> expect = {913, 901, 890, 802, 791, 789, 680,
                                          579, 500, 468, 456, 400, 357, 300,
                                          246, 200, 135, 123, 100, 24};
    std::vector<std::span<uint64_t>> streams = {
        std::span(streambufs[0]),
        std::span(streambufs[1]),
        std::span(streambufs[2]),
        std::span(streambufs[3]),
    };

    auto future = parallel_merge<uint64_t, decltype(cmp)>(
        pool, resources, options, streams, cmp, &output[0]);

    future->block();

    CHECK(output.size() == expect.size());
    CHECK(output == expect);
  }
}
