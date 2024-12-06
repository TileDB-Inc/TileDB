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

#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

using namespace tiledb::algorithm;
using namespace tiledb::common;

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
        spans, cmp, which, search_bounds);

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

    auto cmp = std::less<T>{};
    auto result =
        ParallelMerge<T>::identify_merge_unit(spans, &cmp, target_items);
    RC_ASSERT(target_items == result->num_items());

    for (size_t s = 0; s < streams.size(); s++) {
      if (streams[s].empty()) {
        RC_ASSERT(result->starts[s] == 0);
        RC_ASSERT(result->ends[s] == 0);
      } else if (result->starts[s] == result->ends[s]) {
        // this stream is not used at all, its first item must exceed
        // what lies at the boundary for all other streams
        for (size_t s2 = 0; s2 < streams.size(); s2++) {
          if (result->starts[s2] != result->ends[s2]) {
            RC_ASSERT(streams[s][0] >= streams[s2][result->ends[s2] - 1]);
          }
        }
      } else {
        // items from this stream in range `[starts[s], ends[s])` are included
        // in the merge unit, so everything outside of the merge unit must
        // exceed it
        auto& bound_item = streams[s][result->ends[s] - 1];
        for (size_t s2 = 0; s2 < streams.size(); s2++) {
          if (result->ends[s2] == streams[s2].size()) {
            // the entirety of `s2` is included in the merge unit,
            // we can infer no relation to `bound_item`
            continue;
          } else {
            RC_ASSERT(bound_item <= streams[s2][result->ends[s2]]);
          }
        }
      }
    }
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

      auto which = gen::elementOf(which_candidates);

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

      return gen::apply(
          [](Streams<T> streams,
             uint64_t which,
             std::vector<std::pair<uint64_t, uint64_t>> bounds) {
            MergeUnit search_bounds;
            for (size_t s = 0; s < bounds.size(); s++) {
              const auto& bound = bounds[s];
              if (s == which && bound.first == bound.second) {
                // tweak to ensure that the split point is valid
                if (bound.first == 0) {
                  search_bounds.starts.push_back(0);
                  search_bounds.ends.push_back(1);
                } else {
                  search_bounds.starts.push_back(bound.first - 1);
                  search_bounds.ends.push_back(bound.second);
                }
              } else {
                search_bounds.starts.push_back(bound.first);
                search_bounds.ends.push_back(bound.second);
              }
            }
            return VerifySplitPointStream{
                .streams = streams,
                .which = which,
                .search_bounds = search_bounds};
          },
          gen::just(streams),
          which,
          gen_stream_bounds);
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
      return VerifyIdentifyMergeUnit{
          .streams = fields.first, .target_items = fields.second};
    });
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

}  // namespace rc

TEST_CASE(
    "parallel merge rapidcheck VerifySplitPointStream",
    "[algorithm][parallel_merge]") {
  rc::prop(
      "verify_split_point_stream_bounds",
      [](VerifySplitPointStream<uint64_t> input) { input.verify(); });
}

TEST_CASE(
    "parallel merge rapidcheck VerifyIdentifyMergeUnit",
    "[algorithm][parallel_merge]") {
  rc::prop(
      "verify_split_point_stream_bounds",
      [](VerifyIdentifyMergeUnit<uint64_t> input) { input.verify(); });
}

TEST_CASE("parallel merge example", "[algorithm][parallel_merge]") {
  std::vector<uint64_t> output(20);

  ParallelMergeOptions options = {.parallel_factor = 4, .min_merge_items = 4};

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

    std::unique_ptr<ParallelMergeFuture> future =
        parallel_merge<uint64_t, decltype(cmp)>(
            pool, options, streams, cmp, &output[0]);

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

    std::unique_ptr<ParallelMergeFuture> future =
        parallel_merge<uint64_t, decltype(cmp)>(
            pool, options, streams, cmp, &output[0]);

    future->block();

    CHECK(output.size() == expect.size());
    CHECK(output == expect);
  }
}
