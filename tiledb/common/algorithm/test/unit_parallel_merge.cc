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

const Streams<uint64_t> EXAMPLE_STREAMS = {
    {123, 456, 789, 890, 901},
    {135, 357, 579, 791, 913},
    {24, 246, 468, 680, 802},
    {100, 200, 300, 400, 500}};

template <
    ParallelMergeable I,
    class Compare = std::less<typename I::value_type::value_type>>
struct ParallelMergePublic : public ParallelMerge<I, Compare> {
  using ParallelMerge<I, Compare>::split_point_stream_bounds;
};

/**
 * Illustrates the steps of the split point algorithm
 */
TEST_CASE(
    "parallel merge split point stream bounds example",
    "[algorithm][parallel_merge]") {
  using PM = ParallelMergePublic<decltype(EXAMPLE_STREAMS)>;

  auto cmp =
      std::less<typename decltype(EXAMPLE_STREAMS)::value_type::value_type>{};

  auto memory_tracker = get_test_memory_tracker();
  auto& resource =
      *memory_tracker->get_resource(MemoryType::PARALLEL_MERGE_CONTROL);

  // below illustrates the steps the algorithm takes to identify
  // 10 tuples from `EXAMPLE_STREAMS` which comprise a merge unit

  // first step, choose stream 0 as the split point stream, 789 is the split
  // point
  const MergeUnit search_0(resource, {0, 0, 0, 0}, {5, 5, 5, 5});
  const MergeUnit expect_0(resource, {0, 0, 0, 0}, {3, 3, 4, 5});
  const MergeUnit result_0 = PM::split_point_stream_bounds(
      EXAMPLE_STREAMS, cmp, resource, 0, search_0);
  REQUIRE(expect_0 == result_0);

  // we found 15 tuples less than 789, discard positions and try again
  // using stream 1 midpoint 357 as the split point
  const MergeUnit search_1(resource, {0, 0, 0, 0}, {3, 3, 4, 5});
  const MergeUnit expect_1(resource, {0, 0, 0, 0}, {1, 2, 2, 3});
  const MergeUnit result_1 = PM::split_point_stream_bounds(
      EXAMPLE_STREAMS, cmp, resource, 1, search_1);
  REQUIRE(expect_1 == result_1);

  // we found 8 tuples, add positions to result and then continue
  // using stream 2 midpoint 468 as the next split point
  const MergeUnit search_2(resource, {1, 2, 2, 3}, {3, 3, 4, 5});
  const MergeUnit expect_2(resource, {1, 2, 2, 3}, {2, 2, 3, 4});
  const MergeUnit result_2 = PM::split_point_stream_bounds(
      EXAMPLE_STREAMS, cmp, resource, 2, search_2);
  REQUIRE(expect_2 == result_2);

  // that found 3 more tuples which is too many, discard above bounds
  // and advance to stream 4 midpoint 400 as split point
  const MergeUnit search_3(resource, {1, 2, 2, 3}, {2, 2, 3, 4});
  const MergeUnit expect_3(resource, {1, 2, 2, 3}, {1, 2, 2, 4});
  const MergeUnit result_3 = PM::split_point_stream_bounds(
      EXAMPLE_STREAMS, cmp, resource, 3, search_3);
  REQUIRE(expect_3 == result_3);

  // that only found itself, add to bounds and then wrap around
  // to stream 0 for the next split point 456
  const MergeUnit search_4(resource, {1, 2, 2, 4}, {2, 2, 3, 4});
  const MergeUnit expect_4(resource, {1, 2, 2, 4}, {2, 2, 2, 4});
  const MergeUnit result_4 = PM::split_point_stream_bounds(
      EXAMPLE_STREAMS, cmp, resource, 0, search_4);
  REQUIRE(expect_4 == result_4);

  // now the algorithm can yield {{0, 0, 0, 0}, {2, 2, 2, 4}} which is exactly
  // 10 tuples
}

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
    auto result = ParallelMerge<decltype(spans)>::split_point_stream_bounds(
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
    auto result = ParallelMerge<decltype(spans)>::identify_merge_unit(
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

    // SAFETY: the merge unit will begin writing at index
    // `unit.output_start()`
    T* output_buffer = output.data();
    T* output_ptr = output_buffer - unit.output_start();

    std::vector<std::span<T>> spans;
    for (size_t s = 0; s < streams.size(); s++) {
      auto& stream = streams[s];
      spans.push_back(std::span(stream));
    }

    auto result = ParallelMerge<decltype(spans)>::tournament_merge(
        spans, &cmp, unit, output_ptr);
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
    auto future =
        parallel_merge(pool, resources, options, spans, cmp, &output.data()[0]);

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
  SECTION("Shrink", "Instances found previously") {
    VerifyParallelMerge<uint64_t> instance;

    instance.pool_concurrency = 16;
    instance.options.parallel_factor = 9;
    instance.options.min_merge_items = 838;

    instance.streams.push_back({0, 1964708352853888873});
    instance.streams.push_back(
        {340504051726841736,
         344852335005584131,
         691432333882738270,
         905545882978089750,
         963620181783757219,
         2280318111133887983,
         2360815377599083728,
         2690651619726011594,
         2702620732101879003,
         3914614302172357712,
         3920420695891434736,
         3977654368208177782,
         4146162831399918660,
         4505912157534417120,
         8178660334482019917});
    instance.streams.push_back(
        {250243497698869483,  342520146670513946,  460639582018147099,
         846477702552957623,  878044668058705049,  1357775358844315809,
         1370649669225041685, 1622322930626715479, 2422936557820787022,
         2664728687050573609, 4086304083561519276, 4604089473340659230,
         4695253583520271148, 4815681767159335799, 5076904023605818640,
         5161056711708826442, 5186830854753185896, 5320898922395769611,
         5702839098081247517, 6822853618435881949, 6906251812414549285,
         7306870941006900029, 7598257804853437932, 8000178328658993696,
         8447950993548860398, 8603384033055905231, 9090603704217711025});
    instance.streams.push_back(
        {365034819649908200,  537278200643201754,  628028489233162476,
         1056053721546457072, 1631089707251576705, 2157012731714267023,
         2173302468290621506, 2190114075257687709, 3536586244893021144,
         4098871024727029043, 4219940064230999996, 5598434201692323521,
         5733035996731085551, 6151143939934632694, 6492184551178156461,
         6722564512870941217, 6828171325375387848, 6850196870345667255,
         6906839647346621570, 7360142404381869265, 7806912150418303184,
         8205639776231111202, 8313443253139136789, 8374628726628789598,
         8523234285186696347, 8807175409409426574, 8980081069707960942});
    instance.streams.push_back(
        {4756539517425999047,
         4798031049377651616,
         5977213149434464454,
         6005322683776308758,
         6224906161411016770});
    instance.streams.push_back(
        {111560816646238841,  163505226104379596,  242548861111792893,
         324636646688740289,  331129684786145021,  450004049859509952,
         550145571669736402,  654817179640157417,  671213505367837278,
         706726065752612744,  999609168490346640,  1100961644439277701,
         1414872839773979159, 1480413849222835949, 1738904315782367161,
         2027102963545979713, 2050103317583867222, 2107545739634791825,
         2141915563107849007, 2397505339096866433, 2576574260658871131,
         2845069992345569991, 3638249576331329231, 4441729953183251969,
         4452501912875377316, 4723777203540135155, 4766912539124929330,
         4819717683949706938, 4829678320063540316, 5088008666876198638,
         5107137635514295670, 5226988857720318312, 5382090861412964157,
         5433266385705824660, 5436982627608092533, 5517492712986358013,
         5747809558130145132, 6176513921995629268, 6211511678766187527,
         6275198405564436990, 6506598163638930866, 6507880583921708819,
         6623175656306519005, 6630776298659461624, 6700610119070243161,
         6873876316847742320, 6980573326640936743, 7252188579248074502,
         7326588526750597920, 7732810562794454604, 8063314292280663775,
         8099787036675479852, 8210799376119817540, 8349334719321385082,
         8494590863039018141, 8688281034194548590});
    instance.streams.push_back(
        {320254704479545700,  420655069887453760,  429060508926581977,
         460196499028024337,  953561779682446307,  1039074518241554955,
         1188212173726072620, 1249146672866809218, 1704032370646125413,
         1737139332407886824, 1842382122035045074, 1844020417020201978,
         2207529352739723407, 2228893670053514494, 2923067297152209597,
         3144322480149264161, 3500619286656977840, 3533301382068690249,
         3964300661998913793, 4283662014244198584, 4446399025796556476,
         4536325536394672331, 4626130514711806540, 4682353918542575176,
         5676795806028379698, 5866113662510159298, 5879769286594349065,
         6276319267672760976, 6609827901507963179, 6646639152544114166,
         6859661685633003582, 6925850022106562901, 6985856750423128642,
         7158392145585477116, 7210529973088861882, 7412129063702359716,
         8016834345512203897, 8088581722283968874, 8296428221780161990,
         8731882166226556027, 9015400517265605074, 9138799535618320177,
         9155087561764082181});
    instance.streams.push_back(
        {51454735997831121,   117143172861056921,  157721973952731687,
         881932815709816266,  927958564919087108,  976777298133626077,
         1032588504959403168, 1091640153918895492, 2054775938455049603,
         2113295226998029101, 2334647935409448680, 2484803221265381168,
         2523622069586548513, 2568249104269020600, 2593969581548133852,
         2739976198173245229, 3251441493648931270, 3305261233013288899,
         3544069342123672695, 3862199197989559994, 4217932160089300610,
         4415142305312094124, 4603038276368455611, 5042299410124212582,
         5076219980086298771, 6539019728857392915, 6945670032672847129,
         7327131529652747193, 7518215687926471612, 7593723469225582738,
         7602455822485877224, 7687988919468341787, 7744840933722738994,
         7902192185997485984, 8447693537973016685, 8912337266114505543,
         8986535159330870946, 9080616779175741434, 9148761422118920839,
         9176500996858521366});
    instance.streams.push_back(
        {137326360377084220,
         149180667847225337,
         316502800165470534,
         1869340588901188621,
         2164437463958863510,
         2231858092884084971,
         2291182276971213098,
         2416425506771763220,
         3384721358062562721,
         3867990211623809440,
         3902778783808334295,
         4816387939906796007,
         5589009429684157617,
         5834067346218124068,
         6763754842773870515,
         8115638931351625710,
         8687742730714094669,
         8795917704285277995,
         8803839723448146545});

    instance.streams.push_back(
        {23678161213040705,   510241764617141901,  536672264796840765,
         817271100240475214,  824992929621593785,  936259153877883971,
         960608553447658296,  1016644615123228297, 1079513929429818370,
         1247622598380225718, 1265875085621850877, 1286630358723915721,
         1585784388405292846, 1882514364265324397, 1962983099044038040,
         2047240307888766369, 2089641636142471494, 2104081503879861800,
         2157499291986582082, 2242462293059025198, 2397818829671002449,
         2474643594359212111, 2626879015906626636, 2800942538936715183,
         2950124780977040035, 2962729703188401848, 3195363707418545400,
         3254400008948970680, 3341934193963561380, 3568105357608078905,
         3607416972776535502, 3742895572352962311, 3750922277236933324,
         3862781470922951700, 4133639907119172438, 4167991245860412544,
         4215009550182849694, 4348634397772330783, 4540720827918093668,
         4605892725108032743, 5112147611803069534, 5240743235082602958,
         5293780188735699423, 5344803738723997122, 5359644938166001876,
         5383045845773588147, 5460539756774575500, 5485135325974371263,
         5635140245844161553, 5710092722342554088, 5937538870686885810,
         6035986245880797533, 6332037239660591599, 6377740404404122118,
         6513905600152676553, 6745922411070403519, 6842657271710417778,
         7012233895223404545, 7082439372294707788, 7169430476557444103,
         7252294858470375727, 7296001643492304039, 7362899468051412298,
         7429857992136000225, 7554448534418702302, 7584251785448506899,
         7962776980238621413, 8113407862216908129, 8190116647855294077,
         8243475005557144977, 8246578745154143591, 8300197884230884027,
         8511569804890566846, 8521339560457020348, 8524049315697237765,
         8609444196105514408, 8705353794789460453, 8804664057451386903,
         8995133238524296428, 9002674372116479015});
    instance.streams.push_back(
        {852799182518779523,  976211879747489697,  1037751937156943550,
         1183309793253415613, 1278268616507222314, 1451165745236086062,
         1621512904012972626, 1651735659866361412, 1777214878523913043,
         2214931377439983634, 2283177876896964407, 2738593353743079856,
         2820127664090891864, 2844373930338721165, 3189296936039027077,
         3254954514620592062, 3494386171639606090, 3551531641784773313,
         3684669015657700411, 3778424844868346851, 3807518953727490121,
         4014024793634145913, 4376528050311066505, 4709788895635016301,
         4802875159745211525, 4912077114664418965, 5154896847848578782,
         5399889012382577346, 5428207563089827676, 5490404187634371736,
         5658622587346343974, 5749250937460737403, 5854686605696362016,
         5982049066807674509, 6242946560536538596, 6541217505859643210,
         6573036243604757175, 6705697932294170025, 6713109902807273426,
         6921284692183694826, 6957992858094231231, 6967608779543192373,
         7154467612072689977, 7165366467803280993, 7528984442376590632,
         7562549652554641450, 7717229551377374413, 7754567712155703384,
         7868914565741615787, 7937187999987813578, 8701842928973852450,
         8892464805414749943, 9115325089189555539});
    instance.streams.push_back(
        {20943666391368365,   50301707991823441,   78148549653558169,
         223537727943675077,  457374694131826701,  677769231307288719,
         738690972072549792,  808779816463584996,  1177985475557649325,
         1237824973985190974, 1409756236817959093, 1435911996961227602,
         1503641274947220311, 1511764275313859679, 1528131221941858934,
         1534381681586604241, 1542839573900239322, 1544003133799503333,
         1588804139273928590, 1801974190474587685, 1823514731001628521,
         2310083928250447665, 2326244911525083082, 2472708620182869557,
         2503087945401652204, 2600086538901526228, 2628865213158208270,
         2667076894330604699, 2707422557831791237, 2777846399740576177,
         2926005513144882263, 3140746181082342021, 3234383478068003737,
         3284748605682722794, 3654468547581799774, 3950170168303459161,
         3951670650353826130, 4047945913306191959, 4113807635664092203,
         4266045397888357498, 4299374661736063607, 4438427720679448353,
         4723519166837053894, 4771139015133323752, 4869515068761357173,
         4970536998559858982, 5073557385565822888, 5198166880328273127,
         5271572181663246052, 5314082606635789328, 5477307833345145180,
         5524207557087096653, 5886015987106668609, 6026454889985748648,
         6084803043157522481, 6158372282711088755, 6168222106544477004,
         6204006830525076915, 6408019547372630191, 6513075043672880140,
         6674516535180876795, 6822681571021993635, 6862399161419494149,
         7067115452221214899, 7138516978426804873, 7689152016280202686,
         7909580144088976322, 8324672428675972990, 8445663724062838862,
         8623264473608867537, 8670488819051193171, 8790796972651896445,
         8974117459504560162, 9096662290612606577});
    instance.streams.push_back(
        {63166126530205759,   242392958169270718,  299572426752384365,
         371210267059975449,  409803653906946475,  468886860378125805,
         470084126843809657,  481593649516078721,  903834583636702311,
         958411081937070554,  1643594388685317464, 1867077484313968133,
         1878992910711600903, 2096269166292483012, 2221603162432482372,
         2367859021422440563, 2640404573406261967, 2677835376674463112,
         2762566780438590041, 2970834855784194277, 3245590543761084988,
         3331926875433018809, 3354894536277567765, 3359505071381462687,
         3402470977684680351, 3551747279395294877, 3643287767386033028,
         3785191949950430905, 4110028576828673249, 4170786080207290662,
         4219806117295035085, 4455232860654935884, 4536653577501826109,
         4545527876600156094, 4625757865323486607, 5113820071837780591,
         5431359545392859663, 5644169066530322315, 5812510450528493936,
         5887112635987962293, 5930820878774918392, 6034785964355711944,
         6046455894880780916, 6359379134180690054, 6427488655203662660,
         6488922940107192870, 6630859187399752714, 6647199501483481209,
         6702497299919482097, 6913283435188544583, 7000765135102730735,
         7323710144182268219, 7609074946912556631, 7630879955827434637,
         8012837334883764116, 8177369519382860768, 8373691128186397719,
         8586863860394192504, 8658966137662110078, 8745623093981340006,
         8759582612872391189, 9216498227193543282});
    instance.streams.push_back(
        {83371540939874687,   99090153966040961,   265066980165143733,
         270185734230162060,  270764349377328729,  288791090302716150,
         515685752694836926,  527086694921111292,  537609362202016533,
         599732720196005842,  612031408334258699,  896904216546241482,
         999769207989122942,  1133043511208190641, 1693690049598026418,
         1772719663166431987, 1951519083505548569, 2091446513470438219,
         2893471902139543033, 2901452159654803955, 2911580424380098401,
         3011003540331906665, 3117245771853887492, 3340914718518675556,
         3454665172494129776, 3540466628941670785, 3631479891179203900,
         3820561738171968624, 3936713133309989730, 3974381306592113118,
         4072672321187480082, 4134085955791964608, 4252036902039087422,
         4648478980133725331, 4846311851024512063, 4922935334112814400,
         5143553175086741003, 5160210906682153305, 5190619362343208008,
         5286806527989322631, 5347835397983529763, 5480058516342719799,
         5509729406506462759, 6127143000822511824, 6247595783004300435,
         6538518445609528857, 6896728072888269877, 7042490559673111571,
         7797198894572665496, 7929344577293117573, 7953224965042879744,
         7972399940972724227, 7986080139156520841, 8024168897009383804,
         8496115956499380773, 8534953934598353778, 8562337432838158120,
         8630779688559053755, 8738379673480272412, 8936361841694130292,
         8961445256133042831, 9050699752745546434, 9092917754555593883,
         9141148046934586086, 9201142433737462276});
    instance.streams.push_back(
        {978079929146482953,
         1409525660235222906,
         1509503421641846840,
         1731861974956539197,
         2287095917157287151,
         2385684395183412530,
         2877469094727427554,
         2877927961277983997,
         3309349110943104743,
         4453367091600589556,
         5445630170469231733,
         5578150355180782693,
         5965388825909305963,
         7337522975923020524,
         7397754781949302590,
         7445025070938323285,
         8373329521362769317,
         8912516122168670635});
    instance.streams.push_back(
        {115099052366245073,  348810044115197638,  454297756558378945,
         525963111296184808,  593512962558545249,  798491669620362826,
         869327439478613518,  981097126473349404,  1002101216421266929,
         1034813295165320003, 1264393434589177395, 1345021521632827357,
         1350700929957070172, 1580333759835292921, 2012016518513982623,
         2391834124653836594, 2498010035820125293, 2899557215378165650,
         3159601364147854417, 3340910529178310262, 3383200101416954525,
         3595715391364568339, 3626747055096018491, 3745851203974667752,
         3815940356287831934, 3852280379023155391, 4075227241719476687,
         4277592629799541448, 4689266042484411530, 4808153135201443937,
         4906570846257116542, 5179952845682833086, 5182478464701929134,
         5292145640151081163, 5437316131059031693, 5541408742880580812,
         5682887433002060720, 5714191344223292326, 5731579309052433765,
         5865851265832527092, 6188296807941703853, 6308053424554918208,
         6453124965166334493, 6521001068166938079, 6648365436846851505,
         6686617273303982256, 6710657999109138778, 6742339195490840062,
         6821214221249683902, 6842753050238529660, 6944981180425698867,
         7040942354944121390, 7406993941394416541, 7495074592260989740,
         7527885580667786327, 8021058472116249049, 8303714322765813099,
         8368006548853101370, 8520467236911439331, 8655923628540909373,
         8730767083443288498, 8777309848157974118, 8803674745857201165,
         9120894070780185000, 9125782479443805331});
    instance.streams.push_back(
        {82214946185582147,   638230386769953760,  930974016942623358,
         1225826573214176199, 1381192865341561544, 1909694923570278723,
         2780702356789299978, 3765543648183879203, 4018997926153994916,
         4089453881191544673, 4414382114507979262, 4662817260219833523,
         5168913145368538531, 5507337850840720077, 5747627772310775789,
         5991145201999384747, 6480288725892269305, 6807643059838787541,
         7597697085082207038, 7962815082597967978, 7969930733722087093,
         8001999050896731948, 8657206603771597168, 8750184224838691451,
         8761404347128873411, 9069817638306973173});
    instance.streams.push_back(
        {487921498259078381,  679071658617431230,  764374888490611317,
         925238360244807201,  1157497519003408600, 1188010947874881034,
         1294245316709222942, 1308759129017077937, 1456021713368322450,
         1562145014347624567, 1665773474303121608, 1830853550932232765,
         1984890043982131546, 2027238322486055348, 2028467493513280808,
         2048654823136184085, 2050731743184357900, 2051006077165771338,
         2122605183153034700, 2164916519025182090, 2306688405259976416,
         2569911000316990930, 2707215687757190194, 2798872767746140499,
         3039005933784495926, 3155860803036790280, 3287334551636549192,
         3460037407701804509, 3480514362615843109, 3529761578628417370,
         3605421548746285977, 3668137967771604963, 3854196067176047399,
         4174953448500066916, 4183641199393801913, 4257822442447196266,
         4517889586519195136, 4545792098053641234, 4713067602976952130,
         4974919041423989357, 5200537300214769139, 5346144590813326687,
         5348593616115520032, 5450244594874073013, 5534173425947140314,
         5691231366916640932, 5854564983197580789, 6033804493935938541,
         6050661574631158637, 6338616026487638908, 6468369487438837308,
         6718920907523993661, 7494540413174816428, 7700271121561213222,
         7747996660032220767, 8153855686220060373, 8301835542127523736,
         8310814362457213780, 8357421137243286428, 8421758511454956292,
         8488672609098859079, 8601102709344884123, 8717181881737517673,
         8863705693387701392, 8949916871602876731});
    instance.streams.push_back(
        {629323900936271853,
         1137842357866871600,
         2544291276218199502,
         2548577492105538193,
         4478450303133459836,
         5759012633820715055,
         5841580894289216512,
         6595012440364481517,
         7748533340176401956,
         7823400066738152787,
         7836255795681407155,
         8164837487421063835,
         8331526455846075045,
         8720314151969493218,
         8922460715960042588,
         8984160442812207787});
    instance.streams.push_back(
        {285705971133895684,  1137948778300781933, 1456321730948168173,
         1701005447601122263, 1779219982736449125, 1901064185213667642,
         2323311467631549246, 3062745247368715424, 3255723597016881970,
         3620694899140096178, 3669875189360761436, 3820970890922102074,
         4421532518240930469, 4668525690843200017, 5059454289865985611,
         5138688983424923464, 5251311098633968937, 5272795211633015455,
         5429117732141768878, 5594384885252184809, 5736762777112863449,
         6375704680289738215, 6464379414503375352, 6595935814828333306,
         6883681170390130206, 6939426714908848607, 7195435632323483879,
         7393834961036793238, 7486339410612782998, 7822901423881124560,
         8205963132292198176, 8461200943179660674, 8784667488105527035,
         8795332699575539093, 9062848554475650570});
    instance.streams.push_back(
        {649342514129014019,  1013609001391672817, 1552770829407841903,
         1706074387936052883, 2548683352257880495, 2780139330698936149,
         2951212375452138462, 3870386150608349786, 3960748984116259407,
         4515559401023148294, 4826228728631893094, 4828509659393215759,
         4836725078690018234, 5656385505541132678, 6107120255981371338,
         6586396852017701806, 6916523393735689456, 6917559196143595205,
         7077742276716875042, 7177046026795199725, 7339158529155971675,
         7856953593588045735});
    instance.streams.push_back(
        {103312494178528482,  234405838252514844,  330032410803652666,
         360939175401072749,  519867640403597499,  593307860016959663,
         623778226224542360,  640643113234747207,  861311118685843861,
         1214613703731224172, 1529705270541373318, 2793264392155230386,
         3103915778589791456, 3146440560008244613, 3257465928619382554,
         3282715791883507152, 3323036804885755637, 4222176784995031174,
         4289430572947015273, 4321162610100759218, 4589140594212039846,
         4645145067993788922, 4816731277388022047, 4837005048398405969,
         5090487625726538849, 5298654518040793720, 5629080880414982651,
         6271444793144846681, 6524487041982597712, 7152771782154854398,
         7479111365116323952, 7669307042553249175, 7835952280336708833,
         8012716915770825646, 8016767728769763250, 8647290126259516347,
         8908070696861763158});
    instance.streams.push_back(
        {468137721914325517,  505117020292705358,  617453048083099567,
         722432666278755340,  743944767710608524,  817337702921561702,
         964298007710047270,  1085788254735572558, 1229163532786185187,
         1267904705291553727, 1335338329848030023, 1456849486329528319,
         1458456523464408631, 1522873537225983581, 1690285984647312583,
         2279670235761762162, 2338619718483254870, 2393649659784264625,
         2494691992724655834, 2562853983115884052, 2631662446792181160,
         2671788079680194427, 2750913865345085626, 2777943866185224959,
         2819109626164132074, 2878115195039336529, 2927533075635477881,
         3074465162329395288, 3207616553987777400, 3436178864499235290,
         3687937503069163511, 3790031988860279331, 3808430880928299164,
         3972622864508455896, 3987422619560181033, 4304895466987371555,
         4591838038828380747, 4603042037887037413, 4770973830065599712,
         4830870656411977203, 5064320335183742324, 5068835356504398856,
         5247762342994044306, 5388649844889336005, 5430693440992736553,
         5857226827051826792, 5947841653468176431, 5971499925210093244,
         6474956286328840336, 6479747409942328448, 6594678433421674820,
         6648975724640599917, 6743815068136504683, 7164603377895064448,
         7176624776583216008, 7193972700885868440, 7267424656938049115,
         7755322816414385148, 7761365782584823534, 8208479161312198501,
         8231630458641300420, 8331053690100244654, 8534276101518867142,
         8542420471227393797, 8579650596911953516});
    instance.streams.push_back(
        {92056098981300951,   183191604288414135,  425609311770953489,
         733399501951125206,  815017252624361671,  1174214440473219376,
         1527447727224758418, 1604558772613236346, 1815102969572261701,
         2020023237241195659, 2133221346465028689, 2309573638621438843,
         3059647754764119277, 3089018963508080823, 3520610057292262122,
         3706959419050779691, 4088044684824043707, 4266682710981069621,
         4367141727169343278, 4405220041098668848, 4572714270240917773,
         4582869992764212527, 4730057491341003468, 4863235522401558598,
         4960168984954372159, 5213732910162448626, 5265577214356543708,
         5579206437592754639, 5886198738755368709, 6057211466533567402,
         6272910612650927157, 6817377680671947413, 6819793996159518888,
         6835624039613756154, 7130584319880415797, 7563998286582581021,
         8048710272435454281, 8106671345277378212, 8200067328726125681,
         8397152345872926563, 8416609622231974393, 8432816603155051087,
         8546110210248055888, 8559128773431956735, 9040583859392571998,
         9079747481631520099, 9154620002255343025});
    instance.streams.push_back(
        {186460269856899145,  312877223343858913,  392010051167932117,
         592734175915682324,  638573601945881452,  674255518590737590,
         688880322638418820,  1013857353576768172, 1112411780679572365,
         1139883811869746750, 1190575836840241598, 1207982001775715382,
         1211552828881387578, 1233350427106076327, 1385471799097662144,
         1531490738187958505, 1935384258720069435, 2071863720326007106,
         2289216337643919462, 2408743966916292164, 2454481366897894733,
         2814363792059893654, 2826202475368046979, 2833923842432602204,
         2975442383331736876, 3208835367202654229, 3292645341676952881,
         3305481577550342988, 3584241433084694833, 3743273293915887787,
         3879826256137027085, 4495397115498651467, 4589766514545949658,
         4645502849012582421, 4698623771475978677, 4921845594641293690,
         4973700052797881949, 5054012183269618598, 5121747673560119933,
         5155381082608426532, 5169820373996821774, 5193501081967012768,
         5300401153239731147, 5312160546165835590, 5366563483338550359,
         5407723435082400908, 5479116058910514884, 5852869581120320600,
         6056660948569423595, 6059285926721535698, 6066133154606264656,
         6150528155680608358, 6205405588570672356, 6455658007269102337,
         6517690050619506041, 6604837358100922800, 6740391577115078550,
         6754870494384726974, 6782744164019537637, 6858670266450659431,
         7031477863821497751, 7090103973012968883, 7125261418080735528,
         7333138589807036720, 7350755014925499557, 7353877654437638031,
         7400869418000115287, 7543529844050914728, 7603067751752487016,
         7799970764974260643, 7827063332152321532, 7975289673723089483,
         8090629125471632636, 8251074770046802788, 8328759754833931749,
         8685070722362163095, 8866312412480244316, 8866825913959048686,
         8917266639179383883, 8951106491369592720, 9006103287160623971,
         9081097821549706483, 9139726052133853214, 9156509614091645994,
         9211010153690967898});
    instance.streams.push_back(
        {104230526944818390,  194872867561694437,  212441359160354232,
         321012325665976628,  431120611506335132,  533888981277404640,
         993196096130371670,  1030830090146476659, 1092324484284608889,
         1385143609848204220, 1447957761663850140, 1883972771473232369,
         1886132719335527511, 2041392723108802553, 2593917574312472347,
         2937372744931372239, 2996487507555474375, 3670702336583709390,
         3878940726871043993, 4157152156857244222, 4960850397334813825,
         5113920412938398589, 5169812923568587478, 5285613458152029336,
         5559283782542258439, 6157831502356165880, 6566627599020312553,
         7006145820121144150, 7225573382617770716, 7287615231139773891,
         7303873576606859546, 7317271246953513712, 7349671905717534681,
         7421533849456513697, 7526668599944036583, 7599086076101535523,
         7623492095781433338, 7711283192366365947, 7949095350580553873,
         8228783759112069242, 8295567568192555838, 8534365345714775019,
         8758541163334201549, 8821924156918821279, 8883120207037353129});
    instance.streams.push_back(
        {58386620028953901,   603149038174852283,  784733004041771519,
         849256375728271061,  876434749911498209,  1153277607489026889,
         1284332405460994971, 1544280345202798527, 1569222782641349918,
         1605199731050122714, 1749308567067587821, 1863672956468109621,
         1926342313223625877, 2017927708422559327, 2027659154571342628,
         2117877383956303269, 2147965921916703578, 2233730766407072802,
         2286207197542499020, 2475529216448935277, 2537071554667498648,
         2561933176400165365, 2582706607166598310, 2629988231088809001,
         2668258253087373077, 2740522624473634895, 2750386422056636885,
         2772828149547986246, 2867225468300520004, 2895603989194672142,
         2916624250131739096, 3001758350138792451, 3023960594088693992,
         3103897686160767364, 3241265739486382608, 3272279529480798961,
         3519715009004256471, 3579844164298648535, 3659500891570190670,
         3829842546426485320, 3960671906396409644, 4140246018246094004,
         4420865993105853873, 4508843839749714898, 4523034759866345601,
         4957603909945162337, 5227269089039875466, 5336771787899565333,
         5345440780239373841, 5350973736703669852, 5510225308627802955,
         5695942359188224540, 5809619522614336735, 6728065708154526109,
         6829383044318697405, 6946524976100950664, 7153006052997047142,
         7167920757371519098, 7332662781768146263, 7373793185529622940,
         7447826569645645832, 7490493972617251142, 7831303308378876856,
         7843091442338072549, 7906935755119598122, 7914194807915183145,
         8039495272051275925, 8314689053957053388, 8387542429457488819,
         8466597753541104284, 8483741723290222858, 8599301570471318808,
         8716226300070556585, 8756763689271138139, 8955295406776997764,
         9012673292027226953, 9036242292240002258});
    instance.streams.push_back(
        {221152499512725057,  468653913289994511,  573406392190002398,
         608549633585505280,  721590223379320754,  893804687502387698,
         994645852819618858,  1010504207871866436, 1226458553438834225,
         1322102128875218366, 1352617939551229058, 1367657169503026665,
         1431054573484265696, 1716664507247859085, 1819808470744051546,
         1832883987492520423, 1913575203897912572, 2078343662767049992,
         2144184772162852472, 2175338692854700871, 2515520083286899021,
         2658219409155723888, 2662216173057158227, 2791102212542615133,
         2810251461404023989, 3010160067457807675, 3198247003463721391,
         3207210396276830453, 3655687150569838438, 3742949808956029213,
         3939408096155747775, 3960751161684758408, 4062454131612436236,
         4126733056338249731, 4144406660403408900, 4160281903605117507,
         4160886814405501932, 4301845892945418166, 4335382680745413153,
         4505056660537898379, 4539434107911381923, 4559707416640177661,
         4581627552336714482, 4634932691215475440, 4652256940502250041,
         4709034174943716141, 4755067195445668372, 5031870437171497866,
         5086241845842489323, 5194885361498644327, 5335820239968792638,
         5381866204428555333, 5496620869292551625, 5607696027294180101,
         5641888110506579814, 5669980928511650063, 5692882051604805422,
         5869033276887872018, 5921828484003683005, 6018591452797071010,
         6043330380531903780, 6073539888671339222, 6349804831539426958,
         6466951481745709342, 6541925016453000741, 6606577194341690198,
         6655821998787366041, 6869403876421968685, 6952236217247677418,
         7064632969560758954, 7262799951330799140, 7299731795147493418,
         7452516877284055758, 7516278093926782953, 7724657529939371422,
         8277537304437407399, 8287042823318847978, 8375033050716909821,
         8383421555375425502, 8502394728509959035, 8502811416815954103,
         8601942285631187625, 8668259048921332853, 8679917789291195612,
         8902185247016931860, 8975374683294029673, 9018123807247419003,
         9081591481240593402, 9203064079702243357});
    instance.streams.push_back(
        {9528858636270754,    74767308924267190,   92523084385092125,
         282510914521185986,  565939653640943279,  766893259352706773,
         1025334812935058435, 1086642043696211947, 1090103539577005545,
         1117559866996102610, 1150892608993861532, 1431520706401660610,
         1461925516829895752, 1491154270719434347, 1564570908804651001,
         1686708764851230308, 1693018175221748741, 1827964802829643811,
         1871856400612904987, 2269031422942301096, 2321184329693335268,
         2576411078823723038, 2901956572209495751, 3299554731045776001,
         3578582730292621865, 3605481165030718200, 3622286981235981834,
         4644544455473625042, 4704715844976049230, 4707408952461877611,
         5079828316722713851, 5332860656232543172, 5357945905588425444,
         5462910895481110403, 5486651617427880829, 5920963317972710651,
         5945547189608426266, 6097995450464204286, 6155072814328938262,
         6427618809519937314, 6498736386875881011, 6557345715154802672,
         6591361732789478133, 6795736613838753671, 7063310025635337958,
         7288379786078052405, 7477826447866021589, 7544407848018322037,
         7864854448541658478, 7924809231654480847, 8155610344096072120,
         8168318381948177277, 8489138127444983219, 8533218767481812480,
         8899267526102130921, 9051763646967677194});
    instance.streams.push_back(
        {1160239440999495868, 1160627100977908024, 1445977391255418495,
         2100709699684953613, 2311002752371175131, 2326987246415777165,
         2409509260037050400, 2770408159376628543, 2885970397539782206,
         2897849329158521471, 3073323606059489201, 3097044942437084142,
         3169575954896219039, 3319951716499728030, 3494135475371065426,
         3496002297262349575, 3841375314084544552, 4007888085756092075,
         4717250350765676101, 4830685926850246039, 5066619425573186987,
         5142827563397788282, 5840091769545424048, 6185402624342137099,
         6520242562806050579, 7340354838898445609, 7446287944723287024,
         7819425906479635150, 8179247966270686645, 8793860582000764117,
         8876989220535143815, 8954309083312314973, 9189126043219875235});
    instance.streams.push_back(
        {285145543975647006,  400464406369380005,  425052447908362043,
         558717929621862200,  780184890604101452,  802552417514008088,
         821353789735020113,  1461771191595340049, 1707838069528986431,
         1968758736472278720, 2179970727013701927, 2301096876229371288,
         2586755882369725036, 2642888823133285734, 2724932478710618595,
         2799219108908357183, 3210508660878406295, 3390360820647531457,
         3500608069640623603, 3794000156360803026, 3870372465245409129,
         3962864942943613290, 4052594741754433954, 4066439209995535105,
         4625364126734950672, 4755100478414716611, 4837056974522792783,
         5138435736439382420, 5252516194543331360, 5276885357297734928,
         5339285312869309795, 5383414072988834799, 5419750407305078642,
         5504475032307800929, 5531444670009604260, 5610198686052584931,
         5879342333105160359, 6227163917223061198, 6394549805539869304,
         6534105811771329013, 6538298296181075612, 6683665059254528394,
         6915570322469724208, 7215054837440533904, 7281318269792757267,
         7374371223175351465, 7562113046836517303, 7838864936942600980,
         7844181398943447729, 7942351656727780460, 7950201023600342051,
         8016629451324720863, 8044191201507598865, 8229239515369106446,
         8993425554747953330});
    instance.streams.push_back(
        {125583000667520524,  214839438467369427,  249367830951934430,
         308261031714688831,  1134768329775635958, 1183613722982518046,
         1357058262944026234, 1357710861438500066, 1748413022866802117,
         1857598835930134266, 3055019199902384006, 3062281030982283629,
         3528678498241652912, 4063396096182487012, 4467122486929931357,
         4846856950656598934, 5785035888379523440, 5855646996685670595,
         6095919523788855289, 6205455101336312663, 6666077315644367652,
         6757656301020430328, 7275449995231256065, 7306530842416062343,
         7409626532873823691, 7534989846036976651, 7970400668449759273,
         8069921964831893849, 8196425277077193920, 8389499930778050745,
         8701072344837825080});
    instance.streams.push_back(
        {33564926325826831,
         260291524539990308,
         374076536813175702,
         445706191910983060,
         2615929502664660778,
         2870926401217071659,
         3088279460307954084,
         4633051241451350592,
         4636848118939709473,
         4982408764483706312,
         6145844808415620138,
         6503848779295974662,
         6803286364456511128,
         7301808229090216723,
         7527961052034746578,
         8755659592883848787});
    instance.streams.push_back(
        {50391867020209568,   109973211041671007,  426365914246397281,
         780216749836386995,  927137594156658301,  1280297890312031835,
         1356872469290895499, 1646520938759084189, 1770128407464172181,
         1876106980342957607, 1992958432625854788, 2093272732169720418,
         2241800693961349596, 2315318181472203645, 2726778221200855105,
         2731148968788143056, 2784451205304767311, 2889302029852103008,
         2914456568933696331, 3033308498900870284, 3069608453528247024,
         3147683791728929502, 3162396888809983919, 3173374637506992417,
         3306363691517815071, 3567685134602033707, 3732317047444193546,
         3864954392412328092, 4304660060986936816, 4666673387047506586,
         4765432491107097576, 5108976781632236584, 5110400105803816066,
         5200335758573175396, 5640989876800271598, 5665274647100247796,
         5702149274760003174, 5728664901759643253, 5788098188481174646,
         5795560782524298146, 5852686300889056461, 5973152740219653583,
         5977429576954595040, 6103223664281897127, 6111609016377058074,
         6235168972471541048, 6237552891612236200, 6335487740557681471,
         6353432702630052474, 6365163378435576774, 6779431742606687646,
         7055429296271398220, 7142092728874400757, 7160088139694496237,
         7161080095306591569, 7195482977425683059, 7265328107229114597,
         7323761413577848136, 7330533757178675459, 7346545798057982568,
         7543397985293350879, 7605471257315203444, 7677075232577432180,
         7770682335946669870, 7801171200783882642, 7879542219695847235,
         7889434293679595443, 8027893157968605900, 8131696298458998640,
         8221580404217896279, 8287527045961328812, 8340326440093299701,
         8355257415634238153, 8638160453384811767, 8654116231443860953,
         8713028169329039008, 8741251814621908606});
    instance.streams.push_back(
        {38552260850125970,   306379511766359735,  711300096798339321,
         892018019103254274,  2364206832643103382, 2420535690560486819,
         2980386295304646523, 3101600218221266377, 3141790607746941505,
         3999134727658658069, 4462290689715673199, 5204914504379723995,
         5467769513963022713, 5963515685465164797, 6307703548487249061,
         6521727204313461172, 6746273471781072806, 7133296936471547422,
         7413405836526190068, 7460132468170665268, 9118746925568133696});
    instance.streams.push_back(
        {311746355635346537,  357280282242159933,  453794801155624312,
         470124658103437043,  480500591405735777,  642968216130651766,
         677382766653156986,  742336241959437289,  995597266969363512,
         1438905128019740072, 1774939855749446509, 2534734553076187949,
         2675412948582352200, 2945299999579094336, 3085388497824334414,
         3244765152133405099, 3327833825330566369, 3703194822023351032,
         3765658082600741596, 3825281533385903512, 3883149142639520189,
         3938352829216712487, 4120683798246541427, 4407478830753418084,
         4714722467734011191, 4778090814542958798, 4865684247595699604,
         5564250447415400601, 5675380593702259207, 5953049262426054107,
         6322306904987766773, 6576158849963679983, 6585910776451113262,
         6695013865519715556, 6822580595787479730, 7005579721591160602,
         7053187268953133165, 7137337825055068764, 7154614606682799364,
         7162695738484944585, 7204207140983511889, 7657343589727357634,
         7726203520276412700, 7885282374693638275, 7898080252964135291,
         8063562120530344324, 8111621434828014272, 8136473663541304080,
         8955369150293893931, 8974612926096998142});
    instance.streams.push_back(
        {197420451713811676,  281445277459917203,  740082088833530644,
         1290412288467608392, 1294985646752861418, 1459338324889185197,
         1545467342772798788, 1725296824044696622, 1790928042402142285,
         1819195369020482914, 1845318846420546596, 1916804860036750684,
         1934795655439316734, 2123406663108189928, 2161364767387674755,
         2199349719437568153, 2201446205661462318, 2304422276794393017,
         2367362048086896201, 2520840158493853225, 2637488475771115834,
         2695468587025769956, 2868497673804512236, 2986730852667904426,
         3049027770127401337, 3088814443181055397, 3142185799974885998,
         3145610541985422508, 3376917417266132278, 3474311476906199144,
         3480295733245426278, 3756461875941152626, 3768960023228980524,
         3786412095536719213, 3818558100696073513, 3832731138330958677,
         4013125545855280982, 4058451558097473690, 4061654997981645019,
         4079781890597804187, 4145540956726304260, 4213067535490157135,
         4387646299216426469, 4532586244009288120, 4599368551837569974,
         4719137179319807721, 4774240273842121853, 4823692999474104868,
         4878380451678305609, 5080805857519288366, 5272561874891052360,
         5393458881428272808, 5758340275080427297, 5886639535186242096,
         5912173887580571910, 6054663179080587357, 6138520848359196864,
         6388603114548809966, 6409577084921583203, 6536152899730873147,
         6594754796754003142, 6835272684932377793, 6860056536556710109,
         7219012018184660258, 7316465780676346496, 7661428841823577249,
         7906168207960936451, 8184686816308280852, 8916752945036324140,
         8985070033502476478, 9145582347483890099});
    instance.streams.push_back(
        {153324836101059419,  158780138759790326,  174215935958616755,
         179731076471247779,  269276934678469319,  393639793694666172,
         432631673890631004,  670995085036662336,  721887896591779145,
         748122147291880857,  771326157253533848,  783650624759832650,
         1014975687794962171, 1379488317060232881, 1463947847163713623,
         1559978541312388344, 1677050512347768440, 1679788471700081953,
         1697344722681459414, 1796670363905560694, 1875437320199800047,
         1958046921272092141, 2012390301428218497, 2032697606825981501,
         2063860587455918843, 2076755491339447914, 2163553294232027512,
         2185286087988362175, 2223845822471630205, 2508102531103681824,
         2576117223285183167, 2714809954419134452, 2792838922831161963,
         2900435853024427713, 2937083093165162409, 2939480550795499937,
         3072243433185798666, 3228964562507837847, 3257430266232071473,
         3401500904443689432, 3410334239187579089, 3620509784201584781,
         3707729414802869989, 3750258169852724918, 3756680820327526039,
         3900685920024254630, 4024598042066710501, 4188274758129042402,
         4323638211698224578, 4580570891550236607, 5009980347824435138,
         5122166740162881305, 5329562660783230510, 5372442803515899785,
         5418173101818824583, 5485499651120889655, 5521202335275584192,
         5729929930779710255, 5831082418654309238, 5982740560563910312,
         5995159670704615124, 6088662697234208829, 6120996226448363060,
         6214200379979203888, 6468448109110284451, 6525029247710148615,
         6581357662703674942, 6780008824250326220, 6875650962481044987,
         6938461292439554984, 7027602581413514955, 7100073000167249186,
         7208005517459406600, 7228453795929202340, 7603315537325918661,
         7611050507858817021, 7644795789683514694, 7868219793368483911,
         7966758117976278992, 8011601341464333596, 8077164278247408893,
         8181466141626535480, 8771155570066914574, 8879865897468670264,
         8885481783436167324, 9025760847000160788, 9164430375972757624,
         9173963918183109716});
    instance.streams.push_back(
        {1059433874806495660, 2105370760666799235, 2550457278644318367,
         2588733214791715635, 3081056091507702768, 3536661695747334932,
         3642559614667528631, 3844174653456592779, 3874828053345872276,
         4259604692437009810, 4395405895302928283, 4805831959048227379,
         5035667734207638846, 5089440685667631502, 5170000450239185361,
         5544572753180138321, 5748328650770747767, 6212120819500360922,
         6812019836498165705, 6962001610179228000, 7132285316145578530,
         7511954185336675079, 7912817484509858870, 8708158977779035254,
         8907676047014618471, 8913855313747888928});

    instance.verify();
  }

  SECTION("Rapidcheck") {
    rc::prop("verify_parallel_merge", [](VerifyParallelMerge<uint64_t> input) {
      input.verify();
    });
  }
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

    const std::vector<uint64_t> expect = {24,  100, 123, 135, 200, 246, 300,
                                          357, 400, 456, 468, 500, 579, 680,
                                          789, 791, 802, 890, 901, 913};

    auto future = parallel_merge(
        pool, resources, options, EXAMPLE_STREAMS, cmp, &output[0]);

    future->block();

    CHECK(output.size() == expect.size());
    CHECK(output == expect);
  }

  SECTION("greater") {
    auto cmp = std::greater<uint64_t>{};

    auto descending = EXAMPLE_STREAMS;
    for (auto& stream : descending) {
      std::sort(stream.begin(), stream.end(), std::greater<>());
    }

    const std::vector<uint64_t> expect = {913, 901, 890, 802, 791, 789, 680,
                                          579, 500, 468, 456, 400, 357, 300,
                                          246, 200, 135, 123, 100, 24};

    auto future =
        parallel_merge(pool, resources, options, descending, cmp, &output[0]);

    future->block();

    CHECK(output.size() == expect.size());
    CHECK(output == expect);
  }
}

TEST_CASE("parallel merge assert", "[algorithm][parallel_merge]") {
  iassert(false, "Oops");
}
