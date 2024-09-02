/**
 * @file bench_aggregators.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * Benchmarks the `AggregateWithCount` class.
 */

#include <random>

#include "tiledb/common/common.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_with_count.h"
#include "tiledb/sm/query/readers/aggregators/min_max.h"
#include "tiledb/sm/query/readers/aggregators/safe_sum.h"
#include "tiledb/sm/query/readers/aggregators/sum_type.h"
#include "tiledb/sm/query/readers/aggregators/validity_policies.h"

#include <test/support/src/helper_type.h>
#include <test/support/tdb_catch.h>

using namespace tiledb::sm;
using namespace tiledb::test;

const uint64_t num_cells = 10 * 1024 * 1024;

// Fixed seed for determinism.
static std::vector<uint64_t> generator_seed_arr = {
    0xBE08D299, 0x4E996D11, 0x402A1E10, 0x95379958, 0x22101AA9};

static std::atomic<uint64_t> generator_seed = 0;
std::once_flag once_flag;
static std::mt19937_64 generator;

void set_generator_seed() {
  // Set the global seed only once
  std::call_once(once_flag, []() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<size_t> dis(
        0, generator_seed_arr.size() - 1);
    generator_seed = generator_seed_arr[dis(gen)];
    std::string gen_seed_str =
        "Generator seed: " + std::to_string(generator_seed);
    puts(gen_seed_str.c_str());
  });
}

template <typename T>
T random(size_t max) {
  // Pick generator seed at random.
  set_generator_seed();

  thread_local static std::uniform_int_distribution<size_t> distribution(
      0, max);
  return static_cast<T>(distribution(generator));
}

template <typename T>
struct fixed_type_data {
  using type = T;
  typedef T fixed_type;
};

template <>
struct fixed_type_data<std::string> {
  using type = std::string;
  typedef uint64_t fixed_type;
};

template <typename T>
tuple<
    std::vector<typename fixed_type_data<T>::fixed_type>*,
    std::string*,
    std::vector<uint8_t>*,
    std::vector<uint8_t>*>
get_data() {
  static std::vector<typename fixed_type_data<T>::fixed_type> fixed_data;
  static std::vector<uint8_t> validity_data;
  static std::string var_data;
  static std::vector<uint8_t> bitmap;
  static bool data_generated = false;
  if (!data_generated) {
    fixed_data.resize(num_cells);
    validity_data.resize(num_cells);
    bitmap.resize(num_cells);

    uint64_t offset = 0;
    for (uint64_t c = 0; c < num_cells; c++) {
      if constexpr (std::is_same<T, std::string>::value) {
        uint64_t size = random<uint64_t>(20);
        std::string value;
        for (uint64_t s = 0; s < size; s++) {
          var_data += '0' + random<char>(36);
        }

        fixed_data[c] = offset;
        offset += size;
      } else {
        fixed_data[c] = random<T>(200);
      }

      validity_data[c] = random<uint8_t>(1);
      bitmap[c] = random<uint8_t>(1);
    }

    data_generated = true;
  }

  return {&fixed_data, &var_data, &validity_data, &bitmap};
}

template <
    typename T,
    typename AggregateT,
    typename PolicyT,
    typename ValidityPolicyT>
void run_bench() {
  const bool var_sized = std::is_same<T, std::string>::value;
  bool nullable = GENERATE(true, false);
  bool use_bitmap = GENERATE(true, false);
  bool segmented = GENERATE(true, false);
  const uint64_t increment = segmented ? 4 : num_cells;

  // Get data.
  auto&& [fd, vd, vald, b] = get_data<T>();
  auto fixed_data = fd;
  auto var_data = vd;
  auto validity_data = vald;
  auto bitmap = b;

  DYNAMIC_SECTION(
      "Var sized: " << (var_sized ? "true" : "false")
                    << ", Nullable: " << (nullable ? "true" : "false")
                    << ", Use bitmap: " << (use_bitmap ? "true" : "false")
                    << ", Segmented: " << (segmented ? "true" : "false")) {
    BENCHMARK("Bench") {
      AggregateWithCount<T, AggregateT, PolicyT, ValidityPolicyT> aggregator(
          FieldInfo("a1", var_sized, nullable, 1, tdb_type<T>));
      for (uint64_t s = 0; s < num_cells; s += increment) {
        AggregateBuffer input_data{
            s,
            std::min(s + increment, num_cells),
            fixed_data->data(),
            var_sized ? std::make_optional(var_data->data()) : nullopt,
            nullable ? std::make_optional(validity_data->data()) : nullopt,
            false,
            use_bitmap ? std::make_optional(bitmap->data()) : nullopt,
            0};
        aggregator.template aggregate<uint8_t>(input_data);
      }
    };
  }
}

TEST_CASE("Aggregate with count: sum", "[benchmark][sum][.hide]") {
  typedef uint64_t T;
  run_bench<T, typename sum_type_data<T>::sum_type, SafeSum, NonNull>();
}

typedef tuple<uint64_t, std::string> MaxTypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Aggregate with count: max", "[benchmark][max][.hide]", MaxTypesUnderTest) {
  typedef TestType T;
  run_bench<
      T,
      typename type_data<T>::value_type,
      MinMax<std::greater<typename type_data<T>::value_type>>,
      NonNull>();
}
