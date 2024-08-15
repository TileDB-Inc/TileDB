/**
 * @file unit_xor_pipeline.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This set of unit tests checks running the filter pipeline with the
 * xor filter combined with other filters.
 *
 */

#include <test/support/src/mem_helpers.h>
#include <test/support/tdb_catch.h>
#include <random>

#include "../bit_width_reduction_filter.h"
#include "../bitshuffle_filter.h"
#include "../byteshuffle_filter.h"
#include "../compression_filter.h"
#include "../float_scaling_filter.h"
#include "../positive_delta_filter.h"
#include "../xor_filter.h"
#include "filter_test_support.h"
#include "tiledb/sm/enums/filter_option.h"

using namespace tiledb::sm;

template <typename FloatingType, typename IntType>
void testing_float_scaling_filter() {
  tiledb::sm::Config config;

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(FloatingType);
  const uint64_t cell_size = sizeof(FloatingType);

  Datatype t = Datatype::FLOAT32;
  switch (sizeof(FloatingType)) {
    case 4: {
      t = Datatype::FLOAT32;
    } break;
    case 8: {
      t = Datatype::FLOAT64;
    } break;
    default: {
      INFO(
          "testing_float_scaling_filter: passed floating type with size of not "
          "4 bytes or 8 bytes.");
      CHECK(false);
    }
  }

  auto tile = make_shared<WriterTile>(
      HERE(), constants::format_version, t, cell_size, tile_size, tracker);

  std::vector<FloatingType> float_result_vec;
  double scale = 2.53;
  double foffset = 0.31589;
  uint64_t byte_width = sizeof(IntType);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<FloatingType> dis(0.0, 213.0);

  for (uint64_t i = 0; i < nelts; i++) {
    FloatingType f = dis(gen);
    CHECK_NOTHROW(
        tile->write(&f, i * sizeof(FloatingType), sizeof(FloatingType)));

    IntType val = static_cast<IntType>(round(
        (f - static_cast<FloatingType>(foffset)) /
        static_cast<FloatingType>(scale)));

    FloatingType val_float = static_cast<FloatingType>(
        scale * static_cast<FloatingType>(val) + foffset);
    float_result_vec.push_back(val_float);
  }

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(FloatScalingFilter(t));
  CHECK(pipeline.get_filter<FloatScalingFilter>()
            ->set_option(FilterOption::SCALE_FLOAT_BYTEWIDTH, &byte_width)
            .ok());
  CHECK(pipeline.get_filter<FloatScalingFilter>()
            ->set_option(FilterOption::SCALE_FLOAT_FACTOR, &scale)
            .ok());
  CHECK(pipeline.get_filter<FloatScalingFilter>()
            ->set_option(FilterOption::SCALE_FLOAT_OFFSET, &foffset)
            .ok());

  pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp);

  // Check new size and number of chunks
  CHECK(tile->size() == 0);
  CHECK(tile->filtered_buffer().size() != 0);

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
  run_reverse(config, tp, unfiltered_tile, pipeline);
  for (uint64_t i = 0; i < nelts; i++) {
    FloatingType elt = 0.0f;
    CHECK_NOTHROW(unfiltered_tile.read(
        &elt, i * sizeof(FloatingType), sizeof(FloatingType)));
    CHECK(elt == float_result_vec[i]);
  }
}

TEMPLATE_TEST_CASE(
    "Filter: Test float scaling",
    "[filter][float-scaling]",
    int8_t,
    int16_t,
    int32_t,
    int64_t) {
  testing_float_scaling_filter<float, TestType>();
  testing_float_scaling_filter<double, TestType>();
}

/*
 Defining distribution types to pass into the testing_xor_filter function.
 */
typedef typename std::uniform_int_distribution<int64_t> IntDistribution;
typedef typename std::uniform_real_distribution<double> FloatDistribution;

template <typename T, typename Distribution = IntDistribution>
void testing_xor_filter(Datatype t) {
  tiledb::sm::Config config;

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(T);
  const uint64_t cell_size = sizeof(T);

  auto tile = make_shared<WriterTile>(
      HERE(), constants::format_version, t, cell_size, tile_size, tracker);

  // Setting up the random number generator for the XOR filter testing.
  std::mt19937_64 gen(0x57A672DE);
  Distribution dis(
      std::numeric_limits<T>::min(), std::numeric_limits<T>::max());

  std::vector<T> results;

  for (uint64_t i = 0; i < nelts; i++) {
    T val = static_cast<T>(dis(gen));
    CHECK_NOTHROW(tile->write(&val, i * sizeof(T), sizeof(T)));
    results.push_back(val);
  }

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(XORFilter(t));

  pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp);

  // Check new size and number of chunks
  CHECK(tile->size() == 0);
  CHECK(tile->filtered_buffer().size() != 0);

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
  run_reverse(config, tp, unfiltered_tile, pipeline);
  for (uint64_t i = 0; i < nelts; i++) {
    T elt = 0;
    CHECK_NOTHROW(unfiltered_tile.read(&elt, i * sizeof(T), sizeof(T)));
    CHECK(elt == results[i]);
  }
}

TEST_CASE("Filter: Test XOR", "[filter][xor]") {
  testing_xor_filter<int8_t>(Datatype::INT8);
  testing_xor_filter<uint8_t>(Datatype::UINT8);
  testing_xor_filter<int16_t>(Datatype::INT16);
  testing_xor_filter<uint16_t>(Datatype::UINT16);
  testing_xor_filter<int32_t>(Datatype::INT32);
  testing_xor_filter<uint32_t>(Datatype::UINT32);
  testing_xor_filter<int64_t>(Datatype::INT64);
  testing_xor_filter<uint64_t, std::uniform_int_distribution<uint64_t>>(
      Datatype::UINT64);
  testing_xor_filter<float, FloatDistribution>(Datatype::FLOAT32);
  testing_xor_filter<double, FloatDistribution>(Datatype::FLOAT64);
  testing_xor_filter<char>(Datatype::CHAR);
  testing_xor_filter<int64_t>(Datatype::DATETIME_YEAR);
  testing_xor_filter<int64_t>(Datatype::DATETIME_MONTH);
  testing_xor_filter<int64_t>(Datatype::DATETIME_WEEK);
  testing_xor_filter<int64_t>(Datatype::DATETIME_DAY);
  testing_xor_filter<int64_t>(Datatype::DATETIME_HR);
  testing_xor_filter<int64_t>(Datatype::DATETIME_MIN);
  testing_xor_filter<int64_t>(Datatype::DATETIME_SEC);
  testing_xor_filter<int64_t>(Datatype::DATETIME_MS);
  testing_xor_filter<int64_t>(Datatype::DATETIME_US);
  testing_xor_filter<int64_t>(Datatype::DATETIME_NS);
  testing_xor_filter<int64_t>(Datatype::DATETIME_PS);
  testing_xor_filter<int64_t>(Datatype::DATETIME_FS);
  testing_xor_filter<int64_t>(Datatype::DATETIME_AS);
}

TEST_CASE("Filter: Pipeline filtered output types", "[filter][pipeline]") {
  FilterPipeline pipeline;
  auto tracker = tiledb::test::create_test_memory_tracker();

  SECTION("- DoubleDelta filter reinterprets float->int32") {
    pipeline.add_filter(CompressionFilter(
        tiledb::sm::Compressor::DOUBLE_DELTA,
        0,
        Datatype::FLOAT32,
        Datatype::INT32));
    pipeline.add_filter(BitWidthReductionFilter(Datatype::INT32));
  }

  SECTION("- Delta filter reinterprets float->int32") {
    pipeline.add_filter(CompressionFilter(
        tiledb::sm::Compressor::DELTA, 0, Datatype::FLOAT32, Datatype::INT32));
    pipeline.add_filter(BitWidthReductionFilter(Datatype::INT32));
  }

  SECTION("- FloatScale filter converts float->int32") {
    pipeline.add_filter(
        FloatScalingFilter(sizeof(int32_t), 1.0f, 0.0f, Datatype::FLOAT32));
    pipeline.add_filter(PositiveDeltaFilter(Datatype::INT32));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::DELTA, 0, Datatype::INT32));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::BZIP2, 2, Datatype::INT32));
    pipeline.add_filter(BitshuffleFilter(Datatype::INT32));
    pipeline.add_filter(ByteshuffleFilter(Datatype::INT32));
    pipeline.add_filter(BitWidthReductionFilter(Datatype::INT32));
  }

  size_t byte_width = 0;
  SECTION("- XOR filter expected output types") {
    byte_width = GENERATE(
        sizeof(int8_t), sizeof(int16_t), sizeof(int32_t), sizeof(int64_t));
    pipeline.add_filter(
        FloatScalingFilter(byte_width, 1.0f, 0.0f, Datatype::FLOAT32));
    auto byte_width_t =
        pipeline.get_filter<FloatScalingFilter>()->output_datatype(
            Datatype::FLOAT32);
    pipeline.add_filter(XORFilter(byte_width_t));
  }

  SECTION("- XOR filter expected output types large pipeline") {
    byte_width = GENERATE(
        sizeof(int8_t), sizeof(int16_t), sizeof(int32_t), sizeof(int64_t));
    pipeline.add_filter(
        FloatScalingFilter(byte_width, 1.0f, 0.0f, Datatype::FLOAT32));
    auto byte_width_t =
        pipeline.get_filter<FloatScalingFilter>()->output_datatype(
            Datatype::FLOAT32);
    pipeline.add_filter(PositiveDeltaFilter(byte_width_t));
    pipeline.add_filter(BitshuffleFilter(byte_width_t));
    pipeline.add_filter(ByteshuffleFilter(byte_width_t));
    pipeline.add_filter(XORFilter(byte_width_t));
  }

  // Initial type of tile is float.
  std::vector<float> data = {
      1.0f, 2.1f, 3.2f, 4.3f, 5.4f, 6.5f, 7.6f, 8.7f, 9.8f, 10.9f};
  auto tile = make_shared<WriterTile>(
      HERE(),
      constants::format_version,
      Datatype::FLOAT32,
      sizeof(float),
      sizeof(float) * data.size(),
      tracker);
  for (size_t i = 0; i < data.size(); i++) {
    CHECK_NOTHROW(tile->write(&data[i], i * sizeof(float), sizeof(float)));
  }

  ThreadPool tp(4);
  pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp);
  CHECK(tile->size() == 0);
  CHECK(tile->filtered_buffer().size() != 0);

  auto unfiltered_tile =
      create_tile_for_unfiltering(data.size(), tile, tracker);
  ChunkData chunk_data;
  unfiltered_tile.load_chunk_data(chunk_data);
  REQUIRE(pipeline
              .run_reverse(
                  &dummy_stats,
                  &unfiltered_tile,
                  nullptr,
                  chunk_data,
                  0,
                  chunk_data.filtered_chunks_.size(),
                  tp.concurrency_level(),
                  tiledb::sm::Config())
              .ok());
  std::vector<float> results{
      1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 7.0f, 8.0f, 9.0f, 10.0f, 11.0f};
  for (size_t i = 0; i < data.size(); i++) {
    float val = 0;
    CHECK_NOTHROW(unfiltered_tile.read(&val, i * sizeof(float), sizeof(float)));
    if (pipeline.has_filter(tiledb::sm::FilterType::FILTER_SCALE_FLOAT)) {
      // Loss of precision from rounding in FloatScale filter.
      CHECK(val == results[i]);
    } else {
      CHECK(val == data[i]);
    }
  }
}
