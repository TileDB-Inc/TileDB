/**
 * @file unit-filter-pipeline.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests the `FilterPipeline` class.
 */

#include "test/support/src/helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/bit_width_reduction_filter.h"
#include "tiledb/sm/filter/bitshuffle_filter.h"
#include "tiledb/sm/filter/byteshuffle_filter.h"
#include "tiledb/sm/filter/checksum_md5_filter.h"
#include "tiledb/sm/filter/checksum_sha256_filter.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/filter/float_scaling_filter.h"
#include "tiledb/sm/filter/positive_delta_filter.h"
#include "tiledb/sm/filter/webp_filter.h"
#include "tiledb/sm/filter/xor_filter.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/stdx/utility/to_underlying.h"

#include <test/support/tdb_catch.h>
#include <functional>
#include <random>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

shared_ptr<WriterTile> make_increasing_tile(
    const uint64_t nelts, shared_ptr<MemoryTracker> tracker) {
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);

  auto tile = make_shared<WriterTile>(
      HERE(),
      constants::format_version,
      Datatype::UINT64,
      cell_size,
      tile_size,
      tracker);
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK_NOTHROW(tile->write(&i, i * sizeof(uint64_t), sizeof(uint64_t)));
  }

  return tile;
}

shared_ptr<WriterTile> make_offsets_tile(
    std::vector<uint64_t>& offsets, shared_ptr<MemoryTracker> tracker) {
  const uint64_t offsets_tile_size =
      offsets.size() * constants::cell_var_offset_size;

  auto offsets_tile = make_shared<WriterTile>(
      HERE(),
      constants::format_version,
      Datatype::UINT64,
      constants::cell_var_offset_size,
      offsets_tile_size,
      tracker);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK_NOTHROW(offsets_tile->write(
        &offsets[i],
        i * constants::cell_var_offset_size,
        constants::cell_var_offset_size));
  }

  return offsets_tile;
}

Tile create_tile_for_unfiltering(
    uint64_t nelts,
    shared_ptr<WriterTile> tile,
    shared_ptr<MemoryTracker> tracker) {
  return {
      tile->format_version(),
      tile->type(),
      tile->cell_size(),
      0,
      tile->cell_size() * nelts,
      tile->filtered_buffer().data(),
      tile->filtered_buffer().size(),
      tracker};
}

void run_reverse(
    const tiledb::sm::Config& config,
    ThreadPool& tp,
    Tile& unfiltered_tile,
    FilterPipeline& pipeline,
    bool success = true) {
  ChunkData chunk_data;
  unfiltered_tile.load_chunk_data(chunk_data);
  CHECK(
      success == pipeline
                     .run_reverse(
                         &test::g_helper_stats,
                         &unfiltered_tile,
                         nullptr,
                         chunk_data,
                         0,
                         chunk_data.filtered_chunks_.size(),
                         tp.concurrency_level(),
                         config)
                     .ok());
}

TEST_CASE("Filter: Test encryption", "[filter][encryption]") {
  tiledb::sm::Config config;

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set up test data
  const uint64_t nelts = 1000;
  auto tile = make_increasing_tile(nelts, tracker);

  SECTION("- AES-256-GCM") {
    FilterPipeline pipeline;
    ThreadPool tp(4);
    pipeline.add_filter(EncryptionAES256GCMFilter(Datatype::UINT64));

    // No key set
    CHECK(!pipeline.run_forward(&test::g_helper_stats, tile.get(), nullptr, &tp)
               .ok());

    // Create and set a key
    char key[32];
    for (unsigned i = 0; i < 32; i++)
      key[i] = (char)i;
    auto filter = pipeline.get_filter<EncryptionAES256GCMFilter>();
    filter->set_key(key);

    // Check success
    CHECK(pipeline.run_forward(&test::g_helper_stats, tile.get(), nullptr, &tp)
              .ok());
    CHECK(tile->size() == 0);
    CHECK(tile->filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }

    // Check error decrypting with wrong key.
    tile = make_increasing_tile(nelts, tracker);
    CHECK(pipeline.run_forward(&test::g_helper_stats, tile.get(), nullptr, &tp)
              .ok());
    key[0]++;
    filter->set_key(key);

    auto unfiltered_tile2 = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile2, pipeline, false);

    // Fix key and check success.
    auto unfiltered_tile3 = create_tile_for_unfiltering(nelts, tile, tracker);

    key[0]--;
    filter->set_key(key);
    run_reverse(config, tp, unfiltered_tile3, pipeline);

    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile3.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }
}

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

  CHECK(pipeline.run_forward(&test::g_helper_stats, tile.get(), nullptr, &tp)
            .ok());

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

  CHECK(pipeline.run_forward(&test::g_helper_stats, tile.get(), nullptr, &tp)
            .ok());

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
  REQUIRE(pipeline.run_forward(&test::g_helper_stats, tile.get(), nullptr, &tp)
              .ok());
  CHECK(tile->size() == 0);
  CHECK(tile->filtered_buffer().size() != 0);

  auto unfiltered_tile =
      create_tile_for_unfiltering(data.size(), tile, tracker);
  ChunkData chunk_data;
  unfiltered_tile.load_chunk_data(chunk_data);
  REQUIRE(pipeline
              .run_reverse(
                  &test::g_helper_stats,
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

TEST_CASE(
    "C++ API: Pipeline with filtered type conversions",
    "[cppapi][filter][pipeline]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);
  std::string array_name = "cpp_test_array";
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  tiledb::Domain domain(ctx);
  float domain_lo = static_cast<float>(std::numeric_limits<int64_t>::min());
  float domain_hi = static_cast<float>(std::numeric_limits<int64_t>::max() - 1);

  // Create and initialize dimension.
  auto d1 = tiledb::Dimension::create<float>(
      ctx, "d1", {{domain_lo, domain_hi}}, 2048);

  tiledb::Filter float_scale(ctx, TILEDB_FILTER_SCALE_FLOAT);
  double scale = 1.0f;
  double offset = 0.0f;
  uint64_t byte_width = sizeof(int32_t);

  // Float scale converting tile data from float->int32
  float_scale.set_option(TILEDB_SCALE_FLOAT_BYTEWIDTH, &byte_width);
  float_scale.set_option(TILEDB_SCALE_FLOAT_FACTOR, &scale);
  float_scale.set_option(TILEDB_SCALE_FLOAT_OFFSET, &offset);

  // Delta filter reinterprets int32->uint32
  tiledb::Filter delta(ctx, TILEDB_FILTER_DELTA);

  // Pass uint32 data to BitWidthReduction filter
  tiledb::Filter bit_width_reduction(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION);

  tiledb::FilterList filters(ctx);
  filters.add_filter(float_scale);
  filters.add_filter(delta);
  filters.add_filter(bit_width_reduction);

  // Apply filters to both attribute and dimension.
  REQUIRE_NOTHROW(d1.set_filter_list(filters));
  domain.add_dimension(d1);

  auto a1 = tiledb::Attribute::create<float>(ctx, "a1");
  REQUIRE_NOTHROW(a1.set_filter_list(filters));

  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  REQUIRE_NOTHROW(tiledb::Array::create(array_name, schema));
  std::vector<float> d1_data = {
      1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
  std::vector<float> a1_data = {
      1.0f, 2.1f, 3.2f, 4.3f, 5.4f, 6.5f, 7.6f, 8.7f, 9.8f, 10.9f};

  // Write to array.
  {
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    query.set_data_buffer("d1", d1_data);
    query.set_data_buffer("a1", a1_data);
    query.submit();
    CHECK(tiledb::Query::Status::COMPLETE == query.query_status());
  }

  // Read from array.
  {
    std::vector<float> d1_read(10);
    std::vector<float> a1_read(10);
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    query.set_subarray({domain_lo, domain_hi});
    query.set_data_buffer("a1", a1_read);
    query.set_data_buffer("d1", d1_read);
    query.submit();
    CHECK(tiledb::Query::Status::COMPLETE == query.query_status());
    // Some loss of precision from rounding in FloatScale.
    CHECK(
        std::vector<float>{
            1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 7.0f, 8.0f, 9.0f, 10.0f, 11.0f} ==
        a1_read);
    CHECK(d1_data == d1_read);
  }

  // Cleanup.
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "C++ API: Filter pipeline validation",
    "[cppapi][filter][pipeline][validation]") {
  tiledb::Context ctx;

  tiledb::Domain domain(ctx);
  float domain_lo = static_cast<float>(std::numeric_limits<int64_t>::min());
  float domain_hi = static_cast<float>(std::numeric_limits<int64_t>::max() - 1);
  auto d1 = tiledb::Dimension::create<float>(
      ctx, "d1", {{domain_lo, domain_hi}}, 2048);
  auto a1 = tiledb::Attribute::create<float>(ctx, "a1");

  // FloatScale used for testing different float->integral pipelines.
  tiledb::Filter float_scale(ctx, TILEDB_FILTER_SCALE_FLOAT);
  double scale = 1.0f;
  double offset = 0.0f;
  uint64_t byte_width = sizeof(int32_t);
  // Float scale converting tile data from float->int32
  float_scale.set_option(TILEDB_SCALE_FLOAT_BYTEWIDTH, &byte_width);
  float_scale.set_option(TILEDB_SCALE_FLOAT_FACTOR, &scale);
  float_scale.set_option(TILEDB_SCALE_FLOAT_OFFSET, &offset);

  tiledb::FilterList filters(ctx);
  SECTION("- FloatScale filter accepts float or double byte width input") {
    auto d2 = tiledb::Dimension::create<int8_t>(ctx, "d2", {{1, 100}}, 10);
    auto a2 = tiledb::Attribute::create<int32_t>(ctx, "a2");
    filters.add_filter(float_scale);
    CHECK_THROWS(d2.set_filter_list(filters));
    CHECK_NOTHROW(a2.set_filter_list(filters));
  }

  SECTION("- Delta filters do not accept real datatypes") {
    auto test_filter = GENERATE(
        TILEDB_FILTER_POSITIVE_DELTA,
        TILEDB_FILTER_DOUBLE_DELTA,
        TILEDB_FILTER_DELTA);
    tiledb::Filter delta_filter(ctx, test_filter);
    filters.add_filter(delta_filter);
    // Delta compressors don't accept floats. Should fail without FloatScale.
    CHECK_THROWS(d1.set_filter_list(filters));
    CHECK_THROWS(a1.set_filter_list(filters));

    // Test using FloatScale to convert to integral is accepted.
    tiledb::FilterList filters2(ctx);
    filters2.add_filter(float_scale);
    filters2.add_filter(delta_filter);
    CHECK_NOTHROW(d1.set_filter_list(filters2));
    CHECK_NOTHROW(a1.set_filter_list(filters2));
  }

  SECTION("- Webp filter supports only uint8 attributes") {
    if (webp_filter_exists) {
      tiledb::Filter webp(ctx, TILEDB_FILTER_WEBP);
      filters.add_filter(webp);
      CHECK_THROWS(d1.set_filter_list(filters));
      CHECK_THROWS(a1.set_filter_list(filters));
    }
  }

  SECTION("- Bit width reduction filter supports integral input") {
    tiledb::Filter bit_width_reduction(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION);
    filters.add_filter(bit_width_reduction);
    CHECK_THROWS(d1.set_filter_list(filters));
    CHECK_THROWS(a1.set_filter_list(filters));

    // Test using FloatScale to convert to integral is accepted.
    tiledb::FilterList filters2(ctx);
    filters2.add_filter(float_scale);
    filters2.add_filter(bit_width_reduction);
    CHECK_NOTHROW(d1.set_filter_list(filters2));
    CHECK_NOTHROW(a1.set_filter_list(filters2));
  }

  SECTION("- XOR filter interprets datatype as integral") {
    // Datatype byte size must match size of int8, int16, int32, or int64
    tiledb::Filter xor_filter(ctx, TILEDB_FILTER_XOR);
    filters.add_filter(xor_filter);
    CHECK_NOTHROW(d1.set_filter_list(filters));
    CHECK_NOTHROW(a1.set_filter_list(filters));
  }

  SECTION("- Multiple compressors") {
    tiledb::Filter bzip(ctx, TILEDB_FILTER_BZIP2);
    auto compressor = GENERATE(
        TILEDB_FILTER_GZIP,
        TILEDB_FILTER_LZ4,
        TILEDB_FILTER_RLE,
        TILEDB_FILTER_ZSTD);
    tiledb::Filter compressor_filter(ctx, compressor);
    filters.add_filter(bzip);
    filters.add_filter(compressor_filter);

    CHECK_NOTHROW(d1.set_filter_list(filters));
    CHECK_NOTHROW(a1.set_filter_list(filters));

    // Should throw without FloatScale to convert float->int32.
    auto delta_compressor = GENERATE(
        TILEDB_FILTER_POSITIVE_DELTA,
        TILEDB_FILTER_DOUBLE_DELTA,
        TILEDB_FILTER_DELTA);
    tiledb::Filter delta_filter(ctx, delta_compressor);
    filters.add_filter(delta_filter);
    CHECK_THROWS(d1.set_filter_list(filters));
    CHECK_THROWS(a1.set_filter_list(filters));
  }

  SECTION("- Multiple compressors following type conversion") {
    auto compressor = GENERATE(
        TILEDB_FILTER_DOUBLE_DELTA,
        TILEDB_FILTER_DELTA,
        TILEDB_FILTER_GZIP,
        TILEDB_FILTER_LZ4,
        TILEDB_FILTER_RLE,
        TILEDB_FILTER_ZSTD);
    tiledb::Filter compressor_filter(ctx, compressor);
    tiledb::Filter bzip(ctx, TILEDB_FILTER_BZIP2);
    filters.add_filter(float_scale);
    filters.add_filter(bzip);
    filters.add_filter(compressor_filter);

    CHECK_NOTHROW(d1.set_filter_list(filters));
    CHECK_NOTHROW(a1.set_filter_list(filters));
  }
}
