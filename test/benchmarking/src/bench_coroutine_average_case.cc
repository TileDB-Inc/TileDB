/**
 * @file   bench_coroutine_average_case.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * Coroutine pipeline benchmark — AVERAGE CASE scenario.
 *
 * Represents a realistic mixed workload:
 *   - Sparse array with 5 attributes (fixed + var-length)
 *   - Multiple fragments (3 writes to the same domain)
 *   - Mix of compression levels (zstd for some, lz4 for others)
 *   - Unordered read layout
 *
 * This is the most representative scenario for real-world usage. The coroutine
 * pipeline helps because multiple attributes can overlap their I/O and unfilter
 * phases, and the varied compression levels create staggered completion times.
 *
 * Run comparison:
 *   ./bench_coroutine_average_case setup
 *   ./bench_coroutine_average_case run                 # sync baseline
 *   TILEDB_SM_QUERY_READER_USE_COROUTINE_PIPELINE=true \
 *     ./bench_coroutine_average_case run               # coroutine pipeline
 *   ./bench_coroutine_average_case teardown
 */

#include <tiledb/tiledb>

#include <random>

#include "benchmark.h"
#include "benchmark_config.h"

using namespace tiledb;

class Benchmark : public BenchmarkBase {
 protected:
  virtual void setup() {
    ArraySchema schema(ctx_, TILEDB_SPARSE);
    Domain domain(ctx_);
    domain.add_dimension(Dimension::create<uint32_t>(
        ctx_, "d1", {{1, domain_max_}}, tile_extent_));
    domain.add_dimension(Dimension::create<uint32_t>(
        ctx_, "d2", {{1, domain_max_}}, tile_extent_));
    schema.set_domain(domain);
    schema.set_capacity(tile_capacity_);
    schema.set_allows_dups(true);

    // Heavy compression for some attributes.
    FilterList heavy(ctx_);
    heavy.add_filter({ctx_, TILEDB_FILTER_BYTESHUFFLE})
        .add_filter({ctx_, TILEDB_FILTER_ZSTD});

    // Light compression for others.
    FilterList light(ctx_);
    light.add_filter({ctx_, TILEDB_FILTER_LZ4});

    // Mix of fixed and var-length attributes.
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "fixed1", heavy));
    schema.add_attribute(Attribute::create<double>(ctx_, "fixed2", light));
    schema.add_attribute(Attribute::create<int64_t>(ctx_, "fixed3", heavy));
    schema.add_attribute(Attribute::create<std::string>(ctx_, "var1", light));
    schema.add_attribute(Attribute::create<float>(ctx_, "fixed4", light));

    Array::create(array_uri_, schema);

    // Write multiple fragments to simulate a realistic multi-fragment array.
    std::mt19937 gen(42);  // Fixed seed for reproducibility.
    for (int frag = 0; frag < num_fragments_; frag++) {
      write_fragment(gen, frag);
    }
  }

  virtual void teardown() {
    bench_teardown(ctx_, array_uri_);
  }

  virtual void pre_run() {
    // Allocate read buffers large enough for all data.
    uint64_t max_cells = static_cast<uint64_t>(num_cells_per_fragment_) *
                         num_fragments_ * 2;  // Extra room.
    d1_.resize(max_cells);
    d2_.resize(max_cells);
    fixed1_.resize(max_cells);
    fixed2_.resize(max_cells);
    fixed3_.resize(max_cells);
    fixed4_.resize(max_cells);
    var1_offsets_.resize(max_cells);
    var1_data_.resize(max_cells * avg_string_len_);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    Query query(ctx_, array);

    // Read entire domain.
    query
        .set_subarray(Subarray(ctx_, array)
                          .set_subarray({1u, domain_max_, 1u, domain_max_}))
        .set_layout(TILEDB_UNORDERED)
        .set_data_buffer("d1", d1_)
        .set_data_buffer("d2", d2_)
        .set_data_buffer("fixed1", fixed1_)
        .set_data_buffer("fixed2", fixed2_)
        .set_data_buffer("fixed3", fixed3_)
        .set_data_buffer("fixed4", fixed4_)
        .set_data_buffer("var1", var1_data_)
        .set_offsets_buffer("var1", var1_offsets_);

    // Handle incomplete queries.
    Query::Status status;
    do {
      query.submit();
      status = query.query_status();
    } while (status == Query::Status::INCOMPLETE);

    array.close();
  }

 private:
  void write_fragment(std::mt19937& gen, int frag_idx) {
    std::uniform_int_distribution<uint32_t> coord_dist(1, domain_max_);

    // Generate unique coordinates for this fragment.
    std::vector<uint32_t> d1(num_cells_per_fragment_);
    std::vector<uint32_t> d2(num_cells_per_fragment_);
    std::vector<int32_t> f1(num_cells_per_fragment_);
    std::vector<double> f2(num_cells_per_fragment_);
    std::vector<int64_t> f3(num_cells_per_fragment_);
    std::vector<float> f4(num_cells_per_fragment_);
    std::vector<uint64_t> var_offsets(num_cells_per_fragment_);
    std::string var_data;

    uint64_t offset = 0;
    for (uint64_t i = 0; i < num_cells_per_fragment_; i++) {
      d1[i] = coord_dist(gen);
      d2[i] = coord_dist(gen);
      f1[i] = static_cast<int32_t>(i + frag_idx * 1000);
      f2[i] = static_cast<double>(i) * 1.5 + frag_idx;
      f3[i] = static_cast<int64_t>(i) * 200 + frag_idx;
      f4[i] = static_cast<float>(i) * 0.3f;

      // Variable-length strings of varying length.
      var_offsets[i] = offset;
      size_t len = 4 + (i % 12);  // 4-15 chars.
      for (size_t c = 0; c < len; c++) {
        var_data.push_back('a' + static_cast<char>((i + c) % 26));
      }
      offset += len;
    }

    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("d1", d1)
        .set_data_buffer("d2", d2)
        .set_data_buffer("fixed1", f1)
        .set_data_buffer("fixed2", f2)
        .set_data_buffer("fixed3", f3)
        .set_data_buffer("fixed4", f4)
        .set_data_buffer("var1", var_data)
        .set_offsets_buffer("var1", var_offsets);
    query.submit();
    array.close();
  }

  const std::string array_uri_ = bench_uri("bench_array");
  static constexpr unsigned domain_max_ = 10000;
  static constexpr unsigned tile_extent_ = 500;
  static constexpr unsigned tile_capacity_ = 100000;
  static constexpr uint64_t num_cells_per_fragment_ = 2000000;
  static constexpr int num_fragments_ = 3;
  static constexpr size_t avg_string_len_ = 10;

  Context ctx_{bench_config()};

  // Read buffers.
  std::vector<uint32_t> d1_, d2_;
  std::vector<int32_t> fixed1_;
  std::vector<double> fixed2_;
  std::vector<int64_t> fixed3_;
  std::vector<float> fixed4_;
  std::vector<uint64_t> var1_offsets_;
  std::string var1_data_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
