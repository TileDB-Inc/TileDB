/**
 * @file    taskgraph_filtering.cc
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
 * TODO
 */

#include <iostream>

#include <tiledb/tiledb>
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/bountiful.h"
#include "experimental/tiledb/common/dag/execution/throw_catch.h"
#include "experimental/tiledb/common/dag/graph/taskgraph.h"
#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"

#include <bzlib.h>

using namespace tiledb;
using namespace tiledb::common;

template <class T>
using input_info =
    std::tuple<uint32_t, uint32_t, std::shared_ptr<std::vector<T>>>;

class chunk_generator {
  inline static std::atomic<uint32_t> begin_{0};
  uint32_t array_size_;
  uint32_t chunk_size_;

 public:
  chunk_generator(uint32_t array_size, uint32_t chunk_size)
      : array_size_(array_size)
      , chunk_size_(chunk_size) {
  }

  void reset() {
    begin_.store(0);
  }

  uint32_t operator()(std::stop_source& stop_source) {
    if (begin_ + chunk_size_ >= array_size_) {
      stop_source.request_stop();  // unclear how graph works here, does it
                                   // pulsate one more time or it dies including
                                   // this current operation?
    }

    uint32_t rv = begin_;
    begin_ += chunk_size_;
    return rv;
  }
};

void create_array(Context& ctx, const std::string& array_name) {
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<uint32_t>(
      ctx, "rows", {{0, std::numeric_limits<uint32_t>::max() - 1}}, 4));

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  auto a1 = Attribute::create<uint32_t>(ctx, "a1");
  schema.add_attribute(a1);

  Array::create(array_name, schema);
}

void write_chunk(
    Context& ctx,
    const std::string& array_name,
    const input_info<uint32_t>& in) {
  auto [offset, chunk_size, data] = in;

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR);

  Subarray subarray(ctx, array);
  subarray.add_range(0, offset, offset + chunk_size - 1);
  query.set_subarray(subarray);
  query.set_data_buffer("a1", *data);

  query.submit();
}

void generate_test_data(
    Context& ctx, const std::string& array_name, uint32_t array_size) {
  std::vector<uint32_t> data(array_size);
  std::generate(data.begin(), data.end(), [n = 0]() mutable { return n++; });

  write_chunk(
      ctx,
      array_name,
      std::make_tuple(
          0, array_size, std::make_shared<std::vector<uint32_t>>(data)));
}

auto read_chunk(
    Context& ctx,
    const std::string& array_name,
    uint32_t chunk_size,
    uint32_t in_offset_begin) {
  Array array(ctx, array_name, TILEDB_READ);

  Subarray subarray(ctx, array);
  subarray.add_range(0, in_offset_begin, in_offset_begin + chunk_size - 1);

  std::vector<uint32_t> data(chunk_size);

  Query query(ctx, array, TILEDB_READ);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a1", data);
  query.submit();

  assert(query.query_status() == Query::Status::COMPLETE);

  return std::make_tuple(
      in_offset_begin,
      chunk_size,
      std::make_shared<std::vector<uint32_t>>(data));
}

void validate_results(
    Context& ctx, const std::string& array_name, uint32_t array_size) {
  std::vector<uint32_t> expected_results(array_size);
  std::generate(
      expected_results.begin(), expected_results.end(), [n = 0]() mutable {
        return n++;
      });

  auto [o, c, data] = read_chunk(ctx, array_name, array_size, 0);
  (void)o;
  (void)c;

  for (uint32_t r = 0; r < array_size; ++r) {
    assert(expected_results[r] == data->at(r));
  }
}

auto compress_chunk(const input_info<uint32_t>& in) {
  auto [offset, chunk_size, data_ptr] = in;

  unsigned int out_size = 2 * data_ptr->size() * sizeof(uint32_t);
  std::vector<char> compressed_data(out_size);
  int rc = BZ2_bzBuffToBuffCompress(
      compressed_data.data(),
      &out_size,
      reinterpret_cast<char*>(data_ptr->data()),
      data_ptr->size() * sizeof(uint32_t),
      1,
      0,
      0);

  if (rc != BZ_OK) {
    throw std::runtime_error("compression error");
  }

  compressed_data.resize(data_ptr->size());

  return std::make_tuple(
      offset, chunk_size, std::make_shared<std::vector<char>>(compressed_data));
}

auto decompress_chunk(const input_info<char>& in) {
  auto [offset, chunk_size, data_ptr] = in;

  unsigned int out_size = chunk_size * sizeof(uint32_t);
  std::vector<uint32_t> decompressed_data(chunk_size);

  int rc = BZ2_bzBuffToBuffDecompress(
      reinterpret_cast<char*>(decompressed_data.data()),
      &out_size,
      data_ptr->data(),
      static_cast<unsigned int>(data_ptr->size()),
      0,
      0);
  if (rc != BZ_OK) {
    throw std::runtime_error("decompression error");
  }

  assert(out_size / sizeof(uint32_t) == chunk_size);

  return std::make_tuple(
      offset,
      chunk_size,
      std::make_shared<std::vector<uint32_t>>(decompressed_data));
}

int main() {
  Config cfg;
  Context ctx(cfg);
  std::string array_name("taskgraph_filtering");
  std::string output_array("taskgraph_filtering_output");
  uint32_t array_size = 96;

  if (Object::object(ctx, array_name).type() == Object::Type::Array) {
    tiledb::VFS vfs(ctx);
    vfs.remove_dir(array_name);
    std::cout << "Removed existing array" << std::endl;
  }
  if (Object::object(ctx, output_array).type() == Object::Type::Array) {
    tiledb::VFS vfs(ctx);
    vfs.remove_dir(output_array);
    std::cout << "Removed existing output array" << std::endl;
  }

  create_array(ctx, output_array);
  create_array(ctx, array_name);
  generate_test_data(ctx, array_name, array_size);

  uint32_t num_threads = stoi(cfg.get("sm.compute_concurrency_level"));
  uint32_t chunk_size = array_size / num_threads;

  chunk_generator gen{array_size, chunk_size};
  gen.reset();

  TaskGraph<DuffsScheduler<node>> graph(num_threads);

  for (uint32_t w = 0; w < num_threads; ++w) {
    auto a = initial_node(graph, gen);
    auto b = transform_node(
        graph, [&ctx, &array_name, chunk_size](uint32_t in_offset_begin) {
          return read_chunk(ctx, array_name, chunk_size, in_offset_begin);
        });
    auto c = transform_node(graph, compress_chunk);

    // MIMO node impl doesn't seem available, so I'm not sure how to
    // aggregate the outputs of all compressor nodes then fan back out
    // I'll fall back to making each compressor node pass the buffer directly to
    // a decompressor node
    auto d = transform_node(graph, decompress_chunk);

    auto e = terminal_node(
        graph, [&ctx, &output_array](const input_info<uint32_t>& in) {
          write_chunk(ctx, output_array, in);
        });

    make_edge(graph, a, b);
    make_edge(graph, b, c);
    make_edge(graph, c, d);
    make_edge(graph, d, e);
  }

  schedule(graph);
  sync_wait(graph);

  validate_results(ctx, output_array, array_size);

  return 0;
}
