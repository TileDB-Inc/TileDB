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
 * This example demonstrates basic functionality of task graph APIs.
 * It reads an array, compresses the data, decompresses, writes data into
 * a new array, compares the new array data with original data.
 *
 */

#include <iostream>

#include <tiledb/tiledb>
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/bountiful.h"
#include "experimental/tiledb/common/dag/execution/throw_catch.h"
#include "experimental/tiledb/common/dag/graph/taskgraph.h"

#include <bzlib.h>

using namespace tiledb;
using namespace tiledb::common;

// Alias the type of the input for a task graph node
// In our case, a transform or terminal node gets an offset, a chunk size
// and a shared_ptr to a vector containing data
template <class T>
using input_info =
    std::tuple<uint32_t, uint32_t, std::shared_ptr<std::vector<T>>>;

class chunk_generator {
  // needs to be thread safe, multiple nodes might change it simultaneously
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

  // The task graph calls this operator to get the offset of the next chunk
  uint32_t operator()(std::stop_source& stop_source) {
    // When the stop condition is met, we signal on the stop_source
    // When the stop_source is signaled, the task graph stops pulsating
    // starting with this current flow.
    if (begin_ + chunk_size_ > array_size_) {
      stop_source.request_stop();
    }

    uint32_t rv = begin_;
    begin_ += chunk_size_;
    return rv;
  }
};

void create_array(Context& ctx, const std::string& array_name) {
  // Create a TileDB array with a single "rows" dimension,
  // (0, uint32_max) domain and a tile extent of 4
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<uint32_t>(
      ctx, "rows", {{0, std::numeric_limits<uint32_t>::max() - 1}}, 4));

  // The array is dense
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Create a single fixed length attribute
  auto a1 = Attribute::create<uint32_t>(ctx, "a1");
  schema.add_attribute(a1);

  // Create the (empty) array on disk.
  Array::create(array_name, schema);
}

void write_chunk(
    Context& ctx,
    const std::string& array_name,
    const input_info<uint32_t>& in) {
  // Unpack input coming from the parent node
  auto [offset, chunk_size, data] = in;

  // Open array for writes
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR);

  // Configure the query to write chunk_size elements starting at offset
  Subarray subarray(ctx, array);
  subarray.add_range(0, offset, offset + chunk_size - 1);
  query.set_subarray(subarray);

  // Set the buffer that will be written for attribute a1
  query.set_data_buffer("a1", *data);

  // Perform the write
  query.submit();
}

void generate_test_data(
    Context& ctx, const std::string& array_name, uint32_t array_size) {
  // Write 0..array_size into the array
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
    uint32_t in_offset_begin,
    uint32_t array_size) {
  // Last reader reads till the end of the array
  if (in_offset_begin + 2 * chunk_size > array_size) {
    chunk_size = array_size - in_offset_begin;
  }

  // Open array for reads
  Array array(ctx, array_name, TILEDB_READ);

  // Set a subarray for reading chunk_bytes starting at in_offset_begin
  Subarray subarray(ctx, array);
  subarray.add_range(0, in_offset_begin, in_offset_begin + chunk_size - 1);

  // Alloc a big enough buffer for the query to read into
  std::vector<uint32_t> data(chunk_size);

  // Create a query and configure it with the subarray and buffers created above
  Query query(ctx, array, TILEDB_READ);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a1", data);

  // Perform the read
  query.submit();

  // Make sure query completed
  assert(query.query_status() == Query::Status::COMPLETE);

  // Return the output that will be passed to the child node
  // in the form of input_info<uint32_t>
  return std::make_tuple(
      in_offset_begin,
      chunk_size,
      std::make_shared<std::vector<uint32_t>>(data));
}

// Read data from the array and make sure it contains the expected data
void validate_results(
    Context& ctx, const std::string& array_name, uint32_t array_size) {
  std::vector<uint32_t> expected_results(array_size);
  std::generate(
      expected_results.begin(), expected_results.end(), [n = 0]() mutable {
        return n++;
      });

  auto [o, c, data] = read_chunk(ctx, array_name, array_size, 0, array_size);
  (void)o;
  (void)c;

  for (uint32_t r = 0; r < array_size; ++r) {
    assert(expected_results[r] == data->at(r));
  }
}

auto compress_chunk(const input_info<uint32_t>& in) {
  // Unpack input coming from the parent node
  auto [offset, chunk_size, data_ptr] = in;

  // Alloc a buffer big enough to fit the resulted compressed buffer
  unsigned int out_size = 2 * data_ptr->size() * sizeof(uint32_t);
  std::vector<char> compressed_data(out_size);

  // Bzip2 compress the data
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

  // Shrink the buffer to the real size reported by bzip
  compressed_data.resize(out_size);

  // Return the output which will be consumed by the decompression node
  return std::make_tuple(
      offset, chunk_size, std::make_shared<std::vector<char>>(compressed_data));
}

auto aggregate_compressed(
    const std::tuple<input_info<char>, input_info<char>, input_info<char>>&
        in) {
  // Unpack input coming from the parent node
  auto [t1, t2, t3] = in;
  auto [_1, _2, c1] = t1;
  auto [_3, _4, c2] = t2;
  auto [_5, _6, c3] = t3;

  std::vector<char> agg_buf(c1->size() + c2->size() + c3->size());
  std::copy(c1->begin(), c1->end(), agg_buf.begin());
  std::copy(c2->begin(), c2->end(), agg_buf.begin() + c1->size());
  std::copy(c3->begin(), c3->end(), agg_buf.begin() + c2->size());

  std::cout << c1->size() << " " << c2->size() << " " << c3->size()
            << std::endl;
  // return std::make_tuple(std::make_shared<std::vector<char>>(agg_buf));
}

auto decompress_chunk(const input_info<char>& in) {
  // Unpack input coming from the parent node
  auto [offset, chunk_size, data_ptr] = in;

  // Alloc a buffer big enough to fit the decompressed data
  unsigned int out_size = chunk_size * sizeof(uint32_t);
  std::vector<uint32_t> decompressed_data(chunk_size);

  // Bzip2 decompress the buffer
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

  // Make sure the size of the decompressed buffer is what we expect
  assert(out_size / sizeof(uint32_t) == chunk_size);

  // Return the output which will be consumed by the writing node
  return std::make_tuple(
      offset,
      chunk_size,
      std::make_shared<std::vector<uint32_t>>(decompressed_data));
}

int main() {
  // Create a TileDB context.
  Context ctx;

  // Create a TileDB virtual filesystem object
  tiledb::VFS vfs(ctx);

  // Name of the array used as input
  std::string array_name("taskgraph_filtering");

  // Name of the array used by the task graph as output
  std::string output_array("taskgraph_filtering_output");

  // The size of the arrays
  uint32_t array_size = 100;

  // If the arrays already exist on disk, remove them and start clean
  if (Object::object(ctx, array_name).type() == Object::Type::Array) {
    vfs.remove_dir(array_name);
  }
  if (Object::object(ctx, output_array).type() == Object::Type::Array) {
    vfs.remove_dir(output_array);
  }

  // Create the input array and the output array
  create_array(ctx, output_array);
  create_array(ctx, array_name);

  // Fill the input array with test data
  generate_test_data(ctx, array_name, array_size);

  // The number of threads will dictate how wide the graph will be
  uint32_t num_threads = std::thread::hardware_concurrency();

  // Divide the array into equal chunks to be processed by the task graph
  uint32_t chunk_size = array_size / num_threads;

  // Create a generator object. The task graph uses gen() calls to get the
  // offset of the next chunk to be processed
  chunk_generator gen{array_size, chunk_size};
  gen.reset();

  // Create a TaskGraph object with a DuffsScheduler scheduler type
  // Pass the number of threads the scheduler will use to execute the nodes
  TaskGraph<DuffsScheduler<node>> graph(num_threads);

  /**
   * The for loop below defines the architecture of the task graph, here's a
   * visual ilustration of how the graph will look like for this example.
   *
   *            Input Array 1D
   *    [chunk1...............chunkW]
   *       |                    |
   *     gen().................gen()
   *       |                    |
   *       v                    v
   *    read_chunk().........read_chunk()
   *       |                    |
   *       v                    v
   *    compress_chunk().....compres_chunk()
   *             \          /
   *              v        v
   *         aggregate_compressed()
   *                  |
   *                  v
   *            pass_through_node()
   *                  |
   *                  v
   *              alloc_mimo()
   *            /            \
   *           v              v
   *    decompress_chunk()...decompress_chunk()
   *       |                    |
   *       v                    v
   *    write_chunk()........write_chunk()
   *       |                    |
   *    [chunk1...............chunkW]
   *             Output Array
   *
   * The graph is constructed like W(i.e. width) pipelines of nodes.
   * Each pipeline processes a relatively equal chunk of the input array.
   *
   * The edges of the task graph define the scheduling dependency between nodes.
   * In this example the task graph scheduler cannot schedule a compress_chunk()
   * node before a read_chunk() node if they both belong to the same pipeline
   * like defined in the illustration above. But a compress_chunk(B) can be
   * scheduled to run before a read_chunk(A) if A and B are different pipelines,
   * it's up to the scheduler to choose an order if there is no path(defined
   * by the edges below) in the task graph from a node to another.
   */
  std::vector<function_node<
      DuffsMover3,
      input_info<uint32_t>,
      DuffsMover3,
      input_info<char>>>
      compression_nodes;
  for (uint32_t w = 0; w < 3; ++w) {
    // Create the node that calls the generator to pick the next offset
    // This node takes no input, only generates an output
    auto a = initial_node(graph, gen);

    // Create the node that brings a chunk of the TileDB Array in memory,
    // starting at an offset given by the node above.
    // Use a lambda here to capture some local vars and pass them to the
    // read_chunk function
    auto b = transform_node(
        graph,
        [&ctx, &array_name, chunk_size, array_size](uint32_t in_offset_begin) {
          return read_chunk(
              ctx, array_name, chunk_size, in_offset_begin, array_size);
        });

    // Create a node that will compress the in memory chunk passed from above
    // A transform_node takes 1 input and produces 1 output.
    auto c = transform_node(graph, compress_chunk);

    // Define the execution dependencies using task graph edges
    make_edge(graph, a, b);
    make_edge(graph, b, c);

    compression_nodes.push_back(c);
  }

  auto agg_mimo = terminal_mimo(graph, aggregate_compressed);

  make_edge(graph, compression_nodes[0], make_proxy<0>(agg_mimo));
  make_edge(graph, compression_nodes[1], make_proxy<1>(agg_mimo));
  make_edge(graph, compression_nodes[2], make_proxy<2>(agg_mimo));

  // auto pass_through = transform_node(
  //     graph,
  //     [&ctx, &array_name, chunk_size, array_size](uint32_t in_offset_begin) {
  //       return read_chunk(
  //           ctx, array_name, chunk_size, in_offset_begin, array_size);
  // });

  // make_edge(graph, make_proxy<0>(agg_mimo), pass_through);

  // auto alloc_mimo = mimo_node(
  //     graph,
  //     [](const std::tuple<
  //         input_info<uint32_t>,
  //         input_info<uint32_t>,
  //         input_info<uint32_t>>& in) {
  //       return std::make_tuple(
  //           offset,
  //           chunk_size,
  //           std::make_shared<std::vector<char>>(compressed_data));

  //       // return std::make_tuple(offset, chunk_size,
  //       // std::make_shared<std::vector<char>>(compressed_data));
  // });

  // make_edge(graph, pass_through, make_proxy<0>(alloc_mimo);

  // std::vector<function_node<DuffsMover3, uint32_t, DuffsMover3, uint32_t>>
  // decompress_nodes; for (uint32_t w = 0; w < 3; ++w) {
  //   // Create a node that will decompress the compressed buffer coming from
  //   // above
  //   auto d = transform_node(graph, decompress_chunk);

  //   // Create a node that gets the in memory data from above and writes it
  //   // into the output TileDB array
  //   auto e = terminal_node(
  //       graph, [&ctx, &output_array](const input_info<uint32_t>& in) {
  //         write_chunk(ctx, output_array, in);
  //       });

  //   // Define the execution dependencies using task graph edges
  //   make_edge(graph, d, e);

  //   decompress_nodes.push_back(d);
  // }

  // make_edge(graph,  make_proxy<0>(alloc_mimo), decompress_nodes[0]);
  // make_edge(graph, make_proxy<1>(alloc_mimo) decompress_nodes[1]);
  // make_edge(graph, make_proxy<2>(alloc_mimo), decompress_nodes[2]);

  // Run the scheduler to set up the execution order
  schedule(graph);

  // Start executing the graph
  sync_wait(graph);

  // At this point output_array should contain the same data as the input array
  // validate_results(ctx, output_array, array_size);

  return 0;
}
