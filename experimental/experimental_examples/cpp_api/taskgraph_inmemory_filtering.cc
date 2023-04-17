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
 * This example is very similar as functionality with taskgraph_filtering.cc,
 * but contains no TileDB Array APi so that the taskgraph API is
 * highlighted even better.
 */

#include <chrono>
#include <iostream>

// bzlib.h bleeds "windows.h" symbols
#if !defined(NOMINMAX)
#define NOMINMAX
#endif
#include <bzlib.h>
#if defined(DELETE)
#undef DELETE
#endif

#include <tiledb/tiledb>
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/bountiful.h"
#include "experimental/tiledb/common/dag/execution/throw_catch.h"
#include "experimental/tiledb/common/dag/graph/taskgraph.h"
#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"

using namespace tiledb;
using namespace tiledb::common;

// The vectors used as input and output by the taskgraph
static std::vector<uint32_t> array;
static std::vector<uint32_t> output_array;

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

void write_chunk(const input_info<uint32_t>& in) {
  // Unpack input coming from the parent node
  auto [offset, chunk_size, data] = in;

  // Write the input data into the output array at the given offset
  std::copy(data->begin(), data->end(), output_array.begin() + offset);
}

void generate_test_data() {
  // Write 0..array_size into the array
  std::generate(array.begin(), array.end(), [n = 0]() mutable { return n++; });
}

auto read_chunk(
    uint32_t chunk_size, uint32_t in_offset_begin, uint32_t array_size) {
  // Last reader reads till the end of the array
  if (in_offset_begin + 2 * chunk_size > array_size) {
    chunk_size = array_size - in_offset_begin;
  }

  // Alloc a big enough buffer and read chunk_size bytes
  // starting at in_offset_begin from the input array
  std::vector<uint32_t> data(chunk_size);
  std::copy(
      array.begin() + in_offset_begin,
      array.begin() + in_offset_begin + chunk_size,
      data.begin());

  // Return the output that will be passed to the child node
  // in the form of input_info<uint32_t>
  return std::make_tuple(
      in_offset_begin,
      chunk_size,
      std::make_shared<std::vector<uint32_t>>(data));
}

// Read data from the array and make sure it contains the expected data
void validate_results() {
  assert(std::equal(array.begin(), array.end(), output_array.begin()));
}

auto compress_chunk(const input_info<uint32_t>& in) {
  // Unpack input coming from the parent node
  auto [offset, chunk_size, data_ptr] = in;

  // Alloc a buffer big enough to fit the resulted compressed buffer
  auto in_size = static_cast<unsigned int>(data_ptr->size() * sizeof(uint32_t));
  std::vector<char> compressed_data(2 * in_size);
  unsigned int out_size;

  // Bzip2 compress the data
  int rc = BZ2_bzBuffToBuffCompress(
      compressed_data.data(),
      &out_size,
      reinterpret_cast<char*>(data_ptr->data()),
      in_size,
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
  // The size of the arrays
  uint32_t array_size = 100;
  array.resize(array_size);
  output_array.resize(array_size);

  // Fill the input array with test data
  generate_test_data();

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
   *       |                    |
   *       v                    v
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
   * by the edges below) in the graph from a node to another.
   */

  for (uint32_t w = 0; w < num_threads; ++w) {
    // Create the node that calls the generator to pick the next offset
    // This node takes no input, only generates an output
    auto a = initial_node(graph, gen);

    // Create the node that reads a chunk of the array in memory,
    // starting at an offset given by the node above.
    // Use a lambda here to capture some local vars and pass them to the
    // read_chunk function
    auto b = transform_node(
        graph, [chunk_size, array_size](uint32_t in_offset_begin) {
          return read_chunk(chunk_size, in_offset_begin, array_size);
        });

    // Create a node that will compress the in memory chunk passed from above
    // A transform_node takes 1 input and produces 1 output.
    auto c = transform_node(graph, compress_chunk);

    // Create a node that will decompress the compressed buffer coming from
    // above
    auto d = transform_node(graph, decompress_chunk);

    // Create a node that gets the in decompressed data from above and writes it
    // into the output array
    auto e = terminal_node(graph, write_chunk);

    // Define the execution dependencies using task graph edges
    make_edge(graph, a, b);
    make_edge(graph, b, c);
    make_edge(graph, c, d);
    make_edge(graph, d, e);
  }

  // Run the scheduler to set up the execution order
  schedule(graph);

  // Start executing the graph
  sync_wait(graph);

  // At this point the output array should contain the same data as the
  // input array
  validate_results();

  return 0;
}
