/**
 * @file flow_graph/library/nodes/bit_bucket.h
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
 * @section DESCRIPTION A bit bucket node that discards all inflowing data
 */

#ifndef TILEDB_FLOW_GRAPH_BIT_BUCKET_H
#define TILEDB_FLOW_GRAPH_BIT_BUCKET_H

#include "../../node.h"

namespace tiledb::flow_graph::library {

template <class T, node_services NS>
class BitBucket {
 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_node_body = true;
  } invariants;
  class InputPort {
  } input;
  void operator()() {
  }
};

template <class T>
class BitBucketInputPortSpecification {
 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_input_port_static_specification{true};
  } invariants;
  using flow_type = T;
};

template <class T>
struct BitBucketSpecification {
  static constexpr struct invariant_type {
    static constexpr bool i_am_node_static_specification = true;
  } invariants;
  static constexpr BitBucketInputPortSpecification<T> input;
  static constexpr std::tuple input_ports{input};
  static constexpr std::tuple output_ports{};
  BitBucketSpecification() = default;
  template <node_services NS>
  using node_body_template = BitBucket<T, NS>;
};

namespace {
class Opaque;  // placeholder for an arbitrary and unknown flow type
}
static_assert(
    node_body<BitBucketSpecification<Opaque>::node_body_template>,
    "`BitBucket` is supposed to be node body");

static_assert(
    node_static_specification<BitBucketSpecification<Opaque>>,
    "`BitBucketSpecification` is supposed to be a node specification");

}  // namespace tiledb::flow_graph::library

#endif  // TILEDB_FLOW_GRAPH_BIT_BUCKET_H
