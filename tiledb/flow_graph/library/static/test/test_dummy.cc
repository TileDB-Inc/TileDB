/**
 * @file flow_graph/library/dummy/test/main.cc
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
 */

#include <variant>  // for std::monostate

#include "tdb_catch.h"

#include "../../../system/edge_static_specification.h"
#include "../dummy.h"

using namespace tiledb::flow_graph;

//----------------------------------
// Test types
//----------------------------------
/*
 * We want to ensure that graph elements instantiate correctly with all kinds
 * of types that might be encountered:
 * - `std::monostate` (similar semantics as `void`)
 * - intrinsic: int, char, float
 * - class, union
 *   - with various constructibilities
 */
class TestFlowTypeClassDefaultConstructible {
  int x_{};

 public:
  TestFlowTypeClassDefaultConstructible() = default;
};
class TestFlowTypeClassPrivateConstructible {
  int x_{};
  TestFlowTypeClassPrivateConstructible() = default;
};
class TestFlowTypeClassNotDefaultConstructible {
  int x_;

 public:
  TestFlowTypeClassNotDefaultConstructible() = delete;
};
union TestFlowTypeUnionDefaultConstructible {
 public:
  std::monostate x_;
  [[maybe_unused]] int y_;
  TestFlowTypeUnionDefaultConstructible() = default;
};
union TestFlowTypeUnionPrivateConstructible {
  TestFlowTypeUnionPrivateConstructible() = default;

 public:
  std::monostate x_;
  [[maybe_unused]] int y_;
};
union TestFlowTypeUnionNotDefaultConstructible {
 public:
  std::monostate x_;
  [[maybe_unused]] int y_;
  TestFlowTypeUnionNotDefaultConstructible() = delete;
};

/**
 * Type list for checking that ports and nodes may be instantiated with
 * arbitrary types.
 */
using FlowTypes = std::tuple<
    std::monostate,
    int,
    char,
    float,
    TestFlowTypeClassDefaultConstructible,
    TestFlowTypeClassPrivateConstructible,
    TestFlowTypeClassNotDefaultConstructible,
    TestFlowTypeUnionDefaultConstructible,
    TestFlowTypeUnionPrivateConstructible,
    TestFlowTypeUnionNotDefaultConstructible>;
/*
 * Note: We test the dummy elements with `void` and the type list separately. We
 * don't expect actually functioning nodes to instantiate with `void`, but the
 * dummies are fine with it.
 */

//----------------------------------
// Output Port
//----------------------------------
TEST_CASE("dummy output port instance, void", "[fgl][dummy]") {
  static_assert(
      output_port_static_specification<DummyOutputPortSpecification<void>>,
      "It's supposed to be a specification node");
  (void)DummyOutputPortSpecification<void>{};
}

TEMPLATE_LIST_TEST_CASE(
    "dummy output port instance, type list", "[fgl][dummy]", FlowTypes) {
  static_assert(
      output_port_static_specification<DummyOutputPortSpecification<TestType>>,
      "It's supposed to be a specification node");
  (void)DummyOutputPortSpecification<TestType>{};
}

//----------------------------------
// Input Port
//----------------------------------
TEST_CASE("dummy input port instance, void", "[fgl][dummy]") {
  static_assert(
      input_port_static_specification<DummyInputPortSpecification<void>>,
      "It's supposed to be a specification node");
  (void)DummyInputPortSpecification<void>{};
}

TEMPLATE_LIST_TEST_CASE(
    "dummy input port instance, type list", "[fgl][dummy]", FlowTypes) {
  static_assert(
      input_port_static_specification<DummyInputPortSpecification<TestType>>,
      "It's supposed to be a specification node");
  (void)DummyInputPortSpecification<TestType>{};
}

//----------------------------------
// Output Node
//----------------------------------
TEST_CASE("dummy output node instance, void", "[fgl][dummy]") {
  (void)DummyOutputNodeSpecification<void>{};
  static_assert(
      node_static_specification<DummyOutputNodeSpecification<void>>,
      "It's supposed to be a specification node");
}

TEMPLATE_LIST_TEST_CASE(
    "dummy output node instance, type list", "[fgl][dummy]", FlowTypes) {
  static_assert(
      node_static_specification<DummyOutputNodeSpecification<TestType>>,
      "It's supposed to be a specification node");
  (void)DummyOutputNodeSpecification<TestType>{};
}

//----------------------------------
// Input Node
//----------------------------------
TEST_CASE("dummy input node instance, void", "[fgl][dummy]") {
  static_assert(
      node_static_specification<DummyInputNodeSpecification<void>>,
      "It's supposed to be a specification node");
  (void)DummyInputNodeSpecification<void>{};
}

TEMPLATE_LIST_TEST_CASE(
    "dummy input node instance, type list", "[fgl][dummy]", FlowTypes) {
  static_assert(
      node_static_specification<DummyInputNodeSpecification<TestType>>,
      "It's supposed to be a specification node");
  (void)DummyInputNodeSpecification<TestType>{};
}

//----------------------------------
// Edge
//----------------------------------
TEST_CASE("dummy edge instance, void", "[fgl][dummy]") {
  DummyOutputNodeSpecification<void> a{};
  DummyInputNodeSpecification<void> b{};

  static_specification::validator<DummyEdgeSpecification<
      DummyOutputNodeSpecification<void>,
      DummyOutputPortSpecification<void>,
      DummyInputNodeSpecification<void>,
      DummyInputPortSpecification<void>>>();
  static_assert(
      edge_static_specification<DummyEdgeSpecification<
          DummyOutputNodeSpecification<void>,
          DummyOutputPortSpecification<void>,
          DummyInputNodeSpecification<void>,
          DummyInputPortSpecification<void>>>,
      "It's supposed to be a specification edge");
  // Construct with fully qualified type
  (void)DummyEdgeSpecification<
      DummyOutputNodeSpecification<void>,
      DummyOutputPortSpecification<void>,
      DummyInputNodeSpecification<void>,
      DummyInputPortSpecification<void>>{a, a.output, b, b.input};
  // Construct with user-defined deduction guide
  (void)DummyEdgeSpecification{a, a.output, b, b.input};
}

//---------------------
//----------------------------------
//-------------------------------------------------------
