/**
 * @file flow_graph/test/test_graph_type.cc
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
 * @section DESCRIPTION Tests for `graph_type.h`. These eventually might be
 * moved into their own directory.
 */

#include "tdb_catch.h"
#include "tiledb/flow_graph/system/graph_type.h"

using namespace tiledb::flow_graph;

//-------------------------------------------------------
// test data
//-------------------------------------------------------
constexpr std::tuple t0{};
constexpr std::tuple t1{0};
constexpr std::tuple t2{0, 1};
constexpr std::tuple t3{0, 1, 2};

// tuples of member references
struct T1 {
  // elements of list; values 0,1
  static constexpr int a{0};
  static constexpr int b{0};
  static constexpr int c{1};
  // absent from list; values 1,2
  static constexpr int d{1};
  static constexpr int e{1};
  static constexpr int f{2};
  // reference list to test against
  constexpr static std::tuple<const int&, const int&, const int&> x{a, b, c};
  constexpr static reference_tuple x2{a, b, c};
};
// also absent from list; values 1,2
constexpr int g1{1};
constexpr int g2{2};

//-------------------------------------------------------
// product_type
//-------------------------------------------------------

TEST_CASE("product_type vs. std::tuple", "[fgl][product_type]") {
  static_assert(product_type<std::tuple<>>);
  static_assert(product_type<std::tuple<int>>);
  static_assert(product_type<std::tuple<int, int>>);
  static_assert(product_type<decltype(t0)>);
  static_assert(product_type<decltype(t1)>);
  static_assert(product_type<decltype(t2)>);
  static_assert(product_type<decltype(t3)>);
  static_assert(product_type<decltype(T1::x)>);
  static_assert(product_type<decltype(T1::x2)>);
}

TEST_CASE("factor_sizeof vs. std::tuple", "[fgl][factor_sizeof]") {
  static_assert(factor_sizeof(std::tuple<>{}) == 0);
  static_assert(factor_sizeof(std::tuple<int>{}) == 1);
  static_assert(factor_sizeof(std::tuple<int, int>{}) == 2);
  static_assert(factor_sizeof(t0) == 0);
  static_assert(factor_sizeof(t1) == 1);
  static_assert(factor_sizeof(t2) == 2);
  static_assert(factor_sizeof(t3) == 3);
  static_assert(factor_sizeof(T1::x) == 3);
  static_assert(factor_sizeof(T1::x2) == 3);
}

TEST_CASE(
    "product_type vs. reference_tuple",
    "[fgl][product_type][reference_tuple]") {
  static_assert(product_type<reference_tuple<>>);
  static_assert(product_type<reference_tuple<int>>);
  static_assert(product_type<reference_tuple<int, int>>);
  static_assert(product_type<decltype(T1::x2)>);
}

TEST_CASE(
    "factor_sizeof vs. reference_tuple",
    "[fgl][factor_sizeof][reference_tuple]") {
  static_assert(factor_sizeof(reference_tuple<>{}) == 0);
  static_assert(factor_sizeof(reference_tuple<int>{}) == 1);
  static_assert(factor_sizeof(reference_tuple<int, int>{}) == 2);
  static_assert(factor_sizeof(T1::x2) == 3);
}

//-------------------------------------------------------
// `is_reference_element_of` and `contains_lvalue_references`
//-------------------------------------------------------
TEST_CASE(
    "is_reference_element_of_old vs. std::tuple",
    "[fgl][is_reference_element_of]") {
  static_assert(is_reference_element_of(T1::a, T1::x));
  static_assert(is_reference_element_of(T1::b, T1::x));
  static_assert(is_reference_element_of(T1::c, T1::x));
  static_assert(!is_reference_element_of(T1::d, T1::x));
  static_assert(!is_reference_element_of(T1::e, T1::x));
  static_assert(!is_reference_element_of(T1::f, T1::x));
  static_assert(!is_reference_element_of(g1, T1::x));
  static_assert(!is_reference_element_of(g2, T1::x));
}

TEST_CASE(
    "is_reference_element_of_old vs. reference_tuple",
    "[fgl][is_reference_element_of_old][reference_tuple]") {
  static_assert(is_reference_element_of(T1::a, T1::x2));
  static_assert(is_reference_element_of(T1::b, T1::x2));
  static_assert(is_reference_element_of(T1::c, T1::x2));
  static_assert(!is_reference_element_of(T1::d, T1::x2));
  static_assert(!is_reference_element_of(T1::e, T1::x2));
  static_assert(!is_reference_element_of(T1::f, T1::x2));
  static_assert(!is_reference_element_of(g1, T1::x2));
  static_assert(!is_reference_element_of(g2, T1::x2));
}

TEST_CASE(
    "contains_lvalue_references vs. std::tuple",
    "[fgl][contains_lvalue_references]") {
  static_assert(contains_lvalue_references_v(T1::x));
}

TEST_CASE(
    "contains_lvalue_references vs. reference_tuple",
    "[fgl][contains_lvalue_references][reference_tuple]") {
  static_assert(contains_lvalue_references_v(T1::x2));
}

//-------------------------------------------------------
// predicate_class_over_product
//-------------------------------------------------------

struct false_funobject {
  template <class T>
  constexpr bool operator()(T&&) {
    return false;
  }
  constexpr false_funobject() = default;
};

struct true_funobject {
  template <class T>
  constexpr bool operator()(T&&) {
    return true;
  }
  constexpr true_funobject() = default;
};

TEST_CASE(
    "predicate_class_over_product vs. std::tuple",
    "[fgl][predicate_class_over_product][reference_tuple]") {
  static_assert(predicate_over_product<true_funobject, decltype(T1::x)>);
  static_assert(predicate_over_product<false_funobject, decltype(T1::x)>);
}

TEST_CASE(
    "predicate_class_over_product vs. reference_tuple",
    "[fgl][predicate_class_over_product][reference_tuple]") {
  static_assert(predicate_over_product<true_funobject, decltype(T1::x2)>);
  static_assert(predicate_over_product<false_funobject, decltype(T1::x2)>);
}

TEST_CASE("count_until_satisfied_p", "[fgl][predicate_class_over_product]") {
  static_assert(count_until_satisfied_p(true_funobject{}, T1::x) == 0);
  static_assert(count_until_satisfied_p(true_funobject{}, T1::x2) == 0);
  static_assert(
      count_until_satisfied_p(false_funobject{}, T1::x) ==
      factor_sizeof(T1::x));
  static_assert(
      count_until_satisfied_p(false_funobject{}, T1::x2) ==
      factor_sizeof(T1::x2));
}
