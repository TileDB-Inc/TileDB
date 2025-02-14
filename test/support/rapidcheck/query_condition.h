/**
 * @file test/support/rapidcheck/query_condition.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file defines rapidcheck generators for query condition AST.
 */

#ifndef TILEDB_RAPIDCHECK_QUERY_CONDITION_H
#define TILEDB_RAPIDCHECK_QUERY_CONDITION_H

#include "tiledb/sm/query/ast/query_ast.h"

#include <test/support/src/array_templates.h>
#include <test/support/stdx/tuple.h>
#include <test/support/tdb_rapidcheck.h>

namespace rc {

template <>
struct Arbitrary<tiledb::sm::QueryConditionOp> {
  static Gen<tiledb::sm::QueryConditionOp> arbitrary() {
    // NOT IN and IN are not handled yet by the users of this
    return gen::element(
        tiledb::sm::QueryConditionOp::LT,
        tiledb::sm::QueryConditionOp::LE,
        tiledb::sm::QueryConditionOp::GT,
        tiledb::sm::QueryConditionOp::GE,
        tiledb::sm::QueryConditionOp::EQ,
        tiledb::sm::QueryConditionOp::NE);
  }
};

template <FragmentType Fragment>
Gen<tdb_unique_ptr<tiledb::sm::ASTNode>> make_query_condition() {
  using DimensionTuple =
      stdx::decay_tuple<decltype(std::declval<Fragment>().dimensions())>;
  using AttributeTuple =
      stdx::decay_tuple<decltype(std::declval<Fragment>().attributes())>;
  using FieldTuple = decltype(std::tuple_cat(
      std::declval<DimensionTuple>(), std::declval<AttributeTuple>()));

  auto field = gen::inRange<size_t>(0, std::tuple_size_v<FieldTuple>);
  auto op = gen::arbitrary<tiledb::sm::QueryConditionOp>();

  auto parts = gen::mapcat(gen::pair(field, op), [](auto arg) {
    size_t field;
    tiledb::sm::QueryConditionOp op;
    std::tie(field, op) = arg;

    const auto field_names = QueryConditionEvalSchema<Fragment>().field_names_;

    using R = std::pair<std::string, Gen<std::string>>;
    auto make_value_gen = [&](auto i) -> std::optional<R> {
      if (field == i) {
        using ValueType =
            typename std::tuple_element<i, FieldTuple>::type::value_type;
        // NB: `arbitrary` will provide mostly useless values
        // This range likely suits the current test cases
        auto value = gen::inRange<ValueType>(-128, 128);
        auto p = std::make_pair(field_names[i], gen::map(value, [](auto value) {
                                  return std::string(
                                      reinterpret_cast<char*>(&value),
                                      sizeof(value));
                                }));
        return std::optional<R>{p};
      } else {
        return std::optional<R>{};
      }
    };

    auto fielddata = stdx::fold_optional<R>(
        std::make_index_sequence<std::tuple_size_v<FieldTuple>>{},
        make_value_gen);

    return gen::tuple(
        gen::just(fielddata.value().first),
        gen::just(op),
        fielddata.value().second);
  });

  return gen::map(parts, [](auto arg) {
    return tdb_unique_ptr<tiledb::sm::ASTNode>(new tiledb::sm::ASTNodeVal(
        std::get<0>(arg),
        std::get<2>(arg).data(),
        std::get<2>(arg).size(),
        std::get<1>(arg)));
  });
}

}  // namespace rc

#endif
