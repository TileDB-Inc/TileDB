/**
 * @file test/performance/tiledb_submit_a_b.cc
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
 * This file is an executable whose purpose is to compare the performance
 * of a tiledb query running with two different configurations.
 *
 * Usage: $0 <config.json> <array URI 1> [additional array URIs...]
 *
 * For each array in the argument list, a query is created for each
 * of the configurations in "configurations".
 * The `run()` function alternates taking a step (`tiledb_query_submit`)
 * with a query of each of those configurations, checking that they all
 * have the same results, and recording various metrics from each
 * (most notably, the time spent calling `tiledb_query_submit`)
 *
 * After executing upon all of the arrays, the timing information is
 * dumped to `/tmp/tiledb_submit_a_b.json`.
 *
 * The arrays must have the same schema, whose physical type is reflected
 * by the `using Fragment = <FragmentType>` declaration in the middle of
 * this file. The `using Fragment` declaration sets the types of the buffers
 * which are used to read the array.
 *
 * The nature of different configurations are specified via the required
 * argument `config.json`. The following example illustrates the currently
 * supported keys:
 *
 * ```
 * {
 *   "configurations": [
 *     {
 *       "name": "a",
 *       "num_user_cells": 16777216,
 *       "memory_budget": {
 *         "total": "1073741824"
 *       },
 *       "config": {
 *         "sm.query.sparse_global_order.preprocess_tile_merge": 0
 *       }
 *     },
 *     {
 *       "name": "b",
 *       "num_user_cells: 16777216
 *       "memory_budget": {
 *         "total": "1073741824"
 *       },
 *       "config": {
 *         "sm.query.sparse_global_order.preprocess_tile_merge": 128
 *       }
 *     },
 *   ],
 *   "metrics": [
 *      "loop_num",
 *      "internal_loop_num",
 *      ...
 *   ],
 *   "timers": [
 *      "preprocess_result_tile_order_compute",
 *      "preprocess_result_tile_order_await",
 *      ...
 *   ]
 *   "queries": [
 *     {
 *       "label": "my_query",
 *       "layout": "global_order",
 *       "select": [
 *          "a",
 *          "b",
 *          ...
 *       ]
 *       "subarray": [
 *         {
 *           "start": {
 *             "%": 30
 *           },
 *           "end": {
 *             "%": 50
 *           }
 *         }
 *       ],
 *       "condition": {
 *         "operator": "&&",
 *         "args": [
 *           {
 *             "operator": "<"
 *             "field": "a",
 *             "value": 5
 *           },
 *           {
 *             "operator": "<>"
 *             "field": "b",
 *             "value": 10
 *           }
 *         ]
 *       }
 *     }
 *   ]
 * }
 * ```
 *
 * The "config" field of each configuration object is a set of key-value
 * pairs which are set on a `tileb::Config` object for instances
 * of each query.
 * - The "name" field identifies the configuration.
 * - The "num_user_cells" field sets the size of the user buffer.
 * - The "memory_budget" field sets the query memory budget.
 * - The "config" field is a list of key-value pairs which are
 *   passed to the query configuration options.
 *
 * The "metrics" and "timers" fields indicate the list of statistics
 * to collect for each query. For "timers" the timer sum is
 * reported.
 *
 * Each item in "queries" specifies a query to run the comparison for.
 *
 * The "label" query identifies the query in the output.
 *
 * The "layout" field is required and sets the results order.
 *
 * The "select" field is a list of attributes to select
 * (in addition to the dimensions). If this key is not provided
 * then all attributes are selected.
 *
 * The "subarray" path is an optional list of range specifications on
 * each dimension. Currently the only supported specification is
 * an object with a field "%" which adds a subarray range whose
 * bounds cover the specified percent of the non-empty domain
 * for that dimension.
 */
#include <test/support/src/array_helpers.h>
#include <test/support/src/array_templates.h>
#include <test/support/src/error_helpers.h>
#include <test/support/stdx/tuple.h>

#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/query/query_api_internal.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/stats/duration_instrument.h"
#include "tiledb/type/apply_with_type.h"

#include <nlohmann/json.hpp>

#include <fstream>
#include <iostream>
#include <span>

using namespace tiledb::test;
using namespace tiledb::test::templates;

using Asserter = AsserterRuntimeException;

using json = nlohmann::json;

template <typename T>
json optional_json(std::optional<T> value) {
  if (value.has_value()) {
    return json(value.value());
  } else {
    return json();
  }
}

struct Configuration {
  std::string name_;
  std::optional<uint64_t> num_user_cells_;
  tiledb::Config qconf_;

  tiledb::test::SparseGlobalOrderReaderMemoryBudget memory_;

  Configuration() {
    memory_.total_budget_ = std::to_string(1024 * 1024 * 1024);
    memory_.ratio_tile_ranges_ = "0.01";
  }

  void memory_budget_from_json(const json& jmem) {
    if (jmem.find("total") != jmem.end()) {
      memory_.total_budget_ = jmem["total"].get<std::string>();
    }
    // TODO: other fields as needed
    memory_.apply(qconf_.ptr().get());
  }
};

/**
 * Records durations as reported by `tiledb::sm::stats::DurationInstrument`.
 */
struct StatKeeper {
  struct StatKey {
    std::string uri_;
    std::string qlabel_;
    std::string configname_;
  };

  struct StatValue {
    std::vector<double> durations;
    std::map<std::string, json> metrics;
  };

  using Timer = tiledb::sm::stats::DurationInstrument<StatKeeper, StatKey>;

  /** records statistics of each submit call by "array.query" */
  std::map<std::string, std::map<std::string, std::map<std::string, StatValue>>>
      statistics;

  StatValue& get(const StatKey& stat) {
    return statistics[stat.uri_][stat.qlabel_][stat.configname_];
  }

  Timer start_timer(const StatKey& stat) {
    return tiledb::sm::stats::DurationInstrument<StatKeeper, StatKey>(
        *this, stat);
  }

  void report_duration(
      const StatKey& stat, const std::chrono::duration<double> duration) {
    get(stat).durations.push_back(duration.count());
  }

  void report_metric(
      const StatKey& stat, const std::string& name, const json& value) {
    get(stat).metrics[name] = value;
  }

  void report_timer(
      const StatKey& stat, const std::string& name, const json& value) {
    get(stat).metrics[name] = value;
  }

  /**
   * Write durations to a file for analysis.
   */
  void dump_durations(std::ostream& dump) const {
    json arrays;

    for (const auto& uri : statistics) {
      json array;
      array["uri"] = uri.first;

      for (const auto& qlabel : uri.second) {
        json run;
        run["query"] = qlabel.first;
        for (const auto& config : qlabel.second) {
          json query;
          query["first"] = config.second.durations[0];
          query["sum"] = std::accumulate(
              config.second.durations.begin(),
              config.second.durations.end(),
              (double)(0));

          for (const auto& metric : config.second.metrics) {
            query[metric.first] = metric.second;
          }

          run[config.first] = query;
        }

        array["queries"].push_back(run);
      }

      arrays.push_back(array);
    }

    dump << std::setw(4) << arrays << std::endl;
  }
};

struct SubarrayDimension {
  std::optional<int> start_percent_;
  std::optional<int> end_percent_;

  static SubarrayDimension from_json(const json& jdim) {
    SubarrayDimension dim;
    if (jdim.find("start") != jdim.end()) {
      dim.start_percent_.emplace(jdim["start"]["%"].get<int>());
    }
    if (jdim.find("end") != jdim.end()) {
      dim.end_percent_.emplace(jdim["end"]["%"].get<int>());
    }
    return dim;
  }

  void apply(
      tiledb::Array& array, unsigned dim, tiledb::Subarray& subarray) const {
    // FIXME: make generic domain type
    int64_t non_empty_domain[2], subarray_domain[2];
    std::tie(non_empty_domain[0], non_empty_domain[1]) =
        array.non_empty_domain<int64_t>(dim);

    subarray_domain[0] = non_empty_domain[0];
    subarray_domain[1] = non_empty_domain[1];

    const auto span = (non_empty_domain[1] - non_empty_domain[0] + 1);
    if (start_percent_.has_value()) {
      subarray_domain[0] =
          non_empty_domain[0] + (span * start_percent_.value()) / 100;
    }
    if (end_percent_.has_value()) {
      subarray_domain[1] =
          non_empty_domain[0] + ((span * end_percent_.value()) / 100);
    }

    subarray.add_range(dim, subarray_domain[0], subarray_domain[1]);
  }
};

/**
 * A node of a query condition syntax tree.
 */
class QueryCondition {
  struct Intermediate {
    tiledb::sm::QueryConditionCombinationOp operator_;
    std::vector<QueryCondition> children_;

    tiledb::QueryCondition build(const tiledb::Query& query) const {
      std::optional<tiledb::QueryCondition> combinator;
      for (const auto& arg : children_) {
        if (!combinator.has_value()) {
          combinator.emplace(arg.build(query));
        } else {
          combinator->combine(
              arg.build(query),
              static_cast<tiledb_query_condition_combination_op_t>(operator_));
        }
      }
      return combinator.value();
    }
  };

  struct Atom {
    tiledb::sm::QueryConditionOp operator_;
    std::string field_;
    json value_;

    tiledb::QueryCondition build(const tiledb::Query& query) const {
      const auto& schema = query.array().schema();
      tiledb_datatype_t dt =
          (schema.has_attribute(field_) ?
               schema.attribute(field_).type() :
               schema.domain().dimension(field_).type());

      tiledb::QueryCondition condition(query.ctx());

      if (schema.ptr()->array_schema()->cell_val_num(field_) ==
              tiledb::sm::constants::var_num &&
          (dt == TILEDB_STRING_ASCII || dt == TILEDB_STRING_UTF8)) {
        condition.init(
            field_,
            value_.get<std::string>(),
            static_cast<tiledb_query_condition_op_t>(operator_));
      } else {
        auto do_init = [&](auto arg) {
          using value_type = std::decay_t<std::remove_pointer_t<decltype(arg)>>;
          const value_type value = value_.get<value_type>();
          condition.init(
              field_,
              static_cast<const void*>(&value),
              sizeof(value),
              static_cast<tiledb_query_condition_op_t>(operator_));
        };
        tiledb::type::apply_with_type(
            do_init, static_cast<tiledb::sm::Datatype>(dt));
      }
      return condition;
    }
  };

  std::optional<Intermediate> intermediate_;
  std::optional<Atom> atom_;

 public:
  static QueryCondition from_json(const json& json) {
    const std::string op = json["operator"].get<std::string>();
    if (op == "&&") {
      return intermediate_from_json(
          json, tiledb::sm::QueryConditionCombinationOp::AND);
    } else if (op == "||") {
      return intermediate_from_json(
          json, tiledb::sm::QueryConditionCombinationOp::OR);
    } else if (op == "!") {
      return intermediate_from_json(
          json, tiledb::sm::QueryConditionCombinationOp::NOT);
    } else if (op == "<") {
      return atom_from_json(json, tiledb::sm::QueryConditionOp::LT);
    } else if (op == "<=") {
      return atom_from_json(json, tiledb::sm::QueryConditionOp::LE);
    } else if (op == "=" || op == "==") {
      return atom_from_json(json, tiledb::sm::QueryConditionOp::EQ);
    } else if (op == ">=") {
      return atom_from_json(json, tiledb::sm::QueryConditionOp::GE);
    } else if (op == ">") {
      return atom_from_json(json, tiledb::sm::QueryConditionOp::GT);
    } else if (op == "!=" || op == "<>") {
      return atom_from_json(json, tiledb::sm::QueryConditionOp::NE);
    } else {
      throw std::runtime_error("Invalid 'operator' for query condition: " + op);
    }
  }

  tiledb::QueryCondition build(const tiledb::Query& query) const {
    if (intermediate_.has_value()) {
      return intermediate_->build(query);
    } else {
      return atom_->build(query);
    }
  }

 private:
  static QueryCondition intermediate_from_json(
      const json& json, tiledb::sm::QueryConditionCombinationOp op) {
    Intermediate node;
    node.operator_ = op;
    for (const auto& arg : json["args"]) {
      node.children_.push_back(from_json(arg));
    }

    QueryCondition wrapper;
    wrapper.intermediate_ = node;
    return wrapper;
  }

  static QueryCondition atom_from_json(
      const json& json, tiledb::sm::QueryConditionOp op) {
    Atom atom;
    atom.operator_ = op;
    atom.field_ = json["field"].get<std::string>();
    atom.value_ = json["value"];

    QueryCondition wrapper;
    wrapper.atom_ = atom;
    return wrapper;
  }
};

struct Query {
  std::string label_;
  tiledb_layout_t layout_;
  std::optional<std::vector<std::string>> select_;
  std::vector<std::optional<SubarrayDimension>> subarray_;
  std::optional<QueryCondition> condition_;

  static Query from_json(const json& jq) {
    Query q;
    q.label_ = jq["label"].get<std::string>();

    if (jq.find("select") != jq.end()) {
      q.select_.emplace(std::vector<std::string>());
      for (const auto& field : jq["select"]) {
        q.select_->push_back(field.get<std::string>());
      }
    }

    const auto layout = jq["layout"].get<std::string>();
    if (layout == "global_order") {
      q.layout_ = TILEDB_GLOBAL_ORDER;
    } else if (layout == "row_major") {
      q.layout_ = TILEDB_ROW_MAJOR;
    } else if (layout == "col_major") {
      q.layout_ = TILEDB_COL_MAJOR;
    } else if (layout == "unordered") {
      throw std::runtime_error(
          "TILEDB_UNORDERED is not implemented; the unstable results order "
          "means "
          "we cannot compare the coordinates from interleaved submits. This "
          "can "
          "be implemented by buffering all of the query results and then "
          "sorting "
          "on the coordinates");
    } else {
      throw std::runtime_error("Invalid 'layout' for query: " + layout);
    }

    if (jq.find("subarray") != jq.end()) {
      const auto& jsub = jq["subarray"];
      for (const auto& jdim : jsub) {
        if (jdim.is_null()) {
          q.subarray_.push_back(std::nullopt);
        } else {
          q.subarray_.push_back(SubarrayDimension::from_json(jdim));
        }
      }
    }

    if (jq.find("condition") != jq.end()) {
      const auto& jcondition = jq["condition"];
      q.condition_.emplace(QueryCondition::from_json(jcondition));
    }

    return q;
  }

  /**
   * Applies this configuration to a query.
   */
  void apply(
      tiledb::Context& ctx, tiledb::Array& array, tiledb::Query& query) const {
    query.set_layout(layout_);

    if (!subarray_.empty()) {
      tiledb::Subarray subarray(ctx, array);

      tiledb::ArraySchema schema = array.schema();

      for (unsigned d = 0; d < subarray_.size(); d++) {
        if (subarray_[d].has_value()) {
          subarray_[d].value().apply(array, d, subarray);
        }
      }
      query.set_subarray(subarray);
    }

    if (condition_.has_value()) {
      query.set_condition(condition_->build(query));
    }
  }
};

template <FragmentType Fragment>
tiledb::common::Status assertGlobalOrder(
    const tiledb::Array& array,
    const Fragment& data,
    size_t num_cells,
    size_t start,
    size_t end) {
  tiledb::sm::GlobalCellCmp globalcmp(
      array.ptr()->array()->array_schema_latest().domain());

  for (uint64_t i = start + 1; i < end; i++) {
    const auto prevtuple = std::apply(
        [&]<typename... Ts>(const query_buffers<Ts>&... dims) {
          return global_cell_cmp_std_tuple(std::make_tuple(dims[i - 1]...));
        },
        data.dimensions());
    const auto nexttuple = std::apply(
        [&]<typename... Ts>(const query_buffers<Ts>&... dims) {
          return global_cell_cmp_std_tuple(std::make_tuple(dims[i]...));
        },
        data.dimensions());

    if (globalcmp(nexttuple, prevtuple)) {
      return tiledb::common::Status_Error(
          "Out of order: start= " + std::to_string(start) +
          ", pos=" + std::to_string(i) + ", end=" + std::to_string(end) +
          ", num_cells=" + std::to_string(num_cells));
    }
  }
  return tiledb::common::Status_Ok();
}

template <typename T>
struct require_type {
  static void dimension(const tiledb::sm::Dimension& dim) {
    const auto dt = dim.type();
    const bool is_same = tiledb::type::apply_with_type(
        [](auto arg) { return std::is_same_v<T, decltype(arg)>; }, dt);
    if (!is_same) {
      throw std::runtime_error(
          "Incompatible template type for dimension '" + dim.name() +
          "' of type " + datatype_str(dt));
    }
  }

  static void attribute(const tiledb::sm::Attribute& att) {
    const auto dt = att.type();
    const bool is_same = tiledb::type::apply_with_type(
        [](auto arg) { return std::is_same_v<T, decltype(arg)>; }, dt);
    if (!is_same) {
      throw std::runtime_error(
          "Incompatible template type for attribute '" + att.name() +
          "' of type " + datatype_str(dt));
    }
  }
};

template <FragmentType Fragment>
static void check_compatibility(
    const tiledb::Array& array, const Query& query_config) {
  using DimensionTuple = decltype(std::declval<Fragment>().dimensions());
  using AttributeTuple = decltype(std::declval<Fragment>().attributes());

  constexpr auto expect_num_dims = std::tuple_size_v<DimensionTuple>;
  constexpr auto max_num_atts = std::tuple_size_v<AttributeTuple>;

  const auto& schema = array.ptr()->array()->array_schema_latest();

  if (schema.domain().dim_num() != expect_num_dims) {
    throw std::runtime_error(
        "Expected " + std::to_string(expect_num_dims) + " dimensions, found " +
        std::to_string(schema.domain().dim_num()));
  }
  if (schema.attribute_num() < max_num_atts) {
    throw std::runtime_error(
        "Expected " + std::to_string(max_num_atts) + " attributes, found " +
        std::to_string(schema.attribute_num()));
  }

  unsigned d = 0;
  std::apply(
      [&]<typename... Ts>(query_buffers<Ts>...) {
        (require_type<Ts>::dimension(*schema.domain().shared_dimension(d++)),
         ...);
      },
      stdx::decay_tuple<DimensionTuple>());

  if (query_config.select_.has_value()) {
    unsigned a = 0;
    auto get_attribute = [&](unsigned a) {
      if (a >= query_config.select_.value().size()) {
        throw std::runtime_error(
            "FragmentType and select list do not match: too few "
            "attributes "
            "selected");
      } else {
        return *schema.attribute(query_config.select_.value()[a++]);
      }
    };
    std::apply(
        [&]<typename... Ts>(query_buffers<Ts>...) {
          (require_type<Ts>::attribute(get_attribute(a++)), ...);
        },
        stdx::decay_tuple<AttributeTuple>());
    if (a != query_config.select_.value().size()) {
      throw std::runtime_error(
          "FragmentType and select list do not match: too many attributes "
          "selected");
    }
  } else {
    // select all attributes, or at least the first N based on fragment
    unsigned a = 0;
    std::apply(
        [&]<typename... Ts>(query_buffers<Ts>...) {
          (require_type<Ts>::attribute(*schema.attribute(a++)), ...);
        },
        stdx::decay_tuple<AttributeTuple>());
  }
}

template <FragmentType Fragment>
static void run(
    StatKeeper& stat_keeper,
    const char* array_uri,
    const Query& query_config,
    const std::span<Configuration> configs,
    const std::span<std::string> metrics,
    const std::span<std::string> timers) {
  std::vector<uint64_t> num_user_cells;
  for (const auto& config : configs) {
    num_user_cells.push_back(
        config.num_user_cells_.value_or(1024 * 1024 * 128));
  }

  std::vector<Fragment> qdata(configs.size());

  auto reset = [&](size_t q) {
    std::apply(
        [&](auto&... outfield) { (outfield.resize(num_user_cells[q]), ...); },
        std::tuple_cat(qdata[q].dimensions(), qdata[q].attributes()));
  };
  for (size_t q = 0; q < qdata.size(); q++) {
    reset(q);
  }

  tiledb::Context ctx;

  // Open array for reading.
  tiledb::Array array(ctx, array_uri, TILEDB_READ);
  check_compatibility<Fragment>(array, query_config);

  auto dimension_name = [&](unsigned d) -> std::string {
    return std::string(
        array.ptr()->array_schema_latest().domain().dimension_ptr(d)->name());
  };
  auto attribute_name = [&](unsigned a) -> std::string {
    if (query_config.select_.has_value()) {
      return query_config.select_.value()[a];
    } else {
      return std::string(
          array.ptr()->array_schema_latest().attribute(a)->name());
    }
  };

  std::vector<tiledb::Query> queries;
  for (const auto& config : configs) {
    tiledb::Query query(ctx, array, TILEDB_READ);
    query.set_config(config.qconf_);
    query_config.apply(ctx, array, query);

    queries.push_back(query);
  }

  std::vector<StatKeeper::StatKey> keys;
  for (const auto& config : configs) {
    keys.push_back(StatKeeper::StatKey{
        .uri_ = std::string(array_uri),
        .qlabel_ = query_config.label_,
        .configname_ = config.name_});
  }

  // helper to do basic checks on both
  auto do_submit = [&](auto& key, auto& query, auto& outdata)
      -> std::pair<tiledb::Query::Status, uint64_t> {
    // make field size locations
    auto field_sizes = templates::query::make_field_sizes<Asserter>(outdata);

    // add fields to query
    templates::query::set_fields<Asserter>(
        ctx.ptr().get(),
        query.ptr().get(),
        field_sizes,
        outdata,
        dimension_name,
        attribute_name);

    tiledb::Query::Status status;
    {
      StatKeeper::Timer qtimer = stat_keeper.start_timer(key);
      status = query.submit();
    }

    const uint64_t num_cells =
        templates::query::num_cells<Asserter>(outdata, field_sizes);

    if (num_cells < outdata.size()) {
      // since the user buffer did not fill up the query must be complete
      ASSERTER(status == tiledb::Query::Status::COMPLETE);
    }

    return std::make_pair(status, num_cells);
  };

  while (true) {
    std::vector<tiledb::Query::Status> status;
    std::vector<uint64_t> num_cells;

    for (size_t q = 0; q < queries.size(); q++) {
      auto result = do_submit(keys[q], queries[q], qdata[q]);
      status.push_back(result.first);
      num_cells.push_back(result.second);
    }

    for (size_t q = 0; q < queries.size(); q++) {
      std::apply(
          [&](auto&... outfield) { (outfield.resize(num_cells[0]), ...); },
          std::tuple_cat(qdata[q].dimensions(), qdata[q].attributes()));

      ASSERTER(num_cells[q] <= num_user_cells[q]);

      if (q > 0) {
        ASSERTER(num_cells[q] == num_cells[0]);
        ASSERTER(qdata[q].dimensions() == qdata[0].dimensions());

        // NB: this is only correct if there are no duplicate coordinates,
        // in which case we need to adapt what CSparseGlobalOrderFx::run does
        ASSERTER(qdata[q].attributes() == qdata[0].attributes());
      }
    }

    for (size_t q = 0; q < queries.size(); q++) {
      reset(q);
    }

    for (size_t q = 1; q < queries.size(); q++) {
      ASSERTER(status[q] == status[0]);
    }

    if (queries[0].query_layout() == TILEDB_GLOBAL_ORDER) {
      // assert that the results arrive in global order
      // do it in parallel, it is slow
      auto* tp = ctx.ptr()->context().compute_tp();
      const size_t parallel_factor = std::max<uint64_t>(
          1, std::min<uint64_t>(tp->concurrency_level(), num_cells[0] / 1024));
      const size_t items_per = num_cells[0] / parallel_factor;
      const auto isGlobalOrder =
          tiledb::sm::parallel_for(tp, 0, parallel_factor, [&](uint64_t i) {
            const uint64_t mystart = i * items_per;
            const uint64_t myend =
                std::min((i + 1) * items_per + 1, num_cells[0]);
            return assertGlobalOrder(
                array, qdata[0], num_cells[0], mystart, myend);
          });
      if (!isGlobalOrder.ok()) {
        throw std::runtime_error(isGlobalOrder.to_string());
      }
    }

    if (status[0] == tiledb::Query::Status::COMPLETE) {
      break;
    }
  }

  // record additional stats
  for (size_t q = 0; q < queries.size(); q++) {
    const tiledb::sm::Query& query_internal = *queries[q].ptr().get()->query_;
    const auto& stats = *query_internal.stats();

    for (const auto& metric : metrics) {
      stat_keeper.report_metric(
          keys[q], metric, optional_json(stats.find_counter(metric)));
    }
    for (const auto& timer : timers) {
      stat_keeper.report_timer(
          keys[q], timer, optional_json(stats.find_timer(timer + ".sum")));
    }
  }
}

// change this to match the schema of the target arrays
using Fragment = Fragment2D<int64_t, int64_t, int64_t>;

/**
 * Reads key-value pairs from a JSON object to construct and return a
 * `tiledb_config_t`.
 */
tiledb::Config json2config(const json& j) {
  std::map<std::string, std::string> params;
  const json jconf = j["config"];
  for (auto it = jconf.begin(); it != jconf.end(); ++it) {
    const auto key = it.key();
    const auto value =
        (it.value().is_string() ? it.value().get<std::string>() :
                                  nlohmann::to_string(it.value()));
    params[key] = value;
  }

  return tiledb::Config(params);
}

int main(int argc, char** argv) {
  json config;
  {
    std::ifstream configfile(argv[1]);
    if (configfile.fail()) {
      std::cerr << "Error opening config file: " << argv[1] << std::endl;
      return -1;
    }
    config = json::parse(
        std::istreambuf_iterator<char>(configfile),
        std::istreambuf_iterator<char>());
  }

  std::vector<Configuration> qconfs;
  for (const auto& jsoncfg : config["configurations"]) {
    Configuration cfg;
    cfg.name_ = jsoncfg["name"].get<std::string>();
    cfg.qconf_ = json2config(jsoncfg);
    if (jsoncfg.find("num_user_cells") != jsoncfg.end()) {
      cfg.num_user_cells_.emplace(jsoncfg["num_user_cells"].get<uint64_t>());
    }
    if (jsoncfg.find("memory_budget") != jsoncfg.end()) {
      cfg.memory_budget_from_json(jsoncfg["memory_budget"]);
    }
    qconfs.push_back(cfg);
  }

  std::vector<std::string> metrics;
  if (config.find("metrics") != config.end()) {
    for (const auto& jmetric : config["metrics"]) {
      metrics.push_back(jmetric.get<std::string>());
    }
  }
  std::vector<std::string> timers;
  if (config.find("timers") != config.end()) {
    for (const auto& jtimer : config["timers"]) {
      timers.push_back(jtimer.get<std::string>());
    }
  }

  StatKeeper stat_keeper;

  std::span<const char* const> array_uris(
      static_cast<const char* const*>(&argv[2]), argc - 2);

  for (const auto& query : config["queries"]) {
    Query qq = Query::from_json(query);

    for (const auto& array_uri : array_uris) {
      try {
        run<Fragment>(stat_keeper, array_uri, qq, qconfs, metrics, timers);
      } catch (const std::exception& e) {
        std::cerr << "Error on array \"" << array_uri << "\": " << e.what()
                  << std::endl;

        return 1;
      }
    }
  }

  std::ofstream out("/tmp/tiledb_submit_a_b.json");
  stat_keeper.dump_durations(out);

  return 0;
}
