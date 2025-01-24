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
 * For each array in the argument list, two queries are created:
 * one with configuration "A" and one with configuration "B".
 * The `run()` function alternates taking a step (`tiledb_query_submit`)
 * with the "A" query and the "B" query, checking that both have the
 * same results and recording the time spent calling `tiledb_query_submit`.
 *
 * After executing upon all of the arrays, the timing information is
 * dumped to `/tmp/time_keeper.out`.
 *
 * The arrays must have the same schema, whose physical type is reflected
 * by the `using Fragment = <FragmentType>` declaration in the middle of
 * this file. The `using Fragment` declaration sets the types of the buffers
 * which are used to read the array.
 *
 * The nature of the "A" and "B" configurations are specified via the required
 * argument `config.json`. The following example illustrates the currently
 * supported keys:
 *
 * ```
 * {
 *   "a": {
 *     "config": {
 *       "sm.query.sparse_global_order.preprocess_tile_merge": 0
 *     }
 *   },
 *   "b": {
 *     "config": {
 *       "sm.query.sparse_global_order.preprocess_tile_merge": 128
 *     }
 *   },
 *   "queries": [
 *     {
 *       "layout": "global_order",
 *       "subarray": [
 *         {
 *           "%": 30
 *         }
 *       ]
 *     }
 *   ]
 * }
 * ```
 *
 * The paths "a.config" and "b.config" are sets of key-value pairs
 * which are used to configure the "A" and "B" queries respectively.
 *
 * Each item in "queries" specifies a query to run the comparison for.
 *
 * The "layout" field is required and sets the results order.
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
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/query/query_api_internal.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/stats/duration_instrument.h"
#include "tiledb/type/apply_with_type.h"

#include "external/include/nlohmann/json.hpp"

#include <fstream>
#include <iostream>
#include <span>

using namespace tiledb::test;
using namespace tiledb::test::templates;

using Asserter = AsserterRuntimeException;

using json = nlohmann::json;

/**
 * Records durations as reported by `tiledb::sm::stats::DurationInstrument`.
 */
struct TimeKeeper {
  struct StatKey {
    std::string uri_;
    std::string qlabel_;
    bool is_a_;
  };

  using Timer = tiledb::sm::stats::DurationInstrument<TimeKeeper, StatKey>;

  std::map<
      std::string,
      std::
          map<std::string, std::pair<std::vector<double>, std::vector<double>>>>
      durations;

  Timer start_timer(const StatKey& stat) {
    return tiledb::sm::stats::DurationInstrument<TimeKeeper, StatKey>(
        *this, stat);
  }

  void report_duration(
      const StatKey& stat, const std::chrono::duration<double> duration) {
    auto& stats = durations[stat.uri_][stat.qlabel_];
    if (stat.is_a_) {
      stats.first.push_back(duration.count());
    } else {
      stats.second.push_back(duration.count());
    }
  }

  /**
   * Write durations to a file for analysis.
   */
  void dump_durations(const char* path) const {
    std::ofstream dump(path);

    json arrays;

    for (const auto& uri : durations) {
      json array;
      array["uri"] = uri.first;

      for (const auto& qlabel : uri.second) {
        const auto& a_durations = qlabel.second.first;
        const auto& b_durations = qlabel.second.second;
        const double a_sum =
            std::accumulate(a_durations.begin(), a_durations.end(), (double)0);
        const double b_sum =
            std::accumulate(b_durations.begin(), b_durations.end(), (double)0);

        json a;
        a["first"] = a_durations[0];
        a["sum"] = a_sum;

        json b;
        b["first"] = b_durations[0];
        b["sum"] = b_sum;

        json run;
        run["query"] = qlabel.first;
        run["a"] = a;
        run["b"] = b;

        array["queries"].push_back(run);
      }

      arrays.push_back(array);
    }

    dump << std::setw(4) << arrays << std::endl;
  }
};

capi_return_t shrink_domain(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    unsigned dim,
    tiledb_subarray_t* subarray,
    int percent) {
  // FIXME: make generic
  int is_empty;
  int64_t domain[2];
  RETURN_IF_ERR(tiledb_array_get_non_empty_domain_from_index(
      ctx, array, dim, &domain[0], &is_empty));
  if (is_empty) {
    return TILEDB_OK;
  }

  const auto span = (domain[1] - domain[0] + 1);
  domain[0] += ((span * (100 - percent)) / 100) / 2;
  domain[1] -= ((span * (100 - percent)) / 100) / 2;

  return tiledb_subarray_add_range(
      ctx, subarray, dim, &domain[0], &domain[1], 0);
}

struct QueryConfig {
  std::string label_;
  tiledb_layout_t layout_;
  std::vector<std::optional<int>> subarray_percent_;

  static QueryConfig from_json(const json& jq) {
    QueryConfig q;
    q.label_ = jq["label"].get<std::string>();

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
        if (jdim.find("%") != jdim.end()) {
          const int percent = jdim["%"].get<int>();
          q.subarray_percent_.push_back(percent);
        } else {
          q.subarray_percent_.push_back(std::nullopt);
        }
      }
    }
    return q;
  }

  /**
   * Applies this configuration to a query.
   */
  capi_return_t apply(
      tiledb_ctx_t* ctx, tiledb_array_t* array, tiledb_query_t* query) const {
    TRY(ctx, tiledb_query_set_layout(ctx, query, layout_));

    if (!subarray_percent_.empty()) {
      tiledb_subarray_t* subarray;
      RETURN_IF_ERR(tiledb_subarray_alloc(ctx, array, &subarray));

      tiledb_array_schema_t* schema;
      RETURN_IF_ERR(tiledb_array_get_schema(ctx, array, &schema));

      tiledb_domain_t* domain;
      RETURN_IF_ERR(tiledb_array_schema_get_domain(ctx, schema, &domain));

      for (unsigned d = 0; d < subarray_percent_.size(); d++) {
        if (subarray_percent_[d].has_value()) {
          TRY(ctx,
              shrink_domain(
                  ctx, array, d, subarray, subarray_percent_[d].value()));
        }
      }

      TRY(ctx, tiledb_query_set_subarray_t(ctx, query, subarray));
    }

    return TILEDB_OK;
  }
};

template <FragmentType Fragment>
tiledb::common::Status assertGlobalOrder(
    tiledb_array_t* array,
    const Fragment& data,
    size_t num_cells,
    size_t start,
    size_t end) {
  tiledb::sm::GlobalCellCmp globalcmp(
      array->array()->array_schema_latest().domain());

  for (uint64_t i = start + 1; i < end; i++) {
    const auto prevtuple = std::apply(
        [&]<typename... Ts>(const std::vector<Ts>&... dims) {
          return global_cell_cmp_std_tuple(std::make_tuple(dims[i - 1]...));
        },
        data.dimensions());
    const auto nexttuple = std::apply(
        [&]<typename... Ts>(const std::vector<Ts>&... dims) {
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
static void check_compatibility(tiledb_array_t* array) {
  using DimensionTuple = decltype(std::declval<Fragment>().dimensions());
  using AttributeTuple = decltype(std::declval<Fragment>().attributes());

  constexpr auto expect_num_dims = std::tuple_size_v<DimensionTuple>;
  constexpr auto max_num_atts = std::tuple_size_v<AttributeTuple>;

  const auto& schema = array->array()->array_schema_latest();

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
      [&]<typename... Ts>(std::vector<Ts>...) {
        (require_type<Ts>::dimension(*schema.domain().shared_dimension(d++)),
         ...);
      },
      stdx::decay_tuple<DimensionTuple>());

  unsigned a = 0;
  std::apply(
      [&]<typename... Ts>(std::vector<Ts>...) {
        (require_type<Ts>::attribute(*schema.attribute(a++)), ...);
      },
      stdx::decay_tuple<AttributeTuple>());
}

template <FragmentType Fragment>
static void run(
    TimeKeeper& time_keeper,
    const char* array_uri,
    const QueryConfig& query_config,
    tiledb_config_t* a_config,
    tiledb_config_t* b_config) {
  tiledb::test::SparseGlobalOrderReaderMemoryBudget memory;
  memory.total_budget_ = std::to_string(1024 * 1024 * 1024);
  memory.ratio_tile_ranges_ = "0.01";

  const uint64_t num_user_cells = 1024 * 1024 * 128;

  Fragment a_data;
  Fragment b_data;

  auto reset = [num_user_cells](Fragment& buf) {
    std::apply(
        [&](auto&... outfield) { (outfield.resize(num_user_cells), ...); },
        std::tuple_cat(buf.dimensions(), buf.attributes()));
  };
  reset(a_data);
  reset(b_data);

  tiledb_config_t* config;
  {
    tiledb_error_t* error = nullptr;
    ASSERTER(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  }
  memory.apply(config);

  tiledb_ctx_t* ctx;
  ASSERTER(tiledb_ctx_alloc(config, &ctx) == TILEDB_OK);

  tiledb_config_free(&config);

  // Open array for reading.
  CApiArray array(ctx, array_uri, TILEDB_READ);
  check_compatibility<Fragment>(array);

  auto dimension_name = [&](unsigned d) -> std::string {
    return std::string(
        array->array_schema_latest().domain().dimension_ptr(d)->name());
  };
  auto attribute_name = [&](unsigned a) -> std::string {
    return std::string(array->array_schema_latest().attribute(a)->name());
  };

  // Create query which does NOT do merge
  tiledb_query_t* a_query;
  TRY(ctx, tiledb_query_alloc(ctx, array, TILEDB_READ, &a_query));
  TRY(ctx, tiledb_query_set_config(ctx, a_query, a_config));
  TRY(ctx, query_config.apply(ctx, array, a_query));

  // Create query which DOES do merge
  tiledb_query_t* b_query;
  TRY(ctx, tiledb_query_alloc(ctx, array, TILEDB_READ, &b_query));
  TRY(ctx, tiledb_query_set_config(ctx, b_query, b_config));
  TRY(ctx, query_config.apply(ctx, array, b_query));

  const TimeKeeper::StatKey a_key = {
      .uri_ = std::string(array_uri),
      .qlabel_ = query_config.label_,
      .is_a_ = true,
  };
  const TimeKeeper::StatKey b_key = {
      .uri_ = std::string(array_uri),
      .qlabel_ = query_config.label_,
      .is_a_ = false,
  };

  // helper to do basic checks on both
  auto do_submit = [&](auto& key, auto& query, auto& outdata)
      -> std::pair<tiledb_query_status_t, uint64_t> {
    // make field size locations
    auto dimension_sizes =
        templates::query::make_field_sizes<Asserter>(outdata.dimensions());
    auto attribute_sizes =
        templates::query::make_field_sizes<Asserter>(outdata.attributes());

    // add fields to query
    templates::query::set_fields<Asserter>(
        ctx, query, dimension_sizes, outdata.dimensions(), dimension_name);
    templates::query::set_fields<Asserter>(
        ctx, query, attribute_sizes, outdata.attributes(), attribute_name);

    {
      TimeKeeper::Timer qtimer = time_keeper.start_timer(key);
      TRY(ctx, tiledb_query_submit(ctx, query));
    }

    tiledb_query_status_t status;
    TRY(ctx, tiledb_query_get_status(ctx, query, &status));

    const uint64_t dim_num_cells = templates::query::num_cells<Asserter>(
        outdata.dimensions(), dimension_sizes);
    const uint64_t att_num_cells = templates::query::num_cells<Asserter>(
        outdata.attributes(), attribute_sizes);

    ASSERTER(dim_num_cells == att_num_cells);

    if (dim_num_cells < outdata.size()) {
      // since the user buffer did not fill up the query must be complete
      ASSERTER(status == TILEDB_COMPLETED);
    }

    return std::make_pair(status, dim_num_cells);
  };

  while (true) {
    tiledb_query_status_t a_status, b_status;
    uint64_t a_num_cells, b_num_cells;

    std::tie(a_status, a_num_cells) = do_submit(a_key, a_query, a_data);
    std::tie(b_status, b_num_cells) = do_submit(b_key, b_query, b_data);

    ASSERTER(a_num_cells == b_num_cells);
    ASSERTER(a_num_cells <= num_user_cells);

    std::apply(
        [&](auto&... outfield) { (outfield.resize(a_num_cells), ...); },
        std::tuple_cat(a_data.dimensions(), a_data.attributes()));
    std::apply(
        [&](auto&... outfield) { (outfield.resize(b_num_cells), ...); },
        std::tuple_cat(b_data.dimensions(), b_data.attributes()));

    ASSERTER(a_data.dimensions() == b_data.dimensions());

    // NB: this is only correct if there are no duplicate coordinates,
    // in which case we need to adapt what CSparseGlobalOrderFx::run does
    ASSERTER(a_data.attributes() == b_data.attributes());

    reset(a_data);
    reset(b_data);

    ASSERTER(a_status == b_status);

    if (a_query->query_->layout() == tiledb::sm::Layout::GLOBAL_ORDER) {
      // assert that the results arrive in global order
      // do it in parallel, it is slow
      auto* tp = ctx->context().compute_tp();
      const size_t parallel_factor = std::max<uint64_t>(
          1, std::min<uint64_t>(tp->concurrency_level(), a_num_cells / 1024));
      const size_t items_per = a_num_cells / parallel_factor;
      const auto isGlobalOrder = tiledb::sm::parallel_for(
          ctx->context().compute_tp(), 0, parallel_factor, [&](uint64_t i) {
            const uint64_t mystart = i * items_per;
            const uint64_t myend =
                std::min((i + 1) * items_per + 1, a_num_cells);
            return assertGlobalOrder(
                array, a_data, a_num_cells, mystart, myend);
          });
      if (!isGlobalOrder.ok()) {
        throw std::runtime_error(isGlobalOrder.to_string());
      }
    }

    if (a_status == TILEDB_COMPLETED) {
      break;
    }
  }
}

// change this to match the schema of the target arrays
using Fragment = Fragment2D<int64_t, int64_t, int64_t>;

/**
 * Reads key-value pairs from a JSON object to construct and return a
 * `tiledb_config_t`.
 */
capi_return_t json2config(tiledb_config_t** config, const json& j) {
  tiledb_error_t* error;
  RETURN_IF_ERR(tiledb_config_alloc(config, &error));
  const json jconf = j["config"];
  for (auto it = jconf.begin(); it != jconf.end(); ++it) {
    const auto key = it.key();
    const auto value = nlohmann::to_string(it.value());
    RETURN_IF_ERR(
        tiledb_config_set(*config, key.c_str(), value.c_str(), &error));
  }

  return TILEDB_OK;
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

  tiledb_config_t *a_conf, *b_conf;
  RETURN_IF_ERR(json2config(&a_conf, config["a"]));
  RETURN_IF_ERR(json2config(&b_conf, config["b"]));

  TimeKeeper time_keeper;

  std::span<const char* const> array_uris(
      static_cast<const char* const*>(&argv[2]), argc - 2);

  for (const auto& query : config["queries"]) {
    QueryConfig qq = QueryConfig::from_json(query);

    for (const auto& array_uri : array_uris) {
      try {
        run<Fragment>(time_keeper, array_uri, qq, a_conf, b_conf);
      } catch (const std::exception& e) {
        std::cerr << "Error on array \"" << array_uri << "\": " << e.what()
                  << std::endl;

        return 1;
      }
    }
  }

  tiledb_config_free(&b_conf);
  tiledb_config_free(&a_conf);

  time_keeper.dump_durations("/tmp/time_keeper.out");

  return 0;
}
