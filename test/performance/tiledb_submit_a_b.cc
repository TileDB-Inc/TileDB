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
 *   "query": {
 *     "layout": "global_order",
 *     "subarray": [
 *       {
 *         "%": 30
 *       }
 *     ]
 *   }
 * }
 * ```
 *
 * The paths "a.config" and "b.config" are sets of key-value pairs
 * which are used to configure the "A" and "B" queries respectively.
 *
 * The "query.layout" path is required and sets the results order.
 *
 * The "query.subarray" path is an optional list of range specifications on
 * each dimension. Currently the only supported specification is
 * an object with a field "%" which adds a subarray range whose
 * bounds cover the specified percent of the non-empty domain
 * for that dimension.
 */
#include <test/support/src/array_helpers.h>
#include <test/support/src/array_templates.h>
#include <test/support/src/error_helpers.h>

#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/query/query_api_internal.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/stats/duration_instrument.h"

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
  std::map<std::string, std::vector<double>> durations;

  tiledb::sm::stats::DurationInstrument<TimeKeeper> start_timer(
      const std::string& stat) {
    return tiledb::sm::stats::DurationInstrument<TimeKeeper>(*this, stat);
  }

  void report_duration(
      const std::string& stat, const std::chrono::duration<double> duration) {
    durations[stat].push_back(duration.count());
  }

  /**
   * Write durations to a file for analysis.
   */
  void dump_durations(const char* path) const {
    std::ofstream dump(path);

    for (const auto& stat : durations) {
      dump << stat.first << " = [";

      bool is_first = true;
      for (const auto& duration : stat.second) {
        if (!is_first) {
          dump << ", ";
        } else {
          is_first = false;
        }
        dump << duration;
      }

      dump << "]" << std::endl << std::endl;
    }
  }
};

/**
 * Constructs the query using the configuration's common "query" options.
 */
capi_return_t construct_query(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_t* query,
    const json& jq) {
  const auto layout = jq["layout"].get<std::string>();
  if (layout == "global_order") {
    TRY(ctx, tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER));
  } else if (layout == "row_major") {
    TRY(ctx, tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
  } else if (layout == "col_major") {
    TRY(ctx, tiledb_query_set_layout(ctx, query, TILEDB_COL_MAJOR));
  } else if (layout == "unordered") {
    throw std::runtime_error(
        "TILEDB_UNORDERED is not implemented; the unstable results order means "
        "we cannot compare the coordinates from interleaved submits. This can "
        "be implemented by buffering all of the query results and then sorting "
        "on the coordinates");
  } else {
    throw std::runtime_error("Invalid 'layout' for query: " + layout);
  }

  if (jq.find("subarray") != jq.end()) {
    tiledb_subarray_t* subarray;
    RETURN_IF_ERR(tiledb_subarray_alloc(ctx, array, &subarray));

    tiledb_array_schema_t* schema;
    RETURN_IF_ERR(tiledb_array_get_schema(ctx, array, &schema));

    tiledb_domain_t* domain;
    RETURN_IF_ERR(tiledb_array_schema_get_domain(ctx, schema, &domain));

    int d = 0;
    const auto& jsub = jq["subarray"];
    for (const auto& jdim : jsub) {
      if (jdim.find("%") != jdim.end()) {
        const int percent = jdim["%"].get<int>();

        // FIXME: make generic
        int is_empty;
        int64_t domain[2];
        RETURN_IF_ERR(tiledb_array_get_non_empty_domain_from_index(
            ctx, array, d, &domain[0], &is_empty));
        if (is_empty) {
          continue;
        }

        const auto span = (domain[1] - domain[0] + 1);
        domain[0] += ((span * (100 - percent)) / 100) / 2;
        domain[1] -= ((span * (100 - percent)) / 100) / 2;

        RETURN_IF_ERR(tiledb_subarray_add_range(
            ctx, subarray, d, &domain[0], &domain[1], 0));
      }
    }

    tiledb_query_set_subarray_t(ctx, query, subarray);
  }

  return TILEDB_OK;
}

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
          "Out of order: pos=" + std::to_string(i) +
          ", num_cells=" + std::to_string(num_cells));
    }
  }
  return tiledb::common::Status_Ok();
}

template <FragmentType Fragment>
static void run(
    TimeKeeper& time_keeper,
    const char* array_uri,
    const json& query_config,
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
  construct_query(ctx, array, a_query, query_config);

  // Create query which DOES do merge
  tiledb_query_t* b_query;
  TRY(ctx, tiledb_query_alloc(ctx, array, TILEDB_READ, &b_query));
  TRY(ctx, tiledb_query_set_config(ctx, b_query, b_config));
  construct_query(ctx, array, b_query, query_config);

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
      tiledb::sm::stats::DurationInstrument<TimeKeeper> qtimer =
          time_keeper.start_timer(key);
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

  const std::string a_key = std::string(array_uri) + ".a";
  const std::string b_key = std::string(array_uri) + ".b";

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

  for (const auto& array_uri : array_uris) {
    try {
      run<Fragment>(time_keeper, array_uri, config["query"], a_conf, b_conf);
    } catch (const std::exception& e) {
      std::cerr << "Error on array \"" << array_uri << "\": " << e.what()
                << std::endl;
    }
  }

  tiledb_config_free(&b_conf);
  tiledb_config_free(&a_conf);

  time_keeper.dump_durations("/tmp/time_keeper.out");

  return 0;
}
