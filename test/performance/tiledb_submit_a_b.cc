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
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
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

  Fragment a;
  Fragment b;

  auto reset = [num_user_cells](Fragment& buf) {
    std::apply(
        [&](auto&... outfield) { (outfield.resize(num_user_cells), ...); },
        std::tuple_cat(buf.dimensions(), buf.attributes()));
  };
  reset(a);
  reset(b);

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
    auto dimension_sizes = []<typename... Ds>(std::tuple<Ds...> dimensions) {
      return query_applicator<Asserter, Ds...>::make_field_sizes(dimensions);
    }(outdata.dimensions());
    auto attribute_sizes = []<typename... As>(std::tuple<As...> attributes) {
      return query_applicator<Asserter, As...>::make_field_sizes(attributes);
    }(outdata.attributes());

    // add fields to query
    [&]<typename... Ds>(std::tuple<Ds...> dims) {
      query_applicator<Asserter, Ds...>::set(
          ctx, query, dimension_sizes, dims, dimension_name);
    }(outdata.dimensions());
    [&]<typename... As>(std::tuple<As...> atts) {
      query_applicator<Asserter, As...>::set(
          ctx, query, attribute_sizes, atts, attribute_name);
    }(outdata.attributes());

    {
      tiledb::sm::stats::DurationInstrument<TimeKeeper> qtimer =
          time_keeper.start_timer(key);
      TRY(ctx, tiledb_query_submit(ctx, query));
    }

    tiledb_query_status_t status;
    TRY(ctx, tiledb_query_get_status(ctx, query, &status));

    const uint64_t dim_num_cells = [&]<typename... Ds>(auto dims) {
      return query_applicator<Asserter, Ds...>::num_cells(
          dims, dimension_sizes);
    }(outdata.dimensions());
    const uint64_t att_num_cells = [&]<typename... As>(auto atts) {
      return query_applicator<Asserter, As...>::num_cells(
          atts, attribute_sizes);
    }(outdata.attributes());

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

    std::tie(a_status, a_num_cells) = do_submit(a_key, a_query, a);
    std::tie(b_status, b_num_cells) = do_submit(b_key, b_query, b);

    ASSERTER(a_num_cells == b_num_cells);

    std::apply(
        [&](auto&... outfield) { (outfield.resize(a_num_cells), ...); },
        std::tuple_cat(a.dimensions(), a.attributes()));
    std::apply(
        [&](auto&... outfield) { (outfield.resize(b_num_cells), ...); },
        std::tuple_cat(b.dimensions(), b.attributes()));

    ASSERTER(a.dimensions() == b.dimensions());

    // NB: this is only correct if there are no duplicate coordinates,
    // in which case we need to adapt what CSparseGlobalOrderFx::run does
    ASSERTER(a.attributes() == b.attributes());

    reset(a);
    reset(b);

    ASSERTER(a_status == b_status);
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
