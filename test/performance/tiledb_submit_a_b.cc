#include <test/support/src/array_helpers.h>
#include <test/support/src/array_templates.h>
#include <test/support/src/error_helpers.h>

#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/stats/duration_instrument.h"

#include <fstream>
#include <span>

using namespace tiledb::test;
using namespace tiledb::test::templates;

using Asserter = AsserterRuntimeException;

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

template <FragmentType Fragment>
static void run(
    TimeKeeper& time_keeper,
    const char* array_uri,
    tiledb_config_t* a_config,
    tiledb_config_t* b_config) {
  tiledb::test::SparseGlobalOrderReaderMemoryBudget memory;
  memory.total_budget_ = std::to_string(1024 * 1024 * 1024);
  memory.ratio_tile_ranges_ = "0.01";

  const uint64_t num_user_cells = 1024 * 1024;

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
  TRY(ctx, tiledb_query_set_layout(ctx, a_query, TILEDB_GLOBAL_ORDER));
  TRY(ctx, tiledb_query_set_config(ctx, a_query, a_config));

  // Create query which DOES do merge
  tiledb_query_t* b_query;
  TRY(ctx, tiledb_query_alloc(ctx, array, TILEDB_READ, &b_query));
  TRY(ctx, tiledb_query_set_layout(ctx, b_query, TILEDB_GLOBAL_ORDER));
  TRY(ctx, tiledb_query_set_config(ctx, b_query, b_config));

  // helper to do basic checks on both
  auto do_submit = [&](auto& key, auto& query, auto& outdata) -> uint64_t {
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
      ASSERTER(status == TILEDB_COMPLETED);
    }

    return dim_num_cells;
  };

  const std::string a_key = std::string(array_uri) + ".a";
  const std::string b_key = std::string(array_uri) + ".b";

  while (true) {
    const uint64_t a_num_cells = do_submit(a_key, a_query, a);
    const uint64_t on_num_cells = do_submit(b_key, b_query, b);

    ASSERTER(a_num_cells == on_num_cells);

    std::apply(
        [&](auto&... outfield) { (outfield.resize(a_num_cells), ...); },
        std::tuple_cat(a.dimensions(), a.attributes()));
    std::apply(
        [&](auto&... outfield) { (outfield.resize(on_num_cells), ...); },
        std::tuple_cat(b.dimensions(), b.attributes()));

    ASSERTER(a.dimensions() == b.dimensions());

    // NB: this is only correct if there are no duplicate coordinates,
    // in which case we need to adapt what CSparseGlobalOrderFx::run does
    ASSERTER(a.attributes() == b.attributes());

    reset(a);
    reset(b);

    if (a_num_cells < num_user_cells) {
      break;
    }
  }
}

using Fragment = Fragment2D<int64_t, int64_t, float>;

/**
 * Runs sparse global order reader on a 2D array
 * with two `int64_t` dimensions and a `float` attribute.
 * This schema is common in SOMA, comparing the results
 * from "preprocess merge off" and "preprocess merge on".
 *
 * The time of each `tiledb_query_submit` is recorded
 * for both variations, and then dumped to `/tmp/time_keeper.out`
 * when the test is completed.
 */
int main(int argc, char** argv) {
  tiledb_error_t* error;

  tiledb_config_t* a_conf;
  RETURN_IF_ERR(tiledb_config_alloc(&a_conf, &error));
  RETURN_IF_ERR(tiledb_config_set(
      a_conf,
      "sm.query.sparse_global_order.preprocess_tile_merge",
      "0",
      &error));

  tiledb_config_t* b_conf;
  RETURN_IF_ERR(tiledb_config_alloc(&b_conf, &error));
  RETURN_IF_ERR(tiledb_config_set(
      b_conf,
      "sm.query.sparse_global_order.preprocess_tile_merge",
      "128",
      &error));

  TimeKeeper time_keeper;

  std::span<const char* const> array_uris(
      static_cast<const char* const*>(&argv[1]), argc - 1);

  for (const auto& array_uri : array_uris) {
    run<Fragment>(time_keeper, array_uri, a_conf, b_conf);
  }

  tiledb_config_free(&b_conf);
  tiledb_config_free(&a_conf);

  time_keeper.dump_durations("/tmp/time_keeper.out");

  return 0;
}

