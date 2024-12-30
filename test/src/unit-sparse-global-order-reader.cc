/**
 * @file unit-sparse-global-order-reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * Tests for the sparse global order reader.
 */

#include "test/support/rapidcheck/array.h"
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/readers/result_tile.h"
#include "tiledb/sm/query/readers/sparse_global_order_reader.h"
#include "tiledb/sm/stats/duration_instrument.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

#include <fstream>
#include <numeric>

using namespace tiledb;
using namespace tiledb::test;

using tiledb::sm::Datatype;
using tiledb::test::tdbrc::AttributeType;
using tiledb::test::tdbrc::DimensionType;
using tiledb::test::tdbrc::FragmentType;

namespace rc {
Gen<std::vector<tdbrc::Domain<int>>> make_subarray_1d(
    const tdbrc::Domain<int>& domain);
}

/* ********************************* */
/*         STRUCT DEFINITION         */
/* ********************************* */

/**
 * Contains the data for a single fragment
 * of the 1D array created by `CSparseGlobalOrderFx`
 */
struct FxFragment1D {
  std::vector<int> coords;
  std::vector<int> atts;

  uint64_t size() const {
    return coords.size();
  }

  std::tuple<std::vector<int>&> dimensions() {
    return std::tuple<std::vector<int>&>(coords);
  }

  std::tuple<const std::vector<int>&> dimensions() const {
    return std::tuple<const std::vector<int>&>(coords);
  }

  std::tuple<std::vector<int>&> attributes() {
    return std::tuple<std::vector<int>&>(atts);
  }

  std::tuple<const std::vector<int>&> attributes() const {
    return std::tuple<const std::vector<int>&>(atts);
  }
};

struct MemoryBudget {
  std::string total_budget_;
  std::string ratio_tile_ranges_;
  std::string ratio_array_data_;
  std::string ratio_coords_;

  MemoryBudget()
      : total_budget_("1048576")
      , ratio_tile_ranges_("0.1")
      , ratio_array_data_("0.1")
      , ratio_coords_("0.5") {
  }
};

/**
 * Options for configuring the CSparseGlobalFx default 1D array
 */
struct DefaultArray1DConfig {
  uint64_t capacity;
  bool allow_dups;

  tdbrc::Dimension<Datatype::INT32> dimension;

  DefaultArray1DConfig()
      : capacity(2)
      , allow_dups(false) {
    dimension.domain.lower_bound = 1;
    dimension.domain.upper_bound = 200;
    dimension.extent = 2;
  }

  DefaultArray1DConfig with_allow_dups(bool allow_dups) const {
    auto copy = *this;
    copy.allow_dups = allow_dups;
    return copy;
  }
};

/**
 * An instance of one-dimension array input to `CSparseGlobalOrderFx::run`
 */
struct FxRun1D {
  uint64_t num_user_cells;
  std::vector<FxFragment1D> fragments;

  // NB: for now this always has length 1, global order query does not
  // support multi-range subarray
  std::vector<tdbrc::Domain<int>> subarray;

  DefaultArray1DConfig array;
  MemoryBudget memory;

  bool accept(const FxFragment1D& fragment, int record) const {
    if (subarray.empty()) {
      return true;
    } else {
      const int coord = fragment.coords[record];
      for (const auto& range : subarray) {
        if (range.contains(coord)) {
          return true;
        }
      }
      return false;
    }
  }

  bool intersects(const sm::NDRange& mbr) const {
    auto accept = [&](const auto& range) {
      const auto& untyped_mbr = mbr[0];
      const tdbrc::Domain<int> typed_mbr(
          untyped_mbr.start_as<int>(), untyped_mbr.end_as<int>());
      return range.intersects(typed_mbr);
    };
    return subarray.empty() ||
           std::any_of(subarray.begin(), subarray.end(), accept);
  }

  std::tuple<const tdbrc::Dimension<Datatype::INT32>&> dimensions() const {
    return std::tuple<const tdbrc::Dimension<Datatype::INT32>&>(
        array.dimension);
  }

  std::tuple<Datatype> attributes() const {
    return std::make_tuple(Datatype::INT32);
  }
};

/**
 * RAII to make sure we close our arrays so that keeping the same array URI
 * open from one test to the next doesn't muck things up
 * (this is especially important for rapidcheck)
 */
struct CApiArray {
  tiledb_ctx_t* ctx_;
  tiledb_array_t* array_;

  CApiArray()
      : ctx_(nullptr)
      , array_(nullptr) {
  }

  CApiArray(tiledb_ctx_t* ctx, const char* uri, tiledb_query_type_t mode)
      : ctx_(ctx)
      , array_(nullptr) {
    auto rc = tiledb_array_alloc(ctx, uri, &array_);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx, array_, mode);
    REQUIRE(rc == TILEDB_OK);
  }

  CApiArray(CApiArray&& from)
      : ctx_(from.ctx_)
      , array_(from.movefrom()) {
  }

  ~CApiArray() {
    if (array_) {
      auto rc = tiledb_array_close(ctx_, array_);
      REQUIRE(rc == TILEDB_OK);
      tiledb_array_free(&array_);
    }
  }

  tiledb_array_t* movefrom() {
    auto array = array_;
    array_ = nullptr;
    return array;
  }

  operator tiledb_array_t*() const {
    return array_;
  }

  tiledb_array_t* operator->() const {
    return array_;
  }
};

template <typename T>
concept InstanceType = requires(const T& instance) {
  instance.fragments;
  instance.subarray;
  instance.dimensions();
  instance.attributes();
};

struct CSparseGlobalOrderFx {
  tiledb_ctx_t* ctx_ = nullptr;
  tiledb_vfs_t* vfs_ = nullptr;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "test_sparse_global_order";
  tiledb_array_t* array_ = nullptr;

  MemoryBudget memory_;

  template <typename Asserter = tiledb::test::AsserterCatch>
  void create_default_array_1d(
      const DefaultArray1DConfig& config = DefaultArray1DConfig());

  void create_default_array_1d_strings(bool allow_dups = false);

  template <typename Asserter = tiledb::test::AsserterCatch>
  void write_1d_fragment(
      int* coords, uint64_t* coords_size, int* data, uint64_t* data_size);

  template <typename Asserter, FragmentType Fragment>
  void write_fragment(const Fragment& fragment);

  void write_1d_fragment_strings(
      int* coords,
      uint64_t* coords_size,
      char* data,
      uint64_t* data_size,
      uint64_t* offsets,
      uint64_t* offsets_size);
  void write_delete_condition(char* value_to_delete, uint64_t value_size);
  int32_t read(
      bool set_subarray,
      int qc_idx,
      int* coords,
      uint64_t* coords_size,
      int* data,
      uint64_t* data_size,
      tiledb_query_t** query = nullptr,
      CApiArray* array_ret = nullptr,
      std::vector<int> subarray = {1, 10});
  int32_t read_strings(
      bool set_subarray,
      int* coords,
      uint64_t* coords_size,
      char* data,
      uint64_t* data_size,
      uint64_t* offsets,
      uint64_t* offsets_size,
      tiledb_query_t** query = nullptr,
      tiledb_array_t** array_ret = nullptr,
      std::vector<int> subarray = {1, 10});
  void reset_config();
  void update_config();

  template <typename Asserter, InstanceType Instance>
  void create_array(const Instance& instance);

  /**
   * Runs an input against a fresh array.
   * Inserts fragments one at a time, then reads them back in global order
   * and checks that what we read out matches what we put in.
   */
  template <typename Asserter, InstanceType Instance>
  void run(Instance instance);

  template <typename CAPIReturn>
  std::string error_if_any(CAPIReturn apirc) const;

  CSparseGlobalOrderFx();
  ~CSparseGlobalOrderFx();
};

#define TRY(thing)                           \
  do {                                       \
    auto rc = (thing);                       \
    RCCATCH_REQUIRE("" == error_if_any(rc)); \
  } while (0)

template <typename CAPIReturn>
std::string error_if_any(tiledb_ctx_t* ctx, CAPIReturn apirc) {
  if (apirc == TILEDB_OK) {
    return "";
  }

  tiledb_error_t* error = NULL;
  auto rc = tiledb_ctx_get_last_error(ctx, &error);
  REQUIRE(rc == TILEDB_OK);
  if (error == nullptr) {
    // probably should be unreachable
    return "";
  }

  const char* msg;
  rc = tiledb_error_message(error, &msg);
  REQUIRE(rc == TILEDB_OK);

  return std::string(msg);
}

template <typename CAPIReturn>
std::string CSparseGlobalOrderFx::error_if_any(CAPIReturn apirc) const {
  return ::error_if_any(ctx_, apirc);
}

CSparseGlobalOrderFx::CSparseGlobalOrderFx() {
  reset_config();

  // Create temporary directory based on the supported filesystem.
#ifdef _WIN32
  temp_dir_ = tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  temp_dir_ = "file://" + tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif
  create_dir(temp_dir_, ctx_, vfs_);
  array_name_ = temp_dir_ + ARRAY_NAME;
}

CSparseGlobalOrderFx::~CSparseGlobalOrderFx() {
  if (array_) {
    tiledb_array_free(&array_);
  }
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void CSparseGlobalOrderFx::reset_config() {
  memory_ = MemoryBudget();
  update_config();
}

void CSparseGlobalOrderFx::update_config() {
  if (ctx_ != nullptr)
    tiledb_ctx_free(&ctx_);

  if (vfs_ != nullptr)
    tiledb_vfs_free(&vfs_);

  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.query.sparse_global_order.reader",
          "refactored",
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.mem.total_budget",
          memory_.total_budget_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.mem.reader.sparse_global_order.ratio_tile_ranges",
          memory_.ratio_tile_ranges_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.mem.reader.sparse_global_order.ratio_array_data",
          memory_.ratio_array_data_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "sm.mem.reader.sparse_global_order.ratio_coords",
          memory_.ratio_coords_.c_str(),
          &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);
}

template <typename Asserter>
void CSparseGlobalOrderFx::create_default_array_1d(
    const DefaultArray1DConfig& config) {
  tiledb_object_t type;
  auto rc = tiledb_object_type(ctx_, array_name_.c_str(), &type);
  RCCATCH_REQUIRE(rc == TILEDB_OK);
  if (type == TILEDB_ARRAY) {
    rc = tiledb_array_delete(ctx_, array_name_.c_str());
    RCCATCH_REQUIRE("" == error_if_any(rc));
  }

  tiledb::test::create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_INT32},
      std::vector<void*>{(void*)(&config.dimension.domain.lower_bound)},
      std::vector<void*>{(void*)(&config.dimension.extent)},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      config.capacity,
      config.allow_dups);
}

void CSparseGlobalOrderFx::create_default_array_1d_strings(bool allow_dups) {
  int domain[] = {1, 200};
  int tile_extent = 2;
  tiledb::test::create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_INT32},
      {domain},
      {&tile_extent},
      {"a"},
      {TILEDB_STRING_ASCII},
      {TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      allow_dups);
}

template <typename Asserter>
void CSparseGlobalOrderFx::write_1d_fragment(
    int* coords, uint64_t* coords_size, int* data, uint64_t* data_size) {
  // Open array for writing.
  CApiArray array(ctx_, array_name_.c_str(), TILEDB_WRITE);

  // Create the query.
  tiledb_query_t* query;
  auto rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  RCCATCH_REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  RCCATCH_REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  RCCATCH_REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords, coords_size);
  RCCATCH_REQUIRE(rc == TILEDB_OK);

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  RCCATCH_REQUIRE("" == error_if_any(rc));

  // Clean up.
  tiledb_query_free(&query);
}

template <typename Asserter, typename... Ts>
struct query_applicator {
  static auto make_field_sizes(
      const std::tuple<Ts&...> fields,
      uint64_t cell_limit = std::numeric_limits<uint64_t>::max()) {
    std::optional<uint64_t> num_cells;
    auto make_field_size = [&]<typename T>(const std::vector<T>& field) {
      const uint64_t field_cells = std::min(cell_limit, field.size());
      const uint64_t field_size = field_cells * sizeof(T);
      if (num_cells.has_value()) {
        // precondition: each field must have the same number of cells
        RCCATCH_REQUIRE(field_cells == num_cells.value());
      } else {
        num_cells.emplace(field_cells);
      }
      return field_size;
    };

    return std::apply(
        [make_field_size](const auto&... field) {
          return std::make_tuple(make_field_size(field)...);
        },
        fields);
  }

  static void set(
      tiledb_ctx_t* ctx,
      tiledb_query_t* query,
      auto& field_sizes,
      std::tuple<Ts&...> fields,
      const char* basename,
      uint64_t cell_offset = 0) {
    auto set_data_buffer =
        [&](const std::string& name, auto& field, uint64_t& field_size) {
          auto ptr = const_cast<void*>(
              static_cast<const void*>(&field.data()[cell_offset]));
          auto rc = tiledb_query_set_data_buffer(
              ctx, query, name.c_str(), ptr, &field_size);
          RCCATCH_REQUIRE("" == error_if_any(ctx, rc));
        };

    uint64_t d = 1;
    std::apply(
        [&](const auto&... field) {
          std::apply(
              [&]<typename... Us>(Us&... field_size) {
                (set_data_buffer(
                     basename + std::to_string(d++), field, field_size),
                 ...);
              },
              field_sizes);
        },
        fields);
  }

  static uint64_t num_cells(const auto& fields, const auto& field_sizes) {
    std::optional<uint64_t> num_cells;

    auto check_field = [&]<typename T>(
                           const std::vector<T>& field, uint64_t field_size) {
      RCCATCH_REQUIRE(field_size % sizeof(T) == 0);
      RCCATCH_REQUIRE(field_size <= field.size() * sizeof(T));
      if (num_cells.has_value()) {
        RCCATCH_REQUIRE(num_cells.value() == field_size / sizeof(T));
      } else {
        num_cells.emplace(field_size / sizeof(T));
      }
    };

    std::apply(
        [&](const auto&... field) {
          std::apply(
              [&]<typename... Us>(const auto&... field_size) {
                (check_field(field, field_size), ...);
              },
              field_sizes);
        },
        fields);

    return num_cells.value();
  }
};

template <typename Asserter, FragmentType Fragment>
void CSparseGlobalOrderFx::write_fragment(const Fragment& fragment) {
  // Open array for writing.
  CApiArray array(ctx_, array_name_.c_str(), TILEDB_WRITE);

  const auto dimensions = fragment.dimensions();
  const auto attributes = fragment.attributes();

  // make field size locations
  auto dimension_sizes = []<typename... Ds>(std::tuple<Ds...> dimensions) {
    return query_applicator<Asserter, Ds...>::make_field_sizes(dimensions);
  }(dimensions);
  auto attribute_sizes = []<typename... As>(std::tuple<As...> attributes) {
    return query_applicator<Asserter, As...>::make_field_sizes(attributes);
  }(attributes);

  // Create the query.
  tiledb_query_t* query;
  auto rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  RCCATCH_REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  RCCATCH_REQUIRE(rc == TILEDB_OK);

  // add dimensions to query
  [&]<typename... Ds>(std::tuple<Ds...> dims) {
    query_applicator<Asserter, Ds...>::set(
        ctx_, query, dimension_sizes, dims, "d");
  }(dimensions);

  // add attributes to query
  [&]<typename... As>(std::tuple<As...> atts) {
    query_applicator<Asserter, As...>::set(
        ctx_, query, attribute_sizes, atts, "a");
  }(attributes);

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  RCCATCH_REQUIRE("" == error_if_any(rc));

  // check that sizes match what we expect
  const uint64_t expect_num_cells = fragment.size();
  const uint64_t dim_num_cells = [&]<typename... Ds>(auto dims) {
    return query_applicator<Asserter, Ds...>::num_cells(dims, dimension_sizes);
  }(dimensions);
  const uint64_t att_num_cells = [&]<typename... As>(auto atts) {
    return query_applicator<Asserter, As...>::num_cells(atts, attribute_sizes);
  }(attributes);

  RCCATCH_REQUIRE(dim_num_cells == expect_num_cells);
  RCCATCH_REQUIRE(att_num_cells == expect_num_cells);

  // Clean up.
  tiledb_query_free(&query);
}

void CSparseGlobalOrderFx::write_1d_fragment_strings(
    int* coords,
    uint64_t* coords_size,
    char* data,
    uint64_t* data_size,
    uint64_t* offsets,
    uint64_t* offsets_size) {
  // Open array for writing.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Create the query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a", offsets, offsets_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords, coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array.
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);

  // Clean up.
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void CSparseGlobalOrderFx::write_delete_condition(
    char* value_to_delete, uint64_t value_size) {
  // Open array for delete.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_DELETE);
  REQUIRE(rc == TILEDB_OK);

  // Create the query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_DELETE, &query);
  REQUIRE(rc == TILEDB_OK);

  // Add condition.
  tiledb_query_condition_t* qc;
  rc = tiledb_query_condition_alloc(ctx_, &qc);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_condition_init(
      ctx_, qc, "a", value_to_delete, value_size, TILEDB_EQ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_condition(ctx_, query, qc);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array.
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);

  // Clean up.
  tiledb_query_condition_free(&qc);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

int32_t CSparseGlobalOrderFx::read(
    bool set_subarray,
    int qc_idx,
    int* coords,
    uint64_t* coords_size,
    int* data,
    uint64_t* data_size,
    tiledb_query_t** query_ret,
    CApiArray* array_ret,
    std::vector<int> subarray) {
  // Open array for reading.
  CApiArray array(ctx_, array_name_.c_str(), TILEDB_READ);

  // Create query.
  tiledb_query_t* query;
  auto rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  if (set_subarray) {
    // Set subarray.
    tiledb_subarray_t* sub;
    rc = tiledb_subarray_alloc(ctx_, array, &sub);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_subarray_set_subarray(ctx_, sub, subarray.data());
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_subarray_t(ctx_, query, sub);
    CHECK(rc == TILEDB_OK);
    tiledb_subarray_free(&sub);
  }

  if (qc_idx != 0) {
    tiledb_query_condition_t* query_condition = nullptr;
    rc = tiledb_query_condition_alloc(ctx_, &query_condition);
    CHECK(rc == TILEDB_OK);

    if (qc_idx == 1) {
      int32_t val = 11;
      rc = tiledb_query_condition_init(
          ctx_, query_condition, "a", &val, sizeof(int32_t), TILEDB_LT);
      CHECK(rc == TILEDB_OK);
    } else if (qc_idx == 2) {
      // Negated query condition should produce the same results.
      int32_t val = 11;
      tiledb_query_condition_t* qc;
      rc = tiledb_query_condition_alloc(ctx_, &qc);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_condition_init(
          ctx_, qc, "a", &val, sizeof(int32_t), TILEDB_GE);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_condition_negate(ctx_, qc, &query_condition);
      CHECK(rc == TILEDB_OK);

      tiledb_query_condition_free(&qc);
    }

    rc = tiledb_query_set_condition(ctx_, query, query_condition);
    CHECK(rc == TILEDB_OK);

    tiledb_query_condition_free(&query_condition);
  }

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords, coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  auto ret = tiledb_query_submit(ctx_, query);
  if (query_ret == nullptr || array_ret == nullptr) {
    // Clean up (RAII will do it for the array)
    tiledb_query_free(&query);
  } else {
    *query_ret = query;
    new (array_ret) CApiArray(std::move(array));
  }

  return ret;
}

int32_t CSparseGlobalOrderFx::read_strings(
    bool set_subarray,
    int* coords,
    uint64_t* coords_size,
    char* data,
    uint64_t* data_size,
    uint64_t* offsets,
    uint64_t* offsets_size,
    tiledb_query_t** query_ret,
    tiledb_array_t** array_ret,
    std::vector<int> subarray) {
  // Open array for reading.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  if (set_subarray) {
    // Set subarray.
    tiledb_subarray_t* sub;
    rc = tiledb_subarray_alloc(ctx_, array, &sub);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_subarray_set_subarray(ctx_, sub, subarray.data());
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_subarray_t(ctx_, query, sub);
    CHECK(rc == TILEDB_OK);
    tiledb_subarray_free(&sub);
  }

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data, data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a", offsets, offsets_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", coords, coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  auto ret = tiledb_query_submit(ctx_, query);
  if (query_ret == nullptr || array_ret == nullptr) {
    // Clean up.
    rc = tiledb_array_close(ctx_, array);
    CHECK(rc == TILEDB_OK);
    tiledb_array_free(&array);
    tiledb_query_free(&query);
  } else {
    *query_ret = query;
    *array_ret = array;
  }

  return ret;
}

/**
 * @return true if the array fragments are "too wide" to merge with the given
 * memory budget. "too wide" means that there are overlapping tiles from
 * different fragments to fit in memory, so the merge cannot progress without
 * producing out-of-order data*.
 *
 * *there are ways to get around this but they are not implemented.
 */
template <InstanceType Instance>
static bool can_complete_in_memory_budget(
    tiledb_ctx_t* ctx, const char* array_uri, const Instance& instance) {
  CApiArray array(ctx, array_uri, TILEDB_READ);

  // TODO: verify that the conditions for the error are correct
  const auto& fragment_metadata = array->array()->fragment_metadata();
  for (const auto& fragment : fragment_metadata) {
    const_cast<sm::FragmentMetadata*>(fragment.get())
        ->loaded_metadata()
        ->load_rtree(array->array()->get_encryption_key());
  }

  auto tiles_size = [&](unsigned f, uint64_t t) {
    using BitmapType = uint8_t;

    size_t data_size = 0;
    for (size_t d = 0;
         d < std::tuple_size<decltype(instance.dimensions())>::value;
         d++) {
      data_size +=
          fragment_metadata[f]->tile_size("d" + std::to_string(d + 1), t);
    }

    const size_t rt_size = sizeof(sm::GlobalOrderResultTile<BitmapType>);
    const size_t subarray_size =
        (instance.subarray.empty() ?
             0 :
             fragment_metadata[f]->cell_num(t) * sizeof(BitmapType));
    return data_size + rt_size + subarray_size;
  };

  sm::GlobalCellCmp globalcmp(array->array()->array_schema_latest().domain());
  stdx::reverse_comparator<sm::GlobalCellCmp> reverseglobalcmp(globalcmp);

  using RT = sm::ResultTileId;

  auto cmp_pq_lower_bound = [&](const RT& rtl, const RT& rtr) {
    const sm::RangeLowerBound l_mbr = {
        .mbr = fragment_metadata[rtl.fragment_idx_]->mbr(rtl.tile_idx_)};
    const sm::RangeLowerBound r_mbr = {
        .mbr = fragment_metadata[rtr.fragment_idx_]->mbr(rtr.tile_idx_)};
    return reverseglobalcmp(l_mbr, r_mbr);
  };
  auto cmp_pq_upper_bound = [&](const RT& rtl, const RT& rtr) {
    const sm::RangeUpperBound l_mbr = {
        .mbr = fragment_metadata[rtl.fragment_idx_]->mbr(rtl.tile_idx_)};
    const sm::RangeUpperBound r_mbr = {
        .mbr = fragment_metadata[rtr.fragment_idx_]->mbr(rtr.tile_idx_)};
    return reverseglobalcmp(l_mbr, r_mbr);
  };

  /**
   * @return true if `rtl.upper < rtr.lower`, i.e. we can process
   * the entirety of `rtl` before any of `rtr`
   */
  auto cmp_upper_to_lower = [&](const RT& rtl, const RT& rtr) {
    const sm::RangeUpperBound l_mbr = {
        .mbr = fragment_metadata[rtl.fragment_idx_]->mbr(rtl.tile_idx_)};
    const sm::RangeLowerBound r_mbr = {
        .mbr = fragment_metadata[rtr.fragment_idx_]->mbr(rtr.tile_idx_)};

    int cmp;
    if (instance.subarray.empty()) {
      cmp = globalcmp.compare(l_mbr, r_mbr);
    } else {
      assert(instance.subarray.size() == 1);  // need to tweak this logic if not

      const auto& subarray = instance.subarray[0];
      if (subarray.upper_bound < *static_cast<const int*>(l_mbr.coord(0))) {
        // in this case, the bitmap will filter out the other coords in the
        // tile and it will be discarded
        std::vector<Range> subarrayrange = {subarray.range()};
        const sm::RangeUpperBound sub_mbr = {.mbr = subarrayrange};
        cmp = globalcmp.compare(sub_mbr, r_mbr);
      } else {
        cmp = globalcmp.compare(l_mbr, r_mbr);
      }
    }
    return cmp <= 0;
  };

  // order all tiles on their lower bound
  std::priority_queue<RT, std::vector<RT>, decltype(cmp_pq_lower_bound)>
      mbr_lower_bound(cmp_pq_lower_bound);
  for (unsigned f = 0; f < instance.fragments.size(); f++) {
    for (uint64_t t = 0; t < fragment_metadata[f]->tile_num(); t++) {
      if (instance.intersects(fragment_metadata[f]->mbr(t))) {
        mbr_lower_bound.push(
            sm::ResultTileId{.fragment_idx_ = f, .tile_idx_ = t});
      }
    }
  }

  // and track tiles which are active using their upper bound
  std::priority_queue<RT, std::vector<RT>, decltype(cmp_pq_upper_bound)>
      mbr_upper_bound(cmp_pq_upper_bound);

  // there must be some point where the tiles have overlapping MBRs
  // and take more memory
  const uint64_t coords_budget = std::stoi(instance.memory.total_budget_) *
                                 std::stod(instance.memory.ratio_coords_);
  uint64_t active_tile_size = 0;
  uint64_t next_tile_size = 0;
  while (active_tile_size + next_tile_size < coords_budget &&
         !mbr_lower_bound.empty()) {
    RT next_rt;

    // add new result tiles
    while (!mbr_lower_bound.empty()) {
      next_rt = mbr_lower_bound.top();

      next_tile_size = tiles_size(next_rt.fragment_idx_, next_rt.tile_idx_);
      if (active_tile_size + next_tile_size <= coords_budget) {
        mbr_lower_bound.pop();
        active_tile_size += next_tile_size;
        mbr_upper_bound.push(next_rt);
      } else {
        break;
      }
    }

    if (mbr_lower_bound.empty()) {
      break;
    }

    // emit from created result tiles, removing any which are exhausted
    next_rt = mbr_lower_bound.top();
    while (!mbr_upper_bound.empty() &&
           cmp_upper_to_lower(mbr_upper_bound.top(), next_rt)) {
      auto finish_rt = mbr_upper_bound.top();
      mbr_upper_bound.pop();
      active_tile_size -=
          tiles_size(finish_rt.fragment_idx_, finish_rt.tile_idx_);
    }
  }

  return mbr_lower_bound.empty();
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: Tile ranges budget exceeded",
    "[sparse-global-order][tile-ranges][budget-exceeded]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int coords[] = {1, 2, 3, 4, 5};
  uint64_t coords_size = sizeof(coords);
  int data[] = {1, 2, 3, 4, 5};
  uint64_t data_size = sizeof(data);
  write_1d_fragment(coords, &coords_size, data, &data_size);

  // We should have one tile range (size 16) which will be bigger than budget
  // (10).
  memory_.total_budget_ = "1000";
  memory_.ratio_tile_ranges_ = "0.01";
  update_config();

  // Try to read.
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc = read(true, 0, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_ERR);

  // Check we hit the correct error.
  tiledb_error_t* error = NULL;
  rc = tiledb_ctx_get_last_error(ctx_, &error);
  CHECK(rc == TILEDB_OK);

  const char* msg;
  rc = tiledb_error_message(error, &msg);
  CHECK(rc == TILEDB_OK);

  std::string error_str(msg);
  CHECK(
      error_str.find("Exceeded memory budget for result tile ranges") !=
      std::string::npos);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: Tile ranges budget exceeded sc-23660 A",
    "[sparse-global-order][tile-ranges][budget-exceeded][.regression]") {
  // Observed to produce the observed_bad_data in v2.12.2.

  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int coords1[] = {1, 3, 5, 7, 9};
  uint64_t coords1_size = sizeof(coords1);
  int data1[] = {11, 33, 55, 77, 99};
  uint64_t data1_size = sizeof(data1);
  write_1d_fragment(coords1, &coords1_size, data1, &data1_size);

  // Write another fragment with coords(/values) interleaved with prev.
  int coords2[] = {2, 4, 6, 8, 10};
  uint64_t coords2_size = sizeof(coords2);
  int data2[] = {22, 44, 66, 88, 1010};
  uint64_t data2_size = sizeof(data2);
  write_1d_fragment(coords2, &coords2_size, data2, &data2_size);

  // Specific relationship for failure not known, but these values
  // will result in failure with data being written.
  memory_.total_budget_ = "10000";
  // Failure here occurs with the value of 0.1 for ratio_tile_ranges_.
  update_config();

  CApiArray array;
  tiledb_query_t* query = nullptr;

  // Try to read.
  int coords_r[2];
  int data_r[2];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  std::vector<int> subarray{4, 10};
  int rc;
  tiledb_query_status_t status;
  rc = read(
      true,
      0,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      &query,
      &array,
      subarray);
  CHECK(rc == TILEDB_OK);

  std::vector<int> retrieved_data;
  retrieved_data.reserve(10);

  do {
    auto nitems = data_r_size / sizeof(int);
    for (auto ui = 0u; ui < nitems; ++ui) {
      retrieved_data.emplace_back(data_r[ui]);
    }

    // Check incomplete query status.
    tiledb_query_get_status(ctx_, query, &status);
    if (status == TILEDB_INCOMPLETE) {
      rc = tiledb_query_submit(ctx_, query);
      CHECK(rc == TILEDB_OK);
    }
  } while (status == TILEDB_INCOMPLETE);

  // The correct data should be...
  std::vector<int> expected_correct_data{44, 55, 66, 77, 88, 99, 1010};
  // But the error situation in v2.12.2 instead produced...
  std::vector<int> observed_bad_data{44, 66, 88, 1010};
  CHECK_FALSE(retrieved_data == observed_bad_data);
  CHECK(retrieved_data == expected_correct_data);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: Tile ranges budget exceeded sc-23660 B",
    "[sparse-global-order][tile-ranges][budget-exceeded][.regression]") {
  // Observed to produce the observed_bad_data in v2.12.2.

  // Similar in nature to the "... A" version, but using some differently
  // written data.

  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  int coords1[] = {1, 4, 7, 10};
  uint64_t coords1_size = sizeof(coords1);
  int data1[] = {11, 44, 77, 1010};
  uint64_t data1_size = sizeof(data1);

  // Write another fragment with coords(/values) interleaved with prev.
  int coords2[] = {2, 5, 8, 11};
  uint64_t coords2_size = sizeof(coords2);
  int data2[] = {22, 55, 88, 1111};
  uint64_t data2_size = sizeof(data2);

  // Write another fragment with coords(/values) interleaved with prev.
  int coords3[] = {3, 6, 9, 12};
  uint64_t coords3_size = sizeof(coords3);
  int data3[] = {33, 66, 99, 1212};
  uint64_t data3_size = sizeof(data3);

  write_1d_fragment(coords1, &coords1_size, data1, &data1_size);
  write_1d_fragment(coords3, &coords3_size, data3, &data3_size);
  write_1d_fragment(coords2, &coords2_size, data2, &data2_size);

  // specific relationship for failure not known, but these values
  // will result in failure with data being written.
  memory_.total_budget_ = "15000";
  // Failure here occurs with the value of 0.1 for ratio_tile_ranges_.
  update_config();

  CApiArray array;
  tiledb_query_t* query = nullptr;

  // Try to read.
  int coords_r[1];
  int data_r[1];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  std::vector<int> subarray{5, 11};
  int rc;
  tiledb_query_status_t status;
  rc = read(
      true,
      0,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      &query,
      &array,
      subarray);
  CHECK(rc == TILEDB_OK);

  std::vector<int> retrieved_data;
  retrieved_data.reserve(10);

  do {
    auto nitems = data_r_size / sizeof(int);
    for (auto ui = 0u; ui < nitems; ++ui) {
      retrieved_data.emplace_back(data_r[ui]);
    }

    // Check incomplete query status.
    tiledb_query_get_status(ctx_, query, &status);
    if (status == TILEDB_INCOMPLETE) {
      rc = tiledb_query_submit(ctx_, query);
      CHECK(rc == TILEDB_OK);
    }
  } while (status == TILEDB_INCOMPLETE);

  // The correct data should be...
  std::vector<int> expected_correct_data{55, 66, 77, 88, 99, 1010, 1111};
  // But the error situation in v2.12.2 instead produced...
  std::vector<int> observed_bad_data{55, 66, 88, 99, 1111};
  CHECK_FALSE(retrieved_data == observed_bad_data);
  CHECK(retrieved_data == expected_correct_data);
}

/**
 * Tests that the reader will not yield results out of order across multiple
 * iterations or `submit`s if the fragments are heavily skewed when the memory
 * budget is heavily constrained.
 *
 * e.g. two fragments
 * F0: 1-1000,1001-2000,2001-3000
 * F1: 2001-3000
 *
 * If the memory budget allows only one tile per fragment at a time then there
 * must be a mechanism for emitting (F0, T1) before (F1, T0) even though the
 * the memory budget might not process them in the same loop.
 */
TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: fragment skew",
    "[sparse-global-order]") {
  auto doit = [this]<typename Asserter>(
                  size_t fragment_size,
                  size_t num_user_cells,
                  int extent,
                  const std::vector<tdbrc::Domain<int>>& subarray =
                      std::vector<tdbrc::Domain<int>>()) {
    // Write a fragment F0 with unique coordinates
    struct FxFragment1D fragment0;
    fragment0.coords.resize(fragment_size);
    std::iota(fragment0.coords.begin(), fragment0.coords.end(), 1);

    // Write a fragment F1 with lots of duplicates
    // [100,100,100,100,100,101,101,101,101,101,102,102,102,102,102,...]
    struct FxFragment1D fragment1;
    fragment1.coords.resize(fragment0.coords.size());
    for (size_t i = 0; i < fragment1.coords.size(); i++) {
      fragment1.coords[i] =
          static_cast<int>(i / 10) + (fragment0.coords.size() / 2);
    }

    // atts are whatever
    fragment0.atts.resize(fragment0.coords.size());
    std::iota(fragment0.atts.begin(), fragment0.atts.end(), 0);
    fragment1.atts.resize(fragment1.coords.size());
    std::iota(
        fragment1.atts.begin(), fragment1.atts.end(), fragment0.coords.size());

    struct FxRun1D instance;
    instance.fragments.push_back(fragment0);
    instance.fragments.push_back(fragment1);
    instance.num_user_cells = num_user_cells;

    instance.array.dimension.extent = extent;
    instance.array.allow_dups = true;

    instance.memory.total_budget_ = "20000";
    instance.memory.ratio_array_data_ = "0.5";

    instance.subarray = subarray;

    run<Asserter, FxRun1D>(instance);
  };

  SECTION("Example") {
    doit.operator()<tiledb::test::AsserterCatch>(200, 8, 2);
  }

  SECTION("Rapidcheck") {
    rc::prop("rapidcheck fragment skew", [doit]() {
      const size_t fragment_size = *rc::gen::inRange(2, 200);
      const size_t num_user_cells = *rc::gen::inRange(1, 1024);
      const int extent = *rc::gen::inRange(1, 200);
      const auto subarray = *rc::make_subarray_1d(tdbrc::Domain<int>(1, 200));
      doit.operator()<tiledb::test::AsserterRapidcheck>(
          fragment_size, num_user_cells, extent, subarray);
    });
  }
}

/**
 * Tests that the reader will not yield results out of order across multiple
 * iterations or `submit`s if the tile MBRs across different fragments are
 * interleaved.
 *
 * The test sets up data with two fragments so that each tile overlaps with
 * two tiles from the other fragment.  This way when the tiles are arranged
 * in global order the only way to ensure that we don't emit out of order
 * results with a naive implementation is to have *all* the tiles loaded
 * in one pass, which is not practical.
 */
TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: fragment interleave",
    "[sparse-global-order]") {
  // NB: the tile extent is 2
  auto doit = [this]<typename Asserter>(
                  size_t fragment_size,
                  size_t num_user_cells,
                  const std::vector<tdbrc::Domain<int>>& subarray = {}) {
    struct FxFragment1D fragment0;
    struct FxFragment1D fragment1;

    // Write a fragment F0 with tiles [1,3][3,5][5,7][7,9]...
    fragment0.coords.resize(fragment_size);
    fragment0.coords[0] = 1;
    for (size_t i = 1; i < fragment0.coords.size(); i++) {
      fragment0.coords[i] = 1 + 2 * ((i + 1) / 2);
    }

    // Write a fragment F1 with tiles [2,4][4,6][6,8][8,10]...
    fragment1.coords.resize(fragment0.coords.size());
    for (size_t i = 0; i < fragment1.coords.size(); i++) {
      fragment1.coords[i] = fragment0.coords[i] + 1;
    }

    // atts don't really matter
    fragment0.atts.resize(fragment0.coords.size());
    std::iota(fragment0.atts.begin(), fragment0.atts.end(), 0);

    fragment1.atts.resize(fragment1.coords.size());
    std::iota(
        fragment1.atts.begin(), fragment1.atts.end(), fragment0.atts.size());

    struct FxRun1D instance;
    instance.fragments.push_back(fragment0);
    instance.fragments.push_back(fragment1);
    instance.num_user_cells = num_user_cells;
    instance.subarray = subarray;
    instance.memory.total_budget_ = "20000";
    instance.memory.ratio_array_data_ = "0.5";
    instance.array.allow_dups = true;

    run<Asserter, FxRun1D>(instance);
  };

  SECTION("Example") {
    doit.operator()<tiledb::test::AsserterCatch>(196, 8);
  }

  SECTION("Rapidcheck") {
    rc::prop("rapidcheck fragment interleave", [doit]() {
      const size_t fragment_size = *rc::gen::inRange(2, 196);
      const size_t num_user_cells = *rc::gen::inRange(1, 1024);
      const auto subarray = *rc::make_subarray_1d(tdbrc::Domain<int>(1, 200));
      doit.operator()<tiledb::test::AsserterRapidcheck>(
          fragment_size, num_user_cells, subarray);
    });
  }

  /**
   * Now we should have 200 tiles, each of which has 2 coordinates,
   * and each of which is size is 1584.
   * In global order we skew towards fragment 1 which has lots of duplicates.
   */
}

/**
 * Tests that the reader correctly returns an error if it cannot
 * make progress due to being unable to make progress.
 * The reader can stall if there is a point where F fragments
 * fit in memory, but `F + 1` or more fragments overlap
 *
 * For example, suppose 2 tiles fit in memory. If the tiles are:
 * [0, 1, 2, 3, 4]
 * [1, 2, 3, 4, 5]
 * [2, 3, 4, 5, 6]
 *
 * Then we will process the first two tiles first after ordering
 * them on their lower bound.  We cannot emit anything past "2"
 * because it would be out of order with the third tile.
 *
 * After the first iteration, the state is
 * [_, _, _, 3, 4]
 * [_, _, 3, 4, 5]
 * [2, 3, 4, 5, 6]
 *
 * We could make progress by un-loading a tile and then loading
 * the third tile, but instead we will just error out because
 * that's complicated.
 *
 * The user's recourse to that is to either:
 * 1) increase memory budget; or
 * 2) consolidate some fragments.
 */
TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: many overlapping fragments",
    "[sparse-global-order]") {
  auto doit = [this]<typename Asserter>(
                  size_t num_fragments,
                  size_t fragment_size,
                  size_t num_user_cells,
                  const std::vector<tdbrc::Domain<int>>& subarray =
                      std::vector<tdbrc::Domain<int>>()) {
    const uint64_t total_budget = 100000;
    const double ratio_coords = 0.2;

    FxRun1D instance;
    instance.array.capacity = num_fragments * 2;
    instance.array.dimension.extent = num_fragments * 2;
    instance.array.allow_dups = true;
    instance.subarray = subarray;

    instance.memory.total_budget_ = std::to_string(total_budget);
    instance.memory.ratio_coords_ = std::to_string(ratio_coords);
    instance.memory.ratio_array_data_ = "0.6";

    for (size_t f = 0; f < num_fragments; f++) {
      FxFragment1D fragment;
      fragment.coords.resize(fragment_size);
      std::iota(
          fragment.coords.begin(),
          fragment.coords.end(),
          instance.array.dimension.domain.lower_bound + f);

      fragment.atts.resize(fragment_size);
      std::iota(
          fragment.atts.begin(),
          fragment.atts.end(),
          fragment_size * num_fragments);

      instance.fragments.push_back(fragment);
    }

    instance.num_user_cells = num_user_cells;

    run<Asserter, FxRun1D>(instance);
  };

  SECTION("Example") {
    doit.operator()<tiledb::test::AsserterCatch>(16, 100, 64);
  }

  SECTION("Rapidcheck") {
    rc::prop("rapidcheck many overlapping fragments", [doit]() {
      const size_t num_fragments = *rc::gen::inRange(10, 24);
      const size_t fragment_size = *rc::gen::inRange(2, 200 - 64);
      // const size_t num_user_cells = *rc::gen::inRange(1, 1024 * 1024);
      const size_t num_user_cells = 1024;
      const auto subarray = *rc::make_subarray_1d(tdbrc::Domain<int>(1, 200));
      doit.operator()<tiledb::test::AsserterRapidcheck>(
          num_fragments, fragment_size, num_user_cells, subarray);
    });
  }
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: tile offsets budget exceeded",
    "[sparse-global-order][tile-offsets][budget-exceeded]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  // Write a fragment.
  std::vector<int> coords(200);
  std::iota(coords.begin(), coords.end(), 1);
  uint64_t coords_size = coords.size() * sizeof(int);

  std::vector<int> data(200);
  std::iota(data.begin(), data.end(), 1);
  uint64_t data_size = data.size() * sizeof(int);

  write_1d_fragment(coords.data(), &coords_size, data.data(), &data_size);

  // We should have 100 tiles (tile offset size 800) which will be bigger than
  // leftover budget.
  memory_.total_budget_ = "3000";
  memory_.ratio_array_data_ = "0.5";
  update_config();

  // Try to read.
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc = read(true, 0, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_ERR);

  // Check we hit the correct error.
  tiledb_error_t* error = NULL;
  rc = tiledb_ctx_get_last_error(ctx_, &error);
  CHECK(rc == TILEDB_OK);

  const char* msg;
  rc = tiledb_error_message(error, &msg);
  CHECK(rc == TILEDB_OK);

  std::string error_str(msg);
  CHECK(
      error_str.find("SparseGlobalOrderReader: Cannot load tile offsets, "
                     "computed size") != std::string::npos);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: coords budget forcing one tile at a time",
    "[sparse-global-order][small-coords-budget]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  bool use_subarray = false;
  SECTION("No subarray") {
    use_subarray = false;
  }
  SECTION("Subarray") {
    use_subarray = true;
  }

  int num_frags = 2;
  for (int i = 1; i < num_frags + 1; i++) {
    // Write a fragment.
    int coords[] = {
        i,
        num_frags + i,
        2 * num_frags + i,
        3 * num_frags + i,
        4 * num_frags + i};
    uint64_t coords_size = sizeof(coords);
    int data[] = {
        i,
        num_frags + i,
        2 * num_frags + i,
        3 * num_frags + i,
        4 * num_frags + i};
    uint64_t data_size = sizeof(data);
    write_1d_fragment(coords, &coords_size, data, &data_size);
  }

  // FIXME: there is no per fragment budget anymore
  // Two result tile (2 * (~3000 + 8) will be bigger than the per fragment
  // budget (1000).
  memory_.total_budget_ = "35000";
  memory_.ratio_coords_ = "0.11";
  update_config();

  CApiArray array;
  tiledb_query_t* query = nullptr;

  uint32_t rc;

  // Try to read.
  int coords_r[10];
  int data_r[10];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  rc = read(
      use_subarray,
      0,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      &query,
      &array);
  CHECK(rc == TILEDB_OK);

  // Check the internal loop count against expected value.
  auto stats =
      ((sm::SparseGlobalOrderReader<uint8_t>*)query->query_->strategy())
          ->stats();
  REQUIRE(stats != nullptr);
  auto counters = stats->counters();
  REQUIRE(counters != nullptr);
  auto loop_num = counters->find("Context.Query.Reader.internal_loop_num");
  CHECK(5 == loop_num->second);

  // Check incomplete query status.
  tiledb_query_status_t status;
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  CHECK(40 == data_r_size);
  CHECK(40 == coords_r_size);

  int coords_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  int data_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c, data_r, data_r_size));

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: coords budget too small",
    "[sparse-global-order][coords-budget][too-small]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  bool use_subarray = false;
  SECTION("- No subarray") {
    use_subarray = false;
  }
  SECTION("- Subarray") {
    use_subarray = true;
  }

  // Write a fragment.
  int coords[] = {1, 2, 3, 4, 5};
  uint64_t coords_size = sizeof(coords);
  int data[] = {1, 2, 3, 4, 5};
  uint64_t data_size = sizeof(data);
  write_1d_fragment(coords, &coords_size, data, &data_size);

  // One result tile (8 + ~440) will be bigger than the budget (400).
  memory_.total_budget_ = "19000";
  memory_.ratio_coords_ = "0.04";
  update_config();

  // Try to read.
  int coords_r[5];
  int data_r[5];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  auto rc =
      read(use_subarray, 0, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_ERR);

  // Check we hit the correct error.
  tiledb_error_t* error = NULL;
  rc = tiledb_ctx_get_last_error(ctx_, &error);
  CHECK(rc == TILEDB_OK);

  const char* msg;
  rc = tiledb_error_message(error, &msg);
  CHECK(rc == TILEDB_OK);

  std::string error_str(msg);
  CHECK(error_str.find("Cannot load a single tile") != std::string::npos);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: qc removes full tile",
    "[sparse-global-order][qc-removes-tile]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  bool use_subarray = false;
  int tile_idx = 0;
  int qc_idx = GENERATE(1, 2);
  SECTION("No subarray") {
    use_subarray = false;
    SECTION("First tile") {
      tile_idx = 0;
    }
    SECTION("Second tile") {
      tile_idx = 1;
    }
    SECTION("Last tile") {
      tile_idx = 2;
    }
  }
  SECTION("Subarray") {
    use_subarray = true;
    SECTION("First tile") {
      tile_idx = 0;
    }
    SECTION("Second tile") {
      tile_idx = 1;
    }
    SECTION("Last tile") {
      tile_idx = 2;
    }
  }

  int coords_1[] = {1, 2, 3};
  int data_1[] = {1, 2, 3};

  int coords_2[] = {4, 5, 6};
  int data_2[] = {4, 5, 6};

  int coords_3[] = {12, 13, 14};
  int data_3[] = {12, 13, 14};

  uint64_t coords_size = sizeof(coords_1);
  uint64_t data_size = sizeof(data_1);

  // Create the array so the removed tile is at the correct index.
  switch (tile_idx) {
    case 0:
      write_1d_fragment(coords_3, &coords_size, data_3, &data_size);
      write_1d_fragment(coords_1, &coords_size, data_1, &data_size);
      write_1d_fragment(coords_2, &coords_size, data_2, &data_size);
      break;

    case 1:
      write_1d_fragment(coords_1, &coords_size, data_1, &data_size);
      write_1d_fragment(coords_3, &coords_size, data_3, &data_size);
      write_1d_fragment(coords_2, &coords_size, data_2, &data_size);
      break;

    case 2:
      write_1d_fragment(coords_1, &coords_size, data_1, &data_size);
      write_1d_fragment(coords_2, &coords_size, data_2, &data_size);
      write_1d_fragment(coords_3, &coords_size, data_3, &data_size);
      break;
  }

  // Read.
  int coords_r[6];
  int data_r[6];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  auto rc = read(
      use_subarray, qc_idx, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);

  // Should read two tile (6 values).
  CHECK(24 == data_r_size);
  CHECK(24 == coords_r_size);

  int coords_c[] = {1, 2, 3, 4, 5, 6};
  int data_c[] = {1, 2, 3, 4, 5, 6};
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c, data_r, data_r_size));
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: qc removes tile from second fragment "
    "replacing data from first fragment",
    "[sparse-global-order][qc-removes-replacement-data]") {
  // Create default array.
  reset_config();

  bool use_subarray = GENERATE(true, false);
  bool dups = GENERATE(false, true);
  bool extra_fragment = GENERATE(true, false);
  int qc_idx = GENERATE(1, 2);

  create_default_array_1d(DefaultArray1DConfig().with_allow_dups(dups));

  int coords_1[] = {1, 2, 3};
  int data_1[] = {2, 2, 2};

  int coords_2[] = {1, 2, 3};
  int data_2[] = {12, 12, 12};

  uint64_t coords_size = sizeof(coords_1);
  uint64_t data_size = sizeof(data_1);
  write_1d_fragment(coords_1, &coords_size, data_1, &data_size);
  write_1d_fragment(coords_2, &coords_size, data_2, &data_size);

  if (extra_fragment) {
    write_1d_fragment(coords_2, &coords_size, data_2, &data_size);
  }

  // Read.
  int coords_r[9];
  int data_r[9];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  auto rc = read(
      use_subarray, qc_idx, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);

  if (dups) {
    CHECK(3 * sizeof(int) == coords_r_size);
    CHECK(3 * sizeof(int) == data_r_size);

    int coords_c[] = {1, 2, 3};
    int data_c[] = {2, 2, 2};
    CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
    CHECK(!std::memcmp(data_c, data_r, data_r_size));
  } else {
    // Should read nothing.
    CHECK(0 == data_r_size);
    CHECK(0 == coords_r_size);
  }
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: qc removes tile from second fragment "
    "replacing data from first fragment, 2",
    "[sparse-global-order][qc-removes-replacement-data]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  int qc_idx = GENERATE(1, 2);
  bool use_subarray = false;
  SECTION("- No subarray") {
    use_subarray = false;
  }
  SECTION("- Subarray") {
    use_subarray = true;
  }

  int coords_1[] = {1, 2, 3};
  int data_1[] = {2, 2, 2};

  int coords_2[] = {1, 2, 3};
  int data_2[] = {12, 4, 12};

  uint64_t coords_size = sizeof(coords_1);
  uint64_t data_size = sizeof(data_1);
  write_1d_fragment(coords_1, &coords_size, data_1, &data_size);
  write_1d_fragment(coords_2, &coords_size, data_2, &data_size);

  // Read.
  int coords_r[6];
  int data_r[6];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  auto rc = read(
      use_subarray, qc_idx, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);

  // One value.
  CHECK(sizeof(int) == data_r_size);
  CHECK(sizeof(int) == coords_r_size);

  int coords_c[] = {2};
  int data_c[] = {4};
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c, data_r, data_r_size));
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: merge with subarray and dups",
    "[sparse-global-order][merge][subarray][dups]") {
  // Create default array.
  reset_config();
  create_default_array_1d(DefaultArray1DConfig().with_allow_dups(true));

  bool use_subarray = false;
  int qc_idx = GENERATE(1, 2);

  int coords_1[] = {8, 9, 10, 11, 12, 13};
  int data_1[] = {8, 9, 10, 11, 12, 13};

  int coords_2[] = {8, 9, 10, 11, 12, 13};
  int data_2[] = {8, 9, 10, 11, 12, 13};

  uint64_t coords_size = sizeof(coords_1);
  uint64_t data_size = sizeof(data_1);

  // Create the array.
  write_1d_fragment(coords_1, &coords_size, data_1, &data_size);
  write_1d_fragment(coords_2, &coords_size, data_2, &data_size);

  // Read.
  int coords_r[6];
  int data_r[6];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  auto rc = read(
      use_subarray, qc_idx, coords_r, &coords_r_size, data_r, &data_r_size);
  CHECK(rc == TILEDB_OK);

  // Should read (6 values).
  CHECK(24 == data_r_size);
  CHECK(24 == coords_r_size);

  int coords_c[] = {8, 8, 9, 9, 10, 10};
  int data_c[] = {8, 8, 9, 9, 10, 10};
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c, data_r, data_r_size));
}

TEST_CASE(
    "Sparse global order reader: user buffer cannot fit single cell",
    "[sparse-global-order][user-buffer][too-small][rest]") {
  VFSTestSetup vfs_test_setup;
  std::string array_name = vfs_test_setup.array_uri("test_sparse_global_order");
  auto ctx = vfs_test_setup.ctx();

  // Create array with var-sized attribute.
  Domain dom(ctx);
  dom.add_dimension(Dimension::create<int64_t>(ctx, "d1", {{1, 4}}, 2));

  Attribute attr(ctx, "a", TILEDB_STRING_ASCII);
  attr.set_cell_val_num(TILEDB_VAR_NUM);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.add_attribute(attr);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_domain(dom);
  schema.set_allows_dups(true);

  Array::create(array_name, schema);

  // Write a fragment.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  std::vector<int64_t> d1 = {1, 2, 3};
  std::string a1_data{
      "astringofsize15"
      "foo"
      "bar"};
  std::vector<uint64_t> a1_offsets{0, 15, 18};

  query.set_data_buffer("d1", d1);
  query.set_data_buffer("a", a1_data);
  query.set_offsets_buffer("a", a1_offsets);

  // Submit query
  query.submit_and_finalize();

  // Read using a buffer that can't fit a single result
  Array array2(ctx, array_name, TILEDB_READ);
  Query query2(ctx, array2, TILEDB_READ);

  std::string attr_val;
  // The first result is 15 bytes long so it won't fit in 10 byte user buffer
  attr_val.resize(10);
  std::vector<uint64_t> attr_off(sizeof(uint64_t));

  auto layout = GENERATE(TILEDB_GLOBAL_ORDER, TILEDB_UNORDERED);

  query2.set_layout(layout);
  query2.set_data_buffer("a", (char*)attr_val.data(), attr_val.size());
  query2.set_offsets_buffer("a", attr_off);

  // The user buffer cannot fit a single result so it should return Incomplete
  // with the right reason
  query2.submit();
  REQUIRE(query2.query_status() == Query::Status::INCOMPLETE);

  tiledb_query_status_details_t details;
  auto rc = tiledb_query_get_status_details(
      ctx.ptr().get(), query2.ptr().get(), &details);
  CHECK(rc == TILEDB_OK);
  CHECK(details.incomplete_reason == TILEDB_REASON_USER_BUFFER_SIZE);

  array2.close();
}

TEST_CASE(
    "Sparse global order reader: attribute copy memory limit",
    "[sparse-global-order][attribute-copy][memory-limit][rest]") {
  Config config;
  config["sm.mem.total_budget"] = "20000";
  VFSTestSetup vfs_test_setup(config.ptr().get());
  std::string array_name = vfs_test_setup.array_uri("test_sparse_global_order");
  auto ctx = vfs_test_setup.ctx();

  // Create array with var-sized attribute.
  Domain dom(ctx);
  dom.add_dimension(Dimension::create<int64_t>(ctx, "d1", {{1, 4}}, 2));

  Attribute attr(ctx, "a", TILEDB_STRING_ASCII);
  attr.set_cell_val_num(TILEDB_VAR_NUM);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.add_attribute(attr);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_domain(dom);
  schema.set_allows_dups(true);
  schema.set_capacity(4);

  Array::create(array_name, schema);

  // Write a fragment with two tiles of 4 cells each. The var size tiles will
  // have a size of 5000.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  std::vector<int64_t> d1 = {1, 1, 2, 2, 3, 3, 4, 4};
  std::string a1_data;
  a1_data.resize(10000);
  for (uint64_t i = 0; i < 10000; i++) {
    a1_data[i] = '0' + i % 10;
  }
  std::vector<uint64_t> a1_offsets{0, 1250, 2500, 3750, 5000, 6250, 7500, 8750};

  query.set_data_buffer("d1", d1);
  query.set_data_buffer("a", a1_data);
  query.set_offsets_buffer("a", a1_offsets);
  CHECK_NOTHROW(query.submit_and_finalize());

  // Read using a budget that can only fit one of the var size tiles.
  Array array2(ctx, array_name, TILEDB_READ);
  Query query2(ctx, array2, TILEDB_READ);

  std::string attr_val;
  attr_val.resize(5000);
  std::vector<uint64_t> attr_off(sizeof(uint64_t));

  query2.set_layout(TILEDB_GLOBAL_ORDER);
  query2.set_data_buffer("a", (char*)attr_val.data(), attr_val.size());
  query2.set_offsets_buffer("a", attr_off);

  // Submit and validate we only get 4 cells back (one tile).
  auto st = query2.submit();
  CHECK(st == Query::Status::INCOMPLETE);

  auto result_num = (int)query2.result_buffer_elements()["a"].first;
  CHECK(result_num == 4);

  // Submit and validate we get the last 4 cells back.
  st = query2.submit();
  CHECK(st == Query::Status::COMPLETE);

  result_num = (int)query2.result_buffer_elements()["a"].first;
  CHECK(result_num == 4);

  array2.close();
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: no new coords tile",
    "[sparse-global-order][no-new-coords-tile]") {
  // Create default array.
  reset_config();
  create_default_array_1d();

  bool use_subarray = false;
  SECTION("No subarray") {
    use_subarray = false;
  }
  SECTION("Subarray") {
    use_subarray = true;
  }

  int num_frags = 2;
  for (int i = 1; i < num_frags + 1; i++) {
    // Write a fragment.
    int coords[] = {
        i,
        num_frags + i,
        2 * num_frags + i,
        3 * num_frags + i,
        4 * num_frags + i};
    uint64_t coords_size = sizeof(coords);
    int data[] = {
        i,
        num_frags + i,
        2 * num_frags + i,
        3 * num_frags + i,
        4 * num_frags + i};
    uint64_t data_size = sizeof(data);
    write_1d_fragment(coords, &coords_size, data, &data_size);
  }

  // FIXME: there is no per fragment budget anymore
  // Two result tile (2 * (~4000 + 8) will be bigger than the per fragment
  // budget (1000).
  memory_.total_budget_ = "40000";
  memory_.ratio_coords_ = "0.22";
  update_config();

  CApiArray array;
  tiledb_query_t* query = nullptr;

  // Try to read.
  int coords_r[1];
  int data_r[1];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  tiledb_query_status_t status;
  uint32_t rc = read(
      use_subarray,
      0,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      &query,
      &array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_INCOMPLETE);

  uint64_t loop_idx = 1;
  CHECK(4 == data_r_size);
  CHECK(4 == coords_r_size);
  CHECK(!std::memcmp(&loop_idx, coords_r, coords_r_size));
  CHECK(!std::memcmp(&loop_idx, data_r, data_r_size));
  loop_idx++;

  while (status == TILEDB_INCOMPLETE) {
    rc = tiledb_query_submit(ctx_, query);
    CHECK("" == error_if_any(rc));
    tiledb_query_get_status(ctx_, query, &status);
    CHECK(4 == data_r_size);
    CHECK(4 == coords_r_size);
    CHECK(!std::memcmp(&loop_idx, coords_r, coords_r_size));
    CHECK(!std::memcmp(&loop_idx, data_r, data_r_size));
    loop_idx++;
  }
  CHECK(loop_idx == 11);
  CHECK(rc == TILEDB_OK);

  // Clean up.
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: correct read state on duplicates",
    "[sparse-global-order][no-dups][read-state]") {
  bool dups = GENERATE(false, true);
  create_default_array_1d(DefaultArray1DConfig().with_allow_dups(dups));

  // Write one fragment in coordinates 1-10 with data 1-10.
  int coords[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t coords_size = sizeof(coords);
  int data1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t data1_size = sizeof(data1);
  write_1d_fragment(coords, &coords_size, data1, &data1_size);

  // Write another fragment with the same coordinates but data 11-20.
  int data2[] = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
  uint64_t data2_size = sizeof(data2);
  write_1d_fragment(coords, &coords_size, data2, &data2_size);

  CApiArray array;
  tiledb_query_t* query = nullptr;

  // Read with buffers that can only fit one cell.
  int coords_r[1];
  int data_r[1];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);

  // Read the first cell.
  int rc;
  tiledb_query_status_t status;
  rc = read(
      true, 0, coords_r, &coords_r_size, data_r, &data_r_size, &query, &array);
  CHECK(rc == TILEDB_OK);

  CHECK(coords_r[0] == 1);

  if (dups) {
    CHECK((data_r[0] == 1 || data_r[0] == 11));

    for (int i = 3; i <= 21; i++) {
      // Check incomplete query status.
      tiledb_query_get_status(ctx_, query, &status);
      CHECK(status == TILEDB_INCOMPLETE);

      rc = tiledb_query_submit(ctx_, query);
      CHECK(rc == TILEDB_OK);

      CHECK(coords_r[0] == i / 2);
      CHECK((data_r[0] == i / 2 + 10 || data_r[0] == i / 2));
    }
  } else {
    CHECK(data_r[0] == 11);

    for (int i = 2; i <= 10; i++) {
      // Check incomplete query status.
      tiledb_query_get_status(ctx_, query, &status);
      CHECK(status == TILEDB_INCOMPLETE);

      rc = tiledb_query_submit(ctx_, query);
      CHECK(rc == TILEDB_OK);

      CHECK(coords_r[0] == i);
      CHECK(data_r[0] == i + 10);
    }
  }

  // Check completed query status.
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Clean up.
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: revert deleted duplicate",
    "[sparse-global-order][revert][deleted-duplicate]") {
  bool deleted_dup = GENERATE(true, false);
  create_default_array_1d_strings(false);

  // Write the first fragment.
  int coords1[] = {3, 4, 5, 6};
  uint64_t coords_size1 = sizeof(coords1);
  char data1[] = "333444555666";

  if (deleted_dup) {
    data1[8] = '2';
  }

  uint64_t data1_size = sizeof(data1) - 1;
  uint64_t offsets1[] = {0, 3, 6, 9};
  uint64_t offsets1_size = sizeof(offsets1);
  write_1d_fragment_strings(
      coords1, &coords_size1, data1, &data1_size, offsets1, &offsets1_size);

  // Write the second fragment.
  int coords2[] = {1, 2, 5, 7};
  uint64_t coords_size2 = sizeof(coords2);
  char data2[] = "111222552777";
  uint64_t data2_size = sizeof(data2) - 1;
  uint64_t offsets2[] = {0, 3, 6, 9};
  uint64_t offsets2_size = sizeof(offsets2);
  write_1d_fragment_strings(
      coords2, &coords_size2, data2, &data2_size, offsets2, &offsets2_size);

  // Delete the cell at coordinate 5, but only for the second fragment.
  char to_delete[] = "552";
  write_delete_condition(to_delete, 3);

  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;

  // Read with fixed size buffers that can fit the whole dataset but var sized
  // buffer can only fit 3 cells. This will revert progress for the second
  // fragment before cell at coord 5 for sure because of the cell at coordinate
  // 4 but might not regress progress for the second fragment before cell at
  // coord 5 as cell at coordinate 5 was deleted.
  int coords_r[100];
  char data_r[9];
  uint64_t offsets_r[100];
  uint64_t coords_r_size = sizeof(coords_r);
  uint64_t data_r_size = sizeof(data_r);
  uint64_t offsets_r_size = sizeof(offsets_r);

  // Read the first cell.
  int rc;
  tiledb_query_status_t status;
  rc = read_strings(
      false,
      coords_r,
      &coords_r_size,
      data_r,
      &data_r_size,
      offsets_r,
      &offsets_r_size,
      &query,
      &array);
  CHECK(rc == TILEDB_OK);

  // Validate the first read.
  int coords_c[] = {1, 2, 3};
  char data_c[] = "111222333";
  uint64_t offsets_c[] = {0, 3, 6};
  CHECK(coords_r_size == 12);
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  CHECK(data_r_size == 9);
  CHECK(!std::memcmp(data_c, data_r, data_r_size));
  CHECK(offsets_r_size == 24);
  CHECK(!std::memcmp(offsets_c, offsets_r, offsets_r_size));

  // Check completed query status.
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_INCOMPLETE);

  // Reset buffer sizes.
  coords_r_size = sizeof(coords_r);
  data_r_size = sizeof(data_r);
  offsets_r_size = sizeof(offsets_r);

  // Submit query.
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Validate the second read.
  int coords_c2[] = {4, 6, 7};
  char data_c2[] = "4446667777";
  uint64_t offsets_c2[] = {0, 3, 6};
  CHECK(coords_r_size == 12);
  CHECK(!std::memcmp(coords_c2, coords_r, coords_r_size));
  CHECK(data_r_size == 9);
  CHECK(!std::memcmp(data_c2, data_r, data_r_size));
  CHECK(offsets_r_size == 24);
  CHECK(!std::memcmp(offsets_c2, offsets_r, offsets_r_size));

  // Check completed query status.
  tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Clean up.
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

template <typename Asserter, InstanceType Instance>
void CSparseGlobalOrderFx::create_array(const Instance& instance) {
  tiledb_object_t type;
  auto rc = tiledb_object_type(ctx_, array_name_.c_str(), &type);
  RCCATCH_REQUIRE(rc == TILEDB_OK);
  if (type == TILEDB_ARRAY) {
    rc = tiledb_array_delete(ctx_, array_name_.c_str());
    RCCATCH_REQUIRE("" == error_if_any(rc));
  }

  const auto dimensions = instance.dimensions();
  const auto attributes = instance.attributes();

  std::vector<std::string> dimension_names;
  std::vector<tiledb_datatype_t> dimension_types;
  std::vector<void*> dimension_ranges;
  std::vector<void*> dimension_extents;
  auto add_dimension = [&]<Datatype D>(const tdbrc::Dimension<D>& dimension) {
    using CoordType = tdbrc::Dimension<D>::value_type;
    dimension_names.push_back("d" + std::to_string(dimension_names.size() + 1));
    dimension_types.push_back(static_cast<tiledb_datatype_t>(D));
    dimension_ranges.push_back(
        const_cast<CoordType*>(&dimension.domain.lower_bound));
    dimension_extents.push_back(const_cast<CoordType*>(&dimension.extent));
  };
  std::apply(
      [&]<Datatype... Ds>(const tdbrc::Dimension<Ds>&... dimension) {
        (add_dimension(dimension), ...);
      },
      dimensions);

  std::vector<std::string> attribute_names;
  std::vector<tiledb_datatype_t> attribute_types;
  std::vector<uint32_t> attribute_cell_val_nums;
  std::vector<std::pair<tiledb_filter_type_t, int>> attribute_compressors;
  auto add_attribute = [&](Datatype attribute) {
    attribute_names.push_back("a" + std::to_string(attribute_names.size() + 1));
    attribute_types.push_back(static_cast<tiledb_datatype_t>(attribute));
    attribute_cell_val_nums.push_back(1);
    attribute_compressors.push_back(std::make_pair(TILEDB_FILTER_NONE, -1));
  };
  std::apply(
      [&]<typename... As>(As... attribute) { (add_attribute(attribute), ...); },
      attributes);

  tiledb::test::create_array(
      ctx_,
      array_name_,
      TILEDB_SPARSE,
      dimension_names,
      dimension_types,
      dimension_ranges,
      dimension_extents,
      attribute_names,
      attribute_types,
      attribute_cell_val_nums,
      attribute_compressors,
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      instance.array.capacity,
      instance.array.allow_dups);
}

template <typename Asserter, InstanceType Instance>
void CSparseGlobalOrderFx::run(Instance instance) {
  RCCATCH_REQUIRE(instance.num_user_cells > 0);

  reset_config();

  memory_ = instance.memory;
  update_config();

  // the tile extent is 2
  // create_default_array_1d<Asserter>(instance.array);
  create_array<Asserter, decltype(instance)>(instance);

  // write all fragments
  for (auto& fragment : instance.fragments) {
    write_fragment<Asserter, decltype(fragment)>(fragment);
  }

  std::decay_t<decltype(instance.fragments[0])> expect;
  for (const auto& fragment : instance.fragments) {
    auto expect_dimensions = expect.dimensions();
    auto expect_attributes = expect.attributes();

    if (instance.subarray.empty()) {
      stdx::extend(expect_dimensions, fragment.dimensions());
      stdx::extend(expect_attributes, fragment.attributes());
    } else {
      std::vector<uint64_t> accept;
      for (uint64_t i = 0; i < fragment.size(); i++) {
        if (instance.accept(fragment, i)) {
          accept.push_back(i);
        }
      }
      const auto fdimensions =
          stdx::select(fragment.dimensions(), std::span(accept));
      const auto fattributes =
          stdx::select(fragment.attributes(), std::span(accept));
      stdx::extend(expect_dimensions, stdx::reference_tuple(fdimensions));
      stdx::extend(expect_attributes, stdx::reference_tuple(fattributes));
    }
  }

  // sort for naive comparison
  {
    std::vector<uint64_t> idxs(expect.size());
    std::iota(idxs.begin(), idxs.end(), 0);

    std::sort(idxs.begin(), idxs.end(), [&](uint64_t ia, uint64_t ib) -> bool {
      return std::apply(
          [ia, ib]<typename... Ts>(const std::vector<Ts>&... dims) {
            const auto l = {dims[ia]...};
            const auto r = {dims[ib]...};
            return std::lexicographical_compare(
                l.begin(), l.end(), r.begin(), r.end());
          },
          expect.dimensions());
    });

    expect.dimensions() = stdx::select(
        stdx::reference_tuple(expect.dimensions()), std::span(idxs));
    expect.attributes() = stdx::select(
        stdx::reference_tuple(expect.attributes()), std::span(idxs));
  }

  std::vector<int> expectcoords;
  std::vector<int> expectatts;
  std::tie(expectcoords) = expect.dimensions();
  std::tie(expectatts) = expect.attributes();

  // Open array for reading.
  CApiArray array(ctx_, array_name_.c_str(), TILEDB_READ);

  // Create query
  tiledb_query_t* query;
  auto rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  RCCATCH_REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  RCCATCH_REQUIRE(rc == TILEDB_OK);

  if (!instance.subarray.empty()) {
    tiledb_subarray_t* subarray;
    TRY(tiledb_subarray_alloc(ctx_, array, &subarray));
    for (const auto& range : instance.subarray) {
      TRY(tiledb_subarray_add_range(
          ctx_, subarray, 0, &range.lower_bound, &range.upper_bound, nullptr));
    }
    TRY(tiledb_query_set_subarray_t(ctx_, query, subarray));
    tiledb_subarray_free(&subarray);
  }

  // Prepare output buffer
  std::decay_t<decltype(instance.fragments[0])> out;

  auto outdims = out.dimensions();
  auto outatts = out.attributes();
  std::apply(
      [&](auto&... field) {
        (field.resize(std::max(1UL, expectcoords.size()), 0), ...);
      },
      std::tuple_cat(outdims, outatts));

  // Query loop
  uint64_t outcursor = 0;
  while (true) {
    // make field size locations
    auto dimension_sizes = [&]<typename... Ds>(std::tuple<Ds...> outdims) {
      return query_applicator<Asserter, Ds...>::make_field_sizes(
          outdims, instance.num_user_cells);
    }(outdims);
    auto attribute_sizes = [&]<typename... As>(std::tuple<As...> outdims) {
      return query_applicator<Asserter, As...>::make_field_sizes(
          outdims, instance.num_user_cells);
    }(outatts);

    // add fields to query
    [&]<typename... Ds>(std::tuple<Ds...> dims) {
      query_applicator<Asserter, Ds...>::set(
          ctx_, query, dimension_sizes, dims, "d", outcursor);
    }(outdims);
    [&]<typename... As>(std::tuple<As...> atts) {
      query_applicator<Asserter, As...>::set(
          ctx_, query, attribute_sizes, atts, "a", outcursor);
    }(outatts);

    rc = tiledb_query_submit(ctx_, query);
    {
      const auto err = error_if_any(rc);
      if (err.find("Cannot load enough tiles to emit results from all "
                   "fragments in global order") != std::string::npos) {
        RCCATCH_REQUIRE(!can_complete_in_memory_budget(
            ctx_, array_name_.c_str(), instance));
        tiledb_query_free(&query);
        return;
      } else {
        RCCATCH_REQUIRE("" == err);
      }
    }

    tiledb_query_status_t status;
    rc = tiledb_query_get_status(ctx_, query, &status);
    RCCATCH_REQUIRE(rc == TILEDB_OK);

    const uint64_t dim_num_cells = [&]<typename... Ds>(auto dims) {
      return query_applicator<Asserter, Ds...>::num_cells(
          dims, dimension_sizes);
    }(outdims);
    const uint64_t att_num_cells = [&]<typename... As>(auto atts) {
      return query_applicator<Asserter, As...>::num_cells(
          atts, attribute_sizes);
    }(outatts);

    RCCATCH_REQUIRE(dim_num_cells == att_num_cells);

    if (dim_num_cells < instance.num_user_cells) {
      RCCATCH_REQUIRE(status == TILEDB_COMPLETED);
    } else {
      RCCATCH_REQUIRE(dim_num_cells == instance.num_user_cells);
    }

    outcursor += dim_num_cells;

    if (status == TILEDB_COMPLETED) {
      break;
    }
  }

  // Clean up.
  tiledb_query_free(&query);

  std::apply(
      [outcursor](auto&... outfield) { (outfield.resize(outcursor), ...); },
      std::tuple_cat(outdims, outatts));

  std::vector<int> outcoords, outa1;
  std::tie(outcoords) = outdims;
  std::tie(outa1) = outatts;

  RCCATCH_REQUIRE(expect.dimensions() == outdims);

  // Checking attributes is more complicated because equal coords
  // can manifest their attributes in any order.
  // Identify the runs of equal coords and then compare using those
  size_t attcursor = 0;
  size_t runlength = 1;
  for (size_t i = 1; i < out.size(); i++) {
    if (std::apply(
            [&](const auto&... outdim) {
              return (... && (outdim[i] == outdim[i - 1]));
            },
            outdims)) {
      runlength++;
    } else {
      auto viewtuple = [&](const auto& atttuple, size_t i) {
        return std::apply(
            [&](const auto&... att) { return std::make_tuple(att[i]...); },
            atttuple);
      };

      std::set<decltype(viewtuple(outatts, 0))> outattsrun;
      std::set<decltype(viewtuple(outatts, 0))> expectattsrun;

      for (size_t j = attcursor; j < attcursor + runlength; j++) {
        outattsrun.insert(viewtuple(outatts, j));
        expectattsrun.insert(viewtuple(expect.attributes(), j));
      }

      RCCATCH_REQUIRE(outattsrun == expectattsrun);

      attcursor += runlength;
      runlength = 1;
    }
  }

  // lastly, check the correctness of our memory budgeting function
  RCCATCH_REQUIRE(
      can_complete_in_memory_budget(ctx_, array_name_.c_str(), instance));
}

namespace rc {

Gen<std::vector<tdbrc::Domain<int>>> make_subarray_1d(
    const tdbrc::Domain<int>& domain) {
  // NB: when (if) multi-range subarray is supported for global order
  // (or if this is used for non-global order)
  // change `num_ranges` to use the weighted element version
  std::optional<Gen<int>> num_ranges;
  if (true) {
    num_ranges = gen::just<int>(1);
  } else {
    num_ranges = gen::weightedElement<int>(
        {{50, 1}, {25, 2}, {13, 3}, {7, 4}, {4, 5}, {1, 6}});
  }

  return gen::mapcat(*num_ranges, [domain](int num_ranges) {
    return gen::container<std::vector<tdbrc::Domain<int>>>(
        num_ranges, rc::make_range<int>(domain));
  });
}

template <>
struct Arbitrary<FxRun1D> {
  static Gen<FxRun1D> arbitrary() {
    constexpr Datatype DimensionType = Datatype::INT32;
    using CoordType = tiledb::type::datatype_traits<DimensionType>::value_type;

    auto dimension = gen::arbitrary<tdbrc::Dimension<DimensionType>>();

    auto fragments =
        gen::mapcat(dimension, [](tdbrc::Dimension<DimensionType> dimension) {
          auto fragment = gen::map(
              rc::make_fragment_1d<CoordType, int>(dimension.domain),
              [](tdbrc::Fragment1D<CoordType, int> fragment) {
                return FxFragment1D{
                    .coords = fragment.dim_,
                    .atts = std::get<0>(fragment.atts_)};
              });

          return gen::tuple(
              gen::just(dimension),
              make_subarray_1d(dimension.domain),
              gen::nonEmpty(
                  gen::container<std::vector<FxFragment1D>>(fragment)));
        });

    auto num_user_cells = gen::inRange(1, 8 * 1024 * 1024);

    return gen::apply(
        [](std::tuple<
               tdbrc::Dimension<DimensionType>,
               std::vector<tdbrc::Domain<CoordType>>,
               std::vector<FxFragment1D>> fragments,
           int num_user_cells) {
          FxRun1D instance;
          std::tie(
              instance.array.dimension, instance.subarray, instance.fragments) =
              fragments;

          instance.num_user_cells = num_user_cells;
          instance.array.allow_dups = true;

          return instance;
        },
        fragments,
        num_user_cells);
  }
};

template <>
void show<FxRun1D>(const FxRun1D& instance, std::ostream& os) {
  size_t f = 0;

  os << "{" << std::endl;
  os << "\t\"fragments\": [" << std::endl;
  for (const auto& fragment : instance.fragments) {
    os << "\t\t{" << std::endl;
    os << "\t\t\t\"coords\": [" << std::endl;
    os << "\t\t\t\t";
    show(fragment.coords, os);
    os << std::endl;
    os << "\t\t\t], " << std::endl;
    os << "\t\t\t\"atts\": [" << std::endl;
    os << "\t\t\t\t";
    show(fragment.atts, os);
    os << std::endl;
    os << "\t\t\t] " << std::endl;
    os << "\t\t}";
    if ((f++) + 1 < instance.fragments.size()) {
      os << ", " << std::endl;
    } else {
      os << std::endl;
    }
  }
  os << "\t]," << std::endl;
  os << "\t\"num_user_cells\": " << instance.num_user_cells << std::endl;
  os << "\t\"array\": {" << std::endl;
  os << "\t\t\"allow_dups\": " << instance.array.allow_dups << std::endl;
  os << "\t\t\"domain\": [" << instance.array.dimension.domain.lower_bound
     << ", " << instance.array.dimension.domain.upper_bound << "],"
     << std::endl;
  os << "\t\t\"extent\": " << instance.array.dimension.extent << std::endl;
  os << "\t}," << std::endl;
  os << "\t\"memory\": {" << std::endl;
  os << "\t\t\"total_budget\": " << instance.memory.total_budget_ << ", "
     << std::endl;
  os << "\t\t\"ratio_tile_ranges\": " << instance.memory.ratio_tile_ranges_
     << ", " << std::endl;
  os << "\t\t\"ratio_array_data\": " << instance.memory.ratio_array_data_
     << ", " << std::endl;
  os << "\t\t\"ratio_coords\": " << instance.memory.ratio_coords_ << std::endl;
  os << "\t}" << std::endl;
  os << "}";
}

}  // namespace rc

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: rapidcheck 1d",
    "[sparse-global-order]") {
  SECTION("Rapidcheck") {
    rc::prop("rapidcheck arbitrary 1d", [this](FxRun1D instance) {
      run<tiledb::test::AsserterRapidcheck, FxRun1D>(instance);
    });
  }
}

/**
 * This test will fail if multi-range subarrays become supported
 * for global order.
 * When that happens please update all the related `NB` comments
 * (and also feel free to remove this at that time).
 */
TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: multi-range subarray signal",
    "[sparse-global-order]") {
  FxFragment1D fragment0;
  fragment0.coords.push_back(1);
  fragment0.atts.push_back(1);

  FxRun1D instance;
  instance.fragments.push_back(fragment0);
  instance.subarray.push_back({1, 1});
  instance.subarray.push_back({3, 3});

  // NB: `Rapidcheck` just throws a normal exception,
  // we are not actually in a rapidcheck context.
  REQUIRE_THROWS(run<tiledb::test::AsserterRapidcheck, FxRun1D>(instance));

  tiledb_error_t* error = NULL;
  auto rc = tiledb_ctx_get_last_error(ctx_, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error != nullptr);

  const char* msg;
  rc = tiledb_error_message(error, &msg);
  REQUIRE(rc == TILEDB_OK);

  REQUIRE(
      std::string(msg).find(
          "Multi-range reads are not supported on a global order query") !=
      std::string::npos);
}

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
 * Runs sparse global order reader on a 2D array which is
 * highly representative of SOMA workloads
 */
TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: benchmark compare",
    "[sparse-global-order]") {
  using Asserter = tiledb::test::AsserterCatch;

  const char* array_uri =
      "s3://tiledb-johnkerl/s/v/tabula-sapiens-immune/ms/RNA/X/data";

  memory_.total_budget_ = std::to_string(1024 * 1024 * 1024);
  memory_.ratio_tile_ranges_ = "0.01";
  update_config();

  const uint64_t num_user_cells = 1024 * 1024;

  std::vector<int64_t> offdim0(num_user_cells);
  std::vector<int64_t> offdim1(num_user_cells);
  std::vector<float> offdata(num_user_cells);
  std::vector<int64_t> ondim0(num_user_cells);
  std::vector<int64_t> ondim1(num_user_cells);
  std::vector<float> ondata(num_user_cells);

  // Open array for reading.
  CApiArray array(ctx_, array_uri, TILEDB_READ);

  // Create query which does NOT do merge
  tiledb_query_t* off_query;
  auto rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &off_query);
  RCCATCH_REQUIRE("" == error_if_any(rc));
  rc = tiledb_query_set_layout(ctx_, off_query, TILEDB_GLOBAL_ORDER);
  RCCATCH_REQUIRE("" == error_if_any(rc));

  // Create query which DOES do merge
  tiledb_query_t* on_query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &on_query);
  RCCATCH_REQUIRE("" == error_if_any(rc));
  rc = tiledb_query_set_layout(ctx_, on_query, TILEDB_GLOBAL_ORDER);
  RCCATCH_REQUIRE("" == error_if_any(rc));
  {
    tiledb_config_t* qconfig;
    tiledb_error_t* error = nullptr;
    RCCATCH_REQUIRE(tiledb_config_alloc(&qconfig, &error) == TILEDB_OK);
    RCCATCH_REQUIRE(error == nullptr);
    rc = tiledb_config_set(
        qconfig,
        "sm.query.sparse_global_order.preprocess_tile_merge",
        "true",
        &error);
    RCCATCH_REQUIRE("" == error_if_any(rc));
    rc = tiledb_query_set_config(ctx_, on_query, qconfig);
    RCCATCH_REQUIRE("" == error_if_any(rc));
    tiledb_config_free(&qconfig);
  }

  TimeKeeper time_keeper;

  // helper to do basic checks on both
  auto do_submit = [&](auto& key,
                       auto& query,
                       auto& outdim0,
                       auto& outdim1,
                       auto& outdata) -> uint64_t {
    uint64_t outdim0_size = outdim0.size() * sizeof(int64_t);
    uint64_t outdim1_size = outdim1.size() * sizeof(int64_t);
    uint64_t outdata_size = outdata.size() * sizeof(float);

    rc = tiledb_query_set_data_buffer(
        ctx_, query, "soma_dim_0", &outdim0[0], &outdim0_size);
    RCCATCH_REQUIRE("" == error_if_any(rc));
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "soma_dim_1", &outdim1[0], &outdim1_size);
    RCCATCH_REQUIRE("" == error_if_any(rc));
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "soma_data", &outdata[0], &outdata_size);
    RCCATCH_REQUIRE("" == error_if_any(rc));

    {
      tiledb::sm::stats::DurationInstrument<TimeKeeper> qtimer =
          time_keeper.start_timer(key);
      rc = tiledb_query_submit(ctx_, query);
      RCCATCH_REQUIRE("" == error_if_any(rc));
    }

    tiledb_query_status_t status;
    rc = tiledb_query_get_status(ctx_, query, &status);
    RCCATCH_REQUIRE("" == error_if_any(rc));

    RCCATCH_REQUIRE(outdim0_size % sizeof(int64_t) == 0);
    RCCATCH_REQUIRE(outdim1_size % sizeof(int64_t) == 0);
    RCCATCH_REQUIRE(outdata_size % sizeof(float) == 0);

    const uint64_t num_dim0 = outdim0_size / sizeof(int64_t);
    const uint64_t num_dim1 = outdim1_size / sizeof(int64_t);
    const uint64_t num_data = outdata_size / sizeof(float);

    RCCATCH_REQUIRE(num_dim0 == num_dim1);
    RCCATCH_REQUIRE(num_dim0 == num_data);

    if (num_dim0 < outdim0.size()) {
      RCCATCH_REQUIRE(status == TILEDB_COMPLETED);
    }

    return num_dim0;
  };

  while (true) {
    const uint64_t off_num_cells =
        do_submit("off", off_query, offdim0, offdim1, offdata);
    const uint64_t on_num_cells =
        do_submit("on", on_query, ondim0, ondim1, ondata);

    RCCATCH_REQUIRE(off_num_cells == on_num_cells);

    offdim0.resize(off_num_cells);
    offdim1.resize(off_num_cells);
    offdata.resize(off_num_cells);
    ondim0.resize(on_num_cells);
    ondim1.resize(on_num_cells);
    ondata.resize(on_num_cells);

    RCCATCH_REQUIRE(offdim0 == ondim0);
    RCCATCH_REQUIRE(offdim1 == ondim1);
    RCCATCH_REQUIRE(offdata == ondata);

    offdim0.resize(num_user_cells);
    offdim1.resize(num_user_cells);
    offdata.resize(num_user_cells);
    ondim0.resize(num_user_cells);
    ondim1.resize(num_user_cells);
    ondata.resize(num_user_cells);

    if (off_num_cells < num_user_cells) {
      break;
    }
  }

  time_keeper.dump_durations("/tmp/time_keeper.out");
}
