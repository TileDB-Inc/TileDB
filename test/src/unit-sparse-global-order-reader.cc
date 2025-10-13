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

#include "test/support/rapidcheck/array_templates.h"
#include "test/support/rapidcheck/query_condition.h"
#include "test/support/src/array_helpers.h"
#include "test/support/src/array_templates.h"
#include "test/support/src/error_helpers.h"
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/ast/query_ast.h"
#include "tiledb/sm/query/readers/result_tile.h"
#include "tiledb/sm/query/readers/sparse_global_order_reader.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

#include <test/support/assert_helpers.h>

#include <algorithm>
#include <fstream>
#include <numeric>

using namespace tiledb;
using namespace tiledb::test;

using tiledb::sm::Datatype;
using tiledb::test::templates::AttributeType;
using tiledb::test::templates::DimensionType;
using tiledb::test::templates::FragmentType;
using tiledb::test::templates::query_buffers;
using tiledb::test::templates::static_attribute;

template <typename D>
using Subarray1DType = std::vector<templates::Domain<D>>;

template <typename D1, typename D2>
using Subarray2DType = std::vector<std::pair<
    std::optional<templates::Domain<D1>>,
    std::optional<templates::Domain<D2>>>>;

namespace rc {

template <DimensionType D>
Gen<std::vector<templates::Domain<D>>> make_subarray_1d(
    const templates::Domain<D>& domain);

Gen<Subarray2DType<int, int>> make_subarray_2d(
    const templates::Domain<int>& d1, const templates::Domain<int>& d2);

}  // namespace rc

/* ********************************* */
/*         STRUCT DEFINITION         */
/* ********************************* */

/**
 * Options for configuring the CSparseGlobalFx default 1D array
 */
template <Datatype DIMENSION_TYPE = Datatype::INT32>
struct DefaultArray1DConfig {
  uint64_t capacity_;
  bool allow_dups_;

  templates::Dimension<DIMENSION_TYPE> dimension_;

  DefaultArray1DConfig()
      : capacity_(2)
      , allow_dups_(false) {
    dimension_.domain.lower_bound = 1;
    dimension_.domain.upper_bound = 200;
    dimension_.extent = 2;
  }

  DefaultArray1DConfig with_allow_dups(bool allow_dups) const {
    auto copy = *this;
    copy.allow_dups_ = allow_dups;
    return copy;
  }
};

/**
 * An instance of one-dimension array input to `CSparseGlobalOrderFx::run`
 */
template <
    Datatype DIMENSION_TYPE = Datatype::INT32,
    typename ATTR_TYPE = static_attribute<Datatype::INT32, 1, false>,
    typename... ATTR_TYPES>
struct FxRun1D {
  using CoordType = tiledb::type::datatype_traits<DIMENSION_TYPE>::value_type;

  using FragmentType = templates::Fragment1D<
      CoordType,
      typename ATTR_TYPE::cell_type,
      typename ATTR_TYPES::cell_type...>;

  static constexpr Datatype DimensionType = DIMENSION_TYPE;

  uint64_t num_user_cells;
  std::vector<FragmentType> fragments;

  // NB: for now this always has length 1, global order query does not
  // support multi-range subarray
  std::vector<templates::Domain<CoordType>> subarray;

  // for evaluating
  std::optional<tdb_unique_ptr<tiledb::sm::ASTNode>> condition;

  DefaultArray1DConfig<DIMENSION_TYPE> array;
  SparseGlobalOrderReaderMemoryBudget memory;

  uint64_t tile_capacity() const {
    return array.capacity_;
  }

  bool allow_duplicates() const {
    return array.allow_dups_;
  }

  tiledb_layout_t tile_order() const {
    // for 1D it is the same
    return TILEDB_ROW_MAJOR;
  }

  tiledb_layout_t cell_order() const {
    // for 1D it is the same
    return TILEDB_ROW_MAJOR;
  }

  /**
   * Add `subarray` to a read query
   */
  template <typename Asserter>
  capi_return_t apply_subarray(
      tiledb_ctx_t* ctx, tiledb_subarray_t* subarray) const {
    for (const auto& range : this->subarray) {
      RETURN_IF_ERR(tiledb_subarray_add_range(
          ctx, subarray, 0, &range.lower_bound, &range.upper_bound, nullptr));
    }
    return TILEDB_OK;
  }

  /**
   * @return true if the cell at index `record` in `fragment` passes `subarray`
   */
  bool pass_subarray(const FragmentType& fragment, int record) const {
    if (subarray.empty()) {
      return true;
    } else {
      const CoordType coord = fragment.dimension()[record];
      for (const auto& range : subarray) {
        if (range.contains(coord)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * @return true if `mbr` intersects with any of the ranges in `subarray`
   */
  bool intersects(const sm::NDRange& mbr) const {
    auto accept = [&](const auto& range) {
      const auto& untyped_mbr = mbr[0];
      const templates::Domain<CoordType> typed_mbr(
          untyped_mbr.start_as<CoordType>(), untyped_mbr.end_as<CoordType>());
      return range.intersects(typed_mbr);
    };
    return subarray.empty() ||
           std::any_of(subarray.begin(), subarray.end(), accept);
  }

  /**
   * @return a new range which is `mbr` with its upper bound "clamped" to that
   * of `subarray`
   */
  sm::NDRange clamp(const sm::NDRange& mbr) const {
    if (subarray.empty()) {
      return mbr;
    }
    assert(subarray.size() == 1);
    if (subarray[0].upper_bound < mbr[0].end_as<CoordType>()) {
      // in this case, the bitmap will filter out the other coords in the
      // tile and it will be discarded
      return std::vector<type::Range>{subarray[0].range()};
    } else {
      return mbr;
    }
  }

  std::tuple<const templates::Dimension<DIMENSION_TYPE>&> dimensions() const {
    return std::tuple<const templates::Dimension<DIMENSION_TYPE>&>(
        array.dimension_);
  }

  std::vector<std::tuple<Datatype, uint32_t, bool>> attributes() const {
    std::vector<std::tuple<Datatype, uint32_t, bool>> a = {
        tiledb::test::templates::attribute_properties<ATTR_TYPE>()};
    (a.push_back(tiledb::test::templates::attribute_properties<ATTR_TYPES>()),
     ...);
    return a;
  }
};

struct FxRun2D {
  using Coord0Type = int;
  using Coord1Type = int;
  using Attribute = static_attribute<Datatype::INT32, 1, false>;
  using FragmentType =
      templates::Fragment2D<Coord0Type, Coord1Type, Attribute::cell_type>;

  std::vector<FragmentType> fragments;
  std::vector<std::pair<
      std::optional<templates::Domain<Coord0Type>>,
      std::optional<templates::Domain<Coord1Type>>>>
      subarray;
  std::optional<tdb_unique_ptr<tiledb::sm::ASTNode>> condition;

  size_t num_user_cells;

  uint64_t capacity;
  bool allow_dups;
  tiledb_layout_t tile_order_;
  tiledb_layout_t cell_order_;
  templates::Dimension<Datatype::INT32> d1;
  templates::Dimension<Datatype::INT32> d2;

  SparseGlobalOrderReaderMemoryBudget memory;

  FxRun2D()
      : capacity(64)
      , allow_dups(true)
      , tile_order_(TILEDB_ROW_MAJOR)
      , cell_order_(TILEDB_ROW_MAJOR) {
    d1.domain = templates::Domain<int>(1, 200);
    d1.extent = 8;
    d2.domain = templates::Domain<int>(1, 200);
    d2.extent = 8;
  }

  uint64_t tile_capacity() const {
    return capacity;
  }

  bool allow_duplicates() const {
    return allow_dups;
  }

  tiledb_layout_t tile_order() const {
    return tile_order_;
  }

  tiledb_layout_t cell_order() const {
    return cell_order_;
  }

  /**
   * Add `subarray` to a read query
   */
  template <typename Asserter>
  capi_return_t apply_subarray(
      tiledb_ctx_t* ctx, tiledb_subarray_t* subarray) const {
    for (const auto& range : this->subarray) {
      if (range.first.has_value()) {
        RETURN_IF_ERR(tiledb_subarray_add_range(
            ctx,
            subarray,
            0,
            &range.first->lower_bound,
            &range.first->upper_bound,
            nullptr));
      } else {
        RETURN_IF_ERR(tiledb_subarray_add_range(
            ctx,
            subarray,
            0,
            &d1.domain.lower_bound,
            &d1.domain.upper_bound,
            nullptr));
      }
      if (range.second.has_value()) {
        RETURN_IF_ERR(tiledb_subarray_add_range(
            ctx,
            subarray,
            1,
            &range.second->lower_bound,
            &range.second->upper_bound,
            nullptr));
      } else {
        RETURN_IF_ERR(tiledb_subarray_add_range(
            ctx,
            subarray,
            1,
            &d2.domain.lower_bound,
            &d2.domain.upper_bound,
            nullptr));
      }
    }
    return TILEDB_OK;
  }

  /**
   * @return true if the cell at index `record` in `fragment` passes `subarray`
   */
  bool pass_subarray(const FragmentType& fragment, int record) const {
    if (subarray.empty() && !condition.has_value()) {
      return true;
    } else {
      const int r = fragment.d1()[record], c = fragment.d2()[record];
      for (const auto& range : subarray) {
        if (range.first.has_value() && !range.first->contains(r)) {
          continue;
        } else if (range.second.has_value() && !range.second->contains(c)) {
          continue;
        } else {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * @return true if `mbr` intersects with any of the ranges in `subarray`
   */
  bool intersects(const sm::NDRange& mbr) const {
    if (subarray.empty()) {
      return true;
    }

    const templates::Domain<Coord0Type> typed_domain0(
        mbr[0].start_as<Coord0Type>(), mbr[0].end_as<Coord0Type>());
    const templates::Domain<Coord0Type> typed_domain1(
        mbr[1].start_as<Coord1Type>(), mbr[1].end_as<Coord1Type>());

    for (const auto& range : subarray) {
      if (range.first.has_value() && !range.first->intersects(typed_domain0)) {
        continue;
      } else if (
          range.second.has_value() &&
          !range.second->intersects(typed_domain1)) {
        continue;
      } else {
        return true;
      }
    }
    return false;
  }

  /**
   * @return a new range which is `mbr` with its upper bound "clamped" to that
   * of `subarray`
   */
  sm::NDRange clamp(const sm::NDRange& mbr) const {
    sm::NDRange clamped = mbr;
    for (const auto& range : subarray) {
      if (range.first.has_value() &&
          range.first->upper_bound < clamped[0].end_as<Coord0Type>()) {
        clamped[0].set_end_fixed(&range.first->upper_bound);
      }
      if (range.second.has_value() &&
          range.second->upper_bound < clamped[1].end_as<Coord1Type>()) {
        clamped[1].set_end_fixed(&range.second->upper_bound);
      }
    }
    return clamped;
  }

  using CoordsRefType = std::tuple<
      const templates::Dimension<Datatype::INT32>&,
      const templates::Dimension<Datatype::INT32>&>;
  CoordsRefType dimensions() const {
    return CoordsRefType(d1, d2);
  }

  std::vector<std::tuple<Datatype, uint32_t, bool>> attributes() const {
    std::vector<std::tuple<Datatype, uint32_t, bool>> a = {
        tiledb::test::templates::attribute_properties<Attribute>()};
    return a;
  }
};

/**
 * Describes types which can be used with `CSparseGlobalOrderFx::run`.
 */
template <typename T>
concept InstanceType = requires(const T& instance) {
  { instance.tile_capacity() } -> std::convertible_to<uint64_t>;
  { instance.allow_duplicates() } -> std::same_as<bool>;
  { instance.tile_order() } -> std::same_as<tiledb_layout_t>;

  { instance.num_user_cells } -> std::convertible_to<size_t>;

  instance.fragments;
  instance.memory;
  instance.subarray;
  instance.condition;
  instance.dimensions();
  instance.attributes();

  // also `pass_subarray(Self::FragmentType, int)`, unclear how to represent
  // that
};

struct CSparseGlobalOrderFx {
  VFSTestSetup vfs_test_setup_;

  std::string array_name_;
  const char* ARRAY_NAME = "test_sparse_global_order";
  tiledb_array_t* array_ = nullptr;

  SparseGlobalOrderReaderMemoryBudget memory_;

  tiledb_ctx_t* context() const {
    return vfs_test_setup_.ctx_c;
  }

  template <typename Asserter = tiledb::test::AsserterCatch>
  void create_default_array_1d(
      const DefaultArray1DConfig<>& config = DefaultArray1DConfig<>());

  void create_default_array_1d_strings(bool allow_dups = false);

  template <typename Asserter = tiledb::test::AsserterCatch>
  void write_1d_fragment(
      int* coords, uint64_t* coords_size, int* data, uint64_t* data_size);

  template <typename Asserter, FragmentType Fragment>
  void write_fragment(const Fragment& fragment, CApiArray* existing = nullptr);

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

  template <typename Asserter, InstanceType Instance>
  DeleteArrayGuard run_create(Instance& instance);
  template <typename Asserter, InstanceType Instance>
  void run_execute(Instance& instance);

  /**
   * Runs an input against a fresh array.
   * Inserts fragments one at a time, then reads them back in global order
   * and checks that what we read out matches what we put in.
   */
  template <typename Asserter, InstanceType Instance>
  void run(Instance& instance);

  template <typename CAPIReturn>
  std::optional<std::string> error_if_any(CAPIReturn apirc) const;

  CSparseGlobalOrderFx();
  ~CSparseGlobalOrderFx();

 protected:
  // test helper functions

  template <typename Asserter>
  void instance_fragment_skew(
      size_t fragment_size,
      size_t num_user_cells,
      int extent,
      const std::vector<templates::Domain<int>>& subarray =
          std::vector<templates::Domain<int>>(),
      tdb_unique_ptr<tiledb::sm::ASTNode> condition = nullptr);

  template <typename Asserter>
  void instance_fragment_interleave(
      size_t fragment_size,
      size_t num_user_cells,
      const std::vector<templates::Domain<int>>& subarray = {},
      tdb_unique_ptr<tiledb::sm::ASTNode> condition = nullptr);

  template <typename Asserter>
  void instance_fragment_wide_overlap(
      size_t num_fragments,
      size_t fragment_size,
      size_t num_user_cells,
      bool allow_dups,
      const std::vector<templates::Domain<int>>& subarray =
          std::vector<templates::Domain<int>>(),
      tdb_unique_ptr<tiledb::sm::ASTNode> condition = nullptr);

  template <typename Asserter>
  void instance_merge_bound_duplication(
      size_t num_fragments,
      size_t fragment_size,
      size_t num_user_cells,
      size_t tile_capacity,
      bool allow_dups,
      const Subarray1DType<int>& subarray = {},
      tdb_unique_ptr<tiledb::sm::ASTNode> condition = nullptr);

  template <typename Asserter>
  void instance_out_of_order_mbrs(
      tiledb_layout_t tile_order,
      tiledb_layout_t cell_order,
      size_t num_fragments,
      size_t fragment_size,
      size_t num_user_cells,
      size_t tile_capacity,
      bool allow_dups,
      const Subarray2DType<int, int>& subarray = {},
      tdb_unique_ptr<tiledb::sm::ASTNode> condition = nullptr);

  template <typename Asserter>
  void instance_fragment_skew_2d_merge_bound(
      tiledb_layout_t tile_order,
      tiledb_layout_t cell_order,
      size_t approximate_memory_tiles,
      size_t num_user_cells,
      size_t num_fragments,
      size_t tile_capacity,
      bool allow_dups,
      const Subarray2DType<int, int>& subarray = {},
      tdb_unique_ptr<tiledb::sm::ASTNode> condition = nullptr);

  template <typename Asserter>
  void instance_fragment_full_copy_1d(
      size_t num_fragments,
      templates::Dimension<Datatype::INT64> dimension,
      const Subarray1DType<int64_t>& subarray = {},
      tdb_unique_ptr<tiledb::sm::ASTNode> condition = nullptr);

  template <typename Asserter>
  void instance_repeatable_read_submillisecond(
      const std::vector<uint64_t>& fragment_same_timestamp_runs);
};

template <typename CAPIReturn>
std::optional<std::string> CSparseGlobalOrderFx::error_if_any(
    CAPIReturn apirc) const {
  return tiledb::test::error_if_any(context(), apirc);
}

CSparseGlobalOrderFx::CSparseGlobalOrderFx() {
  reset_config();

  array_name_ = vfs_test_setup_.array_uri("tiledb_test");
}

CSparseGlobalOrderFx::~CSparseGlobalOrderFx() {
  if (array_) {
    tiledb_array_free(&array_);
  }
}

void CSparseGlobalOrderFx::reset_config() {
  memory_ = SparseGlobalOrderReaderMemoryBudget();
  update_config();
}

void CSparseGlobalOrderFx::update_config() {
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

  REQUIRE(memory_.apply(config) == nullptr);

  vfs_test_setup_.update_config(config);
}

template <typename Asserter>
void CSparseGlobalOrderFx::create_default_array_1d(
    const DefaultArray1DConfig<>& config) {
  tiledb::test::create_array(
      context(),
      array_name_,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_INT32},
      std::vector<void*>{(void*)(&config.dimension_.domain.lower_bound)},
      std::vector<void*>{(void*)(&config.dimension_.extent)},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      config.capacity_,
      config.allow_dups_);
}

void CSparseGlobalOrderFx::create_default_array_1d_strings(bool allow_dups) {
  int domain[] = {1, 200};
  int tile_extent = 2;
  tiledb::test::create_array(
      context(),
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
  CApiArray array(context(), array_name_.c_str(), TILEDB_WRITE);

  // Create the query.
  tiledb_query_t* query;
  auto rc = tiledb_query_alloc(context(), array, TILEDB_WRITE, &query);
  ASSERTER(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(context(), query, TILEDB_UNORDERED);
  ASSERTER(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(context(), query, "a", data, data_size);
  ASSERTER(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(context(), query, "d", coords, coords_size);
  ASSERTER(rc == TILEDB_OK);

  // Submit query.
  rc = tiledb_query_submit(context(), query);
  ASSERTER(std::optional<std::string>() == error_if_any(rc));

  // Clean up.
  tiledb_query_free(&query);
}

/**
 * Writes a generic `FragmentType` into the array.
 */
template <typename Asserter, FragmentType Fragment>
void CSparseGlobalOrderFx::write_fragment(
    const Fragment& fragment, CApiArray* existing) {
  std::optional<CApiArray> constructed;
  if (!existing) {
    // Open array for writing.
    constructed.emplace(context(), array_name_.c_str(), TILEDB_WRITE);
    existing = &constructed.value();
  }

  CApiArray& array = *existing;
  Context cppctx = vfs_test_setup_.ctx();
  Array cpparray(cppctx, array, false);

  templates::query::write_fragment<Asserter, Fragment>(
      fragment, cpparray, TILEDB_UNORDERED);
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
  auto rc = tiledb_array_alloc(context(), array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(context(), array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Create the query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(context(), array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(context(), query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(context(), query, "a", data, data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      context(), query, "a", offsets, offsets_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(context(), query, "d", coords, coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Submit query.
  rc = tiledb_query_submit(context(), query);
  REQUIRE(rc == TILEDB_OK);

  // Close array.
  rc = tiledb_array_close(context(), array);
  REQUIRE(rc == TILEDB_OK);

  // Clean up.
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void CSparseGlobalOrderFx::write_delete_condition(
    char* value_to_delete, uint64_t value_size) {
  // Open array for delete.
  tiledb_array_t* array;
  auto rc = tiledb_array_alloc(context(), array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(context(), array, TILEDB_DELETE);
  REQUIRE(rc == TILEDB_OK);

  // Create the query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(context(), array, TILEDB_DELETE, &query);
  REQUIRE(rc == TILEDB_OK);

  // Add condition.
  tiledb_query_condition_t* qc;
  rc = tiledb_query_condition_alloc(context(), &qc);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_condition_init(
      context(), qc, "a", value_to_delete, value_size, TILEDB_EQ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_condition(context(), query, qc);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  rc = tiledb_query_submit(context(), query);
  REQUIRE(rc == TILEDB_OK);

  // Close array.
  rc = tiledb_array_close(context(), array);
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
  CApiArray array(context(), array_name_.c_str(), TILEDB_READ);

  const char* dimname = array->array()
                            ->array_schema_latest()
                            .domain()
                            .dimension_ptr(0)
                            ->name()
                            .c_str();
  const char* attname =
      array->array()->array_schema_latest().attribute(0)->name().c_str();

  // Create query.
  tiledb_query_t* query;
  auto rc = tiledb_query_alloc(context(), array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  if (set_subarray) {
    // Set subarray.
    tiledb_subarray_t* sub;
    rc = tiledb_subarray_alloc(context(), array, &sub);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_subarray_set_subarray(context(), sub, subarray.data());
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_subarray_t(context(), query, sub);
    CHECK(rc == TILEDB_OK);
    tiledb_subarray_free(&sub);
  }

  if (qc_idx != 0) {
    tiledb_query_condition_t* query_condition = nullptr;
    rc = tiledb_query_condition_alloc(context(), &query_condition);
    CHECK(rc == TILEDB_OK);

    if (qc_idx == 1) {
      int32_t val = 11;
      rc = tiledb_query_condition_init(
          context(),
          query_condition,
          attname,
          &val,
          sizeof(int32_t),
          TILEDB_LT);
      CHECK(rc == TILEDB_OK);
    } else if (qc_idx == 2) {
      // Negated query condition should produce the same results.
      int32_t val = 11;
      tiledb_query_condition_t* qc;
      rc = tiledb_query_condition_alloc(context(), &qc);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_condition_init(
          context(), qc, attname, &val, sizeof(int32_t), TILEDB_GE);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_condition_negate(context(), qc, &query_condition);
      CHECK(rc == TILEDB_OK);

      tiledb_query_condition_free(&qc);
    }

    rc = tiledb_query_set_condition(context(), query, query_condition);
    CHECK(rc == TILEDB_OK);

    tiledb_query_condition_free(&query_condition);
  }

  rc = tiledb_query_set_layout(context(), query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(context(), query, attname, data, data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      context(), query, dimname, coords, coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  auto ret = tiledb_query_submit(context(), query);
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
  auto rc = tiledb_array_alloc(context(), array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(context(), array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(context(), array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  if (set_subarray) {
    // Set subarray.
    tiledb_subarray_t* sub;
    rc = tiledb_subarray_alloc(context(), array, &sub);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_subarray_set_subarray(context(), sub, subarray.data());
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_subarray_t(context(), query, sub);
    CHECK(rc == TILEDB_OK);
    tiledb_subarray_free(&sub);
  }

  rc = tiledb_query_set_layout(context(), query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(context(), query, "a", data, data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      context(), query, "a", offsets, offsets_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(context(), query, "d", coords, coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query.
  auto ret = tiledb_query_submit(context(), query);
  if (query_ret == nullptr || array_ret == nullptr) {
    // Clean up.
    rc = tiledb_array_close(context(), array);
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
 * Determines whether the array fragments are "too wide" to merge with the
 * given memory budget. "Too wide" means that there are overlapping tiles from
 * different fragments to fit in memory, so the merge cannot progress without
 * producing out-of-order data*.
 *
 * *there are ways to get around this but they are not implemented.
 *
 * An answer cannot be determined if the tile MBR lower bounds within a
 * fragment are not in order. This is common for 2+ dimensions, so this won't
 * even try.
 *
 * @return std::nullopt if an answer cannot be determined, true/false if it
 * can
 */
template <typename Asserter, InstanceType Instance>
static std::optional<bool> can_complete_in_memory_budget(
    tiledb_ctx_t* ctx, const char* array_uri, const Instance& instance) {
  if constexpr (std::tuple_size_v<decltype(instance.dimensions())> > 1) {
    // don't even bother
    return std::nullopt;
  }

  CApiArray array(ctx, array_uri, TILEDB_READ);

  const auto& array_schema = array->array()->array_schema_latest();
  const auto& fragment_metadata = array->array()->fragment_metadata();

  for (const auto& fragment : fragment_metadata) {
    const_cast<sm::FragmentMetadata*>(fragment.get())
        ->loaded_metadata()
        ->load_rtree(array->array()->get_encryption_key());
  }

  auto tiles_size = [&](unsigned f, uint64_t t) {
    using BitmapType = uint8_t;

    size_t data_size = 0;
    size_t bitmap_size = 0;

    for (size_t d = 0;
         d < std::tuple_size<decltype(instance.dimensions())>::value;
         d++) {
      data_size +=
          fragment_metadata[f]->tile_size("d" + std::to_string(d + 1), t);
    }

    if (!instance.subarray.empty()) {
      bitmap_size += fragment_metadata[f]->cell_num(t) * sizeof(BitmapType);
    }

    if (instance.condition.has_value()) {
      // query condition fields are eagerly materialized
      std::unordered_set<std::string> fields;
      instance.condition.value()->get_field_names(fields);

      for (const auto& field : fields) {
        if (!array_schema.is_dim(field)) {
          data_size += tiledb::sm::ReaderBase::get_attribute_tile_size(
              array_schema, fragment_metadata, field, f, t);
        }
        if (array_schema.is_nullable(field)) {
          data_size += fragment_metadata[f]->cell_num(t) *
                       tiledb::sm::constants::cell_validity_size;
        }
      }

      bitmap_size += fragment_metadata[f]->cell_num(t) * sizeof(BitmapType);
    }

    const size_t rt_size = sizeof(sm::GlobalOrderResultTile<BitmapType>);
    return data_size + rt_size + bitmap_size;
  };

  const auto& domain = array_schema.domain();
  sm::GlobalCellCmp globalcmp(domain);
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
   * Simulates comparison of a loaded tile to the merge bound.
   *
   * @return true if `rtl.upper < rtr.lower`, i.e. we can process
   * the entirety of `rtl` before any of `rtr`
   */
  auto cmp_merge_bound = [&](const RT& rtl, const RT& rtr) {
    const auto rtl_mbr_clamped = instance.clamp(
        fragment_metadata[rtl.fragment_idx_]->mbr(rtl.tile_idx_));
    const sm::RangeUpperBound l_mbr = {.mbr = rtl_mbr_clamped};
    const sm::RangeLowerBound r_mbr = {
        .mbr = fragment_metadata[rtr.fragment_idx_]->mbr(rtr.tile_idx_)};

    const int cmp = globalcmp.compare(l_mbr, r_mbr);
    if (instance.allow_duplicates()) {
      return cmp <= 0;
    } else {
      return cmp < 0;
    }
  };

  // order all tiles on their lower bound
  std::priority_queue<RT, std::vector<RT>, decltype(cmp_pq_lower_bound)>
      mbr_lower_bound(cmp_pq_lower_bound);
  for (unsigned f = 0; f < instance.fragments.size(); f++) {
    for (uint64_t t = 0; t < fragment_metadata[f]->tile_num(); t++) {
      if (instance.intersects(fragment_metadata[f]->mbr(t))) {
        mbr_lower_bound.push(sm::ResultTileId(f, t));
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
  /*
   * Iterate through the tiles in the same order that the sparse
   * reader would process them in, tracking memory usage as we go.
   */
  const uint64_t num_tiles = mbr_lower_bound.size();
  uint64_t active_tile_size = sizeof(RT) * num_tiles;
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
           cmp_merge_bound(mbr_upper_bound.top(), next_rt)) {
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
  rc = tiledb_ctx_get_last_error(context(), &error);
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
    tiledb_query_get_status(context(), query, &status);
    if (status == TILEDB_INCOMPLETE) {
      rc = tiledb_query_submit(context(), query);
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
    tiledb_query_get_status(context(), query, &status);
    if (status == TILEDB_INCOMPLETE) {
      rc = tiledb_query_submit(context(), query);
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

template <typename... Args>
struct PAssertFailureCallbackShowRapidcheckInput {
  std::tuple<const Args&...> inputs_;

  PAssertFailureCallbackShowRapidcheckInput(const Args&... inputs)
      : inputs_(inputs...) {
  }

  void operator()() const {
    std::cerr << "LAST RAPIDCHECK INPUT:" << std::endl;
    rc::show<decltype(inputs_)>(inputs_, std::cerr);
    std::cerr << std::endl;
  }
};

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
template <typename Asserter>
void CSparseGlobalOrderFx::instance_fragment_skew(
    size_t fragment_size,
    size_t num_user_cells,
    int extent,
    const std::vector<templates::Domain<int>>& subarray,
    tdb_unique_ptr<tiledb::sm::ASTNode> condition) {
  using InstanceType = FxRun1D<
      Datatype::INT32,
      static_attribute<Datatype::INT32, 1, false>,
      static_attribute<
          Datatype::STRING_UTF8,
          tiledb::sm::cell_val_num_var,
          true>>;

  PAssertFailureCallbackRegistration showInput(
      PAssertFailureCallbackShowRapidcheckInput(
          fragment_size, num_user_cells, extent, subarray, *condition.get()));

  // Write a fragment F0 with unique coordinates
  InstanceType::FragmentType fragment0;
  fragment0.dimension().resize(fragment_size);
  std::iota(fragment0.dimension().begin(), fragment0.dimension().end(), 1);

  // Write a fragment F1 with lots of duplicates
  // [100,100,100,100,100,101,101,101,101,101,102,102,102,102,102,...]
  InstanceType::FragmentType fragment1;
  fragment1.dimension().resize(fragment0.dimension().num_cells());
  for (size_t i = 0; i < fragment1.dimension().num_cells(); i++) {
    fragment1.dimension()[i] =
        static_cast<int>((i / 10) + (fragment0.dimension().num_cells() / 2));
  }

  // atts are whatever, used just for query condition and correctness check
  auto& f0atts = std::get<0>(fragment0.atts_);
  f0atts.resize(fragment0.num_cells());
  std::iota(f0atts.begin(), f0atts.end(), 0);
  for (uint64_t i = 0; i < fragment0.num_cells(); i++) {
    if ((i * i) % 7 == 0) {
      std::get<1>(fragment0.atts_).push_back(std::nullopt);
    } else {
      std::vector<char> f0str;
      for (uint64_t j = 0; j < (i * i) % 11; j++) {
        f0str.push_back(static_cast<char>(0x41 + ((i + (j * j)) % 26)));
      }
      std::get<1>(fragment0.atts_).push_back(f0str);
    }
  }

  auto& f1atts = std::get<0>(fragment1.atts_);
  f1atts.resize(fragment1.dimension().num_cells());
  std::iota(
      f1atts.begin(), f1atts.end(), int(fragment0.dimension().num_cells()));
  for (uint64_t i = 0; i < fragment1.dimension().num_cells(); i++) {
    if ((i * i) % 11 == 0) {
      std::get<1>(fragment1.atts_).push_back(std::nullopt);
    } else {
      std::vector<char> f1str;
      for (uint64_t j = 0; j < (i * i) % 11; j++) {
        f1str.push_back(static_cast<char>(0x61 + ((i + (j * j)) % 26)));
      }
      std::get<1>(fragment1.atts_).push_back(f1str);
    }
  }

  InstanceType instance;
  instance.fragments.push_back(fragment0);
  instance.fragments.push_back(fragment1);
  instance.num_user_cells = num_user_cells;

  instance.array.dimension_.extent = extent;
  instance.array.allow_dups_ = true;

  instance.memory.total_budget_ = "30000";
  instance.memory.ratio_array_data_ = "0.5";

  instance.subarray = subarray;
  if (condition) {
    instance.condition.emplace(std::move(condition));
  }

  run<Asserter>(instance);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: fragment skew",
    "[sparse-global-order][rest]") {
#ifdef _WIN32
  SKIP(
      "CORE-328: This test fails on nightly [windows-latest - Sanitizer: OFF | "
      "Assertions: ON | Debug] To re-enable when fixed.");
#endif
  SECTION("Example") {
    instance_fragment_skew<tiledb::test::AsserterCatch>(200, 8, 2);
  }

  SECTION("Condition") {
    int value = 110;
    tdb_unique_ptr<tiledb::sm::ASTNode> qc(new tiledb::sm::ASTNodeVal(
        "a1", &value, sizeof(int), tiledb::sm::QueryConditionOp::GE));
    instance_fragment_skew<tiledb::test::AsserterCatch>(
        200, 8, 2, {}, std::move(qc));
  }

  SECTION("Shrink", "Some examples found by rapidcheck") {
    instance_fragment_skew<tiledb::test::AsserterCatch>(
        2, 1, 1, {templates::Domain<int>(1, 1)});
    instance_fragment_skew<tiledb::test::AsserterCatch>(
        2, 1, 1, {templates::Domain<int>(1, 2)});
    instance_fragment_skew<tiledb::test::AsserterCatch>(
        39, 1, 1, {templates::Domain<int>(20, 21)});
  }
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: fragment skew rapidcheck",
    "[sparse-global-order][rest][rapidcheck][!mayfail]") {
  rc::prop("rapidcheck fragment skew", [this]() {
    const size_t fragment_size = *rc::gen::inRange(2, 200);
    const size_t num_user_cells = *rc::gen::inRange(1, 1024);
    const int extent = *rc::gen::inRange(1, 200);
    const auto subarray = *rc::make_subarray_1d(templates::Domain<int>(1, 200));
    auto condition =
        *rc::make_query_condition<FxRun1D<>::FragmentType>(std::make_tuple(
            templates::Domain<int>(1, 200), templates::Domain<int>(0, 400)));
    instance_fragment_skew<tiledb::test::AsserterRapidcheck>(
        fragment_size, num_user_cells, extent, subarray, std::move(condition));
  });
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
template <typename Asserter>
void CSparseGlobalOrderFx::instance_fragment_interleave(
    size_t fragment_size,
    size_t num_user_cells,
    const std::vector<templates::Domain<int>>& subarray,
    tdb_unique_ptr<tiledb::sm::ASTNode> condition) {
  PAssertFailureCallbackRegistration showInput(
      PAssertFailureCallbackShowRapidcheckInput(
          fragment_size, num_user_cells, subarray, *condition.get()));

  // NB: the tile extent is 2

  templates::Fragment1D<int, int> fragment0;
  templates::Fragment1D<int, int> fragment1;

  // Write a fragment F0 with tiles [1,3][3,5][5,7][7,9]...
  fragment0.dimension().resize(fragment_size);
  fragment0.dimension()[0] = 1;
  for (size_t i = 1; i < fragment0.dimension().num_cells(); i++) {
    fragment0.dimension()[i] = static_cast<int>(1 + 2 * ((i + 1) / 2));
  }

  // Write a fragment F1 with tiles [2,4][4,6][6,8][8,10]...
  fragment1.dimension().resize(fragment0.dimension().num_cells());
  for (size_t i = 0; i < fragment1.dimension().num_cells(); i++) {
    fragment1.dimension()[i] = fragment0.dimension()[i] + 1;
  }

  // atts don't really matter
  auto& f0atts = std::get<0>(fragment0.atts_);
  f0atts.resize(fragment0.dimension().num_cells());
  std::iota(f0atts.begin(), f0atts.end(), 0);

  auto& f1atts = std::get<0>(fragment1.atts_);
  f1atts.resize(fragment1.dimension().num_cells());
  std::iota(f1atts.begin(), f1atts.end(), int(f0atts.num_cells()));

  FxRun1D instance;
  instance.fragments.push_back(fragment0);
  instance.fragments.push_back(fragment1);
  instance.num_user_cells = num_user_cells;
  instance.subarray = subarray;
  if (condition) {
    instance.condition.emplace(std::move(condition));
  }
  instance.memory.total_budget_ = "20000";
  instance.memory.ratio_array_data_ = "0.5";
  instance.array.allow_dups_ = true;

  run<Asserter>(instance);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: fragment interleave",
    "[sparse-global-order][rest]") {
  SECTION("Example") {
    instance_fragment_interleave<tiledb::test::AsserterCatch>(196, 8);
  }
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: fragment interleave rapidcheck",
    "[sparse-global-order][rest][rapidcheck][!mayfail]") {
  rc::prop("rapidcheck fragment interleave", [this]() {
    const size_t fragment_size = *rc::gen::inRange(2, 196);
    const size_t num_user_cells = *rc::gen::inRange(1, 1024);
    const auto subarray = *rc::make_subarray_1d(templates::Domain<int>(1, 200));
    auto condition =
        *rc::make_query_condition<FxRun1D<>::FragmentType>(std::make_tuple(
            templates::Domain<int>(1, 200), templates::Domain<int>(1, 200)));
    instance_fragment_interleave<tiledb::test::AsserterRapidcheck>(
        fragment_size, num_user_cells, subarray, std::move(condition));
  });
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
template <typename Asserter>
void CSparseGlobalOrderFx::instance_fragment_wide_overlap(
    size_t num_fragments,
    size_t fragment_size,
    size_t num_user_cells,
    bool allow_dups,
    const std::vector<templates::Domain<int>>& subarray,
    tdb_unique_ptr<tiledb::sm::ASTNode> condition) {
  PAssertFailureCallbackRegistration showInput(
      PAssertFailureCallbackShowRapidcheckInput(
          num_fragments,
          fragment_size,
          num_user_cells,
          allow_dups,
          subarray,
          *condition.get()));

  const uint64_t total_budget = 100000;
  const double ratio_coords = 0.2;

  FxRun1D instance;
  instance.array.capacity_ = num_fragments * 2;
  instance.array.dimension_.extent = int(num_fragments) * 2;
  instance.array.allow_dups_ = allow_dups;
  instance.subarray = subarray;
  if (condition) {
    instance.condition.emplace(std::move(condition));
  }

  instance.memory.total_budget_ = std::to_string(total_budget);
  instance.memory.ratio_coords_ = std::to_string(ratio_coords);
  instance.memory.ratio_array_data_ = "0.6";

  for (size_t f = 0; f < num_fragments; f++) {
    templates::Fragment1D<int, int> fragment;
    fragment.dimension().resize(fragment_size);
    std::iota(
        fragment.dimension().begin(),
        fragment.dimension().end(),
        instance.array.dimension_.domain.lower_bound + static_cast<int>(f));

    auto& atts = std::get<0>(fragment.atts_);
    atts.resize(fragment_size);
    std::iota(
        atts.begin(),
        atts.end(),
        static_cast<int>(fragment_size * num_fragments));

    instance.fragments.push_back(fragment);
  }

  instance.num_user_cells = num_user_cells;

  run<Asserter>(instance);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: fragment wide overlap",
    "[sparse-global-order][rest]") {
  SECTION("Example") {
    instance_fragment_wide_overlap<tiledb::test::AsserterCatch>(
        16, 100, 64, true);
  }

  SECTION("Condition") {
    const int value = 3;
    tdb_unique_ptr<tiledb::sm::ASTNode> qc(new tiledb::sm::ASTNodeVal(
        "d1", &value, sizeof(int), tiledb::sm::QueryConditionOp::LE));
    instance_fragment_wide_overlap<tiledb::test::AsserterCatch>(
        10,
        21,
        1024,
        false,
        Subarray1DType<int>{templates::Domain<int>(3, 22)},
        std::move(qc));
  }

  SECTION("Shrink", "Some examples found by rapidcheck") {
    instance_fragment_wide_overlap<tiledb::test::AsserterCatch>(
        10, 2, 64, false, Subarray1DType<int>{templates::Domain<int>(1, 3)});
    instance_fragment_wide_overlap<tiledb::test::AsserterCatch>(
        12,
        15,
        1024,
        false,
        Subarray1DType<int>{templates::Domain<int>(1, 12)});
  }
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: fragment wide overlap rapidcheck",
    "[sparse-global-order][rest][rapidcheck][!mayfail]") {
  rc::prop("rapidcheck fragment wide overlap", [this]() {
    const size_t num_fragments = *rc::gen::inRange(10, 24);
    const size_t fragment_size = *rc::gen::inRange(2, 200 - 64);
    const size_t num_user_cells = 1024;
    const bool allow_dups = *rc::gen::arbitrary<bool>();
    const auto subarray = *rc::make_subarray_1d(templates::Domain<int>(1, 200));
    auto condition =
        *rc::make_query_condition<FxRun1D<>::FragmentType>(std::make_tuple(
            templates::Domain<int>(1, 200),
            templates::Domain<int>(
                0, static_cast<int>(num_fragments * fragment_size))));
    instance_fragment_wide_overlap<tiledb::test::AsserterRapidcheck>(
        num_fragments,
        fragment_size,
        num_user_cells,
        allow_dups,
        subarray,
        std::move(condition));
  });
}

/**
 * Tests that the reader will not yield duplicate coordinates if
 * coordinates in loaded tiles are equal to the merge bound,
 * and the merge bound is an actual datum in yet-to-be-loaded tiles.
 *
 * Example:
 * many fragments which overlap at the ends
 * [0, 1000], [1000, 2000], [2000, 3000]
 * If we can load the tiles of the first two fragments but not
 * the third, then the merge bound will be 2000. But it is only
 * correct to emit the 2000 from the second fragment if
 * duplicates are allowed.
 *
 * This test illustrates that the merge bound must not be an
 * inclusive bound (unless duplicates are allowed).
 */
template <typename Asserter>
void CSparseGlobalOrderFx::instance_merge_bound_duplication(
    size_t num_fragments,
    size_t fragment_size,
    size_t num_user_cells,
    size_t tile_capacity,
    bool allow_dups,
    const Subarray1DType<int>& subarray,
    tdb_unique_ptr<tiledb::sm::ASTNode> condition) {
  PAssertFailureCallbackRegistration showInput(
      PAssertFailureCallbackShowRapidcheckInput(
          num_fragments,
          fragment_size,
          num_user_cells,
          tile_capacity,
          allow_dups,
          subarray,
          *condition.get()));

  FxRun1D instance;
  instance.num_user_cells = num_user_cells;
  instance.subarray = subarray;
  instance.memory.total_budget_ = "50000";
  instance.memory.ratio_array_data_ = "0.5";
  instance.array.capacity_ = tile_capacity;
  instance.array.allow_dups_ = allow_dups;
  instance.array.dimension_.domain.lower_bound = 0;
  instance.array.dimension_.domain.upper_bound =
      static_cast<int>(num_fragments * fragment_size);
  if (condition) {
    instance.condition.emplace(std::move(condition));
  }

  for (size_t f = 0; f < num_fragments; f++) {
    templates::Fragment1D<int, int> fragment;
    fragment.dimension().resize(fragment_size);
    std::iota(
        fragment.dimension().begin(),
        fragment.dimension().end(),
        static_cast<int>(f * (fragment_size - 1)));

    auto& atts = std::get<0>(fragment.atts_);
    atts.resize(fragment_size);
    std::iota(atts.begin(), atts.end(), static_cast<int>(f * fragment_size));

    instance.fragments.push_back(fragment);
  }

  instance.num_user_cells = num_user_cells;

  run<Asserter>(instance);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: merge bound duplication",
    "[sparse-global-order][rest]") {
#ifdef _WIN32
  SKIP(
      "CORE-328: This test fails on nightly [windows-latest - Sanitizer: OFF | "
      "Assertions: ON | Debug] To re-enable when fixed.");
#endif
  SECTION("Example") {
    instance_merge_bound_duplication<tiledb::test::AsserterCatch>(
        16, 16, 1024, 16, false);
  }

  SECTION("Condition") {
    int value = 1;
    tdb_unique_ptr<tiledb::sm::ASTNode> qc(new tiledb::sm::ASTNodeVal(
        "a1", &value, sizeof(int), tiledb::sm::QueryConditionOp::EQ));
    instance_merge_bound_duplication<tiledb::test::AsserterCatch>(
        4,
        12,
        1024,
        1024,
        false,
        std::vector<templates::Domain<int>>{templates::Domain<int>(0, 11)},
        std::move(qc));
  }

  SECTION("Shrink", "Some examples found by rapidcheck") {
    int value = 1329;
    tdb_unique_ptr<tiledb::sm::ASTNode> qc(new tiledb::sm::ASTNodeVal(
        "a1", &value, sizeof(int), tiledb::sm::QueryConditionOp::EQ));
    instance_merge_bound_duplication<tiledb::test::AsserterCatch>(
        21,
        133,
        1024,
        1024,
        false,
        std::vector<templates::Domain<int>>{templates::Domain<int>(397, 1320)},
        std::move(qc));

    value = 1;
    qc.reset(new tiledb::sm::ASTNodeVal(
        "d1", &value, sizeof(value), tiledb::sm::QueryConditionOp::LT));

    instance_merge_bound_duplication<tiledb::test::AsserterCatch>(
        17,
        160,
        1024,
        1,
        false,
        Subarray1DType<int>{templates::Domain<int>(0, 1908)},
        std::move(qc));
  }
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: merge bound duplication rapidcheck",
    "[sparse-global-order][rest][rapidcheck][!mayfail]") {
  rc::prop("rapidcheck merge bound duplication", [this]() {
    const size_t num_fragments = *rc::gen::inRange(4, 32);
    const size_t fragment_size = *rc::gen::inRange(2, 256);
    const size_t num_user_cells = 1024;
    const size_t tile_capacity = *rc::gen::inRange<size_t>(1, fragment_size);
    const bool allow_dups = *rc::gen::arbitrary<bool>();
    const auto subarray = *rc::make_subarray_1d(templates::Domain<int>(
        0, static_cast<int>(num_fragments * fragment_size)));
    auto condition =
        *rc::make_query_condition<FxRun1D<>::FragmentType>(std::make_tuple(
            templates::Domain<int>(1, 200),
            templates::Domain<int>(
                0, static_cast<int>(num_fragments * fragment_size))));
    instance_merge_bound_duplication<tiledb::test::AsserterRapidcheck>(
        num_fragments,
        fragment_size,
        num_user_cells,
        tile_capacity,
        allow_dups,
        subarray,
        std::move(condition));
  });
}

/**
 * Tests that the reader will not yield out of order when
 * the tile MBRs in a fragment are not in the global order.
 *
 * The coordinates in the fragment are in global order, but
 * the tile MBRs may not be.  This is because the MBR uses
 * the minimum coordinate value *in each dimension*, which
 * for two dimensions and higher is not always the same as
 * the minimum coordinate in the tile.
 *
 * In this test, we have tile extents of 4 in both dimensions.
 * Let's say the attribute value is the index of the cell
 * as enumerated in row-major order and we have a densely
 * populated array.
 *
 *             c-T1              c-T2              c-T3
 *      |                 |                 |                 |
 *   ---+----+---+---+----+----+---+---+----+----+---+---+----+---
 *      |   1   2   3   4 |   5   6   7   8 |  9   10  11  12 |
 * r-T1 | 201 202 203 204 | 205 206 207 208 | 209 210 211 212 | ...
 *      | 401 402 403 404 | 405 406 407 408 | 409 410 411 412 |
 *      | 601 602 603 604 | 605 606 607 608 | 609 610 611 612 |
 *   ---+----+---+---+----+----+---+---+----+----+---+---+----+---
 *
 * If the capacity is 17, then:
 * Data tile 1 holds
 * [1, 2, 3, 4, 201, 202, 203, 204, 401, 402, 403, 404, 601, 602, 603, 604,
 * 5]. Data tile 2 holds [6, 7, 8, 205, 206, 207, 208, 405, 406, 407, 408,
 * 605, 606, 607, 608, 9, 10].
 *
 * Data tile 1 has a MBR of [(1, 1), (5, 4)].
 * Data tile 2 has a MBR of [(5, 1), (10, 4)].
 *
 * The lower bound of data tile 2's MBR is less than the upper bound
 * of data tile 1's MBR. Hence the coordinates of the tiles are in global
 * order but the MBRs are not.
 *
 * In a non-dense setting, this happens if the data tile boundary
 * happens in the middle of a space tile which has coordinates on both
 * sides, AND the space tile after the boundary contains a coordinate
 * which is lesser in the second dimension.
 */
template <typename Asserter>
void CSparseGlobalOrderFx::instance_out_of_order_mbrs(
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order,
    size_t num_fragments,
    size_t fragment_size,
    size_t num_user_cells,
    size_t tile_capacity,
    bool allow_dups,
    const Subarray2DType<int, int>& subarray,
    tdb_unique_ptr<tiledb::sm::ASTNode> condition) {
  PAssertFailureCallbackRegistration showInput(
      PAssertFailureCallbackShowRapidcheckInput(
          tile_order,
          cell_order,
          num_fragments,
          fragment_size,
          num_user_cells,
          tile_capacity,
          allow_dups,
          subarray,
          *condition.get()));

  FxRun2D instance;
  instance.num_user_cells = num_user_cells;
  instance.capacity = tile_capacity;
  instance.allow_dups = allow_dups;
  instance.tile_order_ = tile_order;
  instance.cell_order_ = cell_order;
  instance.subarray = subarray;
  if (condition) {
    instance.condition.emplace(std::move(condition));
  }

  auto row = [&](size_t f, size_t i) -> int {
    return 1 + static_cast<int>(
                   ((num_fragments * i) + f) / instance.d1.domain.upper_bound);
  };
  auto col = [&](size_t f, size_t i) -> int {
    return 1 + static_cast<int>(
                   ((num_fragments * i) + f) % instance.d1.domain.upper_bound);
  };

  for (size_t f = 0; f < num_fragments; f++) {
    templates::Fragment2D<int, int, int> fdata;
    fdata.d1().reserve(fragment_size);
    fdata.d2().reserve(fragment_size);
    std::get<0>(fdata.atts_).reserve(fragment_size);

    for (size_t i = 0; i < fragment_size; i++) {
      fdata.d1().push_back(row(f, i));
      fdata.d2().push_back(col(f, i));
      std::get<0>(fdata.atts_)
          .push_back(static_cast<int>(f * fragment_size + i));
    }

    instance.fragments.push_back(fdata);
  }

  auto guard = run_create<Asserter, FxRun2D>(instance);

  // validate that we have set up the condition we claim,
  // i.e. some fragment has out-of-order MBRs
  {
    CApiArray array(context(), array_name_.c_str(), TILEDB_READ);

    const auto& fragment_metadata = array->array()->fragment_metadata();
    for (const auto& fragment : fragment_metadata) {
      const_cast<sm::FragmentMetadata*>(fragment.get())
          ->loaded_metadata()
          ->load_rtree(array->array()->get_encryption_key());
    }

    sm::GlobalCellCmp globalcmp(array->array()->array_schema_latest().domain());

    // check that we actually have out-of-order MBRs
    // (disable on REST where we have no metadata)
    // (disable with rapidcheck where this may not be guaranteed)
    if (!vfs_test_setup_.is_rest() &&
        std::is_same<Asserter, tiledb::test::AsserterCatch>::value) {
      bool any_out_of_order = false;
      for (size_t f = 0; !any_out_of_order && f < fragment_metadata.size();
           f++) {
        for (size_t t = 1;
             !any_out_of_order && t < fragment_metadata[f]->tile_num();
             t++) {
          const sm::RangeUpperBound lt = {
              .mbr = fragment_metadata[f]->mbr(t - 1)};
          const sm::RangeLowerBound rt = {.mbr = fragment_metadata[f]->mbr(t)};
          if (globalcmp(rt, lt)) {
            any_out_of_order = true;
          }
        }
      }
      ASSERTER(any_out_of_order);
    }
  }

  run_execute<Asserter, FxRun2D>(instance);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: out-of-order MBRs",
    "[sparse-global-order][rest]") {
  SECTION("Example") {
    instance_out_of_order_mbrs<tiledb::test::AsserterCatch>(
        TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR, 4, 100, 32, 6, false);
  }

  SECTION("Shrinking") {
    Subarray2DType<int, int> subarray = {std::make_pair(
        std::optional<templates::Domain<int>>{templates::Domain<int>(44, 49)},
        std::optional<templates::Domain<int>>{templates::Domain<int>(9, 24)})};
    int value = 58;
    tdb_unique_ptr<tiledb::sm::ASTNode> qc(new tiledb::sm::ASTNodeVal(
        "d1", &value, sizeof(int), tiledb::sm::QueryConditionOp::LT));
    instance_out_of_order_mbrs<tiledb::test::AsserterCatch>(
        TILEDB_COL_MAJOR,
        TILEDB_ROW_MAJOR,
        3,
        72,
        120,
        5,
        true,
        subarray,
        std::move(qc));
  }
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: out-of-order MBRs rapidcheck",
    "[sparse-global-order][rest][rapidcheck][!mayfail]") {
  rc::prop("rapidcheck out-of-order MBRs", [this](bool allow_dups) {
    const auto tile_order =
        *rc::gen::element(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    const auto cell_order =
        *rc::gen::element(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    const size_t num_fragments = *rc::gen::inRange(2, 8);
    const size_t fragment_size = *rc::gen::inRange(32, 400);
    const size_t num_user_cells = *rc::gen::inRange(1, 1024);
    const size_t tile_capacity = *rc::gen::inRange(4, 32);
    const auto subarray = *rc::make_subarray_2d(
        templates::Domain<int>(1, 200), templates::Domain<int>(1, 200));
    auto condition =
        *rc::make_query_condition<FxRun2D::FragmentType>(std::make_tuple(
            templates::Domain<int>(1, 200),
            templates::Domain<int>(1, 200),
            templates::Domain<int>(
                0, static_cast<int>((num_fragments + 1) * fragment_size))));

    instance_out_of_order_mbrs<tiledb::test::AsserterRapidcheck>(
        tile_order,
        cell_order,
        num_fragments,
        fragment_size,
        num_user_cells,
        tile_capacity,
        allow_dups,
        subarray,
        std::move(condition));
  });
}

/**
 * Similar to the "fragment skew" test, but in two dimensions.
 * In 2+ dimensions the lower bounds of the tile MBRs are not
 * necessarily in order for each fragment.
 *
 * This test verifies the need to set the merge bound to
 * the MBR lower bound of fragments which haven't had any tiles loaded.
 *
 * Let's use an example with dimension extents and tile capacities of 4.
 *
 *             c-T1              c-T2              c-T3
 *      |                 |                 |                 |
 *   ---+----+---+---+----+----+---+---+----+----+---+---+----+---
 *      | 1    1   1   1  | 1               | 1               |
 * r-T1 | 1    1   1   1  |                 |                 |
 *      | 1    1   1   1  |                 |                 |
 *      | 1    1          |      2   1   1  |                 |
 *   ---+----+---+---+----+----+---+---+----+----+---+---+----+---
 *
 * Fragment 1 MBR lower bounds: [(1, 1), (2, 1), (3, 1), (1, 5), (1, 7)]
 * Fragment 2 MBR lower bounds: [(4, 6)]
 *
 * What if the memory budget here permits loading just four tiles?
 *
 * The un-loaded tiles have bounds (1, 7) and (4, 6), but the
 * coordinate from (4, 6) has a lesser value. The merge bound
 * is necessary to ensure we don't emit out-of-order coordinates.
 */
constexpr size_t fragment_skew_2d_merge_bound_d1_extent = 4;
constexpr size_t fragment_skew_2d_merge_bound_d2_extent = 4;

template <typename Asserter>
void CSparseGlobalOrderFx::instance_fragment_skew_2d_merge_bound(
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order,
    size_t approximate_memory_tiles,
    size_t num_user_cells,
    size_t num_fragments,
    size_t tile_capacity,
    bool allow_dups,
    const Subarray2DType<int, int>& subarray,
    tdb_unique_ptr<tiledb::sm::ASTNode> condition) {
  PAssertFailureCallbackRegistration showInput(
      PAssertFailureCallbackShowRapidcheckInput(
          tile_order,
          cell_order,
          approximate_memory_tiles,
          num_user_cells,
          num_fragments,
          tile_capacity,
          allow_dups,
          subarray,
          *condition.get()));

  FxRun2D instance;
  instance.tile_order_ = tile_order;
  instance.cell_order_ = cell_order;
  instance.d1.extent = fragment_skew_2d_merge_bound_d1_extent;
  instance.d2.extent = fragment_skew_2d_merge_bound_d2_extent;
  instance.capacity = tile_capacity;
  instance.allow_dups = allow_dups;
  instance.num_user_cells = num_user_cells;
  instance.subarray = subarray;
  if (condition) {
    instance.condition.emplace(std::move(condition));
  }

  const size_t tile_size_estimate = (1600 + instance.capacity * sizeof(int));
  const double coords_ratio =
      (1.02 / (std::stold(instance.memory.total_budget_) / tile_size_estimate /
               approximate_memory_tiles));
  instance.memory.ratio_coords_ = std::to_string(coords_ratio);

  int att = 0;

  // for each fragment, make one fragment which is just a point
  // so that its MBR is equal to its coordinate (and thus more likely
  // to be out of order with respect to the MBRs of tiles from other
  // fragments)
  for (size_t f = 0; f < num_fragments; f++) {
    FxRun2D::FragmentType fdata;
    // make one mostly dense space tile
    const int trow = instance.d1.domain.lower_bound +
                     static_cast<int>(f * instance.d1.extent);
    const int tcol = instance.d2.domain.lower_bound +
                     static_cast<int>(f * instance.d2.extent);
    for (int i = 0; i < instance.d1.extent * instance.d2.extent - 2; i++) {
      fdata.d1().push_back(trow + i / instance.d1.extent);
      fdata.d2().push_back(tcol + i % instance.d1.extent);
      std::get<0>(fdata.atts_).push_back(att++);
    }

    // then some sparse coords in the next space tile,
    // fill the data tile (if the capacity is 4), we'll call it T
    fdata.d1().push_back(trow);
    fdata.d2().push_back(tcol + instance.d2.extent);
    std::get<0>(fdata.atts_).push_back(att++);
    fdata.d1().push_back(trow + instance.d1.extent - 1);
    fdata.d2().push_back(tcol + instance.d2.extent + 2);
    std::get<0>(fdata.atts_).push_back(att++);

    // then begin a new data tile "Tnext" which straddles the bounds of that
    // space tile. this will have a low MBR.
    fdata.d1().push_back(trow + instance.d1.extent - 1);
    fdata.d2().push_back(tcol + instance.d2.extent + 3);
    std::get<0>(fdata.atts_).push_back(att++);
    fdata.d1().push_back(trow);
    fdata.d2().push_back(tcol + 2 * instance.d2.extent);
    std::get<0>(fdata.atts_).push_back(att++);

    // then add a point P which is less than the lower bound of Tnext's MBR,
    // and also between the last two coordinates of T
    FxRun2D::FragmentType fpoint;
    fpoint.d1().push_back(trow + instance.d1.extent - 1);
    fpoint.d2().push_back(tcol + instance.d1.extent + 1);
    std::get<0>(fpoint.atts_).push_back(att++);

    instance.fragments.push_back(fdata);
    instance.fragments.push_back(fpoint);
  }

  run<Asserter>(instance);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: fragment skew 2d merge bound",
    "[sparse-global-order][rest]") {
  SECTION("Example") {
    instance_fragment_skew_2d_merge_bound<tiledb::test::AsserterCatch>(
        TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR, 4, 1024, 1, 4, false);
  }

  SECTION("Shrink", "Some examples found by rapidcheck") {
    instance_fragment_skew_2d_merge_bound<tiledb::test::AsserterCatch>(
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        2,
        9,
        1,
        2,
        false,
        {std::make_pair(templates::Domain<int>(2, 2), std::nullopt)});
  }
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: fragment skew 2d merge bound rapidcheck",
    "[sparse-global-order][rest][rapidcheck][!mayfail]") {
  rc::prop("rapidcheck fragment skew 2d merge bound", [this]() {
    const auto tile_order =
        *rc::gen::element(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    const auto cell_order =
        *rc::gen::element(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    const size_t approximate_memory_tiles = *rc::gen::inRange(2, 64);
    const size_t num_user_cells = *rc::gen::inRange(1, 1024);
    const size_t num_fragments = *rc::gen::inRange(1, 8);
    const size_t tile_capacity = *rc::gen::inRange(1, 16);
    const bool allow_dups = *rc::gen::arbitrary<bool>();
    const auto subarray = *rc::make_subarray_2d(
        templates::Domain<int>(1, 200), templates::Domain<int>(1, 200));
    auto condition =
        *rc::make_query_condition<FxRun2D::FragmentType>(std::make_tuple(
            templates::Domain<int>(1, 200),
            templates::Domain<int>(1, 200),
            templates::Domain<int>(
                0,
                static_cast<int>(
                    2 * num_fragments * fragment_skew_2d_merge_bound_d1_extent *
                    fragment_skew_2d_merge_bound_d2_extent))));
    instance_fragment_skew_2d_merge_bound<tiledb::test::AsserterRapidcheck>(
        tile_order,
        cell_order,
        approximate_memory_tiles,
        num_user_cells,
        num_fragments,
        tile_capacity,
        allow_dups,
        subarray,
        std::move(condition));
  });
}

/**
 * Test usage patterns where each fragment is kind of an entire data set,
 * e.g. one fragment for observations from Monday, one for observations
 * from Tuesday, one for observations from Wednesday, etc.
 *
 * This sort of usage is not really what fragments and time travel were
 * originally built to support, but it is more flexible with respect
 * to schema evolution than having time as another dimension would be,
 * and as such at least one prominent customer is known to do this.
 *
 * What this looks like is each fragment covering essentially the entire
 * domain of the array.  Queries which open the array at a specific
 * timestamp are nice and easy; queries which do not open with a specific
 * timestamp are sad because they do tons of work to de-duplicate.
 */
using FragmentFullCopy1DFxRunType = FxRun1D<
    Datatype::INT64,
    static_attribute<Datatype::INT64, 1, true>,
    static_attribute<
        Datatype::STRING_ASCII,
        tiledb::sm::cell_val_num_var,
        false>>;
template <typename Asserter>
void CSparseGlobalOrderFx::instance_fragment_full_copy_1d(
    size_t num_fragments,
    templates::Dimension<Datatype::INT64> dimension,
    const Subarray1DType<int64_t>& subarray,
    tdb_unique_ptr<tiledb::sm::ASTNode> condition) {
  using FxRunType = FragmentFullCopy1DFxRunType;

  PAssertFailureCallbackRegistration showInput(
      PAssertFailureCallbackShowRapidcheckInput(
          num_fragments, dimension, subarray, *condition.get()));

  const size_t fragment_size =
      std::min<size_t>(1024 * 32, dimension.domain.num_cells());

  FxRunType instance;
  instance.num_user_cells = 1024 * 1024;
  instance.array.dimension_ = dimension;
  instance.memory.total_budget_ = std::to_string(1024 * 1024 * 4);
  instance.array.capacity_ = std::min<uint64_t>(
      1024, std::max<uint64_t>(16, dimension.domain.num_cells() / 1024));
  instance.array.allow_dups_ = false;
  instance.subarray = subarray;
  if (condition) {
    instance.condition.emplace(std::move(condition));
  }

  for (size_t f = 0; f < num_fragments; f++) {
    FxRunType::FragmentType fragment;

    fragment.dimension().resize(fragment_size);
    std::iota(
        fragment.dimension().begin(),
        fragment.dimension().end(),
        dimension.domain.lower_bound);

    std::get<0>(fragment.atts_).resize(fragment.dimension().num_cells());
    std::iota(
        std::get<0>(fragment.atts_).begin(),
        std::get<0>(fragment.atts_).end(),
        0);

    std::get<1>(fragment.atts_).resize(0);
    for (uint64_t i = 0; i < fragment_size; i++) {
      std::vector<char> values;
      for (uint64_t j = 0; j < (i * i) % 11; j++) {
        values.push_back(static_cast<char>((0x41 + i + (j * j)) % 26));
      }
      std::get<1>(fragment.atts_).push_back(values);
    }

    instance.fragments.push_back(fragment);
  }

  run<Asserter>(instance);
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: fragment full copy 1d",
    "[sparse-global-order][rest]") {
  SECTION("Example") {
    templates::Domain<int64_t> domain(0, 262143);
    instance_fragment_full_copy_1d<AsserterCatch>(
        8, templates::Dimension<Datatype::INT64>(domain, 1024));
  }

  SECTION("Condition") {
    int64_t value = 524288;
    tdb_unique_ptr<tiledb::sm::ASTNode> qc(new tiledb::sm::ASTNodeVal(
        "a1", &value, sizeof(value), tiledb::sm::QueryConditionOp::GE));
    templates::Domain<int64_t> domain(0, 262143);
    instance_fragment_full_copy_1d<AsserterCatch>(
        8,
        templates::Dimension<Datatype::INT64>(domain, 1024),
        {},
        std::move(qc));
  }
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: fragment full copy 1d rapidcheck",
    "[sparse-global-order][rest][rapidcheck][!mayfail]") {
  using FxRunType = FragmentFullCopy1DFxRunType;

  rc::prop("rapidcheck fragment full copy 1d", [this]() {
    const size_t num_fragments = *rc::gen::inRange(2, 16);
    const auto dimension =
        *rc::gen::arbitrary<templates::Dimension<FxRunType::DimensionType>>();
    const auto subarray = *rc::make_subarray_1d(dimension.domain);

    const auto domains = std::make_tuple(
        dimension.domain,
        templates::Domain<int64_t>(0, dimension.domain.upper_bound),
        templates::Domain<char>('A', 'Z'));
    auto condition =
        *rc::make_query_condition<FxRunType::FragmentType>(domains);

    instance_fragment_full_copy_1d<AsserterRapidcheck>(
        num_fragments, dimension, subarray, std::move(condition));
  });
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
  rc = tiledb_ctx_get_last_error(context(), &error);
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
  tiledb_query_get_status(context(), query, &status);
  CHECK(status == TILEDB_COMPLETED);

  CHECK(40 == data_r_size);
  CHECK(40 == coords_r_size);

  int coords_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  int data_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  CHECK(!std::memcmp(coords_c, coords_r, coords_r_size));
  CHECK(!std::memcmp(data_c, data_r, data_r_size));

  // Clean up.
  rc = tiledb_array_close(context(), array);
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
  rc = tiledb_ctx_get_last_error(context(), &error);
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

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: user buffer cannot fit single cell",
    "[sparse-global-order][user-buffer][too-small][rest]") {
  std::string array_name =
      vfs_test_setup_.array_uri("test_sparse_global_order");
  auto ctx = vfs_test_setup_.ctx();

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

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: attribute copy memory limit",
    "[sparse-global-order][attribute-copy][memory-limit][rest]") {
  Config config;
  config["sm.mem.total_budget"] = "20000";
  std::string array_name =
      vfs_test_setup_.array_uri("test_sparse_global_order");
  auto ctx = vfs_test_setup_.ctx();

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
  tiledb_query_get_status(context(), query, &status);
  CHECK(status == TILEDB_INCOMPLETE);

  uint64_t loop_idx = 1;
  CHECK(4 == data_r_size);
  CHECK(4 == coords_r_size);
  CHECK(!std::memcmp(&loop_idx, coords_r, coords_r_size));
  CHECK(!std::memcmp(&loop_idx, data_r, data_r_size));
  loop_idx++;

  while (status == TILEDB_INCOMPLETE) {
    rc = tiledb_query_submit(context(), query);
    CHECK(std::optional<std::string>() == error_if_any(rc));
    tiledb_query_get_status(context(), query, &status);
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
      tiledb_query_get_status(context(), query, &status);
      CHECK(status == TILEDB_INCOMPLETE);

      rc = tiledb_query_submit(context(), query);
      CHECK(rc == TILEDB_OK);

      CHECK(coords_r[0] == i / 2);
      CHECK((data_r[0] == i / 2 + 10 || data_r[0] == i / 2));
    }
  } else {
    CHECK(data_r[0] == 11);

    for (int i = 2; i <= 10; i++) {
      // Check incomplete query status.
      tiledb_query_get_status(context(), query, &status);
      CHECK(status == TILEDB_INCOMPLETE);

      rc = tiledb_query_submit(context(), query);
      CHECK(rc == TILEDB_OK);

      CHECK(coords_r[0] == i);
      CHECK(data_r[0] == i + 10);
    }
  }

  // Check completed query status.
  tiledb_query_get_status(context(), query, &status);
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
  // fragment before cell at coord 5 for sure because of the cell at
  // coordinate 4 but might not regress progress for the second fragment
  // before cell at coord 5 as cell at coordinate 5 was deleted.
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
  tiledb_query_get_status(context(), query, &status);
  CHECK(status == TILEDB_INCOMPLETE);

  // Reset buffer sizes.
  coords_r_size = sizeof(coords_r);
  data_r_size = sizeof(data_r);
  offsets_r_size = sizeof(offsets_r);

  // Submit query.
  rc = tiledb_query_submit(context(), query);
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
  tiledb_query_get_status(context(), query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Clean up.
  rc = tiledb_array_close(context(), array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

/**
 * Test that we get "repeatable reads" when multiple fragments
 * are written at the same timestamp.  That is, for a fixed array
 * A, reading the contents of A should always produce the same results.
 */
template <typename Asserter>
void CSparseGlobalOrderFx::instance_repeatable_read_submillisecond(
    const std::vector<uint64_t>& fragment_same_timestamp_runs) {
  PAssertFailureCallbackRegistration showInput{
      PAssertFailureCallbackShowRapidcheckInput(fragment_same_timestamp_runs)};

  uint64_t num_fragments = 0;
  for (const auto same_timestamp_run : fragment_same_timestamp_runs) {
    num_fragments += same_timestamp_run;
  }

  FxRun2D instance;
  instance.allow_dups = false;

  /*
   * Each fragment (T, F) writes its fragment index into both (1, 1)
   * and (2 + T, 2 + F).
   *
   * (1, 1) is the coordinate where they must be de-duplicated.
   * The other coordinate is useful for ensuring that all fragments
   * are included in the result set, and for debugging by inspecting tile
   * MBRs.
   */
  for (uint64_t t = 0; t < fragment_same_timestamp_runs.size(); t++) {
    for (uint64_t f = 0; f < fragment_same_timestamp_runs[t]; f++) {
      FxRun2D::FragmentType fragment;
      fragment.d1() = {1, 2 + static_cast<int>(t)};
      fragment.d2() = {1, 2 + static_cast<int>(f)};
      std::get<0>(fragment.atts_) = std::vector<int>{
          static_cast<int>(instance.fragments.size()),
          static_cast<int>(instance.fragments.size())};

      instance.fragments.push_back(fragment);
    }
  }

  create_array<Asserter, decltype(instance)>(instance);

  DeleteArrayGuard arrayguard(context(), array_name_.c_str());

  /*
   * Write each fragment at a fixed timestamp.
   * Opening for write at timestamp `t` causes all the fragments to have `t`
   * as their start and end timestamps.
   */
  for (uint64_t i = 0, t = 0; t < fragment_same_timestamp_runs.size(); t++) {
    tiledb_array_t* raw_array;
    TRY(context(),
        tiledb_array_alloc(context(), array_name_.c_str(), &raw_array));
    TRY(context(),
        tiledb_array_set_open_timestamp_start(context(), raw_array, t + 1));
    TRY(context(),
        tiledb_array_set_open_timestamp_end(context(), raw_array, t + 1));

    CApiArray array(context(), raw_array, TILEDB_WRITE);
    for (uint64_t f = 0; f < fragment_same_timestamp_runs[t]; f++, i++) {
      write_fragment<Asserter, decltype(instance.fragments[i])>(
          instance.fragments[i], &array);
    }
  }

  CApiArray array(context(), array_name_.c_str(), TILEDB_READ);

  // Value from (1, 1).
  // Because all the writes are at the same timestamp we make no guarantee
  // of ordering. `attvalue` may be the value from any of the fragments,
  // but it must be the same value each time we read.
  std::optional<int> attvalue;

  for (uint64_t f = 0; f < num_fragments; f++) {
    std::vector<int> dim1(instance.fragments.size() * 4);
    std::vector<int> dim2(instance.fragments.size() * 4);
    std::vector<int> atts(instance.fragments.size() * 4);
    uint64_t dim1_size = sizeof(int) * dim1.size();
    uint64_t dim2_size = sizeof(int) * dim2.size();
    uint64_t atts_size = sizeof(int) * atts.size();

    tiledb_query_t* query;
    TRY(context(), tiledb_query_alloc(context(), array, TILEDB_READ, &query));
    TRY(context(),
        tiledb_query_set_layout(context(), query, TILEDB_GLOBAL_ORDER));
    TRY(context(),
        tiledb_query_set_data_buffer(
            context(), query, "d1", dim1.data(), &dim1_size));
    TRY(context(),
        tiledb_query_set_data_buffer(
            context(), query, "d2", dim2.data(), &dim2_size));
    TRY(context(),
        tiledb_query_set_data_buffer(
            context(), query, "a1", atts.data(), &atts_size));

    tiledb_query_status_t status;
    TRY(context(), tiledb_query_submit(context(), query));
    TRY(context(), tiledb_query_get_status(context(), query, &status));
    tiledb_query_free(&query);
    ASSERTER(status == TILEDB_COMPLETED);

    ASSERTER(dim1_size == (1 + num_fragments) * sizeof(int));
    ASSERTER(dim2_size == (1 + num_fragments) * sizeof(int));
    ASSERTER(atts_size == (1 + num_fragments) * sizeof(int));

    ASSERTER(dim1[0] == 1);
    ASSERTER(dim2[0] == 1);
    if (attvalue.has_value()) {
      ASSERTER(attvalue.value() == atts[0]);
    } else {
      attvalue.emplace(atts[0]);
    }

    uint64_t c = 1;
    for (uint64_t t = 0; t < fragment_same_timestamp_runs.size(); t++) {
      for (uint64_t f = 0; f < fragment_same_timestamp_runs[t]; f++, c++) {
        ASSERTER(dim1[c] == 2 + static_cast<int>(t));
        ASSERTER(dim2[c] == 2 + static_cast<int>(f));
        ASSERTER(atts[c] == static_cast<int>(c - 1));
      }
    }
  }
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: repeatable reads for sub-millisecond "
    "fragments",
    "[sparse-global-order][sub-millisecond][rest]") {
  SECTION("Example") {
    instance_repeatable_read_submillisecond<AsserterCatch>({10});
  }
}

TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: repeatable reads for sub-millisecond "
    "fragments rapidcheck",
    "[sparse-global-order][sub-millisecond][rest][rapidcheck][!mayfail]") {
  rc::prop("rapidcheck consistent read order for sub-millisecond", [this]() {
    const auto runs = *rc::gen::suchThat(
        rc::gen::nonEmpty(
            rc::gen::container<std::vector<uint64_t>>(rc::gen::inRange(1, 8))),
        [](auto value) { return value.size() <= 6; });
    instance_repeatable_read_submillisecond<AsserterRapidcheck>(runs);
  });
}

/**
 * Creates an array with a schema whose dimensions and attributes
 * come from `Instance`.
 */
template <typename Asserter, InstanceType Instance>
void CSparseGlobalOrderFx::create_array(const Instance& instance) {
  templates::ddl::create_array(
      array_name_,
      Context(context(), false),
      instance.dimensions(),
      instance.attributes(),
      instance.tile_order(),
      instance.cell_order(),
      instance.tile_capacity(),
      instance.allow_duplicates());
}

/**
 * Runs a correctness check upon `instance`.
 *
 * Inserts all of the fragments, then submits a global order read
 * query and compares the results of the query against the
 * expected result order computed from the input data.
 */
template <typename Asserter, InstanceType Instance>
void CSparseGlobalOrderFx::run(Instance& instance) {
  reset_config();

  auto tmparray = run_create<Asserter, Instance>(instance);
  run_execute<Asserter, Instance>(instance);
}

template <typename Asserter, InstanceType Instance>
DeleteArrayGuard CSparseGlobalOrderFx::run_create(Instance& instance) {
  ASSERTER(instance.num_user_cells > 0);

  reset_config();

  memory_ = instance.memory;
  update_config();

  // the tile extent is 2
  // create_default_array_1d<Asserter>(instance.array);
  create_array<Asserter, decltype(instance)>(instance);

  DeleteArrayGuard arrayguard(context(), array_name_.c_str());

  // write all fragments
  for (auto& fragment : instance.fragments) {
    write_fragment<Asserter, decltype(fragment)>(fragment);
  }

  return arrayguard;
}

template <typename Asserter, InstanceType Instance>
void CSparseGlobalOrderFx::run_execute(Instance& instance) {
  ASSERTER(instance.num_user_cells > 0);

  std::decay_t<decltype(instance.fragments[0])> expect;

  // for de-duplicating, track the fragment that each coordinate came from
  // we will use this to select the coordinate from the most recent fragment
  std::vector<uint64_t> expect_fragment;

  for (uint64_t f = 0; f < instance.fragments.size(); f++) {
    const auto& fragment = instance.fragments[f];

    auto expect_dimensions = expect.dimensions();
    auto expect_attributes = expect.attributes();

    if (instance.subarray.empty() && !instance.condition.has_value()) {
      stdx::extend(expect_dimensions, fragment.dimensions());
      stdx::extend(expect_attributes, fragment.attributes());
      expect_fragment.insert(expect_fragment.end(), fragment.size(), f);
    } else {
      std::vector<uint64_t> accept;
      std::optional<
          templates::QueryConditionEvalSchema<typename Instance::FragmentType>>
          eval;
      if (instance.condition.has_value()) {
        eval.emplace();
      }
      for (uint64_t i = 0; i < fragment.size(); i++) {
        if (!instance.pass_subarray(fragment, i)) {
          continue;
        }
        accept.push_back(i);
      }

      const auto fdimensions =
          stdx::select(fragment.dimensions(), std::span(accept));
      const auto fattributes =
          stdx::select(fragment.attributes(), std::span(accept));
      stdx::extend(expect_dimensions, stdx::reference_tuple(fdimensions));
      stdx::extend(expect_attributes, stdx::reference_tuple(fattributes));

      expect_fragment.insert(expect_fragment.end(), accept.size(), f);
    }
  }

  // Open array for reading.
  CApiArray array(context(), array_name_.c_str(), TILEDB_READ);

  // finish preparing expected results - arrange in global order, dedup, and
  // apply query condition
  {
    std::vector<uint64_t> idxs(expect.size());
    std::iota(idxs.begin(), idxs.end(), 0);

    // sort in global order
    sm::GlobalCellCmp globalcmp(array->array()->array_schema_latest().domain());

    auto icmp = [&](uint64_t ia, uint64_t ib) -> bool {
      return std::apply(
          [&globalcmp, ia, ib]<typename... Ts>(
              const query_buffers<Ts>&... dims) {
            const auto l = std::make_tuple(dims[ia]...);
            const auto r = std::make_tuple(dims[ib]...);
            return globalcmp(
                templates::global_cell_cmp_std_tuple<decltype(l)>(l),
                templates::global_cell_cmp_std_tuple<decltype(r)>(r));
          },
          expect.dimensions());
    };

    std::sort(idxs.begin(), idxs.end(), icmp);

    // de-duplicate coordinates
    if (!instance.allow_duplicates()) {
      std::map<uint64_t, uint64_t, decltype(icmp)> dedup(icmp);
      for (const auto& idx : idxs) {
        auto it = dedup.find(idx);
        if (it == dedup.end()) {
          dedup[idx] = expect_fragment[idx];
        } else if (expect_fragment[idx] > it->second) {
          // NB: looks weird but we need this value of `idx`
          dedup.erase(it);
          dedup[idx] = expect_fragment[idx];
        }
      }

      idxs.clear();
      idxs.reserve(dedup.size());
      for (const auto& idx : dedup) {
        idxs.push_back(idx.first);
      }
    }

    // apply query condition
    if (instance.condition.has_value()) {
      std::vector<uint64_t> accept;
      templates::QueryConditionEvalSchema<typename Instance::FragmentType> eval;
      for (uint64_t i = 0; i < idxs.size(); i++) {
        if (eval.test(expect, idxs[i], *instance.condition.value().get())) {
          accept.push_back(idxs[i]);
        }
      }
      idxs = accept;
    }

    expect.dimensions() = stdx::select(
        stdx::reference_tuple(expect.dimensions()), std::span(idxs));
    expect.attributes() = stdx::select(
        stdx::reference_tuple(expect.attributes()), std::span(idxs));
  }

  // Create query
  tiledb_query_t* query;
  auto rc = tiledb_query_alloc(context(), array, TILEDB_READ, &query);
  ASSERTER(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(context(), query, TILEDB_GLOBAL_ORDER);
  ASSERTER(rc == TILEDB_OK);

  if (!instance.subarray.empty()) {
    tiledb_subarray_t* subarray;
    TRY(context(), tiledb_subarray_alloc(context(), array, &subarray));
    TRY(context(),
        instance.template apply_subarray<Asserter>(context(), subarray));
    TRY(context(), tiledb_query_set_subarray_t(context(), query, subarray));
    tiledb_subarray_free(&subarray);
  }

  if (instance.condition.has_value()) {
    tiledb::sm::QueryCondition qc(instance.condition->get()->clone());
    const auto rc =
        query->query_->set_condition(qc);  // SAFETY: this performs a deep copy
    ASSERTER(rc.to_string() == "Ok");
  }

  // Prepare output buffer
  std::decay_t<decltype(instance.fragments[0])> out;

  auto outdims = out.dimensions();
  auto outatts = out.attributes();
  std::apply(
      [&](auto&... field) {
        (field.resize(std::max<uint64_t>(1, expect.size()), 0), ...);
      },
      std::tuple_cat(outdims, outatts));

  using field_sizes_type =
      templates::query::fragment_field_sizes_t<decltype(out)>;

  // Query loop
  field_sizes_type outcursor;
  while (true) {
    // make field size locations
    field_sizes_type field_sizes = templates::query::make_field_sizes<Asserter>(
        out, instance.num_user_cells);

    // add fields to query
    templates::query::set_fields<Asserter>(
        context(),
        query,
        field_sizes,
        out,
        [](unsigned d) { return "d" + std::to_string(d + 1); },
        [](unsigned a) { return "a" + std::to_string(a + 1); },
        outcursor);

    rc = tiledb_query_submit(context(), query);
    {
      const auto err = error_if_any(rc);
      if (err.has_value()) {
        if (err->find("Cannot load enough tiles to emit results from all "
                      "fragments in global order") != std::string::npos) {
          if (!vfs_test_setup_.is_rest()) {
            // skip for REST since we will not have access to tile sizes
            const auto can_complete =
                can_complete_in_memory_budget<Asserter, Instance>(
                    context(), array_name_.c_str(), instance);
            if (can_complete.has_value()) {
              ASSERTER(!can_complete.value());
            }
          }
          tiledb_query_free(&query);
          return;
        }
        if (err->find("Cannot set array memory budget") != std::string::npos) {
          if (!vfs_test_setup_.is_rest()) {
            // skip for REST since we will not have accurate array memory
            const uint64_t array_usage =
                array->array()->memory_tracker()->get_memory_usage();
            const uint64_t array_budget =
                std::stod(instance.memory.ratio_array_data_) *
                std::stol(instance.memory.total_budget_);
            ASSERTER(array_usage > array_budget);
          }
          tiledb_query_free(&query);
          return;
        }
        if constexpr (std::is_same_v<Asserter, AsserterRapidcheck>) {
          if (err->find("Cannot allocate space for preprocess result tile ID "
                        "list") != std::string::npos) {
            // not enough memory to determine tile order
            // we can probably make some assertions about what this should
            // have looked like but for now we'll let it go
            tiledb_query_free(&query);
            return;
          }
          if (err->find("Cannot load tile offsets") != std::string::npos) {
            // not enough memory budget for tile offsets, don't bother
            // asserting about it (for now?)
            tiledb_query_free(&query);
            return;
          }
        }
      }
      // other errors we do not expect, this fails deterministically
      ASSERTER(std::optional<std::string>() == err);
    }

    tiledb_query_status_t status;
    rc = tiledb_query_get_status(context(), query, &status);
    ASSERTER(rc == TILEDB_OK);

    const uint64_t num_cells =
        templates::query::num_cells<Asserter>(out, field_sizes);

    const uint64_t num_cells_bound =
        std::min<uint64_t>(instance.num_user_cells, expect.size());
    if (num_cells < num_cells_bound) {
      ASSERTER(status == TILEDB_COMPLETED);
    } else {
      ASSERTER(num_cells == num_cells_bound);
    }

    std::apply(
        [&](auto&... field) {
          std::apply(
              [&](const auto&... field_cursor) {
                std::apply(
                    [&](const auto&... field_size) {
                      (field.apply_cursor(field_cursor, field_size), ...);
                    },
                    field_sizes);
              },
              outcursor);
        },
        std::tuple_cat(outdims, outatts));

    const uint64_t cursor_cells =
        templates::query::num_cells<Asserter>(out, outcursor);
    ASSERTER(cursor_cells + num_cells <= expect.size());

    // accumulate
    std::apply(
        [&](auto&... field) {
          std::apply(
              [&](auto&... field_cursor) {
                std::apply(
                    [&](const auto&... field_size) {
                      (field.accumulate_cursor(field_cursor, field_size), ...);
                    },
                    field_sizes);
              },
              outcursor);
        },
        std::tuple_cat(outdims, outatts));

    if (status == TILEDB_COMPLETED) {
      break;
    }
  }

  // Clean up.
  tiledb_query_free(&query);

  std::apply(
      [outcursor](auto&... outfield) {
        std::apply(
            [&](const auto&... field_cursor) {
              (outfield.finish_multipart_read(field_cursor), ...);
            },
            outcursor);
      },
      std::tuple_cat(outdims, outatts));

  ASSERTER(expect.dimensions() == outdims);

  // Checking attributes is more complicated because:
  // 1) when dups are off, equal coords will choose the attribute from one
  // fragment. 2) when dups are on, the attributes may manifest in any order.
  // Identify the runs of equal coords and then compare using those
  size_t attcursor = 0;
  size_t runlength = 1;

  auto viewtuple = [&](const auto& atttuple, size_t i) {
    return std::apply(
        [&](const auto&... att) { return std::make_tuple(att[i]...); },
        atttuple);
  };

  for (size_t i = 1; i < out.size(); i++) {
    if (std::apply(
            [&](const auto&... outdim) {
              return (... && (outdim[i] == outdim[i - 1]));
            },
            outdims)) {
      runlength++;
    } else if (instance.allow_duplicates()) {
      std::set<decltype(viewtuple(outatts, 0))> outattsrun;
      std::set<decltype(viewtuple(outatts, 0))> expectattsrun;

      for (size_t j = attcursor; j < attcursor + runlength; j++) {
        outattsrun.insert(viewtuple(outatts, j));
        expectattsrun.insert(viewtuple(expect.attributes(), j));
      }

      ASSERTER(outattsrun == expectattsrun);

      attcursor += runlength;
      runlength = 1;
    } else {
      REQUIRE(runlength == 1);

      const auto out = viewtuple(expect.attributes(), i);

      // at least all the attributes will come from the same fragment
      if (out != viewtuple(expect.attributes(), i)) {
        // the found attribute values should match at least one of the
        // fragments
        bool matched = false;
        for (size_t f = 0; !matched && f < instance.fragments.size(); f++) {
          for (size_t ic = 0; !matched && ic < instance.fragments[f].size();
               ic++) {
            if (viewtuple(instance.fragments[f].dimensions(), ic) ==
                viewtuple(expect.dimensions(), ic)) {
              matched =
                  (viewtuple(instance.fragments[f].attributes(), ic) == out);
            }
          }
        }
        ASSERTER(matched);
      }

      attcursor += runlength;
      runlength = 1;
    }
  }

  // lastly, check the correctness of our memory budgeting function
  // (skip for REST since we will not have access to tile sizes)
  if (!vfs_test_setup_.is_rest()) {
    const auto can_complete = can_complete_in_memory_budget<Asserter, Instance>(
        context(), array_name_.c_str(), instance);
    if (can_complete.has_value()) {
      ASSERTER(can_complete.has_value());
    }
  }
}

// rapidcheck generators and Arbitrary specializations
namespace rc {

/**
 * @return a generator of valid subarrays within `domain`
 */
template <DimensionType D>
Gen<std::vector<templates::Domain<D>>> make_subarray_1d(
    const templates::Domain<D>& domain) {
  // NB: when (if) multi-range subarray is supported for global order
  // (or if this is used for non-global order)
  // change `num_ranges` to use the weighted element version
  std::optional<Gen<D>> num_ranges;
  if (true) {
    num_ranges = gen::just<D>(1);
  } else {
    num_ranges = gen::weightedElement<D>(
        {{50, 1}, {25, 2}, {13, 3}, {7, 4}, {4, 5}, {1, 6}});
  }

  return gen::mapcat(*num_ranges, [domain](int num_ranges) {
    return gen::container<std::vector<templates::Domain<D>>>(
        num_ranges, rc::make_range<D>(domain));
  });
}

template <Datatype DIMENSION_TYPE, typename... ATTR_TYPES>
struct Arbitrary<FxRun1D<DIMENSION_TYPE, ATTR_TYPES...>> {
  using value_type = FxRun1D<DIMENSION_TYPE, ATTR_TYPES...>;

  static Gen<value_type> arbitrary() {
    auto dimension = gen::arbitrary<templates::Dimension<DIMENSION_TYPE>>();
    auto allow_dups = gen::arbitrary<bool>();

    auto domain =
        gen::suchThat(gen::pair(allow_dups, dimension), [](const auto& domain) {
          bool allow_dups;
          templates::Dimension<DIMENSION_TYPE> dimension;
          std::tie(allow_dups, dimension) = domain;
          if (allow_dups) {
            return true;
          } else {
            // need to ensure that rapidcheck uniqueness can generate enough
            // cases
            return dimension.domain.num_cells() >= 256;
          }
        });

    auto fragments = gen::mapcat(
        domain, [](std::pair<bool, templates::Dimension<DIMENSION_TYPE>> arg) {
          bool allow_dups;
          templates::Dimension<DIMENSION_TYPE> dimension;
          std::tie(allow_dups, dimension) = arg;

          auto fragment = rc::
              make_fragment_1d<typename value_type::CoordType, ATTR_TYPES...>(
                  allow_dups, dimension.domain);

          auto fragments = gen::nonEmpty(
              gen::container<std::vector<typename value_type::FragmentType>>(
                  fragment));

          auto fragments_and_qc = gen::mapcat(fragments, [](auto fragments) {
            auto condition =
                make_query_condition<typename value_type::FragmentType>(
                    field_domains(fragments));
            return gen::pair(gen::just(fragments), condition);
          });

          return gen::tuple(
              gen::just(allow_dups),
              gen::just(dimension),
              make_subarray_1d(dimension.domain),
              fragments_and_qc);
        });

    auto num_user_cells = gen::inRange(1, 8 * 1024 * 1024);

    return gen::apply(
        [](auto fragments, int num_user_cells) {
          FxRun1D instance;
          instance.array.allow_dups_ = std::get<0>(fragments);
          instance.array.dimension_ = std::get<1>(fragments);
          instance.subarray = std::get<2>(fragments);
          instance.fragments = std::move(std::get<3>(fragments).first);
          instance.condition = std::move(std::get<3>(fragments).second);

          instance.num_user_cells = num_user_cells;

          return instance;
        },
        fragments,
        num_user_cells);
  }
};

/**
 * @return a generator of valid subarrays within the domains `d1` and `d2`
 */
Gen<Subarray2DType<int, int>> make_subarray_2d(
    const templates::Domain<int>& d1, const templates::Domain<int>& d2) {
  // NB: multi-range subarray is not supported (yet?) for global order read

  return gen::apply(
      [](auto d1, auto d2) {
        std::optional<typename decltype(d1)::ValueType> d1opt;
        std::optional<typename decltype(d2)::ValueType> d2opt;
        if (d1) {
          d1opt.emplace(*d1);
        }
        if (d2) {
          d2opt.emplace(*d2);
        }
        return std::vector<std::pair<decltype(d1opt), decltype(d2opt)>>{
            std::make_pair(d1opt, d2opt)};
      },
      gen::maybe(rc::make_range<int>(d1)),
      gen::maybe(rc::make_range(d2)));
}

template <>
struct Arbitrary<FxRun2D> {
  static Gen<FxRun2D> arbitrary() {
    constexpr Datatype Dim0Type = Datatype::INT32;
    constexpr Datatype Dim1Type = Datatype::INT32;
    using Coord0Type = FxRun2D::Coord0Type;
    using Coord1Type = FxRun2D::Coord1Type;

    static_assert(std::is_same_v<
                  tiledb::type::datatype_traits<Dim0Type>::value_type,
                  Coord0Type>);
    static_assert(std::is_same_v<
                  tiledb::type::datatype_traits<Dim1Type>::value_type,
                  Coord1Type>);

    auto allow_dups = gen::arbitrary<bool>();
    auto d0 = gen::arbitrary<templates::Dimension<Dim0Type>>();
    auto d1 = gen::arbitrary<templates::Dimension<Dim1Type>>();

    auto domain =
        gen::suchThat(gen::tuple(allow_dups, d0, d1), [](const auto& arg) {
          bool allow_dups;
          templates::Dimension<Dim0Type> d0;
          templates::Dimension<Dim1Type> d1;
          std::tie(allow_dups, d0, d1) = arg;
          if (allow_dups) {
            return true;
          } else {
            // need to ensure that rapidcheck uniqueness can generate enough
            // cases
            return (d0.domain.num_cells() + d1.domain.num_cells()) >= 12;
          }
        });

    auto fragments = gen::mapcat(domain, [](auto arg) {
      bool allow_dups;
      templates::Dimension<Dim0Type> d0;
      templates::Dimension<Dim1Type> d1;
      std::tie(allow_dups, d0, d1) = arg;

      auto fragment = rc::make_fragment_2d<Coord0Type, Coord1Type, int>(
          allow_dups, d0.domain, d1.domain);

      auto fragments = gen::nonEmpty(
          gen::container<std::vector<FxRun2D::FragmentType>>(fragment));

      auto fragments_and_qc = gen::mapcat(fragments, [](auto fragments) {
        auto condition = make_query_condition<FxRun2D::FragmentType>(
            field_domains(fragments));
        return gen::pair(gen::just(fragments), condition);
      });

      return gen::tuple(
          gen::just(allow_dups),
          gen::just(d0),
          gen::just(d1),
          make_subarray_2d(d0.domain, d1.domain),
          fragments_and_qc);
    });

    auto num_user_cells = gen::inRange(1, 8 * 1024 * 1024);
    auto tile_order = gen::element(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    auto cell_order = gen::element(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

    return gen::apply(
        [](auto fragments,
           int num_user_cells,
           tiledb_layout_t tile_order,
           tiledb_layout_t cell_order) {
          FxRun2D instance;
          instance.allow_dups = std::get<0>(fragments);
          instance.d1 = std::get<1>(fragments);
          instance.d2 = std::get<2>(fragments);
          instance.subarray = std::get<3>(fragments);
          instance.fragments = std::move(std::get<4>(fragments).first);
          instance.condition = std::move(std::get<4>(fragments).second);

          // TODO: capacity
          instance.num_user_cells = num_user_cells;
          instance.tile_order_ = tile_order;
          instance.cell_order_ = cell_order;

          return instance;
        },
        fragments,
        num_user_cells,
        tile_order,
        cell_order);
  }
};

/**
 * Specializes `show` to print the final test case after shrinking
 */
template <>
void show<FxRun2D>(const FxRun2D& instance, std::ostream& os) {
  size_t f = 0;

  os << "{" << std::endl;
  os << "\t\"fragments\": [" << std::endl;
  for (const auto& fragment : instance.fragments) {
    os << "\t\t{" << std::endl;
    os << "\t\t\t\"d1\": [" << std::endl;
    os << "\t\t\t\t";
    show(fragment.d1(), os);
    os << std::endl;
    os << "\t\t\t\"d2\": [" << std::endl;
    os << "\t\t\t\t";
    show(fragment.d2(), os);
    os << std::endl;
    os << "\t\t\t], " << std::endl;
    os << "\t\t\t\"atts\": [" << std::endl;
    os << "\t\t\t\t";
    show(std::get<0>(fragment.atts_), os);
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
  os << "\t\t\"allow_dups\": " << instance.allow_dups << std::endl;
  os << "\t\t\"dimensions\": [" << std::endl;
  os << "\t\t\t{" << std::endl;
  os << "\t\t\t\t\"domain\": [" << instance.d1.domain.lower_bound << ", "
     << instance.d1.domain.upper_bound << "]," << std::endl;
  os << "\t\t\t\t\"extent\": " << instance.d1.extent << "," << std::endl;
  os << "\t\t\t}," << std::endl;
  os << "\t\t\t{" << std::endl;
  os << "\t\t\t\t\"domain\": [" << instance.d2.domain.lower_bound << ", "
     << instance.d2.domain.upper_bound << "]," << std::endl;
  os << "\t\t\t\t\"extent\": " << instance.d2.extent << "," << std::endl;
  os << "\t\t\t}" << std::endl;
  os << "\t\t]" << std::endl;

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

/**
 * Applies `::run` to completely arbitrary 1D input.
 *
 * `NonShrinking` is used because the shrink space is very large,
 * and rapidcheck does not appear to give up. Hence if an instance
 * fails, it will shrink for an arbitrarily long time, which is
 * not appropriate for CI. If this happens, copy the seed and open a story,
 * and whoever investigates can remove the `NonShrinking` part and let
 * it run for... well who knows how long, really.
 */
TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: rapidcheck 1d",
    "[sparse-global-order][rest][rapidcheck][!mayfail]") {
  rc::prop(
      "rapidcheck arbitrary 1d", [this](rc::NonShrinking<FxRun1D<>> instance) {
        run<tiledb::test::AsserterRapidcheck, decltype(instance)::value_type>(
            instance);
      });
}

/**
 * Applies `::run` to completely arbitrary 2D input.
 *
 * `NonShrinking` is used because the shrink space is very large,
 * and rapidcheck does not appear to give up. Hence if an instance
 * fails, it will shrink for an arbitrarily long time, which is
 * not appropriate for CI. If this happens, copy the seed and open a story,
 * and whoever investigates can remove the `NonShrinking` part and let
 * it run for... well who knows how long, really.
 */
TEST_CASE_METHOD(
    CSparseGlobalOrderFx,
    "Sparse global order reader: rapidcheck 2d",
    "[sparse-global-order][rest][rapidcheck][!mayfail]") {
  rc::prop(
      "rapidcheck arbitrary 2d", [this](rc::NonShrinking<FxRun2D> instance) {
        run<tiledb::test::AsserterRapidcheck, FxRun2D>(instance);
      });
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
  using Asserter = AsserterCatch;

  // Create default array.
  reset_config();
  create_default_array_1d();

  int coords[5];
  uint64_t coords_size = sizeof(coords);

  // Open array
  CApiArray array(context(), array_name_.c_str(), TILEDB_READ);

  // Create query.
  tiledb_query_t* query;
  TRY(context(), tiledb_query_alloc(context(), array, TILEDB_READ, &query));
  TRY(context(),
      tiledb_query_set_layout(context(), query, TILEDB_GLOBAL_ORDER));
  TRY(context(),
      tiledb_query_set_data_buffer(
          context(), query, "d", &coords[0], &coords_size));

  // Apply subarray
  const int lower_1 = 4, upper_1 = 8;
  const int lower_2 = 16, upper_2 = 32;
  tiledb_subarray_t* sub;
  TRY(context(), tiledb_subarray_alloc(context(), array, &sub));
  TRY(context(),
      tiledb_subarray_add_range(
          context(), sub, 0, &lower_1, &upper_1, nullptr));
  TRY(context(),
      tiledb_subarray_add_range(
          context(), sub, 0, &lower_2, &upper_2, nullptr));
  TRY(context(), tiledb_query_set_subarray_t(context(), query, sub));
  tiledb_subarray_free(&sub);

  auto rc = tiledb_query_submit(context(), query);
  tiledb_query_free(&query);
  REQUIRE(rc == TILEDB_ERR);

  tiledb_error_t* error = nullptr;
  rc = tiledb_ctx_get_last_error(context(), &error);
  REQUIRE(rc == TILEDB_OK);

  const char* msg;
  rc = tiledb_error_message(error, &msg);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(
      std::string(msg).find(
          "Multi-range reads are not supported on a global order query") !=
      std::string::npos);
}
