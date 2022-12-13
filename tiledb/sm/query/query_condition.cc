/**
 * @file query_condition.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2022 TileDB, Inc.
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
 * Implements the QueryCondition class.
 */


#include "tiledb/sm/query/query_condition.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/readers/result_cell_slab.h"
#include "tiledb/storage_format/uri/parse_uri.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <type_traits>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

// These classes are never defined and are only used
// to specialize the query constraints function templates.
class ApplyLegacy;
class ApplyDense;
class ApplySparse;

template<class BitmapType>
struct QCExecContext {
  QCExecContext(
      const ArraySchema& array_schema,
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
      ResultTile* result_tile,
      const std::string& condition_marker,
      const uint64_t start,
      const uint64_t length,
      const uint64_t src_cell,
      const uint64_t stride,
      span<BitmapType>& results)
    : array_schema_(array_schema)
    , fragment_metadata_(fragment_metadata)
    , result_tile_(result_tile)
    , condition_marker_(condition_marker)
    , start_(start)
    , length_(length)
    , src_cell_(src_cell)
    , stride_(stride)
    , results_(results) {
  }

  QCExecContext derive(span<BitmapType>& new_results) {
    return QCExecContext(
        array_schema_,
        fragment_metadata_,
        result_tile_,
        condition_marker_,
        start_,
        length_,
        src_cell_,
        stride_,
        new_results
    );
  }

  const ArraySchema& array_schema_;
  const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata_;
  ResultTile* result_tile_;

  const std::string& condition_marker_;

  const uint64_t start_;
  const uint64_t length_;
  const uint64_t src_cell_;
  const uint64_t stride_;

  span<BitmapType>& results_;
};

QueryCondition::QueryCondition() {
}

QueryCondition::QueryCondition(const std::string& condition_marker)
    : condition_marker_(condition_marker)
    , condition_index_(0) {
}

QueryCondition::QueryCondition(tdb_unique_ptr<tiledb::sm::ASTNode>&& tree)
    : tree_(std::move(tree)) {
}

QueryCondition::QueryCondition(
    const uint64_t condition_index,
    const std::string& condition_marker,
    tdb_unique_ptr<tiledb::sm::ASTNode>&& tree)
    : condition_marker_(condition_marker)
    , condition_index_(condition_index)
    , tree_(std::move(tree)) {
}

QueryCondition::QueryCondition(const QueryCondition& rhs)
    : condition_marker_(rhs.condition_marker_)
    , condition_index_(rhs.condition_index_)
    , tree_(rhs.tree_ == nullptr ? nullptr : rhs.tree_->clone()) {
}

QueryCondition::QueryCondition(QueryCondition&& rhs)
    : condition_marker_(std::move(rhs.condition_marker_))
    , condition_index_(rhs.condition_index_)
    , tree_(std::move(rhs.tree_)) {
}

QueryCondition::~QueryCondition() {
}

QueryCondition& QueryCondition::operator=(const QueryCondition& rhs) {
  if (this != &rhs) {
    condition_marker_ = rhs.condition_marker_;
    condition_index_ = rhs.condition_index_;
    tree_ = rhs.tree_ == nullptr ? nullptr : rhs.tree_->clone();
  }

  return *this;
}

QueryCondition& QueryCondition::operator=(QueryCondition&& rhs) {
  condition_marker_ = std::move(rhs.condition_marker_);
  condition_index_ = std::move(rhs.condition_index_);
  tree_ = std::move(rhs.tree_);
  return *this;
}

Status QueryCondition::init(
    std::string&& field_name,
    const void* const condition_value,
    const uint64_t condition_value_size,
    const QueryConditionOp& op) {
  if (tree_) {
    return Status_QueryConditionError("Cannot reinitialize query condition");
  }

  // AST Construction.
  tree_ = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal, field_name, condition_value, condition_value_size, op));

  return Status::Ok();
}

Status QueryCondition::check(const ArraySchema& array_schema) const {
  if (!tree_) {
    return Status::Ok();
  }

  RETURN_NOT_OK(tree_->check_node_validity(array_schema));
  return Status::Ok();
}

Status QueryCondition::combine(
    const QueryCondition& rhs,
    const QueryConditionCombinationOp combination_op,
    QueryCondition* const combined_cond) const {
  if (combination_op != QueryConditionCombinationOp::AND &&
      combination_op != QueryConditionCombinationOp::OR) {
    return Status_QueryConditionError(
        "Cannot combine query conditions; Only the 'AND' "
        "and 'OR' combination ops are supported");
  }

  combined_cond->field_names_.clear();
  combined_cond->tree_ = this->tree_->combine(rhs.tree_, combination_op);
  return Status::Ok();
}

bool QueryCondition::empty() const {
  return tree_ == nullptr;
}

std::unordered_set<std::string>& QueryCondition::field_names() const {
  if (field_names_.empty() && tree_ != nullptr) {
    tree_->get_field_names(field_names_);
  }

  return field_names_;
}

uint64_t QueryCondition::condition_timestamp() const {
  if (condition_marker_.empty()) {
    return 0;
  }

  std::pair<uint64_t, uint64_t> timestamps;
  if (!utils::parse::get_timestamp_range(URI(condition_marker_), &timestamps)
           .ok()) {
    throw std::logic_error("Error parsing condition marker.");
  }

  return timestamps.first;
}

template<typename DataType, QueryConditionOp CompareOp>
struct BinaryCmp;

/** Full template specialization for `char*` and `QueryConditionOp::LT`. */
template <>
struct BinaryCmp<char*, QueryConditionOp::LT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp < 0;
    }

    return lhs_size < rhs_size;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::LE. */
template <>
struct BinaryCmp<char*, QueryConditionOp::LE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp < 0;
    }

    return lhs_size <= rhs_size;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::GT`. */
template <>
struct BinaryCmp<char*, QueryConditionOp::GT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp > 0;
    }

    return lhs_size > rhs_size;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::GE`. */
template <>
struct BinaryCmp<char*, QueryConditionOp::GE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp > 0;
    }

    return lhs_size >= rhs_size;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::EQ`. */
template <>
struct BinaryCmp<char*, QueryConditionOp::EQ> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs_size != rhs_size) {
      return false;
    }

    return strncmp(
               static_cast<const char*>(lhs),
               static_cast<const char*>(rhs),
               lhs_size) == 0;
  }
};

/** Partial template specialization for `char*` and `QueryConditionOp::NE`. */
template <>
struct BinaryCmp<char*, QueryConditionOp::NE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs_size != rhs_size) {
      return true;
    }

    return strncmp(
               static_cast<const char*>(lhs),
               static_cast<const char*>(rhs),
               lhs_size) != 0;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::LT`. */
template <>
struct BinaryCmp<uint8_t*, QueryConditionOp::LT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp < 0;
    }

    return lhs_size < rhs_size;
  }
};

/** Partial template specialization for `uint8_t*` and `QueryConditionOp::LE. */
template <>
struct BinaryCmp<uint8_t*, QueryConditionOp::LE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp < 0;
    }

    return lhs_size <= rhs_size;
  }
};

/** Partial template specialization for `uint8_t*` and `QueryConditionOp::GT`. */
template <>
struct BinaryCmp<uint8_t*, QueryConditionOp::GT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp > 0;
    }

    return lhs_size > rhs_size;
  }
};

/** Partial template specialization for `uint8_t*` and `QueryConditionOp::GE`. */
template <>
struct BinaryCmp<uint8_t*, QueryConditionOp::GE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return cmp > 0;
    }

    return lhs_size >= rhs_size;
  }
};

/** Partial template specialization for `uint8_t*` and `QueryConditionOp::EQ`. */
template <>
struct BinaryCmp<uint8_t*, QueryConditionOp::EQ> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs_size != rhs_size) {
      return false;
    }

    return strncmp(
               static_cast<const char*>(lhs),
               static_cast<const char*>(rhs),
               lhs_size) == 0;
  }
};

/** Partial template specialization for `uint8_t*` and `QueryConditionOp::NE`. */
template <>
struct BinaryCmp<uint8_t*, QueryConditionOp::NE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs_size != rhs_size) {
      return true;
    }

    return strncmp(
               static_cast<const char*>(lhs),
               static_cast<const char*>(rhs),
               lhs_size) != 0;
  }
};

/** Partial template specialization for `QueryConditionOp::LT`. */
template <typename T>
struct BinaryCmp<T, QueryConditionOp::LT> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) < *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::LE`. */
template <typename T>
struct BinaryCmp<T, QueryConditionOp::LE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) <= *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::GT`. */
template <typename T>
struct BinaryCmp<T, QueryConditionOp::GT> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) > *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::GE`. */
template <typename T>
struct BinaryCmp<T, QueryConditionOp::GE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) >= *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::EQ`. */
template <typename T>
struct BinaryCmp<T, QueryConditionOp::EQ> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) == *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::NE`. */
template <typename T>
struct BinaryCmp<T, QueryConditionOp::NE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) != *static_cast<const T*>(rhs);
  }
};

template<
    class ApplyType,
    typename BitmapType,
    QueryConditionCombinationOp CombineOp>
struct CombineFunc;

template<typename BitmapType>
struct CombineFunc<ApplySparse, BitmapType, QueryConditionCombinationOp::AND> {
  static inline BitmapType combine(BitmapType lhs, BitmapType rhs) {
    return lhs * rhs;
  }
};

template<typename BitmapType>
struct CombineFunc<ApplySparse, BitmapType, QueryConditionCombinationOp::OR> {
  static inline BitmapType combine(BitmapType lhs, BitmapType rhs) {
    return std::max(lhs, rhs);
  }
};

template<class ApplyType, typename BitmapType>
struct CombineFunc<ApplyType, BitmapType, QueryConditionCombinationOp::AND> {
  static inline BitmapType combine(BitmapType lhs, BitmapType rhs) {
    return lhs && rhs;
  }
};

template<class ApplyType, typename BitmapType>
struct CombineFunc<ApplyType, BitmapType, QueryConditionCombinationOp::OR> {
  static inline BitmapType combine(BitmapType lhs, BitmapType rhs) {
    return lhs || rhs;
  }
};

template<
    class ApplyType,
    typename BitmapType,
    QueryConditionCombinationOp CombineOp,
    QueryConditionOp CompareOp,
    typename DataType,
    typename NullableType>
void apply_ast_node(
    QCExecContext<BitmapType>& exec_ctx,
    const tdb_unique_ptr<ASTNode>& node) {

  std::string field_name = node->get_field_name();

  const auto var_size = exec_ctx.array_schema_.var_size(field_name);

  // For shorter references below
  auto result_tile = exec_ctx.result_tile_;
  const auto fragment_metadata = exec_ctx.fragment_metadata_;
  const auto src_cell = exec_ctx.src_cell_;
  const uint64_t start = exec_ctx.start_;
  const auto stride = exec_ctx.stride_;
  auto& results = exec_ctx.results_;

  const void* cv_content = node->get_condition_value_view().content();
  const size_t cv_size = node->get_condition_value_view().size();

  const uint64_t length = results.size();

  if constexpr (std::is_same_v<ApplyType, ApplyLegacy>) {
    const auto nullable = exec_ctx.array_schema_.is_nullable(field_name);

    ByteVecValue fill_value;
    if (field_name != constants::timestamps &&
        field_name != constants::delete_timestamps) {
      if (!exec_ctx.array_schema_.is_dim(field_name)) {
        const auto attribute = exec_ctx.array_schema_.attribute(field_name);
        if (!attribute) {
          throw std::runtime_error("Unknown attribute " + field_name);
        }
        fill_value = attribute->fill_value();
      }
    }

    if (result_tile == nullptr && !nullable) {
      const bool cmp = BinaryCmp<DataType, CompareOp>::cmp(
          fill_value.data(),
          fill_value.size(),
          cv_content,
          cv_size);
      if (!cmp) {
        std::fill(results.begin(), results.end(), 0);
      }
      return;
    }

    // For delete timestamps conditions, if there's no delete metadata or
    // delete condition was already processed, GT condition is always true.
    const auto frag_idx = result_tile->frag_idx();
    if (field_name == constants::delete_timestamps &&
        (!fragment_metadata[frag_idx]->has_delete_meta() ||
         fragment_metadata[frag_idx]->get_processed_conditions_set().count(
              exec_ctx.condition_marker_) != 0)) {
      assert(CompareOp == QueryConditionOp::GT);
      std::fill(results.begin(), results.end(), 1);
      return;
    }

    if (!fragment_metadata[frag_idx]->array_schema()->is_field(field_name)) {
      // Requested field does not exist in this result cell - added by evolution.
      std::fill(results.begin(), results.end(), 0);
      return;
    }

    auto tile_tuple = result_tile->tile_tuple(field_name);

    // For fragments without timestamps, use the fragment first timestamp
    // as a comparison value.
    if (field_name == constants::timestamps && tile_tuple == nullptr) {
      auto timestamp = fragment_metadata[frag_idx]->first_timestamp();
      // PJD: BUG? ApplyLegacy against a __timestamps == null?
      // Because I'm using BinaryCmp here after removing BinaryCmpNullCheck
      assert(cv_content != nullptr);
      const bool cmp = BinaryCmp<DataType, CompareOp>::cmp(
          &timestamp,
          constants::timestamp_size,
          cv_content,
          cv_size);
      for (size_t c = 0; c < length; ++c) {
        results[c] = CombineFunc<ApplyType, BitmapType, CombineOp>::combine(
            results[c], (uint8_t) cmp);
      }
      return;
    }
  }

  auto tile_tuple = result_tile->tile_tuple(field_name);
  uint8_t* buffer_validity;

  if constexpr (
        CombineOp == QueryConditionCombinationOp::OR &&
        NullableType::value) {
    const auto& tile_validity = tile_tuple->validity_tile();
    buffer_validity = static_cast<uint8_t*>(tile_validity.data()) + src_cell;
  } else {
    buffer_validity = nullptr;
  }

  if (var_size) {
    const auto& data_tile = tile_tuple->var_tile();
    const char* data = static_cast<char*>(data_tile.data());
    const uint64_t data_size = data_tile.size();

    const auto& offsets_tile = tile_tuple->fixed_tile();
    const uint64_t* offsets =
        static_cast<uint64_t*>(offsets_tile.data()) + src_cell;

    // Iterate through each cell in this slab.
    for (size_t c = 0; c < length; ++c) {
      const uint64_t offset = offsets[start + c * stride];
      const uint64_t next_offset =
          // PJD: BUG? Should this be: start + (c + 1) * stride
          (start + c * stride + 1 < length) ?
              offsets[start + c * stride + 1] :
              data_size;
      const uint64_t cell_size = next_offset - offset;
      const void* const cell_value = data + offset;

      // Compare the cell value against the value in the value node.
      const bool cmp = BinaryCmp<DataType, CompareOp>::cmp(
          cell_value,
          cell_size,
          cv_content,
          cv_size);

      if constexpr (
          CombineOp == QueryConditionCombinationOp::OR &&
          NullableType::value) {
        results[c] = CombineFunc<ApplyType, BitmapType, CombineOp>::combine(
            results[c], cmp && (buffer_validity[start + c * stride] != 0));
      } else {
        results[c] = CombineFunc<ApplyType, BitmapType, CombineOp>::combine(
            results[c], cmp);
      }
    }
    return;
  }

  const auto& data_tile = tile_tuple->fixed_tile();
  const uint64_t cell_size = data_tile.cell_size();

  const char* data_ptr = static_cast<char*>(data_tile.data());
  const char* data = data_ptr + (start + src_cell) * cell_size;
  const uint64_t data_incr = stride * cell_size;

  // Iterate through each cell in this slab.
  for (size_t c = 0; c < length; c++) {
    const void* const cell_value = data + c * data_incr;

    // Compare the cell value against the value in the value node.
    const bool cmp = BinaryCmp<DataType, CompareOp>::cmp(
        cell_value,
        cell_size,
        cv_content,
        cv_size);

    if constexpr (
        CombineOp == QueryConditionCombinationOp::OR &&
        NullableType::value) {
      results[c] = CombineFunc<ApplyType, BitmapType, CombineOp>::combine(
          results[c], cmp && (buffer_validity[start + c * stride] != 0));
    } else {
      std::string op;
      results[c] = CombineFunc<ApplyType, BitmapType, CombineOp>::combine(
          results[c], cmp);
    }
  }
}

template<
    class ApplyType,
    typename BitmapType,
    QueryConditionCombinationOp CombineOp,
    QueryConditionOp CompareOp,
    typename DataType>
void apply_ast_node(
    QCExecContext<BitmapType>& exec_ctx,
    const tdb_unique_ptr<ASTNode>& node) {

  std::string field_name = node->get_field_name();
  const auto& array_schema = exec_ctx.array_schema_;
  const auto nullable = array_schema.is_nullable(field_name);

  if (nullable) {
    apply_ast_node<
        ApplyType,
        BitmapType,
        CombineOp,
        CompareOp,
        DataType,
        std::true_type
      >(exec_ctx, node);
  } else {
    apply_ast_node<
      ApplyType,
      BitmapType,
      CombineOp,
      CompareOp,
      DataType,
      std::false_type
    >(exec_ctx, node);
  }
}

template<
    class ApplyType,
    typename BitmapType,
    QueryConditionCombinationOp CombineOp,
    QueryConditionOp CompareOp>
void apply_ast_node(
    QCExecContext<BitmapType>& exec_ctx,
    const tdb_unique_ptr<ASTNode>& node) {

  std::string field_name = node->get_field_name();

  // Requested field does not exist in this result cell - added by evolution.
  if (!exec_ctx.array_schema_.is_field(field_name)) {
    std::fill(exec_ctx.results_.begin(), exec_ctx.results_.end(), 0);
    return;
  }

  const auto var_size = exec_ctx.array_schema_.var_size(field_name);
  const auto nullable = exec_ctx.array_schema_.is_nullable(field_name);
  const auto type = exec_ctx.array_schema_.type(field_name);

  const auto result_tile = exec_ctx.result_tile_;
  const auto start = exec_ctx.start_;
  const auto src_cell = exec_ctx.src_cell_;
  const auto stride = exec_ctx.stride_;
  auto& results = exec_ctx.results_;

  // Process the validity buffer now.
  if (nullable) {
    const auto tile_tuple = result_tile->tile_tuple(field_name);
    const auto& tile_validity = tile_tuple->validity_tile();
    const auto buffer_validity =
        static_cast<uint8_t*>(tile_validity.data()) + src_cell;

    if (node->get_condition_value_view().content() == nullptr) {
      // Null values can only be specified for equality operators.
      if (node->get_op() == QueryConditionOp::NE) {
        for (uint64_t c = 0; c < results.size(); ++c) {
          results[c] = CombineFunc<
              ApplyType,
              BitmapType,
              CombineOp
            >::combine(results[c], buffer_validity[start + c * stride] != 0);
        }
      } else {
        for (uint64_t c = 0; c < results.size(); ++c) {
          results[c] = CombineFunc<
              ApplyType,
              BitmapType,
              CombineOp
            >::combine(results[c], buffer_validity[start + c * stride] == 0);
        }
      }
      return;
    } else if constexpr (CombineOp == QueryConditionCombinationOp::AND) {
      // When the combination op is AND,
      // turn off bitmap values for null cells.
      for (uint64_t c = 0; c < results.size(); c++) {
        results[c] *= buffer_validity[start + c * stride] != 0;
      }
    }
  }

  switch (type) {
    case Datatype::INT8:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          int8_t
        >(exec_ctx, node);
    case Datatype::BOOL:
    case Datatype::UINT8:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          uint8_t
        >(exec_ctx, node);
    case Datatype::INT16:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          int16_t
        >(exec_ctx, node);
    case Datatype::UINT16:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          uint16_t
        >(exec_ctx, node);
    case Datatype::INT32:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          int32_t
        >(exec_ctx, node);
    case Datatype::UINT32:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          uint32_t
        >(exec_ctx, node);
    case Datatype::INT64:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          int64_t
        >(exec_ctx, node);
    case Datatype::UINT64:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          uint64_t
        >(exec_ctx, node);
    case Datatype::FLOAT32:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          float
        >(exec_ctx, node);
    case Datatype::FLOAT64:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          double
        >(exec_ctx, node);
    case Datatype::CHAR:
      if (var_size) {
      return apply_ast_node<
            ApplyType,
            BitmapType,
            CombineOp,
            CompareOp,
            char* // Convert to Datatype::STRING_ASCII
          >(exec_ctx, node);
      } else {
      return apply_ast_node<
            ApplyType,
            BitmapType,
            CombineOp,
            CompareOp,
            char
          >(exec_ctx, node);
      }
    case Datatype::STRING_ASCII:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          char* // Convert to Datatype::STRING_ASCII
        >(exec_ctx, node);
    case Datatype::STRING_UTF8:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          uint8_t* // Convert to Datatype::STRING_UTF8
        >(exec_ctx, node);
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          CompareOp,
          uint64_t
        >(exec_ctx, node);
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
      throw std::runtime_error(
      "QueryCondition::apply_ast_node: Cannot perform query comparison; "
      "Unsupported query conditional type on " + node->get_field_name());
    default:
      throw std::logic_error("Unaccounted for datatype in QueryCondition");
  }

  stdx::unreachable();
}

template<
    class ApplyType,
    typename BitmapType,
    QueryConditionCombinationOp CombineOp>
void apply_ast_node(
    QCExecContext<BitmapType>& exec_ctx,
    const tdb_unique_ptr<ASTNode>& node) {

  switch (node->get_op()) {
    case QueryConditionOp::LT:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          QueryConditionOp::LT
        >(exec_ctx, node);
    case QueryConditionOp::LE:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          QueryConditionOp::LE
        >(exec_ctx, node);
    case QueryConditionOp::GT:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          QueryConditionOp::GT
        >(exec_ctx, node);
    case QueryConditionOp::GE:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          QueryConditionOp::GE
        >(exec_ctx, node);
    case QueryConditionOp::EQ:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          QueryConditionOp::EQ
        >(exec_ctx, node);
    case QueryConditionOp::NE:
      return apply_ast_node<
          ApplyType,
          BitmapType,
          CombineOp,
          QueryConditionOp::NE
        >(exec_ctx, node);
    default:
      throw std::runtime_error(
          "QueryCondition::apply_ast_node: Cannot perform query comparison; "
          "Unknown query condition operator.");
  }

  stdx::unreachable();
}

template<
  class ApplyType,
  typename BitmapType,
  QueryConditionCombinationOp CombineOp>
void apply_tree(
    QCExecContext<BitmapType>& exec_ctx,
    const tdb_unique_ptr<ASTNode>& node) {

  if (!node->is_expr()) {
    apply_ast_node<ApplyType, BitmapType, CombineOp>(exec_ctx, node);
    return;
  }

  const auto results_size = exec_ctx.results_.size();
  switch (node->get_combination_op()) {
      /*
       * cl(q; a) means evaluate a clause (which may be compound) with query q
       * given existing bitmap a
       *
       * Identities:
       *
       * cl(q; a) = cl(q; 1) /\ a
       * cl1(q; a) /\ cl2(q; b) = cl1(q; cl2(q; a))
       *
       * cl1(q; a) \/ cl2(q; a) = (cl1(q; 1) /\ a) \/ (cl2(q; 1) /\ a)
       *                        = (cl1(q; 1) \/ cl2(q; 1)) /\ a
       */
    case QueryConditionCombinationOp::AND: {
      /*
       * cl1(q; a) /\ cl2(q; a) = cl1(q; cl2(q; a))
       */
      if constexpr (CombineOp == QueryConditionCombinationOp::AND) {
        for (const auto& child : node->get_children()) {
          apply_tree<ApplyType, BitmapType, CombineOp>(exec_ctx, child);
        }
        return;
      }

      // Handle the cl'(q, a) case
      if constexpr (CombineOp == QueryConditionCombinationOp::OR) {
        std::vector<BitmapType> tmp_results(results_size, 1);
        auto tmp_results_span = span(tmp_results);
        QCExecContext sub_ctx = exec_ctx.derive(tmp_results_span);

        for (const auto& child : node->get_children()) {
          apply_tree<
              ApplyType,
              BitmapType,
              QueryConditionCombinationOp::AND
            >(sub_ctx, child);
        }

        for (size_t c = 0; c < results_size; ++c) {
          exec_ctx.results_[c] |= tmp_results[c];
        }

        return;
      }

      // TODO: Make this a constant
      throw std::logic_error("https://www.youtube.com/watch?v=5IsSpAOD6K8");
    };

      /*
       * cl1(q; a) \/ cl2(q; a) = a /\ (cl1(q; 1) \/ cl2(q; 1))
       *                        = a /\ (cl1'(q; 0) \/ cl2'(q; 0))
       *                        = a /\ (cl1'(q; cl2'(q; 0)))
       */
    case QueryConditionCombinationOp::OR: {
      std::vector<BitmapType> tmp_results(results_size, 0);
      auto tmp_results_span = span(tmp_results);

      QCExecContext sub_ctx = exec_ctx.derive(tmp_results_span);

      for (const auto& child : node->get_children()) {
        apply_tree<
            ApplyType,
            BitmapType,
            QueryConditionCombinationOp::OR
          >(sub_ctx, child);
      }

      for (size_t c = 0; c < results_size; ++c) {
        exec_ctx.results_[c] *= tmp_results[c];
      }

      return;
    };

    case QueryConditionCombinationOp::NOT: {
      // PJD: ENSURE TESTED
      throw std::runtime_error(
          "Query condition NOT operator is not currently supported.");
    };

    default: {
      throw std::logic_error(
          "Invalid combination operator when applying query condition.");
    }
  }

  stdx::unreachable();
}

Status QueryCondition::apply(
    const ArraySchema& array_schema,
    const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
    std::vector<ResultCellSlab>& result_cell_slabs,
    const uint64_t stride) const {
  if (!tree_) {
    return Status::Ok();
  }

  std::vector<ResultCellSlab> ret;

  for (const auto& elem : result_cell_slabs) {
    ResultTile* tile = elem.tile_;
    std::vector<uint8_t> results(elem.length_, 1);
    auto results_span = span(results);

    QCExecContext<uint8_t> exec_ctx(
        array_schema,
        fragment_metadata,
        tile,
        condition_marker_,
        elem.start_,
        elem.length_,
        0,
        stride,
        results_span);

    apply_tree<
        ApplyLegacy,
        uint8_t,
        QueryConditionCombinationOp::AND
      >(exec_ctx, tree_);

    uint64_t pending_start = elem.start_;
    uint64_t pending_length = 0;
    for (size_t c = 0; c < elem.length_; ++c) {
      if (results[c]) {
        pending_length += 1;
      } else {
        if (pending_length > 0) {
          ret.emplace_back(elem.tile_, pending_start, pending_length);
        }
        pending_start = elem.start_ + c + 1;
        pending_length = 0;
      }
    }
    if (pending_length > 0) {
      ret.emplace_back(elem.tile_, pending_start, pending_length);
    }
  }

  result_cell_slabs = std::move(ret);
  return Status::Ok();
}

Status QueryCondition::apply_dense(
    const ArraySchema& array_schema,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t length,
    const uint64_t src_cell,
    const uint64_t stride,
    uint8_t* result_buffer) {
  // Iterate through the tree.
  if (result_buffer == nullptr) {
    return Status_QueryConditionError("The result buffer is null.");
  }

  std::vector<shared_ptr<FragmentMetadata>> fragment_metadata;
  span<uint8_t> results_span(result_buffer + start, length);

  QCExecContext<uint8_t> exec_ctx(
    array_schema,
    fragment_metadata,
    result_tile,
    condition_marker_,
    start,
    length,
    src_cell,
    stride,
    results_span);

  apply_tree<ApplyDense, uint8_t, QueryConditionCombinationOp::AND>(
      exec_ctx,
      tree_);

  return Status::Ok();
}

template <typename BitmapType>
Status QueryCondition::apply_sparse(
    const ArraySchema& array_schema,
    ResultTile& result_tile,
    std::vector<BitmapType>& result_bitmap) {

  std::vector<shared_ptr<FragmentMetadata>> fragment_metadata;
  span<BitmapType> results_span(result_bitmap);

  QCExecContext<BitmapType> exec_ctx(
    array_schema,
    fragment_metadata,
    &result_tile,
    condition_marker_,
    0,
    0,
    0,
    1,
    results_span);

  apply_tree<ApplySparse, BitmapType, QueryConditionCombinationOp::AND>(
      exec_ctx,
      tree_);

  return Status::Ok();
}

QueryCondition QueryCondition::negated_condition() {
  return QueryCondition(tree_->get_negated_tree());
}

const tdb_unique_ptr<ASTNode>& QueryCondition::ast() const {
  return tree_;
}

const std::string& QueryCondition::condition_marker() const {
  return condition_marker_;
}

uint64_t QueryCondition::condition_index() const {
  return condition_index_;
}

void QueryCondition::set_ast(tdb_unique_ptr<ASTNode>&& ast) {
  tree_ = std::move(ast);
}

// Explicit template instantiations.
template Status QueryCondition::apply_sparse<uint8_t>(
    const ArraySchema& array_schema, ResultTile&, std::vector<uint8_t>&);
template Status QueryCondition::apply_sparse<uint64_t>(
    const ArraySchema& array_schema, ResultTile&, std::vector<uint64_t>&);
}  // namespace sm
}  // namespace tiledb
