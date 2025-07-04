/**
 * @file query_condition.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2024 TileDB, Inc.
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
#include "tiledb/common/assert.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/fragment/fragment_identifier.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/query/readers/result_cell_slab.h"

#ifdef HAVE_RUST
#include "tiledb/oxidize/arrow.h"
#include "tiledb/oxidize/expr.h"
#endif

#include <algorithm>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <type_traits>

using namespace tiledb::common;

namespace tiledb::sm {

static bool is_null_test(const ASTNode& node) {
  const ASTNodeVal* node_val = dynamic_cast<const ASTNodeVal*>(&node);
  if (node_val) {
    const auto op = node_val->get_op();
    return (
        node_val->get_value_ptr() == nullptr &&
        (op == QueryConditionOp::EQ || op == QueryConditionOp::NE ||
         op == QueryConditionOp::ALWAYS_TRUE ||
         op == QueryConditionOp::ALWAYS_FALSE));
  } else {
    return false;
  }
}

QueryCondition::QueryCondition() {
}

QueryCondition::QueryCondition(
    const std::string& field_name,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size,
    QueryConditionOp op) {
  tree_ = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal, field_name, data, data_size, offsets, offsets_size, op));
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

  if (op == QueryConditionOp::ALWAYS_TRUE ||
      op == QueryConditionOp::ALWAYS_FALSE) {
    auto op_str = query_condition_op_str(op);
    throw std::runtime_error("Invalid use of internal operation: " + op_str);
  }

  // AST Construction.
  tree_ = tdb_unique_ptr<ASTNode>(tdb_new(
      ASTNodeVal, field_name, condition_value, condition_value_size, op));

  return Status::Ok();
}

void QueryCondition::rewrite_for_schema(const ArraySchema& array_schema) {
  if (!tree_) {
    return;
  }

  tree_->rewrite_for_schema(array_schema);
}

#ifdef HAVE_RUST
bool QueryCondition::rewrite_to_datafusion(const ArraySchema& array_schema) {
  if (!datafusion_.has_value()) {
    std::vector<std::string> select(field_names().begin(), field_names().end());

    try {
      auto logical_expr = tiledb::oxidize::datafusion::logical_expr::create(
          array_schema, *tree_.get());
      auto dfschema =
          tiledb::oxidize::arrow::schema::create(array_schema, select);
      auto physical_expr = tiledb::oxidize::datafusion::physical_expr::create(
          *dfschema, std::move(logical_expr));

      datafusion_.emplace(std::move(dfschema), std::move(physical_expr));
    } catch (const ::rust::Error& e) {
      throw std::logic_error(
          "Unexpected error compiling expression: " + std::string(e.what()));
    }
    return true;
  }
  return false;
}
#endif

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
        "and 'OR' combination ops are supported in this function.");
  }

  combined_cond->field_names_.clear();
  combined_cond->enumeration_field_names_.clear();
  combined_cond->tree_ = this->tree_->combine(*rhs.tree_, combination_op);
  return Status::Ok();
}

Status QueryCondition::negate(
    QueryConditionCombinationOp combination_op,
    QueryCondition* combined_cond) const {
  if (combination_op != QueryConditionCombinationOp::NOT) {
    return Status_QueryConditionError(
        "Cannot negate query condition; Only the 'NOT' "
        "combination op is supported in this function.");
  }

  combined_cond->field_names_.clear();
  combined_cond->enumeration_field_names_.clear();
  combined_cond->tree_ = this->tree_->get_negated_tree();
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

std::unordered_set<std::string>& QueryCondition::enumeration_field_names()
    const {
  if (enumeration_field_names_.empty() && tree_ != nullptr) {
    tree_->get_enumeration_field_names(enumeration_field_names_);
  }

  return enumeration_field_names_;
}

uint64_t QueryCondition::condition_timestamp() const {
  if (condition_marker_.empty()) {
    return 0;
  }

  FragmentID fragment_id{condition_marker_};
  auto timestamps{fragment_id.timestamp_range()};
  return timestamps.first;
}

void QueryCondition::set_use_enumeration(bool use_enumeration) {
  tree_->set_use_enumeration(use_enumeration);
}

/** Full template specialization for `char*` and `QueryConditionOp::LT`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<char*, QueryConditionOp::LT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }

    const std::string_view svl(static_cast<const char*>(lhs), lhs_size);
    const std::string_view svr(static_cast<const char*>(rhs), rhs_size);
    return svl < svr;
  }
};

/** Full template specialization for `char*` and `QueryConditionOp::LE. */
template <>
struct QueryCondition::BinaryCmpNullChecks<char*, QueryConditionOp::LE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }

    const std::string_view svl(static_cast<const char*>(lhs), lhs_size);
    const std::string_view svr(static_cast<const char*>(rhs), rhs_size);
    return svl <= svr;
  }
};

/** Full template specialization for `char*` and `QueryConditionOp::GT`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<char*, QueryConditionOp::GT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }

    const std::string_view svl(static_cast<const char*>(lhs), lhs_size);
    const std::string_view svr(static_cast<const char*>(rhs), rhs_size);
    return svl > svr;
  }
};

/** Full template specialization for `char*` and `QueryConditionOp::GE`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<char*, QueryConditionOp::GE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }

    const std::string_view svl(static_cast<const char*>(lhs), lhs_size);
    const std::string_view svr(static_cast<const char*>(rhs), rhs_size);
    return svl >= svr;
  }
};

/** Full template specialization for `char*` and `QueryConditionOp::EQ`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<char*, QueryConditionOp::EQ> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == rhs) {
      return true;
    }

    if (lhs == nullptr || rhs == nullptr) {
      return false;
    }

    if (lhs_size != rhs_size) {
      return false;
    }

    const std::string_view svl(static_cast<const char*>(lhs), lhs_size);
    const std::string_view svr(static_cast<const char*>(rhs), rhs_size);
    return svl == svr;
  }
};

/** Full template specialization for `char*` and `QueryConditionOp::NE`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<char*, QueryConditionOp::NE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (rhs == nullptr && lhs != nullptr) {
      return true;
    }

    if (lhs == nullptr || rhs == nullptr) {
      return false;
    }

    if (lhs_size != rhs_size) {
      return true;
    }

    const std::string_view svl(static_cast<const char*>(lhs), lhs_size);
    const std::string_view svr(static_cast<const char*>(rhs), rhs_size);
    return svl != svr;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::LT`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<uint8_t*, QueryConditionOp::LT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }

    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = memcmp(
        static_cast<const uint8_t*>(lhs),
        static_cast<const uint8_t*>(rhs),
        min_size);
    if (cmp != 0) {
      return cmp < 0;
    }

    return lhs_size < rhs_size;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::LE. */
template <>
struct QueryCondition::BinaryCmpNullChecks<uint8_t*, QueryConditionOp::LE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }

    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = memcmp(
        static_cast<const uint8_t*>(lhs),
        static_cast<const uint8_t*>(rhs),
        min_size);
    if (cmp != 0) {
      return cmp < 0;
    }

    return lhs_size <= rhs_size;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::GT`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<uint8_t*, QueryConditionOp::GT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }

    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = memcmp(
        static_cast<const uint8_t*>(lhs),
        static_cast<const uint8_t*>(rhs),
        min_size);
    if (cmp != 0) {
      return cmp > 0;
    }

    return lhs_size > rhs_size;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::GE`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<uint8_t*, QueryConditionOp::GE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }

    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = memcmp(
        static_cast<const uint8_t*>(lhs),
        static_cast<const uint8_t*>(rhs),
        min_size);
    if (cmp != 0) {
      return cmp > 0;
    }

    return lhs_size >= rhs_size;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::EQ`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<uint8_t*, QueryConditionOp::EQ> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == rhs) {
      return true;
    }

    if (lhs == nullptr || rhs == nullptr) {
      return false;
    }

    if (lhs_size != rhs_size) {
      return false;
    }

    return memcmp(
               static_cast<const uint8_t*>(lhs),
               static_cast<const uint8_t*>(rhs),
               lhs_size) == 0;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::NE`. */
template <>
struct QueryCondition::BinaryCmpNullChecks<uint8_t*, QueryConditionOp::NE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (rhs == nullptr && lhs != nullptr) {
      return true;
    }

    if (lhs == nullptr || rhs == nullptr) {
      return false;
    }

    if (lhs_size != rhs_size) {
      return true;
    }

    return memcmp(
               static_cast<const uint8_t*>(lhs),
               static_cast<const uint8_t*>(rhs),
               lhs_size) != 0;
  }
};

/** Partial template specialization for `QueryConditionOp::ALWAYS_TRUE`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::ALWAYS_TRUE> {
  static inline bool cmp(const void*, uint64_t, const void*, uint64_t) {
    return true;
  }
};

/** Partial template specialization for `QueryConditionOp::ALWAYS_FALSE`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::ALWAYS_FALSE> {
  static inline bool cmp(const void*, uint64_t, const void*, uint64_t) {
    return false;
  }
};

/** Partial template specialization for `QueryConditionOp::LT`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::LT> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return lhs != nullptr &&
           *static_cast<const T*>(lhs) < *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::LE`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::LE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return lhs != nullptr &&
           *static_cast<const T*>(lhs) <= *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::GT`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::GT> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return lhs != nullptr &&
           *static_cast<const T*>(lhs) > *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::GE`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::GE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return lhs != nullptr &&
           *static_cast<const T*>(lhs) >= *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::EQ`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::EQ> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    if (lhs == rhs) {
      return true;
    }

    if (lhs == nullptr || rhs == nullptr) {
      return false;
    }

    return *static_cast<const T*>(lhs) == *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::NE`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::NE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    if (rhs == nullptr && lhs != nullptr) {
      return true;
    }

    if (lhs == nullptr || rhs == nullptr) {
      return false;
    }

    return *static_cast<const T*>(lhs) != *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::IN`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::IN> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t) {
    if (lhs == nullptr) {
      return false;
    }

    std::string_view sv(static_cast<const char*>(lhs), lhs_size);
    auto members =
        static_cast<const std::unordered_set<std::string_view>*>(rhs);

    return members->find(sv) != members->end();
  }
};

/** Partial template specialization for `QueryConditionOp::NOT_IN`. */
template <typename T>
struct QueryCondition::BinaryCmpNullChecks<T, QueryConditionOp::NOT_IN> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t) {
    if (lhs == nullptr) {
      return false;
    }

    std::string_view sv(static_cast<const char*>(lhs), lhs_size);
    auto members =
        static_cast<const std::unordered_set<std::string_view>*>(rhs);

    return members->find(sv) == members->end();
  }
};

template <typename T, QueryConditionOp Op, typename CombinationOp>
void QueryCondition::apply_ast_node(
    const tdb_unique_ptr<ASTNode>& node,
    const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    const ByteVecValue& fill_value,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CombinationOp combination_op,
    std::span<uint8_t> result_cell_bitmap) const {
  const std::string& field_name = node->get_field_name();
  const void* condition_value_content = node->get_value_ptr();
  const size_t condition_value_size = node->get_value_size();
  uint64_t starting_index = 0;
  for (const auto& rcs : result_cell_slabs) {
    ResultTile* const result_tile = rcs.tile_;
    const uint64_t start = rcs.start_;
    const uint64_t length = rcs.length_;

    // Handle an empty range.
    if (result_tile == nullptr && !nullable) {
      const bool cmp = BinaryCmpNullChecks<T, Op>::cmp(
          fill_value.data(),
          fill_value.size(),
          condition_value_content,
          condition_value_size);
      if (!cmp) {
        for (size_t c = starting_index; c < starting_index + length; ++c) {
          result_cell_bitmap[c] = 0;
        }
      }
    } else {
      if (result_tile == nullptr) {
        throw QueryConditionException(
            "Failed to apply query condition due to null '" + field_name +
            "' ResultTile.");
      }

      const auto& frag_meta = fragment_metadata[result_tile->frag_idx()];
      // If the tile_tuple is null it was skipped by this fragment in
      // ReaderBase::skip_field. This is the case for evolved attributes / dims.
      const auto tile_tuple = result_tile->tile_tuple(field_name);
      // Special attributes handle null tile_tuples specifically.
      // We can skip null tile_tuples only if they are not a special attribute.
      const bool skip_tile_tuple =
          !ArraySchema::is_special_attribute(field_name) &&
          tile_tuple == nullptr;

      // For delete timestamps conditions, if there's no delete metadata or
      // delete condition was already processed, GT condition is always true.
      if (field_name == constants::delete_timestamps &&
          (!frag_meta->has_delete_meta() ||
           frag_meta->loaded_metadata()->get_processed_conditions_set().count(
               condition_marker_) != 0)) {
        iassert(
            Op == QueryConditionOp::GT, "op = {}", query_condition_op_str(Op));
        for (size_t c = starting_index; c < starting_index + length; ++c) {
          result_cell_bitmap[c] = 1;
        }
        starting_index += length;
        continue;
      } else if (
          !frag_meta->array_schema()->is_field(field_name) || skip_tile_tuple) {
        // Requested field does not exist in this result cell - added by
        // evolution.
        for (size_t c = starting_index; c < starting_index + length; ++c) {
          result_cell_bitmap[c] = 0;
        }
        starting_index += length;
        continue;
      }

      uint8_t* buffer_validity = nullptr;

      if (nullable) {
        const auto& tile_validity = tile_tuple->validity_tile();
        buffer_validity = tile_validity.data_as<uint8_t>();
      }

      // Start the pending result cell slab at the start position
      // of the current result cell slab.
      uint64_t c = 0;

      if (var_size) {
        const auto& tile = tile_tuple->var_tile();
        const char* buffer = tile.data_as<char>();

        const auto& tile_offsets = tile_tuple->fixed_tile();
        const offsets_t* buffer_offsets = tile_offsets.data_as<offsets_t>();

        // Iterate through each cell in this slab.
        while (c < length) {
          const uint64_t buffer_offset = buffer_offsets[start + c * stride];
          const uint64_t next_cell_offset =
              buffer_offsets[start + c * stride + 1];
          const uint64_t cell_size = next_cell_offset - buffer_offset;

          const bool null_cell =
              (nullable && buffer_validity[start + c * stride] == 0);

          // Get the cell value.
          const void* const cell_value =
              null_cell ? nullptr : buffer + buffer_offset;

          // Compare the cell value against the value in the value node.
          const bool cmp = BinaryCmpNullChecks<T, Op>::cmp(
              cell_value,
              cell_size,
              condition_value_content,
              condition_value_size);

          result_cell_bitmap[starting_index + c] = combination_op(
              result_cell_bitmap[starting_index + c], (uint8_t)cmp);
          ++c;
        }
      } else {
        // For fragments without timestamps, use the fragment first timestamp
        // as a comparison value.
        if (field_name == constants::timestamps && tile_tuple == nullptr) {
          auto timestamp =
              fragment_metadata[result_tile->frag_idx()]->first_timestamp();
          const bool cmp = BinaryCmpNullChecks<T, Op>::cmp(
              &timestamp,
              constants::timestamp_size,
              condition_value_content,
              condition_value_size);
          while (c < length) {
            result_cell_bitmap[starting_index + c] = combination_op(
                result_cell_bitmap[starting_index + c], (uint8_t)cmp);
            c++;
          }
        } else {
          const auto& tile = tile_tuple->fixed_tile();
          const char* buffer = tile.data_as<char>();
          const uint64_t cell_size = tile.cell_size();
          uint64_t buffer_offset = start * cell_size;
          const uint64_t buffer_offset_inc = stride * cell_size;

          // Iterate through each cell in this slab.
          while (c < length) {
            const bool null_cell =
                nullable && buffer_validity[start + c * stride] == 0;

            // Get the cell value.
            const void* const cell_value =
                null_cell ? nullptr : buffer + buffer_offset;
            buffer_offset += buffer_offset_inc;

            // Compare the cell value against the value in the value node.
            const bool cmp = BinaryCmpNullChecks<T, Op>::cmp(
                cell_value,
                cell_size,
                condition_value_content,
                condition_value_size);

            result_cell_bitmap[starting_index + c] = combination_op(
                result_cell_bitmap[starting_index + c], (uint8_t)cmp);
            ++c;
          }
        }
      }
    }
    starting_index += length;
  }
}

template <typename T, typename CombinationOp>
void QueryCondition::apply_ast_node(
    const tdb_unique_ptr<ASTNode>& node,
    const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    const ByteVecValue& fill_value,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CombinationOp combination_op,
    std::span<uint8_t> result_cell_bitmap) const {
  switch (node->get_op()) {
    case QueryConditionOp::ALWAYS_TRUE:
      apply_ast_node<T, QueryConditionOp::ALWAYS_TRUE, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
      break;
    case QueryConditionOp::ALWAYS_FALSE:
      apply_ast_node<T, QueryConditionOp::ALWAYS_FALSE, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
      break;
    case QueryConditionOp::LT:
      apply_ast_node<T, QueryConditionOp::LT, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
      break;
    case QueryConditionOp::LE:
      apply_ast_node<T, QueryConditionOp::LE, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
      break;
    case QueryConditionOp::GT:
      apply_ast_node<T, QueryConditionOp::GT, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
      break;
    case QueryConditionOp::GE:
      apply_ast_node<T, QueryConditionOp::GE, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
      break;
    case QueryConditionOp::EQ:
      apply_ast_node<T, QueryConditionOp::EQ, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
      break;
    case QueryConditionOp::NE:
      apply_ast_node<T, QueryConditionOp::NE, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
      break;
    case QueryConditionOp::IN:
      apply_ast_node<T, QueryConditionOp::IN, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
      break;
    case QueryConditionOp::NOT_IN:
      apply_ast_node<T, QueryConditionOp::NOT_IN, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
      break;
    default:
      throw std::runtime_error(
          "QueryCondition::apply_ast_node: Cannot perform query comparison; "
          "Unknown query condition operator.");
  }

  return;
}

template <typename CombinationOp>
void QueryCondition::apply_ast_node(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
    const uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CombinationOp combination_op,
    std::span<uint8_t> result_cell_bitmap) const {
  std::string node_field_name = node->get_field_name();

  const auto nullable = array_schema.is_nullable(node_field_name);
  const auto var_size = array_schema.var_size(node_field_name);
  const auto type = array_schema.type(node_field_name);

  ByteVecValue fill_value;
  if (node_field_name != constants::timestamps &&
      node_field_name != constants::delete_timestamps) {
    if (!array_schema.is_dim(node_field_name)) {
      const auto attribute = array_schema.attribute(node_field_name);
      if (!attribute) {
        throw std::runtime_error("Unknown attribute " + node_field_name);
      }
      fill_value = attribute->fill_value();
    }
  }

  switch (type) {
    case Datatype::INT8: {
      apply_ast_node<int8_t, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::BOOL:
    case Datatype::UINT8: {
      apply_ast_node<uint8_t, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::INT16: {
      apply_ast_node<int16_t, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::UINT16: {
      apply_ast_node<uint16_t, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::INT32: {
      apply_ast_node<int32_t, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::UINT32: {
      apply_ast_node<uint32_t, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::INT64: {
      apply_ast_node<int64_t, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::UINT64: {
      apply_ast_node<uint64_t, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::FLOAT32: {
      apply_ast_node<float, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::FLOAT64: {
      apply_ast_node<double, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::STRING_ASCII: {
      apply_ast_node<char*, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::CHAR: {
      if (var_size) {
        apply_ast_node<char*, CombinationOp>(
            node,
            fragment_metadata,
            stride,
            var_size,
            nullable,
            fill_value,
            result_cell_slabs,
            combination_op,
            result_cell_bitmap);
      } else {
        apply_ast_node<char, CombinationOp>(
            node,
            fragment_metadata,
            stride,
            var_size,
            nullable,
            fill_value,
            result_cell_slabs,
            combination_op,
            result_cell_bitmap);
      }
    } break;
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
    case Datatype::TIME_AS: {
      apply_ast_node<int64_t, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::STRING_UTF8: {
      apply_ast_node<uint8_t*, CombinationOp>(
          node,
          fragment_metadata,
          stride,
          var_size,
          nullable,
          fill_value,
          result_cell_slabs,
          combination_op,
          result_cell_bitmap);
    } break;
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::GEOM_WKB:
    case Datatype::GEOM_WKT:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    default:
      if (is_null_test(*node)) {
        apply_ast_node<void*, CombinationOp>(
            node,
            fragment_metadata,
            stride,
            var_size,
            nullable,
            fill_value,
            result_cell_slabs,
            combination_op,
            result_cell_bitmap);
        break;
      }
      throw std::runtime_error(
          "QueryCondition::apply_ast_node: Cannot perform query comparison; "
          "Unsupported datatype " +
          datatype_str(type) + " on " + node->get_field_name());
  }

  return;
}

template <typename CombinationOp>
void QueryCondition::apply_tree(
    const tdb_unique_ptr<ASTNode>& node,
    const QueryCondition::Params& params,
    const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CombinationOp combination_op,
    std::span<uint8_t> result_cell_bitmap) const {
  const auto& array_schema = params.GetSchema();
  if (!node->is_expr()) {
    apply_ast_node(
        node,
        array_schema,
        fragment_metadata,
        stride,
        result_cell_slabs,
        combination_op,
        result_cell_bitmap);
  } else {
    const auto result_bitmap_size = result_cell_bitmap.size();
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

        /*
         * cl1(q; a) /\ cl2(q; a) = cl1(q; cl2(q; a))
         */
      case QueryConditionCombinationOp::AND: {
        if constexpr (std::
                          is_same_v<CombinationOp, std::logical_and<uint8_t>>) {
          for (const auto& child : node->get_children()) {
            apply_tree(
                child,
                params,
                fragment_metadata,
                stride,
                result_cell_slabs,
                std::logical_and<uint8_t>(),
                result_cell_bitmap);
          }

          // Handle the cl'(q, a) case
        } else if constexpr (std::is_same_v<
                                 CombinationOp,
                                 std::logical_or<uint8_t>>) {
          auto resource = params.GetMemoryTracker()->get_resource(
              MemoryType::QUERY_CONDITION);
          tdb::pmr::vector<uint8_t> combination_op_bitmap(
              result_bitmap_size, 1, resource);

          for (const auto& child : node->get_children()) {
            apply_tree(
                child,
                params,
                fragment_metadata,
                stride,
                result_cell_slabs,
                std::logical_and<uint8_t>(),
                combination_op_bitmap);
          }
          for (size_t c = 0; c < result_bitmap_size; ++c) {
            result_cell_bitmap[c] |= combination_op_bitmap[c];
          }
        }
      } break;

        /*
         * cl1(q; a) \/ cl2(q; a) = a /\ (cl1(q; 1) \/ cl2(q; 1))
         *                        = a /\ (cl1'(q; 0) \/ cl2'(q; 0))
         *                        = a /\ (cl1'(q; cl2'(q; 0)))
         */
      case QueryConditionCombinationOp::OR: {
        auto resource = params.GetMemoryTracker()->get_resource(
            MemoryType::QUERY_CONDITION);
        tdb::pmr::vector<uint8_t> combination_op_bitmap(
            result_bitmap_size, 0, resource);

        for (const auto& child : node->get_children()) {
          apply_tree(
              child,
              params,
              fragment_metadata,
              stride,
              result_cell_slabs,
              std::logical_or<uint8_t>(),
              combination_op_bitmap);
        }

        for (size_t c = 0; c < result_bitmap_size; ++c) {
          result_cell_bitmap[c] *= combination_op_bitmap[c];
        }
      } break;
      case QueryConditionCombinationOp::NOT: {
        throw std::runtime_error(
            "Query condition NOT operator is not currently supported.");
      } break;
      default: {
        throw std::logic_error(
            "Invalid combination operator when applying query condition.");
      }
    }
  }
}

Status QueryCondition::apply(
    const QueryCondition::Params& params,
    const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
    std::vector<ResultCellSlab>& result_cell_slabs,
    const uint64_t stride) const {
  if (!tree_) {
    return Status::Ok();
  }

  size_t total_lengths = 0;
  for (const auto& elem : result_cell_slabs) {
    total_lengths += elem.length_;
  }

  auto resource =
      params.GetMemoryTracker()->get_resource(MemoryType::QUERY_CONDITION);
  tdb::pmr::vector<uint8_t> result_cell_bitmap(total_lengths, 1, resource);

  apply_tree(
      tree_,
      params,
      fragment_metadata,
      stride,
      result_cell_slabs,
      std::logical_and<uint8_t>(),
      result_cell_bitmap);

  std::vector<ResultCellSlab> ret;
  uint64_t starting_index = 0;
  for (const auto& elem : result_cell_slabs) {
    uint64_t pending_start = elem.start_;
    uint64_t pending_length = 0;
    for (size_t c = 0; c < elem.length_; ++c) {
      if (result_cell_bitmap[starting_index + c]) {
        pending_length += 1;
      } else {
        if (pending_length > 0) {
          ret.emplace_back(elem.tile_, pending_start, pending_length);
        }
        pending_length = 0;
        pending_start = elem.start_ + c + 1;
      }
    }
    starting_index += elem.length_;
    if (pending_length > 0) {
      ret.emplace_back(elem.tile_, pending_start, pending_length);
    }
  }

  result_cell_slabs = std::move(ret);
  return Status::Ok();
}

/** Default dense dimension condition processing. */
template <
    typename T,
    QueryConditionOp Op,
    typename CombinationOp,
    typename Enable>
struct QueryCondition::DenseDimCondition {
  static void apply_ast_node_dense(
      const std::string,
      const void*,
      const ArraySchema&,
      const uint64_t,
      const uint64_t,
      CombinationOp,
      const void*,
      span<uint8_t>) {
    throw std::runtime_error("Dense dims need to be integers");
  }
};

/** Specialization for integer dense dimension condition processing. */
template <typename T, QueryConditionOp Op, typename CombinationOp>
struct QueryCondition::DenseDimCondition<
    T,
    Op,
    CombinationOp,
    typename std::enable_if<std::is_integral<T>::value, T>::type> {
  static void apply_ast_node_dense(
      const std::string field_name,
      const void* condition_value_content,
      const ArraySchema& array_schema,
      const uint64_t start,
      const uint64_t stride,
      CombinationOp combination_op,
      const void* cell_slab_coords,
      span<uint8_t> result_buffer) {
    auto cell_order{array_schema.cell_order()};
    unsigned dim_idx = 0;
    while (array_schema.dimension_ptr(dim_idx)->name() != field_name) {
      dim_idx++;
    }

    bool slab_dim = (cell_order == Layout::ROW_MAJOR) ?
                        (dim_idx == array_schema.dim_num() - 1) :
                        (dim_idx == 0);
    const T* start_coords = static_cast<const T*>(cell_slab_coords);

    if (slab_dim) {
      for (uint64_t c = 0; c < result_buffer.size(); ++c) {
        T cell_value = start_coords[dim_idx] + start + c * stride;
        const bool cmp = BinaryCmp<T, Op>::cmp(
            &cell_value, sizeof(T), condition_value_content, sizeof(T));
        result_buffer[c] = combination_op(result_buffer[c], (uint8_t)cmp);
      }
    } else {
      const void* cell_value = &start_coords[dim_idx];
      const bool cmp = BinaryCmp<T, Op>::cmp(
          cell_value, sizeof(T), condition_value_content, sizeof(T));
      for (uint64_t c = 0; c < result_buffer.size(); ++c) {
        result_buffer[c] = combination_op(result_buffer[c], (uint8_t)cmp);
      }
    }
  }
};

template <typename T, QueryConditionOp Op, typename CombinationOp>
void QueryCondition::apply_ast_node_dense(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    const ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    CombinationOp combination_op,
    const void* cell_slab_coords,
    span<uint8_t> result_buffer) const {
  const std::string& field_name = node->get_field_name();
  const void* condition_value_content = node->get_value_ptr();
  const size_t condition_value_size = node->get_value_size();

  // Get the nullable buffer.
  const auto tile_tuple = result_tile->tile_tuple(field_name);
  uint8_t* buffer_validity = nullptr;
  if (nullable) {
    const auto& tile_validity = tile_tuple->validity_tile();
    buffer_validity = tile_validity.data_as<uint8_t>() + src_cell;
  }

  if (var_size) {
    // Get var data buffer and tile offsets buffer.
    const auto& tile = tile_tuple->var_tile();
    const char* buffer = tile.data_as<char>();

    const auto& tile_offsets = tile_tuple->fixed_tile();
    const offsets_t* buffer_offsets = tile_offsets.data_as<offsets_t>();

    // Iterate through each cell in this slab.
    for (uint64_t c = 0; c < result_buffer.size(); ++c) {
      // Check the next cell here, which breaks vectorization but as this
      // is string data requiring a strcmp which cannot be vectorized, this is
      // ok.
      const uint64_t offset_idx = src_cell + (start + c) * stride;
      const offsets_t buffer_offset = buffer_offsets[offset_idx];
      const offsets_t next_cell_offset = buffer_offsets[offset_idx + 1];
      const uint64_t cell_size = next_cell_offset - buffer_offset;

      // Get the cell value.
      const void* const cell_value = buffer + buffer_offset;

      // Compare the cell value against the value in the value node.
      const bool cmp = BinaryCmp<T, Op>::cmp(
          cell_value, cell_size, condition_value_content, condition_value_size);

      // Set the value.
      bool buffer_validity_val = buffer_validity == nullptr ?
                                     true :
                                     buffer_validity[(start + c) * stride] != 0;
      result_buffer[c] =
          combination_op(result_buffer[c], (uint8_t)cmp && buffer_validity_val);
    }
  } else {
    if (array_schema.is_dim(field_name)) {
      DenseDimCondition<T, Op, CombinationOp>::apply_ast_node_dense(
          field_name,
          condition_value_content,
          array_schema,
          start,
          stride,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } else {
      // Get the fixed size data buffers.
      const auto& tile = tile_tuple->fixed_tile();
      const char* buffer = tile.data_as<char>();
      const uint64_t cell_size = tile.cell_size();
      uint64_t buffer_offset = ((start * stride) + src_cell) * cell_size;
      const uint64_t buffer_offset_inc = stride * cell_size;

      // Iterate through each cell in this slab.
      for (uint64_t c = 0; c < result_buffer.size(); ++c) {
        // Get the cell value.
        const void* const cell_value = buffer + buffer_offset;
        buffer_offset += buffer_offset_inc;

        // Compare the cell value against the value in the value node.
        const bool cmp = BinaryCmp<T, Op>::cmp(
            cell_value,
            cell_size,
            condition_value_content,
            condition_value_size);

        // Set the value.
        bool buffer_validity_val =
            buffer_validity == nullptr ?
                true :
                buffer_validity[(start + c) * stride] != 0;
        result_buffer[c] = combination_op(
            result_buffer[c], (uint8_t)cmp && buffer_validity_val);
      }
    }
  }
}

template <typename T, typename CombinationOp>
void QueryCondition::apply_ast_node_dense(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    const ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    CombinationOp combination_op,
    const void* cell_slab_coords,
    span<uint8_t> result_buffer) const {
  switch (node->get_op()) {
    case QueryConditionOp::ALWAYS_TRUE:
      apply_ast_node_dense<T, QueryConditionOp::ALWAYS_TRUE, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
      break;
    case QueryConditionOp::ALWAYS_FALSE:
      apply_ast_node_dense<T, QueryConditionOp::ALWAYS_FALSE, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
      break;
    case QueryConditionOp::LT:
      apply_ast_node_dense<T, QueryConditionOp::LT, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
      break;
    case QueryConditionOp::LE:
      apply_ast_node_dense<T, QueryConditionOp::LE, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
      break;
    case QueryConditionOp::GT:
      apply_ast_node_dense<T, QueryConditionOp::GT, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
      break;
    case QueryConditionOp::GE:
      apply_ast_node_dense<T, QueryConditionOp::GE, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
      break;
    case QueryConditionOp::EQ:
      apply_ast_node_dense<T, QueryConditionOp::EQ, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
      break;
    case QueryConditionOp::NE:
      apply_ast_node_dense<T, QueryConditionOp::NE, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
      break;
    case QueryConditionOp::IN:
      apply_ast_node_dense<T, QueryConditionOp::IN, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
      break;
    case QueryConditionOp::NOT_IN:
      apply_ast_node_dense<T, QueryConditionOp::NOT_IN, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
      break;
    default:
      throw std::runtime_error(
          "Cannot perform query comparison; Unknown query condition "
          "operator");
  }
}

template <typename CombinationOp>
void QueryCondition::apply_ast_node_dense(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    const ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    CombinationOp combination_op,
    const void* cell_slab_coords,
    span<uint8_t> result_buffer) const {
  Datatype type;
  bool var_size = false;
  bool nullable = false;
  auto& name = node->get_field_name();
  if (array_schema.is_dim(name)) {
    type = array_schema.dimension_ptr(name)->type();
  } else {
    const auto attribute = array_schema.attribute(node->get_field_name());
    if (!attribute) {
      throw std::runtime_error("Unknown attribute " + node->get_field_name());
    }
    type = attribute->type();
    var_size = attribute->var_size();
    nullable = attribute->nullable();
  }

  // Process the validity buffer now.
  if (nullable && node->get_value_ptr() == nullptr) {
    const auto tile_tuple = result_tile->tile_tuple(name);
    const auto& tile_validity = tile_tuple->validity_tile();
    const auto buffer_validity = tile_validity.data_as<uint8_t>() + src_cell;

    // Null values can only be specified for equality operators.
    if (node->get_op() == QueryConditionOp::NE) {
      for (uint64_t c = 0; c < result_buffer.size(); ++c) {
        result_buffer[c] = combination_op(
            buffer_validity[start + c * stride] != 0, result_buffer[c]);
      }
    } else {
      for (uint64_t c = 0; c < result_buffer.size(); ++c) {
        result_buffer[c] = combination_op(
            buffer_validity[start + c * stride] == 0, result_buffer[c]);
      }
    }
    return;
  }

  switch (type) {
    case Datatype::INT8: {
      apply_ast_node_dense<int8_t, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::BOOL:
    case Datatype::UINT8: {
      apply_ast_node_dense<uint8_t, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::INT16: {
      apply_ast_node_dense<int16_t, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::UINT16: {
      apply_ast_node_dense<uint16_t, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::INT32: {
      apply_ast_node_dense<int32_t, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::UINT32: {
      apply_ast_node_dense<uint32_t, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::INT64: {
      apply_ast_node_dense<int64_t, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::UINT64: {
      apply_ast_node_dense<uint64_t, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::FLOAT32: {
      apply_ast_node_dense<float, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::FLOAT64: {
      apply_ast_node_dense<double, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::STRING_ASCII: {
      apply_ast_node_dense<char*, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::CHAR: {
      if (var_size) {
        apply_ast_node_dense<char*, CombinationOp>(
            node,
            array_schema,
            result_tile,
            start,
            src_cell,
            stride,
            var_size,
            nullable,
            combination_op,
            cell_slab_coords,
            result_buffer);
      } else {
        apply_ast_node_dense<char, CombinationOp>(
            node,
            array_schema,
            result_tile,
            start,
            src_cell,
            stride,
            var_size,
            nullable,
            combination_op,
            cell_slab_coords,
            result_buffer);
      }
    } break;
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
    case Datatype::TIME_AS: {
      apply_ast_node_dense<int64_t, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::STRING_UTF8: {
      apply_ast_node_dense<uint8_t*, CombinationOp>(
          node,
          array_schema,
          result_tile,
          start,
          src_cell,
          stride,
          var_size,
          nullable,
          combination_op,
          cell_slab_coords,
          result_buffer);
    } break;
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::GEOM_WKB:
    case Datatype::GEOM_WKT:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    default:
      if (is_null_test(*node)) {
        apply_ast_node_dense<void*, CombinationOp>(
            node,
            array_schema,
            result_tile,
            start,
            src_cell,
            stride,
            var_size,
            nullable,
            combination_op,
            cell_slab_coords,
            result_buffer);
        break;
      }
      throw std::runtime_error(
          "Cannot perform query comparison; Unsupported query conditional "
          "type " +
          datatype_str(type) + " on " + node->get_field_name());
  }
}

template <typename CombinationOp>
void QueryCondition::apply_tree_dense(
    const tdb_unique_ptr<ASTNode>& node,
    const QueryCondition::Params& params,
    const ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    CombinationOp combination_op,
    const void* cell_slab_coords,
    span<uint8_t> result_buffer) const {
  if (!node->is_expr()) {
    apply_ast_node_dense(
        node,
        params.GetSchema(),
        result_tile,
        start,
        src_cell,
        stride,
        combination_op,
        cell_slab_coords,
        result_buffer);
  } else {
    const auto result_buffer_size = result_buffer.size();
    switch (node->get_combination_op()) {
        /*
         * cl(q; a) means evaluate a clause (which may be compound) with query
         * q given existing bitmap a
         *
         * Identities:
         *
         * cl(q; a) = cl(q; 1) /\ a
         * cl1(q; a) /\ cl2(q; b) = cl1(q; cl2(q; a))
         *
         * cl1(q; a) \/ cl2(q; a) = (cl1(q; 1) /\ a) \/ (cl2(q; 1) /\ a)
         *                        = (cl1(q; 1) \/ cl2(q; 1)) /\ a
         */

        /*
         * cl1(q; a) /\ cl2(q; a) = cl1(q; cl2(q; a))
         */
      case QueryConditionCombinationOp::AND: {
        if constexpr (std::
                          is_same_v<CombinationOp, std::logical_and<uint8_t>>) {
          for (const auto& child : node->get_children()) {
            apply_tree_dense(
                child,
                params,
                result_tile,
                start,
                src_cell,
                stride,
                std::logical_and<uint8_t>(),
                cell_slab_coords,
                result_buffer);
          }

          // Handle the cl'(q, a) case
        } else if constexpr (std::is_same_v<
                                 CombinationOp,
                                 std::logical_or<uint8_t>>) {
          auto resource = params.GetMemoryTracker()->get_resource(
              MemoryType::QUERY_CONDITION);
          tdb::pmr::vector<uint8_t> combination_op_bitmap(
              result_buffer_size, 1, resource);
          span<uint8_t> combination_op_span(
              combination_op_bitmap.data(), result_buffer_size);

          for (const auto& child : node->get_children()) {
            apply_tree_dense(
                child,
                params,
                result_tile,
                start,
                src_cell,
                stride,
                std::logical_and<uint8_t>(),
                cell_slab_coords,
                combination_op_span);
          }
          for (size_t c = 0; c < result_buffer_size; ++c) {
            result_buffer[c] |= combination_op_bitmap[c];
          }
        }
      } break;
        /*
         * cl1(q; a) \/ cl2(q; a) = a /\ (cl1(q; 1) \/ cl2(q; 1))
         *                        = a /\ (cl1'(q; 0) \/ cl2'(q; 0))
         *                        = a /\ (cl1'(q; cl2'(q; 0)))
         */
      case QueryConditionCombinationOp::OR: {
        auto resource = params.GetMemoryTracker()->get_resource(
            MemoryType::QUERY_CONDITION);
        tdb::pmr::vector<uint8_t> combination_op_bitmap(
            result_buffer_size, 0, resource);
        span<uint8_t> combination_op_span(
            combination_op_bitmap.data(), result_buffer_size);
        for (const auto& child : node->get_children()) {
          apply_tree_dense(
              child,
              params,
              result_tile,
              start,
              src_cell,
              stride,
              std::logical_or<uint8_t>(),
              cell_slab_coords,
              combination_op_span);
        }

        for (size_t c = 0; c < result_buffer_size; ++c) {
          result_buffer[c] *= combination_op_bitmap[c];
        }
      } break;
      case QueryConditionCombinationOp::NOT: {
        throw std::runtime_error(
            "Query condition NOT operator is not currently supported.");
      } break;
      default: {
        throw std::logic_error(
            "Invalid combination operator when applying query condition.");
      }
    }
  }
}

Status QueryCondition::apply_dense(
    const QueryCondition::Params& params,
    const ResultTile* result_tile,
    const uint64_t start,
    const uint64_t length,
    const uint64_t src_cell,
    const uint64_t stride,
    const void* cell_slab_coords,
    uint8_t* result_buffer) {
  // Iterate through the tree.
  if (result_buffer == nullptr) {
    return Status_QueryConditionError("The result buffer is null.");
  }

  span<uint8_t> result_span(result_buffer + start, length);
  apply_tree_dense(
      tree_,
      params,
      result_tile,
      start,
      src_cell,
      stride,
      std::logical_and<uint8_t>(),
      cell_slab_coords,
      result_span);
  return Status::Ok();
}

/** Full template specialization for `char*` and `QueryConditionOp::LT`. */
template <>
struct QueryCondition::BinaryCmp<char*, QueryConditionOp::LT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const std::string_view svl(static_cast<const char*>(lhs), lhs_size);
    const std::string_view svr(static_cast<const char*>(rhs), rhs_size);
    return svl < svr;
  }
};

/** Full template specialization for `char*` and `QueryConditionOp::LE. */
template <>
struct QueryCondition::BinaryCmp<char*, QueryConditionOp::LE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const std::string_view svl(static_cast<const char*>(lhs), lhs_size);
    const std::string_view svr(static_cast<const char*>(rhs), rhs_size);
    return svl <= svr;
  }
};

/** Full template specialization for `char*` and `QueryConditionOp::GT`. */
template <>
struct QueryCondition::BinaryCmp<char*, QueryConditionOp::GT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const std::string_view svl(static_cast<const char*>(lhs), lhs_size);
    const std::string_view svr(static_cast<const char*>(rhs), rhs_size);
    return svl > svr;
  }
};

/** Full template specialization for `char*` and `QueryConditionOp::GE`. */
template <>
struct QueryCondition::BinaryCmp<char*, QueryConditionOp::GE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const std::string_view svl(static_cast<const char*>(lhs), lhs_size);
    const std::string_view svr(static_cast<const char*>(rhs), rhs_size);
    return svl >= svr;
  }
};

/** Full template specialization for `char*` and `QueryConditionOp::EQ`. */
template <>
struct QueryCondition::BinaryCmp<char*, QueryConditionOp::EQ> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs_size != rhs_size) {
      return false;
    }

    const std::string_view svl(static_cast<const char*>(lhs), lhs_size);
    const std::string_view svr(static_cast<const char*>(rhs), rhs_size);
    return svl == svr;
  }
};

/** Full template specialization for `char*` and `QueryConditionOp::NE`. */
template <>
struct QueryCondition::BinaryCmp<char*, QueryConditionOp::NE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs_size != rhs_size) {
      return true;
    }

    const std::string_view svl(static_cast<const char*>(lhs), lhs_size);
    const std::string_view svr(static_cast<const char*>(rhs), rhs_size);
    return svl != svr;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::LT`. */
template <>
struct QueryCondition::BinaryCmp<uint8_t*, QueryConditionOp::LT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = memcmp(
        static_cast<const uint8_t*>(lhs),
        static_cast<const char*>(rhs),
        min_size);
    if (cmp != 0) {
      return cmp < 0;
    }

    return lhs_size < rhs_size;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::LE. */
template <>
struct QueryCondition::BinaryCmp<uint8_t*, QueryConditionOp::LE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = memcmp(
        static_cast<const uint8_t*>(lhs),
        static_cast<const uint8_t*>(rhs),
        min_size);
    if (cmp != 0) {
      return cmp < 0;
    }

    return lhs_size <= rhs_size;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::GT`. */
template <>
struct QueryCondition::BinaryCmp<uint8_t*, QueryConditionOp::GT> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = memcmp(
        static_cast<const uint8_t*>(lhs),
        static_cast<const uint8_t*>(rhs),
        min_size);
    if (cmp != 0) {
      return cmp > 0;
    }

    return lhs_size > rhs_size;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::GE`. */
template <>
struct QueryCondition::BinaryCmp<uint8_t*, QueryConditionOp::GE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = memcmp(
        static_cast<const uint8_t*>(lhs),
        static_cast<const uint8_t*>(rhs),
        min_size);
    if (cmp != 0) {
      return cmp > 0;
    }

    return lhs_size >= rhs_size;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::EQ`. */
template <>
struct QueryCondition::BinaryCmp<uint8_t*, QueryConditionOp::EQ> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs_size != rhs_size) {
      return false;
    }

    return memcmp(
               static_cast<const uint8_t*>(lhs),
               static_cast<const uint8_t*>(rhs),
               lhs_size) == 0;
  }
};

/** Full template specialization for `uint8_t*` and `QueryConditionOp::NE`. */
template <>
struct QueryCondition::BinaryCmp<uint8_t*, QueryConditionOp::NE> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs_size != rhs_size) {
      return true;
    }

    return memcmp(
               static_cast<const uint8_t*>(lhs),
               static_cast<const uint8_t*>(rhs),
               lhs_size) != 0;
  }
};

/** Partial template specialization for `QueryConditionOp::LT`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::ALWAYS_TRUE> {
  static inline bool cmp(const void*, uint64_t, const void*, uint64_t) {
    return true;
  }
};

/** Partial template specialization for `QueryConditionOp::LT`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::ALWAYS_FALSE> {
  static inline bool cmp(const void*, uint64_t, const void*, uint64_t) {
    return false;
  }
};

/** Partial template specialization for `QueryConditionOp::LT`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::LT> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) < *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::LE`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::LE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) <= *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::GT`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::GT> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) > *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::GE`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::GE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) >= *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::EQ`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::EQ> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) == *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::NE`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::NE> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    return *static_cast<const T*>(lhs) != *static_cast<const T*>(rhs);
  }
};

/** Partial template specialization for `QueryConditionOp::IN`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::IN> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t) {
    std::string_view sv(static_cast<const char*>(lhs), lhs_size);
    auto members =
        static_cast<const std::unordered_set<std::string_view>*>(rhs);
    return members->find(sv) != members->end();
  }
};

/** Partial template specialization for `QueryConditionOp::NOT_IN`. */
template <typename T>
struct QueryCondition::BinaryCmp<T, QueryConditionOp::NOT_IN> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t) {
    std::string_view sv(static_cast<const char*>(lhs), lhs_size);
    auto members =
        static_cast<const std::unordered_set<std::string_view>*>(rhs);
    return members->find(sv) == members->end();
  }
};

template <typename T>
struct QCMax {
  const T& operator()(const T& a, const T& b) const {
    return std::max(a, b);
  }
};

template <
    typename T,
    QueryConditionOp Op,
    typename BitmapType,
    typename CombinationOp,
    typename nullable>
void QueryCondition::apply_ast_node_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    const ResultTile& result_tile,
    const bool var_size,
    CombinationOp combination_op,
    std::span<BitmapType> result_bitmap) const {
  const auto tile_tuple = result_tile.tile_tuple(node->get_field_name());
  const void* condition_value_content = node->get_value_ptr();
  const size_t condition_value_size = node->get_value_size();
  uint8_t* buffer_validity = nullptr;

  // Check if the combination op = OR and the attribute is nullable.
  if constexpr (
      std::is_same_v<CombinationOp, QCMax<BitmapType>> && nullable::value) {
    const auto& tile_validity = tile_tuple->validity_tile();
    buffer_validity = tile_validity.data_as<uint8_t>();
  }

  if (var_size) {
    // Get var data buffer and tile offsets buffer.
    const auto& tile = tile_tuple->var_tile();
    const char* buffer = tile.data_as<char>();

    const auto& tile_offsets = tile_tuple->fixed_tile();
    const offsets_t* buffer_offsets = tile_offsets.data_as<offsets_t>();
    const uint64_t buffer_offsets_el = tile_offsets.size_as<offsets_t>() - 1;

    // Iterate through each cell.
    for (uint64_t c = 0; c < buffer_offsets_el; ++c) {
      // Check the previous cell here, which breaks vectorization but as this
      // is string data requiring a strcmp which cannot be vectorized, this is
      // ok.
      const offsets_t buffer_offset = buffer_offsets[c];
      const offsets_t next_cell_offset = buffer_offsets[c + 1];
      const uint64_t cell_size = next_cell_offset - buffer_offset;

      // Get the cell value.
      const void* const cell_value = buffer + buffer_offset;

      // Compare the cell value against the value in the value node.
      const bool cmp = BinaryCmp<T, Op>::cmp(
          cell_value, cell_size, condition_value_content, condition_value_size);

      // Set the value, casing on whether the combination op = OR and the
      // attribute is nullable.
      if constexpr (
          std::is_same_v<CombinationOp, QCMax<BitmapType>> && nullable::value) {
        result_bitmap[c] =
            combination_op(result_bitmap[c], cmp && (buffer_validity[c] != 0));
      } else {
        result_bitmap[c] = combination_op(result_bitmap[c], cmp);
      }
    }
  } else {
    // Get the fixed size data buffers.
    const auto& tile = tile_tuple->fixed_tile();
    const char* buffer = tile.data_as<char>();
    const uint64_t cell_size = tile.cell_size();
    const uint64_t buffer_el = tile.size() / cell_size;

    // Iterate through each cell without checking the bitmap to enable
    // vectorization.
    for (uint64_t c = 0; c < buffer_el; ++c) {
      // Get the cell value.
      const void* const cell_value = buffer + c * cell_size;

      // Compare the cell value against the value in the value node.
      const bool cmp = BinaryCmp<T, Op>::cmp(
          cell_value, cell_size, condition_value_content, condition_value_size);

      // Set the value, casing on whether the combination op = OR and the
      // attribute is nullable.
      if constexpr (
          std::is_same_v<CombinationOp, QCMax<BitmapType>> && nullable::value) {
        result_bitmap[c] =
            combination_op(result_bitmap[c], cmp && (buffer_validity[c] != 0));
      } else {
        result_bitmap[c] = combination_op(result_bitmap[c], cmp);
      }
    }
  }
}

template <
    typename T,
    typename BitmapType,
    typename CombinationOp,
    typename nullable>
void QueryCondition::apply_ast_node_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    const ResultTile& result_tile,
    const bool var_size,
    CombinationOp combination_op,
    std::span<BitmapType> result_bitmap) const {
  switch (node->get_op()) {
    case QueryConditionOp::ALWAYS_TRUE:
      apply_ast_node_sparse<
          T,
          QueryConditionOp::ALWAYS_TRUE,
          BitmapType,
          CombinationOp,
          nullable>(node, result_tile, var_size, combination_op, result_bitmap);
      break;
    case QueryConditionOp::ALWAYS_FALSE:
      apply_ast_node_sparse<
          T,
          QueryConditionOp::ALWAYS_FALSE,
          BitmapType,
          CombinationOp,
          nullable>(node, result_tile, var_size, combination_op, result_bitmap);
      break;
    case QueryConditionOp::LT:
      apply_ast_node_sparse<
          T,
          QueryConditionOp::LT,
          BitmapType,
          CombinationOp,
          nullable>(node, result_tile, var_size, combination_op, result_bitmap);
      break;
    case QueryConditionOp::LE:
      apply_ast_node_sparse<
          T,
          QueryConditionOp::LE,
          BitmapType,
          CombinationOp,
          nullable>(node, result_tile, var_size, combination_op, result_bitmap);
      break;
    case QueryConditionOp::GT:
      apply_ast_node_sparse<
          T,
          QueryConditionOp::GT,
          BitmapType,
          CombinationOp,
          nullable>(node, result_tile, var_size, combination_op, result_bitmap);
      break;
    case QueryConditionOp::GE:
      apply_ast_node_sparse<
          T,
          QueryConditionOp::GE,
          BitmapType,
          CombinationOp,
          nullable>(node, result_tile, var_size, combination_op, result_bitmap);
      break;
    case QueryConditionOp::EQ:
      apply_ast_node_sparse<
          T,
          QueryConditionOp::EQ,
          BitmapType,
          CombinationOp,
          nullable>(node, result_tile, var_size, combination_op, result_bitmap);
      break;
    case QueryConditionOp::NE:
      apply_ast_node_sparse<
          T,
          QueryConditionOp::NE,
          BitmapType,
          CombinationOp,
          nullable>(node, result_tile, var_size, combination_op, result_bitmap);
      break;
    case QueryConditionOp::IN:
      apply_ast_node_sparse<
          T,
          QueryConditionOp::IN,
          BitmapType,
          CombinationOp,
          nullable>(node, result_tile, var_size, combination_op, result_bitmap);
      break;
    case QueryConditionOp::NOT_IN:
      apply_ast_node_sparse<
          T,
          QueryConditionOp::NOT_IN,
          BitmapType,
          CombinationOp,
          nullable>(node, result_tile, var_size, combination_op, result_bitmap);
      break;
    default:
      throw std::runtime_error(
          "Cannot perform query comparison; Unknown query condition "
          "operator.");
  }
}

template <typename T, typename BitmapType, typename CombinationOp>
void QueryCondition::apply_ast_node_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    const ResultTile& result_tile,
    const bool var_size,
    const bool nullable,
    CombinationOp combination_op,
    std::span<BitmapType> result_bitmap) const {
  if (nullable) {
    apply_ast_node_sparse<T, BitmapType, CombinationOp, std::true_type>(
        node, result_tile, var_size, combination_op, result_bitmap);
  } else {
    apply_ast_node_sparse<T, BitmapType, CombinationOp, std::false_type>(
        node, result_tile, var_size, combination_op, result_bitmap);
  }
}

template <typename BitmapType, typename CombinationOp>
void QueryCondition::apply_ast_node_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    const ResultTile& result_tile,
    CombinationOp combination_op,
    std::span<BitmapType> result_bitmap) const {
  std::string node_field_name = node->get_field_name();
  // If the tile_tuple is null it may have been skipped by this fragment in
  // ReaderBase::skip_field. This is the case for evolved attributes / dims.
  const auto tile_tuple = result_tile.tile_tuple(node_field_name);
  // Special attributes handle null tile_tuples specifically.
  // We can skip null tile_tuples only if they are not a special attribute.
  const bool skip_tile_tuple =
      !ArraySchema::is_special_attribute(node_field_name) &&
      tile_tuple == nullptr;
  if (!array_schema.is_field(node_field_name) || skip_tile_tuple) {
    std::fill(result_bitmap.begin(), result_bitmap.end(), 0);
    return;
  }

  const auto nullable = array_schema.is_nullable(node_field_name);
  const auto var_size = array_schema.var_size(node_field_name);
  const auto type = array_schema.type(node_field_name);

  // Process the validity buffer now.
  if (nullable) {
    const auto& tile_validity = tile_tuple->validity_tile();
    const auto buffer_validity = tile_validity.data_as<uint8_t>();
    const auto cell_num = result_tile.cell_num();

    if (node->get_value_ptr() == nullptr) {
      // Null values can only be specified for equality operators.
      if (node->get_op() == QueryConditionOp::NE) {
        for (uint64_t c = 0; c < cell_num; c++) {
          result_bitmap[c] =
              combination_op(buffer_validity[c] != 0, result_bitmap[c]);
        }
      } else {
        for (uint64_t c = 0; c < cell_num; c++) {
          result_bitmap[c] =
              combination_op(buffer_validity[c] == 0, result_bitmap[c]);
        }
      }
      return;
    } else if constexpr (std::is_same_v<
                             CombinationOp,
                             std::multiplies<BitmapType>>) {
      // When the combination op is AND, turn off bitmap values for null
      // cells.
      for (uint64_t c = 0; c < cell_num; c++) {
        result_bitmap[c] *= buffer_validity[c] != 0;
      }
    }
  }

  switch (type) {
    case Datatype::INT8: {
      apply_ast_node_sparse<int8_t, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::BOOL:
    case Datatype::UINT8: {
      apply_ast_node_sparse<uint8_t, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::INT16: {
      apply_ast_node_sparse<int16_t, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::UINT16: {
      apply_ast_node_sparse<uint16_t, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::INT32: {
      apply_ast_node_sparse<int32_t, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::UINT32: {
      apply_ast_node_sparse<uint32_t, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::INT64: {
      apply_ast_node_sparse<int64_t, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::UINT64: {
      apply_ast_node_sparse<uint64_t, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::FLOAT32: {
      apply_ast_node_sparse<float, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::FLOAT64: {
      apply_ast_node_sparse<double, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::STRING_ASCII: {
      apply_ast_node_sparse<char*, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::CHAR: {
      if (var_size) {
        apply_ast_node_sparse<char*, BitmapType, CombinationOp>(
            node,
            result_tile,
            var_size,
            nullable,
            combination_op,
            result_bitmap);
      } else {
        apply_ast_node_sparse<char, BitmapType, CombinationOp>(
            node,
            result_tile,
            var_size,
            nullable,
            combination_op,
            result_bitmap);
      }
    } break;
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
    case Datatype::TIME_AS: {
      apply_ast_node_sparse<int64_t, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::STRING_UTF8: {
      apply_ast_node_sparse<uint8_t*, BitmapType, CombinationOp>(
          node, result_tile, var_size, nullable, combination_op, result_bitmap);
    } break;
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::GEOM_WKB:
    case Datatype::GEOM_WKT:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    default:
      if (is_null_test(*node)) {
        apply_ast_node_sparse<void*, BitmapType, CombinationOp>(
            node,
            result_tile,
            var_size,
            nullable,
            combination_op,
            result_bitmap);
        break;
      }
      throw std::runtime_error(
          "Cannot perform query comparison; Unsupported query conditional "
          "type on " +
          node->get_field_name());
  }
}

template <typename BitmapType, typename CombinationOp>
void QueryCondition::apply_tree_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    const QueryCondition::Params& params,
    const ResultTile& result_tile,
    CombinationOp combination_op,
    std::span<BitmapType> result_bitmap) const {
  const auto& array_schema = params.GetSchema();
  if (!node->is_expr()) {
    apply_ast_node_sparse<BitmapType>(
        node, array_schema, result_tile, combination_op, result_bitmap);
  } else {
    const auto result_bitmap_size = result_bitmap.size();
    switch (node->get_combination_op()) {
        /*
         * cl(q; a) means evaluate a clause (which may be compound) with query
         * q given existing bitmap a
         *
         * Identities:
         *
         * cl(q; a) = cl(q; 1) /\ a
         * cl1(q; a) /\ cl2(q; b) = cl1(q; cl2(q; a))
         *
         * cl1(q; a) \/ cl2(q; a) = (cl1(q; 1) /\ a) \/ (cl2(q; 1) /\ a)
         *                        = (cl1(q; 1) \/ cl2(q; 1)) /\ a
         */

        /*
         * cl1(q; a) /\ cl2(q; a) = cl1(q; cl2(q; a))
         */
      case QueryConditionCombinationOp::AND: {
        if constexpr (std::is_same_v<
                          CombinationOp,
                          std::multiplies<BitmapType>>) {
          for (const auto& child : node->get_children()) {
            apply_tree_sparse<BitmapType>(
                child,
                params,
                result_tile,
                std::multiplies<BitmapType>(),
                result_bitmap);
          }

          // Handle the cl'(q, a) case.
          // This cases on whether the combination op = OR.
        } else if constexpr (std::is_same_v<CombinationOp, QCMax<BitmapType>>) {
          auto resource = params.GetMemoryTracker()->get_resource(
              MemoryType::QUERY_CONDITION);
          tdb::pmr::vector<BitmapType> combination_op_bitmap(
              result_bitmap_size, 1, resource);

          for (const auto& child : node->get_children()) {
            apply_tree_sparse<BitmapType>(
                child,
                params,
                result_tile,
                std::multiplies<BitmapType>(),
                combination_op_bitmap);
          }
          for (size_t c = 0; c < result_bitmap_size; ++c) {
            result_bitmap[c] |= combination_op_bitmap[c];
          }
        }
      } break;

        /*
         * cl1(q; a) \/ cl2(q; a) = a /\ (cl1(q; 1) \/ cl2(q; 1))
         *                        = a /\ (cl1'(q; 0) \/ cl2'(q; 0))
         *                        = a /\ (cl1'(q; cl2'(q; 0)))
         */
      case QueryConditionCombinationOp::OR: {
        auto resource = params.GetMemoryTracker()->get_resource(
            MemoryType::QUERY_CONDITION);
        tdb::pmr::vector<BitmapType> combination_op_bitmap(
            result_bitmap_size, 0, resource);

        for (const auto& child : node->get_children()) {
          apply_tree_sparse<BitmapType>(
              child,
              params,
              result_tile,
              QCMax<BitmapType>(),
              combination_op_bitmap);
        }

        for (size_t c = 0; c < result_bitmap_size; ++c) {
          result_bitmap[c] *= combination_op_bitmap[c];
        }
      } break;
      case QueryConditionCombinationOp::NOT: {
        throw std::runtime_error(
            "Query condition NOT operator is not currently supported.");
      } break;
      default: {
        throw std::logic_error(
            "Invalid combination operator when applying query condition.");
      }
    }
  }
}

template <typename BitmapType>
Status QueryCondition::apply_sparse(
    const QueryCondition::Params& params,
    const ResultTile& result_tile,
    std::span<BitmapType> result_bitmap) {
#ifdef HAVE_RUST
  if (datafusion_.has_value()) {
    try {
      datafusion_.value().apply(params, result_tile, result_bitmap);
    } catch (const ::rust::Error& e) {
      throw std::logic_error(
          "Unexpected error evaluating expression: " + std::string(e.what()));
    }
  } else {
    apply_tree_sparse<BitmapType>(
        tree_,
        params,
        result_tile,
        std::multiplies<BitmapType>(),
        result_bitmap);
  }
#else
  apply_tree_sparse<BitmapType>(
      tree_, params, result_tile, std::multiplies<BitmapType>(), result_bitmap);
#endif

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

#ifdef HAVE_RUST
template <typename BitmapType>
void QueryCondition::Datafusion::apply(
    const QueryCondition::Params&,
    const ResultTile& result_tile,
    std::span<BitmapType> result_bitmap) const {
  const auto arrow =
      tiledb::oxidize::arrow::record_batch::create(*schema_, result_tile);
  const auto predicate_eval = expr_->evaluate(*arrow);
  static_assert(
      std::is_same_v<BitmapType, uint8_t> ||
      std::is_same_v<BitmapType, uint64_t>);
  if constexpr (std::is_same_v<BitmapType, uint8_t>) {
    const auto predicate_out_u8 = predicate_eval->cast_to(Datatype::UINT8);
    const auto bitmap = predicate_out_u8->values_u8();
    if (predicate_out_u8->is_scalar() && bitmap.empty()) {
      // all NULLs
      for (auto& result : result_bitmap) {
        result = 0;
      }
    } else if (predicate_out_u8->is_scalar()) {
      // all the same value
      for (auto& result : result_bitmap) {
        result = result * bitmap[0];
      }
    } else if (bitmap.size() == result_bitmap.size()) {
      for (uint64_t i = 0; i < bitmap.size(); i++) {
        result_bitmap[i] *= bitmap[i];
      }
    } else {
      throw std::logic_error(
          "Expression evaluation bitmap has unexpected size");
    }
  } else {
    const auto predicate_out_u64 = predicate_eval->cast_to(Datatype::UINT64);
    const auto bitmap = predicate_out_u64->values_u64();
    if (predicate_out_u64->is_scalar() && bitmap.empty()) {
      // all NULLs
      for (auto& result : result_bitmap) {
        result = 0;
      }
    } else if (predicate_out_u64->is_scalar()) {
      // all the same value
      for (auto& result : result_bitmap) {
        result = result * bitmap[0];
      }
    } else if (bitmap.size() == result_bitmap.size()) {
      for (uint64_t i = 0; i < result_bitmap.size(); i++) {
        result_bitmap[i] *= bitmap[i];
      }
    } else {
      throw std::logic_error(
          "Expression evaluation bitmap has unexpected size");
    }
  }
}
#endif

// Explicit template instantiations.
template Status QueryCondition::apply_sparse<uint8_t>(
    const QueryCondition::Params&, const ResultTile&, std::span<uint8_t>);
template Status QueryCondition::apply_sparse<uint64_t>(
    const QueryCondition::Params&, const ResultTile&, std::span<uint64_t>);
}  // namespace tiledb::sm
