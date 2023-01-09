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
#include <type_traits>

using namespace tiledb::common;

namespace tiledb::sm {

QueryCondition::QueryCondition(const std::string& condition_marker)
    : condition_marker_(condition_marker)
    , condition_index_(0) {
}

QueryCondition::QueryCondition(tdb_unique_ptr<tiledb::sm::ASTNode>&& tree) noexcept
    : tree_(std::move(tree))  {
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

QueryCondition::QueryCondition(QueryCondition&& rhs) noexcept
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

QueryCondition& QueryCondition::operator=(QueryCondition&& rhs) noexcept {
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

template <typename Cmp>
struct QueryCondition::BinaryCmpNullChecks<
    char*,
    Cmp,
    typename std::enable_if_t<(
        (!(std::is_same_v<Cmp, std::equal_to<char*>> ||
           std::is_same_v<Cmp, std::not_equal_to<char*>>)))>> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if (lhs == nullptr) {
      return false;
    }
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return Cmp{}(reinterpret_cast<char* const>(cmp), 0);
    }

    return Cmp{}(
        reinterpret_cast<char* const>(lhs_size),
        reinterpret_cast<char* const>(rhs_size));
  }
};

/** Partial template specialization */
template <typename Cmp>
struct QueryCondition::BinaryCmpNullChecks<
    char*,
    Cmp,
    typename std::enable_if_t<(
        (std::is_same_v<Cmp, std::equal_to<char*>> ||
         std::is_same_v<Cmp, std::not_equal_to<char*>>))>> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {

  static_assert(std::is_same_v<Cmp, std::equal_to<char*>> ||
                std::is_same_v<Cmp, std::not_equal_to<char*>>);


    if constexpr (std::is_same_v<Cmp, std::equal_to<char*>>) {
      if (lhs == rhs) {
        return true;
      }

      if (lhs == nullptr || rhs == nullptr) {
        return false;
      }

      if (lhs_size != rhs_size) {
        return false;
      }
    } else if constexpr (std::is_same_v<Cmp, std::not_equal_to<char*>>) {
      if (rhs == nullptr && lhs != nullptr) {
        return true;
      }

      if (lhs == nullptr || rhs == nullptr) {
        return false;
      }

      if (lhs_size != rhs_size) {
        return true;
      }

    } else {
      throw std::logic_error("Invalid template specialization");
    }

    return Cmp{}(
        reinterpret_cast<char* const>(strncmp(
            static_cast<const char*>(lhs),
            static_cast<const char*>(rhs),
            lhs_size)), 0);
  }
};

/** Generic */
template <typename T, typename Cmp, typename E>
struct QueryCondition::BinaryCmpNullChecks {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {

    static_assert(!std::is_same_v<T, char*>, "Invalid template specialization");
    static_assert(!std::is_same_v<Cmp, std::equal_to<char*>>,
                  "Invalid template specialization");
    static_assert(!std::is_same_v<Cmp, std::not_equal_to<char*>>,
                  "Invalid template specialization");

    return lhs != nullptr &&
           Cmp{}(*static_cast<const T*>(lhs), *static_cast<const T*>(rhs));
  }
};

template <typename T, typename Cmp>
struct QueryCondition::BinaryCmpNullChecks<
    T,
    Cmp,
    typename std::enable_if_t<(
        (std::is_same_v<Cmp, std::equal_to<T>> ||
         std::is_same_v<
             Cmp,
             std::not_equal_to<T>>)&&(!std::is_same_v<T, char*>))>> {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {

    static_assert(!std::is_same_v<T, char*>, "Invalid template specialization");

    if constexpr (std::is_same_v<Cmp, std::equal_to<T>>) {
      if (lhs == rhs) {
        return true;
      }

      if (lhs == nullptr || rhs == nullptr) {
        return false;
      }
    } else if constexpr (std::is_same_v<Cmp, std::not_equal_to<T>>) {
      if (rhs == nullptr && lhs != nullptr) {
        return true;
      }

      if (lhs == nullptr || rhs == nullptr) {
        return false;
      }
    } else {
      // static_assert(false, "Unsupported comparison operator");
      throw std::runtime_error("Unsupported comparison operator");
    }
    return Cmp{}(*static_cast<const T*>(lhs), *static_cast<const T*>(rhs));
  }
};



template <typename T, typename Op, typename CombinationOp>
void QueryCondition::apply_ast_node(
    const tdb_unique_ptr<ASTNode>& node,
    const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    const ByteVecValue& fill_value,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    typename std::common_type<CombinationOp>::type combination_op,
    std::vector<uint8_t>& result_cell_bitmap) const {
  // print_types(Op{}, CombinationOp{});

  const std::string& field_name = node->get_field_name();
  const void* condition_value_content =
      node->get_condition_value_view().content();
  const size_t condition_value_size = node->get_condition_value_view().size();
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
      const auto f = result_tile->frag_idx();

      // For delete timestamps conditions, if there's no delete metadata or
      // delete condition was already processed, GT condition is always true.
      if (field_name == constants::delete_timestamps &&
          (!fragment_metadata[f]->has_delete_meta() ||
           fragment_metadata[f]->get_processed_conditions_set().count(
               condition_marker_) != 0)) {
        // assert(Op == QueryConditionOp::GT);
        assert((std::same_as<Op, std::greater<T>>));
        for (size_t c = starting_index; c < starting_index + length; ++c) {
          result_cell_bitmap[c] = 1;
        }
        starting_index += length;
        continue;
      } else if (!fragment_metadata[f]->array_schema()->is_field(field_name)) {
        // Requested field does not exist in this result cell - added by
        // evolution.
        for (size_t c = starting_index; c < starting_index + length; ++c) {
          result_cell_bitmap[c] = 0;
        }
        starting_index += length;
        continue;
      }

      const auto tile_tuple = result_tile->tile_tuple(field_name);
      uint8_t* buffer_validity = nullptr;

      if (nullable) {
        const auto& tile_validity = tile_tuple->validity_tile();
        buffer_validity = static_cast<uint8_t*>(tile_validity.data());
      }

      // Start the pending result cell slab at the start position
      // of the current result cell slab.
      uint64_t c = 0;

      if (var_size) {
        const auto& tile = tile_tuple->var_tile();
        const char* buffer = static_cast<char*>(tile.data());
        const uint64_t buffer_size = tile.size();

        const auto& tile_offsets = tile_tuple->fixed_tile();
        const uint64_t* buffer_offsets =
            static_cast<uint64_t*>(tile_offsets.data());
        const uint64_t buffer_offsets_el =
            tile_offsets.size() / constants::cell_var_offset_size;

        // Iterate through each cell in this slab.
        while (c < length) {
          const uint64_t buffer_offset = buffer_offsets[start + c * stride];
          const uint64_t next_cell_offset =
              (start + c * stride + 1 < buffer_offsets_el) ?
                  buffer_offsets[start + c * stride + 1] :
                  buffer_size;
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
          const char* buffer = static_cast<char*>(tile.data());
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
    std::vector<uint8_t>& result_cell_bitmap) const {
  auto g = [&, *this](auto ConditionOp) {
    return apply_ast_node<T, decltype(ConditionOp), CombinationOp>(
        node,
        fragment_metadata,
        stride,
        var_size,
        nullable,
        fill_value,
        result_cell_slabs,
        combination_op,
        result_cell_bitmap);
  };

  switch (node->get_op()) {
    case QueryConditionOp::LT:
      g(std::less<T>{});
      break;
    case QueryConditionOp::LE:
      g(std::less_equal<T>{});
      break;
    case QueryConditionOp::GT:
      g(std::greater<T>{});
      break;
    case QueryConditionOp::GE:
      g(std::greater_equal<T>{});
      break;
    case QueryConditionOp::EQ:
      g(std::equal_to<T>{});
      break;
    case QueryConditionOp::NE:
      g(std::not_equal_to<T>{});
      break;
    default:
      throw std::runtime_error(
          "QueryCondition::apply_ast_node: Cannot perform query comparison; "
          "Unknown query condition operator.");
  }
}

template <typename CombinationOp>
void QueryCondition::apply_ast_node(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
    const uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CombinationOp combination_op,
    std::vector<uint8_t>& result_cell_bitmap) const {
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

  auto g = [&, *this](auto T) {
    return apply_ast_node<decltype(T), CombinationOp>(
        node,
        fragment_metadata,
        stride,
        var_size,
        nullable,
        fill_value,
        result_cell_slabs,
        combination_op,
        result_cell_bitmap);
  };

  switch (type) {
    case Datatype::INT8:
      g(int8_t{});
      break;
    case Datatype::UINT8:
      g(uint8_t{});
      break;
    case Datatype::INT16:
      g(int16_t{});
      break;
    case Datatype::UINT16:
      g(uint16_t{});
      break;
    case Datatype::INT32:
      g(int32_t{});
      break;
    case Datatype::UINT32:
      g(uint32_t{});
      break;
    case Datatype::INT64:
      g(int64_t{});
      break;
    case Datatype::UINT64:
      g(uint64_t{});
      break;
    case Datatype::FLOAT32:
      g(float{});
      break;
    case Datatype::FLOAT64:
      g(double{});
      break;
    case Datatype::STRING_ASCII:
      g((char*){});
      break;
    case Datatype::CHAR: {
      if (var_size) {
        g((char*){});
      } else {
        g(char{});
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
      g(int64_t{});
      break;
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    default:
      throw std::runtime_error(
          "QueryCondition::apply_ast_node: Cannot perform query comparison; "
          "Unsupported query "
          "conditional type on " +
          node->get_field_name());
  }

  return;
}

template <typename CombinationOp>
void QueryCondition::apply_tree(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CombinationOp combination_op,
    std::vector<uint8_t>& result_cell_bitmap) const {
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
                array_schema,
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
          std::vector<uint8_t> combination_op_bitmap(result_bitmap_size, 1);

          for (const auto& child : node->get_children()) {
            apply_tree(
                child,
                array_schema,
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
        std::vector<uint8_t> combination_op_bitmap(result_bitmap_size, 0);

        for (const auto& child : node->get_children()) {
          apply_tree(
              child,
              array_schema,
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
    const ArraySchema& array_schema,
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

  std::vector<uint8_t> result_cell_bitmap(total_lengths, 1);
  apply_tree(
      tree_,
      array_schema,
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

template <typename T, typename Op, typename CombinationOp>
void QueryCondition::apply_ast_node_dense(
    const tdb_unique_ptr<ASTNode>& node,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    typename std::common_type_t<CombinationOp> combination_op,
    span<uint8_t> result_buffer) const {
  const std::string& field_name = node->get_field_name();
  const void* condition_value_content =
      node->get_condition_value_view().content();
  const size_t condition_value_size = node->get_condition_value_view().size();

  // Get the nullable buffer.
  const auto tile_tuple = result_tile->tile_tuple(field_name);
  uint8_t* buffer_validity = nullptr;
  if (nullable) {
    const auto& tile_validity = tile_tuple->validity_tile();
    buffer_validity = static_cast<uint8_t*>(tile_validity.data()) + src_cell;
  }

  if (var_size) {
    // Get var data buffer and tile offsets buffer.
    const auto& tile = tile_tuple->var_tile();
    const char* buffer = static_cast<char*>(tile.data());
    const uint64_t buffer_size = tile.size();

    const auto& tile_offsets = tile_tuple->fixed_tile();
    const uint64_t* buffer_offsets =
        static_cast<uint64_t*>(tile_offsets.data()) + src_cell;
    const uint64_t buffer_offsets_el =
        tile_offsets.size() / constants::cell_var_offset_size;

    // Iterate through each cell in this slab.
    for (uint64_t c = 0; c < result_buffer.size(); ++c) {
      // Check the previous cell here, which breaks vectorization but as this
      // is string data requiring a strcmp which cannot be vectorized, this is
      // ok.
      const uint64_t buffer_offset = buffer_offsets[start + c * stride];
      const uint64_t next_cell_offset =
          (start + c * stride + 1 < buffer_offsets_el) ?
              buffer_offsets[start + c * stride + 1] :
              buffer_size;
      const uint64_t cell_size = next_cell_offset - buffer_offset;

      // Get the cell value.
      const void* const cell_value = buffer + buffer_offset;

      // Compare the cell value against the value in the value node.
      const bool cmp = BinaryCmp<T, Op>::cmp(
          cell_value, cell_size, condition_value_content, condition_value_size);

      // Set the value.
      bool buffer_validity_val = buffer_validity == nullptr ?
                                     true :
                                     buffer_validity[start + c * stride] != 0;
      result_buffer[c] =
          combination_op(result_buffer[c], (uint8_t)cmp && buffer_validity_val);
    }
  } else {
    // Get the fixed size data buffers.
    const auto& tile = tile_tuple->fixed_tile();
    const char* buffer = static_cast<char*>(tile.data());
    const uint64_t cell_size = tile.cell_size();
    uint64_t buffer_offset = (start + src_cell) * cell_size;
    const uint64_t buffer_offset_inc = stride * cell_size;

    // Iterate through each cell in this slab.
    for (uint64_t c = 0; c < result_buffer.size(); ++c) {
      // Get the cell value.
      const void* const cell_value = buffer + buffer_offset;
      buffer_offset += buffer_offset_inc;

      // Compare the cell value against the value in the value node.
      const bool cmp = BinaryCmp<T, Op>::cmp(
          cell_value, cell_size, condition_value_content, condition_value_size);

      // Set the value.
      bool buffer_validity_val = buffer_validity == nullptr ?
                                     true :
                                     buffer_validity[start + c * stride] != 0;
      result_buffer[c] =
          combination_op(result_buffer[c], (uint8_t)cmp && buffer_validity_val);
    }
  }
}

template <typename T, typename CombinationOp>
void QueryCondition::apply_ast_node_dense(
    const tdb_unique_ptr<ASTNode>& node,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    const bool var_size,
    const bool nullable,
    CombinationOp combination_op,
    span<uint8_t> result_buffer) const {
  auto g = [&, *this](auto Condition) {
    return apply_ast_node_dense<T, decltype(Condition), CombinationOp>(
        node,
        result_tile,
        start,
        src_cell,
        stride,
        var_size,
        nullable,
        combination_op,
        result_buffer);
  };

  switch (node->get_op()) {
    case QueryConditionOp::LT:
      g(std::less<T>{});
      break;
    case QueryConditionOp::LE:
      g(std::less_equal<T>{});
      break;
    case QueryConditionOp::GT:
      g(std::greater<T>{});
      break;
    case QueryConditionOp::GE:
      g(std::greater_equal<T>{});
      break;
    case QueryConditionOp::EQ:
      g(std::equal_to<T>{});
      break;
    case QueryConditionOp::NE:
      g(std::not_equal_to<T>{});
      break;
    default:
      throw std::runtime_error(
          "Cannot perform query comparison; Unknown query condition operator");
  }
}

template <typename CombinationOp>
void QueryCondition::apply_ast_node_dense(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    CombinationOp combination_op,
    span<uint8_t> result_buffer) const {
  const auto attribute = array_schema.attribute(node->get_field_name());
  if (!attribute) {
    throw std::runtime_error("Unknown attribute " + node->get_field_name());
  }

  const bool var_size = attribute->var_size();
  const bool nullable = attribute->nullable();

  // Process the validity buffer now.
  if (nullable && node->get_condition_value_view().content() == nullptr) {
    const auto tile_tuple = result_tile->tile_tuple(node->get_field_name());
    const auto& tile_validity = tile_tuple->validity_tile();
    const auto buffer_validity =
        static_cast<uint8_t*>(tile_validity.data()) + src_cell;

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

  auto g = [&, *this](auto T) {
    return apply_ast_node_dense<decltype(T), CombinationOp>(
        node,
        result_tile,
        start,
        src_cell,
        stride,
        var_size,
        nullable,
        combination_op,
        result_buffer);
  };

  switch (attribute->type()) {
    case Datatype::INT8:
      g(int8_t{});
      break;
    case Datatype::BOOL:
    case Datatype::UINT8:
      g(uint8_t{});
      break;
    case Datatype::INT16:
      g(int16_t{});
      break;
    case Datatype::UINT16:
      g(uint16_t{});
      break;
    case Datatype::INT32:
      g(int32_t{});
      break;
    case Datatype::UINT32:
      g(uint32_t{});
      break;
    case Datatype::INT64:
      g(int64_t{});
      break;
    case Datatype::UINT64:
      g(uint64_t{});
      break;
    case Datatype::FLOAT32:
      g(float{});
      break;
    case Datatype::FLOAT64:
      g(double{});
      break;
    case Datatype::STRING_ASCII:
      g((char*){});
      break;
    case Datatype::CHAR: {
      if (var_size) {
        g((char*){});
      } else {
        g(char{});
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
      g(int64_t{});
      break;
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    default:
      throw std::runtime_error(
          "Cannot perform query comparison; Unsupported query conditional type "
          "on " +
          node->get_field_name());
  }
}

template <typename CombinationOp>
void QueryCondition::apply_tree_dense(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    ResultTile* result_tile,
    const uint64_t start,
    const uint64_t src_cell,
    const uint64_t stride,
    CombinationOp combination_op,
    span<uint8_t> result_buffer) const {
  if (!node->is_expr()) {
    apply_ast_node_dense(
        node,
        array_schema,
        result_tile,
        start,
        src_cell,
        stride,
        combination_op,
        result_buffer);
  } else {
    const auto result_buffer_size = result_buffer.size();
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
            apply_tree_dense(
                child,
                array_schema,
                result_tile,
                start,
                src_cell,
                stride,
                std::logical_and<uint8_t>(),
                result_buffer);
          }

          // Handle the cl'(q, a) case
        } else if constexpr (std::is_same_v<
                                 CombinationOp,
                                 std::logical_or<uint8_t>>) {
          std::vector<uint8_t> combination_op_bitmap(result_buffer_size, 1);
          span<uint8_t> combination_op_span(
              combination_op_bitmap.data(), result_buffer_size);

          for (const auto& child : node->get_children()) {
            apply_tree_dense(
                child,
                array_schema,
                result_tile,
                start,
                src_cell,
                stride,
                std::logical_and<uint8_t>(),
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
        std::vector<uint8_t> combination_op_bitmap(result_buffer_size, 0);
        span<uint8_t> combination_op_span(
            combination_op_bitmap.data(), result_buffer_size);
        for (const auto& child : node->get_children()) {
          apply_tree_dense(
              child,
              array_schema,
              result_tile,
              start,
              src_cell,
              stride,
              std::logical_or<uint8_t>(),
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

  span<uint8_t> result_span(result_buffer + start, length);
  apply_tree_dense(
      tree_,
      array_schema,
      result_tile,
      start,
      src_cell,
      stride,
      std::logical_and<uint8_t>(),
      result_span);
  return Status::Ok();
}


template <typename Cmp, typename E>
struct QueryCondition::BinaryCmp<char*, Cmp, E> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    const size_t min_size = std::min<size_t>(lhs_size, rhs_size);
    const int cmp = strncmp(
        static_cast<const char*>(lhs), static_cast<const char*>(rhs), min_size);
    if (cmp != 0) {
      return Cmp{}(reinterpret_cast<char* const>(cmp), 0);
    }

    return Cmp{}(
        reinterpret_cast<char* const>(lhs_size),
        reinterpret_cast<char* const>(rhs_size));
  }
};

template <typename Cmp>
struct QueryCondition::BinaryCmp<
    char*,
    Cmp,
    typename std::enable_if_t<(
        std::same_as<Cmp, std::equal_to<char*>> ||
        std::same_as<Cmp, std::not_equal_to<char*>>)>> {
  static inline bool cmp(
      const void* lhs, uint64_t lhs_size, const void* rhs, uint64_t rhs_size) {
    if constexpr (std::same_as<Cmp, std::equal_to<char*>>) {
      if (lhs_size != rhs_size) {
        return false;
      }
    } else if constexpr (std::same_as<Cmp, std::not_equal_to<char*>>) {
      if (lhs_size != rhs_size) {
        return true;
      }
    } else {
      throw std::logic_error(
          "Invalid comparison operator for string comparison.");
    }
    return Cmp{}(
        reinterpret_cast<char* const>(strncmp(
            static_cast<const char*>(lhs),
            static_cast<const char*>(rhs),
            lhs_size)),
        0);
  }
};

/** Generic */
template <typename T, typename Cmp, typename E>
struct QueryCondition::BinaryCmp {
  static inline bool cmp(const void* lhs, uint64_t, const void* rhs, uint64_t) {
    static_assert(!std::same_as<T, char*>);
    return lhs != nullptr &&
           Cmp{}(*static_cast<const T*>(lhs), *static_cast<const T*>(rhs));
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
    typename Op,
    typename BitmapType,
    typename CombinationOp,
    typename nullable>
void QueryCondition::apply_ast_node_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    ResultTile& result_tile,
    const bool var_size,
    CombinationOp combination_op,
    std::vector<BitmapType>& result_bitmap) const {
  const auto tile_tuple = result_tile.tile_tuple(node->get_field_name());
  const void* condition_value_content =
      node->get_condition_value_view().content();
  const size_t condition_value_size = node->get_condition_value_view().size();
  uint8_t* buffer_validity = nullptr;

  // Check if the combination op = OR and the attribute is nullable.
  if constexpr (
      std::is_same_v<CombinationOp, QCMax<BitmapType>> && nullable::value) {
    const auto& tile_validity = tile_tuple->validity_tile();
    buffer_validity = static_cast<uint8_t*>(tile_validity.data());
  }

  if (var_size) {
    // Get var data buffer and tile offsets buffer.
    const auto& tile = tile_tuple->var_tile();
    const char* buffer = static_cast<char*>(tile.data());
    const uint64_t buffer_size = tile.size();

    const auto& tile_offsets = tile_tuple->fixed_tile();
    const uint64_t* buffer_offsets =
        static_cast<uint64_t*>(tile_offsets.data());
    const uint64_t buffer_offsets_el =
        tile_offsets.size() / constants::cell_var_offset_size;

    // Iterate through each cell.
    for (uint64_t c = 0; c < buffer_offsets_el; ++c) {
      // Check the previous cell here, which breaks vectorization but as this
      // is string data requiring a strcmp which cannot be vectorized, this is
      // ok.
      const uint64_t buffer_offset = buffer_offsets[c];
      const uint64_t next_cell_offset =
          (c + 1 < buffer_offsets_el) ? buffer_offsets[c + 1] : buffer_size;
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
    const char* buffer = static_cast<char*>(tile.data());
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
    ResultTile& result_tile,
    const bool var_size,
    CombinationOp combination_op,
    std::vector<BitmapType>& result_bitmap) const {
  auto g = [&, *this](auto Condition) {
    return apply_ast_node_sparse<
        T,
        decltype(Condition),
        BitmapType,
        CombinationOp,
        nullable>(node, result_tile, var_size, combination_op, result_bitmap);
  };

  switch (node->get_op()) {
    case QueryConditionOp::LT:
      g(std::less<T>{});
      break;
    case QueryConditionOp::LE:
      g(std::less_equal<T>{});
      break;
    case QueryConditionOp::GT:
      g(std::greater<T>{});
      break;
    case QueryConditionOp::GE:
      g(std::greater_equal<T>{});
      break;
    case QueryConditionOp::EQ:
      g(std::equal_to<T>{});
      break;
    case QueryConditionOp::NE:
      g(std::not_equal_to<T>{});
      break;
    default:
      throw std::runtime_error(
          "Cannot perform query comparison; Unknown query condition operator.");
  }
}

template <typename T, typename BitmapType, typename CombinationOp>
void QueryCondition::apply_ast_node_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    ResultTile& result_tile,
    const bool var_size,
    const bool nullable,
    std::common_type_t<CombinationOp> combination_op,
    std::vector<BitmapType>& result_bitmap) const {
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
    ResultTile& result_tile,
    CombinationOp combination_op,
    std::vector<BitmapType>& result_bitmap) const {
  std::string node_field_name = node->get_field_name();
  if (!array_schema.is_field(node_field_name)) {
    std::fill(result_bitmap.begin(), result_bitmap.end(), 0);
    return;
  }

  const auto nullable = array_schema.is_nullable(node_field_name);
  const auto var_size = array_schema.var_size(node_field_name);
  const auto type = array_schema.type(node_field_name);

  // Process the validity buffer now.
  if (nullable) {
    const auto tile_tuple = result_tile.tile_tuple(node_field_name);
    const auto& tile_validity = tile_tuple->validity_tile();
    const auto buffer_validity = static_cast<uint8_t*>(tile_validity.data());
    const auto cell_num = result_tile.cell_num();

    if (node->get_condition_value_view().content() == nullptr) {
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
      // When the combination op is AND, turn off bitmap values for null cells.
      for (uint64_t c = 0; c < cell_num; c++) {
        result_bitmap[c] *= buffer_validity[c] != 0;
      }
    }
  }

  auto g = [&, *this](auto T) {
    return apply_ast_node_sparse<decltype(T), BitmapType, CombinationOp>(
        node, result_tile, var_size, nullable, combination_op, result_bitmap);
  };

  switch (type) {
    case Datatype::INT8:
      g(int8_t{});
      break;
    case Datatype::BOOL:
    case Datatype::UINT8:
      g(uint8_t{});
      break;
    case Datatype::INT16:
      g(int16_t{});
      break;
    case Datatype::UINT16:
      g(uint16_t{});
      break;
    case Datatype::INT32:
      g(int32_t{});
      break;
    case Datatype::UINT32:
      g(uint32_t{});
      break;
    case Datatype::INT64:
      g(int64_t{});
      break;
    case Datatype::UINT64:
      g(uint64_t{});
      break;
    case Datatype::FLOAT32:
      g(float{});
      break;
    case Datatype::FLOAT64:
      g(double{});
      break;
    case Datatype::STRING_ASCII:
      g((char*){});
      break;
    case Datatype::CHAR: {
      if (var_size) {
        g((char*){});
      } else {
        g((char){});
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
      g(int64_t{});
      break;
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    default:
      throw std::runtime_error(
          "Cannot perform query comparison; Unsupported query conditional type "
          "on " +
          node->get_field_name());
  }
}

template <typename BitmapType, typename CombinationOp>
void QueryCondition::apply_tree_sparse(
    const tdb_unique_ptr<ASTNode>& node,
    const ArraySchema& array_schema,
    ResultTile& result_tile,
    CombinationOp combination_op,
    std::vector<BitmapType>& result_bitmap) const {
  if (!node->is_expr()) {
    apply_ast_node_sparse<BitmapType, CombinationOp>(
        node, array_schema, result_tile, combination_op, result_bitmap);
  } else {
    const auto result_bitmap_size = result_bitmap.size();
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
        if constexpr (std::is_same_v<
                          CombinationOp,
                          std::multiplies<BitmapType>>) {
          for (const auto& child : node->get_children()) {
            apply_tree_sparse<BitmapType>(
                child,
                array_schema,
                result_tile,
                std::multiplies<BitmapType>(),
                result_bitmap);
          }

          // Handle the cl'(q, a) case.
          // This cases on whether the combination op = OR.
        } else if constexpr (std::is_same_v<CombinationOp, QCMax<BitmapType>>) {
          std::vector<BitmapType> combination_op_bitmap(result_bitmap_size, 1);

          for (const auto& child : node->get_children()) {
            apply_tree_sparse<BitmapType>(
                child,
                array_schema,
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
        std::vector<BitmapType> combination_op_bitmap(result_bitmap_size, 0);

        for (const auto& child : node->get_children()) {
          apply_tree_sparse<BitmapType>(
              child,
              array_schema,
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
    const ArraySchema& array_schema,
    ResultTile& result_tile,
    std::vector<BitmapType>& result_bitmap) {
  apply_tree_sparse<BitmapType>(
      tree_,
      array_schema,
      result_tile,
      std::multiplies<BitmapType>(),
      result_bitmap);

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
}  // namespace tiledb::sm

