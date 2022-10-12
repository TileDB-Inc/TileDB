/**
 * @file query.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines serialization for the Query class
 */

#include <sstream>

// clang-format off
#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/capnp_utils.h"
#endif
// clang-format on

#include "tiledb/sm/query/ast/query_ast.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/buffer/buffer_list.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/hash.h"
#include "tiledb/sm/misc/parse_argument.h"
#include "tiledb/sm/query/deletes_and_updates/deletes.h"
#include "tiledb/sm/query/writers/global_order_writer.h"
#include "tiledb/sm/query/readers/dense_reader.h"
#include "tiledb/sm/query/legacy/reader.h"
#include "tiledb/sm/query/readers/sparse_global_order_reader.h"
#include "tiledb/sm/query/readers/sparse_unordered_with_dups_reader.h"
#include "tiledb/sm/query/writers/writer_base.h"
#include "tiledb/sm/serialization/config.h"
#include "tiledb/sm/serialization/query.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

enum class SerializationContext { CLIENT, SERVER, BACKUP };

shared_ptr<Logger> dummy_logger = make_shared<Logger>(HERE(), "");

Status stats_to_capnp(Stats& stats, capnp::Stats::Builder* stats_builder) {
  // Build counters
  const auto counters = stats.counters();
  if (counters != nullptr && !counters->empty()) {
    auto counters_builder = stats_builder->initCounters();
    auto entries_builder = counters_builder.initEntries(counters->size());
    uint64_t index = 0;
    for (const auto& entry : *counters) {
      entries_builder[index].setKey(entry.first);
      entries_builder[index].setValue(entry.second);
      ++index;
    }
  }

  // Build timers
  const auto timers = stats.timers();
  if (timers != nullptr && !timers->empty()) {
    auto timers_builder = stats_builder->initTimers();
    auto entries_builder = timers_builder.initEntries(timers->size());
    uint64_t index = 0;
    for (const auto& entry : *timers) {
      entries_builder[index].setKey(entry.first);
      entries_builder[index].setValue(entry.second);
      ++index;
    }
  }

  return Status::Ok();
}

Status stats_from_capnp(
    const capnp::Stats::Reader& stats_reader, Stats* stats) {
  if (stats_reader.hasCounters()) {
    auto counters = stats->counters();
    auto counters_reader = stats_reader.getCounters();
    for (const auto entry : counters_reader.getEntries()) {
      (*counters)[std::string(entry.getKey().cStr())] = entry.getValue();
    }
  }

  if (stats_reader.hasTimers()) {
    auto timers = stats->timers();
    auto timers_reader = stats_reader.getTimers();
    for (const auto entry : timers_reader.getEntries()) {
      (*timers)[std::string(entry.getKey().cStr())] = entry.getValue();
    }
  }

  return Status::Ok();
}

Status subarray_to_capnp(
    const ArraySchema& schema,
    const Subarray* subarray,
    capnp::Subarray::Builder* builder) {
  builder->setLayout(layout_str(subarray->layout()));

  const uint32_t dim_num = subarray->dim_num();
  auto ranges_builder = builder->initRanges(dim_num);
  for (uint32_t i = 0; i < dim_num; i++) {
    const auto datatype{schema.dimension_ptr(i)->type()};
    auto range_builder = ranges_builder[i];
    const auto& ranges = subarray->ranges_for_dim(i);
    range_builder.setType(datatype_str(datatype));

    range_builder.setHasDefaultRange(subarray->is_default(i));
    auto range_sizes = range_builder.initBufferSizes(ranges.size());
    auto range_start_sizes = range_builder.initBufferStartSizes(ranges.size());
    // This will copy all of the ranges into one large byte vector
    // Future improvement is to do this in a zero copy manner
    // (kj::ArrayBuilder?)
    auto capnpVector = kj::Vector<uint8_t>();
    uint64_t range_idx = 0;
    for (auto& range : ranges) {
      capnpVector.addAll(kj::ArrayPtr<uint8_t>(
          const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(range.data())),
          range.size()));
      range_sizes.set(range_idx, range.size());
      range_start_sizes.set(range_idx, range.start_size());
      ++range_idx;
    }
    range_builder.setBuffer(capnpVector.asPtr());
  }

  // If stats object exists set its cap'n proto object
  stats::Stats* stats = subarray->stats();
  if (stats != nullptr) {
    auto stats_builder = builder->initStats();
    RETURN_NOT_OK(stats_to_capnp(*stats, &stats_builder));
  }

  if (subarray->relevant_fragments()->size() > 0) {
    auto relevant_fragments_builder =
        builder->initRelevantFragments(subarray->relevant_fragments()->size());
    for (size_t i = 0; i < subarray->relevant_fragments()->size(); ++i) {
      relevant_fragments_builder.set(i, subarray->relevant_fragments()->at(i));
    }
  }

  return Status::Ok();
}

Status subarray_from_capnp(
    const capnp::Subarray::Reader& reader, Subarray* subarray) {
  auto ranges_reader = reader.getRanges();
  uint32_t dim_num = ranges_reader.size();
  for (uint32_t i = 0; i < dim_num; i++) {
    auto range_reader = ranges_reader[i];
    Datatype type = Datatype::UINT8;
    RETURN_NOT_OK(datatype_enum(range_reader.getType(), &type));

    auto data = range_reader.getBuffer();
    auto data_ptr = data.asBytes();
    if (range_reader.hasBufferSizes()) {
      auto buffer_sizes = range_reader.getBufferSizes();
      auto buffer_start_sizes = range_reader.getBufferStartSizes();
      size_t range_count = buffer_sizes.size();
      std::vector<Range> ranges(range_count);
      uint64_t offset = 0;
      for (size_t j = 0; j < range_count; j++) {
        uint64_t range_size = buffer_sizes[j];
        uint64_t range_start_size = buffer_start_sizes[j];
        if (range_start_size != 0) {
          ranges[j] =
              Range(data_ptr.begin() + offset, range_size, range_start_size);
        } else {
          ranges[j] = Range(data_ptr.begin() + offset, range_size);
        }
        offset += range_size;
      }

      RETURN_NOT_OK(subarray->set_ranges_for_dim(i, ranges));

      // Set default indicator
      subarray->set_is_default(i, range_reader.getHasDefaultRange());
    } else {
      // Handle 1.7 style ranges where there is a single range with no sizes
      Range range(data_ptr.begin(), data.size());
      RETURN_NOT_OK(subarray->set_ranges_for_dim(i, {range}));
      subarray->set_is_default(i, range_reader.getHasDefaultRange());
    }
  }

  // If cap'n proto object has stats set it on c++ object
  if (reader.hasStats()) {
    stats::Stats* stats = subarray->stats();
    // We should always have a stats here
    if (stats != nullptr) {
      RETURN_NOT_OK(stats_from_capnp(reader.getStats(), stats));
    }
  }

  if (reader.hasRelevantFragments()) {
    auto relevant_fragments = reader.getRelevantFragments();
    size_t count = relevant_fragments.size();
    subarray->relevant_fragments()->reserve(count);
    for (size_t i = 0; i < count; i++) {
      subarray->relevant_fragments()->emplace_back(relevant_fragments[i]);
    }
  }

  return Status::Ok();
}

Status subarray_partitioner_to_capnp(
    const ArraySchema& schema,
    const SubarrayPartitioner& partitioner,
    capnp::SubarrayPartitioner::Builder* builder) {
  // Subarray
  auto subarray_builder = builder->initSubarray();
  RETURN_NOT_OK(
      subarray_to_capnp(schema, &partitioner.subarray(), &subarray_builder));

  // Per-attr/dim mem budgets
  const auto* budgets = partitioner.get_result_budgets();
  if (!budgets->empty()) {
    auto mem_budgets_builder = builder->initBudget(budgets->size());
    size_t idx = 0;
    for (const auto& pair : (*budgets)) {
      const std::string& name = pair.first;
      auto budget_builder = mem_budgets_builder[idx];
      budget_builder.setAttribute(name);
      auto var_size = schema.var_size(name);

      if (name == constants::coords || !var_size) {
        budget_builder.setOffsetBytes(0);
        budget_builder.setDataBytes(pair.second.size_fixed_);
      } else {
        budget_builder.setOffsetBytes(pair.second.size_fixed_);
        budget_builder.setDataBytes(pair.second.size_var_);
      }

      budget_builder.setValidityBytes(pair.second.size_validity_);

      idx++;
    }
  }

  // Current partition info
  const auto* partition_info = partitioner.current_partition_info();
  // If the array is null that means there is no current partition info
  if (partition_info->partition_.array() != nullptr) {
    auto info_builder = builder->initCurrent();
    auto info_subarray_builder = info_builder.initSubarray();
    RETURN_NOT_OK(subarray_to_capnp(
        schema, &partition_info->partition_, &info_subarray_builder));
    info_builder.setStart(partition_info->start_);
    info_builder.setEnd(partition_info->end_);
    info_builder.setSplitMultiRange(partition_info->split_multi_range_);
  }

  // Partitioner state
  const auto* state = partitioner.state();
  auto state_builder = builder->initState();
  state_builder.setStart(state->start_);
  state_builder.setEnd(state->end_);
  auto single_range_builder =
      state_builder.initSingleRange(state->single_range_.size());
  size_t sr_idx = 0;
  for (const auto& subarray : state->single_range_) {
    auto b = single_range_builder[sr_idx];
    RETURN_NOT_OK(subarray_to_capnp(schema, &subarray, &b));
    sr_idx++;
  }
  auto multi_range_builder =
      state_builder.initMultiRange(state->multi_range_.size());
  size_t m_idx = 0;
  for (const auto& subarray : state->multi_range_) {
    auto b = multi_range_builder[m_idx];
    RETURN_NOT_OK(subarray_to_capnp(schema, &subarray, &b));
    m_idx++;
  }

  // Overall mem budget
  uint64_t mem_budget, mem_budget_var, mem_budget_validity;
  RETURN_NOT_OK(partitioner.get_memory_budget(
      &mem_budget, &mem_budget_var, &mem_budget_validity));
  builder->setMemoryBudget(mem_budget);
  builder->setMemoryBudgetVar(mem_budget_var);
  builder->setMemoryBudgetValidity(mem_budget_validity);

  // If stats object exists set its cap'n proto object
  stats::Stats* stats = partitioner.stats();
  if (stats != nullptr) {
    auto stats_builder = builder->initStats();
    RETURN_NOT_OK(stats_to_capnp(*stats, &stats_builder));
  }

  return Status::Ok();
}

Status subarray_partitioner_from_capnp(
    Stats* query_stats,
    Stats* reader_stats,
    const Config* config,
    const Array* array,
    const capnp::SubarrayPartitioner::Reader& reader,
    SubarrayPartitioner* partitioner,
    ThreadPool* compute_tp,
    const bool& compute_current_tile_overlap) {
  // Get memory budget
  uint64_t memory_budget = 0;
  RETURN_NOT_OK(tiledb::sm::utils::parse::convert(
      Config::SM_MEMORY_BUDGET, &memory_budget));
  uint64_t memory_budget_var = 0;
  RETURN_NOT_OK(tiledb::sm::utils::parse::convert(
      Config::SM_MEMORY_BUDGET_VAR, &memory_budget_var));
  uint64_t memory_budget_validity = 0;

  // Get subarray layout first
  Layout layout = Layout::ROW_MAJOR;
  auto subarray_reader = reader.getSubarray();
  RETURN_NOT_OK(layout_enum(subarray_reader.getLayout(), &layout));

  // Subarray, which is used to initialize the partitioner.
  Subarray subarray(array, layout, query_stats, dummy_logger, false);
  RETURN_NOT_OK(subarray_from_capnp(reader.getSubarray(), &subarray));
  *partitioner = SubarrayPartitioner(
      config,
      subarray,
      memory_budget,
      memory_budget_var,
      memory_budget_validity,
      compute_tp,
      reader_stats,
      dummy_logger);

  // Per-attr mem budgets
  if (reader.hasBudget()) {
    const auto& schema = array->array_schema_latest();
    auto mem_budgets_reader = reader.getBudget();
    auto num_attrs = mem_budgets_reader.size();
    for (size_t i = 0; i < num_attrs; i++) {
      auto mem_budget_reader = mem_budgets_reader[i];
      std::string attr_name = mem_budget_reader.getAttribute();
      auto var_size = schema.var_size(attr_name);
      auto nullable = schema.is_nullable(attr_name);

      if (attr_name == constants::coords || !var_size) {
        if (nullable) {
          RETURN_NOT_OK(partitioner->set_result_budget_nullable(
              attr_name.c_str(),
              mem_budget_reader.getDataBytes(),
              mem_budget_reader.getValidityBytes()));
        } else {
          RETURN_NOT_OK(partitioner->set_result_budget(
              attr_name.c_str(), mem_budget_reader.getDataBytes()));
        }
      } else {
        if (nullable) {
          RETURN_NOT_OK(partitioner->set_result_budget_nullable(
              attr_name.c_str(),
              mem_budget_reader.getOffsetBytes(),
              mem_budget_reader.getDataBytes(),
              mem_budget_reader.getValidityBytes()));
        } else {
          RETURN_NOT_OK(partitioner->set_result_budget(
              attr_name.c_str(),
              mem_budget_reader.getOffsetBytes(),
              mem_budget_reader.getDataBytes()));
        }
      }
    }
  }

  // Current partition info
  if (reader.hasCurrent()) {
    auto partition_info_reader = reader.getCurrent();
    auto* partition_info = partitioner->current_partition_info();
    partition_info->start_ = partition_info_reader.getStart();
    partition_info->end_ = partition_info_reader.getEnd();
    partition_info->split_multi_range_ =
        partition_info_reader.getSplitMultiRange();
    partition_info->partition_ =
        Subarray(array, layout, query_stats, dummy_logger, false);
    RETURN_NOT_OK(subarray_from_capnp(
        partition_info_reader.getSubarray(), &partition_info->partition_));

    if (compute_current_tile_overlap) {
      partition_info->partition_.precompute_tile_overlap(
          partition_info->start_,
          partition_info->end_,
          config,
          compute_tp,
          true);
    }
  }

  // Partitioner state
  auto state_reader = reader.getState();
  auto* state = partitioner->state();
  state->start_ = state_reader.getStart();
  state->end_ = state_reader.getEnd();
  auto sr_reader = state_reader.getSingleRange();
  const unsigned num_sr = sr_reader.size();
  for (unsigned i = 0; i < num_sr; i++) {
    auto subarray_reader_ = sr_reader[i];
    state->single_range_.emplace_back(
        array, layout, query_stats, dummy_logger, false);
    Subarray& subarray_ = state->single_range_.back();
    RETURN_NOT_OK(subarray_from_capnp(subarray_reader_, &subarray_));
  }
  auto m_reader = state_reader.getMultiRange();
  const unsigned num_m = m_reader.size();
  for (unsigned i = 0; i < num_m; i++) {
    auto subarray_reader_ = m_reader[i];
    state->multi_range_.emplace_back(
        array, layout, query_stats, dummy_logger, false);
    Subarray& subarray_ = state->multi_range_.back();
    RETURN_NOT_OK(subarray_from_capnp(subarray_reader_, &subarray_));
  }

  // Overall mem budget
  RETURN_NOT_OK(partitioner->set_memory_budget(
      reader.getMemoryBudget(),
      reader.getMemoryBudgetVar(),
      reader.getMemoryBudgetValidity()));

  // If cap'n proto object has stats set it on c++ object
  if (reader.hasStats()) {
    auto stats = partitioner->stats();
    // We should always have stats
    if (stats != nullptr) {
      RETURN_NOT_OK(stats_from_capnp(reader.getStats(), stats));
    }
  }

  return Status::Ok();
}

Status read_state_to_capnp(
    const ArraySchema& schema,
    const Reader& reader,
    capnp::QueryReader::Builder* builder) {
  auto read_state = reader.read_state();
  auto read_state_builder = builder->initReadState();
  read_state_builder.setOverflowed(read_state->overflowed_);
  read_state_builder.setUnsplittable(read_state->unsplittable_);
  read_state_builder.setInitialized(read_state->initialized_);

  if (read_state->initialized_) {
    auto partitioner_builder = read_state_builder.initSubarrayPartitioner();
    RETURN_NOT_OK(subarray_partitioner_to_capnp(
        schema, read_state->partitioner_, &partitioner_builder));
  }

  return Status::Ok();
}

Status index_read_state_to_capnp(
    const SparseIndexReaderBase::ReadState* read_state,
    capnp::ReaderIndex::Builder* builder) {
  auto read_state_builder = builder->initReadState();

  read_state_builder.setDoneAddingResultTiles(
      read_state->done_adding_result_tiles_);

  auto frag_tile_idx_builder =
      read_state_builder.initFragTileIdx(read_state->frag_idx_.size());
  for (size_t i = 0; i < read_state->frag_idx_.size(); ++i) {
    frag_tile_idx_builder[i].setTileIdx(read_state->frag_idx_[i].tile_idx_);
    frag_tile_idx_builder[i].setCellIdx(read_state->frag_idx_[i].cell_idx_);
  }

  return Status::Ok();
}

Status dense_read_state_to_capnp(
    const ArraySchema& schema,
    const DenseReader& reader,
    capnp::QueryReader::Builder* builder) {
  auto read_state = reader.read_state();
  auto read_state_builder = builder->initReadState();
  read_state_builder.setOverflowed(read_state->overflowed_);
  read_state_builder.setUnsplittable(read_state->unsplittable_);
  read_state_builder.setInitialized(read_state->initialized_);

  if (read_state->initialized_) {
    auto partitioner_builder = read_state_builder.initSubarrayPartitioner();
    RETURN_NOT_OK(subarray_partitioner_to_capnp(
        schema, read_state->partitioner_, &partitioner_builder));
  }

  return Status::Ok();
}

Status read_state_from_capnp(
    const Array* array,
    const capnp::ReadState::Reader& read_state_reader,
    Query* query,
    Reader* reader,
    ThreadPool* compute_tp) {
  auto read_state = reader->read_state();

  read_state->overflowed_ = read_state_reader.getOverflowed();
  read_state->unsplittable_ = read_state_reader.getUnsplittable();
  read_state->initialized_ = read_state_reader.getInitialized();

  // Subarray partitioner
  if (read_state_reader.hasSubarrayPartitioner()) {
    RETURN_NOT_OK(subarray_partitioner_from_capnp(
        query->stats(),
        reader->stats(),
        query->config(),
        array,
        read_state_reader.getSubarrayPartitioner(),
        &read_state->partitioner_,
        compute_tp,
        // If the current partition is unsplittable, this means we need to make
        // sure the tile_overlap for the current is computed because we won't go
        // to the next partition
        read_state->unsplittable_));
  }

  return Status::Ok();
}

Status index_read_state_from_capnp(
    const ArraySchema& schema,
    const capnp::ReadStateIndex::Reader& read_state_reader,
    SparseIndexReaderBase* reader) {
  auto read_state = reader->read_state();

  read_state->done_adding_result_tiles_ =
      read_state_reader.getDoneAddingResultTiles();

  assert(read_state_reader.hasFragTileIdx());
  read_state->frag_idx_.clear();
  for (const auto rcs : read_state_reader.getFragTileIdx()) {
    auto tile_idx = rcs.getTileIdx();
    auto cell_idx = rcs.getCellIdx();

    read_state->frag_idx_.emplace_back(tile_idx, cell_idx);
  }

  return Status::Ok();
}

Status dense_read_state_from_capnp(
    const Array* array,
    const capnp::ReadState::Reader& read_state_reader,
    Query* query,
    DenseReader* reader,
    ThreadPool* compute_tp) {
  auto read_state = reader->read_state();

  read_state->overflowed_ = read_state_reader.getOverflowed();
  read_state->unsplittable_ = read_state_reader.getUnsplittable();
  read_state->initialized_ = read_state_reader.getInitialized();

  // Subarray partitioner
  if (read_state_reader.hasSubarrayPartitioner()) {
    RETURN_NOT_OK(subarray_partitioner_from_capnp(
        query->stats(),
        reader->stats(),
        query->config(),
        array,
        read_state_reader.getSubarrayPartitioner(),
        &read_state->partitioner_,
        compute_tp,
        // If the current partition is unsplittable, this means we need to make
        // sure the tile_overlap for the current is computed because we won't go
        // to the next partition
        read_state->unsplittable_));
  }

  return Status::Ok();
}

/**
 * @brief Validation function for the field name of an AST node.
 *
 * @param field_name Query Condition AST node field name.
 */
void ensure_qc_field_name_is_valid(const std::string& field_name) {
  if (field_name == "") {
    throw std::runtime_error(
        "Invalid Query Condition field name " + field_name);
  }
}

/**
 * @brief Validation function for the condition value of an AST node.
 *
 * @param data Query Condition AST node condition value byte vector start.
 * @param size The number of bytes in the byte vector.
 */
void ensure_qc_condition_value_is_valid(const void* data, size_t size) {
  if (size != 0 && data == nullptr) {
    // Getting the address of data in string form.
    std::ostringstream address;
    address << data;
    std::string address_name = address.str();
    throw std::runtime_error(
        "Invalid Query Condition condition value " + address_name +
        " with size " + std::to_string(size));
  }
}

/**
 * @brief Validation function for the capnp representation of the
 * condition value of an AST node.
 *
 * @param vec The vector representing the Query Condition AST node
 * condition vector.
 */
void ensure_qc_capnp_condition_value_is_valid(const kj::Vector<uint8_t>& vec) {
  ensure_qc_condition_value_is_valid(vec.begin(), vec.size());
}

static Status condition_ast_to_capnp(
    const tdb_unique_ptr<ASTNode>& node, capnp::ASTNode::Builder* ast_builder) {
  if (!node->is_expr()) {
    // Store the boolean expression tag.
    ast_builder->setIsExpression(false);

    // Validate and store the field name.
    const std::string field_name = node->get_field_name();
    ensure_qc_field_name_is_valid(field_name);
    ast_builder->setFieldName(field_name);

    // Copy the condition value into a capnp vector of bytes.
    const UntypedDatumView& value = node->get_condition_value_view();
    auto capnpValue = kj::Vector<uint8_t>();
    capnpValue.addAll(kj::ArrayPtr<uint8_t>(
        const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(value.content())),
        value.size()));

    // Validate and store the condition value vector of bytes.
    ensure_qc_capnp_condition_value_is_valid(capnpValue);
    ast_builder->setValue(capnpValue.asPtr());

    // Validate and store the query condition op.
    const std::string op_str = query_condition_op_str(node->get_op());
    ensure_qc_op_string_is_valid(op_str);
    ast_builder->setOp(op_str);
  } else {
    // Store the boolean expression tag.
    ast_builder->setIsExpression(true);

    // We assume that the serialized values of the child nodes are validated
    // properly.
    auto children_builder =
        ast_builder->initChildren(node->get_children().size());

    for (size_t i = 0; i < node->get_children().size(); ++i) {
      auto child_builder = children_builder[i];
      condition_ast_to_capnp(node->get_children()[i], &child_builder);
    }

    // Validate and store the query condition combination op.
    const std::string op_str =
        query_condition_combination_op_str(node->get_combination_op());
    ensure_qc_combo_op_string_is_valid(op_str);
    ast_builder->setCombinationOp(op_str);
  }
  return Status::Ok();
}

static void clause_to_capnp(
    const tdb_unique_ptr<ASTNode>& node,
    capnp::ConditionClause::Builder* clause_builder) {
  // Validate and store the field name.
  std::string field_name = node->get_field_name();
  ensure_qc_field_name_is_valid(field_name);
  clause_builder->setFieldName(field_name);

  // Copy the condition value into a capnp vector of bytes.
  const UntypedDatumView& value = node->get_condition_value_view();
  auto capnpValue = kj::Vector<uint8_t>();
  capnpValue.addAll(kj::ArrayPtr<uint8_t>(
      const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(value.content())),
      value.size()));

  // Validate and store the condition value vector of bytes.
  ensure_qc_capnp_condition_value_is_valid(capnpValue);
  clause_builder->setValue(capnpValue.asPtr());

  // Validate and store the query condition op.
  const std::string op_str = query_condition_op_str(node->get_op());
  ensure_qc_op_string_is_valid(op_str);
  clause_builder->setOp(op_str);
}

Status condition_to_capnp(
    const QueryCondition& condition,
    capnp::Condition::Builder* condition_builder) {
  const tdb_unique_ptr<ASTNode>& ast = condition.ast();
  assert(!condition.empty());

  // Validate and store the query condition AST.
  auto ast_builder = condition_builder->initTree();
  RETURN_NOT_OK(condition_ast_to_capnp(ast, &ast_builder));

  // For backwards compatability, we should also set the clauses and combination
  // ops vectors when the current query condition.
  if (ast->is_backwards_compatible()) {
    if (ast->is_expr()) {
      // We assume that the serialized values of the clauses are validated
      // properly.
      const std::vector<tdb_unique_ptr<ASTNode>>& clauses_vec =
          ast->get_children();
      auto clauses_builder = condition_builder->initClauses(clauses_vec.size());
      for (size_t i = 0; i < clauses_vec.size(); ++i) {
        auto clause_builder = clauses_builder[i];
        clause_to_capnp(clauses_vec[i], &clause_builder);
      }

      // Validating and storing the combination op vector.
      if (clauses_vec.size() > 1) {
        auto combination_ops_builder =
            condition_builder->initClauseCombinationOps(clauses_vec.size() - 1);
        for (size_t i = 0; i < clauses_vec.size() - 1; ++i) {
          const std::string op_str = query_condition_combination_op_str(
              QueryConditionCombinationOp::AND);
          ensure_qc_combo_op_string_is_valid(op_str);
          combination_ops_builder.set(i, op_str);
        }
      }
    } else {
      // We assume the serialized value of the clause is verified properly.
      auto clauses_builder = condition_builder->initClauses(1);
      auto clause_builder = clauses_builder[0];
      clause_to_capnp(ast, &clause_builder);
    }
  }
  return Status::Ok();
}

Status reader_to_capnp(
    const Query& query,
    const Reader& reader,
    capnp::QueryReader::Builder* reader_builder) {
  const auto& array_schema = query.array_schema();

  // Subarray layout
  const auto& layout = layout_str(query.layout());
  reader_builder->setLayout(layout);

  // Subarray
  auto subarray_builder = reader_builder->initSubarray();
  RETURN_NOT_OK(
      subarray_to_capnp(array_schema, query.subarray(), &subarray_builder));

  // Read state
  RETURN_NOT_OK(read_state_to_capnp(array_schema, reader, reader_builder));

  const auto& condition = query.condition();
  if (!condition.empty()) {
    auto condition_builder = reader_builder->initCondition();
    RETURN_NOT_OK(condition_to_capnp(condition, &condition_builder));
  }

  // If stats object exists set its cap'n proto object
  stats::Stats* stats = reader.stats();
  if (stats != nullptr) {
    auto stats_builder = reader_builder->initStats();
    RETURN_NOT_OK(stats_to_capnp(*stats, &stats_builder));
  }

  return Status::Ok();
}

Status index_reader_to_capnp(
    const Query& query,
    const SparseIndexReaderBase& reader,
    capnp::ReaderIndex::Builder* reader_builder) {
  const auto& array_schema = query.array_schema();

  // Subarray layout
  const auto& layout = layout_str(query.layout());
  reader_builder->setLayout(layout);

  // Subarray
  auto subarray_builder = reader_builder->initSubarray();
  RETURN_NOT_OK(
      subarray_to_capnp(array_schema, query.subarray(), &subarray_builder));

  // Read state
  RETURN_NOT_OK(index_read_state_to_capnp(reader.read_state(), reader_builder));

  const auto& condition = query.condition();
  if (!condition.empty()) {
    auto condition_builder = reader_builder->initCondition();
    RETURN_NOT_OK(condition_to_capnp(condition, &condition_builder));
  }

  // If stats object exists set its cap'n proto object
  stats::Stats* stats = reader.stats();
  if (stats != nullptr) {
    auto stats_builder = reader_builder->initStats();
    RETURN_NOT_OK(stats_to_capnp(*stats, &stats_builder));
  }

  return Status::Ok();
}

Status dense_reader_to_capnp(
    const Query& query,
    const DenseReader& reader,
    capnp::QueryReader::Builder* reader_builder) {
  const auto& array_schema = query.array_schema();

  // Subarray layout
  const auto& layout = layout_str(query.layout());
  reader_builder->setLayout(layout);

  // Subarray
  auto subarray_builder = reader_builder->initSubarray();
  RETURN_NOT_OK(
      subarray_to_capnp(array_schema, query.subarray(), &subarray_builder));

  // Read state
  RETURN_NOT_OK(
      dense_read_state_to_capnp(array_schema, reader, reader_builder));

  const auto& condition = query.condition();
  if (!condition.empty()) {
    auto condition_builder = reader_builder->initCondition();
    RETURN_NOT_OK(condition_to_capnp(condition, &condition_builder));
  }

  // If stats object exists set its cap'n proto object
  stats::Stats* stats = reader.stats();
  if (stats != nullptr) {
    auto stats_builder = reader_builder->initStats();
    RETURN_NOT_OK(stats_to_capnp(*stats, &stats_builder));
  }

  return Status::Ok();
}

tdb_unique_ptr<ASTNode> condition_ast_from_capnp(
    const capnp::ASTNode::Reader& ast_reader) {
  if (!ast_reader.getIsExpression()) {
    // Getting and validating the field name.
    std::string field_name = ast_reader.getFieldName();
    ensure_qc_field_name_is_valid(field_name);

    // Getting and validating the condition value.
    auto condition_value = ast_reader.getValue();
    const void* data =
        static_cast<const void*>(condition_value.asBytes().begin());
    size_t size = condition_value.size();
    ensure_qc_condition_value_is_valid(data, size);

    // Getting and validating the query condition operator.
    QueryConditionOp op = QueryConditionOp::LT;
    Status s = query_condition_op_enum(ast_reader.getOp(), &op);
    if (!s.ok()) {
      throw std::runtime_error(
          "condition_ast_from_capnp: query_condition_op_enum failed.");
    }
    ensure_qc_op_is_valid(op);

    return tdb_unique_ptr<ASTNode>(
        tdb_new(ASTNodeVal, field_name, data, size, op));
  }

  // Getting and validating the query condition combination operator.
  std::string combination_op_str = ast_reader.getCombinationOp();
  QueryConditionCombinationOp combination_op = QueryConditionCombinationOp::AND;
  Status s =
      query_condition_combination_op_enum(combination_op_str, &combination_op);
  if (!s.ok()) {
    throw std::runtime_error(
        "condition_ast_from_capnp: query_condition_combination_op_enum "
        "failed.");
  }
  ensure_qc_combo_op_is_valid(combination_op);

  // We assume that the deserialized values of the child nodes are validated
  // properly.
  std::vector<tdb_unique_ptr<ASTNode>> ast_nodes;
  for (const auto child : ast_reader.getChildren()) {
    ast_nodes.push_back(condition_ast_from_capnp(child));
  }
  return tdb_unique_ptr<ASTNode>(
      tdb_new(ASTNodeExpr, std::move(ast_nodes), combination_op));
}

Status condition_from_capnp(
    const capnp::Condition::Reader& condition_reader,
    QueryCondition* const condition) {
  if (condition_reader.hasClauses()) {  // coming from older API
    // Accumulating the AST value nodes from the clause list.
    std::vector<tdb_unique_ptr<ASTNode>> ast_nodes;
    for (const auto clause : condition_reader.getClauses()) {
      // Getting and validating the field name.
      std::string field_name = clause.getFieldName();
      ensure_qc_field_name_is_valid(field_name);

      // Getting and validating the condition value.
      auto condition_value = clause.getValue();
      const void* data =
          static_cast<const void*>(condition_value.asBytes().begin());
      size_t size = condition_value.size();
      ensure_qc_condition_value_is_valid(data, size);

      // Getting and validating the query condition operator.
      QueryConditionOp op = QueryConditionOp::LT;
      Status s = query_condition_op_enum(clause.getOp(), &op);
      if (!s.ok()) {
        throw std::runtime_error(
            "condition_ast_from_capnp: query_condition_op_enum failed.");
      }
      ensure_qc_op_is_valid(op);

      ast_nodes.push_back(tdb_unique_ptr<ASTNode>(
          tdb_new(ASTNodeVal, field_name, data, size, op)));
    }

    // Constructing the tree from the list of AST nodes.
    assert(ast_nodes.size() > 0);
    if (ast_nodes.size() == 1) {
      condition->set_ast(std::move(ast_nodes[0]));
    } else {
      auto tree_ptr = tdb_unique_ptr<ASTNode>(tdb_new(
          ASTNodeExpr, std::move(ast_nodes), QueryConditionCombinationOp::AND));
      condition->set_ast(std::move(tree_ptr));
    }
  } else if (condition_reader.hasTree()) {
    // Constructing the query condition from the AST representation.
    // We assume that the deserialized values of the AST are validated properly.
    auto ast_reader = condition_reader.getTree();
    condition->set_ast(condition_ast_from_capnp(ast_reader));
  }
  return Status::Ok();
}

Status reader_from_capnp(
    const capnp::QueryReader::Reader& reader_reader,
    Query* query,
    Reader* reader,
    ThreadPool* compute_tp) {
  auto array = query->array();

  // Layout
  Layout layout = Layout::ROW_MAJOR;
  RETURN_NOT_OK(layout_enum(reader_reader.getLayout(), &layout));

  // Subarray
  Subarray subarray(array, layout, query->stats(), dummy_logger, false);
  auto subarray_reader = reader_reader.getSubarray();
  RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
  RETURN_NOT_OK(query->set_subarray_unsafe(subarray));

  // Read state
  if (reader_reader.hasReadState())
    RETURN_NOT_OK(read_state_from_capnp(
        array, reader_reader.getReadState(), query, reader, compute_tp));

  // Query condition
  if (reader_reader.hasCondition()) {
    auto condition_reader = reader_reader.getCondition();
    QueryCondition condition;
    RETURN_NOT_OK(condition_from_capnp(condition_reader, &condition));
    RETURN_NOT_OK(query->set_condition(condition));
  }

  // If cap'n proto object has stats set it on c++ object
  if (reader_reader.hasStats()) {
    stats::Stats* stats = reader->stats();
    // We should always have a stats here
    if (stats != nullptr) {
      RETURN_NOT_OK(stats_from_capnp(reader_reader.getStats(), stats));
    }
  }

  return Status::Ok();
}

Status index_reader_from_capnp(
    const ArraySchema& schema,
    const capnp::ReaderIndex::Reader& reader_reader,
    Query* query,
    SparseIndexReaderBase* reader) {
  auto array = query->array();

  // Layout
  Layout layout = Layout::ROW_MAJOR;
  RETURN_NOT_OK(layout_enum(reader_reader.getLayout(), &layout));

  // Subarray
  Subarray subarray(array, layout, query->stats(), dummy_logger, false);
  auto subarray_reader = reader_reader.getSubarray();
  RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
  RETURN_NOT_OK(query->set_subarray_unsafe(subarray));

  // Read state
  if (reader_reader.hasReadState())
    RETURN_NOT_OK(index_read_state_from_capnp(
        schema, reader_reader.getReadState(), reader));

  // Query condition
  if (reader_reader.hasCondition()) {
    auto condition_reader = reader_reader.getCondition();
    QueryCondition condition;
    RETURN_NOT_OK(condition_from_capnp(condition_reader, &condition));
    RETURN_NOT_OK(query->set_condition(condition));
  }

  // If cap'n proto object has stats set it on c++ object
  if (reader_reader.hasStats()) {
    stats::Stats* stats = reader->stats();
    // We should always have a stats here
    if (stats != nullptr) {
      RETURN_NOT_OK(stats_from_capnp(reader_reader.getStats(), stats));
    }
  }

  return Status::Ok();
}

Status dense_reader_from_capnp(
    const ArraySchema& schema,
    const capnp::QueryReader::Reader& reader_reader,
    Query* query,
    DenseReader* reader,
    ThreadPool* compute_tp) {
  auto array = query->array();

  // Layout
  Layout layout = Layout::ROW_MAJOR;
  RETURN_NOT_OK(layout_enum(reader_reader.getLayout(), &layout));

  // Subarray
  Subarray subarray(array, layout, query->stats(), dummy_logger, false);
  auto subarray_reader = reader_reader.getSubarray();
  RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
  RETURN_NOT_OK(query->set_subarray_unsafe(subarray));

  // Read state
  if (reader_reader.hasReadState())
    RETURN_NOT_OK(dense_read_state_from_capnp(
        array, reader_reader.getReadState(), query, reader, compute_tp));

  // Query condition
  if (reader_reader.hasCondition()) {
    auto condition_reader = reader_reader.getCondition();
    QueryCondition condition;
    RETURN_NOT_OK(condition_from_capnp(condition_reader, &condition));
    RETURN_NOT_OK(query->set_condition(condition));
  }

  // If cap'n proto object has stats set it on c++ object
  if (reader_reader.hasStats()) {
    stats::Stats* stats = reader->stats();
    // We should always have a stats here
    if (stats != nullptr) {
      RETURN_NOT_OK(stats_from_capnp(reader_reader.getStats(), stats));
    }
  }

  return Status::Ok();
}

Status delete_from_capnp(
    const capnp::Delete::Reader& delete_reader,
    Query* query,
    Deletes* delete_strategy) {
  // Query condition
  if (delete_reader.hasCondition()) {
    auto condition_reader = delete_reader.getCondition();
    QueryCondition condition;
    RETURN_NOT_OK(condition_from_capnp(condition_reader, &condition));
    RETURN_NOT_OK(query->set_condition(condition));
  }

  // If cap'n proto object has stats set it on c++ object
  if (delete_reader.hasStats()) {
    stats::Stats* stats = delete_strategy->stats();
    // We should always have a stats here
    if (stats != nullptr) {
      RETURN_NOT_OK(stats_from_capnp(delete_reader.getStats(), stats));
    }
  }

  return Status::Ok();
}

Status delete_to_capnp(
    const Query& query,
    Deletes& delete_strategy,
    capnp::Delete::Builder* delete_builder) {
  auto condition = query.condition();
  if (!condition.empty()) {
    auto condition_builder = delete_builder->initCondition();
    RETURN_NOT_OK(condition_to_capnp(condition, &condition_builder));
  }

  // If stats object exists set its cap'n proto object
  stats::Stats* stats = delete_strategy.stats();
  if (stats != nullptr) {
    auto stats_builder = delete_builder->initStats();
    RETURN_NOT_OK(stats_to_capnp(*stats, &stats_builder));
  }

  return Status::Ok();
}

Status writer_to_capnp(
    const Query& query,
    WriterBase& writer,
    capnp::Writer::Builder* writer_builder) {
  writer_builder->setCheckCoordDups(writer.get_check_coord_dups());
  writer_builder->setCheckCoordOOB(writer.get_check_coord_oob());
  writer_builder->setDedupCoords(writer.get_dedup_coords());

  const auto& array_schema = query.array_schema();

  if (array_schema.dense()) {
    std::vector<uint8_t> subarray_flat;
    RETURN_NOT_OK(query.subarray()->to_byte_vec(&subarray_flat));
    auto subarray_builder = writer_builder->initSubarray();
    RETURN_NOT_OK(utils::serialize_subarray(
        subarray_builder, array_schema, &subarray_flat[0]));
  }

  // Subarray
  const auto subarray_ranges = query.subarray();
  if (!subarray_ranges->empty()) {
    auto subarray_builder = writer_builder->initSubarrayRanges();
    RETURN_NOT_OK(
        subarray_to_capnp(array_schema, subarray_ranges, &subarray_builder));
  }

  // If stats object exists set its cap'n proto object
  stats::Stats* stats = writer.stats();
  if (stats != nullptr) {
    auto stats_builder = writer_builder->initStats();
    RETURN_NOT_OK(stats_to_capnp(*stats, &stats_builder));
  }

  if (query.layout() == Layout::GLOBAL_ORDER) {
    auto& globalwriter = dynamic_cast<GlobalOrderWriter&>(writer);
    auto globalstate = globalwriter.get_global_state();
    // Remote global order writes have global_write_state equal to nullptr
    // before the first submit(). The state gets initialized when do_work() gets
    // invoked. Once GlobalOrderWriter becomes C41 compliant, we can delete this
    // check.
    if (globalstate) {
      auto global_state_builder = writer_builder->initGlobalWriteStateV1();
      RETURN_NOT_OK(global_write_state_to_capnp(
          query, globalwriter, &global_state_builder));
    }
  }

  return Status::Ok();
}

Status writer_from_capnp(
    const Query& query,
    const capnp::Writer::Reader& writer_reader,
    WriterBase* writer) {
  writer->set_check_coord_dups(writer_reader.getCheckCoordDups());
  writer->set_check_coord_oob(writer_reader.getCheckCoordOOB());
  writer->set_dedup_coords(writer_reader.getDedupCoords());

  // If cap'n proto object has stats set it on c++ object
  if (writer_reader.hasStats()) {
    stats::Stats* stats = writer->stats();
    // We should always have a stats here
    if (stats != nullptr) {
      RETURN_NOT_OK(stats_from_capnp(writer_reader.getStats(), stats));
    }
  }

  if (query.layout() == Layout::GLOBAL_ORDER &&
      writer_reader.hasGlobalWriteStateV1()) {
    auto global_writer = dynamic_cast<GlobalOrderWriter*>(writer);

    // global_write_state is not allocated when deserializing into a
    // new Query object
    if (!global_writer->get_global_state()) {
      RETURN_NOT_OK(global_writer->alloc_global_write_state());
      RETURN_NOT_OK(global_writer->init_global_write_state());
    }
    RETURN_NOT_OK(global_write_state_from_capnp(
        query, writer_reader.getGlobalWriteStateV1(), global_writer));
  }

  return Status::Ok();
}

Status query_to_capnp(
    Query& query,
    capnp::Query::Builder* query_builder,
    const bool client_side) {
  // For easy reference
  auto layout = query.layout();
  auto type = query.type();
  auto array = query.array();

  if (array == nullptr) {
    return LOG_STATUS(
        Status_SerializationError("Cannot serialize; array is null."));
  }

  const auto& schema = query.array_schema();

  // Serialize basic fields
  query_builder->setType(query_type_str(type));
  query_builder->setLayout(layout_str(layout));
  query_builder->setStatus(query_status_str(query.status()));

  // Serialize array
  if (query.array() != nullptr) {
    auto builder = query_builder->initArray();
    RETURN_NOT_OK(array_to_capnp(array, &builder, client_side));
  }

  // Serialize attribute buffer metadata
  const auto buffer_names = query.buffer_names();
  auto attr_buffers_builder =
      query_builder->initAttributeBufferHeaders(buffer_names.size());
  uint64_t total_fixed_len_bytes = 0;
  uint64_t total_var_len_bytes = 0;
  uint64_t total_validity_len_bytes = 0;
  for (uint64_t i = 0; i < buffer_names.size(); i++) {
    auto attr_buffer_builder = attr_buffers_builder[i];
    const auto& name = buffer_names[i];
    const auto& buff = query.buffer(name);
    attr_buffer_builder.setName(name);
    if (buff.buffer_var_ != nullptr && buff.buffer_var_size_ != nullptr) {
      // Variable-sized buffer
      total_var_len_bytes += *buff.buffer_var_size_;
      attr_buffer_builder.setVarLenBufferSizeInBytes(*buff.buffer_var_size_);
      total_fixed_len_bytes += *buff.buffer_size_;
      attr_buffer_builder.setFixedLenBufferSizeInBytes(*buff.buffer_size_);

      // Set original user requested sizes
      attr_buffer_builder.setOriginalVarLenBufferSizeInBytes(
          buff.original_buffer_var_size_);
      attr_buffer_builder.setOriginalFixedLenBufferSizeInBytes(
          buff.original_buffer_size_);
    } else if (buff.buffer_ != nullptr && buff.buffer_size_ != nullptr) {
      // Fixed-length buffer
      total_fixed_len_bytes += *buff.buffer_size_;
      attr_buffer_builder.setFixedLenBufferSizeInBytes(*buff.buffer_size_);
      attr_buffer_builder.setVarLenBufferSizeInBytes(0);

      // Set original user requested sizes
      attr_buffer_builder.setOriginalVarLenBufferSizeInBytes(0);
      attr_buffer_builder.setOriginalFixedLenBufferSizeInBytes(
          buff.original_buffer_size_);
    } else {
      assert(false);
    }

    if (buff.validity_vector_.buffer_size() != nullptr) {
      total_validity_len_bytes += *buff.validity_vector_.buffer_size();
      attr_buffer_builder.setValidityLenBufferSizeInBytes(
          *buff.validity_vector_.buffer_size());

      // Set original user requested sizes
      attr_buffer_builder.setOriginalValidityLenBufferSizeInBytes(
          buff.original_validity_vector_size_);
    }
  }

  query_builder->setTotalFixedLengthBufferBytes(total_fixed_len_bytes);
  query_builder->setTotalVarLenBufferBytes(total_var_len_bytes);
  query_builder->setTotalValidityBufferBytes(total_validity_len_bytes);

  if (type == QueryType::READ) {
    bool all_dense = true;
    for (auto& frag_md : array->fragment_metadata()) {
      all_dense &= frag_md->dense();
    }

    auto reader = dynamic_cast<ReaderBase*>(query.strategy(true));
    query_builder->setVarOffsetsMode(reader->offsets_mode());
    query_builder->setVarOffsetsAddExtraElement(
        reader->offsets_extra_element());
    query_builder->setVarOffsetsBitsize(reader->offsets_bitsize());

    if (query.use_refactored_sparse_unordered_with_dups_reader(
            layout, schema) ||
        query.use_refactored_sparse_global_order_reader(layout, schema)) {
      auto builder = query_builder->initReaderIndex();
      auto reader = dynamic_cast<SparseIndexReaderBase*>(query.strategy(true));
      RETURN_NOT_OK(index_reader_to_capnp(query, *reader, &builder));
    } else if (query.use_refactored_dense_reader(schema, all_dense)) {
      auto builder = query_builder->initDenseReader();
      auto reader = dynamic_cast<DenseReader*>(query.strategy(true));
      RETURN_NOT_OK(dense_reader_to_capnp(query, *reader, &builder));
    } else {
      auto builder = query_builder->initReader();
      auto reader = dynamic_cast<Reader*>(query.strategy());
      RETURN_NOT_OK(reader_to_capnp(query, *reader, &builder));
    }
  } else if (type == QueryType::WRITE) {
    auto builder = query_builder->initWriter();
    auto writer = dynamic_cast<WriterBase*>(query.strategy(true));

    query_builder->setVarOffsetsMode(writer->offsets_mode());
    query_builder->setVarOffsetsAddExtraElement(
        writer->offsets_extra_element());
    query_builder->setVarOffsetsBitsize(writer->offsets_bitsize());
    RETURN_NOT_OK(writer_to_capnp(query, *writer, &builder));
  } else {
    auto builder = query_builder->initDelete();
    auto delete_strategy = dynamic_cast<Deletes*>(query.strategy(true));
    RETURN_NOT_OK(delete_to_capnp(query, *delete_strategy, &builder));
  }

  // Serialize Config
  const Config* config = query.config();
  auto config_builder = query_builder->initConfig();
  RETURN_NOT_OK(config_to_capnp(config, &config_builder));

  // If stats object exists set its cap'n proto object
  stats::Stats* stats = query.stats();
  if (stats != nullptr) {
    auto stats_builder = query_builder->initStats();
    RETURN_NOT_OK(stats_to_capnp(*stats, &stats_builder));
  }

  auto& written_fragment_info = query.get_written_fragment_info();
  if (!written_fragment_info.empty()) {
    auto builder =
        query_builder->initWrittenFragmentInfo(written_fragment_info.size());
    for (uint64_t i = 0; i < written_fragment_info.size(); ++i) {
      builder[i].setUri(written_fragment_info[i].uri_);
      auto range_builder = builder[i].initTimestampRange(2);
      range_builder.set(0, written_fragment_info[i].timestamp_range_.first);
      range_builder.set(1, written_fragment_info[i].timestamp_range_.second);
    }
  }

  return Status::Ok();
}

Status query_from_capnp(
    const capnp::Query::Reader& query_reader,
    const SerializationContext context,
    void* buffer_start,
    CopyState* const copy_state,
    Query* const query,
    ThreadPool* compute_tp) {
  using namespace tiledb::sm;

  auto type = query->type();
  auto array = query->array();
  const auto& schema = query->array_schema();

  if (array == nullptr) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot deserialize; array pointer is null."));
  }

  // Deserialize query type (sanity check).
  QueryType query_type = QueryType::READ;
  RETURN_NOT_OK(query_type_enum(query_reader.getType().cStr(), &query_type));
  if (query_type != type) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot deserialize; Query opened for " + query_type_str(type) +
        " but got serialized type for " + query_reader.getType().cStr()));
  }

  if (query_type != QueryType::READ && query_type != QueryType::WRITE &&
      query_type != QueryType::DELETE &&
      query_type != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot deserialize; Unsupported query type."));
  }

  // Deserialize layout.
  Layout layout = Layout::UNORDERED;
  RETURN_NOT_OK(layout_enum(query_reader.getLayout().cStr(), &layout));

  // Deserialize array instance.
  RETURN_NOT_OK(array_from_capnp(query_reader.getArray(), array));

  // Marks the query as remote
  // Needs to happen before the reset strategy call below so that the marker
  // propagates to the underlying strategy. Also this marker needs to live
  // on the Query not on the strategy because for the first remote submit,
  // the strategy gets reset once more during query submit on the server side
  // (much later).
  if (context == SerializationContext::SERVER) {
    query->set_remote_query();
  }

  // Make sure we have the right query strategy in place.
  bool force_legacy_reader =
      query_type == QueryType::READ && query_reader.hasReader();
  RETURN_NOT_OK(query->reset_strategy_with_layout(layout, force_legacy_reader));

  // Deserialize Config
  if (query_reader.hasConfig()) {
    tdb_unique_ptr<Config> decoded_config = nullptr;
    auto config_reader = query_reader.getConfig();
    RETURN_NOT_OK(config_from_capnp(config_reader, &decoded_config));
    if (decoded_config != nullptr) {
      RETURN_NOT_OK(query->set_config(*decoded_config));
    }
  }

  // Deserialize and set attribute buffers.
  if (!query_reader.hasAttributeBufferHeaders()) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot deserialize; no attribute buffer headers in message."));
  }

  auto buffer_headers = query_reader.getAttributeBufferHeaders();
  auto attribute_buffer_start = static_cast<char*>(buffer_start);
  for (auto buffer_header : buffer_headers) {
    const std::string name = buffer_header.getName().cStr();

    // Get buffer sizes required
    const uint64_t fixedlen_size = buffer_header.getFixedLenBufferSizeInBytes();
    const uint64_t varlen_size = buffer_header.getVarLenBufferSizeInBytes();
    const uint64_t validitylen_size =
        buffer_header.getValidityLenBufferSizeInBytes();

    // Get current copy state for the attribute (contains destination offsets
    // for memcpy into user buffers).
    QueryBufferCopyState* attr_copy_state = nullptr;
    if (copy_state != nullptr)
      attr_copy_state = &(*copy_state)[name];

    // Get any buffers already set on this query object.
    uint64_t* existing_offset_buffer = nullptr;
    uint64_t existing_offset_buffer_size = 0;
    void* existing_buffer = nullptr;
    uint64_t existing_buffer_size = 0;
    uint8_t* existing_validity_buffer = nullptr;
    uint64_t existing_validity_buffer_size = 0;

    // For writes and read (client side) we need ptrs to set the sizes properly
    uint64_t* existing_buffer_size_ptr = nullptr;
    uint64_t* existing_offset_buffer_size_ptr = nullptr;
    uint64_t* existing_validity_buffer_size_ptr = nullptr;

    auto var_size = schema.var_size(name);
    auto nullable = schema.is_nullable(name);
    if (type == QueryType::READ && context == SerializationContext::SERVER) {
      const QueryBuffer& query_buffer = query->buffer(name);
      // We use the query_buffer directly in order to get the original buffer
      // sizes. This avoid a problem where an incomplete query will change the
      // users buffer size to the smaller results and we end up not being able
      // to correctly calculate if the new results can fit into the users buffer
      if (var_size) {
        if (!nullable) {
          existing_offset_buffer = static_cast<uint64_t*>(query_buffer.buffer_);
          existing_offset_buffer_size = query_buffer.original_buffer_size_;
          existing_buffer = query_buffer.buffer_var_;
          existing_buffer_size = query_buffer.original_buffer_var_size_;
        } else {
          existing_offset_buffer = static_cast<uint64_t*>(query_buffer.buffer_);
          existing_offset_buffer_size = query_buffer.original_buffer_size_;
          existing_buffer = query_buffer.buffer_var_;
          existing_buffer_size = query_buffer.original_buffer_var_size_;
          existing_validity_buffer = query_buffer.validity_vector_.buffer();
          existing_validity_buffer_size =
              query_buffer.original_validity_vector_size_;
        }
      } else {
        if (!nullable) {
          existing_buffer = query_buffer.buffer_;
          existing_buffer_size = query_buffer.original_buffer_size_;
        } else {
          existing_buffer = query_buffer.buffer_;
          existing_buffer_size = query_buffer.original_buffer_size_;
          existing_validity_buffer = query_buffer.validity_vector_.buffer();
          existing_validity_buffer_size =
              query_buffer.original_validity_vector_size_;
        }
      }
    } else if (query_type == QueryType::WRITE || type == QueryType::READ) {
      // For writes we need to use get_buffer and clientside
      if (var_size) {
        if (!nullable) {
          RETURN_NOT_OK(query->get_data_buffer(
              name.c_str(), &existing_buffer, &existing_buffer_size_ptr));
          RETURN_NOT_OK(query->get_offsets_buffer(
              name.c_str(),
              &existing_offset_buffer,
              &existing_offset_buffer_size_ptr));

          if (existing_offset_buffer_size_ptr != nullptr)
            existing_offset_buffer_size = *existing_offset_buffer_size_ptr;
          if (existing_buffer_size_ptr != nullptr)
            existing_buffer_size = *existing_buffer_size_ptr;
        } else {
          RETURN_NOT_OK(query->get_data_buffer(
              name.c_str(), &existing_buffer, &existing_buffer_size_ptr));
          RETURN_NOT_OK(query->get_offsets_buffer(
              name.c_str(),
              &existing_offset_buffer,
              &existing_offset_buffer_size_ptr));
          RETURN_NOT_OK(query->get_validity_buffer(
              name.c_str(),
              &existing_validity_buffer,
              &existing_validity_buffer_size_ptr));

          if (existing_offset_buffer_size_ptr != nullptr)
            existing_offset_buffer_size = *existing_offset_buffer_size_ptr;
          if (existing_buffer_size_ptr != nullptr)
            existing_buffer_size = *existing_buffer_size_ptr;
          if (existing_validity_buffer_size_ptr != nullptr)
            existing_validity_buffer_size = *existing_validity_buffer_size_ptr;
        }
      } else {
        if (!nullable) {
          RETURN_NOT_OK(query->get_data_buffer(
              name.c_str(), &existing_buffer, &existing_buffer_size_ptr));

          if (existing_buffer_size_ptr != nullptr)
            existing_buffer_size = *existing_buffer_size_ptr;
        } else {
          RETURN_NOT_OK(query->get_data_buffer(
              name.c_str(), &existing_buffer, &existing_buffer_size_ptr));
          RETURN_NOT_OK(query->get_validity_buffer(
              name.c_str(),
              &existing_validity_buffer,
              &existing_validity_buffer_size_ptr));

          if (existing_buffer_size_ptr != nullptr)
            existing_buffer_size = *existing_buffer_size_ptr;
          if (existing_validity_buffer_size_ptr != nullptr)
            existing_validity_buffer_size = *existing_validity_buffer_size_ptr;
        }
      }
    } else {
      // Delete queries have nothing to deserialize.
    }

    if (context == SerializationContext::CLIENT) {
      // For queries on the client side, we require that buffers have been
      // set by the user, and that they are large enough for all the serialized
      // data.
      const uint64_t curr_data_size =
          attr_copy_state == nullptr ? 0 : attr_copy_state->data_size;
      const uint64_t data_size_left = existing_buffer_size - curr_data_size;
      const uint64_t curr_offset_size =
          attr_copy_state == nullptr ? 0 : attr_copy_state->offset_size;
      const uint64_t offset_size_left =
          existing_offset_buffer_size == 0 ?
              0 :
              existing_offset_buffer_size - curr_offset_size;
      const uint64_t curr_validity_size =
          attr_copy_state == nullptr ? 0 : attr_copy_state->validity_size;
      const uint64_t validity_size_left =
          existing_validity_buffer_size == 0 ?
              0 :
              existing_validity_buffer_size - curr_validity_size;

      const bool has_mem_for_data =
          (var_size && data_size_left >= varlen_size) ||
          (!var_size && data_size_left >= fixedlen_size);
      const bool has_mem_for_offset =
          (var_size && offset_size_left >= fixedlen_size) || !var_size;
      const bool has_mem_for_validity = validity_size_left >= validitylen_size;
      if (!has_mem_for_data || !has_mem_for_offset || !has_mem_for_validity) {
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing read query; buffer too small for buffer "
            "'" +
            name + "'."));
      }

      // For reads, copy the response data into user buffers. For writes,
      // nothing to do.
      if (type == QueryType::READ) {
        if (var_size) {
          // Var size attribute; buffers already set.
          char* offset_dest = (char*)existing_offset_buffer + curr_offset_size;
          char* data_dest = (char*)existing_buffer + curr_data_size;
          char* validity_dest =
              (char*)existing_validity_buffer + curr_validity_size;
          uint64_t fixedlen_size_to_copy = fixedlen_size;

          // If the last query included an extra offset we will skip the first
          // offset in this query The first offset is the 0 position which ends
          // up the same as the extra offset in the last query 0th is converted
          // below to curr_data_size which is also the n+1 offset in arrow mode
          if (attr_copy_state != nullptr &&
              attr_copy_state->last_query_added_extra_offset) {
            attribute_buffer_start += sizeof(uint64_t);
            fixedlen_size_to_copy -= sizeof(uint64_t);
          }

          std::memcpy(
              offset_dest, attribute_buffer_start, fixedlen_size_to_copy);
          attribute_buffer_start += fixedlen_size_to_copy;
          std::memcpy(data_dest, attribute_buffer_start, varlen_size);
          attribute_buffer_start += varlen_size;
          if (nullable) {
            std::memcpy(
                validity_dest, attribute_buffer_start, validitylen_size);
            attribute_buffer_start += validitylen_size;
          }

          // The offsets in each buffer correspond to the values in its
          // data buffer. To build a single contigious buffer, we must
          // ensure the offset values continue in ascending order from the
          // previous buffer. For example, consider the following example
          // storing int32 values.
          //
          // Buffer #1:
          //   offsets: [0, 8, 16]
          //   values:  [1, 2, 3, 4, 5, 6, 7]
          //
          // Buffer #2:
          //   offsets: [0, 12]
          //   values:  [100, 200, 300, 400, 500]
          //
          // The final, contigious buffer will be:
          //   offsets: [0, 8, 16, 28, 40]
          //   values:  [1, 2, 3, 4, 5, 6, 7, 100, 200, 300, 400, 500]
          //
          // The last two offsets, `28, 40` were calculated by adding `28`
          // to offsets of Buffer #2: [0, 12]. The `28` was calculated as
          // the byte size of the values in Buffer #1.
          if (curr_data_size > 0) {
            for (uint64_t i = 0; i < (fixedlen_size_to_copy / sizeof(uint64_t));
                 ++i) {
              reinterpret_cast<uint64_t*>(offset_dest)[i] += curr_data_size;
            }
          }

          if (attr_copy_state == nullptr) {
            // Set the size directly on the query (so user can introspect on
            // result size).
            if (existing_offset_buffer_size_ptr != nullptr)
              *existing_offset_buffer_size_ptr =
                  curr_offset_size + fixedlen_size_to_copy;
            if (existing_buffer_size_ptr != nullptr)
              *existing_buffer_size_ptr = curr_data_size + varlen_size;
            if (nullable && existing_validity_buffer_size_ptr != nullptr)
              *existing_validity_buffer_size_ptr =
                  curr_validity_size + validitylen_size;
          } else {
            // Accumulate total bytes copied (caller's responsibility to
            // eventually update the query).
            attr_copy_state->offset_size += fixedlen_size_to_copy;
            attr_copy_state->data_size += varlen_size;
            if (nullable)
              attr_copy_state->validity_size += validitylen_size;

            // Set whether the extra offset was included or not
            attr_copy_state->last_query_added_extra_offset =
                query_reader.getVarOffsetsAddExtraElement();
          }
        } else {
          // Fixed size attribute; buffers already set.
          char* data_dest = (char*)existing_buffer + curr_data_size;

          std::memcpy(data_dest, attribute_buffer_start, fixedlen_size);
          attribute_buffer_start += fixedlen_size;
          if (nullable) {
            char* validity_dest =
                (char*)existing_validity_buffer + curr_validity_size;
            std::memcpy(
                validity_dest, attribute_buffer_start, validitylen_size);
            attribute_buffer_start += validitylen_size;
          }

          if (attr_copy_state == nullptr) {
            if (existing_buffer_size_ptr != nullptr)
              *existing_buffer_size_ptr = curr_data_size + fixedlen_size;
            if (nullable && existing_validity_buffer_size_ptr != nullptr)
              *existing_validity_buffer_size_ptr =
                  curr_validity_size + validitylen_size;
          } else {
            attr_copy_state->data_size += fixedlen_size;
            if (nullable)
              attr_copy_state->validity_size += validitylen_size;
          }
        }
      }
    } else if (context == SerializationContext::SERVER) {
      // Always expect null buffers when deserializing.
      if (existing_buffer != nullptr || existing_offset_buffer != nullptr ||
          existing_validity_buffer != nullptr)
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing read query; unexpected "
            "buffer set on server-side."));

      Query::SerializationState::AttrState* attr_state;
      RETURN_NOT_OK(query->get_attr_serialization_state(name, &attr_state));
      if (type == QueryType::READ) {
        // On reads, just set null pointers with accurate size so that the
        // server can introspect and allocate properly sized buffers separately.
        Buffer offsets_buff(nullptr, fixedlen_size);
        Buffer varlen_buff(nullptr, varlen_size);
        Buffer validitylen_buff(nullptr, validitylen_size);
        // For the server on reads we want to set the original user requested
        // buffer sizes This handles the case of incomplete queries where on the
        // second `submit()` call the client's buffer size will be the first
        // submit's result size not the original user set buffer size. To work
        // around this we revert the server to always use the full original user
        // requested buffer sizes.
        // We check for > 0 for fallback for clients older than 2.2.5
        if (buffer_header.getOriginalFixedLenBufferSizeInBytes() > 0) {
          attr_state->fixed_len_size =
              buffer_header.getOriginalFixedLenBufferSizeInBytes();
        } else {
          attr_state->fixed_len_size =
              buffer_header.getFixedLenBufferSizeInBytes();
        }

        if (buffer_header.getOriginalVarLenBufferSizeInBytes() > 0) {
          attr_state->var_len_size =
              buffer_header.getOriginalVarLenBufferSizeInBytes();
        } else {
          attr_state->var_len_size = buffer_header.getVarLenBufferSizeInBytes();
        }

        if (buffer_header.getOriginalValidityLenBufferSizeInBytes() > 0) {
          attr_state->validity_len_size =
              buffer_header.getOriginalValidityLenBufferSizeInBytes();
        } else {
          attr_state->validity_len_size =
              buffer_header.getValidityLenBufferSizeInBytes();
        }

        attr_state->fixed_len_data.swap(offsets_buff);
        attr_state->var_len_data.swap(varlen_buff);
        attr_state->validity_len_data.swap(validitylen_buff);
        if (var_size) {
          if (!nullable) {
            RETURN_NOT_OK(query->set_data_buffer(
                name, nullptr, &attr_state->var_len_size, false));
            RETURN_NOT_OK(query->set_offsets_buffer(
                name, nullptr, &attr_state->fixed_len_size, false));
          } else {
            RETURN_NOT_OK(query->set_buffer_vbytemap(
                name,
                nullptr,
                &attr_state->fixed_len_size,
                nullptr,
                &attr_state->var_len_size,
                nullptr,
                &attr_state->validity_len_size,
                false));
          }
        } else {
          if (!nullable) {
            RETURN_NOT_OK(query->set_data_buffer(
                name, nullptr, &attr_state->fixed_len_size, false));
          } else {
            RETURN_NOT_OK(query->set_buffer_vbytemap(
                name,
                nullptr,
                &attr_state->fixed_len_size,
                nullptr,
                &attr_state->validity_len_size,
                false));
          }
        }
      } else if (query_type == QueryType::WRITE) {
        // On writes, just set buffer pointers wrapping the data in the message.
        if (var_size) {
          auto* offsets = reinterpret_cast<uint64_t*>(attribute_buffer_start);
          auto* varlen_data = attribute_buffer_start + fixedlen_size;
          auto* validity = reinterpret_cast<uint8_t*>(
              attribute_buffer_start + fixedlen_size + varlen_size);

          attribute_buffer_start +=
              fixedlen_size + varlen_size + validitylen_size;

          Buffer offsets_buff(offsets, fixedlen_size);
          Buffer varlen_buff(varlen_data, varlen_size);
          Buffer validity_buff(
              validitylen_size > 0 ? validity : nullptr, validitylen_size);

          attr_state->fixed_len_size = fixedlen_size;
          attr_state->var_len_size = varlen_size;
          attr_state->validity_len_size = validitylen_size;

          attr_state->fixed_len_data.swap(offsets_buff);
          attr_state->var_len_data.swap(varlen_buff);
          attr_state->validity_len_data.swap(validity_buff);

          if (!nullable) {
            RETURN_NOT_OK(query->set_data_buffer(
                name, varlen_data, &attr_state->var_len_size));
            RETURN_NOT_OK(query->set_offsets_buffer(
                name, offsets, &attr_state->fixed_len_size));
          } else {
            RETURN_NOT_OK(query->set_buffer_vbytemap(
                name,
                offsets,
                &attr_state->fixed_len_size,
                varlen_data,
                &attr_state->var_len_size,
                validity,
                &attr_state->validity_len_size));
          }
        } else {
          auto* data = attribute_buffer_start;
          auto* validity = reinterpret_cast<uint8_t*>(
              attribute_buffer_start + fixedlen_size);

          attribute_buffer_start += fixedlen_size + validitylen_size;

          Buffer buff(data, fixedlen_size);
          Buffer varlen_buff(nullptr, 0);
          Buffer validity_buff(
              validitylen_size > 0 ? validity : nullptr, validitylen_size);

          attr_state->fixed_len_size = fixedlen_size;
          attr_state->var_len_size = varlen_size;
          attr_state->validity_len_size = validitylen_size;

          attr_state->fixed_len_data.swap(buff);
          attr_state->var_len_data.swap(varlen_buff);
          attr_state->validity_len_data.swap(validity_buff);

          if (!nullable) {
            RETURN_NOT_OK(query->set_data_buffer(
                name, data, &attr_state->fixed_len_size));
          } else {
            RETURN_NOT_OK(query->set_buffer_vbytemap(
                name,
                data,
                &attr_state->fixed_len_size,
                validity,
                &attr_state->validity_len_size));
          }
        }
      } else {
        // Delete queries have no buffers to deserialize.
      }
    }
  }

  // Deserialize reader/writer.
  // Also set subarray on query if it exists. Prior to 1.8 the subarray was set
  // on the reader or writer directly Now we set it on the query class after the
  // heterogeneous coordinate changes
  if (type == QueryType::READ) {
    auto reader = dynamic_cast<ReaderBase*>(query->strategy());
    if (query_reader.hasVarOffsetsMode()) {
      RETURN_NOT_OK(reader->set_offsets_mode(query_reader.getVarOffsetsMode()));
    }

    RETURN_NOT_OK(reader->set_offsets_extra_element(
        query_reader.getVarOffsetsAddExtraElement()));

    if (query_reader.getVarOffsetsBitsize() > 0) {
      RETURN_NOT_OK(
          reader->set_offsets_bitsize(query_reader.getVarOffsetsBitsize()));
    }

    if (query_reader.hasReaderIndex()) {
      auto reader_reader = query_reader.getReaderIndex();
      RETURN_NOT_OK(index_reader_from_capnp(
          schema,
          reader_reader,
          query,
          dynamic_cast<SparseIndexReaderBase*>(query->strategy())));
    } else if (query_reader.hasDenseReader()) {
      auto reader_reader = query_reader.getDenseReader();
      RETURN_NOT_OK(dense_reader_from_capnp(
          schema,
          reader_reader,
          query,
          dynamic_cast<DenseReader*>(query->strategy()),
          compute_tp));
    } else {
      auto reader_reader = query_reader.getReader();
      RETURN_NOT_OK(reader_from_capnp(
          reader_reader,
          query,
          dynamic_cast<Reader*>(query->strategy()),
          compute_tp));
    }
  } else if (query_type == QueryType::WRITE) {
    auto writer_reader = query_reader.getWriter();
    auto writer = dynamic_cast<WriterBase*>(query->strategy());

    if (query_reader.hasVarOffsetsMode()) {
      RETURN_NOT_OK(writer->set_offsets_mode(query_reader.getVarOffsetsMode()));
    }

    RETURN_NOT_OK(writer->set_offsets_extra_element(
        query_reader.getVarOffsetsAddExtraElement()));

    if (query_reader.getVarOffsetsBitsize() > 0) {
      RETURN_NOT_OK(
          writer->set_offsets_bitsize(query_reader.getVarOffsetsBitsize()));
    }

    RETURN_NOT_OK(writer_from_capnp(*query, writer_reader, writer));

    // For sparse writes we want to explicitly set subarray to nullptr.
    const bool sparse_write =
        !schema.dense() || query->layout() == Layout::UNORDERED;
    if (!sparse_write) {
      if (writer_reader.hasSubarray()) {
        auto subarray_reader = writer_reader.getSubarray();
        void* subarray = nullptr;
        RETURN_NOT_OK(
            utils::deserialize_subarray(subarray_reader, schema, &subarray));
        RETURN_NOT_OK_ELSE(query->set_subarray(subarray), tdb_free(subarray));
        tdb_free(subarray);
      }

      // Subarray
      if (writer_reader.hasSubarrayRanges()) {
        Subarray subarray(array, layout, query->stats(), dummy_logger, false);
        auto subarray_reader = writer_reader.getSubarrayRanges();
        RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
        RETURN_NOT_OK(query->set_subarray_unsafe(subarray));
      }
    }
  } else {
    auto delete_reader = query_reader.getDelete();
    auto delete_strategy = dynamic_cast<Deletes*>(query->strategy());
    RETURN_NOT_OK(delete_from_capnp(delete_reader, query, delete_strategy));
  }

  // Deserialize status. This must come last because various setters above
  // will reset it.
  QueryStatus query_status = QueryStatus::UNINITIALIZED;
  RETURN_NOT_OK(
      query_status_enum(query_reader.getStatus().cStr(), &query_status));
  query->set_status(query_status);

  // If cap'n proto object has stats set it on c++ object
  if (query_reader.hasStats()) {
    stats::Stats* stats = query->stats();
    // We should always have a stats here
    if (stats != nullptr) {
      RETURN_NOT_OK(stats_from_capnp(query_reader.getStats(), stats));
    }
  }

  if (query_reader.hasWrittenFragmentInfo()) {
    auto reader_list = query_reader.getWrittenFragmentInfo();
    auto& written_fi = query->get_written_fragment_info();
    for (auto fi : reader_list) {
      written_fi.emplace_back(
          URI(fi.getUri().cStr()),
          std::make_pair(fi.getTimestampRange()[0], fi.getTimestampRange()[1]));
    }
  }

  return Status::Ok();
}

Status query_serialize(
    Query* query,
    SerializationType serialize_type,
    bool clientside,
    BufferList* serialized_buffer) {
  if (serialize_type == SerializationType::JSON)
    return LOG_STATUS(Status_SerializationError(
        "Cannot serialize query; json format not supported."));

  try {
    ::capnp::MallocMessageBuilder message;
    capnp::Query::Builder query_builder = message.initRoot<capnp::Query>();
    RETURN_NOT_OK(query_to_capnp(*query, &query_builder, clientside));

    // Determine whether we should be serializing the buffer data.
    const bool serialize_buffers =
        (clientside && query->type() == QueryType::WRITE) ||
        (clientside && query->type() == QueryType::MODIFY_EXCLUSIVE) ||
        (!clientside && query->type() == QueryType::READ);

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(query_builder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        Buffer header;
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(header.realloc(json_len + 1));
        RETURN_NOT_OK(header.write(capnp_json.cStr(), json_len));
        RETURN_NOT_OK(header.write(&nul, 1));
        RETURN_NOT_OK(serialized_buffer->add_buffer(std::move(header)));
        // TODO: At this point the buffer data should also be serialized.
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();

        // Write the serialized query
        Buffer header;
        RETURN_NOT_OK(header.realloc(message_chars.size()));
        RETURN_NOT_OK(
            header.write(message_chars.begin(), message_chars.size()));
        RETURN_NOT_OK(serialized_buffer->add_buffer(std::move(header)));

        // Concatenate buffers to end of message
        if (serialize_buffers) {
          auto attr_buffer_builders = query_builder.getAttributeBufferHeaders();
          for (auto attr_buffer_builder : attr_buffer_builders) {
            const std::string name = attr_buffer_builder.getName().cStr();

            auto query_buffer = query->buffer(name);

            if (query_buffer.buffer_var_size_ != nullptr &&
                query_buffer.buffer_var_ != nullptr) {
              Buffer offsets(query_buffer.buffer_, *query_buffer.buffer_size_);
              RETURN_NOT_OK(serialized_buffer->add_buffer(std::move(offsets)));
              Buffer data(
                  query_buffer.buffer_var_, *query_buffer.buffer_var_size_);
              RETURN_NOT_OK(serialized_buffer->add_buffer(std::move(data)));
            } else if (
                query_buffer.buffer_size_ != nullptr &&
                query_buffer.buffer_ != nullptr) {
              Buffer data(query_buffer.buffer_, *query_buffer.buffer_size_);
              RETURN_NOT_OK(serialized_buffer->add_buffer(std::move(data)));
            } else {
              assert(false);
            }

            if (query_buffer.validity_vector_.buffer_size() != nullptr) {
              Buffer validity(
                  query_buffer.validity_vector_.buffer(),
                  *query_buffer.validity_vector_.buffer_size());
              RETURN_NOT_OK(serialized_buffer->add_buffer(std::move(validity)));
            }
          }
        }

        break;
      }
      default:
        return LOG_STATUS(Status_SerializationError(
            "Cannot serialize; unknown serialization type"));
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot serialize; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot serialize; exception: " + std::string(e.what())));
  }

  return Status::Ok();
}

Status do_query_deserialize(
    const Buffer& serialized_buffer,
    SerializationType serialize_type,
    const SerializationContext context,
    CopyState* const copy_state,
    Query* query,
    ThreadPool* compute_tp) {
  if (serialize_type == SerializationType::JSON)
    return LOG_STATUS(Status_SerializationError(
        "Cannot deserialize query; json format not supported."));

  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::Query::Builder query_builder =
            message_builder.initRoot<capnp::Query>();
        json.decode(
            kj::StringPtr(
                static_cast<const char*>(serialized_buffer.cur_data())),
            query_builder);
        capnp::Query::Reader query_reader = query_builder.asReader();
        return query_from_capnp(
            query_reader, context, nullptr, copy_state, query, compute_tp);
      }
      case SerializationType::CAPNP: {
        // Capnp FlatArrayMessageReader requires 64-bit alignment.
        if (!utils::is_aligned<sizeof(uint64_t)>(serialized_buffer.cur_data()))
          return LOG_STATUS(Status_SerializationError(
              "Could not deserialize query; buffer is not 8-byte aligned."));

        // Set traversal limit to 10GI (TODO: make this a config option)
        ::capnp::ReaderOptions readerOptions;
        readerOptions.traversalLimitInWords = uint64_t(1024) * 1024 * 1024 * 10;
        ::capnp::FlatArrayMessageReader reader(
            kj::arrayPtr(
                reinterpret_cast<const ::capnp::word*>(
                    serialized_buffer.cur_data()),
                (serialized_buffer.size() - serialized_buffer.offset()) /
                    sizeof(::capnp::word)),
            readerOptions);

        capnp::Query::Reader query_reader = reader.getRoot<capnp::Query>();

        // Get a pointer to the start of the attribute buffer data (which
        // was concatenated after the CapnP message on serialization).
        auto attribute_buffer_start = reader.getEnd();
        auto buffer_start = const_cast<::capnp::word*>(attribute_buffer_start);
        return query_from_capnp(
            query_reader, context, buffer_start, copy_state, query, compute_tp);
      }
      default:
        return LOG_STATUS(Status_SerializationError(
            "Cannot deserialize; unknown serialization type."));
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot deserialize; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot deserialize; exception: " + std::string(e.what())));
  }
  return Status::Ok();
}

Status query_deserialize(
    const Buffer& serialized_buffer,
    SerializationType serialize_type,
    bool clientside,
    CopyState* copy_state,
    Query* query,
    ThreadPool* compute_tp) {
  // Create an original, serialized copy of the 'query' that we will revert
  // to if we are unable to deserialize 'serialized_buffer'.
  BufferList original_bufferlist;
  RETURN_NOT_OK(
      query_serialize(query, serialize_type, clientside, &original_bufferlist));

  // The first buffer is always the serialized Query object.
  tiledb::sm::Buffer* original_buffer;
  RETURN_NOT_OK(original_bufferlist.get_buffer(0, &original_buffer));
  original_buffer->reset_offset();

  // Similarly, we must create a copy of 'copy_state'.
  tdb_unique_ptr<CopyState> original_copy_state = nullptr;
  if (copy_state) {
    original_copy_state =
        tdb_unique_ptr<CopyState>(tdb_new(CopyState, *copy_state));
  }

  // Deserialize 'serialized_buffer'.
  const Status st = do_query_deserialize(
      serialized_buffer,
      serialize_type,
      clientside ? SerializationContext::CLIENT : SerializationContext::SERVER,
      copy_state,
      query,
      compute_tp);

  // If the deserialization failed, deserialize 'serialized_query_original'
  // into 'query' to ensure that 'query' is in the state it was before the
  // deserialization of 'serialized_buffer' failed.
  if (!st.ok()) {
    if (original_copy_state) {
      *copy_state = *original_copy_state;
    } else {
      copy_state = NULL;
    }

    const Status st2 = do_query_deserialize(
        *original_buffer,
        serialize_type,
        SerializationContext::BACKUP,
        copy_state,
        query,
        compute_tp);
    if (!st2.ok()) {
      LOG_ERROR(st2.message());
      return st2;
    }
  }

  return st;
}

Status query_est_result_size_reader_to_capnp(
    Query& query,
    capnp::EstimatedResultSize::Builder* est_result_size_builder) {
  using namespace tiledb::sm;

  auto est_buffer_size_map = query.get_est_result_size_map();
  auto max_mem_size_map = query.get_max_mem_size_map();

  auto result_sizes_builder = est_result_size_builder->initResultSizes();
  auto result_sizes_builder_entries =
      result_sizes_builder.initEntries(est_buffer_size_map.size());
  int i = 0;
  for (auto& it : est_buffer_size_map) {
    auto range_builder = result_sizes_builder_entries[i];
    range_builder.setKey(it.first);
    capnp::EstimatedResultSize::ResultSize::Builder result_size_builder =
        range_builder.initValue();
    result_size_builder.setSizeFixed(it.second.size_fixed_);
    result_size_builder.setSizeVar(it.second.size_var_);
    result_size_builder.setSizeValidity(it.second.size_validity_);
    ++i;
  }

  auto memory_sizes_builder = est_result_size_builder->initMemorySizes();
  auto memory_sizes_builder_entries =
      memory_sizes_builder.initEntries(est_buffer_size_map.size());
  i = 0;
  for (auto& it : max_mem_size_map) {
    auto range_builder = memory_sizes_builder_entries[i];
    range_builder.setKey(it.first);
    capnp::EstimatedResultSize::MemorySize::Builder result_size_builder =
        range_builder.initValue();
    result_size_builder.setSizeFixed(it.second.size_fixed_);
    result_size_builder.setSizeVar(it.second.size_var_);
    result_size_builder.setSizeValidity(it.second.size_validity_);
    ++i;
  }

  return Status::Ok();
}

Status query_est_result_size_reader_from_capnp(
    const capnp::EstimatedResultSize::Reader& est_result_size_reader,
    Query* const query) {
  using namespace tiledb::sm;

  auto est_result_sizes = est_result_size_reader.getResultSizes();
  auto max_memory_sizes = est_result_size_reader.getMemorySizes();

  std::unordered_map<std::string, Subarray::ResultSize> est_result_sizes_map;
  for (auto it : est_result_sizes.getEntries()) {
    std::string name = it.getKey();
    auto result_size = it.getValue();
    est_result_sizes_map.emplace(
        name,
        Subarray::ResultSize{result_size.getSizeFixed(),
                             result_size.getSizeVar(),
                             result_size.getSizeValidity()});
  }

  std::unordered_map<std::string, Subarray::MemorySize> max_memory_sizes_map;
  for (auto it : max_memory_sizes.getEntries()) {
    std::string name = it.getKey();
    auto memory_size = it.getValue();
    max_memory_sizes_map.emplace(
        name,
        Subarray::MemorySize{memory_size.getSizeFixed(),
                             memory_size.getSizeVar(),
                             memory_size.getSizeValidity()});
  }

  return query->set_est_result_size(est_result_sizes_map, max_memory_sizes_map);
}

Status query_est_result_size_serialize(
    Query* query,
    SerializationType serialize_type,
    bool clientside,
    Buffer* serialized_buffer) {
  (void)clientside;
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::EstimatedResultSize::Builder est_result_size_builder =
        message.initRoot<capnp::EstimatedResultSize>();
    RETURN_NOT_OK(query_est_result_size_reader_to_capnp(
        *query, &est_result_size_builder));

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(est_result_size_builder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(serialized_buffer->realloc(json_len + 1));
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len));
        RETURN_NOT_OK(serialized_buffer->write(&nul, 1));
        break;
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();

        // Write the serialized query estimated results
        const auto nbytes = message_chars.size();
        RETURN_NOT_OK(serialized_buffer->realloc(nbytes));
        RETURN_NOT_OK(serialized_buffer->write(message_chars.begin(), nbytes));
        break;
      }
      default:
        return LOG_STATUS(Status_SerializationError(
            "Cannot serialize; unknown serialization type"));
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot serialize; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot serialize; exception: " + std::string(e.what())));
  }

  return Status::Ok();
}

Status query_est_result_size_deserialize(
    Query* query,
    SerializationType serialize_type,
    bool clientside,
    const Buffer& serialized_buffer) {
  (void)clientside;
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::EstimatedResultSize::Builder estimated_result_size_builder =
            message_builder.initRoot<capnp::EstimatedResultSize>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            estimated_result_size_builder);
        capnp::EstimatedResultSize::Reader estimated_result_size_reader =
            estimated_result_size_builder.asReader();
        RETURN_NOT_OK(query_est_result_size_reader_from_capnp(
            estimated_result_size_reader, query));
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::EstimatedResultSize::Reader estimated_result_size_reader =
            reader.getRoot<capnp::EstimatedResultSize>();
        RETURN_NOT_OK(query_est_result_size_reader_from_capnp(
            estimated_result_size_reader, query));
        break;
      }
      default: {
        return LOG_STATUS(
            Status_SerializationError("Error deserializing query est result "
                                      "size; Unknown serialization type "
                                      "passed"));
      }
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing query est result size; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing query est result size; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status global_write_state_to_capnp(
    const Query& query,
    GlobalOrderWriter& globalwriter,
    capnp::GlobalWriteState::Builder* state_builder) {
  auto& write_state = *globalwriter.get_global_state();

  auto& cells_written = write_state.cells_written_;
  if (!cells_written.empty()) {
    auto cells_writen_builder = state_builder->initCellsWritten();
    auto entries_builder =
        cells_writen_builder.initEntries(cells_written.size());
    uint64_t index = 0;
    for (auto& entry : cells_written) {
      entries_builder[index].setKey(entry.first);
      entries_builder[index].setValue(entry.second);
      ++index;
    }
  }

  if (write_state.frag_meta_) {
    auto frag_meta = write_state.frag_meta_;
    auto frag_meta_builder = state_builder->initFragMeta();
    RETURN_NOT_OK(fragment_metadata_to_capnp(*frag_meta, &frag_meta_builder));
  }

  if (write_state.last_cell_coords_.has_value()) {
    auto& single_coord = write_state.last_cell_coords_.value();
    auto coord_builder = state_builder->initLastCellCoords();

    auto& coords = single_coord.get_coords();
    if (!coords.empty()) {
      auto builder = coord_builder.initCoords(coords.size());
      for (uint64_t i = 0; i < coords.size(); ++i) {
        builder.init(i, coords[i].size());
        for (uint64_t j = 0; j < coords.size(); ++j) {
          builder[i].set(j, coords[i][j]);
        }
      }
    }
    auto& sizes = single_coord.get_sizes();
    if (!sizes.empty()) {
      auto builder = coord_builder.initSizes(sizes.size());
      for (uint64_t i = 0; i < sizes.size(); ++i) {
        builder.set(i, sizes[i]);
      }
    }
    auto& single_offset = single_coord.get_single_offset();
    if (!single_offset.empty()) {
      auto builder = coord_builder.initSingleOffset(single_offset.size());
      for (uint64_t i = 0; i < single_offset.size(); ++i) {
        builder.set(i, single_offset[i]);
      }
    }
  }

  state_builder->setLastHilbertValue(write_state.last_hilbert_value_);

  // Serialize the multipart upload state
  auto&& [st, multipart_states] = globalwriter.multipart_upload_state();
  RETURN_NOT_OK(st);

  if (!multipart_states.empty()) {
    auto multipart_builder = state_builder->initMultiPartUploadStates();
    auto entries_builder =
        multipart_builder.initEntries(multipart_states.size());
    uint64_t index = 0;
    for (auto& entry : multipart_states) {
      entries_builder[index].setKey(entry.first);
      auto multipart_entry_builder = entries_builder[index].initValue();
      ++index;
      auto& state = entry.second;
      multipart_entry_builder.setPartNumber(state.part_number);
      if (state.upload_id.has_value()) {
        multipart_entry_builder.setUploadId(*state.upload_id);
      }
      if (state.bucket.has_value()) {
        multipart_entry_builder.setBucket(*state.bucket);
      }
      if (state.s3_key.has_value()) {
        multipart_entry_builder.setS3Key(*state.s3_key);
      }
      if (!state.status.ok()) {
        multipart_entry_builder.setStatus(state.status.message());
      }

      auto& completed_parts = state.completed_parts;
      // Get completed parts
      if (!completed_parts.empty()) {
        auto builder = multipart_entry_builder.initCompletedParts(
            state.completed_parts.size());
        for (uint64_t i = 0; i < state.completed_parts.size(); ++i) {
          if (state.completed_parts[i].e_tag.has_value()) {
            builder[i].setETag(*state.completed_parts[i].e_tag);
          }
          builder[i].setPartNumber(state.completed_parts[i].part_number);
        }
      }
    }
  }

  return Status::Ok();
}

Status global_write_state_from_capnp(
    const Query& query,
    const capnp::GlobalWriteState::Reader& state_reader,
    GlobalOrderWriter* globalwriter) {
  auto write_state = globalwriter->get_global_state();

  if (state_reader.hasCellsWritten()) {
    auto& cells_written = write_state->cells_written_;
    auto cell_written_reader = state_reader.getCellsWritten();
    for (const auto& entry : cell_written_reader.getEntries()) {
      cells_written[std::string(entry.getKey().cStr())] = entry.getValue();
    }
  }

  if (state_reader.hasLastCellCoords()) {
    auto single_coord_reader = state_reader.getLastCellCoords();
    if (single_coord_reader.hasCoords() &&
        single_coord_reader.hasSingleOffset() &&
        single_coord_reader.hasSizes()) {
      std::vector<std::vector<uint8_t>> coords;
      std::vector<uint64_t> sizes;
      std::vector<uint64_t> single_offset;
      for (const auto& t : single_coord_reader.getCoords()) {
        auto& last = coords.emplace_back();
        last.reserve(t.size());
        for (const auto& v : t) {
          last.emplace_back(v);
        }
      }

      sizes.reserve(single_coord_reader.getSizes().size());
      for (const auto& size : single_coord_reader.getSizes()) {
        sizes.emplace_back(size);
      }
      single_offset.reserve(single_coord_reader.getSingleOffset().size());
      for (const auto& so : single_coord_reader.getSingleOffset()) {
        single_offset.emplace_back(so);
      }

      write_state->last_cell_coords_.emplace(
          query.array_schema(), coords, sizes, single_offset);
    }
  }

  write_state->last_hilbert_value_ = state_reader.getLastHilbertValue();

  if (state_reader.hasFragMeta()) {
    auto frag_meta = write_state->frag_meta_;
    auto frag_meta_reader = state_reader.getFragMeta();
    RETURN_NOT_OK(fragment_metadata_from_capnp(
        query.array_schema_shared(), frag_meta_reader, frag_meta));
  }

  // Deserialize the multipart upload state
  if (state_reader.hasMultiPartUploadStates()) {
    auto multipart_reader = state_reader.getMultiPartUploadStates();
    if (multipart_reader.hasEntries()) {
      for (auto entry : multipart_reader.getEntries()) {
        VFS::MultiPartUploadState deserialized_state;
        auto state = entry.getValue();
        auto uri = URI(entry.getKey().cStr());
        if (state.hasUploadId()) {
          deserialized_state.upload_id = state.getUploadId();
        }
        if (state.hasBucket()) {
          deserialized_state.bucket = state.getBucket();
        }
        if (state.hasS3Key()) {
          deserialized_state.s3_key = state.getS3Key();
        }
        if (state.hasStatus()) {
          deserialized_state.status =
              Status(std::string(state.getStatus().cStr()), "");
        }
        deserialized_state.part_number = entry.getValue().getPartNumber();
        if (state.hasCompletedParts()) {
          auto& parts = deserialized_state.completed_parts;
          for (auto part : state.getCompletedParts()) {
            parts.emplace_back();
            parts.back().part_number = part.getPartNumber();
            if (part.hasETag()) {
              parts.back().e_tag = std::string(part.getETag().cStr());
            }
          }
        }

        RETURN_NOT_OK(
            globalwriter->set_multipart_upload_state(uri, deserialized_state));
      }
    }
  }

  return Status::Ok();
}

#else

Status query_serialize(Query*, SerializationType, bool, BufferList*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status query_deserialize(
    const Buffer&, SerializationType, bool, CopyState*, Query*, ThreadPool*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

Status query_est_result_size_serialize(
    Query*, SerializationType, bool, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status query_est_result_size_deserialize(
    Query*, SerializationType, bool, const Buffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
