/**
 * @file query.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/sm/serialization/query_aggregates.h"
#endif
// clang-format on

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
#include "tiledb/sm/query/ast/query_ast.h"
#include "tiledb/sm/query/deletes_and_updates/deletes_and_updates.h"
#include "tiledb/sm/query/legacy/reader.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/query_remote_buffer_storage.h"
#include "tiledb/sm/query/readers/dense_reader.h"
#include "tiledb/sm/query/readers/ordered_dim_label_reader.h"
#include "tiledb/sm/query/readers/sparse_global_order_reader.h"
#include "tiledb/sm/query/readers/sparse_unordered_with_dups_reader.h"
#include "tiledb/sm/query/writers/global_order_writer.h"
#include "tiledb/sm/query/writers/unordered_writer.h"
#include "tiledb/sm/query/writers/writer_base.h"
#include "tiledb/sm/serialization/config.h"
#include "tiledb/sm/serialization/fragment_metadata.h"
#include "tiledb/sm/serialization/query.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb::sm::serialization {

#ifdef TILEDB_SERIALIZATION

shared_ptr<Logger> dummy_logger = make_shared<Logger>(HERE(), "");

void stats_to_capnp(const Stats& stats, capnp::Stats::Builder* stats_builder) {
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
}

StatsData stats_from_capnp(const capnp::Stats::Reader& stats_reader) {
  std::unordered_map<std::string, uint64_t> counters;
  std::unordered_map<std::string, double> timers;

  if (stats_reader.hasCounters()) {
    auto counters_reader = stats_reader.getCounters();
    for (const auto entry : counters_reader.getEntries()) {
      auto key = std::string_view{entry.getKey().cStr(), entry.getKey().size()};
      counters[std::string(key)] = entry.getValue();
    }
  }

  if (stats_reader.hasTimers()) {
    auto timers_reader = stats_reader.getTimers();
    for (const auto entry : timers_reader.getEntries()) {
      auto key = std::string_view{entry.getKey().cStr(), entry.getKey().size()};
      timers[std::string(key)] = entry.getValue();
    }
  }

  return stats::StatsData(counters, timers);
}

void range_buffers_to_capnp(
    const std::vector<Range>& ranges,
    capnp::SubarrayRanges::Builder& range_builder) {
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

std::vector<Range> range_buffers_from_capnp(
    capnp::SubarrayRanges::Reader& range_reader) {
  auto data_ptr = range_reader.getBuffer().asBytes();
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

  return ranges;
}

Status subarray_to_capnp(
    const ArraySchema& schema,
    const Subarray* subarray,
    capnp::Subarray::Builder* builder) {
  builder->setLayout(layout_str(subarray->layout()));
  builder->setCoalesceRanges(subarray->coalesce_ranges());

  // Add ranges
  const uint32_t dim_num = subarray->dim_num();
  auto ranges_builder = builder->initRanges(dim_num);
  for (uint32_t i = 0; i < dim_num; i++) {
    const auto datatype{schema.dimension_ptr(i)->type()};
    auto range_builder = ranges_builder[i];
    const auto& ranges = subarray->ranges_for_dim(i);
    range_builder.setType(datatype_str(datatype));
    range_builder.setHasDefaultRange(subarray->is_default(i));
    range_buffers_to_capnp(ranges, range_builder);
  }

  // Add label ranges
  const auto label_ranges_num = subarray->label_ranges_num();
  if (label_ranges_num > 0) {
    auto label_ranges_builder = builder->initLabelRanges(label_ranges_num);
    auto label_id = 0;
    for (uint32_t i = 0; i < dim_num; i++) {
      if (subarray->has_label_ranges(i)) {
        auto label_range_builder = label_ranges_builder[label_id++];
        if (label_id > label_ranges_num) {
          throw StatusException(
              Status_SerializationError("Label id exceeds the total number of "
                                        "label ranges for the subarray."));
        }
        label_range_builder.setDimensionId(i);
        const auto& label_name = subarray->get_label_name(i);
        label_range_builder.setName(label_name);

        auto range_builder = label_range_builder.initRanges();
        const auto datatype{schema.dimension_ptr(i)->type()};
        const auto& label_ranges = subarray->ranges_for_label(label_name);
        range_builder.setType(datatype_str(datatype));
        range_buffers_to_capnp(label_ranges, range_builder);
      }
    }
  }

  // Add attribute ranges
  const auto& attr_ranges = subarray->get_attribute_ranges();
  if (attr_ranges.size() > 0) {
    auto attribute_ranges_builder = builder->initAttributeRanges();
    auto entries_builder =
        attribute_ranges_builder.initEntries(attr_ranges.size());
    size_t i = 0;
    for (const auto& attr_range : attr_ranges) {
      auto entry = entries_builder[i++];
      entry.setKey(attr_range.first);
      auto attr_range_builder = entry.initValue();
      range_buffers_to_capnp(attr_range.second, attr_range_builder);
    }
  }

  // If stats object exists set its cap'n proto object
  const auto& stats = subarray->stats();
  auto stats_builder = builder->initStats();
  stats_to_capnp(stats, &stats_builder);

  if (subarray->relevant_fragments().relevant_fragments_size() > 0) {
    auto relevant_fragments_builder = builder->initRelevantFragments(
        subarray->relevant_fragments().relevant_fragments_size());
    for (size_t i = 0;
         i < subarray->relevant_fragments().relevant_fragments_size();
         ++i) {
      relevant_fragments_builder.set(i, subarray->relevant_fragments()[i]);
    }
  }

  return Status::Ok();
}

Status subarray_from_capnp(
    const capnp::Subarray::Reader& reader, Subarray* subarray) {
  subarray->set_coalesce_ranges(reader.getCoalesceRanges());
  auto ranges_reader = reader.getRanges();
  uint32_t dim_num = ranges_reader.size();
  for (uint32_t i = 0; i < dim_num; i++) {
    auto range_reader = ranges_reader[i];

    auto data = range_reader.getBuffer();
    auto data_ptr = data.asBytes();
    if (range_reader.hasBufferSizes()) {
      auto ranges = range_buffers_from_capnp(range_reader);
      subarray->set_ranges_for_dim(i, ranges);

      // Set default indicator
      subarray->set_is_default(i, range_reader.getHasDefaultRange());
    } else {
      // Handle 1.7 style ranges where there is a single range with no sizes
      Range range(data_ptr.begin(), data.size());
      subarray->set_ranges_for_dim(i, {range});
      subarray->set_is_default(i, range_reader.getHasDefaultRange());
    }
  }

  if (reader.hasLabelRanges()) {
    subarray->add_default_label_ranges(dim_num);
    auto label_ranges_reader = reader.getLabelRanges();
    uint32_t label_num = label_ranges_reader.size();
    for (uint32_t i = 0; i < label_num; i++) {
      auto label_range_reader = label_ranges_reader[i];
      auto dim_id = label_range_reader.getDimensionId();
      auto label_name = label_range_reader.getName();

      // Deserialize ranges for this dim label
      auto range_reader = label_range_reader.getRanges();
      auto ranges = range_buffers_from_capnp(range_reader);

      // Set ranges for this dim label on the subarray
      subarray->set_label_ranges_for_dim(dim_id, label_name, ranges);
    }
  }

  if (reader.hasAttributeRanges()) {
    std::unordered_map<std::string, std::vector<Range>> attr_ranges;
    auto attr_ranges_reader = reader.getAttributeRanges();
    if (attr_ranges_reader.hasEntries()) {
      for (auto attr_ranges_entry : attr_ranges_reader.getEntries()) {
        auto range_reader = attr_ranges_entry.getValue();
        auto key = std::string_view{
            attr_ranges_entry.getKey().cStr(),
            attr_ranges_entry.getKey().size()};
        attr_ranges[std::string{key}] = range_buffers_from_capnp(range_reader);
      }
    }

    for (const auto& attr_range : attr_ranges)
      subarray->set_attribute_ranges(attr_range.first, attr_range.second);
  }

  // If cap'n proto object has stats set it on c++ object
  if (reader.hasStats()) {
    auto stats_data = stats_from_capnp(reader.getStats());
    subarray->set_stats(stats_data);
  }

  if (reader.hasRelevantFragments()) {
    auto relevant_fragments = reader.getRelevantFragments();
    size_t count = relevant_fragments.size();
    std::vector<unsigned> rf;
    rf.reserve(count);
    for (size_t i = 0; i < count; i++) {
      rf.emplace_back(relevant_fragments[i]);
    }

    subarray->relevant_fragments() = RelevantFragments(rf);
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
  const auto& stats = partitioner.stats();
  auto stats_builder = builder->initStats();
  stats_to_capnp(stats, &stats_builder);

  return Status::Ok();
}

Status subarray_partitioner_from_capnp(
    Stats* query_stats,
    Stats* reader_stats,
    const Config& config,
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
  Subarray subarray(array, layout, query_stats, dummy_logger, true);
  RETURN_NOT_OK(subarray_from_capnp(reader.getSubarray(), &subarray));
  *partitioner = SubarrayPartitioner(
      &config,
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
        Subarray(array, layout, query_stats, dummy_logger, true);
    RETURN_NOT_OK(subarray_from_capnp(
        partition_info_reader.getSubarray(), &partition_info->partition_));

    if (compute_current_tile_overlap) {
      partition_info->partition_.precompute_tile_overlap(
          partition_info->start_,
          partition_info->end_,
          &config,
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
        array, layout, query_stats, dummy_logger, true);
    Subarray& subarray_ = state->single_range_.back();
    RETURN_NOT_OK(subarray_from_capnp(subarray_reader_, &subarray_));
  }
  auto m_reader = state_reader.getMultiRange();
  const unsigned num_m = m_reader.size();
  for (unsigned i = 0; i < num_m; i++) {
    auto subarray_reader_ = m_reader[i];
    state->multi_range_.emplace_back(
        array, layout, query_stats, dummy_logger, true);
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
    auto stats_data = stats_from_capnp(reader.getStats());
    partitioner->set_stats(stats_data);
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
    const SparseIndexReaderBase::ReadState& read_state,
    capnp::ReaderIndex::Builder* builder) {
  auto read_state_builder = builder->initReadState();

  read_state_builder.setDoneAddingResultTiles(
      read_state.done_adding_result_tiles());

  auto frag_tile_idx_builder =
      read_state_builder.initFragTileIdx(read_state.frag_idx().size());
  for (size_t i = 0; i < read_state.frag_idx().size(); ++i) {
    frag_tile_idx_builder[i].setTileIdx(read_state.frag_idx()[i].tile_idx_);
    frag_tile_idx_builder[i].setCellIdx(read_state.frag_idx()[i].cell_idx_);
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
    ThreadPool* compute_tp,
    bool client_side) {
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
        read_state->unsplittable_ && !client_side));
  }

  return Status::Ok();
}

tiledb::sm::SparseIndexReaderBase::ReadState index_read_state_from_capnp(
    const ArraySchema& schema,
    const capnp::ReadStateIndex::Reader& read_state_reader) {
  bool done_reading = read_state_reader.getDoneAddingResultTiles();
  assert(read_state_reader.hasFragTileIdx());
  std::vector<FragIdx> fragment_indexes;
  for (const auto rcs : read_state_reader.getFragTileIdx()) {
    auto tile_idx = rcs.getTileIdx();
    auto cell_idx = rcs.getCellIdx();

    fragment_indexes.emplace_back(tile_idx, cell_idx);
  }

  return tiledb::sm::SparseIndexReaderBase::ReadState(
      std::move(fragment_indexes), done_reading);
}

Status dense_read_state_from_capnp(
    const Array* array,
    const capnp::ReadState::Reader& read_state_reader,
    Query* query,
    DenseReader* reader,
    ThreadPool* compute_tp,
    bool client_side) {
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
        read_state->unsplittable_ && !client_side));
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

    // Copy the condition data into a capnp vector of bytes.
    auto& data = node->get_data();
    auto capnpData = kj::Vector<uint8_t>();
    capnpData.addAll(
        kj::ArrayPtr<uint8_t>(const_cast<uint8_t*>(data.data()), data.size()));

    // Validate and store the condition value vector of bytes.
    ensure_qc_capnp_condition_value_is_valid(capnpData);
    ast_builder->setValue(capnpData.asPtr());

    if (node->get_op() == QueryConditionOp::IN ||
        node->get_op() == QueryConditionOp::NOT_IN) {
      // Copy the condition offsets into a capnp vector of bytes
      auto& offsets = node->get_offsets();
      auto capnpOffsets = kj::Vector<uint8_t>();
      capnpOffsets.addAll(kj::ArrayPtr<uint8_t>(
          const_cast<uint8_t*>(offsets.data()), offsets.size()));

      // Validate and store the condition value offsets
      ensure_qc_capnp_condition_value_is_valid(capnpOffsets);
      ast_builder->setOffsets(capnpOffsets.asPtr());
    }

    // Validate and store the query condition op.
    const std::string op_str = query_condition_op_str(node->get_op());
    ensure_qc_op_string_is_valid(op_str);
    ast_builder->setOp(op_str);

    // Store whether this expression should skip the enumeration lookup.
    ast_builder->setUseEnumeration(node->use_enumeration());
  } else {
    // Store the boolean expression tag.
    ast_builder->setIsExpression(true);

    // We assume that the serialized values of the child nodes are validated
    // properly.
    auto children_builder =
        ast_builder->initChildren(node->get_children().size());

    for (size_t i = 0; i < node->get_children().size(); ++i) {
      auto child_builder = children_builder[i];
      throw_if_not_ok(
          condition_ast_to_capnp(node->get_children()[i], &child_builder));
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
  auto& data = node->get_data();
  auto capnpData = kj::Vector<uint8_t>();
  capnpData.addAll(
      kj::ArrayPtr<uint8_t>(const_cast<uint8_t*>(data.data()), data.size()));

  // Validate and store the condition value vector of bytes.
  ensure_qc_capnp_condition_value_is_valid(capnpData);
  clause_builder->setValue(capnpData.asPtr());

  // Validate and store the query condition op.
  const std::string op_str = query_condition_op_str(node->get_op());
  ensure_qc_op_string_is_valid(op_str);
  clause_builder->setOp(op_str);

  clause_builder->setUseEnumeration(node->use_enumeration());
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
  if (condition.has_value()) {
    auto condition_builder = reader_builder->initCondition();
    RETURN_NOT_OK(condition_to_capnp(condition.value(), &condition_builder));
  }

  // If stats object exists set its cap'n proto object
  const auto& stats = *reader.stats();
  auto stats_builder = reader_builder->initStats();
  stats_to_capnp(stats, &stats_builder);

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
  if (condition.has_value()) {
    auto condition_builder = reader_builder->initCondition();
    RETURN_NOT_OK(condition_to_capnp(condition.value(), &condition_builder));
  }

  // If stats object exists set its cap'n proto object
  const auto& stats = *reader.stats();
  auto stats_builder = reader_builder->initStats();
  stats_to_capnp(stats, &stats_builder);

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
  if (condition.has_value()) {
    auto condition_builder = reader_builder->initCondition();
    RETURN_NOT_OK(condition_to_capnp(condition.value(), &condition_builder));
  }

  // If stats object exists set its cap'n proto object
  const auto& stats = *reader.stats();
  auto stats_builder = reader_builder->initStats();
  stats_to_capnp(stats, &stats_builder);

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

    // Getting and validating the condition offsets.
    const void* offsets = nullptr;
    size_t offsets_size = 0;
    if (ast_reader.hasOffsets()) {
      auto condition_offsets = ast_reader.getOffsets();
      offsets = static_cast<const void*>(condition_offsets.asBytes().begin());
      offsets_size = condition_offsets.size();
      ensure_qc_condition_value_is_valid(offsets, offsets_size);
    }

    // Getting and validating the query condition operator.
    QueryConditionOp op = QueryConditionOp::LT;
    Status s = query_condition_op_enum(ast_reader.getOp(), &op);
    if (!s.ok()) {
      throw std::runtime_error(
          "condition_ast_from_capnp: query_condition_op_enum failed.");
    }
    ensure_qc_op_is_valid(op);

    auto use_enumeration = ast_reader.getUseEnumeration();

    if (op == QueryConditionOp::IN || op == QueryConditionOp::NOT_IN) {
      return tdb_unique_ptr<ASTNode>(tdb_new(
          ASTNodeVal,
          field_name,
          size == 0 ? nullptr : data,
          size,
          offsets,
          offsets_size,
          op,
          use_enumeration));
    }

    return tdb_unique_ptr<ASTNode>(
        tdb_new(ASTNodeVal, field_name, data, size, op, use_enumeration));
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

QueryCondition condition_from_capnp(
    const capnp::Condition::Reader& condition_reader) {
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

      bool use_enumeration = clause.getUseEnumeration();

      ast_nodes.push_back(tdb_unique_ptr<ASTNode>(
          tdb_new(ASTNodeVal, field_name, data, size, op, use_enumeration)));
    }

    // Constructing the tree from the list of AST nodes.
    assert(ast_nodes.size() > 0);
    if (ast_nodes.size() == 1) {
      return QueryCondition(std::move(ast_nodes[0]));
    } else {
      auto tree_ptr = tdb_unique_ptr<ASTNode>(tdb_new(
          ASTNodeExpr, std::move(ast_nodes), QueryConditionCombinationOp::AND));
      return QueryCondition(std::move(tree_ptr));
    }
  } else if (condition_reader.hasTree()) {
    // Constructing the query condition from the AST representation.
    // We assume that the deserialized values of the AST are validated properly.
    auto ast_reader = condition_reader.getTree();
    return QueryCondition(condition_ast_from_capnp(ast_reader));
  }
  throw std::runtime_error(
      "condition_from_capnp: serialized QC has no tree or clauses.");
}

Status reader_from_capnp(
    const capnp::QueryReader::Reader& reader_reader,
    Query* query,
    Reader* reader,
    ThreadPool* compute_tp,
    bool client_side) {
  auto array = query->array();

  // Layout
  Layout layout = Layout::ROW_MAJOR;
  RETURN_NOT_OK(layout_enum(reader_reader.getLayout(), &layout));

  // Subarray
  Subarray subarray(array, layout, query->stats(), dummy_logger, true);
  auto subarray_reader = reader_reader.getSubarray();
  RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
  RETURN_NOT_OK(query->set_subarray_unsafe(subarray));

  // Read state
  if (reader_reader.hasReadState())
    RETURN_NOT_OK(read_state_from_capnp(
        array,
        reader_reader.getReadState(),
        query,
        reader,
        compute_tp,
        client_side));

  // Query condition
  if (reader_reader.hasCondition()) {
    auto condition_reader = reader_reader.getCondition();
    QueryCondition condition = condition_from_capnp(condition_reader);
    RETURN_NOT_OK(query->set_condition(condition));
  }

  // If cap'n proto object has stats set it on c++ object
  if (reader_reader.hasStats()) {
    auto stats_data = stats_from_capnp(reader_reader.getStats());
    reader->set_stats(stats_data);
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
  Subarray subarray(array, layout, query->stats(), dummy_logger, true);
  auto subarray_reader = reader_reader.getSubarray();
  RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
  RETURN_NOT_OK(query->set_subarray_unsafe(subarray));

  // Read state
  if (reader_reader.hasReadState())
    reader->set_read_state(
        index_read_state_from_capnp(schema, reader_reader.getReadState()));

  // Query condition
  if (reader_reader.hasCondition()) {
    auto condition_reader = reader_reader.getCondition();
    QueryCondition condition = condition_from_capnp(condition_reader);
    RETURN_NOT_OK(query->set_condition(condition));
  }

  // If cap'n proto object has stats set it on c++ object
  if (reader_reader.hasStats()) {
    auto stats_data = stats_from_capnp(reader_reader.getStats());
    reader->set_stats(stats_data);
  }

  return Status::Ok();
}

Status dense_reader_from_capnp(
    const ArraySchema& schema,
    const capnp::QueryReader::Reader& reader_reader,
    Query* query,
    DenseReader* reader,
    ThreadPool* compute_tp,
    bool client_side) {
  auto array = query->array();

  // Layout
  Layout layout = Layout::ROW_MAJOR;
  RETURN_NOT_OK(layout_enum(reader_reader.getLayout(), &layout));

  // Subarray
  Subarray subarray(array, layout, query->stats(), dummy_logger, true);
  auto subarray_reader = reader_reader.getSubarray();
  RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
  RETURN_NOT_OK(query->set_subarray_unsafe(subarray));

  // Read state
  if (reader_reader.hasReadState())
    RETURN_NOT_OK(dense_read_state_from_capnp(
        array,
        reader_reader.getReadState(),
        query,
        reader,
        compute_tp,
        client_side));

  // Query condition
  if (reader_reader.hasCondition()) {
    auto condition_reader = reader_reader.getCondition();
    QueryCondition condition = condition_from_capnp(condition_reader);
    RETURN_NOT_OK(query->set_condition(condition));
  }

  // If cap'n proto object has stats set it on c++ object
  if (reader_reader.hasStats()) {
    auto stats_data = stats_from_capnp(reader_reader.getStats());
    reader->set_stats(stats_data);
  }

  return Status::Ok();
}

Status delete_from_capnp(
    const capnp::Delete::Reader& delete_reader,
    Query* query,
    DeletesAndUpdates* delete_strategy) {
  // Query condition
  if (delete_reader.hasCondition()) {
    auto condition_reader = delete_reader.getCondition();
    QueryCondition condition = condition_from_capnp(condition_reader);
    RETURN_NOT_OK(query->set_condition(condition));
  }

  // If cap'n proto object has stats set it on c++ object
  if (delete_reader.hasStats()) {
    auto stats_data = stats_from_capnp(delete_reader.getStats());
    delete_strategy->set_stats(stats_data);
  }

  return Status::Ok();
}

Status delete_to_capnp(
    const Query& query,
    DeletesAndUpdates& delete_strategy,
    capnp::Delete::Builder* delete_builder) {
  auto condition = query.condition();
  if (condition.has_value()) {
    auto condition_builder = delete_builder->initCondition();
    RETURN_NOT_OK(condition_to_capnp(condition.value(), &condition_builder));
  }

  // If stats object exists set its cap'n proto object
  const auto& stats = *delete_strategy.stats();
  auto stats_builder = delete_builder->initStats();
  stats_to_capnp(stats, &stats_builder);

  return Status::Ok();
}

Status writer_to_capnp(
    const Query& query,
    WriterBase& writer,
    capnp::Writer::Builder* writer_builder,
    bool client_side) {
  writer_builder->setCheckCoordDups(writer.get_check_coord_dups());
  writer_builder->setCheckCoordOOB(writer.get_check_coord_oob());
  writer_builder->setDedupCoords(writer.get_dedup_coords());

  const auto& array_schema = query.array_schema();

  // Subarray
  const auto subarray = query.subarray();
  if (!subarray->empty() || subarray->has_label_ranges()) {
    auto subarray_builder = writer_builder->initSubarrayRanges();
    RETURN_NOT_OK(subarray_to_capnp(array_schema, subarray, &subarray_builder));
  }

  // If stats object exists set its cap'n proto object
  const auto& stats = *writer.stats();
  auto stats_builder = writer_builder->initStats();
  stats_to_capnp(stats, &stats_builder);

  if (query.layout() == Layout::GLOBAL_ORDER) {
    auto& global_writer = dynamic_cast<GlobalOrderWriter&>(writer);
    auto globalstate = global_writer.get_global_state();
    // Remote global order writes have global_write_state equal to nullptr
    // before the first submit(). The state gets initialized when do_work() gets
    // invoked. Once GlobalOrderWriter becomes C41 compliant, we can delete this
    // check.
    if (globalstate) {
      auto global_state_builder = writer_builder->initGlobalWriteStateV1();
      RETURN_NOT_OK(global_write_state_to_capnp(
          query, global_writer, &global_state_builder, client_side));
    }
  } else if (query.layout() == Layout::UNORDERED) {
    auto& unordered_writer = dynamic_cast<UnorderedWriter&>(writer);
    auto unordered_writer_state_builder =
        writer_builder->initUnorderedWriterState();
    RETURN_NOT_OK(unordered_write_state_to_capnp(
        query, unordered_writer, &unordered_writer_state_builder));
  }

  return Status::Ok();
}

Status writer_from_capnp(
    const Query& query,
    const capnp::Writer::Reader& writer_reader,
    WriterBase* writer,
    const SerializationContext context) {
  writer->set_check_coord_dups(writer_reader.getCheckCoordDups());
  writer->set_check_coord_oob(writer_reader.getCheckCoordOOB());
  writer->set_dedup_coords(writer_reader.getDedupCoords());

  // If cap'n proto object has stats set it on c++ object
  if (writer_reader.hasStats()) {
    auto stats_data = stats_from_capnp(writer_reader.getStats());
    writer->set_stats(stats_data);
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
        query, writer_reader.getGlobalWriteStateV1(), global_writer, context));
  } else if (query.layout() == Layout::UNORDERED) {
    auto unordered_writer = dynamic_cast<UnorderedWriter*>(writer);

    RETURN_NOT_OK(unordered_write_state_from_capnp(
        query,
        writer_reader.getUnorderedWriterState(),
        unordered_writer,
        context));
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
  std::vector<std::string> buffer_names =
      client_side ? query.unwritten_buffer_names() : query.buffer_names();
  const auto dim_label_names = query.dimension_label_buffer_names();
  buffer_names.insert(
      buffer_names.end(), dim_label_names.begin(), dim_label_names.end());

  // Add aggregate buffers
  const auto aggregate_names = query.aggregate_buffer_names();
  buffer_names.insert(
      buffer_names.end(), aggregate_names.begin(), aggregate_names.end());

  // TODO: add API in query to get all buffers in one go
  // Deserialization gets the list of buffers from wire then call
  // set_data_buffer which knows on which structure to set. We should have a
  // function here as well that gets all buffer names or even better, all
  // buffers

  uint64_t total_fixed_len_bytes = 0;
  uint64_t total_var_len_bytes = 0;
  uint64_t total_validity_len_bytes = 0;
  auto& query_buffer_storage = query.get_remote_buffer_cache();

  // Builder for query attribute / dimension buffers.
  // This builder also serializes dimension label buffers for the query.
  auto attr_buffers_builder =
      query_builder->initAttributeBufferHeaders(buffer_names.size());
  for (uint64_t i = 0; i < buffer_names.size(); i++) {
    const auto& name = buffer_names[i];
    const auto& buff = query.buffer(name);
    auto buffer_builder = attr_buffers_builder[i];

    // Adjust buffer sizes if cache exists for remote global order writes.
    uint64_t cached_fixed_size = 0;
    uint64_t cached_var_size = 0;
    uint64_t cached_validity_size = 0;
    // The cache will only exist if the query is a remote global order write.
    // Dimension labels do not support global order writes.
    if (!schema.is_dim_label(name) && query_buffer_storage.has_value()) {
      // Remote global order writes must be tile-aligned.
      // The cache sizes here reflect the bytes prepended to this query.
      // If all writes were tile-aligned these will be zero.
      // In the case of an unaligned write, we hold the tile overflow bytes from
      // submission by adjusting user buffer sizes, tile-aligning the write.
      // Once this write completes the cache is updated with the previously held
      // tile-overflow bytes, and the following submission is prepended with
      // this data. See the QueryRemoteBufferStorage class for more info.
      const auto& buff_cache =
          query_buffer_storage->get_query_buffer_cache(name);
      cached_fixed_size = buff_cache.buffer_.size();
      cached_var_size = buff_cache.buffer_var_.size();
      cached_validity_size = buff_cache.buffer_validity_.size();
    }

    buffer_builder.setName(name);
    if (buff.buffer_var_ != nullptr && buff.buffer_var_size_ != nullptr) {
      // Variable-sized buffer
      // Include cached bytes for remote global order writes prepend data.
      uint64_t var_buff_size = *buff.buffer_var_size_ + cached_var_size;
      total_var_len_bytes += var_buff_size;
      buffer_builder.setVarLenBufferSizeInBytes(var_buff_size);

      uint64_t offset_buff_size = *buff.buffer_size_ + cached_fixed_size;
      total_fixed_len_bytes += offset_buff_size;
      buffer_builder.setFixedLenBufferSizeInBytes(offset_buff_size);

      // Set original user requested sizes
      buffer_builder.setOriginalVarLenBufferSizeInBytes(
          buff.original_buffer_var_size_);
      buffer_builder.setOriginalFixedLenBufferSizeInBytes(
          buff.original_buffer_size_);
    } else if (buff.buffer_ != nullptr && buff.buffer_size_ != nullptr) {
      // Fixed-length buffer
      // Include cached bytes for remote global order writes prepend data.
      uint64_t buff_size = *buff.buffer_size_ + cached_fixed_size;
      total_fixed_len_bytes += buff_size;
      buffer_builder.setFixedLenBufferSizeInBytes(buff_size);

      buffer_builder.setVarLenBufferSizeInBytes(0);
      // Set original user requested sizes
      buffer_builder.setOriginalVarLenBufferSizeInBytes(0);
      buffer_builder.setOriginalFixedLenBufferSizeInBytes(
          buff.original_buffer_size_);
    } else {
      throw StatusException(Status_SerializationError(
          "Unable to serialize query buffers with invalid size."));
    }

    if (buff.validity_vector_.buffer_size() != nullptr) {
      // Include cached bytes for remote global order writes prepend data.
      uint64_t buff_size =
          *buff.validity_vector_.buffer_size() + cached_validity_size;
      total_validity_len_bytes += buff_size;
      buffer_builder.setValidityLenBufferSizeInBytes(buff_size);

      // Set original user requested sizes
      buffer_builder.setOriginalValidityLenBufferSizeInBytes(
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
    } else if (query.dimension_label_ordered_read()) {
      auto builder = query_builder->initOrderedDimLabelReader();
      auto reader = dynamic_cast<OrderedDimLabelReader*>(query.strategy(true));
      ordered_dim_label_reader_to_capnp(query, *reader, &builder);
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
    RETURN_NOT_OK(writer_to_capnp(query, *writer, &builder, client_side));
  } else {
    auto builder = query_builder->initDelete();
    auto delete_strategy =
        dynamic_cast<DeletesAndUpdates*>(query.strategy(true));
    RETURN_NOT_OK(delete_to_capnp(query, *delete_strategy, &builder));
  }

  // Serialize Config
  auto config_builder = query_builder->initConfig();
  RETURN_NOT_OK(config_to_capnp(query.config(), &config_builder));

  // If stats object exists set its cap'n proto object
  auto stats = query.stats();
  if (stats) {
    auto stats_builder = query_builder->initStats();
    stats_to_capnp(*stats, &stats_builder);
  }

  auto& written_fragment_info = query.get_written_fragment_info();
  if (!written_fragment_info.empty()) {
    auto builder =
        query_builder->initWrittenFragmentInfo(written_fragment_info.size());
    for (uint64_t i = 0; i < written_fragment_info.size(); ++i) {
      builder[i].setUri(written_fragment_info[i]
                            .uri_.remove_trailing_slash()
                            .last_path_part());
      auto range_builder = builder[i].initTimestampRange(2);
      range_builder.set(0, written_fragment_info[i].timestamp_range_.first);
      range_builder.set(1, written_fragment_info[i].timestamp_range_.second);
    }
  }

  auto& written_buffers = query.get_written_buffers();
  if (!written_buffers.empty()) {
    auto builder = query_builder->initWrittenBuffers(written_buffers.size());
    int i = 0;
    for (auto& written_buffer : written_buffers) {
      builder.set(i++, written_buffer);
    }
  }

  // The server should throw if it's about to serialize an incomplete query
  // that has aggregates on it, this behavior is currently not supported.
  if (!client_side && query.status() == QueryStatus::INCOMPLETE &&
      query.has_aggregates()) {
    throw StatusException(Status_SerializationError(
        "Aggregates are not currently supported in incomplete remote "
        "queries"));
  }
  query_channels_to_capnp(query, query_builder);

  return Status::Ok();
}

Status query_from_capnp(
    const capnp::Query::Reader& query_reader,
    const SerializationContext context,
    void* buffer_start,
    CopyState* const copy_state,
    Query* const query,
    ThreadPool* compute_tp,
    const bool allocate_buffers) {
  using namespace tiledb::sm;

  auto array = query->array();
  if (array == nullptr) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot deserialize; array pointer is null."));
  }

  // Deserialize query type (sanity check).
  auto type = query->type();
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

  // Reset QueryStatus to UNINITIALIZED
  // Note: this is a pre-C.41 hack. At present, Strategy creation is the point
  // of construction through the regular API. However, the Query cannot be
  // considered fully initialized until everything has been deserialized. We
  // must "reset" the status here to bypass future checks on a
  // fully-initialized Query until the final QueryStatus is deserialized.
  query->set_status(QueryStatus::UNINITIALIZED);

  // Deserialize Config
  if (query_reader.hasConfig()) {
    tdb_unique_ptr<Config> decoded_config = nullptr;
    auto config_reader = query_reader.getConfig();
    RETURN_NOT_OK(config_from_capnp(config_reader, &decoded_config));
    if (decoded_config != nullptr) {
      query->unsafe_set_config(*decoded_config);
    }
  }

  // It's important that deserialization of query channels/aggregates happens
  // before deserializing buffers. set_data_buffer won't know whether a buffer
  // is aggregate or not if the list of aggregates per channel is not populated.
  query_channels_from_capnp(query_reader, query);

  const auto& schema = query->array_schema();
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

    // TODO: This is yet another instance where there needs to be a common
    // mechanism for reporting the common properties of a field.
    // Refactor to use query_field_t.
    bool var_size = false;
    bool nullable = false;
    auto aggregate = query->get_aggregate(name);
    if (aggregate.has_value()) {
      var_size = aggregate.value()->aggregation_var_sized();
      nullable = aggregate.value()->aggregation_nullable();
    } else {
      var_size = schema.var_size(name);
      nullable = schema.is_nullable(name);
    }
    const QueryBuffer& query_buffer = query->buffer(name);
    if (type == QueryType::READ) {
      // We use the query_buffer directly in order to get the original buffer
      // sizes. This avoid a problem where an incomplete query will change the
      // users buffer size to the smaller results and we end up not being able
      // to correctly calculate if the new results can fit into the users
      // buffer
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
    } else if (query_type == QueryType::WRITE) {
      // For writes we need ptrs to set the sizes properly
      uint64_t* existing_buffer_size_ptr = nullptr;
      uint64_t* existing_offset_buffer_size_ptr = nullptr;
      uint64_t* existing_validity_buffer_size_ptr = nullptr;

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
      // For reads, copy the response data into user buffers. For writes,
      // nothing to do.
      if (type == QueryType::READ) {
        // For read queries on the client side, we require buffers have been
        // set by the user, and that they are large enough for all serialized
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
        const bool has_mem_for_validity =
            validity_size_left >= validitylen_size;

        if (!has_mem_for_data || !has_mem_for_offset || !has_mem_for_validity) {
          return LOG_STATUS(Status_SerializationError(
              "Error deserializing query; buffer too small for buffer '" +
              name + "'."));
        }

        // Copy the response data into user buffers.
        if (var_size) {
          // Var size attribute; buffers already set.
          char* offset_dest = (char*)existing_offset_buffer + curr_offset_size;
          char* data_dest = (char*)existing_buffer + curr_data_size;
          char* validity_dest =
              (char*)existing_validity_buffer + curr_validity_size;
          uint64_t fixedlen_size_to_copy = fixedlen_size;

          // If the last query included an extra offset we will skip the first
          // offset in this query The first offset is the 0 position which
          // ends up the same as the extra offset in the last query 0th is
          // converted below to curr_data_size which is also the n+1 offset in
          // arrow mode
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
          // data buffer. To build a single contiguous buffer, we must
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
          // The final, contiguous buffer will be:
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

          // attr_copy_state==nulptr models the case of deserialization code
          // being called via the C API tiledb_query_deserialize (e.g. unit
          // test serialization wrappers).
          // When a rest_client exists, there is always a non null copy_state
          // incoming argument from which attr_copy_state is initialized
          if (attr_copy_state == nullptr) {
            // Set the size directly on the query (so user can introspect on
            // result size).
            // Subsequent incomplete submits will use the original buffer
            // sizes members from the query in the beginning of the loop
            // to calculate if user buffers have enough space to hold the
            // data, here we only care that after data received from the wire
            // is copied within the user buffers, the buffer sizes are
            // accurate so user can introspect.
            *query_buffer.buffer_size_ =
                curr_offset_size + fixedlen_size_to_copy;
            *query_buffer.buffer_var_size_ = curr_data_size + varlen_size;
            if (nullable) {
              *query_buffer.validity_vector_.buffer_size() =
                  curr_validity_size + validitylen_size;
            }
          } else {
            // Accumulate total bytes copied (caller's responsibility to
            // eventually update the query).
            attr_copy_state->offset_size += fixedlen_size_to_copy;
            attr_copy_state->data_size += varlen_size;
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

          // attr_copy_state==nulptr models the case of deserialization code
          // being called via the C API tiledb_query_deserialize (e.g. unit
          // test serialization wrappers).
          // When a rest_client exists, there is always a non null copy_state
          // incoming argument from which attr_copy_state is initialized
          if (attr_copy_state == nullptr) {
            // Set the size directly on the query (so user can introspect on
            // result size).
            // Subsequent incomplete submits will use the original buffer
            // sizes members from the query in the beginning of the loop
            // to calculate if user buffers have enough space to hold the
            // data, here we only care that after data received from the wire
            // is copied within the user buffers, the buffer sizes are
            // accurate so user can introspect.
            *query_buffer.buffer_size_ = curr_data_size + fixedlen_size;
            if (nullable) {
              *query_buffer.validity_vector_.buffer_size() =
                  curr_validity_size + validitylen_size;
            }
          } else {
            attr_copy_state->data_size += fixedlen_size;
            if (nullable)
              attr_copy_state->validity_size += validitylen_size;
          }
        }
      }
      // Nothing to do for writes on client side.

    } else if (context == SerializationContext::SERVER) {
      // Always expect null buffers when deserializing.
      if (existing_buffer != nullptr || existing_offset_buffer != nullptr ||
          existing_validity_buffer != nullptr)
        return LOG_STATUS(
            Status_SerializationError("Error deserializing query; unexpected "
                                      "buffer set on server-side."));

      Query::SerializationState::AttrState* attr_state;
      RETURN_NOT_OK(query->get_attr_serialization_state(name, &attr_state));
      if (type == QueryType::READ) {
        // On reads, just set null pointers with accurate size so that the
        // server can introspect and allocate properly sized buffers
        // separately.
        Buffer offsets_buff(nullptr, fixedlen_size);
        Buffer varlen_buff(nullptr, varlen_size);
        Buffer validitylen_buff(nullptr, validitylen_size);

        // Aggregates don't have incomplete queries, and only set the results on
        // completion, so we don't need to have go allocate memory.
        if (query->is_aggregate(name)) {
          offsets_buff = Buffer(fixedlen_size);
          varlen_buff = Buffer(fixedlen_size);
          validitylen_buff = Buffer(validitylen_size);
        }

        // For the server on reads we want to set the original user requested
        // buffer sizes This handles the case of incomplete queries where on
        // the second `submit()` call the client's buffer size will be the
        // first submit's result size not the original user set buffer size.
        // To work around this we revert the server to always use the full
        // original user requested buffer sizes. We check for > 0 for fallback
        // for clients older than 2.2.5
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
          throw_if_not_ok(query->set_data_buffer(
              name,
              attr_state->var_len_data.data(),
              &attr_state->var_len_size,
              allocate_buffers,
              true));
          throw_if_not_ok(query->set_offsets_buffer(
              name,
              attr_state->fixed_len_data.data_as<uint64_t>(),
              &attr_state->fixed_len_size,
              allocate_buffers,
              true));
          if (nullable) {
            throw_if_not_ok(query->set_validity_buffer(
                name,
                attr_state->validity_len_data.data_as<uint8_t>(),
                &attr_state->validity_len_size,
                allocate_buffers,
                true));
          }
        } else {
          throw_if_not_ok(query->set_data_buffer(
              name,
              attr_state->fixed_len_data.data(),
              &attr_state->fixed_len_size,
              allocate_buffers,
              true));
          if (nullable) {
            throw_if_not_ok(query->set_validity_buffer(
                name,
                attr_state->validity_len_data.data_as<uint8_t>(),
                &attr_state->validity_len_size,
                allocate_buffers,
                true));
          }
        }
      } else if (query_type == QueryType::WRITE) {
        // On writes, just set buffer pointers wrapping the data in the
        // message.
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

          throw_if_not_ok(query->set_data_buffer(
              name,
              varlen_data,
              &attr_state->var_len_size,
              allocate_buffers,
              true));
          throw_if_not_ok(query->set_offsets_buffer(
              name,
              offsets,
              &attr_state->fixed_len_size,
              allocate_buffers,
              true));
          if (nullable) {
            throw_if_not_ok(query->set_validity_buffer(
                name,
                validity,
                &attr_state->validity_len_size,
                allocate_buffers,
                true));
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

          throw_if_not_ok(query->set_data_buffer(
              name, data, &attr_state->fixed_len_size, allocate_buffers, true));
          if (nullable) {
            throw_if_not_ok(query->set_validity_buffer(
                name,
                validity,
                &attr_state->validity_len_size,
                allocate_buffers,
                true));
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
    } else if (query_reader.hasOrderedDimLabelReader()) {
      auto reader_reader = query_reader.getOrderedDimLabelReader();
      ordered_dim_label_reader_from_capnp(
          reader_reader,
          query,
          dynamic_cast<OrderedDimLabelReader*>(query->strategy()),
          compute_tp);
    } else if (query_reader.hasDenseReader()) {
      auto reader_reader = query_reader.getDenseReader();
      RETURN_NOT_OK(dense_reader_from_capnp(
          schema,
          reader_reader,
          query,
          dynamic_cast<DenseReader*>(query->strategy()),
          compute_tp,
          context == SerializationContext::CLIENT));
    } else {
      auto reader_reader = query_reader.getReader();
      RETURN_NOT_OK(reader_from_capnp(
          reader_reader,
          query,
          dynamic_cast<Reader*>(query->strategy()),
          compute_tp,
          context == SerializationContext::CLIENT));
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

    RETURN_NOT_OK(writer_from_capnp(*query, writer_reader, writer, context));

    // For sparse writes we want to explicitly set subarray to nullptr.
    const bool sparse_write =
        !schema.dense() || query->layout() == Layout::UNORDERED;
    if (!sparse_write) {
      if (writer_reader.hasSubarray()) {
        auto subarray_reader = writer_reader.getSubarray();
        void* subarray = nullptr;
        RETURN_NOT_OK(
            utils::deserialize_subarray(subarray_reader, schema, &subarray));
        try {
          query->set_subarray_unsafe(subarray);
        } catch (...) {
          tdb_free(subarray);
          throw;
        }
        tdb_free(subarray);
      }

      // Subarray
      if (writer_reader.hasSubarrayRanges()) {
        Subarray subarray(array, layout, query->stats(), dummy_logger, true);
        auto subarray_reader = writer_reader.getSubarrayRanges();
        RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
        RETURN_NOT_OK(query->set_subarray_unsafe(subarray));
      }
    }
  } else {
    auto delete_reader = query_reader.getDelete();
    auto delete_strategy = dynamic_cast<DeletesAndUpdates*>(query->strategy());
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
    auto stats_data = stats_from_capnp(query_reader.getStats());
    query->set_stats(stats_data);
  }

  if (query_reader.hasWrittenFragmentInfo()) {
    auto reader_list = query_reader.getWrittenFragmentInfo();
    auto& written_fi = query->get_written_fragment_info();
    for (auto fi : reader_list) {
      auto write_version = query->array_schema().write_version();
      auto frag_dir_uri = ArrayDirectory::generate_fragment_dir_uri(
          write_version,
          query->array_schema().array_uri().add_trailing_slash());
      auto fragment_name = std::string(fi.getUri().cStr());
      written_fi.emplace_back(
          frag_dir_uri.join_path(fragment_name),
          std::make_pair(fi.getTimestampRange()[0], fi.getTimestampRange()[1]));
    }
  }

  if (query_reader.hasWrittenBuffers()) {
    auto& written_buffers = query->get_written_buffers();
    written_buffers.clear();

    auto written_buffer_list = query_reader.getWrittenBuffers();
    for (auto written_buffer : written_buffer_list) {
      written_buffers.emplace(written_buffer.cStr());
    }
  } else if (
      query_type == QueryType::WRITE &&
      query_status == QueryStatus::COMPLETED &&
      context == SerializationContext::SERVER && layout == Layout::UNORDERED) {
    // Handle pre 2.16 clients...
    // For pre-2.16 clients, unordered writes always had to write all attributes
    // + dimensions at once so we just list all fields here.
    auto& written_buffers = query->get_written_buffers();
    written_buffers.clear();
    for (const auto& it : schema.attributes()) {
      written_buffers.emplace(it->name());
    }
    for (const auto& it : schema.dim_names()) {
      written_buffers.emplace(it);
    }
  }

  return Status::Ok();
}

Status array_from_query_deserialize(
    span<const char> serialized_buffer,
    SerializationType serialize_type,
    Array& array,
    ContextResources& resources,
    shared_ptr<MemoryTracker> memory_tracker) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::Query::Builder query_builder =
            message_builder.initRoot<capnp::Query>();
        json.decode(kj::StringPtr(serialized_buffer.data()), query_builder);
        capnp::Query::Reader query_reader = query_builder.asReader();
        // Deserialize array instance.
        array_from_capnp(
            query_reader.getArray(), resources, &array, false, memory_tracker);
        break;
      }
      case SerializationType::CAPNP: {
        // Capnp FlatArrayMessageReader requires 64-bit alignment.
        if (!utils::is_aligned<sizeof(uint64_t)>(serialized_buffer.data()))
          return LOG_STATUS(Status_SerializationError(
              "Could not deserialize query; buffer is not 8-byte aligned."));

        // Set traversal limit from config
        uint64_t limit = resources.config()
                             .get<uint64_t>("rest.capnp_traversal_limit")
                             .value();
        ::capnp::ReaderOptions readerOptions;
        // capnp uses the limit in words
        readerOptions.traversalLimitInWords = limit / sizeof(::capnp::word);

        ::capnp::FlatArrayMessageReader reader(
            kj::arrayPtr(
                reinterpret_cast<const ::capnp::word*>(
                    serialized_buffer.data()),
                serialized_buffer.size() / sizeof(::capnp::word)),
            readerOptions);

        capnp::Query::Reader query_reader = reader.getRoot<capnp::Query>();
        // Deserialize array instance.
        array_from_capnp(
            query_reader.getArray(), resources, &array, false, memory_tracker);
        break;
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

Status query_serialize(
    Query* query,
    SerializationType serialize_type,
    bool clientside,
    BufferList& serialized_buffer) {
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
        serialized_buffer.emplace_buffer().assign_null_terminated(capnp_json);
        // TODO: At this point the buffer data should also be serialized.
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);

        // Write the serialized query
        serialized_buffer.emplace_buffer().assign(protomessage.asChars());

        // Concatenate buffers to end of message
        if (serialize_buffers) {
          const auto& query_buffer_storage = query->get_remote_buffer_cache();

          auto attr_buffer_builders = query_builder.getAttributeBufferHeaders();
          for (auto attr_buffer_builder : attr_buffer_builders) {
            const std::string name = attr_buffer_builder.getName().cStr();
            auto buffer = query->buffer(name);
            if (buffer.buffer_var_size_ != nullptr &&
                buffer.buffer_var_ != nullptr) {
              // Variable size buffers.

              // If we are not appending offsets we can use non-owning
              // buffers.
              if (query_buffer_storage.has_value()) {
                const auto& buffer_cache =
                    query_buffer_storage->get_query_buffer_cache(name);

                serialized_buffer.emplace_buffer(
                    SerializationBuffer::NonOwned,
                    buffer_cache.buffer_.data(),
                    buffer_cache.buffer_.size());

                if (buffer_cache.buffer_.size() > 0) {
                  SerializationBuffer& data =
                      serialized_buffer.emplace_buffer();
                  data.assign(span(
                      reinterpret_cast<const char*>(buffer.buffer_),
                      *buffer.buffer_size_));
                  // Ensure ascending order for appended user offsets.
                  // Copy user offsets so we don't modify buffer in client
                  // code.
                  auto data_mut = data.owned_mutable_span();
                  uint64_t var_buffer_size = buffer_cache.buffer_var_.size();
                  for (uint64_t i = 0; i < data.size();
                       i += constants::cell_var_offset_size) {
                    *reinterpret_cast<uint64_t*>(data_mut.data() + i) +=
                        var_buffer_size;
                  }

                } else {
                  serialized_buffer.emplace_buffer(
                      SerializationBuffer::NonOwned,
                      reinterpret_cast<const char*>(buffer.buffer_),
                      *buffer.buffer_size_);
                }

                serialized_buffer.emplace_buffer(
                    SerializationBuffer::NonOwned,
                    buffer_cache.buffer_var_.data(),
                    buffer_cache.buffer_var_.size());
              } else {
                serialized_buffer.emplace_buffer(
                    SerializationBuffer::NonOwned,
                    buffer.buffer_,
                    *buffer.buffer_size_);
              }

              serialized_buffer.emplace_buffer(
                  SerializationBuffer::NonOwned,
                  buffer.buffer_var_,
                  *buffer.buffer_var_size_);
            } else if (
                buffer.buffer_size_ != nullptr && buffer.buffer_ != nullptr) {
              // Fixed size buffers.
              if (query_buffer_storage != nullopt) {
                const auto& buffer_cache =
                    query_buffer_storage->get_query_buffer_cache(name);
                serialized_buffer.emplace_buffer(
                    SerializationBuffer::NonOwned,
                    buffer_cache.buffer_.data(),
                    buffer_cache.buffer_.size());
              }

              serialized_buffer.emplace_buffer(
                  SerializationBuffer::NonOwned,
                  buffer.buffer_,
                  *buffer.buffer_size_);
            } else {
              throw StatusException(Status_SerializationError(
                  "Unable to serialize invalid query buffers."));
            }

            if (buffer.validity_vector_.buffer_size() != nullptr) {
              // Validity buffers.
              if (query_buffer_storage != nullopt) {
                const auto& buffer_cache =
                    query_buffer_storage->get_query_buffer_cache(name);
                serialized_buffer.emplace_buffer(
                    SerializationBuffer::NonOwned,
                    buffer_cache.buffer_validity_.data(),
                    buffer_cache.buffer_validity_.size());
              }

              serialized_buffer.emplace_buffer(
                  SerializationBuffer::NonOwned,
                  buffer.validity_vector_.buffer(),
                  *buffer.validity_vector_.buffer_size());
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
    span<const char> serialized_buffer,
    SerializationType serialize_type,
    const SerializationContext context,
    CopyState* const copy_state,
    Query* query,
    ThreadPool* compute_tp,
    const bool allocate_buffers) {
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
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            query_builder);
        capnp::Query::Reader query_reader = query_builder.asReader();
        return query_from_capnp(
            query_reader,
            context,
            nullptr,
            copy_state,
            query,
            compute_tp,
            allocate_buffers);
      }
      case SerializationType::CAPNP: {
        // Capnp FlatArrayMessageReader requires 64-bit alignment.
        if (!utils::is_aligned<sizeof(uint64_t)>(serialized_buffer.data()))
          return LOG_STATUS(Status_SerializationError(
              "Could not deserialize query; buffer is not 8-byte aligned."));

        // Set traversal limit from config
        uint64_t limit =
            query->config().get<uint64_t>("rest.capnp_traversal_limit").value();
        ::capnp::ReaderOptions readerOptions;
        // capnp uses the limit in words
        readerOptions.traversalLimitInWords = limit / sizeof(::capnp::word);

        ::capnp::FlatArrayMessageReader reader(
            kj::arrayPtr(
                reinterpret_cast<const ::capnp::word*>(
                    serialized_buffer.data()),
                serialized_buffer.size() / sizeof(::capnp::word)),
            readerOptions);

        capnp::Query::Reader query_reader = reader.getRoot<capnp::Query>();

        // Get a pointer to the start of the attribute buffer data (which
        // was concatenated after the CapnP message on serialization).
        auto attribute_buffer_start = reader.getEnd();
        auto buffer_start = const_cast<::capnp::word*>(attribute_buffer_start);
        return query_from_capnp(
            query_reader,
            context,
            buffer_start,
            copy_state,
            query,
            compute_tp,
            allocate_buffers);
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
    span<const char> serialized_buffer,
    SerializationType serialize_type,
    bool clientside,
    CopyState* copy_state,
    Query* query,
    ThreadPool* compute_tp,
    shared_ptr<MemoryTracker> memory_tracker) {
  // Create an original, serialized copy of the 'query' that we will revert
  // to if we are unable to deserialize 'serialized_buffer'.
  BufferList original_bufferlist(
      memory_tracker->get_resource(MemoryType::SERIALIZATION_BUFFER));
  RETURN_NOT_OK(
      query_serialize(query, serialize_type, clientside, original_bufferlist));

  // The first buffer is always the serialized Query object.
  auto& original_buffer = original_bufferlist.get_buffer(0);

  // Similarly, we must create a copy of 'copy_state'.
  tdb_unique_ptr<CopyState> original_copy_state = nullptr;
  if (copy_state) {
    original_copy_state =
        tdb_unique_ptr<CopyState>(tdb_new(CopyState, *copy_state));
  }
  auto allocate_buffers = query->type() == QueryType::WRITE;

  // Deserialize 'serialized_buffer'.
  const Status st = do_query_deserialize(
      serialized_buffer,
      serialize_type,
      clientside ? SerializationContext::CLIENT : SerializationContext::SERVER,
      copy_state,
      query,
      compute_tp,
      allocate_buffers);

  // If the deserialization failed, deserialize 'original_buffer'
  // into 'query' to ensure that 'query' is in the state it was before the
  // deserialization of 'serialized_buffer' failed.
  if (!st.ok()) {
    if (original_copy_state) {
      *copy_state = *original_copy_state;
    } else {
      copy_state = NULL;
    }

    const Status st2 = do_query_deserialize(
        original_buffer,
        serialize_type,
        SerializationContext::BACKUP,
        copy_state,
        query,
        compute_tp,
        allocate_buffers);
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
    auto name = std::string_view{it.getKey().cStr(), it.getKey().size()};
    auto result_size = it.getValue();
    est_result_sizes_map.emplace(
        name,
        Subarray::ResultSize{
            result_size.getSizeFixed(),
            result_size.getSizeVar(),
            result_size.getSizeValidity()});
  }

  std::unordered_map<std::string, Subarray::MemorySize> max_memory_sizes_map;
  for (auto it : max_memory_sizes.getEntries()) {
    auto name = std::string_view{it.getKey().cStr(), it.getKey().size()};
    auto memory_size = it.getValue();
    max_memory_sizes_map.emplace(
        name,
        Subarray::MemorySize{
            memory_size.getSizeFixed(),
            memory_size.getSizeVar(),
            memory_size.getSizeValidity()});
  }

  return query->set_est_result_size(est_result_sizes_map, max_memory_sizes_map);
}

Status query_est_result_size_serialize(
    Query* query,
    SerializationType serialize_type,
    bool,
    SerializationBuffer& serialized_buffer) {
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
        serialized_buffer.assign_null_terminated(capnp_json);
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        serialized_buffer.assign(protomessage.asChars());
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
    bool,
    span<const char> serialized_buffer) {
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
    GlobalOrderWriter& global_writer,
    capnp::GlobalWriteState::Builder* state_builder,
    bool client_side) {
  auto& write_state = *global_writer.get_global_state();

  auto& cells_written = write_state.cells_written_;
  if (!cells_written.empty()) {
    auto cells_written_builder = state_builder->initCellsWritten();
    auto entries_builder =
        cells_written_builder.initEntries(cells_written.size());
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
    fragment_meta_sizes_offsets_to_capnp(*frag_meta, &frag_meta_builder);
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
        for (uint64_t j = 0; j < coords[i].size(); ++j) {
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
  auto&& [st, multipart_states] =
      global_writer.multipart_upload_state(client_side);
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

      if (state.buffered_chunks.has_value()) {
        auto& buffered_chunks = state.buffered_chunks.value();
        auto builder =
            multipart_entry_builder.initBufferedChunks(buffered_chunks.size());
        for (uint64_t i = 0; i < buffered_chunks.size(); ++i) {
          builder[i].setUri(buffered_chunks[i].uri);
          builder[i].setSize(buffered_chunks[i].size);
        }
      }
    }
  }

  return Status::Ok();
}

Status global_write_state_from_capnp(
    const Query& query,
    const capnp::GlobalWriteState::Reader& state_reader,
    GlobalOrderWriter* global_writer,
    const SerializationContext context) {
  auto write_state = global_writer->get_global_state();

  if (state_reader.hasCellsWritten()) {
    auto& cells_written = write_state->cells_written_;
    auto cell_written_reader = state_reader.getCellsWritten();
    for (const auto& entry : cell_written_reader.getEntries()) {
      auto key = std::string_view{entry.getKey().cStr(), entry.getKey().size()};
      cells_written[std::string{key}] = entry.getValue();
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
        auto buffer_uri =
            std::string_view{entry.getKey().cStr(), entry.getKey().size()};

        if (state.hasUploadId()) {
          deserialized_state.upload_id = state.getUploadId().cStr();
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

        if (state.hasBufferedChunks()) {
          deserialized_state.buffered_chunks.emplace();
          auto& chunks = deserialized_state.buffered_chunks.value();
          for (auto chunk : state.getBufferedChunks()) {
            chunks.emplace_back();
            chunks.back().size = chunk.getSize();
            if (chunk.hasUri()) {
              chunks.back().uri = std::string(chunk.getUri().cStr());
            }
          }
        }

        RETURN_NOT_OK(global_writer->set_multipart_upload_state(
            std::string{buffer_uri},
            deserialized_state,
            context == SerializationContext::CLIENT));
      }
    }
  }

  return Status::Ok();
}

Status unordered_write_state_to_capnp(
    const Query& query,
    UnorderedWriter& unordered_writer,
    capnp::UnorderedWriterState::Builder* state_builder) {
  state_builder->setIsCoordsPass(unordered_writer.is_coords_pass());

  auto& cell_pos = unordered_writer.cell_pos();
  if (!cell_pos.empty()) {
    auto builder = state_builder->initCellPos(cell_pos.size());
    int i = 0;
    for (auto& pos : cell_pos) {
      builder.set(i++, pos);
    }
  }

  auto& coord_dups = unordered_writer.coord_dups();
  if (!coord_dups.empty()) {
    auto builder = state_builder->initCoordDups(coord_dups.size());
    int i = 0;
    for (auto& duplicate : coord_dups) {
      builder.set(i++, duplicate);
    }
  }

  auto frag_meta = unordered_writer.frag_meta();
  if (frag_meta != nullptr) {
    auto frag_meta_builder = state_builder->initFragMeta();
    fragment_meta_sizes_offsets_to_capnp(*frag_meta, &frag_meta_builder);
    RETURN_NOT_OK(fragment_metadata_to_capnp(*frag_meta, &frag_meta_builder));
  }

  return Status::Ok();
}

Status unordered_write_state_from_capnp(
    const Query& query,
    const capnp::UnorderedWriterState::Reader& state_reader,
    UnorderedWriter* unordered_writer,
    SerializationContext context) {
  unordered_writer->is_coords_pass() = state_reader.getIsCoordsPass();

  if (state_reader.hasCellPos()) {
    auto& cell_pos = unordered_writer->cell_pos();
    cell_pos.clear();
    auto cell_pos_list = state_reader.getCellPos();
    cell_pos.reserve(cell_pos_list.size());
    for (auto pos : cell_pos_list) {
      cell_pos.emplace_back(pos);
    }
  }

  if (state_reader.hasCoordDups()) {
    auto& coord_dups = unordered_writer->coord_dups();
    coord_dups.clear();
    auto coord_dups_list = state_reader.getCoordDups();
    for (auto duplicate : coord_dups_list) {
      coord_dups.emplace(duplicate);
    }
  }

  if (state_reader.hasFragMeta()) {
    // Fragment metadata is not allocated when deserializing into a new Query
    // object.
    if (unordered_writer->frag_meta() == nullptr) {
      RETURN_NOT_OK(unordered_writer->alloc_frag_meta());
    }
    auto frag_meta = unordered_writer->frag_meta();
    auto frag_meta_reader = state_reader.getFragMeta();
    RETURN_NOT_OK(fragment_metadata_from_capnp(
        query.array_schema_shared(), frag_meta_reader, frag_meta));
  }

  return Status::Ok();
}

void ordered_dim_label_reader_to_capnp(
    const Query& query,
    const OrderedDimLabelReader& reader,
    capnp::QueryReader::Builder* reader_builder) {
  const auto& array_schema = query.array_schema();

  // Subarray layout
  const auto& layout = layout_str(query.layout());
  reader_builder->setLayout(layout);

  // Subarray
  auto subarray_builder = reader_builder->initSubarray();
  throw_if_not_ok(
      subarray_to_capnp(array_schema, query.subarray(), &subarray_builder));

  reader_builder->setDimLabelIncreasing(
      query.dimension_label_increasing_order());

  // If stats object exists set its cap'n proto object
  const auto& stats = *reader.stats();
  auto stats_builder = reader_builder->initStats();
  stats_to_capnp(stats, &stats_builder);
}

void ordered_dim_label_reader_from_capnp(
    const capnp::QueryReader::Reader& reader_reader,
    Query* query,
    OrderedDimLabelReader* reader,
    ThreadPool* compute_tp) {
  auto array = query->array();

  // Layout
  Layout layout = Layout::ROW_MAJOR;
  throw_if_not_ok(layout_enum(reader_reader.getLayout(), &layout));

  // Subarray
  Subarray subarray(array, layout, query->stats(), dummy_logger, false);
  auto subarray_reader = reader_reader.getSubarray();
  throw_if_not_ok(subarray_from_capnp(subarray_reader, &subarray));
  throw_if_not_ok(query->set_subarray_unsafe(subarray));

  // OrderedDimLabelReader requires an initialized subarray for construction.
  query->set_dimension_label_ordered_read(
      reader_reader.getDimLabelIncreasing());
  throw_if_not_ok(query->reset_strategy_with_layout(layout, false));
  reader = dynamic_cast<OrderedDimLabelReader*>(query->strategy(true));

  // If cap'n proto object has stats set it on c++ object
  if (reader_reader.hasStats()) {
    auto stats_data = stats_from_capnp(reader_reader.getStats());
    reader->set_stats(stats_data);
  }
}

#else

Status query_serialize(Query*, SerializationType, bool, BufferList&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status query_deserialize(
    span<const char>,
    SerializationType,
    bool,
    CopyState*,
    Query*,
    ThreadPool*,
    shared_ptr<MemoryTracker>) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

Status array_from_query_deserialize(
    span<const char>,
    SerializationType,
    Array&,
    ContextResources&,
    shared_ptr<MemoryTracker>) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

Status query_est_result_size_serialize(
    Query*, SerializationType, bool, SerializationBuffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status query_est_result_size_deserialize(
    Query*, SerializationType, bool, span<const char>) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace tiledb::sm::serialization
