/**
 * @file   query.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include "tiledb/sm/serialization/query.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/buffer/buffer_list.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/reader.h"
#include "tiledb/sm/query/writer.h"

#ifdef _WIN32
#include "tiledb/sm/serialization/meet-capnproto-win32-include-expectations.h"
#endif

#include "tiledb/sm/serialization/capnp_utils.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#endif

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

enum class SerializationContext { CLIENT, SERVER, BACKUP };

Status array_to_capnp(
    const Array& array, capnp::Array::Builder* array_builder) {
  array_builder->setUri(array.array_uri().to_string());
  array_builder->setTimestamp(array.timestamp());

  return Status::Ok();
}

Status array_from_capnp(
    const capnp::Array::Reader& array_reader, Array* array) {
  RETURN_NOT_OK(array->set_uri(array_reader.getUri().cStr()));
  RETURN_NOT_OK(array->set_timestamp(array_reader.getTimestamp()));

  return Status::Ok();
}

Status subarray_to_capnp(
    const ArraySchema* schema,
    const Subarray* subarray,
    capnp::Subarray::Builder* builder) {
  builder->setLayout(layout_str(subarray->layout()));

  const uint32_t dim_num = subarray->dim_num();
  auto ranges_builder = builder->initRanges(dim_num);
  for (uint32_t i = 0; i < dim_num; i++) {
    const auto datatype = schema->dimension(i)->type();
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

  return Status::Ok();
}

Status subarray_partitioner_to_capnp(
    const ArraySchema* schema,
    const SubarrayPartitioner& partitioner,
    capnp::SubarrayPartitioner::Builder* builder) {
  // Subarray
  auto subarray_builder = builder->initSubarray();
  RETURN_NOT_OK(
      subarray_to_capnp(schema, partitioner.subarray(), &subarray_builder));

  // Per-attr/dim mem budgets
  const auto* budgets = partitioner.get_result_budgets();
  if (!budgets->empty()) {
    auto mem_budgets_builder = builder->initBudget(budgets->size());
    size_t idx = 0;
    for (const auto& pair : (*budgets)) {
      const std::string& name = pair.first;
      auto budget_builder = mem_budgets_builder[idx];
      budget_builder.setAttribute(name);
      auto var_size = schema->var_size(name);

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

  return Status::Ok();
}

Status subarray_partitioner_from_capnp(
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
  Subarray subarray(array, layout, false);
  RETURN_NOT_OK(subarray_from_capnp(reader.getSubarray(), &subarray));
  *partitioner = SubarrayPartitioner(
      subarray,
      memory_budget,
      memory_budget_var,
      memory_budget_validity,
      compute_tp);

  // Per-attr mem budgets
  if (reader.hasBudget()) {
    const ArraySchema* schema = array->array_schema();
    auto mem_budgets_reader = reader.getBudget();
    auto num_attrs = mem_budgets_reader.size();
    for (size_t i = 0; i < num_attrs; i++) {
      auto mem_budget_reader = mem_budgets_reader[i];
      std::string attr_name = mem_budget_reader.getAttribute();
      auto var_size = schema->var_size(attr_name);
      auto nullable = schema->is_nullable(attr_name);

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
    partition_info->partition_ = Subarray(array, layout, false);
    RETURN_NOT_OK(subarray_from_capnp(
        partition_info_reader.getSubarray(), &partition_info->partition_));

    if (compute_current_tile_overlap) {
      partition_info->partition_.compute_tile_overlap(compute_tp);
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
    state->single_range_.emplace_back(array, layout, false);
    Subarray& subarray_ = state->single_range_.back();
    RETURN_NOT_OK(subarray_from_capnp(subarray_reader_, &subarray_));
  }
  auto m_reader = state_reader.getMultiRange();
  const unsigned num_m = m_reader.size();
  for (unsigned i = 0; i < num_m; i++) {
    auto subarray_reader_ = m_reader[i];
    state->multi_range_.emplace_back(array, layout, false);
    Subarray& subarray_ = state->multi_range_.back();
    RETURN_NOT_OK(subarray_from_capnp(subarray_reader_, &subarray_));
  }

  // Overall mem budget
  RETURN_NOT_OK(partitioner->set_memory_budget(
      reader.getMemoryBudget(),
      reader.getMemoryBudgetVar(),
      reader.getMemoryBudgetValidity()));

  return Status::Ok();
}

Status read_state_to_capnp(
    const ArraySchema* schema,
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

Status read_state_from_capnp(
    const Array* array,
    const capnp::ReadState::Reader& read_state_reader,
    Reader* reader,
    ThreadPool* compute_tp) {
  auto read_state = reader->read_state();

  read_state->overflowed_ = read_state_reader.getOverflowed();
  read_state->unsplittable_ = read_state_reader.getUnsplittable();
  read_state->initialized_ = read_state_reader.getInitialized();

  // Subarray partitioner
  if (read_state_reader.hasSubarrayPartitioner()) {
    RETURN_NOT_OK(subarray_partitioner_from_capnp(
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

Status reader_to_capnp(
    const Reader& reader, capnp::QueryReader::Builder* reader_builder) {
  auto array_schema = reader.array_schema();

  // Subarray layout
  const auto& layout = layout_str(reader.layout());
  reader_builder->setLayout(layout);

  // Subarray
  auto subarray_builder = reader_builder->initSubarray();
  RETURN_NOT_OK(
      subarray_to_capnp(array_schema, reader.subarray(), &subarray_builder));

  // Read state
  RETURN_NOT_OK(read_state_to_capnp(array_schema, reader, reader_builder));

  return Status::Ok();
}

Status reader_from_capnp(
    const capnp::QueryReader::Reader& reader_reader,
    Reader* reader,
    ThreadPool* compute_tp) {
  auto array = reader->array();

  // Layout
  Layout layout = Layout::ROW_MAJOR;
  RETURN_NOT_OK(layout_enum(reader_reader.getLayout(), &layout));
  RETURN_NOT_OK(reader->set_layout(layout));

  // Subarray
  Subarray subarray(array, layout, false);
  auto subarray_reader = reader_reader.getSubarray();
  RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
  RETURN_NOT_OK(reader->set_subarray(subarray));

  // Read state
  if (reader_reader.hasReadState())
    RETURN_NOT_OK(read_state_from_capnp(
        array, reader_reader.getReadState(), reader, compute_tp));

  return Status::Ok();
}

Status writer_to_capnp(
    const Writer& writer, capnp::Writer::Builder* writer_builder) {
  writer_builder->setCheckCoordDups(writer.get_check_coord_dups());
  writer_builder->setCheckCoordOOB(writer.get_check_coord_oob());
  writer_builder->setDedupCoords(writer.get_dedup_coords());

  const auto* schema = writer.array_schema();
  const auto* subarray = writer.subarray();
  if (subarray != nullptr) {
    auto subarray_builder = writer_builder->initSubarray();
    RETURN_NOT_OK(
        utils::serialize_subarray(subarray_builder, schema, subarray));
  }

  // Subarray
  const auto subarray_ranges = writer.subarray_ranges();
  if (!subarray_ranges->empty()) {
    auto subarray_builder = writer_builder->initSubarrayRanges();
    const ArraySchema* array_schema = writer.array_schema();
    RETURN_NOT_OK(
        subarray_to_capnp(array_schema, subarray_ranges, &subarray_builder));
  }

  return Status::Ok();
}

Status writer_from_capnp(
    const capnp::Writer::Reader& writer_reader, Writer* writer) {
  writer->set_check_coord_dups(writer_reader.getCheckCoordDups());
  writer->set_check_coord_oob(writer_reader.getCheckCoordOOB());
  writer->set_dedup_coords(writer_reader.getDedupCoords());

  return Status::Ok();
}

Status query_to_capnp(
    const Query& query, capnp::Query::Builder* query_builder) {
  // For easy reference
  auto layout = query.layout();
  auto type = query.type();
  auto array = query.array();

  if (layout == Layout::GLOBAL_ORDER && query.type() == QueryType::WRITE)
    return LOG_STATUS(
        Status::SerializationError("Cannot serialize; global order "
                                   "serialization not supported for writes."));

  if (array == nullptr)
    return LOG_STATUS(
        Status::SerializationError("Cannot serialize; array is null."));

  const auto* schema = query.array_schema();
  if (schema == nullptr)
    return LOG_STATUS(
        Status::SerializationError("Cannot serialize; array schema is null."));

  const auto* domain = schema->domain();
  if (domain == nullptr)
    return LOG_STATUS(
        Status::SerializationError("Cannot serialize; array domain is null."));

  // Serialize basic fields
  query_builder->setType(query_type_str(type));
  query_builder->setLayout(layout_str(layout));
  query_builder->setStatus(query_status_str(query.status()));

  // Serialize array
  if (query.array() != nullptr) {
    auto builder = query_builder->initArray();
    RETURN_NOT_OK(array_to_capnp(*array, &builder));
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
    auto builder = query_builder->initReader();
    auto reader = query.reader();

    query_builder->setVarOffsetsMode(reader->offsets_mode());
    query_builder->setVarOffsetsAddExtraElement(
        reader->offsets_extra_element());
    query_builder->setVarOffsetsBitsize(reader->offsets_bitsize());
    RETURN_NOT_OK(reader_to_capnp(*reader, &builder));
  } else {
    auto builder = query_builder->initWriter();
    auto writer = query.writer();

    query_builder->setVarOffsetsMode(writer->get_offsets_mode());
    query_builder->setVarOffsetsAddExtraElement(
        writer->get_offsets_extra_element());
    query_builder->setVarOffsetsBitsize(writer->get_offsets_bitsize());
    RETURN_NOT_OK(writer_to_capnp(*writer, &builder));
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

  const auto* schema = query->array_schema();
  if (schema == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Cannot deserialize; array schema is null."));

  const auto* domain = schema->domain();
  if (domain == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Cannot deserialize; array domain is null."));

  if (array == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Cannot deserialize; array pointer is null."));

  // Deserialize query type (sanity check).
  QueryType query_type = QueryType::READ;
  RETURN_NOT_OK(query_type_enum(query_reader.getType().cStr(), &query_type));
  if (query_type != type)
    return LOG_STATUS(Status::SerializationError(
        "Cannot deserialize; Query opened for " + query_type_str(type) +
        " but got serialized type for " + query_reader.getType().cStr()));

  // Deserialize layout.
  Layout layout = Layout::UNORDERED;
  RETURN_NOT_OK(layout_enum(query_reader.getLayout().cStr(), &layout));
  RETURN_NOT_OK(query->set_layout(layout));

  // Deserialize array instance.
  RETURN_NOT_OK(array_from_capnp(query_reader.getArray(), array));

  // Deserialize and set attribute buffers.
  if (!query_reader.hasAttributeBufferHeaders())
    return LOG_STATUS(Status::SerializationError(
        "Cannot deserialize; no attribute buffer headers in message."));

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

    auto var_size = schema->var_size(name);
    auto nullable = schema->is_nullable(name);
    if (type == QueryType::READ && context == SerializationContext::SERVER) {
      const QueryBuffer& query_buffer = query->buffer(name);
      // We use the query_buffer directly in order to get the original buffer
      // sizes This avoid a problem where an incomplete query will change the
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
    } else {
      // For writes we need to use get_buffer and clientside
      if (var_size) {
        if (!nullable) {
          RETURN_NOT_OK(query->get_buffer(
              name.c_str(),
              &existing_offset_buffer,
              &existing_offset_buffer_size_ptr,
              &existing_buffer,
              &existing_buffer_size_ptr));

          if (existing_offset_buffer_size_ptr != nullptr)
            existing_offset_buffer_size = *existing_offset_buffer_size_ptr;
          if (existing_offset_buffer_size_ptr != nullptr)
            existing_buffer_size = *existing_buffer_size_ptr;
        } else {
          RETURN_NOT_OK(query->get_buffer_vbytemap(
              name.c_str(),
              &existing_offset_buffer,
              &existing_offset_buffer_size_ptr,
              &existing_buffer,
              &existing_buffer_size_ptr,
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
          RETURN_NOT_OK(query->get_buffer(
              name.c_str(), &existing_buffer, &existing_buffer_size_ptr));

          if (existing_buffer_size_ptr != nullptr)
            existing_buffer_size = *existing_buffer_size_ptr;
        } else {
          RETURN_NOT_OK(query->get_buffer_vbytemap(
              name.c_str(),
              &existing_buffer,
              &existing_buffer_size_ptr,
              &existing_validity_buffer,
              &existing_validity_buffer_size_ptr));

          if (existing_buffer_size_ptr != nullptr)
            existing_buffer_size = *existing_buffer_size_ptr;
          if (existing_validity_buffer_size_ptr != nullptr)
            existing_validity_buffer_size = *existing_validity_buffer_size_ptr;
        }
      }
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
        return LOG_STATUS(Status::SerializationError(
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
          char* validity_dest = (char*)existing_buffer + curr_validity_size;

          std::memcpy(offset_dest, attribute_buffer_start, fixedlen_size);
          attribute_buffer_start += fixedlen_size;
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
            for (uint64_t i = 0; i < (fixedlen_size / sizeof(uint64_t)); ++i) {
              reinterpret_cast<uint64_t*>(offset_dest)[i] += curr_data_size;
            }
          }

          if (attr_copy_state == nullptr) {
            // Set the size directly on the query (so user can introspect on
            // result size).
            if (existing_offset_buffer_size_ptr != nullptr)
              *existing_offset_buffer_size_ptr = fixedlen_size;
            if (existing_buffer_size_ptr != nullptr)
              *existing_buffer_size_ptr = varlen_size;
            if (nullable && existing_validity_buffer_size_ptr != nullptr)
              *existing_validity_buffer_size_ptr = validitylen_size;
          } else {
            // Accumulate total bytes copied (caller's responsibility to
            // eventually update the query).
            attr_copy_state->offset_size += fixedlen_size;
            attr_copy_state->data_size += varlen_size;
            if (nullable)
              attr_copy_state->validity_size += validitylen_size;
          }
        } else {
          // Fixed size attribute; buffers already set.
          char* data_dest = (char*)existing_buffer + curr_data_size;
          char* validity_dest = (char*)existing_buffer + curr_validity_size;

          std::memcpy(data_dest, attribute_buffer_start, fixedlen_size);
          attribute_buffer_start += fixedlen_size;
          if (nullable) {
            std::memcpy(
                validity_dest, attribute_buffer_start, validitylen_size);
            attribute_buffer_start += validitylen_size;
          }

          if (attr_copy_state == nullptr) {
            if (existing_buffer_size_ptr != nullptr)
              *existing_buffer_size_ptr = fixedlen_size;
            if (nullable && existing_validity_buffer_size_ptr != nullptr)
              *existing_validity_buffer_size_ptr = validitylen_size;
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
        return LOG_STATUS(Status::SerializationError(
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
            RETURN_NOT_OK(query->set_buffer(
                name,
                nullptr,
                &attr_state->fixed_len_size,
                nullptr,
                &attr_state->var_len_size,
                false));
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
            RETURN_NOT_OK(query->set_buffer(
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
      } else {
        // On writes, just set buffer pointers wrapping the data in the message.
        if (var_size) {
          auto* offsets = reinterpret_cast<uint64_t*>(attribute_buffer_start);
          auto* varlen_data = attribute_buffer_start + fixedlen_size;
          auto* validity =
              reinterpret_cast<uint8_t*>(attribute_buffer_start + varlen_size);

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
            RETURN_NOT_OK(query->set_buffer(
                name,
                offsets,
                &attr_state->fixed_len_size,
                varlen_data,
                &attr_state->var_len_size));
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
            RETURN_NOT_OK(
                query->set_buffer(name, data, &attr_state->fixed_len_size));
          } else {
            RETURN_NOT_OK(query->set_buffer_vbytemap(
                name,
                data,
                &attr_state->fixed_len_size,
                validity,
                &attr_state->validity_len_size));
          }
        }
      }
    }
  }

  // Deserialize reader/writer.
  // Also set subarray on query if it exists. Prior to 1.8 the subarray was set
  // on the reader or writer directly Now we set it on the query class after the
  // heterogeneous coordinate changes
  if (type == QueryType::READ) {
    auto reader_reader = query_reader.getReader();
    auto reader = query->reader();

    if (query_reader.hasVarOffsetsMode()) {
      RETURN_NOT_OK(reader->set_offsets_mode(query_reader.getVarOffsetsMode()));
    }

    RETURN_NOT_OK(reader->set_offsets_extra_element(
        query_reader.getVarOffsetsAddExtraElement()));

    if (query_reader.getVarOffsetsBitsize() > 0) {
      RETURN_NOT_OK(
          reader->set_offsets_bitsize(query_reader.getVarOffsetsBitsize()));
    }

    RETURN_NOT_OK(reader_from_capnp(reader_reader, reader, compute_tp));
  } else {
    auto writer_reader = query_reader.getWriter();
    auto writer = query->writer();

    if (query_reader.hasVarOffsetsMode()) {
      RETURN_NOT_OK(writer->set_offsets_mode(query_reader.getVarOffsetsMode()));
    }

    RETURN_NOT_OK(writer->set_offsets_extra_element(
        query_reader.getVarOffsetsAddExtraElement()));

    if (query_reader.getVarOffsetsBitsize() > 0) {
      RETURN_NOT_OK(
          writer->set_offsets_bitsize(query_reader.getVarOffsetsBitsize()));
    }

    RETURN_NOT_OK(writer_from_capnp(writer_reader, writer));

    // For sparse writes we want to explicitly set subarray to nullptr.
    const bool sparse_write =
        !schema->dense() || query->layout() == Layout::UNORDERED;
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
        Subarray subarray(array, layout, false);
        auto subarray_reader = writer_reader.getSubarrayRanges();
        RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
        RETURN_NOT_OK(query->writer()->set_subarray(subarray));
      }
    }
  }

  // Deserialize status. This must come last because various setters above
  // will reset it.
  QueryStatus query_status = QueryStatus::UNINITIALIZED;
  RETURN_NOT_OK(
      query_status_enum(query_reader.getStatus().cStr(), &query_status));
  query->set_status(query_status);

  return Status::Ok();
}

Status query_serialize(
    Query* query,
    SerializationType serialize_type,
    bool clientside,
    BufferList* serialized_buffer) {
  if (serialize_type == SerializationType::JSON)
    return LOG_STATUS(Status::SerializationError(
        "Cannot serialize query; json format not supported."));

  const auto* array_schema = query->array_schema();
  if (array_schema == nullptr || query->array() == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Cannot serialize; array or array schema is null."));

  try {
    ::capnp::MallocMessageBuilder message;
    capnp::Query::Builder query_builder = message.initRoot<capnp::Query>();
    RETURN_NOT_OK(query_to_capnp(*query, &query_builder));

    // Determine whether we should be serializing the buffer data.
    const bool serialize_buffers =
        (clientside && query->type() == QueryType::WRITE) ||
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
        return LOG_STATUS(Status::SerializationError(
            "Cannot serialize; unknown serialization type"));
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Cannot serialize; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status::SerializationError(
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
    return LOG_STATUS(Status::SerializationError(
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
          return LOG_STATUS(Status::SerializationError(
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
        return LOG_STATUS(Status::SerializationError(
            "Cannot deserialize; unknown serialization type."));
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Cannot deserialize; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status::SerializationError(
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
      LOG_FATAL(st2.message());
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
        return LOG_STATUS(Status::SerializationError(
            "Cannot serialize; unknown serialization type"));
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Cannot serialize; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Cannot serialize; exception: " + std::string(e.what())));
  }

  return Status::Ok();
}

Status query_est_result_size_deserialize(
    Query* query,
    SerializationType serialize_type,
    bool clientside,
    const Buffer& serialized_buffer) {
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
            Status::SerializationError("Error deserializing query est result "
                                       "size; Unknown serialization type "
                                       "passed"));
      }
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Error deserializing query est result size; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Error deserializing query est result size; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

#else

Status query_serialize(Query*, SerializationType, bool, BufferList*) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status query_deserialize(
    const Buffer&, SerializationType, bool, CopyState*, Query*, ThreadPool*) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

Status query_est_result_size_serialize(
    Query*, SerializationType, bool, Buffer*) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status query_est_result_size_deserialize(
    Query*, SerializationType, bool, const Buffer&) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
