/**
 * @file   query.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/serialization/capnp_utils.h"

#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#endif

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

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

  return Status::Ok();
}

Status writer_from_capnp(
    const capnp::Writer::Reader& writer_reader, Writer* writer) {
  writer->set_check_coord_dups(writer_reader.getCheckCoordDups());
  writer->set_check_coord_oob(writer_reader.getCheckCoordOOB());
  writer->set_dedup_coords(writer_reader.getDedupCoords());

  const auto* schema = writer->array_schema();
  // For sparse writes we want to explicitly set subarray to nullptr.
  const bool sparse_write =
      !schema->dense() || writer->layout() == Layout::UNORDERED;
  if (writer_reader.hasSubarray() && !sparse_write) {
    auto subarray_reader = writer_reader.getSubarray();
    void* subarray = nullptr;
    RETURN_NOT_OK(
        utils::deserialize_subarray(subarray_reader, schema, &subarray));
    RETURN_NOT_OK_ELSE(writer->set_subarray(subarray), std::free(subarray));
    std::free(subarray);
  } else {
    RETURN_NOT_OK(writer->set_subarray(nullptr));
  }

  return Status::Ok();
}

Status subarray_to_capnp(
    const Subarray* subarray, capnp::Subarray::Builder* builder) {
  builder->setLayout(layout_str(subarray->layout()));

  const uint32_t dim_num = subarray->dim_num();
  auto ranges_builder = builder->initRanges(dim_num);
  for (uint32_t i = 0; i < dim_num; i++) {
    auto range_builder = ranges_builder[i];
    const auto* ranges = subarray->ranges_for_dim(i);
    range_builder.setType(datatype_str(ranges->type_));
    range_builder.setHasDefaultRange(ranges->has_default_range_);
    range_builder.setBuffer(kj::arrayPtr(
        static_cast<const uint8_t*>(ranges->buffer_.data()),
        ranges->buffer_.size()));
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

    Subarray::Ranges ranges(type);
    auto data_ptr = range_reader.getBuffer();
    RETURN_NOT_OK(ranges.buffer_.realloc(data_ptr.size()));
    RETURN_NOT_OK(
        ranges.buffer_.write((void*)data_ptr.begin(), data_ptr.size()));
    ranges.has_default_range_ = range_reader.getHasDefaultRange();

    RETURN_NOT_OK(subarray->set_ranges_for_dim(i, ranges));
  }

  return Status::Ok();
}

Status subarray_partitioner_to_capnp(
    const ArraySchema* schema,
    const SubarrayPartitioner& partitioner,
    capnp::SubarrayPartitioner::Builder* builder) {
  // Subarray
  auto subarray_builder = builder->initSubarray();
  RETURN_NOT_OK(subarray_to_capnp(partitioner.subarray(), &subarray_builder));

  // Per-attr mem budgets
  const auto* attr_budgets = partitioner.get_attr_result_budgets();
  if (!attr_budgets->empty()) {
    auto mem_budgets_builder = builder->initBudget(attr_budgets->size());
    size_t attr_idx = 0;
    for (const auto& pair : (*attr_budgets)) {
      const std::string& attr_name = pair.first;
      auto budget_builder = mem_budgets_builder[attr_idx];
      budget_builder.setAttribute(attr_name);
      if (attr_name == constants::coords || !schema->var_size(attr_name)) {
        budget_builder.setOffsetBytes(0);
        budget_builder.setDataBytes(pair.second.size_fixed_);
      } else {
        budget_builder.setOffsetBytes(pair.second.size_fixed_);
        budget_builder.setDataBytes(pair.second.size_var_);
      }
      attr_idx++;
    }
  }

  // Current partition info
  const auto* partition_info = partitioner.current_partition_info();
  auto info_builder = builder->initCurrent();
  auto info_subarray_builder = info_builder.initSubarray();
  RETURN_NOT_OK(
      subarray_to_capnp(&partition_info->partition_, &info_subarray_builder));
  info_builder.setStart(partition_info->start_);
  info_builder.setEnd(partition_info->end_);
  info_builder.setSplitMultiRange(partition_info->split_multi_range_);

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
    RETURN_NOT_OK(subarray_to_capnp(&subarray, &b));
    sr_idx++;
  }
  auto multi_range_builder =
      state_builder.initMultiRange(state->multi_range_.size());
  size_t m_idx = 0;
  for (const auto& subarray : state->multi_range_) {
    auto b = multi_range_builder[m_idx];
    RETURN_NOT_OK(subarray_to_capnp(&subarray, &b));
    m_idx++;
  }

  // Overall mem budget
  uint64_t mem_budget, mem_budget_var;
  RETURN_NOT_OK(partitioner.get_memory_budget(&mem_budget, &mem_budget_var));
  builder->setMemoryBudget(mem_budget);
  builder->setMemoryBudgetVar(mem_budget_var);

  return Status::Ok();
}

Status subarray_partitioner_from_capnp(
    const Array* array,
    const capnp::SubarrayPartitioner::Reader& reader,
    SubarrayPartitioner* partitioner) {
  // Get memory budget
  uint64_t memory_budget = 0;
  RETURN_NOT_OK(tiledb::sm::utils::parse::convert(
      Config::SM_MEMORY_BUDGET, &memory_budget));
  uint64_t memory_budget_var = 0;
  RETURN_NOT_OK(tiledb::sm::utils::parse::convert(
      Config::SM_MEMORY_BUDGET_VAR, &memory_budget_var));

  // Get subarray layout first
  Layout layout = Layout::ROW_MAJOR;
  auto subarray_reader = reader.getSubarray();
  RETURN_NOT_OK(layout_enum(subarray_reader.getLayout(), &layout));

  // Subarray, which is used to initialize the partitioner.
  Subarray subarray(array, layout);
  RETURN_NOT_OK(subarray_from_capnp(reader.getSubarray(), &subarray));
  *partitioner =
      SubarrayPartitioner(subarray, memory_budget, memory_budget_var);

  // Per-attr mem budgets
  if (reader.hasBudget()) {
    const ArraySchema* schema = array->array_schema();
    auto mem_budgets_reader = reader.getBudget();
    auto num_attrs = mem_budgets_reader.size();
    for (size_t i = 0; i < num_attrs; i++) {
      auto mem_budget_reader = mem_budgets_reader[i];
      std::string attr_name = mem_budget_reader.getAttribute();
      if (attr_name == constants::coords || !schema->var_size(attr_name)) {
        RETURN_NOT_OK(partitioner->set_result_budget(
            attr_name.c_str(), mem_budget_reader.getDataBytes()));
      } else {
        RETURN_NOT_OK(partitioner->set_result_budget(
            attr_name.c_str(),
            mem_budget_reader.getOffsetBytes(),
            mem_budget_reader.getDataBytes()));
      }
    }
  }

  // Current partition info
  auto partition_info_reader = reader.getCurrent();
  auto* partition_info = partitioner->current_partition_info();
  partition_info->start_ = partition_info_reader.getStart();
  partition_info->end_ = partition_info_reader.getEnd();
  partition_info->split_multi_range_ =
      partition_info_reader.getSplitMultiRange();
  partition_info->partition_ = Subarray(array, layout);
  RETURN_NOT_OK(subarray_from_capnp(
      partition_info_reader.getSubarray(), &partition_info->partition_));

  // Partitioner state
  auto state_reader = reader.getState();
  auto* state = partitioner->state();
  state->start_ = state_reader.getStart();
  state->end_ = state_reader.getEnd();
  auto sr_reader = state_reader.getSingleRange();
  const unsigned num_sr = sr_reader.size();
  for (unsigned i = 0; i < num_sr; i++) {
    auto subarray_reader = sr_reader[i];
    state->single_range_.emplace_back(array, layout);
    Subarray& subarray = state->single_range_.back();
    RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
  }
  auto m_reader = state_reader.getMultiRange();
  const unsigned num_m = m_reader.size();
  for (unsigned i = 0; i < num_m; i++) {
    auto subarray_reader = m_reader[i];
    state->multi_range_.emplace_back(array, layout);
    Subarray& subarray = state->multi_range_.back();
    RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
  }

  // Overall mem budget
  RETURN_NOT_OK(partitioner->set_memory_budget(
      reader.getMemoryBudget(), reader.getMemoryBudgetVar()));

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
    Reader* reader) {
  auto read_state = reader->read_state();

  read_state->overflowed_ = read_state_reader.getOverflowed();
  read_state->unsplittable_ = read_state_reader.getUnsplittable();
  read_state->initialized_ = read_state_reader.getInitialized();

  // Subarray partitioner
  if (read_state_reader.hasSubarrayPartitioner()) {
    RETURN_NOT_OK(subarray_partitioner_from_capnp(
        array,
        read_state_reader.getSubarrayPartitioner(),
        &read_state->partitioner_));
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
  RETURN_NOT_OK(subarray_to_capnp(reader.subarray(), &subarray_builder));

  // Read state
  RETURN_NOT_OK(read_state_to_capnp(array_schema, reader, reader_builder));

  return Status::Ok();
}

Status reader_from_capnp(
    const capnp::QueryReader::Reader& reader_reader, Reader* reader) {
  auto array = reader->array();

  // Layout
  Layout layout = Layout::ROW_MAJOR;
  RETURN_NOT_OK(layout_enum(reader_reader.getLayout(), &layout));
  RETURN_NOT_OK(reader->set_layout(layout));

  // Subarray
  Subarray subarray(array, layout);
  auto subarray_reader = reader_reader.getSubarray();
  RETURN_NOT_OK(subarray_from_capnp(subarray_reader, &subarray));
  RETURN_NOT_OK(reader->set_subarray(subarray));

  // Read state
  if (reader_reader.hasReadState())
    RETURN_NOT_OK(
        read_state_from_capnp(array, reader_reader.getReadState(), reader));

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
  const auto attr_names = query.attributes();
  auto attr_buffers_builder =
      query_builder->initAttributeBufferHeaders(attr_names.size());
  uint64_t total_fixed_len_bytes = 0;
  uint64_t total_var_len_bytes = 0;
  for (uint64_t i = 0; i < attr_names.size(); i++) {
    auto attr_buffer_builder = attr_buffers_builder[i];
    const auto& attribute_name = attr_names[i];
    const auto& buff = query.attribute_buffer(attribute_name);
    const bool is_coords = attribute_name == constants::coords;
    const auto* attr = schema->attribute(attribute_name);
    if (!is_coords && attr == nullptr)
      return LOG_STATUS(Status::SerializationError(
          "Cannot serialize; no attribute named '" + attribute_name + "'."));

    const bool var_size = !is_coords && attr->var_size();
    attr_buffer_builder.setName(attribute_name);
    if (var_size &&
        (buff.buffer_var_ != nullptr && buff.buffer_var_size_ != nullptr)) {
      // Variable-sized attribute.
      if (buff.buffer_ == nullptr || buff.buffer_size_ == nullptr)
        return LOG_STATUS(Status::SerializationError(
            "Cannot serialize; no offset buffer set for attribute '" +
            attribute_name + "'."));
      total_var_len_bytes += *buff.buffer_var_size_;
      attr_buffer_builder.setVarLenBufferSizeInBytes(*buff.buffer_var_size_);
      total_fixed_len_bytes += *buff.buffer_size_;
      attr_buffer_builder.setFixedLenBufferSizeInBytes(*buff.buffer_size_);
    } else if (buff.buffer_ != nullptr && buff.buffer_size_ != nullptr) {
      // Fixed-length attribute
      total_fixed_len_bytes += *buff.buffer_size_;
      attr_buffer_builder.setFixedLenBufferSizeInBytes(*buff.buffer_size_);
      attr_buffer_builder.setVarLenBufferSizeInBytes(0);
    }
  }

  query_builder->setTotalFixedLengthBufferBytes(total_fixed_len_bytes);
  query_builder->setTotalVarLenBufferBytes(total_var_len_bytes);

  if (type == QueryType::READ) {
    auto builder = query_builder->initReader();
    auto reader = query.reader();
    RETURN_NOT_OK(reader_to_capnp(*reader, &builder));
  } else {
    auto builder = query_builder->initWriter();
    auto writer = query.writer();
    RETURN_NOT_OK(writer_to_capnp(*writer, &builder));
  }

  return Status::Ok();
}

Status query_from_capnp(
    const capnp::Query::Reader& query_reader,
    const bool clientside,
    void* buffer_start,
    CopyState* const copy_state,
    Query* const query) {
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
    const std::string attribute_name = buffer_header.getName().cStr();
    const bool is_coords = attribute_name == constants::coords;
    const auto* attr = schema->attribute(attribute_name);
    if (!is_coords && attr == nullptr)
      return LOG_STATUS(Status::SerializationError(
          "Cannot deserialize; no attribute named '" + attribute_name +
          "' in array schema."));

    // Get buffer sizes required
    const uint64_t fixedlen_size = buffer_header.getFixedLenBufferSizeInBytes();
    const uint64_t varlen_size = buffer_header.getVarLenBufferSizeInBytes();

    // Get current copy state for the attribute (contains destination offsets
    // for memcpy into user buffers).
    QueryBufferCopyState* attr_copy_state = nullptr;
    if (copy_state != nullptr)
      attr_copy_state = &(*copy_state)[attribute_name];

    // Get any buffers already set on this query object.
    uint64_t* existing_offset_buffer = nullptr;
    uint64_t* existing_offset_buffer_size = nullptr;
    void* existing_buffer = nullptr;
    uint64_t* existing_buffer_size = nullptr;
    const bool var_size = !is_coords && attr->var_size();
    if (var_size) {
      RETURN_NOT_OK(query->get_buffer(
          attribute_name.c_str(),
          &existing_offset_buffer,
          &existing_offset_buffer_size,
          &existing_buffer,
          &existing_buffer_size));
    } else {
      RETURN_NOT_OK(query->get_buffer(
          attribute_name.c_str(), &existing_buffer, &existing_buffer_size));
    }

    if (clientside) {
      // For queries on the client side, we require that buffers have been
      // set by the user, and that they are large enough for all the serialized
      // data.
      const bool null_buffer =
          existing_buffer == nullptr || existing_buffer_size == nullptr;
      const bool null_offset_buffer = existing_offset_buffer == nullptr ||
                                      existing_offset_buffer_size == nullptr;
      if ((var_size && (null_buffer || null_offset_buffer)) ||
          (!var_size && null_buffer))
        return LOG_STATUS(Status::SerializationError(
            "Error deserializing read query; buffer not set for attribute '" +
            attribute_name + "'."));

      // Check sizes
      const uint64_t curr_data_size =
          attr_copy_state == nullptr ? 0 : attr_copy_state->data_size;
      const uint64_t data_size_left = *existing_buffer_size - curr_data_size;
      const uint64_t curr_offset_size =
          attr_copy_state == nullptr ? 0 : attr_copy_state->offset_size;
      const uint64_t offset_size_left =
          existing_offset_buffer_size == nullptr ?
              0 :
              *existing_offset_buffer_size - curr_offset_size;
      if ((var_size && (offset_size_left < fixedlen_size ||
                        data_size_left < varlen_size)) ||
          (!var_size && data_size_left < fixedlen_size)) {
        return LOG_STATUS(Status::SerializationError(
            "Error deserializing read query; buffer too small for attribute "
            "'" +
            attribute_name + "'."));
      }

      // For reads, copy the response data into user buffers. For writes,
      // nothing to do.
      if (type == QueryType::READ) {
        if (var_size) {
          char* offset_dest = (char*)existing_offset_buffer + curr_offset_size;
          char* data_dest = (char*)existing_buffer + curr_data_size;
          // Var size attribute; buffers already set.
          std::memcpy(offset_dest, attribute_buffer_start, fixedlen_size);
          attribute_buffer_start += fixedlen_size;
          std::memcpy(data_dest, attribute_buffer_start, varlen_size);
          attribute_buffer_start += varlen_size;

          if (attr_copy_state == nullptr) {
            // Set the size directly on the query (so user can introspect on
            // result size).
            *existing_offset_buffer_size = fixedlen_size;
            *existing_buffer_size = varlen_size;
          } else {
            // Accumulate total bytes copied (caller's responsibility to
            // eventually update the query).
            attr_copy_state->offset_size += fixedlen_size;
            attr_copy_state->data_size += varlen_size;
          }
        } else {
          // Fixed size attribute; buffers already set.
          char* data_dest = (char*)existing_buffer + curr_data_size;
          std::memcpy(data_dest, attribute_buffer_start, fixedlen_size);
          attribute_buffer_start += fixedlen_size;

          if (attr_copy_state == nullptr) {
            *existing_buffer_size = fixedlen_size;
          } else {
            attr_copy_state->data_size += fixedlen_size;
          }
        }
      }
    } else {
      // Server-side; always expect null buffers when deserializing.
      if (existing_buffer != nullptr || existing_offset_buffer != nullptr)
        return LOG_STATUS(Status::SerializationError(
            "Error deserializing read query; unexpected "
            "buffer set on server-side."));

      Query::SerializationState::AttrState* attr_state;
      RETURN_NOT_OK(
          query->get_attr_serialization_state(attribute_name, &attr_state));
      if (type == QueryType::READ) {
        // On reads, just set null pointers with accurate size so that the
        // server can introspect and allocate properly sized buffers separately.
        Buffer offsets_buff(nullptr, fixedlen_size);
        Buffer varlen_buff(nullptr, varlen_size);
        attr_state->fixed_len_size = fixedlen_size;
        attr_state->var_len_size = varlen_size;
        attr_state->fixed_len_data.swap(offsets_buff);
        attr_state->var_len_data.swap(varlen_buff);
        if (var_size) {
          RETURN_NOT_OK(query->set_buffer(
              attribute_name,
              nullptr,
              &attr_state->fixed_len_size,
              nullptr,
              &attr_state->var_len_size,
              false));
        } else {
          RETURN_NOT_OK(query->set_buffer(
              attribute_name, nullptr, &attr_state->fixed_len_size, false));
        }
      } else {
        // On writes, just set buffer pointers wrapping the data in the message.
        if (var_size) {
          auto* offsets = reinterpret_cast<uint64_t*>(attribute_buffer_start);
          auto* varlen_data = attribute_buffer_start + fixedlen_size;
          attribute_buffer_start += fixedlen_size + varlen_size;
          Buffer offsets_buff(offsets, fixedlen_size);
          Buffer varlen_buff(varlen_data, varlen_size);
          attr_state->fixed_len_size = fixedlen_size;
          attr_state->var_len_size = varlen_size;
          attr_state->fixed_len_data.swap(offsets_buff);
          attr_state->var_len_data.swap(varlen_buff);
          RETURN_NOT_OK(query->set_buffer(
              attribute_name,
              offsets,
              &attr_state->fixed_len_size,
              varlen_data,
              &attr_state->var_len_size));
        } else {
          auto* data = attribute_buffer_start;
          attribute_buffer_start += fixedlen_size;
          Buffer buff(data, fixedlen_size);
          Buffer varlen_buff(nullptr, 0);
          attr_state->fixed_len_size = fixedlen_size;
          attr_state->var_len_size = varlen_size;
          attr_state->fixed_len_data.swap(buff);
          attr_state->var_len_data.swap(varlen_buff);
          RETURN_NOT_OK(query->set_buffer(
              attribute_name, data, &attr_state->fixed_len_size));
        }
      }
    }
  }

  // Deserialize reader/writer.
  if (type == QueryType::READ) {
    auto reader_reader = query_reader.getReader();
    RETURN_NOT_OK(reader_from_capnp(reader_reader, query->reader()));
  } else {
    auto writer_reader = query_reader.getWriter();
    RETURN_NOT_OK(writer_from_capnp(writer_reader, query->writer()));
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
  STATS_FUNC_IN(serialization_query_serialize);

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

        // Iterate over attributes and concatenate buffers to end of message.
        if (serialize_buffers) {
          auto attr_buffer_builders = query_builder.getAttributeBufferHeaders();
          for (auto attr_buffer_builder : attr_buffer_builders) {
            const std::string attribute_name =
                attr_buffer_builder.getName().cStr();
            const bool is_coords = attribute_name == constants::coords;
            const auto* attr = array_schema->attribute(attribute_name);
            if (!is_coords && attr == nullptr)
              return LOG_STATUS(Status::SerializationError(
                  "Cannot serialize; no attribute named '" + attribute_name +
                  "'."));

            const bool var_size = !is_coords && attr->var_size();
            if (var_size) {
              // Variable size attribute buffer
              uint64_t* offset_buffer = nullptr;
              uint64_t* offset_buffer_size = nullptr;
              void* buffer = nullptr;
              uint64_t* buffer_size = nullptr;
              RETURN_NOT_OK(query->get_buffer(
                  attribute_name.c_str(),
                  &offset_buffer,
                  &offset_buffer_size,
                  &buffer,
                  &buffer_size));

              if (offset_buffer != nullptr) {
                if (offset_buffer_size == nullptr || buffer == nullptr ||
                    buffer_size == nullptr)
                  return LOG_STATUS(Status::SerializationError(
                      "Cannot serialize; unexpected null buffers."));

                Buffer offsets(offset_buffer, *offset_buffer_size);
                Buffer data(buffer, *buffer_size);
                RETURN_NOT_OK(
                    serialized_buffer->add_buffer(std::move(offsets)));
                RETURN_NOT_OK(serialized_buffer->add_buffer(std::move(data)));
              }
            } else {
              // Fixed size attribute buffer
              void* buffer = nullptr;
              uint64_t* buffer_size = nullptr;
              RETURN_NOT_OK(query->get_buffer(
                  attribute_name.c_str(), &buffer, &buffer_size));

              if (buffer != nullptr) {
                if (buffer_size == nullptr)
                  return LOG_STATUS(Status::SerializationError(
                      "Cannot serialize; unexpected null buffer size."));

                Buffer data(buffer, *buffer_size);
                RETURN_NOT_OK(serialized_buffer->add_buffer(std::move(data)));
              }
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

  STATS_FUNC_OUT(serialization_query_serialize);
}

Status do_query_deserialize(
    const Buffer& serialized_buffer,
    SerializationType serialize_type,
    const bool clientside,
    CopyState* const copy_state,
    Query* query) {
  STATS_FUNC_IN(serialization_query_deserialize);

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
            query_reader, clientside, nullptr, copy_state, query);
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
            query_reader, clientside, buffer_start, copy_state, query);
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

  STATS_FUNC_OUT(serialization_query_deserialize);
}

Status query_deserialize(
    const Buffer& serialized_buffer,
    SerializationType serialize_type,
    bool clientside,
    CopyState* copy_state,
    Query* query) {
  // Create an original, serialized copy of the 'query' that we will revert
  // to if we are unable to deserialize 'serialized_buffer'.
  BufferList original_bufferlist;
  RETURN_NOT_OK(
      query_serialize(query, serialize_type, clientside, &original_bufferlist));

  // The first buffer is always the serialized Query object.
  tiledb::sm::Buffer* original_buffer;
  RETURN_NOT_OK(original_bufferlist.get_buffer(0, &original_buffer));

  // Similarly, we must create a copy of 'copy_state'.
  std::unique_ptr<CopyState> original_copy_state = nullptr;
  if (copy_state) {
    original_copy_state = std::unique_ptr<CopyState>(copy_state);
  }

  // Deserialize 'serialized_buffer'.
  const Status st = do_query_deserialize(
      serialized_buffer, serialize_type, clientside, copy_state, query);

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
        *original_buffer, serialize_type, clientside, copy_state, query);
    if (!st2.ok()) {
      LOG_FATAL(st2.message());
    }
  }

  return st;
}

#else

Status query_serialize(Query*, SerializationType, bool, BufferList*) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status query_deserialize(
    const Buffer&, SerializationType, bool, CopyState*, Query*) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
