/**
 * @file   query.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

  return Status::Ok();
}

Status writer_from_capnp(
    const capnp::Writer::Reader& writer_reader, Writer* writer) {
  writer->set_check_coord_dups(writer_reader.getCheckCoordDups());
  writer->set_check_coord_oob(writer_reader.getCheckCoordOOB());
  writer->set_dedup_coords(writer_reader.getDedupCoords());

  return Status::Ok();
}

Status reader_to_capnp(
    const Reader& reader, capnp::QueryReader::Builder* reader_builder) {
  auto read_state = reader.read_state();
  auto array_schema = reader.array_schema();

  if (!read_state->initialized_)
    return Status::Ok();

  auto read_state_builder = reader_builder->initReadState();
  read_state_builder.setInitialized(read_state->initialized_);
  read_state_builder.setOverflowed(read_state->overflowed_);
  read_state_builder.setUnsplittable(read_state->unsplittable_);

  // Subarray
  if (read_state->subarray_ != nullptr) {
    auto subarray_builder = read_state_builder.initSubarray();
    RETURN_NOT_OK(utils::serialize_subarray(
        subarray_builder, array_schema, read_state->subarray_));
  }

  // Current partition
  if (read_state->cur_subarray_partition_ != nullptr) {
    auto subarray_builder = read_state_builder.initCurSubarrayPartition();
    RETURN_NOT_OK(utils::serialize_subarray(
        subarray_builder, array_schema, read_state->cur_subarray_partition_));
  }

  // Subarray partitions
  if (!read_state->subarray_partitions_.empty()) {
    auto partitions_builder = read_state_builder.initSubarrayPartitions(
        read_state->subarray_partitions_.size());
    size_t i = 0;
    for (const void* subarray : read_state->subarray_partitions_) {
      capnp::DomainArray::Builder builder = partitions_builder[i];
      RETURN_NOT_OK(utils::serialize_subarray(builder, array_schema, subarray));
      i++;
    }
  }

  return Status::Ok();
}

Status reader_from_capnp(
    const capnp::QueryReader::Reader& reader_reader, Reader* reader) {
  if (!reader_reader.hasReadState())
    return Status::Ok();

  auto read_state_reader = reader_reader.getReadState();
  auto read_state = reader->read_state();
  auto array_schema = reader->array_schema();

  read_state->initialized_ = read_state_reader.getInitialized();
  read_state->overflowed_ = read_state_reader.getOverflowed();
  read_state->unsplittable_ = read_state_reader.getUnsplittable();

  // Deserialize subarray
  std::free(read_state->subarray_);
  read_state->subarray_ = nullptr;
  if (read_state_reader.hasSubarray()) {
    auto subarray_reader = read_state_reader.getSubarray();
    RETURN_NOT_OK(utils::deserialize_subarray(
        subarray_reader, array_schema, &read_state->subarray_));
  }

  // Deserialize current partition
  std::free(read_state->cur_subarray_partition_);
  read_state->cur_subarray_partition_ = nullptr;
  if (read_state_reader.hasCurSubarrayPartition()) {
    auto subarray_reader = read_state_reader.getCurSubarrayPartition();
    RETURN_NOT_OK(utils::deserialize_subarray(
        subarray_reader, array_schema, &read_state->cur_subarray_partition_));
  }

  // Deserialize partitions
  for (auto* subarray : read_state->subarray_partitions_)
    std::free(subarray);
  read_state->subarray_partitions_.clear();
  if (read_state_reader.hasSubarrayPartitions()) {
    auto partitions_reader = read_state_reader.getSubarrayPartitions();
    const size_t num_partitions = partitions_reader.size();
    for (size_t i = 0; i < num_partitions; i++) {
      auto subarray_reader = partitions_reader[i];
      void* partition;
      RETURN_NOT_OK(utils::deserialize_subarray(
          subarray_reader, array_schema, &partition));
      read_state->subarray_partitions_.push_back(partition);
    }
  }

  return Status::Ok();
}

Status query_to_capnp(
    const Query& query, capnp::Query::Builder* query_builder) {
  using namespace tiledb::sm;

  // For easy reference
  auto layout = query.layout();
  auto type = query.type();
  auto array = query.array();

  if (layout == Layout::GLOBAL_ORDER)
    return LOG_STATUS(Status::SerializationError(
        "Cannot serialize; global order serialization not supported."));

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

  // Serialize subarray
  const void* subarray = query.subarray();
  if (subarray != nullptr) {
    auto subarray_builder = query_builder->initSubarray();
    RETURN_NOT_OK(
        utils::serialize_subarray(subarray_builder, schema, subarray));
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
    bool clientside,
    void* buffer_start,
    Query* query) {
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

  // Deserialize and set subarray.
  const bool sparse_write = !schema->dense() || layout == Layout::UNORDERED;
  if (sparse_write) {
    // Sparse writes cannot have a subarray; clear it here.
    RETURN_NOT_OK(query->set_subarray(nullptr));
  } else {
    auto subarray_reader = query_reader.getSubarray();
    void* subarray;
    RETURN_NOT_OK(
        utils::deserialize_subarray(subarray_reader, schema, &subarray));
    RETURN_NOT_OK_ELSE(query->set_subarray(subarray), std::free(subarray));
    std::free(subarray);
  }

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
      if ((var_size && (*existing_offset_buffer_size < fixedlen_size ||
                        *existing_buffer_size < varlen_size)) ||
          (!var_size && *existing_buffer_size < fixedlen_size)) {
        return LOG_STATUS(Status::SerializationError(
            "Error deserializing read query; buffer too small for attribute "
            "'" +
            attribute_name + "'."));
      }

      // For reads, copy the response data into user buffers. For writes,
      // nothing to do.
      if (type == QueryType::READ) {
        if (var_size) {
          // Var size attribute; buffers already set.
          std::memcpy(
              existing_offset_buffer, attribute_buffer_start, fixedlen_size);
          attribute_buffer_start += fixedlen_size;
          std::memcpy(existing_buffer, attribute_buffer_start, varlen_size);
          attribute_buffer_start += varlen_size;

          // Need to update the buffer size to the actual data size so that
          // the user can check the result size on reads.
          *existing_offset_buffer_size = fixedlen_size;
          *existing_buffer_size = varlen_size;
        } else {
          // Fixed size attribute; buffers already set.
          std::memcpy(existing_buffer, attribute_buffer_start, fixedlen_size);
          attribute_buffer_start += fixedlen_size;

          // Need to update the buffer size to the actual data size so that
          // the user can check the result size on reads.
          *existing_buffer_size = fixedlen_size;
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
        Buffer offsets_buff(nullptr, fixedlen_size, false);
        Buffer varlen_buff(nullptr, varlen_size, false);
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
          Buffer offsets_buff(offsets, fixedlen_size, false);
          Buffer varlen_buff(varlen_data, varlen_size, false);
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
          Buffer buff(data, fixedlen_size, false);
          Buffer varlen_buff(nullptr, 0, false);
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
    Buffer* serialized_buffer) {
  STATS_FUNC_IN(serialization_query_serialize);

  if (serialize_type == SerializationType::JSON)
    return LOG_STATUS(Status::SerializationError(
        "Cannot serialize query; json format not supported."));

  try {
    ::capnp::MallocMessageBuilder message;
    capnp::Query::Builder query_builder = message.initRoot<capnp::Query>();
    RETURN_NOT_OK(query_to_capnp(*query, &query_builder));

    // Determine whether we should be serializing the buffer data.
    const bool serialize_buffers =
        (clientside && query->type() == QueryType::WRITE) ||
        (!clientside && query->type() == QueryType::READ);

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    uint64_t total_fixed_len_bytes =
        query_builder.getTotalFixedLengthBufferBytes();
    uint64_t total_var_len_bytes = query_builder.getTotalVarLenBufferBytes();
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(query_builder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(serialized_buffer->realloc(json_len + 1));
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len))
        RETURN_NOT_OK(serialized_buffer->write(&nul, 1));
        // TODO: At this point the buffer data should also be serialized.
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();

        auto total_nbytes = message_chars.size();
        if (serialize_buffers)
          total_nbytes += total_fixed_len_bytes + total_var_len_bytes;

        // Write the serialized query
        RETURN_NOT_OK(serialized_buffer->realloc(total_nbytes));
        RETURN_NOT_OK(serialized_buffer->write(
            message_chars.begin(), message_chars.size()));

        const auto* array_schema = query->array_schema();
        if (array_schema == nullptr || query->array() == nullptr)
          return LOG_STATUS(Status::SerializationError(
              "Cannot serialize; array or array schema is null."));

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
                RETURN_NOT_OK(serialized_buffer->write(
                    offset_buffer, *offset_buffer_size));
                RETURN_NOT_OK(serialized_buffer->write(buffer, *buffer_size));
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
                RETURN_NOT_OK(serialized_buffer->write(buffer, *buffer_size));
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

Status query_deserialize(
    const Buffer& serialized_buffer,
    SerializationType serialize_type,
    bool clientside,
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
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            query_builder);
        capnp::Query::Reader query_reader = query_builder.asReader();
        return query_from_capnp(query_reader, clientside, nullptr, query);
      }
      case SerializationType::CAPNP: {
        // Capnp FlatArrayMessageReader requires 64-bit alignment.
        if (!utils::is_aligned<sizeof(uint64_t)>(serialized_buffer.data()))
          return LOG_STATUS(Status::SerializationError(
              "Could not deserialize query; buffer is not 8-byte aligned."));

        // Set traversal limit to 10GI (TODO: make this a config option)
        ::capnp::ReaderOptions readerOptions;
        readerOptions.traversalLimitInWords = uint64_t(1024) * 1024 * 1024 * 10;
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
        return query_from_capnp(query_reader, clientside, buffer_start, query);
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

#else

Status query_serialize(Query*, SerializationType, bool, Buffer*) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status query_deserialize(const Buffer&, SerializationType, bool, Query*) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
