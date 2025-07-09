/**
 * @file   array_schema.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file declares serialization functions for ArraySchema.
 */

#ifndef TILEDB_SERIALIZATION_ARRAY_SCHEMA_H
#define TILEDB_SERIALIZATION_ARRAY_SCHEMA_H

#include <unordered_map>

#include "tiledb/common/status.h"
#include "tiledb/sm/config/config.h"

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class ArraySchema;
class Dimension;
class MemoryTracker;
class SerializationBuffer;
class URI;
enum class SerializationType : uint8_t;

namespace serialization {

class LoadArraySchemaRequest {
 public:
  explicit LoadArraySchemaRequest(const Config& config)
      : include_enumerations_latest_schema_(config.get<bool>(
            "rest.load_enumerations_on_array_open", Config::must_find))
      , include_enumerations_all_schemas_(config.get<bool>(
            "rest.load_enumerations_on_array_open", Config::must_find)) {
  }

  inline bool include_enumerations_latest_schema() const {
    return include_enumerations_latest_schema_;
  }

  inline bool include_enumerations_all_schemas() const {
    return include_enumerations_all_schemas_;
  }

 private:
  bool include_enumerations_latest_schema_;
  bool include_enumerations_all_schemas_;
};

#ifdef TILEDB_SERIALIZATION
/**
 * Serialize a filter to cap'n proto object
 *
 * @param filter Filter to serialize.
 * @param filter_builder Cap'n proto class.
 * @return Status
 */
Status filter_to_capnp(
    const Filter* filter, capnp::Filter::Builder* filter_builder);

/**
 * Deserialize a filter from a cap'n proto object
 *
 * @param filter_reader Cap'n proto object
 * @param datatype Datatype the filter operates on within it's pipeline.
 * @return Status
 */
tuple<Status, optional<shared_ptr<Filter>>> filter_from_capnp(
    const capnp::Filter::Reader& filter_reader, Datatype datatype);

/**
 * Serialize an array schema to cap'n proto object
 *
 * @param array_schema Array schema to serialize
 * @param array_schema_builder Cap'n proto class
 * @param client_side indicate if client or server side. If server side we won't
 * serialize the array URI
 * @param storage_uri optional storage URI to use when creating an array
 * @return Status
 */
Status array_schema_to_capnp(
    const ArraySchema& array_schema,
    capnp::ArraySchema::Builder* array_schema_builder,
    const bool client_side,
    std::optional<std::string> storage_uri = std::nullopt);

/**
 * Deserialize an array schema from a cap'n proto object
 *
 * @param schema_reader Cap'n proto object
 * @param uri A URI object
 * @param memory_tracker The memory tracker to use.
 * @return a new ArraySchema
 */
shared_ptr<ArraySchema> array_schema_from_capnp(
    const capnp::ArraySchema::Reader& schema_reader,
    const URI& uri,
    shared_ptr<MemoryTracker> memory_tracker);

/**
 * Serialize an ArrayCreateRequest via Cap'n Proto
 *
 * @param array_create_builder ArrayCreateRequest Cap'n Proto builder
 * @param array_schema Array schema for the created array
 * @param storage_uri Storage URI to use when creating an array
 */
void array_create_to_capnp(
    capnp::ArrayCreateRequest::Builder* array_create_builder,
    const ArraySchema& array_schema,
    std::string storage_uri);

/**
 * Serialize a dimension label to cap'n proto object
 *
 * @param dim_label Dimension label to serialize
 * @param builder Cap'n proto class.
 * @param client_side Indicate if client or server side.
 */
void dimension_label_to_capnp(
    const DimensionLabel& dimension_label,
    capnp::DimensionLabel::Builder* dim_label_builder,
    const bool client_side);

/**
 * Deserialize a dimension label from a cap'n proto object
 *
 * @param reader Cap'n proto reader object.
 * @param memory_tracker The memory tracker to use.
 * @return A new DimensionLabel.
 */
shared_ptr<DimensionLabel> dimension_label_from_capnp(
    const capnp::DimensionLabel::Reader& reader,
    shared_ptr<MemoryTracker> memory_tracker);

#endif  // TILEDB_SERIALIZATION

/**
 * Serialize an array schema via Cap'n Prto
 * @param array_schema schema object to serialize
 * @param serialize_type format to serialize into Cap'n Proto or JSON
 * @param serialized_buffer buffer to store serialized bytes in
 * @param client_side indicate if client or server side. If server side we won't
 * serialize the array URI
 * @param storage_uri optional storage URI to use when creating an array
 * @return
 */
Status array_schema_serialize(
    const ArraySchema& array_schema,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer,
    const bool client_side,
    std::optional<std::string> storage_uri = std::nullopt);

shared_ptr<ArraySchema> array_schema_deserialize(
    SerializationType serialize_type,
    span<const char> serialized_buffer,
    shared_ptr<MemoryTracker> memory_tracker);

/**
 * Serialize an ArrayCreateRequest
 *
 * @param array_schema Array schema for the created array
 * @param serialize_type Format to serialize into Cap'n Proto or JSON
 * @param serialized_buffer Buffer to store serialized bytes in
 * @param storage_uri Storage URI to use when creating an array
 */
void array_create_serialize(
    const ArraySchema& array_schema,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer,
    std::string storage_uri);

/**
 * Deserialize an ArrayCreateRequest
 *
 * @param serialize_type Format to serialize into Cap'n Proto or JSON
 * @param serialized_buffer Buffer to read serialized bytes from
 * @param memory_tracker The MemoryTracker to use for deserialization.
 */
shared_ptr<ArraySchema> array_create_deserialize(
    SerializationType serialize_type,
    span<const char> serialized_buffer,
    shared_ptr<MemoryTracker> memory_tracker);

Status nonempty_domain_serialize(
    const Array* array,
    const void* nonempty_domain,
    bool is_empty,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer);

Status nonempty_domain_deserialize(
    const Array* array,
    span<const char> serialized_buffer,
    SerializationType serialize_type,
    void* nonempty_domain,
    bool* is_empty);

Status nonempty_domain_serialize(
    Array* array,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer);

Status nonempty_domain_deserialize(
    Array* array,
    span<const char> serialized_buffer,
    SerializationType serialize_type);

void serialize_load_array_schema_request(
    const Config& config,
    const LoadArraySchemaRequest& req,
    SerializationType serialization_type,
    SerializationBuffer& data);

LoadArraySchemaRequest deserialize_load_array_schema_request(
    SerializationType serialization_type, span<const char> data);

void serialize_load_array_schema_response(
    const Array& array,
    SerializationType serialization_type,
    SerializationBuffer& data);

std::tuple<
    shared_ptr<ArraySchema>,
    std::unordered_map<std::string, shared_ptr<ArraySchema>>>
deserialize_load_array_schema_response(
    const URI& uri,
    const Config& config,
    SerializationType serialization_type,
    span<const char> data,
    shared_ptr<MemoryTracker> memory_tracker);

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION_ARRAY_SCHEMA_H
