/**
 * @file   array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file declares serialization functions for Array.
 */

// clang-format off
#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif
// clang-format on

#include <string>

#include "tiledb/common/common.h"
#include "tiledb/sm/buffer/buffer_list.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/array_schema.h"

using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

Status array_directory_to_capnp(
    const ArrayDirectory& array_directory,
    capnp::ArrayDirectory::Builder* array_directory_builder) {
  // set unfiltered fragment uris
  const auto& unfiltered_fragment_uris =
      array_directory.unfiltered_fragment_uris();
  if (!unfiltered_fragment_uris.empty()) {
    auto unfiltered_uris_builder =
        array_directory_builder->initUnfilteredFragmentUris(
            unfiltered_fragment_uris.size());
    for (size_t i = 0; i < unfiltered_fragment_uris.size(); i++) {
      unfiltered_uris_builder.set(i, unfiltered_fragment_uris[i]);
    }
  }

  // set consolidated commit uris
  const auto& consolidated_commit_uris =
      array_directory.consolidated_commit_uris_set();
  if (!consolidated_commit_uris.empty()) {
    auto consolidated_commit_uris_builder =
        array_directory_builder->initConsolidatedCommitUris(
            consolidated_commit_uris.size());
    size_t i = 0;
    for (auto& uri : consolidated_commit_uris) {
      consolidated_commit_uris_builder.set(i++, uri);
    }
  }

  // set array schema uris
  const auto& array_schema_uris = array_directory.array_schema_uris();
  if (!array_schema_uris.empty()) {
    auto array_schema_uris_builder =
        array_directory_builder->initArraySchemaUris(array_schema_uris.size());
    for (size_t i = 0; i < array_schema_uris.size(); i++) {
      array_schema_uris_builder.set(i, array_schema_uris[i]);
    }
  }

  // set latest array schema uri
  if (!array_directory.latest_array_schema_uri().to_string().empty()) {
    array_directory_builder->setLatestArraySchemaUri(
        array_directory.latest_array_schema_uri());
  }

  // set array meta uris to vacuum
  const auto& array_meta_uris_to_vacuum =
      array_directory.array_meta_uris_to_vacuum();
  if (!array_meta_uris_to_vacuum.empty()) {
    auto array_meta_uris_to_vacuum_builder =
        array_directory_builder->initArrayMetaUrisToVacuum(
            array_meta_uris_to_vacuum.size());
    for (size_t i = 0; i < array_meta_uris_to_vacuum.size(); i++) {
      array_meta_uris_to_vacuum_builder.set(i, array_meta_uris_to_vacuum[i]);
    }
  }

  // set array meta vac uris to vacuum
  const auto& array_meta_vac_uris_to_vacuum =
      array_directory.array_meta_vac_uris_to_vacuum();
  if (!array_meta_vac_uris_to_vacuum.empty()) {
    auto array_meta_vac_uris_to_vacuum_builder =
        array_directory_builder->initArrayMetaVacUrisToVacuum(
            array_meta_vac_uris_to_vacuum.size());
    for (size_t i = 0; i < array_meta_vac_uris_to_vacuum.size(); i++) {
      array_meta_vac_uris_to_vacuum_builder.set(
          i, array_meta_vac_uris_to_vacuum[i]);
    }
  }

  // set commit uris to consolidate
  const auto& commit_uris_to_consolidate =
      array_directory.commit_uris_to_consolidate();
  if (!commit_uris_to_consolidate.empty()) {
    auto commit_uris_to_consolidate_builder =
        array_directory_builder->initCommitUrisToConsolidate(
            commit_uris_to_consolidate.size());
    for (size_t i = 0; i < commit_uris_to_consolidate.size(); i++) {
      commit_uris_to_consolidate_builder.set(i, commit_uris_to_consolidate[i]);
    }
  }

  // set commit uris to vacuum
  const auto& commit_uris_to_vacuum = array_directory.commit_uris_to_vacuum();
  if (!commit_uris_to_vacuum.empty()) {
    auto commit_uris_to_vacuum_builder =
        array_directory_builder->initCommitUrisToVacuum(
            commit_uris_to_vacuum.size());
    for (size_t i = 0; i < commit_uris_to_vacuum.size(); i++) {
      commit_uris_to_vacuum_builder.set(i, commit_uris_to_vacuum[i]);
    }
  }

  // set consolidated commit uris to vacuum
  const auto& consolidated_commit_uris_to_vacuum =
      array_directory.consolidated_commits_uris_to_vacuum();
  if (!consolidated_commit_uris_to_vacuum.empty()) {
    auto consolidated_commit_uris_to_vacuum_builder =
        array_directory_builder->initConsolidatedCommitUrisToVacuum(
            consolidated_commit_uris_to_vacuum.size());
    for (size_t i = 0; i < consolidated_commit_uris_to_vacuum.size(); i++) {
      consolidated_commit_uris_to_vacuum_builder.set(
          i, consolidated_commit_uris_to_vacuum[i]);
    }
  }

  // set array meta uris
  const auto& array_meta_uris = array_directory.array_meta_uris();
  if (!array_meta_uris.empty()) {
    auto array_meta_uris_builder =
        array_directory_builder->initArrayMetaUris(array_meta_uris.size());
    for (size_t i = 0; i < array_meta_uris.size(); i++) {
      auto timestamped_uri_builder = array_meta_uris_builder[i];
      timestamped_uri_builder.setUri(array_meta_uris[i].uri_);
      timestamped_uri_builder.setTimestampStart(
          array_meta_uris[i].timestamp_range_.first);
      timestamped_uri_builder.setTimestampEnd(
          array_meta_uris[i].timestamp_range_.second);
    }
  }

  // set fragment meta uris
  const auto& fragment_meta_uris = array_directory.fragment_meta_uris();
  if (!fragment_meta_uris.empty()) {
    auto fragment_meta_uris_builder =
        array_directory_builder->initFragmentMetaUris(
            fragment_meta_uris.size());
    for (size_t i = 0; i < fragment_meta_uris.size(); i++) {
      fragment_meta_uris_builder.set(i, fragment_meta_uris[i]);
    }
  }

  // set delete tiles location
  const auto& delete_tiles_location = array_directory.delete_tiles_location();
  if (!delete_tiles_location.empty()) {
    auto delete_tiles_location_builder =
        array_directory_builder->initDeleteTilesLocation(
            delete_tiles_location.size());
    for (size_t i = 0; i < delete_tiles_location.size(); i++) {
      auto del_tile_location_builder = delete_tiles_location_builder[i];
      del_tile_location_builder.setUri(delete_tiles_location[i].uri());
      del_tile_location_builder.setConditionMarker(
          delete_tiles_location[i].condition_marker());
      del_tile_location_builder.setOffset(delete_tiles_location[i].offset());
      del_tile_location_builder.setTimestamp(
          delete_tiles_location[i].timestamp());
    }
  }

  // set timestamp start
  array_directory_builder->setTimestampStart(array_directory.timestamp_start());

  // set timestamp end
  array_directory_builder->setTimestampEnd(array_directory.timestamp_end());

  return Status::Ok();
}

Status metadata_to_capnp(
    const Metadata* metadata,
    capnp::ArrayMetadata::Builder* array_metadata_builder) {
  if (metadata == nullptr)
    return LOG_STATUS(Status_SerializationError(
        "Error serializing array metadata; array metadata instance is null"));

  auto entries_builder = array_metadata_builder->initEntries(metadata->num());
  size_t i = 0;
  for (auto it = metadata->begin(); it != metadata->end(); ++it) {
    auto entry_builder = entries_builder[i++];
    const auto& entry = it->second;
    auto datatype = static_cast<Datatype>(entry.type_);
    entry_builder.setKey(it->first);
    entry_builder.setType(datatype_str(datatype));
    entry_builder.setValueNum(entry.num_);
    entry_builder.setValue(kj::arrayPtr(
        static_cast<const uint8_t*>(entry.value_.data()), entry.value_.size()));
    entry_builder.setDel(entry.del_ == 1);
  }

  return Status::Ok();
}

Status metadata_from_capnp(
    const capnp::ArrayMetadata::Reader& array_metadata_reader,
    Metadata* metadata) {
  auto entries_reader = array_metadata_reader.getEntries();
  const size_t num_entries = entries_reader.size();

  for (size_t i = 0; i < num_entries; i++) {
    auto entry_reader = entries_reader[i];
    std::string key = entry_reader.getKey();
    Datatype type = Datatype::UINT8;
    RETURN_NOT_OK(datatype_enum(entry_reader.getType(), &type));
    uint32_t value_num = entry_reader.getValueNum();

    auto value_ptr = entry_reader.getValue();
    const void* value = (void*)value_ptr.begin();
    if (value_ptr.size() != datatype_size(type) * value_num) {
      return LOG_STATUS(Status_SerializationError(
          "Error deserializing array metadata; value size sanity check "
          "failed for " +
          key + "."));
    }

    if (entry_reader.getDel()) {
      RETURN_NOT_OK(metadata->del(key.c_str()));
    } else {
      RETURN_NOT_OK(metadata->put(key.c_str(), type, value_num, value));
    }
  }

  return Status::Ok();
}

Status array_to_capnp(
    Array* array,
    capnp::Array::Builder* array_builder,
    const bool client_side) {
  // The serialized URI is set if it exists
  // this is used for backwards compatibility with pre TileDB 2.5 clients that
  // want to serialized a query object TileDB >= 2.5 no longer needs to send the
  // array URI
  if (!array->array_uri_serialized().to_string().empty()) {
    array_builder->setUri(array->array_uri_serialized());
  }
  array_builder->setStartTimestamp(array->timestamp_start());
  array_builder->setEndTimestamp(array->timestamp_end());

  const auto& array_schema_latest = array->array_schema_latest();
  auto array_schema_latest_builder = array_builder->initArraySchemaLatest();
  RETURN_NOT_OK(array_schema_to_capnp(
      array_schema_latest, &array_schema_latest_builder, client_side));

  const auto& array_schemas_all = array->array_schemas_all();
  auto array_schemas_all_builder = array_builder->initArraySchemasAll();
  auto entries_builder =
      array_schemas_all_builder.initEntries(array_schemas_all.size());
  uint64_t i = 0;
  for (const auto& schema : array_schemas_all) {
    auto entry = entries_builder[i++];
    entry.setKey(schema.first);
    auto schema_builder = entry.initValue();
    RETURN_NOT_OK(array_schema_to_capnp(
        *(schema.second.get()), &schema_builder, client_side));
  }

  if (array->use_refactored_array_open()) {
    if (array->serialize_non_empty_domain()) {
      auto nonempty_domain_builder = array_builder->initNonEmptyDomain();
      RETURN_NOT_OK(
          utils::serialize_non_empty_domain(nonempty_domain_builder, array));
    }

    if (array->serialize_metadata()) {
      auto array_metadata_builder = array_builder->initArrayMetadata();
      // If this is the Cloud server, it should load and serialize metadata
      // If this is the client, it should have previously received the array
      // metadata from the Cloud server, so it should just serialize it
      Metadata* metadata = nullptr;
      // Get metadata. If not loaded, load it first.
      RETURN_NOT_OK(array->metadata(&metadata));
      RETURN_NOT_OK(metadata_to_capnp(metadata, &array_metadata_builder));
    }
  } else {
    if (array->non_empty_domain_computed()) {
      auto nonempty_domain_builder = array_builder->initNonEmptyDomain();
      RETURN_NOT_OK(
          utils::serialize_non_empty_domain(nonempty_domain_builder, array));
    }

    if (array->metadata_loaded()) {
      auto array_metadata_builder = array_builder->initArrayMetadata();
      RETURN_NOT_OK(
          metadata_to_capnp(array->unsafe_metadata(), &array_metadata_builder));
    }
  }

  return Status::Ok();
}

Status array_from_capnp(
    const capnp::Array::Reader& array_reader, Array* array) {
  // The serialized URI is set if it exists
  // this is used for backwards compatibility with pre TileDB 2.5 clients that
  // want to serialized a query object TileDB >= 2.5 no longer needs to receive
  // the array URI
  if (array_reader.hasUri()) {
    RETURN_NOT_OK(array->set_uri_serialized(array_reader.getUri().cStr()));
  }
  RETURN_NOT_OK(array->set_timestamp_start(array_reader.getStartTimestamp()));
  RETURN_NOT_OK(array->set_timestamp_end(array_reader.getEndTimestamp()));

  if (array_reader.hasArraySchemasAll()) {
    std::unordered_map<std::string, shared_ptr<ArraySchema>> all_schemas;
    auto all_schemas_reader = array_reader.getArraySchemasAll();

    if (all_schemas_reader.hasEntries()) {
      auto entries = array_reader.getArraySchemasAll().getEntries();
      for (auto array_schema_build : entries) {
        auto schema{array_schema_from_capnp(
            array_schema_build.getValue(), array->array_uri())};
        schema.set_array_uri(array->array_uri());
        all_schemas[array_schema_build.getKey()] =
            make_shared<ArraySchema>(HERE(), schema);
      }
    }
    array->set_array_schemas_all(all_schemas);
  }

  if (array_reader.hasArraySchemaLatest()) {
    auto array_schema_latest_reader = array_reader.getArraySchemaLatest();
    auto array_schema_latest{array_schema_from_capnp(
        array_schema_latest_reader, array->array_uri())};
    array_schema_latest.set_array_uri(array->array_uri());
    array->set_array_schema_latest(
        make_shared<ArraySchema>(HERE(), array_schema_latest));
  }

  if (array_reader.hasNonEmptyDomain()) {
    const auto& nonempty_domain_reader = array_reader.getNonEmptyDomain();
    // Deserialize
    RETURN_NOT_OK(
        utils::deserialize_non_empty_domain(nonempty_domain_reader, array));
    array->set_non_empty_domain_computed(true);
  }

  if (array_reader.hasArrayMetadata()) {
    const auto& array_metadata_reader = array_reader.getArrayMetadata();
    // Deserialize
    RETURN_NOT_OK(
        metadata_from_capnp(array_metadata_reader, array->unsafe_metadata()));
    array->set_metadata_loaded(true);
  }

  return Status::Ok();
}

Status array_open_to_capnp(
    const Array& array, capnp::ArrayOpen::Builder* array_open_builder) {
  // Set config
  auto config_builder = array_open_builder->initConfig();
  auto config = array.config();
  RETURN_NOT_OK(config_to_capnp(config, &config_builder));

  array_open_builder->setQueryType(query_type_str(array.get_query_type()));

  return Status::Ok();
}

Status array_open_from_capnp(
    const capnp::ArrayOpen::Reader& array_open_reader, Array* array) {
  if (array == nullptr) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing array open; array is null."));
  }

  if (array_open_reader.hasConfig()) {
    tdb_unique_ptr<Config> decoded_config = nullptr;
    RETURN_NOT_OK(
        config_from_capnp(array_open_reader.getConfig(), &decoded_config));
    RETURN_NOT_OK(array->set_config(*decoded_config));
  }

  if (array_open_reader.hasQueryType()) {
    auto query_type_str = array_open_reader.getQueryType();
    QueryType query_type = QueryType::READ;
    RETURN_NOT_OK(query_type_enum(query_type_str, &query_type));
    array->set_query_type(query_type);
  }

  return Status::Ok();
}

Status metadata_serialize(
    Metadata* metadata,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  if (metadata == nullptr)
    return LOG_STATUS(Status_SerializationError(
        "Error serializing array metadata; array instance is null"));

  try {
    // Serialize
    ::capnp::MallocMessageBuilder message;
    auto builder = message.initRoot<capnp::ArrayMetadata>();

    RETURN_NOT_OK(metadata_to_capnp(metadata, &builder));

    // Copy to buffer
    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(serialized_buffer->realloc(json_len + 1));
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len));
        RETURN_NOT_OK(serialized_buffer->write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        RETURN_NOT_OK(serialized_buffer->realloc(nbytes));
        RETURN_NOT_OK(serialized_buffer->write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error serializing array metadata; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing array metadata; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing array metadata; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status metadata_deserialize(
    Metadata* metadata,
    SerializationType serialize_type,
    const Buffer& serialized_buffer) {
  if (metadata == nullptr)
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing metadata; null metadata instance given."));

  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        auto builder = message_builder.initRoot<capnp::ArrayMetadata>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            builder);
        auto reader = builder.asReader();

        // Deserialize
        RETURN_NOT_OK(metadata_from_capnp(reader, metadata));

        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader msg_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        auto reader = msg_reader.getRoot<capnp::ArrayMetadata>();

        // Deserialize
        RETURN_NOT_OK(metadata_from_capnp(reader, metadata));

        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing array metadata; Unknown serialization type "
            "passed"));
      }
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing array metadata; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing array metadata; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status array_serialize(
    Array* array,
    SerializationType serialize_type,
    Buffer* serialized_buffer,
    const bool client_side) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::Array::Builder ArrayBuilder = message.initRoot<capnp::Array>();
    RETURN_NOT_OK(array_to_capnp(array, &ArrayBuilder, client_side));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(ArrayBuilder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(serialized_buffer->realloc(json_len + 1));
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len));
        RETURN_NOT_OK(serialized_buffer->write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        RETURN_NOT_OK(serialized_buffer->realloc(nbytes));
        RETURN_NOT_OK(serialized_buffer->write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error serializing array; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing array; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing array; exception " + std::string(e.what())));
  }

  return Status::Ok();
}

Status array_deserialize(
    Array* array,
    SerializationType serialize_type,
    const Buffer& serialized_buffer) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::Array::Builder array_builder =
            message_builder.initRoot<capnp::Array>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            array_builder);
        capnp::Array::Reader array_reader = array_builder.asReader();
        RETURN_NOT_OK(array_from_capnp(array_reader, array));
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::Array::Reader array_reader = reader.getRoot<capnp::Array>();
        RETURN_NOT_OK(array_from_capnp(array_reader, array));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing array; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing array; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing array; exception " + std::string(e.what())));
  }

  return Status::Ok();
}

Status array_open_serialize(
    const Array& array,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::ArrayOpen::Builder arrayOpenBuilder =
        message.initRoot<capnp::ArrayOpen>();
    RETURN_NOT_OK(array_open_to_capnp(array, &arrayOpenBuilder));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(arrayOpenBuilder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(serialized_buffer->realloc(json_len + 1));
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len));
        RETURN_NOT_OK(serialized_buffer->write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        RETURN_NOT_OK(serialized_buffer->realloc(nbytes));
        RETURN_NOT_OK(serialized_buffer->write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error serializing array open; Unknown serialization type "
            "passed: " +
            std::to_string(static_cast<uint8_t>(serialize_type))));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing array open; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing array open; exception " + std::string(e.what())));
  }

  return Status::Ok();
}

Status array_open_deserialize(
    Array* array,
    SerializationType serialize_type,
    const Buffer& serialized_buffer) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        json.handleByAnnotation<capnp::ArrayOpen>();
        ::capnp::MallocMessageBuilder message_builder;
        capnp::ArrayOpen::Builder array_open_builder =
            message_builder.initRoot<capnp::ArrayOpen>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            array_open_builder);
        capnp::ArrayOpen::Reader array_open_reader =
            array_open_builder.asReader();
        RETURN_NOT_OK(array_open_from_capnp(array_open_reader, array));
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::ArrayOpen::Reader array_open_reader =
            reader.getRoot<capnp::ArrayOpen>();
        RETURN_NOT_OK(array_open_from_capnp(array_open_reader, array));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing array open; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing array open; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing array open; exception " + std::string(e.what())));
  }

  return Status::Ok();
}

#else

Status array_serialize(Array*, SerializationType, Buffer*, const bool) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status array_deserialize(Array*, SerializationType, const Buffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status array_open_serialize(const Array&, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status array_open_deserialize(Array*, SerializationType, const Buffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

Status metadata_serialize(Metadata*, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status metadata_deserialize(Metadata*, SerializationType, const Buffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
