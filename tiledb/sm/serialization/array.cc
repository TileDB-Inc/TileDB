/**
 * @file   array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/array_directory.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/fragment_metadata.h"

using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {
namespace serialization {

class ArraySerializationStatusException : public StatusException {
 public:
  explicit ArraySerializationStatusException(const std::string& message)
      : StatusException("[TileDB::Serialization][Array]", message) {
  }
};

#ifdef TILEDB_SERIALIZATION

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
    auto key = std::string{std::string_view{
        entry_reader.getKey().cStr(), entry_reader.getKey().size()}};
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
  array_builder->setOpenedAtEndTimestamp(array->timestamp_end_opened_at());

  array_builder->setQueryType(query_type_str(array->get_query_type()));

  if (array->use_refactored_array_open() && array->serialize_enumerations()) {
    array->load_all_enumerations();
  }

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

  if (array->use_refactored_query_submit()) {
    // Serialize array directory (load if not loaded already)
    const auto array_directory = array->load_array_directory();
    auto array_directory_builder = array_builder->initArrayDirectory();
    array_directory_to_capnp(array_directory, &array_directory_builder);

    // Serialize fragment metadata iff loaded (if the array is open for READs)
    if (array->get_query_type() == QueryType::READ) {
      auto fragment_metadata_all = array->fragment_metadata();
      if (!fragment_metadata_all.empty()) {
        auto fragment_metadata_all_builder =
            array_builder->initFragmentMetadataAll(
                fragment_metadata_all.size());
        for (size_t i = 0; i < fragment_metadata_all.size(); i++) {
          auto fragment_metadata_builder = fragment_metadata_all_builder[i];

          // Old fragment with zipped coordinates didn't have a format that
          // allow to dynamically load tile offsets and sizes and since they all
          // get loaded at array open, we need to serialize them here.
          if (fragment_metadata_all[i]->version() <= 2) {
            fragment_meta_sizes_offsets_to_capnp(
                *fragment_metadata_all[i], &fragment_metadata_builder);
          }
          RETURN_NOT_OK(fragment_metadata_to_capnp(
              *fragment_metadata_all[i], &fragment_metadata_builder));
        }
      }
    }
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
    const capnp::Array::Reader& array_reader,
    StorageManager* storage_manager,
    Array* array,
    const bool client_side) {
  // The serialized URI is set if it exists
  // this is used for backwards compatibility with pre TileDB 2.5 clients that
  // want to serialized a query object TileDB >= 2.5 no longer needs to receive
  // the array URI
  if (array_reader.hasUri()) {
    array->set_uri_serialized(array_reader.getUri().cStr());
  }
  array->set_timestamp_start(array_reader.getStartTimestamp());
  array->set_timestamp_end(array_reader.getEndTimestamp());

  if (array_reader.hasQueryType()) {
    auto query_type_str = array_reader.getQueryType();
    QueryType query_type = QueryType::READ;
    RETURN_NOT_OK(query_type_enum(query_type_str, &query_type));
    array->set_query_type(query_type);
    if (!array->is_open()) {
      array->set_serialized_array_open();
    }

    array->set_timestamp_end_opened_at(array_reader.getOpenedAtEndTimestamp());
    if (array->timestamp_end_opened_at() == UINT64_MAX) {
      if (query_type == QueryType::READ) {
        array->set_timestamp_end_opened_at(
            tiledb::sm::utils::time::timestamp_now_ms());
      } else if (
          query_type == QueryType::WRITE ||
          query_type == QueryType::MODIFY_EXCLUSIVE ||
          query_type == QueryType::DELETE || query_type == QueryType::UPDATE) {
        array->set_timestamp_end_opened_at(0);
      } else {
        throw StatusException(Status_SerializationError(
            "Cannot open array; Unsupported query type."));
      }
    }
  }

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

  // Deserialize array directory
  if (array_reader.hasArrayDirectory()) {
    const auto& array_directory_reader = array_reader.getArrayDirectory();
    auto array_dir = array_directory_from_capnp(
        array_directory_reader,
        storage_manager->resources(),
        array->array_uri());
    array->set_array_directory(*array_dir);
  }

  if (array_reader.hasFragmentMetadataAll()) {
    array->fragment_metadata().clear();
    array->fragment_metadata().reserve(
        array_reader.getFragmentMetadataAll().size());
    for (auto frag_meta_reader : array_reader.getFragmentMetadataAll()) {
      auto meta = make_shared<FragmentMetadata>(HERE());
      RETURN_NOT_OK(fragment_metadata_from_capnp(
          array->array_schema_latest_ptr(),
          frag_meta_reader,
          meta,
          storage_manager,
          array->memory_tracker()));
      if (client_side) {
        meta->set_rtree_loaded();
      }
      array->fragment_metadata().emplace_back(meta);
    }
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
    array->unsafe_set_config(*decoded_config);
  }

  if (array_open_reader.hasQueryType()) {
    auto query_type_str = array_open_reader.getQueryType();
    QueryType query_type = QueryType::READ;
    RETURN_NOT_OK(query_type_enum(query_type_str, &query_type));
    array->set_query_type(query_type);
  }

  return Status::Ok();
}

void fragments_timestamps_to_capnp(
    uint64_t start_timestamp,
    uint64_t end_timestamp,
    capnp::ArrayFragmentsTimestamps::Builder* array_fragments_builder) {
  array_fragments_builder->setStartTimestamp(start_timestamp);
  array_fragments_builder->setEndTimestamp(end_timestamp);
}

void fragments_timestamps_from_capnp(
    const capnp::ArrayFragmentsTimestamps::Reader& array_fragments_reader,
    uint64_t& start_timestamp,
    uint64_t& end_timestamp) {
  start_timestamp = array_fragments_reader.getStartTimestamp();
  end_timestamp = array_fragments_reader.getEndTimestamp();

  if (start_timestamp > end_timestamp) {
    throw std::logic_error(
        "[fragments_timestamps_from_capnp] Deserialized timestamps are "
        "invalid");
  }
}

void fragments_timestamps_serialize(
    uint64_t start_timestamp,
    uint64_t end_timestamp,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  try {
    // Serialize
    ::capnp::MallocMessageBuilder message;
    auto builder = message.initRoot<capnp::ArrayFragmentsTimestamps>();
    fragments_timestamps_to_capnp(start_timestamp, end_timestamp, &builder);

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
        throw_if_not_ok(serialized_buffer->realloc(json_len + 1));
        throw_if_not_ok(serialized_buffer->write(capnp_json.cStr(), json_len));
        throw_if_not_ok(serialized_buffer->write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        throw_if_not_ok(serialized_buffer->realloc(nbytes));
        throw_if_not_ok(
            serialized_buffer->write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        throw ArraySerializationStatusException(
            "[fragments_timestamps_serialize] Unknown serialization type "
            "passed");
      }
    }

  } catch (kj::Exception& e) {
    throw ArraySerializationStatusException(
        "[fragments_timestamps_serialize] kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw ArraySerializationStatusException(
        "[fragments_timestamps_serialize] exception " + std::string(e.what()));
  }
}

void fragments_timestamps_deserialize(
    uint64_t& start_timestamp,
    uint64_t& end_timestamp,
    SerializationType serialize_type,
    const Buffer& serialized_buffer) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        auto builder =
            message_builder.initRoot<capnp::ArrayFragmentsTimestamps>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            builder);
        auto reader = builder.asReader();
        // Deserialize
        fragments_timestamps_from_capnp(reader, start_timestamp, end_timestamp);
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader msg_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        auto reader = msg_reader.getRoot<capnp::ArrayFragmentsTimestamps>();
        // Deserialize
        fragments_timestamps_from_capnp(reader, start_timestamp, end_timestamp);
        break;
      }
      default: {
        throw ArraySerializationStatusException(
            "[fragments_timestamps_deserialize] "
            "Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw ArraySerializationStatusException(
        "[fragments_timestamps_deserialize] kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw ArraySerializationStatusException(
        "[fragments_timestamps_deserialize] exception " +
        std::string(e.what()));
  }
}

void fragments_list_to_capnp(
    const std::vector<URI>& fragments,
    capnp::ArrayFragmentsList::Builder* array_fragments_list_builder) {
  auto entries_builder =
      array_fragments_list_builder->initEntries(fragments.size());
  for (size_t i = 0; i < fragments.size(); i++) {
    const auto& relative_uri = serialize_array_uri_to_relative(fragments[i]);
    entries_builder.set(i, relative_uri);
  }
}

void fragments_list_from_capnp(
    const capnp::ArrayFragmentsList::Reader& array_fragments_list_reader,
    std::vector<URI>& fragments,
    const URI& array_uri) {
  if (array_fragments_list_reader.hasEntries()) {
    fragments.reserve(array_fragments_list_reader.getEntries().size());
    for (auto entry : array_fragments_list_reader.getEntries()) {
      fragments.emplace_back(
          deserialize_array_uri_to_absolute(entry.cStr(), array_uri));
    }
  }
}

void fragments_list_serialize(
    const std::vector<URI>& fragments,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  if (fragments.empty()) {
    throw ArraySerializationStatusException(
        "[fragments_list_serialize] Fragments vector is empty");
  }

  try {
    // Serialize
    ::capnp::MallocMessageBuilder message;
    auto builder = message.initRoot<capnp::ArrayFragmentsList>();
    fragments_list_to_capnp(fragments, &builder);

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
        throw_if_not_ok(serialized_buffer->realloc(json_len + 1));
        throw_if_not_ok(serialized_buffer->write(capnp_json.cStr(), json_len));
        throw_if_not_ok(serialized_buffer->write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        throw_if_not_ok(serialized_buffer->realloc(nbytes));
        throw_if_not_ok(
            serialized_buffer->write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        throw ArraySerializationStatusException(
            "[fragments_list_serialize] Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    throw ArraySerializationStatusException(
        "[fragments_list_serialize] kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw ArraySerializationStatusException(
        "[fragments_list_serialize] exception " + std::string(e.what()));
  }
}

void fragments_list_deserialize(
    std::vector<URI>& fragments,
    const URI& array_uri,
    SerializationType serialize_type,
    const Buffer& serialized_buffer) {
  if (array_uri.is_invalid()) {
    throw ArraySerializationStatusException(
        "[fragments_list_deserialize] Invalid Array URI");
  }

  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        auto builder = message_builder.initRoot<capnp::ArrayFragmentsList>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            builder);
        auto reader = builder.asReader();
        // Deserialize
        fragments_list_from_capnp(reader, fragments, array_uri);
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader msg_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        auto reader = msg_reader.getRoot<capnp::ArrayFragmentsList>();
        // Deserialize
        fragments_list_from_capnp(reader, fragments, array_uri);
        break;
      }
      default: {
        throw ArraySerializationStatusException(
            "[fragments_list_deserialize] Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw ArraySerializationStatusException(
        "[fragments_list_deserialize] kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw ArraySerializationStatusException(
        "[fragments_list_deserialize] exception " + std::string(e.what()));
  }
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
    const Buffer& serialized_buffer,
    StorageManager* storage_manager) {
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
        RETURN_NOT_OK(array_from_capnp(array_reader, storage_manager, array));
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::Array::Reader array_reader = reader.getRoot<capnp::Array>();
        RETURN_NOT_OK(array_from_capnp(array_reader, storage_manager, array));
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

Status array_deserialize(
    Array*, SerializationType, const Buffer&, StorageManager*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

Status array_open_serialize(const Array&, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status array_open_deserialize(Array*, SerializationType, const Buffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

void fragments_timestamps_serialize(
    uint64_t, uint64_t, SerializationType, Buffer*) {
  throw ArraySerializationStatusException(
      "Cannot serialize; serialization not enabled.");
}

void fragments_timestamps_deserialize(
    uint64_t&, uint64_t&, SerializationType, const Buffer&) {
  throw ArraySerializationStatusException(
      "Cannot deserialize; serialization not enabled.");
}

void fragments_list_serialize(
    const std::vector<URI>&, SerializationType, Buffer*) {
  throw ArraySerializationStatusException(
      "Cannot serialize; serialization not enabled.");
}

void fragments_list_deserialize(
    std::vector<URI>&, const URI&, SerializationType, const Buffer&) {
  throw ArraySerializationStatusException(
      "Cannot deserialize; serialization not enabled.");
}

Status metadata_serialize(Metadata*, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status metadata_deserialize(Metadata*, SerializationType, const Buffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
