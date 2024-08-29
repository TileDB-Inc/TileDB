/**
 * @file   array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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

namespace tiledb::sm::serialization {

class ArraySerializationException : public StatusException {
 public:
  explicit ArraySerializationException(const std::string& message)
      : StatusException("[TileDB::Serialization][Array]", message) {
  }
};

class ArraySerializationDisabledException : public ArraySerializationException {
 public:
  explicit ArraySerializationDisabledException()
      : ArraySerializationException(
            "Cannot (de)serialize; serialization not enabled.") {
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
    auto entry_key = entry_reader.getKey();
    auto key =
        std::string{std::string_view{entry_key.cStr(), entry_key.size()}};
    Datatype type = datatype_enum(entry_reader.getType());
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
      metadata->del(key.c_str());
    } else {
      metadata->put(key.c_str(), type, value_num, value);
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
    array_builder->setUri(array->array_uri_serialized().to_string());
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
      auto& metadata = array->metadata();
      RETURN_NOT_OK(metadata_to_capnp(&metadata, &array_metadata_builder));
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

void array_from_capnp(
    const capnp::Array::Reader& array_reader,
    ContextResources& resources,
    Array* array,
    const bool client_side,
    shared_ptr<MemoryTracker> memory_tracker) {
  // The serialized URI is set if it exists
  // this is used for backwards compatibility with pre TileDB 2.5 clients that
  // want to serialized a query object TileDB >= 2.5 no longer needs to receive
  // the array URI
  if (array_reader.hasUri()) {
    array->set_uri_serialized(array_reader.getUri().cStr());
  }

  if (array_reader.hasQueryType()) {
    auto query_type_str = array_reader.getQueryType();
    QueryType query_type = QueryType::READ;
    throw_if_not_ok(query_type_enum(query_type_str, &query_type));
    array->set_query_type(query_type);

    array->set_timestamps(
        array_reader.getStartTimestamp(),
        array_reader.getEndTimestamp(),
        query_type == QueryType::READ);
  } else {
    array->set_timestamps(
        array_reader.getStartTimestamp(),
        array_reader.getEndTimestamp(),
        false);
  };

  if (!array->is_open()) {
    array->set_serialized_array_open();
  }

  if (array_reader.hasArraySchemasAll()) {
    std::unordered_map<std::string, shared_ptr<ArraySchema>> all_schemas;
    auto all_schemas_reader = array_reader.getArraySchemasAll();

    if (all_schemas_reader.hasEntries()) {
      auto entries = array_reader.getArraySchemasAll().getEntries();
      for (auto array_schema_build : entries) {
        auto schema = array_schema_from_capnp(
            array_schema_build.getValue(), array->array_uri(), memory_tracker);
        schema->set_array_uri(array->array_uri());
        all_schemas[array_schema_build.getKey()] = schema;
      }
    }
    array->set_array_schemas_all(std::move(all_schemas));
  }

  if (array_reader.hasArraySchemaLatest()) {
    auto array_schema_latest_reader = array_reader.getArraySchemaLatest();
    auto array_schema_latest{array_schema_from_capnp(
        array_schema_latest_reader, array->array_uri(), memory_tracker)};
    array_schema_latest->set_array_uri(array->array_uri());
    array->set_array_schema_latest(array_schema_latest);
  }

  // Deserialize array directory
  if (array_reader.hasArrayDirectory()) {
    const auto& array_directory_reader = array_reader.getArrayDirectory();
    auto array_dir = array_directory_from_capnp(
        array_directory_reader, resources, array->array_uri());
    array->set_array_directory(std::move(*array_dir));
  } else {
    array->set_array_directory(ArrayDirectory(resources, array->array_uri()));
  }

  if (array_reader.hasFragmentMetadataAll()) {
    auto fragment_metadata = array->fragment_metadata();
    fragment_metadata.clear();
    auto fragment_metadata_all_reader = array_reader.getFragmentMetadataAll();
    fragment_metadata.reserve(fragment_metadata_all_reader.size());
    for (auto frag_meta_reader : fragment_metadata_all_reader) {
      auto meta = make_shared<FragmentMetadata>(
          HERE(),
          &resources,
          array->memory_tracker(),
          frag_meta_reader.getVersion());

      auto schema = array->array_schema_latest_ptr();
      if (frag_meta_reader.hasArraySchemaName()) {
        auto fragment_array_schema_name =
            frag_meta_reader.getArraySchemaName().cStr();
        schema = array->array_schemas_all().at(fragment_array_schema_name);
      } else if (array->array_schemas_all().size() > 1) {
        throw ArraySerializationException(
            "Cannot deserialize fragment metadata without an array schema name "
            "in the case of arrays with evolved schemas.");
      }

      // pass the right schema to deserialize fragment metadata
      throw_if_not_ok(
          fragment_metadata_from_capnp(schema, frag_meta_reader, meta));
      // if (client_side) {
      // meta->loaded_metadata()->set_rtree_loaded();
      // }
      fragment_metadata.emplace_back(meta);
    }
    array->set_fragment_metadata(std::move(fragment_metadata));
  }

  if (array_reader.hasNonEmptyDomain()) {
    const auto& nonempty_domain_reader = array_reader.getNonEmptyDomain();
    // Deserialize
    throw_if_not_ok(
        utils::deserialize_non_empty_domain(nonempty_domain_reader, array));
    array->set_non_empty_domain_computed(true);
  }

  if (array_reader.hasArrayMetadata()) {
    const auto& array_metadata_reader = array_reader.getArrayMetadata();
    throw_if_not_ok(
        metadata_from_capnp(array_metadata_reader, array->unsafe_metadata()));
    array->set_metadata_loaded(true);
  }
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
    const Config& config,
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
        // Set traversal limit from config
        uint64_t limit =
            config.get<uint64_t>("rest.capnp_traversal_limit").value();
        ::capnp::ReaderOptions readerOptions;
        // capnp uses the limit in words
        readerOptions.traversalLimitInWords = limit / sizeof(::capnp::word);

        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader msg_reader(
            kj::arrayPtr(
                reinterpret_cast<const ::capnp::word*>(mBytes),
                serialized_buffer.size() / sizeof(::capnp::word)),
            readerOptions);
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

void array_deserialize(
    Array* array,
    SerializationType serialize_type,
    const Buffer& serialized_buffer,
    ContextResources& resources,
    shared_ptr<MemoryTracker> memory_tracker) {
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
        array_from_capnp(array_reader, resources, array, true, memory_tracker);
        break;
      }
      case SerializationType::CAPNP: {
        // Set traversal limit from config
        uint64_t limit = resources.config()
                             .get<uint64_t>("rest.capnp_traversal_limit")
                             .value();
        ::capnp::ReaderOptions readerOptions;
        // capnp uses the limit in words
        readerOptions.traversalLimitInWords = limit / sizeof(::capnp::word);

        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(
            kj::arrayPtr(
                reinterpret_cast<const ::capnp::word*>(mBytes),
                serialized_buffer.size() / sizeof(::capnp::word)),
            readerOptions);
        capnp::Array::Reader array_reader = reader.getRoot<capnp::Array>();
        array_from_capnp(array_reader, resources, array, true, memory_tracker);
        break;
      }
      default: {
        throw ArraySerializationException(
            "Error deserializing array; Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    throw ArraySerializationException(
        "Error deserializing array; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw ArraySerializationException(
        "Error deserializing array; exception " + std::string(e.what()));
  }
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

void array_deserialize(
    Array*,
    SerializationType,
    const Buffer&,
    ContextResources&,
    shared_ptr<MemoryTracker>) {
  throw ArraySerializationDisabledException();
}

Status array_open_serialize(const Array&, SerializationType, Buffer*) {
  throw ArraySerializationDisabledException();
}

Status array_open_deserialize(Array*, SerializationType, const Buffer&) {
  throw ArraySerializationDisabledException();
  ;
}

Status metadata_serialize(Metadata*, SerializationType, Buffer*) {
  throw ArraySerializationDisabledException();
}

Status metadata_deserialize(
    Metadata*, const Config&, SerializationType, const Buffer&) {
  throw ArraySerializationDisabledException();
}

#endif  // TILEDB_SERIALIZATION

}  // namespace tiledb::sm::serialization
