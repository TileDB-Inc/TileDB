/**
 * @file enumeration.cc
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
 * This file implements serialization for the Enumeration class
 */

// clang-format off
#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif
// clang-format on

#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/serialization/enumeration.h"

using namespace tiledb::common;

namespace tiledb::sm::serialization {

class EnumerationSerializationException : public StatusException {
 public:
  explicit EnumerationSerializationException(const std::string& message)
      : StatusException("[TileDB::Serialization][Enumeration]", message) {
  }
};

class EnumerationSerializationDisabledException
    : public EnumerationSerializationException {
 public:
  explicit EnumerationSerializationDisabledException()
      : EnumerationSerializationException(
            "Cannot (de)serialize; serialization not enabled.") {
  }
};

#ifdef TILEDB_SERIALIZATION

void enumeration_to_capnp(
    shared_ptr<const Enumeration> enumeration,
    capnp::Enumeration::Builder& enmr_builder) {
  enmr_builder.setName(enumeration->name());
  enmr_builder.setPathName(enumeration->path_name());
  enmr_builder.setType(datatype_str(enumeration->type()));
  enmr_builder.setCellValNum(enumeration->cell_val_num());
  enmr_builder.setOrdered(enumeration->ordered());

  auto dspan = enumeration->data();
  if (dspan.size() > 0) {
    enmr_builder.setData(::kj::arrayPtr(dspan.data(), dspan.size()));
  }

  auto ospan = enumeration->offsets();
  if (ospan.size() > 0) {
    enmr_builder.setOffsets(::kj::arrayPtr(ospan.data(), ospan.size()));
  }
}

shared_ptr<const Enumeration> enumeration_from_capnp(
    const capnp::Enumeration::Reader& reader,
    shared_ptr<MemoryTracker> memory_tracker) {
  auto name = reader.getName();
  auto path_name = reader.getPathName();
  Datatype datatype = datatype_enum(reader.getType());

  const void* data = nullptr;
  uint64_t data_size = 0;

  if (reader.hasData()) {
    auto data_reader = reader.getData().asBytes();
    data = data_reader.begin();
    data_size = data_reader.size();
  }

  const void* offsets = nullptr;
  uint64_t offsets_size = 0;

  if (reader.hasOffsets()) {
    auto offsets_reader = reader.getOffsets().asBytes();
    offsets = offsets_reader.begin();
    offsets_size = offsets_reader.size();
  }

  return Enumeration::create(
      name,
      path_name,
      datatype,
      reader.getCellValNum(),
      reader.getOrdered(),
      data,
      data_size,
      offsets,
      offsets_size,
      memory_tracker);
}

void load_enumerations_request_to_capnp(
    capnp::LoadEnumerationsRequest::Builder& builder,
    const Config& config,
    const std::vector<std::string>& enumeration_names) {
  auto config_builder = builder.initConfig();
  throw_if_not_ok(config_to_capnp(config, &config_builder));

  auto num_names = enumeration_names.size();
  if (num_names > 0) {
    auto names_builder = builder.initEnumerations(num_names);
    for (size_t i = 0; i < num_names; i++) {
      names_builder.set(i, enumeration_names[i]);
    }
  }
}

std::vector<std::string> load_enumerations_request_from_capnp(
    const capnp::LoadEnumerationsRequest::Reader& reader) {
  std::vector<std::string> ret;
  if (reader.hasEnumerations()) {
    for (auto name_reader : reader.getEnumerations()) {
      ret.push_back(name_reader.cStr());
    }
  }

  return ret;
}

void load_enumerations_response_to_capnp(
    capnp::LoadEnumerationsResponse::Builder& builder,
    const std::unordered_map<
        std::string,
        std::vector<shared_ptr<const Enumeration>>>& enumerations) {
  auto num_schemas = enumerations.size();
  // If there is only one schema, it is always the latest so we don't need to
  // serialize the extra data.
  if (num_schemas == 1) {
    auto num_enmr = enumerations.begin()->second.size();
    auto enmr_builders = builder.initEnumerations(num_enmr);
    for (size_t i = 0; i < num_enmr; i++) {
      auto enmr_builder = enmr_builders[i];
      enumeration_to_capnp(enumerations.begin()->second[i], enmr_builder);
    }
  } else if (num_schemas > 1) {
    // If there were enumerations loaded on multiple schemas, serialize the full
    // map of schema_names and their enumerations.
    auto enmr_map_builder = builder.initAllEnumerations();
    auto map_entry_builder = enmr_map_builder.initEntries(num_schemas);
    for (size_t i = 0; const auto& entry : enumerations) {
      auto num_enmr = entry.second.size();
      // Set the map key to the schema name
      map_entry_builder[i].setKey(entry.first);

      // Build the list of enumerations that map to this schema name.
      auto enmr_builders = map_entry_builder[i++].initValue(num_enmr);
      for (size_t j = 0; j < num_enmr; j++) {
        auto enmr_builder = enmr_builders[j];
        enumeration_to_capnp(entry.second[j], enmr_builder);
      }
    }
  }
}

std::unordered_map<std::string, std::vector<shared_ptr<const Enumeration>>>
load_enumerations_response_from_capnp(
    const capnp::LoadEnumerationsResponse::Reader& reader,
    const ArraySchema& array_schema,
    shared_ptr<MemoryTracker> memory_tracker) {
  std::unordered_map<std::string, std::vector<shared_ptr<const Enumeration>>>
      ret;
  if (reader.hasEnumerations()) {
    std::vector<shared_ptr<const Enumeration>> loaded_enmrs;
    auto enmr_readers = reader.getEnumerations();
    for (auto enmr_reader : enmr_readers) {
      loaded_enmrs.push_back(
          enumeration_from_capnp(enmr_reader, memory_tracker));
    }
    // The name of the latest array schema will not be serialized in the
    // response if we are only loading enumerations from the latest schema.
    return {{array_schema.name(), loaded_enmrs}};
  } else if (reader.hasAllEnumerations()) {
    auto all_enmrs_reader = reader.getAllEnumerations();
    for (auto enmr_entry_reader : all_enmrs_reader.getEntries()) {
      std::vector<shared_ptr<const Enumeration>> loaded_enmrs;
      for (auto enmr_reader : enmr_entry_reader.getValue()) {
        loaded_enmrs.push_back(
            enumeration_from_capnp(enmr_reader, memory_tracker));
      }

      ret[enmr_entry_reader.getKey()] = loaded_enmrs;
    }
  }

  return ret;
}

void serialize_load_enumerations_request(
    const Config& config,
    const std::vector<std::string>& enumeration_names,
    SerializationType serialize_type,
    SerializationBuffer& request) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::LoadEnumerationsRequest::Builder builder =
        message.initRoot<capnp::LoadEnumerationsRequest>();
    load_enumerations_request_to_capnp(builder, config, enumeration_names);

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        request.assign_null_terminated(capnp_json);
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        request.assign(protomessage.asChars());
        break;
      }
      default: {
        throw EnumerationSerializationException(
            "Error serializing load enumerations request; "
            "Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    throw EnumerationSerializationException(
        "Error serializing load enumerations request; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw EnumerationSerializationException(
        "Error serializing load enumerations request; exception " +
        std::string(e.what()));
  }
}

std::vector<std::string> deserialize_load_enumerations_request(
    SerializationType serialize_type, span<const char> request) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::LoadEnumerationsRequest::Builder builder =
            message_builder.initRoot<capnp::LoadEnumerationsRequest>();
        json.decode(kj::StringPtr(request.data()), builder);
        capnp::LoadEnumerationsRequest::Reader reader = builder.asReader();
        return load_enumerations_request_from_capnp(reader);
      }
      case SerializationType::CAPNP: {
        const auto mBytes = reinterpret_cast<const kj::byte*>(request.data());
        ::capnp::FlatArrayMessageReader array_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            request.size() / sizeof(::capnp::word)));
        capnp::LoadEnumerationsRequest::Reader reader =
            array_reader.getRoot<capnp::LoadEnumerationsRequest>();
        return load_enumerations_request_from_capnp(reader);
      }
      default: {
        throw EnumerationSerializationException(
            "Error deserializing load enumerations request; "
            "Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw EnumerationSerializationException(
        "Error deserializing load enumerations request; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw EnumerationSerializationException(
        "Error deserializing load enumerations request; exception " +
        std::string(e.what()));
  }
}

void serialize_load_enumerations_response(
    const std::unordered_map<
        std::string,
        std::vector<shared_ptr<const Enumeration>>>& enumerations,
    SerializationType serialize_type,
    SerializationBuffer& response) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::LoadEnumerationsResponse::Builder builder =
        message.initRoot<capnp::LoadEnumerationsResponse>();
    load_enumerations_response_to_capnp(builder, enumerations);

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        response.assign_null_terminated(capnp_json);
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        response.assign(protomessage.asChars());
        break;
      }
      default: {
        throw EnumerationSerializationException(
            "Error serializing load enumerations response; "
            "Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    throw EnumerationSerializationException(
        "Error serializing load enumerations response; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw EnumerationSerializationException(
        "Error serializing load enumerations response; exception " +
        std::string(e.what()));
  }
}

std::unordered_map<std::string, std::vector<shared_ptr<const Enumeration>>>
deserialize_load_enumerations_response(
    const ArraySchema& array_schema,
    SerializationType serialize_type,
    span<const char> response,
    shared_ptr<MemoryTracker> memory_tracker) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::LoadEnumerationsResponse::Builder builder =
            message_builder.initRoot<capnp::LoadEnumerationsResponse>();
        json.decode(kj::StringPtr(response.data(), response.size()), builder);
        capnp::LoadEnumerationsResponse::Reader reader = builder.asReader();
        return load_enumerations_response_from_capnp(
            reader, array_schema, memory_tracker);
      }
      case SerializationType::CAPNP: {
        const auto mBytes = reinterpret_cast<const kj::byte*>(response.data());
        ::capnp::FlatArrayMessageReader array_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            response.size() / sizeof(::capnp::word)));
        capnp::LoadEnumerationsResponse::Reader reader =
            array_reader.getRoot<capnp::LoadEnumerationsResponse>();
        return load_enumerations_response_from_capnp(
            reader, array_schema, memory_tracker);
      }
      default: {
        throw EnumerationSerializationException(
            "Error deserializing load enumerations response; "
            "Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw EnumerationSerializationException(
        "Error deserializing load enumerations response; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw EnumerationSerializationException(
        "Error deserializing load enumerations response; exception " +
        std::string(e.what()));
  }
}

#else

void serialize_load_enumerations_request(
    const Config&,
    const std::vector<std::string>&,
    SerializationType,
    SerializationBuffer&) {
  throw EnumerationSerializationDisabledException();
}

std::vector<std::string> deserialize_load_enumerations_request(
    SerializationType, span<const char>) {
  throw EnumerationSerializationDisabledException();
}

void serialize_load_enumerations_response(
    const std::
        unordered_map<std::string, std::vector<shared_ptr<const Enumeration>>>&,
    SerializationType,
    SerializationBuffer&) {
  throw EnumerationSerializationDisabledException();
}

std::unordered_map<std::string, std::vector<shared_ptr<const Enumeration>>>
deserialize_load_enumerations_response(
    const Array&,
    SerializationType,
    span<const char>,
    shared_ptr<MemoryTracker>) {
  throw EnumerationSerializationDisabledException();
}

#endif  // TILEDB_SERIALIZATION

}  // namespace tiledb::sm::serialization
