/**
 * @file   fragments.cc
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
 * This file declares serialization functions for fragments.
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
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/serialization/fragments.h"

using namespace tiledb::common;

namespace tiledb::sm::serialization {

class FragmentsSerializationException : public StatusException {
 public:
  explicit FragmentsSerializationException(const std::string& message)
      : StatusException("[TileDB::Serialization][Fragments]", message) {
  }
};

#ifdef TILEDB_SERIALIZATION
void fragments_timestamps_to_capnp(
    const Config& config,
    uint64_t start_timestamp,
    uint64_t end_timestamp,
    capnp::ArrayDeleteFragmentsTimestampsRequest::Builder& builder) {
  auto config_builder = builder.initConfig();
  throw_if_not_ok(config_to_capnp(config, &config_builder));

  builder.setStartTimestamp(start_timestamp);
  builder.setEndTimestamp(end_timestamp);
}

std::tuple<uint64_t, uint64_t> fragments_timestamps_from_capnp(
    const capnp::ArrayDeleteFragmentsTimestampsRequest::Reader& reader) {
  return {reader.getStartTimestamp(), reader.getEndTimestamp()};
}

void serialize_delete_fragments_timestamps_request(
    const Config& config,
    uint64_t start_timestamp,
    uint64_t end_timestamp,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer) {
  try {
    // Serialize
    ::capnp::MallocMessageBuilder message;
    auto builder =
        message.initRoot<capnp::ArrayDeleteFragmentsTimestampsRequest>();
    fragments_timestamps_to_capnp(
        config, start_timestamp, end_timestamp, builder);

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        serialized_buffer.assign_null_terminated(capnp_json);
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        serialized_buffer.assign(protomessage.asChars());
        break;
      }
      default: {
        throw FragmentsSerializationException(
            "[fragments_timestamps_serialize] Unknown serialization type "
            "passed");
      }
    }

  } catch (kj::Exception& e) {
    throw FragmentsSerializationException(
        "[fragments_timestamps_serialize] kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw FragmentsSerializationException(
        "[fragments_timestamps_serialize] exception " + std::string(e.what()));
  }
}

std::tuple<uint64_t, uint64_t> deserialize_delete_fragments_timestamps_request(
    SerializationType serialize_type, span<const char> serialized_buffer) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::MallocMessageBuilder message_builder;
        auto builder =
            message_builder
                .initRoot<capnp::ArrayDeleteFragmentsTimestampsRequest>();
        utils::decode_json_message(serialized_buffer, builder);
        auto reader = builder.asReader();
        // Deserialize
        return fragments_timestamps_from_capnp(reader);
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader msg_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        auto reader =
            msg_reader.getRoot<capnp::ArrayDeleteFragmentsTimestampsRequest>();
        // Deserialize
        return fragments_timestamps_from_capnp(reader);
      }
      default: {
        throw FragmentsSerializationException(
            "[fragments_timestamps_deserialize] "
            "Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw FragmentsSerializationException(
        "[fragments_timestamps_deserialize] kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw FragmentsSerializationException(
        "[fragments_timestamps_deserialize] exception " +
        std::string(e.what()));
  }
}

void fragments_list_to_capnp(
    const Config& config,
    const std::vector<URI>& fragments,
    capnp::ArrayDeleteFragmentsListRequest::Builder& builder) {
  auto config_builder = builder.initConfig();
  throw_if_not_ok(config_to_capnp(config, &config_builder));

  auto entries_builder = builder.initEntries(fragments.size());
  for (size_t i = 0; i < fragments.size(); i++) {
    const auto& relative_uri = serialize_array_uri_to_relative(fragments[i]);
    entries_builder.set(i, relative_uri);
  }
}

std::vector<URI> fragments_list_from_capnp(
    const URI& array_uri,
    const capnp::ArrayDeleteFragmentsListRequest::Reader& reader) {
  if (reader.hasEntries()) {
    std::vector<URI> fragments;
    fragments.reserve(reader.getEntries().size());
    auto get_entries_reader = reader.getEntries();
    for (auto entry : get_entries_reader) {
      fragments.emplace_back(
          deserialize_array_uri_to_absolute(entry.cStr(), URI(array_uri)));
    }
    return fragments;
  } else {
    throw FragmentsSerializationException(
        "[fragments_list_from_capnp] There are no fragments to deserialize");
  }
}

void serialize_delete_fragments_list_request(
    const Config& config,
    const std::vector<URI>& fragments,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer) {
  if (fragments.empty()) {
    throw FragmentsSerializationException(
        "[fragments_list_serialize] Fragments vector is empty");
  }

  try {
    // Serialize
    ::capnp::MallocMessageBuilder message;
    auto builder = message.initRoot<capnp::ArrayDeleteFragmentsListRequest>();
    fragments_list_to_capnp(config, fragments, builder);

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        serialized_buffer.assign_null_terminated(capnp_json);
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        serialized_buffer.assign(protomessage.asChars());
        break;
      }
      default: {
        throw FragmentsSerializationException(
            "[fragments_list_serialize] Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    throw FragmentsSerializationException(
        "[fragments_list_serialize] kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw FragmentsSerializationException(
        "[fragments_list_serialize] exception " + std::string(e.what()));
  }
}

std::vector<URI> deserialize_delete_fragments_list_request(
    const URI& array_uri,
    SerializationType serialize_type,
    span<const char> serialized_buffer) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::MallocMessageBuilder message_builder;
        auto builder =
            message_builder.initRoot<capnp::ArrayDeleteFragmentsListRequest>();
        utils::decode_json_message(serialized_buffer, builder);
        auto reader = builder.asReader();
        // Deserialize
        return fragments_list_from_capnp(array_uri, reader);
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader msg_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        auto reader =
            msg_reader.getRoot<capnp::ArrayDeleteFragmentsListRequest>();
        // Deserialize
        return fragments_list_from_capnp(array_uri, reader);
      }
      default: {
        throw FragmentsSerializationException(
            "[fragments_list_deserialize] Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw FragmentsSerializationException(
        "[fragments_list_deserialize] kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw FragmentsSerializationException(
        "[fragments_list_deserialize] exception " + std::string(e.what()));
  }
}

#else
void serialize_delete_fragments_timestamps_request(
    uint64_t, uint64_t, SerializationType, SerializationBuffer&) {
  throw FragmentsSerializationException(
      "Cannot serialize; serialization not enabled.");
}

std::tuple<uint64_t, uint64_t> deserialize_delete_fragments_timestamps_request(
    SerializationType, span<const char>) {
  throw FragmentsSerializationException(
      "Cannot deserialize; serialization not enabled.");
}

void serialize_delete_fragments_list_request(
    const std::vector<URI>&, SerializationType, SerializationBuffer&) {
  throw FragmentsSerializationException(
      "Cannot serialize; serialization not enabled.");
}

std::vector<URI> deserialize_delete_fragments_list_request(
    const URI&, SerializationType, span<const char>) {
  throw FragmentsSerializationException(
      "Cannot deserialize; serialization not enabled.");
}

#endif  // TILEDB_SERIALIZATION
}  // namespace tiledb::sm::serialization
