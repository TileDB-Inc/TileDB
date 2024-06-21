/**
 * @file consolidation.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file defines serialization for the Consolidation class
 */

#include <sstream>

// clang-format off
#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif
// clang-format on

#include "tiledb/common/logger_public.h"
#include "tiledb/sm/consolidator/consolidator.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/serialization/config.h"
#include "tiledb/sm/serialization/consolidation.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace serialization {

class ConsolidationSerializationException : public StatusException {
 public:
  explicit ConsolidationSerializationException(const std::string& message)
      : StatusException("[TileDB::Serialization][Consolidation]", message) {
  }
};

class ConsolidationSerializationDisabledException
    : public ConsolidationSerializationException {
 public:
  explicit ConsolidationSerializationDisabledException()
      : ConsolidationSerializationException(
            "Cannot (de)serialize; serialization not enabled.") {
  }
};

#ifdef TILEDB_SERIALIZATION

void array_consolidation_request_to_capnp(
    const Config& config,
    const std::vector<std::string>* fragment_uris,
    capnp::ArrayConsolidationRequest::Builder*
        array_consolidation_request_builder) {
  // Validate input arguments to be sure that what we are serializing make sense
  auto mode = Consolidator::mode_from_config(config);
  if (mode != ConsolidationMode::FRAGMENT &&
      (fragment_uris != nullptr && !fragment_uris->empty())) {
    throw ConsolidationSerializationException(
        "[array_consolidation_request_to_capnp] Error serializing "
        "consolidation request. A non-empty fragment list should only be "
        "provided for fragment consolidation.");
  }

  auto config_builder = array_consolidation_request_builder->initConfig();
  throw_if_not_ok(config_to_capnp(config, &config_builder));

  if (fragment_uris != nullptr && !fragment_uris->empty()) {
    auto fragment_list_builder =
        array_consolidation_request_builder->initFragments(
            fragment_uris->size());
    for (size_t i = 0; i < fragment_uris->size(); i++) {
      fragment_list_builder.set(i, (*fragment_uris)[i]);
    }
  }
}

std::pair<Config, std::optional<std::vector<std::string>>>
array_consolidation_request_from_capnp(
    const capnp::ArrayConsolidationRequest::Reader&
        array_consolidation_request_reader) {
  auto config_reader = array_consolidation_request_reader.getConfig();
  tdb_unique_ptr<Config> decoded_config = nullptr;
  throw_if_not_ok(config_from_capnp(config_reader, &decoded_config));

  std::vector<std::string> fragment_uris;
  if (array_consolidation_request_reader.hasFragments()) {
    auto fragment_reader = array_consolidation_request_reader.getFragments();
    fragment_uris.reserve(fragment_reader.size());
    for (const auto& fragment_uri : fragment_reader) {
      fragment_uris.emplace_back(fragment_uri);
    }

    return {*decoded_config, fragment_uris};
  } else {
    return {*decoded_config, std::nullopt};
  }
}

void array_consolidation_request_serialize(
    const Config& config,
    const std::vector<std::string>* fragment_uris,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::ArrayConsolidationRequest::Builder ArrayConsolidationRequestBuilder =
        message.initRoot<capnp::ArrayConsolidationRequest>();
    array_consolidation_request_to_capnp(
        config, fragment_uris, &ArrayConsolidationRequestBuilder);

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(ArrayConsolidationRequestBuilder);
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
        throw ConsolidationSerializationException(
            "[array_consolidation_request_serialize] Error serializing config; "
            "Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    throw ConsolidationSerializationException(
        "[array_consolidation_request_serialize] Error serializing config; "
        "kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw ConsolidationSerializationException(
        "[array_consolidation_request_serialize] Error serializing config; "
        "exception " +
        std::string(e.what()));
  }
}

std::pair<Config, std::optional<std::vector<std::string>>>
array_consolidation_request_deserialize(
    SerializationType serialize_type, const Buffer& serialized_buffer) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::ArrayConsolidationRequest::Builder
            array_consolidation_request_builder =
                message_builder.initRoot<capnp::ArrayConsolidationRequest>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            array_consolidation_request_builder);
        capnp::ArrayConsolidationRequest::Reader
            array_consolidation_request_reader =
                array_consolidation_request_builder.asReader();
        return array_consolidation_request_from_capnp(
            array_consolidation_request_reader);
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::ArrayConsolidationRequest::Reader
            array_consolidation_request_reader =
                reader.getRoot<capnp::ArrayConsolidationRequest>();
        return array_consolidation_request_from_capnp(
            array_consolidation_request_reader);
        break;
      }
      default: {
        throw ConsolidationSerializationException(
            "[array_consolidation_request_deserialize] Error deserializing "
            "config; Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw ConsolidationSerializationException(
        "[array_consolidation_request_deserialize] Error deserializing config; "
        "kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw ConsolidationSerializationException(
        "[array_consolidation_request_deserialize] Error deserializing config; "
        "exception " +
        std::string(e.what()));
  }
}

void consolidation_plan_request_to_capnp(
    capnp::ConsolidationPlanRequest::Builder& builder,
    const Config& config,
    uint64_t fragment_size) {
  auto config_builder = builder.initConfig();
  throw_if_not_ok(config_to_capnp(config, &config_builder));
  builder.setFragmentSize(fragment_size);
}

uint64_t consolidation_plan_request_from_capnp(
    const capnp::ConsolidationPlanRequest::Reader& reader) {
  return reader.getFragmentSize();
}

void consolidation_plan_response_to_capnp(
    capnp::ConsolidationPlanResponse::Builder& builder,
    const ConsolidationPlan& plan) {
  auto node_size = plan.get_num_nodes();
  if (node_size > 0) {
    auto node_builder = builder.initFragmentUrisPerNode(node_size);
    for (size_t i = 0; i < node_size; i++) {
      auto fragment_builder = node_builder.init(i, plan.get_num_fragments(i));
      for (size_t j = 0; j < plan.get_num_fragments(i); j++) {
        fragment_builder.set(j, plan.get_fragment_uri(i, j));
      }
    }
  }
}

std::vector<std::vector<std::string>> consolidation_plan_response_from_capnp(
    const capnp::ConsolidationPlanResponse::Reader& reader) {
  std::vector<std::vector<std::string>> fragment_uris_per_node;
  if (reader.hasFragmentUrisPerNode()) {
    auto node_reader = reader.getFragmentUrisPerNode();
    fragment_uris_per_node.reserve(node_reader.size());
    for (const auto& fragment_reader : node_reader) {
      auto& node = fragment_uris_per_node.emplace_back();
      node.reserve(fragment_reader.size());
      for (const auto& fragment_uri : fragment_reader) {
        node.emplace_back(fragment_uri);
      }
    }
  }

  return fragment_uris_per_node;
}

void serialize_consolidation_plan_request(
    uint64_t fragment_size,
    const Config& config,
    SerializationType serialization_type,
    Buffer& request) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::ConsolidationPlanRequest::Builder builder =
        message.initRoot<capnp::ConsolidationPlanRequest>();
    consolidation_plan_request_to_capnp(builder, config, fragment_size);

    request.reset_size();
    request.reset_offset();

    switch (serialization_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        throw_if_not_ok(request.realloc(json_len + 1));
        throw_if_not_ok(request.write(capnp_json.cStr(), json_len));
        throw_if_not_ok(request.write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        throw_if_not_ok(request.realloc(nbytes));
        throw_if_not_ok(request.write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        throw ConsolidationSerializationException(
            "Error serializing consolidation plan request; "
            "Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    throw ConsolidationSerializationException(
        "Error serializing consolidation plan request; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw ConsolidationSerializationException(
        "Error serializing consolidation plan request; exception " +
        std::string(e.what()));
  }
}

uint64_t deserialize_consolidation_plan_request(
    SerializationType serialization_type, const Buffer& request) {
  try {
    switch (serialization_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::ConsolidationPlanRequest::Builder builder =
            message_builder.initRoot<capnp::ConsolidationPlanRequest>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(request.data())), builder);
        capnp::ConsolidationPlanRequest::Reader reader = builder.asReader();
        return consolidation_plan_request_from_capnp(reader);
      }
      case SerializationType::CAPNP: {
        const auto mBytes = reinterpret_cast<const kj::byte*>(request.data());
        ::capnp::FlatArrayMessageReader array_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            request.size() / sizeof(::capnp::word)));
        capnp::ConsolidationPlanRequest::Reader reader =
            array_reader.getRoot<capnp::ConsolidationPlanRequest>();
        return consolidation_plan_request_from_capnp(reader);
      }
      default: {
        throw ConsolidationSerializationException(
            "Error deserializing consolidation plan request; "
            "Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw ConsolidationSerializationException(
        "Error deserializing consolidation plan request; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw ConsolidationSerializationException(
        "Error deserializing consolidation plan request; exception " +
        std::string(e.what()));
  }
}

void serialize_consolidation_plan_response(
    const ConsolidationPlan& plan,
    SerializationType serialization_type,
    Buffer& response) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::ConsolidationPlanResponse::Builder builder =
        message.initRoot<capnp::ConsolidationPlanResponse>();
    consolidation_plan_response_to_capnp(builder, plan);

    response.reset_size();
    response.reset_offset();

    switch (serialization_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        throw_if_not_ok(response.realloc(json_len + 1));
        throw_if_not_ok(response.write(capnp_json.cStr(), json_len));
        throw_if_not_ok(response.write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        throw_if_not_ok(response.realloc(nbytes));
        throw_if_not_ok(response.write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        throw ConsolidationSerializationException(
            "Error serializing consolidation plan response; "
            "Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    throw ConsolidationSerializationException(
        "Error serializing consolidation plan response; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw ConsolidationSerializationException(
        "Error serializing consolidation plan response; exception " +
        std::string(e.what()));
  }
}

std::vector<std::vector<std::string>> deserialize_consolidation_plan_response(
    SerializationType serialization_type, const Buffer& response) {
  try {
    switch (serialization_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::ConsolidationPlanResponse::Builder builder =
            message_builder.initRoot<capnp::ConsolidationPlanResponse>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(response.data())), builder);
        capnp::ConsolidationPlanResponse::Reader reader = builder.asReader();
        return consolidation_plan_response_from_capnp(reader);
      }
      case SerializationType::CAPNP: {
        const auto mBytes = reinterpret_cast<const kj::byte*>(response.data());
        ::capnp::FlatArrayMessageReader array_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            response.size() / sizeof(::capnp::word)));
        capnp::ConsolidationPlanResponse::Reader reader =
            array_reader.getRoot<capnp::ConsolidationPlanResponse>();
        return consolidation_plan_response_from_capnp(reader);
      }
      default: {
        throw ConsolidationSerializationException(
            "Error deserializing consolidation plan response; "
            "Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw ConsolidationSerializationException(
        "Error deserializing consolidation plan response; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw ConsolidationSerializationException(
        "Error deserializing consolidation plan response; exception " +
        std::string(e.what()));
  }
}

#else

void array_consolidation_request_serialize(
    const Config&,
    const std::vector<std::string>*,
    SerializationType,
    Buffer*) {
  throw ConsolidationSerializationDisabledException();
}

std::pair<Config, std::optional<std::vector<std::string>>>
array_consolidation_request_deserialize(SerializationType, const Buffer&) {
  throw ConsolidationSerializationDisabledException();
}

void serialize_consolidation_plan_request(
    uint64_t, const Config&, SerializationType, Buffer&) {
  throw ConsolidationSerializationDisabledException();
}

uint64_t deserialize_consolidation_plan_request(
    SerializationType, const Buffer&) {
  throw ConsolidationSerializationDisabledException();
}

void serialize_consolidation_plan_response(
    const ConsolidationPlan&, SerializationType, Buffer&) {
  throw ConsolidationSerializationDisabledException();
}

std::vector<std::vector<std::string>> deserialize_consolidation_plan_response(
    SerializationType, const Buffer&) {
  throw ConsolidationSerializationDisabledException();
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
