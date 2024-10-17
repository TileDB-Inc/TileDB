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

Status array_consolidation_request_to_capnp(
    const Config& config,
    capnp::ArrayConsolidationRequest::Builder*
        array_consolidation_request_builder) {
  auto config_builder = array_consolidation_request_builder->initConfig();
  RETURN_NOT_OK(config_to_capnp(config, &config_builder));
  return Status::Ok();
}

Status array_consolidation_request_from_capnp(
    const capnp::ArrayConsolidationRequest::Reader&
        array_consolidation_request_reader,
    tdb_unique_ptr<Config>* config) {
  auto config_reader = array_consolidation_request_reader.getConfig();
  RETURN_NOT_OK(config_from_capnp(config_reader, config));
  return Status::Ok();
}

Status array_consolidation_request_serialize(
    const Config& config,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::ArrayConsolidationRequest::Builder ArrayConsolidationRequestBuilder =
        message.initRoot<capnp::ArrayConsolidationRequest>();
    RETURN_NOT_OK(array_consolidation_request_to_capnp(
        config, &ArrayConsolidationRequestBuilder));

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(ArrayConsolidationRequestBuilder);
        serialized_buffer.assign_null_terminated(capnp_json);
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        serialized_buffer.assign(protomessage.asChars());
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error serializing config; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing config; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing config; exception " + std::string(e.what())));
  }

  return Status::Ok();
}

Status array_consolidation_request_deserialize(
    Config** config,
    SerializationType serialize_type,
    span<const char> serialized_buffer) {
  try {
    tdb_unique_ptr<Config> decoded_config = nullptr;

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::MallocMessageBuilder message_builder;
        capnp::ArrayConsolidationRequest::Builder
            array_consolidation_request_builder =
                message_builder.initRoot<capnp::ArrayConsolidationRequest>();
        utils::decode_json_message(
            serialized_buffer, array_consolidation_request_builder);
        capnp::ArrayConsolidationRequest::Reader
            array_consolidation_request_reader =
                array_consolidation_request_builder.asReader();
        RETURN_NOT_OK(array_consolidation_request_from_capnp(
            array_consolidation_request_reader, &decoded_config));
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
        RETURN_NOT_OK(array_consolidation_request_from_capnp(
            array_consolidation_request_reader, &decoded_config));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing config; Unknown serialization type "
            "passed"));
      }
    }

    if (decoded_config == nullptr)
      return LOG_STATUS(Status_SerializationError(
          "Error serializing config; deserialized config is null"));

    *config = decoded_config.release();
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing config; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing config; exception " + std::string(e.what())));
  }

  return Status::Ok();
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
    SerializationBuffer& request) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::ConsolidationPlanRequest::Builder builder =
        message.initRoot<capnp::ConsolidationPlanRequest>();
    consolidation_plan_request_to_capnp(builder, config, fragment_size);

    switch (serialization_type) {
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
    SerializationType serialization_type, span<const char> request) {
  try {
    switch (serialization_type) {
      case SerializationType::JSON: {
        ::capnp::MallocMessageBuilder message_builder;
        capnp::ConsolidationPlanRequest::Builder builder =
            message_builder.initRoot<capnp::ConsolidationPlanRequest>();
        utils::decode_json_message(request, builder);
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
    SerializationBuffer& response) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::ConsolidationPlanResponse::Builder builder =
        message.initRoot<capnp::ConsolidationPlanResponse>();
    consolidation_plan_response_to_capnp(builder, plan);

    switch (serialization_type) {
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
    SerializationType serialization_type, span<const char> response) {
  try {
    switch (serialization_type) {
      case SerializationType::JSON: {
        ::capnp::MallocMessageBuilder message_builder;
        capnp::ConsolidationPlanResponse::Builder builder =
            message_builder.initRoot<capnp::ConsolidationPlanResponse>();
        utils::decode_json_message(response, builder);
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

Status array_consolidation_request_serialize(
    const Config&, SerializationType, SerializationBuffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status array_consolidation_request_deserialize(
    Config**, SerializationType, span<const char>) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

void serialize_consolidation_plan_request(
    uint64_t, const Config&, SerializationType, SerializationBuffer&) {
  throw ConsolidationSerializationDisabledException();
}

uint64_t deserialize_consolidation_plan_request(
    SerializationType, span<const char>) {
  throw ConsolidationSerializationDisabledException();
}

void serialize_consolidation_plan_response(
    const ConsolidationPlan&, SerializationType, SerializationBuffer&) {
  throw ConsolidationSerializationDisabledException();
}

std::vector<std::vector<std::string>> deserialize_consolidation_plan_response(
    SerializationType, span<const char>) {
  throw ConsolidationSerializationDisabledException();
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
