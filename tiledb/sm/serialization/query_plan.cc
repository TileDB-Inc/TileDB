/**
 * @file query_plan.cc
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
 * This file implements serialization for a query plan
 */

// clang-format off
#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif
// clang-format on

#include "tiledb/sm/query_plan/query_plan.h"

#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/serialization/query.h"
#include "tiledb/sm/serialization/query_plan.h"

using namespace tiledb::common;

namespace tiledb::sm::serialization {

class QueryPlanSerializationException : public StatusException {
 public:
  explicit QueryPlanSerializationException(const std::string& message)
      : StatusException("[TileDB::Serialization][QueryPlan]", message) {
  }
};

class QueryPlanSerializationDisabledException
    : public QueryPlanSerializationException {
 public:
  explicit QueryPlanSerializationDisabledException()
      : QueryPlanSerializationException(
            "Cannot (de)serialize; serialization not enabled.") {
  }
};

#ifdef TILEDB_SERIALIZATION

void query_plan_request_to_capnp(
    capnp::QueryPlanRequest::Builder& builder,
    const Config& config,
    Query& query) {
  auto config_builder = builder.initConfig();
  throw_if_not_ok(config_to_capnp(config, &config_builder));

  auto query_builder = builder.initQuery();
  throw_if_not_ok(query_to_capnp(query, &query_builder, true));
}

void query_plan_request_from_capnp(
    const capnp::QueryPlanRequest::Reader& reader,
    ThreadPool& compute_tp,
    Query& query) {
  if (reader.hasQuery()) {
    auto query_reader = reader.getQuery();
    throw_if_not_ok(query_from_capnp(
        query_reader,
        SerializationContext::SERVER,
        nullptr,
        nullptr,
        &query,
        &compute_tp,
        false));
  }
}

void query_plan_response_to_capnp(
    capnp::QueryPlanResponse::Builder& builder, const QueryPlan& query_plan) {
  builder.setQueryLayout(layout_str(query_plan.query_layout()));
  builder.setStrategyName(query_plan.strategy());
  builder.setArrayType(array_type_str(query_plan.array_type()));
  const auto& attributes = query_plan.attributes();
  if (!attributes.empty()) {
    auto attributes_builder = builder.initAttributeNames(attributes.size());
    for (size_t i = 0; i < attributes.size(); i++) {
      attributes_builder.set(i, attributes[i]);
    }
  }

  const auto& dimensions = query_plan.dimensions();
  if (!dimensions.empty()) {
    auto dimensions_builder = builder.initDimensionNames(dimensions.size());
    for (size_t i = 0; i < dimensions.size(); i++) {
      dimensions_builder.set(i, dimensions[i]);
    }
  }
}

QueryPlan query_plan_response_from_capnp(
    const capnp::QueryPlanResponse::Reader& reader, Query& query) {
  auto layout{Layout::ROW_MAJOR};
  if (reader.hasQueryLayout()) {
    throw_if_not_ok(layout_enum(reader.getQueryLayout().cStr(), &layout));
  }

  std::string strategy{};
  if (reader.hasStrategyName()) {
    strategy = reader.getStrategyName().cStr();
  }

  auto array_type{ArrayType::DENSE};
  if (reader.hasQueryLayout()) {
    throw_if_not_ok(array_type_enum(reader.getArrayType().cStr(), &array_type));
  }

  std::vector<std::string> attributes;
  if (reader.hasAttributeNames()) {
    auto attribute_names = reader.getAttributeNames();
    attributes.reserve(attribute_names.size());
    for (auto attr : attribute_names) {
      attributes.emplace_back(attr);
    }
  }

  std::vector<std::string> dimensions;
  if (reader.hasDimensionNames()) {
    auto dimension_names = reader.getDimensionNames();
    dimensions.reserve(dimension_names.size());
    for (auto dim : dimension_names) {
      dimensions.emplace_back(dim);
    }
  }

  return QueryPlan(query, layout, strategy, array_type, attributes, dimensions);
}

void serialize_query_plan_request(
    const Config& config,
    Query& query,
    SerializationType serialization_type,
    SerializationBuffer& request) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::QueryPlanRequest::Builder builder =
        message.initRoot<capnp::QueryPlanRequest>();
    query_plan_request_to_capnp(builder, config, query);

    switch (serialization_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        request.assign(capnp_json);
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        request.assign(protomessage.asChars());
        break;
      }
      default: {
        throw QueryPlanSerializationException(
            "Error serializing query plan request; "
            "Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    throw QueryPlanSerializationException(
        "Error serializing query plan request; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw QueryPlanSerializationException(
        "Error serializing query plan request; exception " +
        std::string(e.what()));
  }
}

void deserialize_query_plan_request(
    const SerializationType serialization_type,
    span<const char> request,
    ThreadPool& compute_tp,
    Query& query) {
  try {
    switch (serialization_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        json.handleByAnnotation<capnp::QueryPlanRequest>();
        ::capnp::MallocMessageBuilder message_builder;
        capnp::QueryPlanRequest::Builder builder =
            message_builder.initRoot<capnp::QueryPlanRequest>();
        utils::decode_json_message(request, builder, json);
        capnp::QueryPlanRequest::Reader reader = builder.asReader();
        query_plan_request_from_capnp(reader, compute_tp, query);
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes = reinterpret_cast<const kj::byte*>(request.data());
        ::capnp::FlatArrayMessageReader array_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            request.size() / sizeof(::capnp::word)));
        capnp::QueryPlanRequest::Reader reader =
            array_reader.getRoot<capnp::QueryPlanRequest>();
        query_plan_request_from_capnp(reader, compute_tp, query);
        break;
      }
      default: {
        throw QueryPlanSerializationException(
            "Error deserializing query plan request; "
            "Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw QueryPlanSerializationException(
        "Error deserializing query plan request; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw QueryPlanSerializationException(
        "Error deserializing query plan request; exception " +
        std::string(e.what()));
  }
}

void serialize_query_plan_response(
    const QueryPlan& query_plan,
    SerializationType serialization_type,
    SerializationBuffer& response) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::QueryPlanResponse::Builder builder =
        message.initRoot<capnp::QueryPlanResponse>();
    query_plan_response_to_capnp(builder, query_plan);

    switch (serialization_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        response.assign(capnp_json);
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        response.assign(protomessage.asChars());
        break;
      }
      default: {
        throw QueryPlanSerializationException(
            "Error serializing query plan response; "
            "Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    throw QueryPlanSerializationException(
        "Error serializing query plan response; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw QueryPlanSerializationException(
        "Error serializing query plan response; exception " +
        std::string(e.what()));
  }
}

QueryPlan deserialize_query_plan_response(
    Query& query,
    SerializationType serialization_type,
    span<const char> response) {
  try {
    switch (serialization_type) {
      case SerializationType::JSON: {
        ::capnp::MallocMessageBuilder message_builder;
        capnp::QueryPlanResponse::Builder builder =
            message_builder.initRoot<capnp::QueryPlanResponse>();
        utils::decode_json_message(response, builder);
        capnp::QueryPlanResponse::Reader reader = builder.asReader();
        return query_plan_response_from_capnp(reader, query);
      }
      case SerializationType::CAPNP: {
        const auto mBytes = reinterpret_cast<const kj::byte*>(response.data());
        ::capnp::FlatArrayMessageReader array_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            response.size() / sizeof(::capnp::word)));
        capnp::QueryPlanResponse::Reader reader =
            array_reader.getRoot<capnp::QueryPlanResponse>();
        return query_plan_response_from_capnp(reader, query);
      }
      default: {
        throw QueryPlanSerializationException(
            "Error deserializing query plan response; "
            "Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw QueryPlanSerializationException(
        "Error deserializing query plan response; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw QueryPlanSerializationException(
        "Error deserializing query plan response; exception " +
        std::string(e.what()));
  }
}

#else

void serialize_query_plan_request(
    const Config&, Query&, const SerializationType, SerializationBuffer&) {
  throw QueryPlanSerializationDisabledException();
}

void deserialize_query_plan_request(
    const SerializationType, span<const char>, ThreadPool&, Query&) {
  throw QueryPlanSerializationDisabledException();
}

void serialize_query_plan_response(
    const QueryPlan&, const SerializationType, SerializationBuffer&) {
  throw QueryPlanSerializationDisabledException();
}

QueryPlan deserialize_query_plan_response(
    Query&, const SerializationType, span<const char>) {
  throw QueryPlanSerializationDisabledException();
}

#endif  // TILEDB_SERIALIZATION

}  // namespace tiledb::sm::serialization
