/**
 * @file query_plan.cc
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
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/serialization/query.h"
#include "tiledb/sm/serialization/query_plan.h"

using namespace tiledb::common;

namespace tiledb::sm::serialization {

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
        &compute_tp));
  }
}

void query_plan_response_to_capnp(
    capnp::QueryPlanResponse::Builder& builder, const std::string& query_plan) {
  builder.setQueryPlan(query_plan);
}

std::string query_plan_response_from_capnp(
    const capnp::QueryPlanResponse::Reader& reader) {
  std::string query_plan;
  if (reader.hasQueryPlan()) {
    query_plan = reader.getQueryPlan().cStr();
  }

  return query_plan;
}

void serialize_query_plan_request(
    const Config& config,
    Query& query,
    SerializationType serialization_type,
    Buffer& request) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::QueryPlanRequest::Builder builder =
        message.initRoot<capnp::QueryPlanRequest>();
    query_plan_request_to_capnp(builder, config, query);

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
        throw Status_SerializationError(
            "Error serializing query plan request; "
            "Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    throw Status_SerializationError(
        "Error serializing query plan request; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw Status_SerializationError(
        "Error serializing query plan request; exception " +
        std::string(e.what()));
  }
}

void deserialize_query_plan_request(
    const SerializationType serialization_type,
    const Buffer& request,
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
        json.decode(
            kj::StringPtr(static_cast<const char*>(request.data())), builder);
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
        throw Status_SerializationError(
            "Error deserializing query plan request; "
            "Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw Status_SerializationError(
        "Error deserializing query plan request; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw Status_SerializationError(
        "Error deserializing query plan request; exception " +
        std::string(e.what()));
  }
}

void serialize_query_plan_response(
    const std::string& query_plan,
    SerializationType serialization_type,
    Buffer& response) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::QueryPlanResponse::Builder builder =
        message.initRoot<capnp::QueryPlanResponse>();
    query_plan_response_to_capnp(builder, query_plan);

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
        throw Status_SerializationError(
            "Error serializing query plan response; "
            "Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    throw Status_SerializationError(
        "Error serializing query plan response; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw Status_SerializationError(
        "Error serializing query plan response; exception " +
        std::string(e.what()));
  }
}

std::string deserialize_query_plan_response(
    SerializationType serialization_type, const Buffer& response) {
  try {
    switch (serialization_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::QueryPlanResponse::Builder builder =
            message_builder.initRoot<capnp::QueryPlanResponse>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(response.data())), builder);
        capnp::QueryPlanResponse::Reader reader = builder.asReader();
        return query_plan_response_from_capnp(reader);
      }
      case SerializationType::CAPNP: {
        const auto mBytes = reinterpret_cast<const kj::byte*>(response.data());
        ::capnp::FlatArrayMessageReader array_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            response.size() / sizeof(::capnp::word)));
        capnp::QueryPlanResponse::Reader reader =
            array_reader.getRoot<capnp::QueryPlanResponse>();
        return query_plan_response_from_capnp(reader);
      }
      default: {
        throw Status_SerializationError(
            "Error deserializing query plan response; "
            "Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    throw Status_SerializationError(
        "Error deserializing query plan response; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw Status_SerializationError(
        "Error deserializing query plan response; exception " +
        std::string(e.what()));
  }
}

#else

void serialize_query_plan_request(
    const Config&, Query&, const SerializationType, Buffer&) {
  throw Status_SerializationError(
      "Cannot serialize; serialization not enabled.");
}

void deserialize_query_plan_request(
    const SerializationType, const Buffer&, ThreadPool&, Query&) {
  throw Status_SerializationError(
      "Cannot serialize; serialization not enabled.");
}

void serialize_query_plan_response(
    const std::string&, const SerializationType, Buffer&) {
  throw Status_SerializationError(
      "Cannot serialize; serialization not enabled.");
}

std::string deserialize_query_plan_response(
    const SerializationType, const Buffer&) {
  throw Status_SerializationError(
      "Cannot serialize; serialization not enabled.");
}

#endif  // TILEDB_SERIALIZATION

}  // namespace tiledb::sm::serialization
