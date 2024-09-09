/**
 * @file vacuum.cc
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
 * This file defines serialization for the Vacuum class
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

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

Status array_vacuum_request_to_capnp(
    const Config& config,
    capnp::ArrayVacuumRequest::Builder* array_vacuum_request_builder) {
  auto config_builder = array_vacuum_request_builder->initConfig();
  RETURN_NOT_OK(config_to_capnp(config, &config_builder));
  return Status::Ok();
}

Status array_vacuum_request_from_capnp(
    const capnp::ArrayVacuumRequest::Reader& array_vacuum_request_reader,
    tdb_unique_ptr<Config>* config) {
  auto config_reader = array_vacuum_request_reader.getConfig();
  RETURN_NOT_OK(config_from_capnp(config_reader, config));
  return Status::Ok();
}

Status array_vacuum_request_serialize(
    const Config& config,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::ArrayVacuumRequest::Builder ArrayVacuumRequestBuilder =
        message.initRoot<capnp::ArrayVacuumRequest>();
    RETURN_NOT_OK(
        array_vacuum_request_to_capnp(config, &ArrayVacuumRequestBuilder));

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(ArrayVacuumRequestBuilder);
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

Status array_vacuum_request_deserialize(
    Config** config,
    SerializationType serialize_type,
    span<const char> serialized_buffer) {
  try {
    tdb_unique_ptr<Config> decoded_config = nullptr;

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::ArrayVacuumRequest::Builder array_vacuum_request_builder =
            message_builder.initRoot<capnp::ArrayVacuumRequest>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            array_vacuum_request_builder);
        capnp::ArrayVacuumRequest::Reader array_vacuum_request_reader =
            array_vacuum_request_builder.asReader();
        RETURN_NOT_OK(array_vacuum_request_from_capnp(
            array_vacuum_request_reader, &decoded_config));
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::ArrayVacuumRequest::Reader array_vacuum_request_reader =
            reader.getRoot<capnp::ArrayVacuumRequest>();
        RETURN_NOT_OK(array_vacuum_request_from_capnp(
            array_vacuum_request_reader, &decoded_config));
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

#else

Status array_vacuum_request_serialize(
    const Config&, SerializationType, SerializationBuffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status array_vacuum_request_deserialize(
    Config**, SerializationType, span<const char>) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
