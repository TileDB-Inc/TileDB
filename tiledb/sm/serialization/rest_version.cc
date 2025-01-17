/**
 * @file rest_version.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file defines serialization for REST version information.
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
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/serialization/rest_version.h"
#include "tiledb/sm/storage_manager/context.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace serialization {

class RestVersionSerializationException : public StatusException {
 public:
  explicit RestVersionSerializationException(const std::string& message)
      : StatusException("[TileDB::Serialization][RestVersion]", message) {
  }
};

class RestVersionSerializationDisabledException
    : public RestVersionSerializationException {
 public:
  explicit RestVersionSerializationDisabledException()
      : RestVersionSerializationException(
            "Cannot (de)serialize; serialization not enabled.") {
  }
};

#ifdef TILEDB_SERIALIZATION

Status rest_version_to_capnp(
    Context* ctx, capnp::RestVersion::Builder* rest_version_builder) {
  if (ctx->has_rest_client()) {
    // The REST version is initialized in the RestClientRemote constructor.
    rest_version_builder->setTiledbVersion(ctx->rest_client().rest_version());
  } else {
    // We should not hit this case, if TILEDB_SERIALIZATION is enabled there
    // will be a rest client attached to ContextResources.
    throw RestVersionSerializationException(
        "Cannot serialize REST version with no initialized RESTClient.");
  }

  return Status::Ok();
}

std::string rest_version_from_capnp(
    const capnp::RestVersion::Reader& rest_version_reader) {
  if (rest_version_reader.hasTiledbVersion()) {
    return rest_version_reader.getTiledbVersion();
  }
  // TODO: What is best to return here?
  return "2.X.0";
}

Status rest_version_serialize(
    Context* ctx,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::RestVersion::Builder RestVersionBuilder =
        message.initRoot<capnp::RestVersion>();
    RETURN_NOT_OK(rest_version_to_capnp(ctx, &RestVersionBuilder));

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(RestVersionBuilder);
        serialized_buffer.assign(capnp_json);
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        serialized_buffer.assign(protomessage.asChars());
        break;
      }
      default: {
        throw RestVersionSerializationException(
            "Error serializing REST version; Unknown serialization type "
            "passed");
      }
    }

  } catch (kj::Exception& e) {
    throw RestVersionSerializationException(
        "Error serializing REST version; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw RestVersionSerializationException(
        "Error serializing REST version; exception " + std::string(e.what()));
  }

  return Status::Ok();
}

std::string rest_version_deserialize(
    SerializationType serialization_type,
    span<const char> serialized_response) {
  try {
    switch (serialization_type) {
      case SerializationType::JSON: {
        ::capnp::MallocMessageBuilder message_builder;
        capnp::RestVersion::Builder rest_version_builder =
            message_builder.initRoot<capnp::RestVersion>();
        utils::decode_json_message(serialized_response, rest_version_builder);
        capnp::RestVersion::Reader rest_version_reader =
            rest_version_builder.asReader();
        return rest_version_from_capnp(rest_version_reader);
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_response.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_response.size() / sizeof(::capnp::word)));
        capnp::RestVersion::Reader rest_version_reader =
            reader.getRoot<capnp::RestVersion>();
        return rest_version_from_capnp(rest_version_reader);
      }
      default: {
        throw RestVersionSerializationException(
            "Error deserializing REST version; Unknown serialization type "
            "passed");
      }
    }

  } catch (kj::Exception& e) {
    throw RestVersionSerializationException(
        "Error deserializing REST version; kj::Exception: " +
        std::string(e.getDescription().cStr()));
  } catch (std::exception& e) {
    throw RestVersionSerializationException(
        "Error deserializing REST version; exception " + std::string(e.what()));
  }
}

#else

Status rest_version_to_capnp(
    Context* ctx, capnp::RestVersion::Builder* rest_version_builder) {
  throw RestVersionSerializationDisabledException();
}

void rest_version_from_capnp(
    const capnp::RestVersion::Reader& rest_version_reader) {
  throw RestVersionSerializationDisabledException();
}

Status rest_version_serialize(
    SerializationType serialize_type, SerializationBuffer& serialized_buffer) {
  throw RestVersionSerializationDisabledException();
}

std::string rest_version_deserialize(
    SerializationType serialization_type, span<const char> response) {
  throw RestVersionSerializationDisabledException();
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
