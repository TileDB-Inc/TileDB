/**
 * @file rest_capabilities.cc
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
#endif
// clang-format on

#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/rest/rest_client_remote.h"
#include "tiledb/sm/serialization/rest_capabilities.h"
#include "tiledb/sm/storage_manager/context.h"

using namespace tiledb::common;

namespace tiledb::sm::serialization {

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

RestCapabilities rest_version_deserialize(
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

RestCapabilities rest_version_from_capnp(
    const capnp::RestVersion::Reader& rest_version_reader) {
  RestClient::TileDBVersion rest_version{}, rest_minimum_version{};
  if (rest_version_reader.hasDeployedTileDBVersion()) {
    auto version_reader = rest_version_reader.getDeployedTileDBVersion();
    rest_version.major_ = version_reader.getMajor();
    rest_version.minor_ = version_reader.getMinor();
    rest_version.patch_ = version_reader.getPatch();
  } else {
    throw RestVersionSerializationException(
        "Failed to deserialize REST capabilities with no deployed TileDB "
        "version.");
  }

  if (rest_version_reader.hasMinimumSupportedTileDBVersion()) {
    auto version_reader =
        rest_version_reader.getMinimumSupportedTileDBVersion();
    rest_minimum_version.major_ = version_reader.getMajor();
    rest_minimum_version.minor_ = version_reader.getMinor();
    rest_minimum_version.patch_ = version_reader.getPatch();
  } else {
    throw RestVersionSerializationException(
        "Failed to deserialize REST capabilities with no minimum supported "
        "TileDB version.");
  }

  return {rest_version, rest_minimum_version};
}

#else

RestCapabilities rest_version_deserialize(SerializationType, span<const char>) {
  throw RestVersionSerializationDisabledException();
}

RestCapabilities rest_version_from_capnp(const capnp::RestVersion::Reader&) {
  throw RestVersionSerializationDisabledException();
}

#endif  // TILEDB_SERIALIZATION

}  // namespace tiledb::sm::serialization
