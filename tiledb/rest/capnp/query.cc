/**
 * @file   query.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file defines serialization for
 * tiledb::sm::Query
 */

#include "tiledb/rest/capnp/query.h"
#include "capnp/compat/json.h"
#include "capnp/message.h"
#include "capnp/serialize.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"

namespace tiledb {
namespace rest {

tiledb::sm::Status query_serialize(
    tiledb::sm::Query* query,
    tiledb::sm::SerializationType serialize_type,
    char** serialized_string,
    uint64_t* serialized_string_length) {
  STATS_FUNC_IN(serialization_query_serialize);
  try {
    capnp::MallocMessageBuilder message;
    Query::Builder query_builder = message.initRoot<Query>();
    tiledb::sm::Status status = query->capnp(&query_builder);

    if (!status.ok())
      return tiledb::sm::Status::Error(
          "Could not serialize query: " + status.to_string());

    switch (serialize_type) {
      case tiledb::sm::SerializationType::JSON: {
        capnp::JsonCodec json;
        kj::String capnp_json = json.encode(query_builder);
        // size does not include needed null terminator, so add +1
        *serialized_string_length = capnp_json.size() + 1;
        *serialized_string = new char[*serialized_string_length];
        strcpy(*serialized_string, capnp_json.cStr());
        break;
      }
      case tiledb::sm::SerializationType::CAPNP: {
        kj::Array<capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        *serialized_string = new char[message_chars.size()];
        memcpy(*serialized_string, message_chars.begin(), message_chars.size());
        *serialized_string_length = message_chars.size();
        break;
      }
      default: {
        return tiledb::sm::Status::Error("Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    return tiledb::sm::Status::Error(
        std::string("Error serializing query: ") + e.getDescription().cStr());
  } catch (std::exception& e) {
    return tiledb::sm::Status::Error(
        std::string("Error serializing query: ") + e.what());
  }
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_query_serialize);
}

tiledb::sm::Status query_deserialize(
    tiledb::sm::Query* query,
    tiledb::sm::SerializationType serialize_type,
    const char* serialized_string,
    const uint64_t serialized_string_length) {
  STATS_FUNC_IN(serialization_query_deserialize);
  try {
    switch (serialize_type) {
      case tiledb::sm::SerializationType::JSON: {
        capnp::JsonCodec json;
        capnp::MallocMessageBuilder message_builder;
        ::Query::Builder query_builder = message_builder.initRoot<::Query>();
        json.decode(kj::StringPtr(serialized_string), query_builder);
        ::Query::Reader query_reader = query_builder.asReader();
        return query->from_capnp(&query_reader);
        break;
      }
      case tiledb::sm::SerializationType::CAPNP: {
        capnp::ReaderOptions readerOptions;
        // Set limit to 10GI this should be a config option
        readerOptions.traversalLimitInWords = uint64_t(1024) * 1024 * 1024 * 10;
        const kj::byte* mBytes =
            reinterpret_cast<const kj::byte*>(serialized_string);
        capnp::FlatArrayMessageReader reader(
            kj::arrayPtr(
                reinterpret_cast<const capnp::word*>(mBytes),
                serialized_string_length / sizeof(capnp::word)),
            readerOptions);

        Query::Reader query_reader = reader.getRoot<::Query>();

        return query->from_capnp(&query_reader);
        break;
      }
      default: {
        return tiledb::sm::Status::Error("Unknown serialization type passed");
      }
    }
  } catch (kj::Exception& e) {
    return tiledb::sm::Status::Error(
        std::string("Error deserializing query: ") + e.getDescription().cStr());
  } catch (std::exception& e) {
    return tiledb::sm::Status::Error(
        std::string("Error deserializing query: ") + e.what());
  }
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_query_deserialize);
}
}  // namespace rest
}  // namespace tiledb
