/**
 * @file   config.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file defines serialization functions for Config.
 */

#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/serialization/config.h"

#include <set>

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

Status config_to_capnp(
    const Config& config, capnp::Config::Builder* config_builder) {
  auto config_params = config.get_all_params_from_config_or_env();
  auto entries = config_builder->initEntries(config_params.size());
  uint64_t i = 0;
  for (const auto& kv : config_params) {
    entries[i].setKey(kv.first);
    entries[i].setValue(kv.second);
    ++i;
  }
  return Status::Ok();
}

Status config_from_capnp(
    const capnp::Config::Reader& config_reader,
    tdb_unique_ptr<Config>* config) {
  config->reset(tdb_new(Config));

  if (config_reader.hasEntries()) {
    auto entries = config_reader.getEntries();
    bool found_refactored_reader_config = false;
    for (const auto kv : entries) {
      auto key = std::string_view{kv.getKey().cStr(), kv.getKey().size()};
      auto value = std::string_view{kv.getValue().cStr(), kv.getValue().size()};
      RETURN_NOT_OK((*config)->set(std::string{key}, std::string{value}));
      if (key == "sm.query.dense.reader" ||
          key == "sm.query.sparse_global_order.reader" ||
          key == "sm.query.sparse_unordered_with_dups.reader")
        found_refactored_reader_config = true;
    }

    // If we are on a pre TileDB 2.4 client, they will not have the reader
    // options set. For those cases we need to set the reader to the legacy
    // option
    if (!found_refactored_reader_config) {
      RETURN_NOT_OK((*config)->set("sm.query.dense.reader", "legacy"));
      RETURN_NOT_OK(
          (*config)->set("sm.query.sparse_global_order.reader", "legacy"));
      RETURN_NOT_OK((*config)->set(
          "sm.query.sparse_unordered_with_dups.reader", "legacy"));
    }
  }
  return Status::Ok();
}

Status config_serialize(
    const Config& config,
    SerializationType serialize_type,
    Buffer* serialized_buffer,
    const bool) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::Config::Builder configBuilder = message.initRoot<capnp::Config>();
    RETURN_NOT_OK(config_to_capnp(config, &configBuilder));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(configBuilder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(serialized_buffer->realloc(json_len + 1));
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len));
        RETURN_NOT_OK(serialized_buffer->write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        RETURN_NOT_OK(serialized_buffer->realloc(nbytes));
        RETURN_NOT_OK(serialized_buffer->write(message_chars.begin(), nbytes));
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

Status config_deserialize(
    Config** config,
    SerializationType serialize_type,
    const Buffer& serialized_buffer) {
  try {
    tdb_unique_ptr<Config> decoded_config = nullptr;

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::Config::Builder config_builder =
            message_builder.initRoot<capnp::Config>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            config_builder);
        capnp::Config::Reader config_reader = config_builder.asReader();
        RETURN_NOT_OK(config_from_capnp(config_reader, &decoded_config));
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::Config::Reader config_reader = reader.getRoot<capnp::Config>();
        RETURN_NOT_OK(config_from_capnp(config_reader, &decoded_config));
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

Status config_serialize(const Config&, SerializationType, Buffer*, const bool) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status config_deserialize(Config**, SerializationType, const Buffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
