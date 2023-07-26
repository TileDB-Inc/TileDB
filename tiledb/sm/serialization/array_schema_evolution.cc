/**
 * @file   array_schema_evolution.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file defines serialization functions for ArraySchemaEvolution.
 */
#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema_evolution.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/serialization/array_schema.h"

#include <set>

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

Status array_schema_evolution_to_capnp(
    const ArraySchemaEvolution* array_schema_evolution,
    capnp::ArraySchemaEvolution::Builder* array_schema_evolution_builder,
    const bool client_side) {
  if (array_schema_evolution == nullptr)
    return LOG_STATUS(
        Status_SerializationError("Error serializing array schema evolution; "
                                  "array schema evolution is null."));

  // Attributes to drop
  std::vector<std::string> attr_names_to_drop =
      array_schema_evolution->attribute_names_to_drop();

  auto attributes_to_drop_builder =
      array_schema_evolution_builder->initAttributesToDrop(
          attr_names_to_drop.size());

  for (size_t i = 0; i < attr_names_to_drop.size(); i++) {
    attributes_to_drop_builder.set(i, attr_names_to_drop[i]);
  }

  // Attributes to add
  std::vector<std::string> attr_names_to_add =
      array_schema_evolution->attribute_names_to_add();

  auto attributes_to_add_buidler =
      array_schema_evolution_builder->initAttributesToAdd(
          attr_names_to_add.size());

  for (size_t i = 0; i < attr_names_to_add.size(); i++) {
    auto attribute_builder = attributes_to_add_buidler[i];
    auto attr_name = attr_names_to_add[i];
    const Attribute* attr_to_add =
        array_schema_evolution->attribute_to_add(attr_name);
    if (attr_to_add == nullptr) {
      continue;
    }
    attribute_to_capnp(attr_to_add, &attribute_builder);
  }

  auto enmr_names_to_add = array_schema_evolution->enumeration_names_to_add();
  auto num_enmrs = enmr_names_to_add.size();

  if (num_enmrs > 0) {
    auto enmrs_to_add_builder =
        array_schema_evolution_builder->initEnumerationsToAdd(num_enmrs);
    for (size_t i = 0; i < num_enmrs; i++) {
      auto enmr =
          array_schema_evolution->enumeration_to_add(enmr_names_to_add[i]);
      auto builder = enmrs_to_add_builder[i];
      enumeration_to_capnp(enmr, builder);
    }
  }

  // Enumerations to drop
  std::vector<std::string> enmr_names_to_drop =
      array_schema_evolution->enumeration_names_to_drop();

  auto enumerations_to_drop_builder =
      array_schema_evolution_builder->initEnumerationsToDrop(
          enmr_names_to_drop.size());

  for (size_t i = 0; i < enmr_names_to_drop.size(); i++) {
    enumerations_to_drop_builder.set(i, enmr_names_to_drop[i]);
  }

  auto timestamp_builder =
      array_schema_evolution_builder->initTimestampRange(2);
  const auto& timestamp_range = array_schema_evolution->timestamp_range();
  timestamp_builder.set(0, timestamp_range.first);
  timestamp_builder.set(1, timestamp_range.second);

  return Status::Ok();
}

tdb_unique_ptr<ArraySchemaEvolution> array_schema_evolution_from_capnp(
    const capnp::ArraySchemaEvolution::Reader& evolution_reader) {
  // Create attributes to add
  std::unordered_map<std::string, shared_ptr<Attribute>> attrs_to_add;
  auto attrs_to_add_reader = evolution_reader.getAttributesToAdd();
  for (auto attr_reader : attrs_to_add_reader) {
    auto attr = attribute_from_capnp(attr_reader);
    attrs_to_add[attr->name()] = attr;
  }

  // Create attributes to drop
  std::unordered_set<std::string> attrs_to_drop;
  auto attributes_to_drop_reader = evolution_reader.getAttributesToDrop();
  for (auto attr_reader : attributes_to_drop_reader) {
    std::string attr_name = std::string(attr_reader.cStr());
    attrs_to_drop.insert(attr_name);
  }

  // Create enumerations to add
  std::unordered_map<std::string, shared_ptr<const Enumeration>> enmrs_to_add;
  auto enmrs_to_add_reader = evolution_reader.getEnumerationsToAdd();
  for (auto enmr_reader : enmrs_to_add_reader) {
    auto enmr = enumeration_from_capnp(enmr_reader);
    enmrs_to_add[enmr->name()] = enmr;
  }

  // Create enumerations to drop
  std::unordered_set<std::string> enmrs_to_drop;
  auto enmrs_to_drop_reader = evolution_reader.getEnumerationsToDrop();
  for (auto enmr_reader : enmrs_to_drop_reader) {
    std::string enmr_name = std::string(enmr_reader.cStr());
    enmrs_to_drop.insert(enmr_name);
  }

  std::pair<uint64_t, uint64_t> ts_range;
  // Set the range if we have two values
  if (evolution_reader.hasTimestampRange() &&
      evolution_reader.getTimestampRange().size() >= 2) {
    const auto& timestamp_range = evolution_reader.getTimestampRange();
    ts_range = std::make_pair(timestamp_range[0], timestamp_range[1]);
  }

  return tdb_unique_ptr<ArraySchemaEvolution>(tdb_new(
      ArraySchemaEvolution,
      attrs_to_add,
      attrs_to_drop,
      enmrs_to_add,
      enmrs_to_drop,
      ts_range));
}

Status array_schema_evolution_serialize(
    ArraySchemaEvolution* array_schema_evolution,
    SerializationType serialize_type,
    Buffer* serialized_buffer,
    const bool client_side) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::ArraySchemaEvolution::Builder arraySchemaEvolutionBuilder =
        message.initRoot<capnp::ArraySchemaEvolution>();
    RETURN_NOT_OK(array_schema_evolution_to_capnp(
        array_schema_evolution, &arraySchemaEvolutionBuilder, client_side));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(arraySchemaEvolutionBuilder);
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
        return LOG_STATUS(
            Status_SerializationError("Error serializing array schema "
                                      "evolution; Unknown serialization type "
                                      "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing array schema evolution; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing array schema evolution; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status array_schema_evolution_deserialize(
    ArraySchemaEvolution** array_schema_evolution,
    SerializationType serialize_type,
    const Buffer& serialized_buffer) {
  try {
    tdb_unique_ptr<ArraySchemaEvolution> decoded_array_schema_evolution =
        nullptr;

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::ArraySchemaEvolution::Builder array_schema_evolution_builder =
            message_builder.initRoot<capnp::ArraySchemaEvolution>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            array_schema_evolution_builder);
        capnp::ArraySchemaEvolution::Reader array_schema_evolution_reader =
            array_schema_evolution_builder.asReader();
        decoded_array_schema_evolution =
            array_schema_evolution_from_capnp(array_schema_evolution_reader);
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::ArraySchemaEvolution::Reader array_schema_evolution_reader =
            reader.getRoot<capnp::ArraySchemaEvolution>();
        decoded_array_schema_evolution =
            array_schema_evolution_from_capnp(array_schema_evolution_reader);
        break;
      }
      default: {
        return LOG_STATUS(
            Status_SerializationError("Error deserializing array schema "
                                      "evolution; Unknown serialization type "
                                      "passed"));
      }
    }

    if (decoded_array_schema_evolution == nullptr)
      return LOG_STATUS(Status_SerializationError(
          "Error serializing array schema evolution; deserialized schema "
          "evolution is null"));

    *array_schema_evolution = decoded_array_schema_evolution.release();
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing array schema evolution; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing array schema evolution; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

#else

Status array_schema_evolution_serialize(
    ArraySchemaEvolution*, SerializationType, Buffer*, const bool) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status array_schema_evolution_deserialize(
    ArraySchemaEvolution**, SerializationType, const Buffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
