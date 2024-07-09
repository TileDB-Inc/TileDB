/**
 * @file   fragment_info.cc
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
 * This file declares serialization functions for Fragment Info.
 */

// clang-format off
#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif
// clang-format on

#include <string>

#include "tiledb/common/common.h"
#include "tiledb/sm/buffer/buffer_list.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/fragment_info.h"
#include "tiledb/sm/serialization/fragment_metadata.h"

using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

Status fragment_info_request_to_capnp(
    const FragmentInfo& fragment_info,
    capnp::FragmentInfoRequest::Builder* fragment_info_req_builder) {
  // Set config
  auto config_builder = fragment_info_req_builder->initConfig();
  auto config = fragment_info.config();
  RETURN_NOT_OK(config_to_capnp(config, &config_builder));

  return Status::Ok();
}

Status fragment_info_request_from_capnp(
    const capnp::FragmentInfoRequest::Reader& fragment_info_req_reader,
    FragmentInfo* fragment_info) {
  if (fragment_info == nullptr) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing fragment info request; fragment_info is null."));
  }

  if (fragment_info_req_reader.hasConfig()) {
    tdb_unique_ptr<Config> decoded_config = nullptr;
    RETURN_NOT_OK(config_from_capnp(
        fragment_info_req_reader.getConfig(), &decoded_config));
    fragment_info->set_config(*decoded_config);
  }

  return Status::Ok();
}

Status fragment_info_request_serialize(
    const FragmentInfo& fragment_info,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::FragmentInfoRequest::Builder fragment_info_req_builder =
        message.initRoot<capnp::FragmentInfoRequest>();
    RETURN_NOT_OK(fragment_info_request_to_capnp(
        fragment_info, &fragment_info_req_builder));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(fragment_info_req_builder);
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
            "Error serializing fragment info request; Unknown serialization "
            "type "
            "passed: " +
            std::to_string(static_cast<uint8_t>(serialize_type))));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing fragment info request; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing fragment info request; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status fragment_info_request_deserialize(
    FragmentInfo* fragment_info,
    SerializationType serialize_type,
    const Buffer& serialized_buffer) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        json.handleByAnnotation<capnp::FragmentInfoRequest>();
        ::capnp::MallocMessageBuilder message_builder;
        capnp::FragmentInfoRequest::Builder fragment_info_req_builder =
            message_builder.initRoot<capnp::FragmentInfoRequest>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            fragment_info_req_builder);
        capnp::FragmentInfoRequest::Reader fragment_info_req_reader =
            fragment_info_req_builder.asReader();
        RETURN_NOT_OK(fragment_info_request_from_capnp(
            fragment_info_req_reader, fragment_info));
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::FragmentInfoRequest::Reader fragment_info_req_reader =
            reader.getRoot<capnp::FragmentInfoRequest>();
        RETURN_NOT_OK(fragment_info_request_from_capnp(
            fragment_info_req_reader, fragment_info));
        break;
      }
      default: {
        return LOG_STATUS(
            Status_SerializationError("Error deserializing fragment info "
                                      "request; Unknown serialization type "
                                      "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing fragment info request; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing fragment info request; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

std::tuple<Status, std::optional<SingleFragmentInfo>>
single_fragment_info_from_capnp(
    const capnp::SingleFragmentInfo::Reader& single_frag_info_reader,
    FragmentInfo* fragment_info) {
  // Get array schema name
  std::string schema_name;
  if (single_frag_info_reader.hasArraySchemaName()) {
    schema_name = single_frag_info_reader.getArraySchemaName().cStr();
  } else {
    return {
        Status_SerializationError(
            "Missing array schema name from single fragment info capnp reader"),
        nullopt};
  }

  // Use the array schema name to find the corresponding array schema
  auto schema = fragment_info->array_schemas_all().find(schema_name);
  if (schema == fragment_info->array_schemas_all().end()) {
    return {
        Status_SerializationError(
            "Could not find schema" + schema_name +
            "in map of deserialized schemas."),
        nullopt};
  }

  // Get list of single fragment info
  shared_ptr<FragmentMetadata> meta;
  if (single_frag_info_reader.hasMeta()) {
    auto frag_meta_reader = single_frag_info_reader.getMeta();

    auto memory_tracker = fragment_info->resources()->create_memory_tracker();
    meta = make_shared<FragmentMetadata>(
        HERE(),
        fragment_info->resources(),
        memory_tracker,
        frag_meta_reader.getVersion());
    auto st =
        fragment_metadata_from_capnp(schema->second, frag_meta_reader, meta);
  } else {
    return {
        Status_SerializationError(
            "Missing fragment metadata from single fragment info capnp reader"),
        nullopt};
  }

  auto expanded_non_empty_domain = meta->non_empty_domain();
  if (meta->dense()) {
    meta->array_schema()->domain().expand_to_tiles(&expanded_non_empty_domain);
  }
  SingleFragmentInfo single_frag_info{
      meta->fragment_uri(),
      !meta->dense(),
      meta->timestamp_range(),
      single_frag_info_reader.getFragmentSize(),
      meta->non_empty_domain(),
      expanded_non_empty_domain,
      meta};
  // This is needed so that we don't try to load rtee from disk
  single_frag_info.meta()->loaded_metadata()->set_rtree_loaded();

  return {Status::Ok(), single_frag_info};
}

Status single_fragment_info_to_capnp(
    const SingleFragmentInfo& single_frag_info,
    capnp::SingleFragmentInfo::Builder* single_frag_info_builder) {
  // set array schema name
  single_frag_info_builder->setArraySchemaName(
      single_frag_info.array_schema_name());

  // set fragment metadata
  auto frag_meta_builder = single_frag_info_builder->initMeta();
  RETURN_NOT_OK(
      fragment_metadata_to_capnp(*single_frag_info.meta(), &frag_meta_builder));

  // set fragment size
  single_frag_info_builder->setFragmentSize(single_frag_info.fragment_size());
  return Status::Ok();
}

Status fragment_info_from_capnp(
    const capnp::FragmentInfo::Reader& fragment_info_reader,
    const URI& array_uri,
    FragmentInfo* fragment_info,
    shared_ptr<MemoryTracker> memory_tracker) {
  // Get array_schema_latest from capnp
  if (fragment_info_reader.hasArraySchemaLatest()) {
    auto array_schema_latest_reader =
        fragment_info_reader.getArraySchemaLatest();
    auto array_schema_latest{array_schema_from_capnp(
        array_schema_latest_reader, array_uri, memory_tracker)};
    array_schema_latest->set_array_uri(array_uri);
    fragment_info->array_schema_latest() = array_schema_latest;
  }

  // Get array_schemas_all from capnp
  if (fragment_info_reader.hasArraySchemasAll()) {
    auto all_schemas_reader = fragment_info_reader.getArraySchemasAll();

    if (all_schemas_reader.hasEntries()) {
      auto entries = fragment_info_reader.getArraySchemasAll().getEntries();
      for (auto array_schema_build : entries) {
        auto schema{array_schema_from_capnp(
            array_schema_build.getValue(), array_uri, memory_tracker)};
        schema->set_array_uri(array_uri);
        auto key = std::string_view{
            array_schema_build.getKey().cStr(),
            array_schema_build.getKey().size()};
        fragment_info->array_schemas_all()[std::string{key}] = schema;
      }
    }
  }

  // Get single_fragment_info from capnp
  if (fragment_info_reader.hasFragmentInfo()) {
    auto fragment_info_list_reader = fragment_info_reader.getFragmentInfo();
    fragment_info->single_fragment_info_vec().reserve(
        fragment_info_list_reader.size());
    for (auto single_frag_info_reader : fragment_info_list_reader) {
      auto&& [st, single_frag_info] = single_fragment_info_from_capnp(
          single_frag_info_reader, fragment_info);
      RETURN_NOT_OK(st);
      fragment_info->single_fragment_info_vec().emplace_back(
          single_frag_info.value());
    }
  }

  // Get uris to vacuum from capnp
  if (fragment_info_reader.hasToVacuum()) {
    for (auto uri : fragment_info_reader.getToVacuum()) {
      // Reconstruct the relative fragment URI to a full path URI
      auto frag_dir_uri = ArrayDirectory::generate_fragment_dir_uri(
          fragment_info->array_schema_latest()->write_version(),
          array_uri.add_trailing_slash());
      fragment_info->to_vacuum().emplace_back(
          frag_dir_uri.join_path(uri.cStr()));
    }
  }

  // Fill in the rest of values of fragment info class using the above
  // deserialized values
  fragment_info->array_uri() = array_uri;
  fragment_info->unconsolidated_metadata_num() = 0;
  for (const auto& f : fragment_info->single_fragment_info_vec()) {
    fragment_info->unconsolidated_metadata_num() +=
        (uint32_t)!f.has_consolidated_footer();
  }

  fragment_info->loaded() = true;
  return Status::Ok();
}

Status fragment_info_to_capnp(
    const FragmentInfo& fragment_info,
    capnp::FragmentInfo::Builder* fragment_info_builder,
    const bool client_side) {
  // set array_schema_latest
  const auto& array_schema_latest = fragment_info.array_schema_latest();
  auto array_schema_latest_builder =
      fragment_info_builder->initArraySchemaLatest();
  RETURN_NOT_OK(array_schema_to_capnp(
      *array_schema_latest, &array_schema_latest_builder, client_side));

  // set array_schema_all
  const auto& array_schemas_all = fragment_info.array_schemas_all();
  auto array_schemas_all_builder = fragment_info_builder->initArraySchemasAll();
  auto entries_builder =
      array_schemas_all_builder.initEntries(array_schemas_all.size());
  size_t i = 0;
  for (const auto& schema : array_schemas_all) {
    auto entry = entries_builder[i++];
    entry.setKey(schema.first);
    auto schema_builder = entry.initValue();
    RETURN_NOT_OK(array_schema_to_capnp(
        *(schema.second.get()), &schema_builder, client_side));
  }

  // set single fragment info list
  const auto& frag_info = fragment_info.single_fragment_info_vec();
  auto frag_info_builder =
      fragment_info_builder->initFragmentInfo(frag_info.size());
  for (size_t i = 0; i < frag_info.size(); i++) {
    auto single_info_builder = frag_info_builder[i];
    throw_if_not_ok(
        single_fragment_info_to_capnp(frag_info[i], &single_info_builder));
  }

  // set fragment uris to vacuum
  const auto& uris_to_vacuum = fragment_info.to_vacuum();
  auto vacuum_uris_builder =
      fragment_info_builder->initToVacuum(uris_to_vacuum.size());
  for (size_t i = 0; i < uris_to_vacuum.size(); i++) {
    // Strip full URI and send only the name of the fragment to vacuum for
    // security reasons
    vacuum_uris_builder.set(
        i, uris_to_vacuum[i].remove_trailing_slash().last_path_part());
  }

  return Status::Ok();
}

Status fragment_info_serialize(
    const FragmentInfo& fragment_info,
    SerializationType serialize_type,
    Buffer* serialized_buffer,
    const bool client_side) {
  try {
    // Serialize
    ::capnp::MallocMessageBuilder message;
    auto builder = message.initRoot<capnp::FragmentInfo>();

    RETURN_NOT_OK(fragment_info_to_capnp(fragment_info, &builder, client_side));

    // Copy to buffer
    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
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
            "Error serializing fragment info; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing fragment info; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing fragment info; exception " + std::string(e.what())));
  }

  return Status::Ok();
}

Status fragment_info_deserialize(
    FragmentInfo* fragment_info,
    SerializationType serialize_type,
    const URI& uri,
    const Buffer& serialized_buffer,
    shared_ptr<MemoryTracker> memory_tracker) {
  if (fragment_info == nullptr)
    return LOG_STATUS(
        Status_SerializationError("Error deserializing fragment info; null "
                                  "fragment info instance given."));

  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        auto builder = message_builder.initRoot<capnp::FragmentInfo>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            builder);
        auto reader = builder.asReader();

        // Deserialize
        RETURN_NOT_OK(fragment_info_from_capnp(
            reader, uri, fragment_info, memory_tracker));
        break;
      }
      case SerializationType::CAPNP: {
        // Set traversal limit from config
        uint64_t limit = fragment_info->config()
                             .get<uint64_t>("rest.capnp_traversal_limit")
                             .value();
        ::capnp::ReaderOptions readerOptions;
        // capnp uses the limit in words of 8 bytes
        readerOptions.traversalLimitInWords = limit / 8;

        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader msg_reader(
            kj::arrayPtr(
                reinterpret_cast<const ::capnp::word*>(mBytes),
                serialized_buffer.size() / sizeof(::capnp::word)),
            readerOptions);
        auto reader = msg_reader.getRoot<capnp::FragmentInfo>();

        // Deserialize
        RETURN_NOT_OK(fragment_info_from_capnp(
            reader, uri, fragment_info, memory_tracker));
        break;
      }
      default: {
        throw StatusException(Status_SerializationError(
            "Error deserializing fragment info; Unknown serialization type "
            "passed"));
      }
    }
  } catch (kj::Exception& e) {
    throw StatusException(Status_SerializationError(
        "Error deserializing fragment info; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    throw StatusException(Status_SerializationError(
        "Error deserializing fragment info; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

#else

Status fragment_info_serialize(
    const FragmentInfo&, SerializationType, Buffer*, bool) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status fragment_info_deserialize(
    FragmentInfo*,
    SerializationType,
    const URI&,
    const Buffer&,
    shared_ptr<MemoryTracker>) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

Status fragment_info_request_serialize(
    const FragmentInfo&, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status fragment_info_request_deserialize(
    FragmentInfo*, SerializationType, const Buffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
