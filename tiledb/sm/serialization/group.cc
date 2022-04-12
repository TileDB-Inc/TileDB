/**
 * @file   group.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file implements serialization for the Group class
 */

// clang-format off
#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif
// clang-format on

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/group/group.h"
#include "tiledb/sm/group/group_member_v1.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/group.h"

#include <set>

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

Status group_metadata_to_capnp(
    const Group* group, capnp::GroupMetadata::Builder* group_metadata_builder) {
  // Set config
  auto config_builder = group_metadata_builder->initConfig();
  RETURN_NOT_OK(config_to_capnp(group->config(), &config_builder));

  const Metadata* metadata = group->metadata();
  if (metadata->num()) {
    auto metadata_builder = group_metadata_builder->initMetadata();
    RETURN_NOT_OK(metadata_to_capnp(metadata, &metadata_builder));
  }

  return Status::Ok();
}

Status group_member_to_capnp(
    const tdb_shared_ptr<GroupMember>& group_member,
    capnp::GroupMember::Builder* group_member_builder) {
  if (group_member == nullptr)
    return LOG_STATUS(Status_SerializationError(
        "Error serializing group member; group is null."));

  std::string type_str = object_type_str(group_member->type());
  group_member_builder->setType(type_str);

  group_member_builder->setUri(group_member->uri().to_string());

  group_member_builder->setRelative(group_member->relative());

  auto name = group_member->name();
  if (name.has_value()) {
    group_member_builder->setName(name.value());
  }

  return Status::Ok();
}

std::tuple<Status, std::optional<tdb_shared_ptr<GroupMember>>>
group_member_from_capnp(capnp::GroupMember::Reader* group_member_reader) {
  if (!group_member_reader->hasUri()) {
    return {Status_SerializationError(
                "Incomplete group member type in deserialization, missing uri"),
            std::nullopt};
  }

  if (!group_member_reader->hasType()) {
    return {
        Status_SerializationError(
            "Incomplete group member type in deserialization, missing type"),
        std::nullopt};
  }

  ObjectType type;
  RETURN_NOT_OK_TUPLE(
      object_type_enum(group_member_reader->getType().cStr(), &type),
      std::nullopt);
  const char* uri = group_member_reader->getUri().cStr();
  const bool relative = group_member_reader->getRelative();
  std::optional<std::string> name = std::nullopt;
  if (group_member_reader->hasName()) {
    name = group_member_reader->getName().cStr();
  }

  tdb_shared_ptr<GroupMember> group_member =
      tdb::make_shared<GroupMemberV1>(HERE(), URI(uri), type, relative, name);

  return {Status::Ok(), group_member};
}

Status group_details_to_capnp(
    const Group* group,
    capnp::Group::GroupDetails::Builder* group_details_builder) {
  if (group == nullptr)
    return LOG_STATUS(
        Status_SerializationError("Error serializing group; group is null."));

  const auto& group_members = group->members();
  if (!group_members.empty()) {
    auto group_members_builder =
        group_details_builder->initMembers(group_members.size());
    uint64_t i = 0;
    for (const auto& it : group_members) {
      auto group_member_builder = group_members_builder[i];
      RETURN_NOT_OK(group_member_to_capnp(it.second, &group_member_builder));
      // Increment index
      ++i;
    }
  }

  const Metadata* metadata = group->metadata();
  if (metadata->num()) {
    auto group_metadata_builder = group_details_builder->initMetadata();
    RETURN_NOT_OK(metadata_to_capnp(metadata, &group_metadata_builder));
  }

  return Status::Ok();
}

Status group_details_from_capnp(
    const capnp::Group::GroupDetails::Reader& group_details_reader,
    Group* group) {
  if (group_details_reader.hasMembers()) {
    for (auto member : group_details_reader.getMembers()) {
      auto&& [st, group_member] = group_member_from_capnp(&member);
      RETURN_NOT_OK(st);
      RETURN_NOT_OK(group->add_member(group_member.value()));
    }
  }

  if (group_details_reader.hasMetadata()) {
    RETURN_NOT_OK(metadata_from_capnp(
        group_details_reader.getMetadata(), group->unsafe_metadata()));
    group->set_metadata_loaded(true);
  }

  group->set_changes_applied(true);

  return Status::Ok();
}

Status group_to_capnp(
    const Group* group, capnp::Group::Builder* group_builder) {
  if (group == nullptr)
    return LOG_STATUS(
        Status_SerializationError("Error serializing group; group is null."));

  // Set config
  auto config_builder = group_builder->initConfig();
  RETURN_NOT_OK(config_to_capnp(group->config(), &config_builder));

  auto group_details_builder = group_builder->initGroup();
  RETURN_NOT_OK(group_details_to_capnp(group, &group_details_builder));

  return Status::Ok();
}

Status group_from_capnp(
    const capnp::Group::Reader& group_reader, Group* group) {
  if (group_reader.hasConfig()) {
    tdb_unique_ptr<Config> decoded_config = nullptr;
    RETURN_NOT_OK(config_from_capnp(group_reader.getConfig(), &decoded_config));
    RETURN_NOT_OK(group->set_config(*decoded_config));
  }

  if (group_reader.hasGroup()) {
    group->clear();
    RETURN_NOT_OK(group_details_from_capnp(group_reader.getGroup(), group));
  }

  return Status::Ok();
}

Status group_update_details_to_capnp(
    const Group* group,
    capnp::GroupUpdate::GroupUpdateDetails::Builder*
        group_update_details_builder) {
  if (group == nullptr) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing group details; group is null."));
  }

  const auto& group_members_to_add = group->members_to_add();
  if (!group_members_to_add.empty()) {
    auto group_members_to_add_builder =
        group_update_details_builder->initMembersToAdd(
            group_members_to_add.size());
    uint64_t i = 0;
    for (const auto& it : group_members_to_add) {
      auto group_member_to_add_builder = group_members_to_add_builder[i];
      RETURN_NOT_OK(
          group_member_to_capnp(it.second, &group_member_to_add_builder));
      // Increment index
      ++i;
    }
  }

  const auto& group_members_to_remove = group->members_to_remove();
  if (!group_members_to_remove.empty()) {
    auto group_members_to_remove_builder =
        group_update_details_builder->initMembersToRemove(
            group_members_to_remove.size());
    uint64_t i = 0;
    for (const auto& it : group_members_to_remove) {
      group_members_to_remove_builder.set(i, it.c_str());
      // Increment index
      ++i;
    }
  }

  return Status::Ok();
}

Status group_update_from_capnp(
    const capnp::GroupUpdate::GroupUpdateDetails::Reader&
        group_update_details_reader,
    Group* group) {
  if (group_update_details_reader.hasMembersToAdd()) {
    for (auto member_to_add : group_update_details_reader.getMembersToAdd()) {
      auto&& [st, group_member] = group_member_from_capnp(&member_to_add);
      RETURN_NOT_OK(st);
      group->add_member(group_member.value());
    }
  }

  if (group_update_details_reader.hasMembersToRemove()) {
    for (auto uri : group_update_details_reader.getMembersToRemove()) {
      group->mark_member_for_removal(uri.cStr());
    }
  }

  return Status::Ok();
}

Status group_update_to_capnp(
    const Group* group, capnp::GroupUpdate::Builder* group_update_builder) {
  if (group == nullptr) {
    return LOG_STATUS(
        Status_SerializationError("Error serializing group; group is null."));
  }

  // Set config
  auto config_builder = group_update_builder->initConfig();
  RETURN_NOT_OK(config_to_capnp(group->config(), &config_builder));

  auto group_update_details_builder = group_update_builder->initGroupUpdate();
  RETURN_NOT_OK(
      group_update_details_to_capnp(group, &group_update_details_builder));

  return Status::Ok();
}

Status group_update_from_capnp(
    const capnp::GroupUpdate::Reader& group_reader, Group* group) {
  if (group_reader.hasConfig()) {
    tdb_unique_ptr<Config> decoded_config = nullptr;
    RETURN_NOT_OK(config_from_capnp(group_reader.getConfig(), &decoded_config));
    RETURN_NOT_OK(group->set_config(*decoded_config));
  }

  if (group_reader.hasGroupUpdate()) {
    RETURN_NOT_OK(
        group_update_from_capnp(group_reader.getGroupUpdate(), group));
  }

  return Status::Ok();
}

Status group_create_details_to_capnp(
    const Group* group,
    capnp::GroupCreate::GroupCreateDetails::Builder*
        group_create_details_builder) {
  if (group == nullptr) {
    return LOG_STATUS(
        Status_SerializationError("Error serializing group; group is null."));
  }

  const auto& group_uri = group->group_uri();
  if (group_uri.is_tiledb()) {
    std::string group_ns, group_uri_component;
    RETURN_NOT_OK(group->group_uri().get_rest_components(
        &group_ns, &group_uri_component));
    group_create_details_builder->setUri(group_uri_component);
  } else {
    group_create_details_builder->setUri(group_uri.to_string());
  }

  return Status::Ok();
}

Status group_create_to_capnp(
    const Group* group, capnp::GroupCreate::Builder* group_create_builder) {
  if (group == nullptr) {
    return LOG_STATUS(
        Status_SerializationError("Error serializing group; group is null."));
  }

  // Set config
  auto config_builder = group_create_builder->initConfig();
  RETURN_NOT_OK(config_to_capnp(group->config(), &config_builder));

  auto group_create_details_builder = group_create_builder->initGroupDetails();
  RETURN_NOT_OK(
      group_create_details_to_capnp(group, &group_create_details_builder));

  return Status::Ok();
}

Status group_serialize(
    const Group* group,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::Group::Builder groupBuilder = message.initRoot<capnp::Group>();
    RETURN_NOT_OK(group_to_capnp(group, &groupBuilder));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        json.handleByAnnotation<capnp::Group>();
        kj::String capnp_json = json.encode(groupBuilder);
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
            "Error serializing group; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing group; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing group; exception " + std::string(e.what())));
  }

  return Status::Ok();
}

Status group_deserialize(
    Group* group,
    SerializationType serialize_type,
    const Buffer& serialized_buffer) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        json.handleByAnnotation<capnp::Group>();
        ::capnp::MallocMessageBuilder message_builder;
        capnp::Group::Builder group_builder =
            message_builder.initRoot<capnp::Group>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            group_builder);
        capnp::Group::Reader group_reader = group_builder.asReader();
        RETURN_NOT_OK(group_from_capnp(group_reader, group));
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::Group::Reader group_reader = reader.getRoot<capnp::Group>();
        RETURN_NOT_OK(group_from_capnp(group_reader, group));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing group; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing group; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing group; exception " + std::string(e.what())));
  }

  return Status::Ok();
}

Status group_details_serialize(
    const Group* group,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::Group::GroupDetails::Builder groupDetailsBuilder =
        message.initRoot<capnp::Group::GroupDetails>();
    RETURN_NOT_OK(group_details_to_capnp(group, &groupDetailsBuilder));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        json.handleByAnnotation<capnp::Group::GroupDetails>();
        kj::String capnp_json = json.encode(groupDetailsBuilder);
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
            "Error serializing group details; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing group details; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing group details; exception " + std::string(e.what())));
  }

  return Status::Ok();
}

Status group_details_deserialize(
    Group* group,
    SerializationType serialize_type,
    const Buffer& serialized_buffer) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        json.handleByAnnotation<capnp::Group::GroupDetails>();
        ::capnp::MallocMessageBuilder message_builder;
        capnp::Group::GroupDetails::Builder group_details_builder =
            message_builder.initRoot<capnp::Group::GroupDetails>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            group_details_builder);
        capnp::Group::GroupDetails::Reader group_details_reader =
            group_details_builder.asReader();
        RETURN_NOT_OK(group_details_from_capnp(group_details_reader, group));
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::Group::GroupDetails::Reader group_details_reader =
            reader.getRoot<capnp::Group::GroupDetails>();
        RETURN_NOT_OK(group_details_from_capnp(group_details_reader, group));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing group details; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing group details; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing group details; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status group_update_serialize(
    const Group* group,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::GroupUpdate::Builder groupUpdateBuilder =
        message.initRoot<capnp::GroupUpdate>();
    RETURN_NOT_OK(group_update_to_capnp(group, &groupUpdateBuilder));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        json.handleByAnnotation<capnp::GroupUpdate>();
        kj::String capnp_json = json.encode(groupUpdateBuilder);
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
            "Error serializing group; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing group; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing group; exception " + std::string(e.what())));
  }

  return Status::Ok();
}

Status group_update_deserialize(
    Group* group,
    SerializationType serialize_type,
    const Buffer& serialized_buffer) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        json.handleByAnnotation<capnp::GroupUpdate>();
        ::capnp::MallocMessageBuilder message_builder;
        capnp::GroupUpdate::Builder group_update_builder =
            message_builder.initRoot<capnp::GroupUpdate>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            group_update_builder);
        capnp::GroupUpdate::Reader group_reader =
            group_update_builder.asReader();
        RETURN_NOT_OK(group_update_from_capnp(group_reader, group));
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::Group::Reader group_reader = reader.getRoot<capnp::Group>();
        RETURN_NOT_OK(group_from_capnp(group_reader, group));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing group update; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing group update; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing group update; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status group_create_serialize(
    const Group* group,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::GroupCreate::Builder group_create_builder =
        message.initRoot<capnp::GroupCreate>();
    RETURN_NOT_OK(group_create_to_capnp(group, &group_create_builder));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        json.handleByAnnotation<capnp::GroupCreate>();
        kj::String capnp_json = json.encode(group_create_builder);
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
            "Error serializing group create; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing group create; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing group create; exception " + std::string(e.what())));
  }

  return Status::Ok();
}

Status group_metadata_serialize(
    const Group* group,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::GroupMetadata::Builder group_metadata_builder =
        message.initRoot<capnp::GroupMetadata>();
    RETURN_NOT_OK(group_metadata_to_capnp(group, &group_metadata_builder));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        json.handleByAnnotation<capnp::GroupCreate>();
        kj::String capnp_json = json.encode(group_metadata_builder);
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
            "Error serializing group metadata; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing group metadata; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing group metadata; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

#else

Status group_serialize(const Group*, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status group_deserialize(Group*, SerializationType, const Buffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

Status group_details_serialize(const Group*, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status group_details_deserialize(Group*, SerializationType, const Buffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

Status group_update_serialize(const Group*, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status group_update_deserialize(Group*, SerializationType, const Buffer&) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot deserialize; serialization not enabled."));
}

Status group_create_serialize(const Group*, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status group_metadata_serialize(const Group*, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
