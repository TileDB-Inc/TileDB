/**
 * @file   group.h
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
 * This file declares serialization for the Group class
 */

#ifndef TILEDB_SERIALIZATION_GROUP_H
#define TILEDB_SERIALIZATION_GROUP_H

#include <unordered_map>

#include "tiledb/common/status.h"
#include "tiledb/sm/group/group_member.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Group;
class SerializationBuffer;
enum class SerializationType : uint8_t;

namespace serialization {

/**
 * Serialize a group via Cap'n Prto
 *
 * @param Group group object to serialize
 * @param serialize_type format to serialize into Cap'n Proto or JSON
 * @param serialized_buffer buffer to store serialized bytes in
 * @return Status
 */
Status group_serialize(
    Group* group,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer);

/**
 * Deserialize a group via Cap'n proto
 *
 * @param group to deserialize into
 * @param serialize_type format the data is serialized in Cap'n Proto of JSON
 * @param serialized_buffer buffer to read serialized bytes from
 * @return Status
 */
Status group_deserialize(
    Group* group,
    SerializationType serialize_type,
    span<const char> serialized_buffer);

/**
 * Serialize a group details via Cap'n Prto
 *
 * @param Group group object to serialize
 * @param serialize_type format to serialize into Cap'n Proto or JSON
 * @param serialized_buffer buffer to store serialized bytes in
 * serialize the array URI
 * @return Status
 */
Status group_details_serialize(
    Group* group,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer);

/**
 * Deserialize a group details via Cap'n proto
 *
 * @param group to deserialize into
 * @param serialize_type format the data is serialized in Cap'n Proto of JSON
 * @param serialized_buffer buffer to read serialized bytes from
 * @return Status
 */
Status group_details_deserialize(
    Group* group,
    SerializationType serialize_type,
    span<const char> serialized_buffer);

/**
 * Serialize a group's update state via Cap'n Prto
 *
 * @param Group group object to serialize
 * @param serialize_type format to serialize into Cap'n Proto or JSON
 * @param serialized_buffer buffer to store serialized bytes in
 * serialize the array URI
 * @return Status
 */
Status group_update_serialize(
    const Group* group,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer);

/**
 * Deserialize a group's update state via Cap'n proto
 *
 * @param group to deserialize into
 * @param serialize_type format the data is serialized in Cap'n Proto of JSON
 * @param serialized_buffer buffer to read serialized bytes from
 * @return Status
 */
Status group_update_deserialize(
    Group* group,
    SerializationType serialize_type,
    span<const char> serialized_buffer);

/**
 * Serialize a group's creation state via Cap'n Prto
 *
 * @param group group object to serialize
 * @param serialize_type format to serialize into Cap'n Proto or JSON
 * @param serialized_buffer buffer to store serialized bytes in
 * serialize the array URI
 * @return Status
 */
Status group_create_serialize(
    const Group* group,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer,
    bool legacy);

/**
 * Serialize a group metadata for remote POSTING
 *
 * @param group group object to serialize
 * @param serialize_type format to serialize into Cap'n Proto or JSON
 * @param serialized_buffer buffer to store serialized bytes in
 * serialize the array URI
 * @param load flag signaling whether or not metadata should be fetched from
 * storage
 * @return Status
 */
Status group_metadata_serialize(
    Group* group,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer,
    bool load);

#ifdef TILEDB_SERIALIZATION

/**
 * Convert Cap'n Proto message to GroupMember object
 *
 * @param group_member_reader cap'n proto class.
 * @return Status and GroupMember object
 */
std::tuple<Status, std::optional<tdb_shared_ptr<GroupMember>>>
group_member_from_capnp(capnp::GroupMember::Reader* group_member_reader);

/**
 * Convert GroupMember object to Cap'n Proto message.
 *
 * @param group_member GroupMember to serialize info from
 * @param group_member_builder cap'n proto class.
 * @return Status
 */
Status group_member_to_capnp(
    const tdb_shared_ptr<GroupMember>& group_member,
    capnp::GroupMember::Builder* group_member_builder);

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION_GROUP_H
