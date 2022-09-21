/**
 * @file   fragment_info.h
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
 * This file declares serialization functions for fragment info.
 */

#ifndef TILEDB_SERIALIZATION_FRAGMENT_INFO_H
#define TILEDB_SERIALIZATION_FRAGMENT_INFO_H

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/fragment/fragment_info.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class FragmentInfo;
enum class SerializationType : uint8_t;

namespace serialization {

#ifdef TILEDB_SERIALIZATION

/**
 * Convert Cap'n Proto message to Fragment Info.
 *
 * @param fragment_info_reader cap'n proto class.
 * @param fragment_info fragment info object to deserialize into.
 * @return Status
 */
Status fragment_info_from_capnp(
    const capnp::FragmentInfo::Reader& fragment_info_reader,
    const URI& uri,
    FragmentInfo* fragment_info);

/**
 * Convert Fragment Info to Cap'n Proto message.
 *
 * @param fragment_info fragment info to serialize.
 * @param fragment_info_builder cap'n proto class.
 * @param client_side indicate if client or server side.
 * @return Status
 */
Status fragment_info_to_capnp(
    const FragmentInfo& fragment_info,
    capnp::FragmentInfo::Builder* fragment_info_builder,
    const bool client_side);

// TODO: Docstrings + add single fragment info here too
Status fragment_info_request_to_capnp(
    const FragmentInfo& fragment_info,
    capnp::FragmentInfoRequest::Builder* fragment_info_req_builder);

Status fragment_info_request_from_capnp(
    const capnp::FragmentInfoRequest::Reader& fragment_info_req_reader,
    FragmentInfo* fragment_info);

#endif

/**
 * Serialize a fragment info object via Cap'n Proto.
 *
 * @param fragment_info fragment info object to serialize.
 * @param serialize_type format to serialize into Cap'n Proto or JSON.
 * @param serialized_buffer buffer to store serialized bytes in.
 * @param client_side indicate if client or server side.
 * @return Status
 */
Status fragment_info_serialize(
    const FragmentInfo& fragment_info,
    SerializationType serialize_type,
    Buffer* serialized_buffer,
    const bool client_side);

/**
 * Deserialize a Cap'n Proto message into a fragment info object.
 *
 * @param fragment_info fragment info object to deserialize into.
 * @param serialize_type format the data is serialized in: Cap'n Proto of JSON.
 * @param uri array uri that the fragment belongs to
 * @param serialized_buffer buffer to read serialized bytes from.
 * @return Status
 */
Status fragment_info_deserialize(
    FragmentInfo* fragment_info,
    SerializationType serialize_type,
    const URI& uri,
    const Buffer& serialized_buffer);

// TODO: add Docstring
Status fragment_info_request_serialize(
    const FragmentInfo& fragment_info,
    SerializationType serialize_type,
    Buffer* serialized_buffer);

Status fragment_info_request_deserialize(
    FragmentInfo* fragment_info,
    SerializationType serialize_type,
    const Buffer& serialized_buffer);

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION_FRAGMENT_INFO_H
