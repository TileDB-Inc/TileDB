/**
 * @file   rest_version.h
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
 * This file declares serialization functions for REST version information.
 */

#ifndef TILEDB_SERIALIZATION_REST_VERSION_H
#define TILEDB_SERIALIZATION_REST_VERSION_H

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/array/array.h"
#include "tiledb/sm/buffer/buffer.h"

using namespace tiledb::common;

namespace tiledb::sm {

class SerializationBuffer;
enum class SerializationType : uint8_t;

namespace serialization {

#ifdef TILEDB_SERIALIZATION

Status rest_version_to_capnp(
    Context* ctx, capnp::RestVersion::Builder* rest_version_builder);

std::string rest_version_from_capnp(
    const capnp::RestVersion::Reader& rest_version_reader);

Status rest_version_serialize(
    Context* ctx,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer);

std::string rest_version_deserialize(
    SerializationType serialization_type, span<const char> serialized_response);

#else

Status rest_version_to_capnp(
    Context* ctx, capnp::RestVersion::Builder* rest_version_builder);

void rest_version_from_capnp(
    const capnp::RestVersion::Reader& rest_version_reader);

Status rest_version_serialize(
    SerializationType serialize_type, SerializationBuffer& serialized_buffer);

std::string rest_version_deserialize(
    SerializationType serialization_type, span<const char> response);

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace tiledb::sm

#endif  // TILEDB_SERIALIZATION_REST_VERSION_H
