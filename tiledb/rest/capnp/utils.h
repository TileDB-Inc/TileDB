/**
 * @file   utils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 * This file declares utility functions for CapnProto.
 */

#ifndef TILEDB_REST_CAPNP_UTILS_H
#define TILEDB_REST_CAPNP_UTILS_H

#include "tiledb/rest/capnp/tiledb-rest.capnp.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace rest {
namespace capnp {
namespace utils {

/**
 * Calls `set{Int8,Int16,Float32,...}(::kj::ArrayPtr<const T> value)` as
 * necessary on the given Capnp builder object, by dispatching on the given
 * TileDB type.
 *
 * @tparam CapnpT The CapnProto type. Must define the `set*` functions.
 * @param builder The CapnProto builder instance.
 * @param datatype The TileDB datatype of the given array pointer.
 * @param ptr The array pointer to set.
 * @param size The size (num elements) of the array.
 * @return Status
 */
template <typename CapnpT>
tiledb::sm::Status set_capnp_array_ptr(
    CapnpT& builder,
    tiledb::sm::Datatype datatype,
    const void* ptr,
    size_t size) {
  switch (datatype) {
    case tiledb::sm::Datatype::CHAR:
    case tiledb::sm::Datatype::INT8:
      builder.setInt8(kj::arrayPtr(static_cast<const int8_t*>(ptr), size));
      break;
    case tiledb::sm::Datatype::STRING_ASCII:
    case tiledb::sm::Datatype::STRING_UTF8:
    case tiledb::sm::Datatype::UINT8:
      builder.setUint8(kj::arrayPtr(static_cast<const uint8_t*>(ptr), size));
      break;
    case tiledb::sm::Datatype::INT16:
      builder.setInt16(kj::arrayPtr(static_cast<const int16_t*>(ptr), size));
      break;
    case tiledb::sm::Datatype::STRING_UTF16:
    case tiledb::sm::Datatype::STRING_UCS2:
    case tiledb::sm::Datatype::UINT16:
      builder.setUint16(kj::arrayPtr(static_cast<const uint16_t*>(ptr), size));
      break;
    case tiledb::sm::Datatype::INT32:
      builder.setInt32(kj::arrayPtr(static_cast<const int32_t*>(ptr), size));
      break;
    case tiledb::sm::Datatype::STRING_UTF32:
    case tiledb::sm::Datatype::STRING_UCS4:
    case tiledb::sm::Datatype::UINT32:
      builder.setUint32(kj::arrayPtr(static_cast<const uint32_t*>(ptr), size));
      break;
    case tiledb::sm::Datatype::INT64:
      builder.setInt64(kj::arrayPtr(static_cast<const int64_t*>(ptr), size));
      break;
    case tiledb::sm::Datatype::UINT64:
      builder.setUint64(kj::arrayPtr(static_cast<const uint64_t*>(ptr), size));
      break;
    case tiledb::sm::Datatype::FLOAT32:
      builder.setFloat32(kj::arrayPtr(static_cast<const float*>(ptr), size));
      break;
    case tiledb::sm::Datatype::FLOAT64:
      builder.setFloat64(kj::arrayPtr(static_cast<const double*>(ptr), size));
      break;
    default:
      return tiledb::sm::Status::RestError(
          "Cannot set capnp array pointer; unknown TileDB datatype.");
  }

  return tiledb::sm::Status::Ok();
}

/**
 * Calls `set{Int8,Int16,Float32,...}(T value)` as
 * necessary on the given Capnp builder object, by dispatching on the given
 * TileDB type.
 *
 * @tparam CapnpT The CapnProto type. Must define the `set*` functions.
 * @param builder The CapnProto builder instance.
 * @param datatype The TileDB datatype of the given scalar.
 * @param value Pointer to the scalar value to set.
 * @return Status
 */
template <typename CapnpT>
tiledb::sm::Status set_capnp_scalar(
    CapnpT& builder, tiledb::sm::Datatype datatype, const void* value) {
  switch (datatype) {
    case tiledb::sm::Datatype::INT8:
      builder.setInt8(*static_cast<const int8_t*>(value));
      break;
    case tiledb::sm::Datatype::UINT8:
      builder.setUint8(*static_cast<const uint8_t*>(value));
      break;
    case tiledb::sm::Datatype::INT16:
      builder.setInt16(*static_cast<const int16_t*>(value));
      break;
    case tiledb::sm::Datatype::UINT16:
      builder.setUint16(*static_cast<const uint16_t*>(value));
      break;
    case tiledb::sm::Datatype::INT32:
      builder.setInt32(*static_cast<const int32_t*>(value));
      break;
    case tiledb::sm::Datatype::UINT32:
      builder.setUint32(*static_cast<const uint32_t*>(value));
      break;
    case tiledb::sm::Datatype::INT64:
      builder.setInt64(*static_cast<const int64_t*>(value));
      break;
    case tiledb::sm::Datatype::UINT64:
      builder.setUint64(*static_cast<const uint64_t*>(value));
      break;
    case tiledb::sm::Datatype::FLOAT32:
      builder.setFloat32(*static_cast<const float*>(value));
      break;
    case tiledb::sm::Datatype::FLOAT64:
      builder.setFloat64(*static_cast<const double*>(value));
      break;
    default:
      return tiledb::sm::Status::RestError(
          "Cannot set capnp scalar; unknown TileDB datatype.");
  }

  return tiledb::sm::Status::Ok();
}

}  // namespace utils
}  // namespace capnp
}  // namespace rest
}  // namespace tiledb

#endif  // TILEDB_REST_CAPNP_UTILS_H
