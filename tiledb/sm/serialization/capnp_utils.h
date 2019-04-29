/**
 * @file   capnp_utils.h
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

#ifndef TILEDB_CAPNP_UTILS_H
#define TILEDB_CAPNP_UTILS_H

#ifdef TILEDB_SERIALIZATION

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/serialization/tiledb-rest.capnp.h"

namespace tiledb {
namespace sm {
namespace serialization {
namespace utils {

/**
 * Returns true if the given pointer is aligned to the given number of bytes.
 */
template <size_t bytes>
inline bool is_aligned(const void* ptr) {
  return ((uintptr_t)ptr) % bytes == 0;
}

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
      return tiledb::sm::Status::SerializationError(
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
      return tiledb::sm::Status::SerializationError(
          "Cannot set capnp scalar; unknown TileDB datatype.");
  }

  return tiledb::sm::Status::Ok();
}

/**
 * Makes a copy of a typed Capnp List into the given output Buffer.
 *
 * @tparam T The primitive type of elements in the list.
 * @tparam CapnpT The CapnProto List Reader type.
 * @param list_reader The list Reader
 * @param buffer Buffer that will contain a copy of the list.
 * @return Status
 */
template <typename T, typename CapnpT>
tiledb::sm::Status copy_capnp_list(
    const CapnpT& list_reader, tiledb::sm::Buffer* buffer) {
  const auto nelts = list_reader.size();
  RETURN_NOT_OK(buffer->realloc(nelts * sizeof(T)));

  for (size_t i = 0; i < nelts; i++) {
    T val = list_reader[i];
    RETURN_NOT_OK(buffer->write(&val, sizeof(val)));
  }

  return tiledb::sm::Status::Ok();
}

/**
 * Makes a copy of a typed Capnp List into the given output Buffer.
 *
 * This calls `get{Int8,Int16,Float32,...}()` to get the typed list as
 * necessary on the given Capnp reader object, by dispatching on the given
 * TileDB datatype. It then calls
 * `copy_capnp_list(const CapnpT& list_reader, tiledb::sm::Buffer* buffer)` to
 * copy every element in the capnp list to the buffer.
 *
 * @tparam CapnpT The CapnProto type. Must define the `get*` functions.
 * @param reader The CapnProto reader instance.
 * @param datatype The TileDB datatype of the underlying list.
 * @param buffer Buffer that will contain a copy of the list.
 * @return Status
 */
template <typename CapnpT>
tiledb::sm::Status copy_capnp_list(
    const CapnpT& reader,
    tiledb::sm::Datatype datatype,
    tiledb::sm::Buffer* buffer) {
  buffer->reset_size();
  buffer->reset_offset();

  switch (datatype) {
    case tiledb::sm::Datatype::INT8:
      if (reader.hasInt8())
        RETURN_NOT_OK(copy_capnp_list<int8_t>(reader.getInt8(), buffer));
      break;
    case tiledb::sm::Datatype::UINT8:
      if (reader.hasUint8())
        RETURN_NOT_OK(copy_capnp_list<uint8_t>(reader.getUint8(), buffer));
      break;
    case tiledb::sm::Datatype::INT16:
      if (reader.hasInt16())
        RETURN_NOT_OK(copy_capnp_list<int16_t>(reader.getInt16(), buffer));
      break;
    case tiledb::sm::Datatype::UINT16:
      if (reader.hasUint16())
        RETURN_NOT_OK(copy_capnp_list<uint16_t>(reader.getUint16(), buffer));
      break;
    case tiledb::sm::Datatype::INT32:
      if (reader.hasInt32())
        RETURN_NOT_OK(copy_capnp_list<int32_t>(reader.getInt32(), buffer));
      break;
    case tiledb::sm::Datatype::UINT32:
      if (reader.hasUint32())
        RETURN_NOT_OK(copy_capnp_list<uint32_t>(reader.getUint32(), buffer));
      break;
    case tiledb::sm::Datatype::INT64:
      if (reader.hasInt64())
        RETURN_NOT_OK(copy_capnp_list<int64_t>(reader.getInt64(), buffer));
      break;
    case tiledb::sm::Datatype::UINT64:
      if (reader.hasUint64())
        RETURN_NOT_OK(copy_capnp_list<uint64_t>(reader.getUint64(), buffer));
      break;
    case tiledb::sm::Datatype::FLOAT32:
      if (reader.hasFloat32())
        RETURN_NOT_OK(copy_capnp_list<float>(reader.getFloat32(), buffer));
      break;
    case tiledb::sm::Datatype::FLOAT64:
      if (reader.hasFloat64())
        RETURN_NOT_OK(copy_capnp_list<double>(reader.getFloat64(), buffer));
      break;
    default:
      return tiledb::sm::Status::SerializationError(
          "Cannot copy capnp list; unhandled TileDB datatype.");
  }

  return tiledb::sm::Status::Ok();
}

/**
 * Serializes the given arbitrarily typed subarray into the given Capnp builder.
 *
 * @tparam CapnpT Capnp builder type
 * @param builder Builder to set subarray onto
 * @param array_schema Array schema of subarray
 * @param subarray Subarray
 * @return Status
 */
template <typename CapnpT>
tiledb::sm::Status serialize_subarray(
    CapnpT& builder,
    const tiledb::sm::ArraySchema* array_schema,
    const void* subarray) {
  // Check coords type
  const auto coords_type = array_schema->coords_type();
  switch (coords_type) {
    case tiledb::sm::Datatype::CHAR:
    case tiledb::sm::Datatype::STRING_ASCII:
    case tiledb::sm::Datatype::STRING_UTF8:
    case tiledb::sm::Datatype::STRING_UTF16:
    case tiledb::sm::Datatype::STRING_UTF32:
    case tiledb::sm::Datatype::STRING_UCS2:
    case tiledb::sm::Datatype::STRING_UCS4:
    case tiledb::sm::Datatype::ANY:
      // String dimensions not yet supported
      return LOG_STATUS(tiledb::sm::Status::SerializationError(
          "Cannot serialize subarray; unsupported domain type."));
    default:
      break;
  }

  const uint64_t subarray_size = 2 * array_schema->coords_size();
  const uint64_t subarray_length = subarray_size / datatype_size(coords_type);
  RETURN_NOT_OK(
      set_capnp_array_ptr(builder, coords_type, subarray, subarray_length));

  return tiledb::sm::Status::Ok();
}

template <typename CapnpT>
tiledb::sm::Status deserialize_subarray(
    const CapnpT& reader,
    const tiledb::sm::ArraySchema* array_schema,
    void** subarray) {
  // Check coords type
  const auto coords_type = array_schema->coords_type();
  switch (coords_type) {
    case tiledb::sm::Datatype::CHAR:
    case tiledb::sm::Datatype::STRING_ASCII:
    case tiledb::sm::Datatype::STRING_UTF8:
    case tiledb::sm::Datatype::STRING_UTF16:
    case tiledb::sm::Datatype::STRING_UTF32:
    case tiledb::sm::Datatype::STRING_UCS2:
    case tiledb::sm::Datatype::STRING_UCS4:
    case tiledb::sm::Datatype::ANY:
      // String dimensions not yet supported
      return LOG_STATUS(tiledb::sm::Status::SerializationError(
          "Cannot deserialize subarray; unsupported domain type."));
    default:
      break;
  }

  const uint64_t subarray_size = 2 * array_schema->coords_size();
  tiledb::sm::Buffer subarray_buff;
  RETURN_NOT_OK(copy_capnp_list(reader, coords_type, &subarray_buff));

  if (subarray_buff.size() == 0) {
    *subarray = nullptr;
  } else {
    *subarray = std::malloc(subarray_size);
    std::memcpy(*subarray, subarray_buff.data(), subarray_size);
  }

  return tiledb::sm::Status::Ok();
}

}  // namespace utils
}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION

#endif  // TILEDB_CAPNP_UTILS_H
