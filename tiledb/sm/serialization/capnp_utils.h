/**
 * @file   capnp_utils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2021 TileDB, Inc.
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

#include "tiledb-rest.h"

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
class Attribute;
namespace serialization {

/**
 * Serialize a config into a cap'n proto class
 * @param config config to serialize
 * @param config_builder cap'n proto message class
 * @return Status
 */
Status config_to_capnp(
    const Config* config, capnp::Config::Builder* config_builder);

/**
 * Create a config object from a cap'n proto class
 * @param config_reader cap'n proto message class
 * @param config config to deserialize into
 * @return Status
 */
Status config_from_capnp(
    const capnp::Config::Reader& config_reader, tdb_unique_ptr<Config>* config);

/**
 * Serialize an attribute into a cap'n proto class
 * @param attribute attribute to serialize
 * @param attribute_builder cap'n proto message class
 * @return Status
 */
Status attribute_to_capnp(
    const Attribute* attribute, capnp::Attribute::Builder* attribute_builder);

/**
 * Create an attribute object from a cap'n proto class
 * @param attribute_reader cap'n proto message class
 * @param attribute attribute to deserialize into
 * @return Status
 */
Status attribute_from_capnp(
    const capnp::Attribute::Reader& attribute_reader,
    tdb_unique_ptr<Attribute>* attribute);

};  // namespace serialization
};  // namespace sm
};  // namespace tiledb

namespace tiledb {
namespace sm {
class Dimension;
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
Status set_capnp_array_ptr(
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
    case tiledb::sm::Datatype::BLOB:
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
    case tiledb::sm::Datatype::DATETIME_YEAR:
    case tiledb::sm::Datatype::DATETIME_MONTH:
    case tiledb::sm::Datatype::DATETIME_WEEK:
    case tiledb::sm::Datatype::DATETIME_DAY:
    case tiledb::sm::Datatype::DATETIME_HR:
    case tiledb::sm::Datatype::DATETIME_MIN:
    case tiledb::sm::Datatype::DATETIME_SEC:
    case tiledb::sm::Datatype::DATETIME_MS:
    case tiledb::sm::Datatype::DATETIME_US:
    case tiledb::sm::Datatype::DATETIME_NS:
    case tiledb::sm::Datatype::DATETIME_PS:
    case tiledb::sm::Datatype::DATETIME_FS:
    case tiledb::sm::Datatype::DATETIME_AS:
    case tiledb::sm::Datatype::TIME_HR:
    case tiledb::sm::Datatype::TIME_MIN:
    case tiledb::sm::Datatype::TIME_SEC:
    case tiledb::sm::Datatype::TIME_MS:
    case tiledb::sm::Datatype::TIME_US:
    case tiledb::sm::Datatype::TIME_NS:
    case tiledb::sm::Datatype::TIME_PS:
    case tiledb::sm::Datatype::TIME_FS:
    case tiledb::sm::Datatype::TIME_AS:
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
      return Status_SerializationError(
          "Cannot set capnp array pointer; unknown TileDB datatype.");
  }

  return Status::Ok();
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
Status set_capnp_scalar(
    CapnpT& builder, tiledb::sm::Datatype datatype, const void* value) {
  switch (datatype) {
    case tiledb::sm::Datatype::INT8:
      builder.setInt8(*static_cast<const int8_t*>(value));
      break;
    case tiledb::sm::Datatype::BLOB:
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
    case tiledb::sm::Datatype::DATETIME_YEAR:
    case tiledb::sm::Datatype::DATETIME_MONTH:
    case tiledb::sm::Datatype::DATETIME_WEEK:
    case tiledb::sm::Datatype::DATETIME_DAY:
    case tiledb::sm::Datatype::DATETIME_HR:
    case tiledb::sm::Datatype::DATETIME_MIN:
    case tiledb::sm::Datatype::DATETIME_SEC:
    case tiledb::sm::Datatype::DATETIME_MS:
    case tiledb::sm::Datatype::DATETIME_US:
    case tiledb::sm::Datatype::DATETIME_NS:
    case tiledb::sm::Datatype::DATETIME_PS:
    case tiledb::sm::Datatype::DATETIME_FS:
    case tiledb::sm::Datatype::DATETIME_AS:
    case tiledb::sm::Datatype::TIME_HR:
    case tiledb::sm::Datatype::TIME_MIN:
    case tiledb::sm::Datatype::TIME_SEC:
    case tiledb::sm::Datatype::TIME_MS:
    case tiledb::sm::Datatype::TIME_US:
    case tiledb::sm::Datatype::TIME_NS:
    case tiledb::sm::Datatype::TIME_PS:
    case tiledb::sm::Datatype::TIME_FS:
    case tiledb::sm::Datatype::TIME_AS:
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
      return Status_SerializationError(
          "Cannot set capnp scalar; unknown TileDB datatype.");
  }

  return Status::Ok();
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
Status copy_capnp_list(const CapnpT& list_reader, tiledb::sm::Buffer* buffer) {
  const auto nelts = list_reader.size();
  RETURN_NOT_OK(buffer->realloc(nelts * sizeof(T)));

  for (size_t i = 0; i < nelts; i++) {
    T val = list_reader[i];
    RETURN_NOT_OK(buffer->write(&val, sizeof(val)));
  }

  return Status::Ok();
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
Status copy_capnp_list(
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
    case tiledb::sm::Datatype::BLOB:
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
    case tiledb::sm::Datatype::DATETIME_YEAR:
    case tiledb::sm::Datatype::DATETIME_MONTH:
    case tiledb::sm::Datatype::DATETIME_WEEK:
    case tiledb::sm::Datatype::DATETIME_DAY:
    case tiledb::sm::Datatype::DATETIME_HR:
    case tiledb::sm::Datatype::DATETIME_MIN:
    case tiledb::sm::Datatype::DATETIME_SEC:
    case tiledb::sm::Datatype::DATETIME_MS:
    case tiledb::sm::Datatype::DATETIME_US:
    case tiledb::sm::Datatype::DATETIME_NS:
    case tiledb::sm::Datatype::DATETIME_PS:
    case tiledb::sm::Datatype::DATETIME_FS:
    case tiledb::sm::Datatype::DATETIME_AS:
    case tiledb::sm::Datatype::TIME_HR:
    case tiledb::sm::Datatype::TIME_MIN:
    case tiledb::sm::Datatype::TIME_SEC:
    case tiledb::sm::Datatype::TIME_MS:
    case tiledb::sm::Datatype::TIME_US:
    case tiledb::sm::Datatype::TIME_NS:
    case tiledb::sm::Datatype::TIME_PS:
    case tiledb::sm::Datatype::TIME_FS:
    case tiledb::sm::Datatype::TIME_AS:
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
      return Status_SerializationError(
          "Cannot copy capnp list; unhandled TileDB datatype.");
  }

  return Status::Ok();
}

/** Serializes the given array's nonEmptyDomain into the given Capnp builder.
 *
 * @tparam CapnpT Capnp builder type
 * @param builder Builder to set subarray onto
 * @param array Array to get nonEmptyDomain from
 * @return Status
 */
template <typename CapnpT>
Status serialize_non_empty_domain(CapnpT& builder, tiledb::sm::Array* array) {
  auto&& [st, nonEmptyDomain_opt] = array->non_empty_domain();

  RETURN_NOT_OK(st);

  if (nonEmptyDomain_opt.has_value()) {
    const auto& nonEmptyDomain = nonEmptyDomain_opt.value();
    if (!nonEmptyDomain.empty()) {
      auto nonEmptyDomainListBuilder =
          builder.initNonEmptyDomains(array->array_schema_latest().dim_num());

      for (uint64_t dimIdx = 0; dimIdx < nonEmptyDomain.size(); ++dimIdx) {
        const auto& dimNonEmptyDomain = nonEmptyDomain[dimIdx];

        auto dim_builder = nonEmptyDomainListBuilder[dimIdx];
        dim_builder.setIsEmpty(dimNonEmptyDomain.empty());
        auto range_start_sizes = dim_builder.initSizes(1);

        if (!dimNonEmptyDomain.empty()) {
          auto subarray_builder = dim_builder.initNonEmptyDomain();
          RETURN_NOT_OK(utils::set_capnp_array_ptr(
              subarray_builder,
              tiledb::sm::Datatype::UINT8,
              dimNonEmptyDomain.data(),
              dimNonEmptyDomain.size()));

          if (dimNonEmptyDomain.start_size() != 0) {
            range_start_sizes.set(0, dimNonEmptyDomain.start_size());
          }
        }
      }
    }
  }
  return Status::Ok();
}

/** Deserializes the given from Capnp build to array's nonEmptyDomain
 *
 * @tparam CapnpT Capnp builder type
 * @param builder Builder to get nonEmptyDomain from
 * @param array Array to set the nonEmptyDomain on
 * @return Status
 */
template <typename CapnpT>
Status deserialize_non_empty_domain(CapnpT& reader, tiledb::sm::Array* array) {
  capnp::NonEmptyDomainList::Reader r =
      (capnp::NonEmptyDomainList::Reader)reader;

  NDRange ndRange;
  if (r.hasNonEmptyDomains() && r.getNonEmptyDomains().size() > 0) {
    auto nonEmptyDomains = r.getNonEmptyDomains();

    for (uint32_t i = 0; i < nonEmptyDomains.size(); i++) {
      Range range;
      auto nonEmptyDomainObj = nonEmptyDomains[i];
      // We always store nonEmptyDomain as uint8 lists for the heterogeneous/var
      // length version
      auto list = nonEmptyDomainObj.getNonEmptyDomain().getUint8();
      std::vector<uint8_t> vec(list.size());
      for (uint32_t index = 0; index < list.size(); index++) {
        vec[index] = list[index];
      }

      if (nonEmptyDomainObj.hasSizes()) {
        auto sizes = nonEmptyDomainObj.getSizes();
        range.set_range(vec.data(), vec.size(), sizes[0]);
      } else {
        range.set_range(vec.data(), vec.size());
      }

      ndRange.emplace_back(range);
    }
  }

  array->set_non_empty_domain(ndRange);

  return Status::Ok();
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
Status serialize_subarray(
    CapnpT& builder,
    const tiledb::sm::ArraySchema& array_schema,
    const void* subarray) {
  // Check coords type
  auto dim_num = array_schema.dim_num();
  uint64_t subarray_size = 0;
  Datatype first_dimension_datatype = array_schema.dimension(0)->type();
  // If all the dimensions are the same datatype, then we will store the
  // subarray in a type array for <=1.7 compatibility
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dimension = array_schema.dimension(d);
    const auto coords_type = dimension->type();

    if (coords_type != first_dimension_datatype) {
      return Status_SerializationError(
          "Subarray dimension datatypes must be homogeneous");
    }

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
        return LOG_STATUS(Status_SerializationError(
            "Cannot serialize subarray; unsupported domain type."));
      default:
        break;
    }
    subarray_size += 2 * dimension->coord_size();
  }

  // Store subarray in typed array for backwards compatibility with 1.7/1.6
  const uint64_t subarray_length =
      subarray_size / datatype_size(first_dimension_datatype);
  RETURN_NOT_OK(set_capnp_array_ptr(
      builder, first_dimension_datatype, subarray, subarray_length));

  return Status::Ok();
}

template <typename CapnpT>
Status deserialize_subarray(
    const CapnpT& reader,
    const tiledb::sm::ArraySchema& array_schema,
    void** subarray) {
  // Check coords type
  auto dim_num = array_schema.dim_num();
  uint64_t subarray_size = 0;
  Datatype first_dimension_datatype = array_schema.dimension(0)->type();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dimension = array_schema.dimension(d);
    const auto coords_type = dimension->type();

    if (coords_type != first_dimension_datatype) {
      return Status_SerializationError(
          "Subarray dimension datatypes must be homogeneous");
    }

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
        return LOG_STATUS(Status_SerializationError(
            "Cannot deserialize subarray; unsupported domain type."));
      default:
        break;
    }
    subarray_size += 2 * dimension->coord_size();
  }

  tiledb::sm::Buffer subarray_buff;
  // Subarrays only work on homogeneous dimensions so use first dimension
  // datatype to copy from
  RETURN_NOT_OK(
      copy_capnp_list(reader, first_dimension_datatype, &subarray_buff));

  if (subarray_buff.size() == 0) {
    *subarray = nullptr;
  } else {
    *subarray = tdb_malloc(subarray_size);
    std::memcpy(*subarray, subarray_buff.data(), subarray_size);
  }

  return Status::Ok();
}

/**
 * Serializes the given arbitrarily typed coordinates into the given Capnp
 * builder.
 *
 * @tparam CapnpT Capnp builder type
 * @param builder Builder to set subarray onto
 * @param dimension dimension of coordinates
 * @param subarray Subarray
 * @return Status
 */
template <typename CapnpT>
Status serialize_coords(
    CapnpT& builder,
    const tiledb::sm::Dimension* dimension,
    const void* subarray) {
  // Check coords type
  uint64_t subarray_size = 2 * dimension->coord_size();

  // Store subarray in typed array
  const uint64_t subarray_length =
      subarray_size / datatype_size(dimension->type());
  RETURN_NOT_OK(set_capnp_array_ptr(
      builder, dimension->type(), subarray, subarray_length));

  return Status::Ok();
}

template <typename CapnpT>
Status deserialize_coords(
    const CapnpT& reader,
    const tiledb::sm::Dimension* dimension,
    void** subarray) {
  // Check coords type
  uint64_t subarray_size = 2 * dimension->coord_size();

  tiledb::sm::Buffer subarray_buff;
  RETURN_NOT_OK(copy_capnp_list(reader, dimension->type(), &subarray_buff));

  if (subarray_buff.size() == 0) {
    *subarray = nullptr;
  } else {
    *subarray = tdb_malloc(subarray_size);
    std::memcpy(*subarray, subarray_buff.data(), subarray_size);
  }

  return Status::Ok();
}

}  // namespace utils
}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION

#endif  // TILEDB_CAPNP_UTILS_H
