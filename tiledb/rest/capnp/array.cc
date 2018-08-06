/**
 * @file   array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines serialization for
 * tiledb::sm::Attribute/Dimension/Domain/ArraySchema
 */

#include "tiledb/rest/capnp/array.h"
#include "capnp/compat/json.h"
#include "capnp/serialize.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"

namespace tiledb {
namespace rest {
tiledb::sm::Status attribute_to_capnp(
    const tiledb::sm::Attribute* a, Attribute::Builder* attributeBuilder) {
  STATS_FUNC_IN(serialization_attribute_to_capnp);
  if (a != nullptr) {
    attributeBuilder->setName(a->name());
    attributeBuilder->setType(tiledb::sm::datatype_str(a->type()));
    attributeBuilder->setCompressor(
        tiledb::sm::compressor_str(a->compressor()));
    attributeBuilder->setCompressorLevel(a->compression_level());
    attributeBuilder->setCellValNum(a->cell_val_num());
    return tiledb::sm::Status::Ok();
  }
  return tiledb::sm::Status::Error("Attribute passed was null");
  STATS_FUNC_OUT(serialization_attribute_to_capnp);
}

std::unique_ptr<tiledb::sm::Attribute> attribute_from_capnp(
    Attribute::Reader* attribute) {
  STATS_FUNC_IN(serialization_attribute_from_capnp);
  // Set initial value to avoid compiler warnings
  tiledb::sm::Datatype datatype = tiledb::sm::Datatype::ANY;
  auto st = tiledb::sm::datatype_enum(attribute->getType().cStr(), &datatype);
  if (!st.ok()) {
    LOG_STATUS(st);
    return static_cast<std::unique_ptr<tiledb::sm::Attribute>>(nullptr);
  }
  std::unique_ptr<tiledb::sm::Attribute> a =
      std::unique_ptr<tiledb::sm::Attribute>(
          new tiledb::sm::Attribute(attribute->getName(), datatype));

  // Set initial value to avoid compiler warnings
  tiledb::sm::Compressor compressor = tiledb::sm::Compressor::NO_COMPRESSION;
  st = tiledb::sm::compressor_enum(
      attribute->getCompressor().cStr(), &compressor);
  if (!st.ok()) {
    LOG_STATUS(st);
    return static_cast<std::unique_ptr<tiledb::sm::Attribute>>(nullptr);
  }
  a->set_compressor(compressor);
  a->set_compression_level(attribute->getCompressorLevel());
  st = a->set_cell_val_num(attribute->getCellValNum());
  if (!st.ok()) {
    LOG_STATUS(st);
    return static_cast<std::unique_ptr<tiledb::sm::Attribute>>(nullptr);
  }
  return a;
  STATS_FUNC_OUT(serialization_attribute_from_capnp);
}

tiledb::sm::Status dimension_to_capnp(
    const tiledb::sm::Dimension* d, Dimension::Builder* dimensionBuilder) {
  STATS_FUNC_IN(serialization_dimension_to_capnp);
  if (d != nullptr) {
    dimensionBuilder->setName(d->name());
    dimensionBuilder->setType(tiledb::sm::datatype_str(d->type()));
    dimensionBuilder->setNullTileExtent(d->tile_extent() == nullptr);
    Dimension::Domain::Builder domain = dimensionBuilder->initDomain();
    Dimension::TileExtent::Builder tile_extent =
        dimensionBuilder->initTileExtent();
    switch (d->type()) {
      case tiledb::sm::Datatype::INT8:
        domain.setInt8(kj::arrayPtr(static_cast<int8_t*>(d->domain()), 2));
        if (d->tile_extent() != nullptr)
          tile_extent.setInt8(*static_cast<int8_t*>(d->tile_extent()));
        break;
      case tiledb::sm::Datatype::UINT8:
        domain.setUint8(kj::arrayPtr(static_cast<uint8_t*>(d->domain()), 2));
        if (d->tile_extent() != nullptr)
          tile_extent.setUint8(*static_cast<uint8_t*>(d->tile_extent()));
        break;
      case tiledb::sm::Datatype::INT16:
        domain.setInt16(kj::arrayPtr(static_cast<int16_t*>(d->domain()), 2));
        if (d->tile_extent() != nullptr)
          tile_extent.setInt16(*static_cast<int16_t*>(d->tile_extent()));
        break;
      case tiledb::sm::Datatype::UINT16:
        domain.setUint16(kj::arrayPtr(static_cast<uint16_t*>(d->domain()), 2));
        if (d->tile_extent() != nullptr)
          tile_extent.setUint16(*static_cast<uint16_t*>(d->tile_extent()));
        break;
      case tiledb::sm::Datatype::INT32:
        domain.setInt32(kj::arrayPtr(static_cast<int32_t*>(d->domain()), 2));
        if (d->tile_extent() != nullptr)
          tile_extent.setInt32(*static_cast<int32_t*>(d->tile_extent()));
        break;
      case tiledb::sm::Datatype::UINT32:
        domain.setUint32(kj::arrayPtr(static_cast<uint32_t*>(d->domain()), 2));
        if (d->tile_extent() != nullptr)
          tile_extent.setUint32(*static_cast<uint32_t*>(d->tile_extent()));
        break;
      case tiledb::sm::Datatype::INT64:
        domain.setInt64(kj::arrayPtr(static_cast<int64_t*>(d->domain()), 2));
        if (d->tile_extent() != nullptr)
          tile_extent.setInt64(*static_cast<int64_t*>(d->tile_extent()));
        break;
      case tiledb::sm::Datatype::UINT64:
        domain.setUint64(kj::arrayPtr(static_cast<uint64_t*>(d->domain()), 2));
        if (d->tile_extent() != nullptr)
          tile_extent.setUint64(*static_cast<uint64_t*>(d->tile_extent()));
        break;
      case tiledb::sm::Datatype::FLOAT32:
        domain.setFloat32(kj::arrayPtr(static_cast<float*>(d->domain()), 2));
        if (d->tile_extent() != nullptr)
          tile_extent.setFloat32(*static_cast<float*>(d->tile_extent()));
        break;
      case tiledb::sm::Datatype::FLOAT64:
        domain.setFloat64(kj::arrayPtr(static_cast<double*>(d->domain()), 2));
        if (d->tile_extent() != nullptr)
          tile_extent.setFloat64(*static_cast<double*>(d->tile_extent()));
        break;
      default:
        break;
    }
    return tiledb::sm::Status::Ok();
  }
  return tiledb::sm::Status::Error("Dimension passed was null");
  STATS_FUNC_OUT(serialization_dimension_to_capnp);
}

std::unique_ptr<tiledb::sm::Dimension> dimension_from_capnp(
    Dimension::Reader* dimension) {
  STATS_FUNC_IN(serialization_dimension_from_capnp);
  tiledb::sm::Status st;
  // Set initial value to avoid compiler warnings
  tiledb::sm::Datatype datatype = tiledb::sm::Datatype::ANY;
  st = tiledb::sm::datatype_enum(dimension->getType().cStr(), &datatype);
  if (!st.ok()) {
    LOG_STATUS(st);
    return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
  }
  std::unique_ptr<tiledb::sm::Dimension> d =
      std::unique_ptr<tiledb::sm::Dimension>(
          new tiledb::sm::Dimension(dimension->getName(), datatype));

  switch (d->type()) {
    case tiledb::sm::Datatype::INT8: {
      auto domainList = dimension->getDomain().getInt8();
      std::vector<int8_t> tmpDomain(domainList.size());
      for (size_t i = 0; i < domainList.size(); i++)
        tmpDomain[i] = domainList[i];
      st = d->set_domain((void*)tmpDomain.data());
      if (!st.ok()) {
        LOG_STATUS(st);
        return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
      }
      if (!dimension->getNullTileExtent()) {
        int8_t extent = dimension->getTileExtent().getInt8();
        st = d->set_tile_extent((void*)&extent);
        if (!st.ok()) {
          LOG_STATUS(st);
          return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
        }
      }
      break;
    }
    case tiledb::sm::Datatype::UINT8: {
      auto domainList = dimension->getDomain().getUint8();
      std::vector<uint8_t> tmpDomain(domainList.size());
      for (size_t i = 0; i < domainList.size(); i++)
        tmpDomain[i] = domainList[i];
      st = d->set_domain((void*)tmpDomain.data());
      if (!st.ok()) {
        LOG_STATUS(st);
        return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
      }
      if (!dimension->getNullTileExtent()) {
        uint8_t extent = dimension->getTileExtent().getUint8();
        st = d->set_tile_extent((void*)&extent);
        if (!st.ok()) {
          LOG_STATUS(st);
          return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
        }
      }
      break;
    }
    case tiledb::sm::Datatype::INT16: {
      auto domainList = dimension->getDomain().getInt16();
      std::vector<int16_t> tmpDomain(domainList.size());
      for (size_t i = 0; i < domainList.size(); i++)
        tmpDomain[i] = domainList[i];
      st = d->set_domain((void*)tmpDomain.data());
      if (!st.ok()) {
        LOG_STATUS(st);
        return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
      }
      if (!dimension->getNullTileExtent()) {
        int16_t extent = dimension->getTileExtent().getInt16();
        st = d->set_tile_extent((void*)&extent);
        if (!st.ok()) {
          LOG_STATUS(st);
          return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
        }
      }
      break;
    }
    case tiledb::sm::Datatype::UINT16: {
      auto domainList = dimension->getDomain().getUint16();
      std::vector<uint16_t> tmpDomain(domainList.size());
      for (size_t i = 0; i < domainList.size(); i++)
        tmpDomain[i] = domainList[i];
      st = d->set_domain((void*)tmpDomain.data());
      if (!st.ok()) {
        LOG_STATUS(st);
        return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
      }
      if (!dimension->getNullTileExtent()) {
        uint16_t extent = dimension->getTileExtent().getUint16();
        st = d->set_tile_extent((void*)&extent);
        if (!st.ok()) {
          LOG_STATUS(st);
          return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
        }
      }
      break;
    }
    case tiledb::sm::Datatype::INT32: {
      auto domainList = dimension->getDomain().getInt32();
      std::vector<int32_t> tmpDomain(domainList.size());
      for (size_t i = 0; i < domainList.size(); i++)
        tmpDomain[i] = domainList[i];
      st = d->set_domain((void*)tmpDomain.data());
      if (!st.ok()) {
        LOG_STATUS(st);
        return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
      }
      if (!dimension->getNullTileExtent()) {
        int32_t extent = dimension->getTileExtent().getInt32();
        st = d->set_tile_extent((void*)&extent);
        if (!st.ok()) {
          LOG_STATUS(st);
          return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
        }
      }
      break;
    }
    case tiledb::sm::Datatype::UINT32: {
      auto domainList = dimension->getDomain().getUint32();
      std::vector<uint32_t> tmpDomain(domainList.size());
      for (size_t i = 0; i < domainList.size(); i++)
        tmpDomain[i] = domainList[i];
      st = d->set_domain((void*)tmpDomain.data());
      if (!st.ok()) {
        LOG_STATUS(st);
        return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
      }
      if (!dimension->getNullTileExtent()) {
        uint32_t extent = dimension->getTileExtent().getUint32();
        st = d->set_tile_extent((void*)&extent);
        if (!st.ok()) {
          LOG_STATUS(st);
          return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
        }
      }
      break;
    }
    case tiledb::sm::Datatype::INT64: {
      auto domainList = dimension->getDomain().getInt64();
      std::vector<int64_t> tmpDomain(domainList.size());
      for (size_t i = 0; i < domainList.size(); i++)
        tmpDomain[i] = domainList[i];
      st = d->set_domain((void*)tmpDomain.data());
      if (!st.ok()) {
        LOG_STATUS(st);
        return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
      }
      if (!dimension->getNullTileExtent()) {
        int64_t extent = dimension->getTileExtent().getInt64();
        st = d->set_tile_extent((void*)&extent);
        if (!st.ok()) {
          LOG_STATUS(st);
          return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
        }
      }
      break;
    }
    case tiledb::sm::Datatype::UINT64: {
      auto domainList = dimension->getDomain().getUint64();
      std::vector<uint64_t> tmpDomain(domainList.size());
      for (size_t i = 0; i < domainList.size(); i++)
        tmpDomain[i] = domainList[i];
      st = d->set_domain((void*)tmpDomain.data());
      if (!st.ok()) {
        LOG_STATUS(st);
        return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
      }
      if (!dimension->getNullTileExtent()) {
        uint64_t extent = dimension->getTileExtent().getUint64();
        st = d->set_tile_extent((void*)&extent);
        if (!st.ok()) {
          LOG_STATUS(st);
          return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
        }
      }
      break;
    }
    case tiledb::sm::Datatype::FLOAT32: {
      auto domainList = dimension->getDomain().getFloat32();
      std::vector<float> tmpDomain(domainList.size());
      for (size_t i = 0; i < domainList.size(); i++)
        tmpDomain[i] = domainList[i];
      st = d->set_domain((void*)tmpDomain.data());
      if (!st.ok()) {
        LOG_STATUS(st);
        return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
      }
      if (!dimension->getNullTileExtent()) {
        float extent = dimension->getTileExtent().getFloat32();
        st = d->set_tile_extent((void*)&extent);
        if (!st.ok()) {
          LOG_STATUS(st);
          return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
        }
      }
      break;
    }
    case tiledb::sm::Datatype::FLOAT64: {
      auto domainList = dimension->getDomain().getFloat64();
      std::vector<double> tmpDomain(domainList.size());
      for (size_t i = 0; i < domainList.size(); i++)
        tmpDomain[i] = domainList[i];
      st = d->set_domain((void*)tmpDomain.data());
      if (!st.ok()) {
        LOG_STATUS(st);
        return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
      }
      if (!dimension->getNullTileExtent()) {
        double extent = dimension->getTileExtent().getFloat64();
        st = d->set_tile_extent((void*)&extent);
        if (!st.ok()) {
          LOG_STATUS(st);
          return static_cast<std::unique_ptr<tiledb::sm::Dimension>>(nullptr);
        }
      }
      break;
    }
    default:
      break;
  }
  return d;
  STATS_FUNC_OUT(serialization_dimension_from_capnp);
}

tiledb::sm::Status domain_to_capnp(
    const tiledb::sm::Domain* d, Domain::Builder* domainBuilder) {
  STATS_FUNC_IN(serialization_domain_to_capnp);
  if (d != nullptr) {
    domainBuilder->setType(tiledb::sm::datatype_str(d->type()));
    domainBuilder->setTileOrder(tiledb::sm::layout_str(d->tile_order()));
    domainBuilder->setCellOrder(tiledb::sm::layout_str(d->cell_order()));
    capnp::List<Dimension>::Builder dimensions =
        domainBuilder->initDimensions(d->dim_num());
    for (unsigned int i = 0; i < d->dim_num(); i++) {
      Dimension::Builder dimensionBuilder = dimensions[i];
      auto status = dimension_to_capnp(d->dimension(i), &dimensionBuilder);
      if (!status.ok())
        return status;
    }
    return tiledb::sm::Status::Ok();
  }
  return tiledb::sm::Status::Error("Domain passed was null");
  STATS_FUNC_OUT(serialization_domain_to_capnp);
}

std::unique_ptr<tiledb::sm::Domain> domain_from_capnp(Domain::Reader* domain) {
  STATS_FUNC_IN(serialization_domain_from_capnp);
  // Set initial value to avoid compiler warnings
  tiledb::sm::Datatype datatype = tiledb::sm::Datatype::ANY;
  auto st = tiledb::sm::datatype_enum(domain->getType().cStr(), &datatype);
  if (!st.ok()) {
    LOG_STATUS(st);
    return static_cast<std::unique_ptr<tiledb::sm::Domain>>(nullptr);
  }
  std::unique_ptr<tiledb::sm::Domain> d =
      std::unique_ptr<tiledb::sm::Domain>(new tiledb::sm::Domain(datatype));
  auto dimensions = domain->getDimensions();
  for (auto dimension : dimensions) {
    auto dim = dimension_from_capnp(&dimension);
    if (dim == nullptr) {
      LOG_STATUS(tiledb::sm::Status::Error(
          "Could not deserialize dimension from domain"));
      return static_cast<std::unique_ptr<tiledb::sm::Domain>>(nullptr);
    }
    st = d->add_dimension(dim.get());
    if (!st.ok()) {
      LOG_STATUS(st);
      return static_cast<std::unique_ptr<tiledb::sm::Domain>>(nullptr);
    }
  }
  return d;
  STATS_FUNC_OUT(serialization_domain_from_capnp);
}

tiledb::sm::Status array_schema_to_capnp(
    const tiledb::sm::ArraySchema* a,
    ArraySchema::Builder* arraySchemaBuilder) {
  STATS_FUNC_IN(serialization_array_schema_to_capnp);
  if (a != nullptr) {
    arraySchemaBuilder->setVersion(
        kj::arrayPtr(tiledb::sm::constants::version, 3));
    arraySchemaBuilder->setArrayType(
        tiledb::sm::array_type_str(a->array_type()));
    arraySchemaBuilder->setTileOrder(tiledb::sm::layout_str(a->tile_order()));
    arraySchemaBuilder->setCellOrder(tiledb::sm::layout_str(a->cell_order()));
    arraySchemaBuilder->setCapacity(a->capacity());
    arraySchemaBuilder->setCoordsCompression(
        tiledb::sm::compressor_str(a->coords_compression()));
    arraySchemaBuilder->setCoordsCompressionLevel(
        a->coords_compression_level());
    arraySchemaBuilder->setOffsetCompression(
        tiledb::sm::compressor_str(a->cell_var_offsets_compression()));
    arraySchemaBuilder->setOffsetCompressionLevel(
        a->cell_var_offsets_compression_level());
    arraySchemaBuilder->setUri(a->array_uri().to_string());

    Domain::Builder domainBuilder = arraySchemaBuilder->initDomain();
    auto status = domain_to_capnp(a->domain(), &domainBuilder);
    if (!status.ok())
      return status;

    capnp::List<Attribute>::Builder attributes =
        arraySchemaBuilder->initAttributes(a->attribute_num());

    for (size_t i = 0; i < a->attribute_num(); i++) {
      ::Attribute::Builder attributeBuilder = attributes[i];
      status = attribute_to_capnp(a->attribute(i), &attributeBuilder);
      if (!status.ok())
        return status;
    }
    return tiledb::sm::Status::Ok();
  }
  return tiledb::sm::Status::Error("ArraySchema passed was null");
  STATS_FUNC_OUT(serialization_array_schema_to_capnp);
}

std::unique_ptr<tiledb::sm::ArraySchema> array_schema_from_capnp(
    ArraySchema::Reader* arraySchema) {
  STATS_FUNC_IN(serialization_array_schema_from_capnp);
  tiledb::sm::Status st;
  // Set initial value to avoid compiler warnings
  tiledb::sm::ArrayType array_type = tiledb::sm::ArrayType::DENSE;
  st = tiledb::sm::array_type_enum(
      arraySchema->getArrayType().cStr(), &array_type);
  if (!st.ok()) {
    LOG_STATUS(st);
    return static_cast<std::unique_ptr<tiledb::sm::ArraySchema>>(nullptr);
  }
  std::unique_ptr<tiledb::sm::ArraySchema> a =
      std::unique_ptr<tiledb::sm::ArraySchema>(
          new tiledb::sm::ArraySchema(array_type));

  if (arraySchema->getUri() != "")
    a->set_array_uri(tiledb::sm::URI(arraySchema->getUri().cStr()));
  Domain::Reader domain = arraySchema->getDomain();
  std::unique_ptr<tiledb::sm::Domain> d = domain_from_capnp(&domain);
  st = a->set_domain(d.get());
  // Log if domain did not get set correctly
  if (!st.ok()) {
    LOG_STATUS(st);
    return static_cast<std::unique_ptr<tiledb::sm::ArraySchema>>(nullptr);
  }

  // Set initial value to avoid compiler warnings
  tiledb::sm::Layout layout = tiledb::sm::Layout::ROW_MAJOR;
  st = tiledb::sm::layout_enum(arraySchema->getTileOrder().cStr(), &layout);
  if (!st.ok()) {
    LOG_STATUS(st);
    return static_cast<std::unique_ptr<tiledb::sm::ArraySchema>>(nullptr);
  }
  a->set_tile_order(layout);

  st = tiledb::sm::layout_enum(arraySchema->getCellOrder().cStr(), &layout);
  if (!st.ok()) {
    LOG_STATUS(st);
    return static_cast<std::unique_ptr<tiledb::sm::ArraySchema>>(nullptr);
  }
  a->set_cell_order(layout);
  a->set_capacity(arraySchema->getCapacity());

  // Set initial value to avoid compiler warnings
  tiledb::sm::Compressor compressor = tiledb::sm::Compressor::NO_COMPRESSION;
  st = tiledb::sm::compressor_enum(
      arraySchema->getCoordsCompression().cStr(), &compressor);
  if (!st.ok()) {
    LOG_STATUS(st);
    return static_cast<std::unique_ptr<tiledb::sm::ArraySchema>>(nullptr);
  }
  a->set_coords_compressor(compressor);
  a->set_coords_compression_level(arraySchema->getCoordsCompressionLevel());

  st = tiledb::sm::compressor_enum(
      arraySchema->getOffsetCompression().cStr(), &compressor);
  if (!st.ok()) {
    LOG_STATUS(st);
    return static_cast<std::unique_ptr<tiledb::sm::ArraySchema>>(nullptr);
  }
  a->set_cell_var_offsets_compressor(compressor);
  a->set_cell_var_offsets_compression_level(
      arraySchema->getOffsetCompressionLevel());

  capnp::List<Attribute>::Reader attributes = arraySchema->getAttributes();
  for (auto attr : attributes) {
    auto attribute = attribute_from_capnp(&attr);
    if (attribute->name().substr(0, 2) !=
        tiledb::sm::constants::special_name_prefix) {
      st = a->add_attribute(attribute.get());
      if (!st.ok()) {
        LOG_STATUS(st);
        return static_cast<std::unique_ptr<tiledb::sm::ArraySchema>>(nullptr);
      }
    }
  }

  st = a->init();
  if (!st.ok()) {
    LOG_STATUS(st);
    return static_cast<std::unique_ptr<tiledb::sm::ArraySchema>>(nullptr);
  }

  return a;
  STATS_FUNC_OUT(serialization_array_schema_from_capnp);
}
tiledb::sm::Status array_schema_serialize(
    tiledb::sm::ArraySchema* array_schema,
    tiledb::sm::SerializationType serialize_type,
    char** serialized_string,
    uint64_t* serialized_string_length) {
  STATS_FUNC_IN(serialization_array_schema_serialize);
  try {
    capnp::MallocMessageBuilder message;
    ArraySchema::Builder arraySchemaBuilder = message.initRoot<ArraySchema>();
    tiledb::sm::Status status =
        array_schema_to_capnp(array_schema, &arraySchemaBuilder);

    if (!status.ok())
      return tiledb::sm::Status::Error(
          "Could not serialize array_schema: " + status.to_string());

    switch (serialize_type) {
      case tiledb::sm::SerializationType::JSON: {
        capnp::JsonCodec json;
        kj::String capnp_json = json.encode(arraySchemaBuilder);
        // size does not include needed null terminator, so add +1
        *serialized_string_length = capnp_json.size() + 1;
        *serialized_string = new char[*serialized_string_length];
        strcpy(*serialized_string, capnp_json.cStr());
        break;
      }
      case tiledb::sm::SerializationType::CAPNP: {
        kj::Array<capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        *serialized_string = new char[message_chars.size()];
        memcpy(*serialized_string, message_chars.begin(), message_chars.size());
        *serialized_string_length = message_chars.size();
        break;
      }
      default: {
        return tiledb::sm::Status::Error("Unknown serialization type passed");
      }
    }

  } catch (kj::Exception& e) {
    return tiledb::sm::Status::Error(
        std::string("Error serializing array schema: ") +
        e.getDescription().cStr());
  } catch (std::exception& e) {
    return tiledb::sm::Status::Error(
        std::string("Error serializing array schema: ") + e.what());
  }
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_array_schema_serialize);
}

tiledb::sm::Status array_schema_deserialize(
    tiledb::sm::ArraySchema** array_schema,
    tiledb::sm::SerializationType serialize_type,
    const char* serialized_string,
    const uint64_t serialized_string_length) {
  STATS_FUNC_IN(serialization_array_schema_deserialize);
  try {
    std::unique_ptr<tiledb::sm::ArraySchema> decoded_array_schema = nullptr;

    switch (serialize_type) {
      case tiledb::sm::SerializationType::JSON: {
        capnp::JsonCodec json;
        capnp::MallocMessageBuilder message_builder;
        ::ArraySchema::Builder array_schema_builder =
            message_builder.initRoot<::ArraySchema>();
        json.decode(kj::StringPtr(serialized_string), array_schema_builder);
        ::ArraySchema::Reader array_schema_reader =
            array_schema_builder.asReader();
        decoded_array_schema =
            tiledb::rest::array_schema_from_capnp(&array_schema_reader);
        break;
      }
      case tiledb::sm::SerializationType::CAPNP: {
        const kj::byte* mBytes =
            reinterpret_cast<const kj::byte*>(serialized_string);
        capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const capnp::word*>(mBytes),
            serialized_string_length / sizeof(capnp::word)));
        // capnp::FlatArrayMessageReader reader(kj::arrayPtr<const
        // kj::byte>(reinterpret_cast<const kj::byte*>(serialized_string),
        // serialized_string_length));

        ArraySchema::Reader array_schema_reader =
            reader.getRoot<::ArraySchema>();
        decoded_array_schema =
            tiledb::rest::array_schema_from_capnp(&array_schema_reader);

        break;
      }
      default: {
        return tiledb::sm::Status::Error("Unknown serialization type passed");
      }
    }

    if (decoded_array_schema == nullptr) {
      return tiledb::sm::Status::Error(
          "Failed to deserialize TileDB array schema object");
    }

    // Create array schema struct
    *array_schema = decoded_array_schema.release();
    if (*array_schema == nullptr) {
      return tiledb::sm::Status::Error(
          "Failed to allocate TileDB array schema object");
    }

  } catch (kj::Exception& e) {
    return tiledb::sm::Status::Error(
        std::string("Error serializing array schema: ") +
        e.getDescription().cStr());
  } catch (std::exception& e) {
    return tiledb::sm::Status::Error(
        std::string("Error serializing array schema: ") + e.what());
  }
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_array_schema_deserialize);
}
}  // namespace rest
}  // namespace tiledb
