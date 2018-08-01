/**
 * @file   array.h
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
 * This file defines json serialization adls for use with nlohmann json.
 * Each serialization (to_json) /de-serialization (from_json) is wrapped
 * inside a struct for the give type.
 * All serializers are inside the nlohmann namespace
 */

#ifndef TILEDB_JSON_ARRAY_H
#define TILEDB_JSON_ARRAY_H

#include "nlohmann/json.hpp"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"

namespace nlohmann {
template <>
struct adl_serializer<const tiledb::sm::Attribute*> {
  /*
   * Implement json serialization for attribute
   *
   * @param j json object to store serialized data in
   * @param a attribute pointer to serialize
   */
  static void to_json(json& j, const tiledb::sm::Attribute* a) {
    STATS_FUNC_VOID_IN(serialization_attribute_to_json);
    if (a != nullptr)
      j = json{{"name", a->name()},
               {"type", datatype_str(a->type())},
               {"compressor", compressor_str(a->compressor())},
               {"compressor_level", a->compression_level()},
               {"cell_val_num", a->cell_val_num()}};
    else
      j = nullptr;
    STATS_FUNC_VOID_OUT(serialization_attribute_to_json);
  }
};

template <>
struct adl_serializer<tiledb::sm::Attribute> {
  /*
   * Implement json serialization for attribute
   *
   * @param j json object to store serialized data in
   * @param a attribute object to serialize
   */
  void to_json(json& j, const tiledb::sm::Attribute& a) {
    STATS_FUNC_VOID_IN(serialization_attribute_to_json);
    // Set j using pointer to_json
    j = &a;
    STATS_FUNC_VOID_OUT(serialization_attribute_to_json);
  }
  /*
   * Implement json de-serialization for attribute
   *
   * @param j json object
   * @param a attribute object to store de-serializated object in
   */
  static void from_json(const json& j, tiledb::sm::Attribute& a) {
    STATS_FUNC_VOID_IN(serialization_attribute_from_json);
    // Datatype to parse, set default to avoid compiler uninitialized warnings
    tiledb::sm::Datatype datatype = tiledb::sm::Datatype::ANY;
    tiledb::sm::Status status =
        tiledb::sm::datatype_enum(j.at("type"), &datatype);
    if (!status.ok())
      throw std::runtime_error(status.to_string());
    a = tiledb::sm::Attribute(j.at("name"), datatype);

    // Compressor to parse, set default to avoid compiler uninitialized warnings
    tiledb::sm::Compressor compressor = tiledb::sm::Compressor::NO_COMPRESSION;
    status = tiledb::sm::compressor_enum(j.at("compressor"), &compressor);
    if (!status.ok())
      throw std::runtime_error(status.to_string());
    a.set_compressor(compressor);
    a.set_compression_level(j.at("compressor_level"));
    auto st = a.set_cell_val_num(j.at("cell_val_num"));
    if (!st.ok())
      LOG_STATUS(st);
    STATS_FUNC_VOID_OUT(serialization_attribute_from_json);
  }
};

template <>
struct adl_serializer<tiledb::sm::Dimension> {
  /*
   * Implement json serialization for dimension
   *
   * @param j json object to store serialized data in
   * @param d dimension object to serialize
   */
  static void to_json(json& j, const tiledb::sm::Dimension& d) {
    STATS_FUNC_VOID_IN(serialization_dimension_to_json);
    if (d.domain() == nullptr)
      return;

    j = json{
        {"name", d.name()},
        {"type", datatype_str(d.type())},
        {"tile_extent_type", datatype_str(d.type())},
        {"null_tile_extent", d.tile_extent() == nullptr},
    };
    switch (d.type()) {
      case tiledb::sm::Datatype::INT32:
        j["domain"] = std::vector<int32_t>(
            static_cast<int32_t*>(d.domain()),
            static_cast<int32_t*>(d.domain()) + 2);
        if (d.tile_extent() != nullptr)
          j["tile_extent"] = *((int32_t*)d.tile_extent());
        break;
      case tiledb::sm::Datatype::INT64:
        j["domain"] = std::vector<int64_t>(
            static_cast<int64_t*>(d.domain()),
            static_cast<int64_t*>(d.domain()) + 2);
        if (d.tile_extent() != nullptr)
          j["tile_extent"] = *((int64_t*)d.tile_extent());
        break;
      case tiledb::sm::Datatype::INT8:
        j["domain"] = std::vector<int8_t>(
            static_cast<int8_t*>(d.domain()),
            static_cast<int8_t*>(d.domain()) + 2);
        if (d.tile_extent() != nullptr)
          j["tile_extent"] = *((int8_t*)d.tile_extent());
        break;
      case tiledb::sm::Datatype::UINT8:
        j["domain"] = std::vector<uint8_t>(
            static_cast<uint8_t*>(d.domain()),
            static_cast<uint8_t*>(d.domain()) + 2);
        if (d.tile_extent() != nullptr)
          j["tile_extent"] = *((uint8_t*)d.tile_extent());
        break;
      case tiledb::sm::Datatype::INT16:
        j["domain"] = std::vector<int16_t>(
            static_cast<int16_t*>(d.domain()),
            static_cast<int16_t*>(d.domain()) + 2);
        if (d.tile_extent() != nullptr)
          j["tile_extent"] = *((int16_t*)d.tile_extent());
        break;
      case tiledb::sm::Datatype::UINT16:
        j["domain"] = std::vector<uint16_t>(
            static_cast<uint16_t*>(d.domain()),
            static_cast<uint16_t*>(d.domain()) + 2);
        if (d.tile_extent() != nullptr)
          j["tile_extent"] = *((uint16_t*)d.tile_extent());
        break;
      case tiledb::sm::Datatype::UINT32:
        j["domain"] = std::vector<uint32_t>(
            static_cast<uint32_t*>(d.domain()),
            static_cast<uint32_t*>(d.domain()) + 2);
        if (d.tile_extent() != nullptr)
          j["tile_extent"] = *((uint32_t*)d.tile_extent());
        break;
      case tiledb::sm::Datatype::UINT64:
        j["domain"] = std::vector<uint64_t>(
            static_cast<uint64_t*>(d.domain()),
            static_cast<uint64_t*>(d.domain()) + 2);
        if (d.tile_extent() != nullptr)
          j["tile_extent"] = *((uint64_t*)d.tile_extent());
        break;
      case tiledb::sm::Datatype::FLOAT32:
        j["domain"] = std::vector<float>(
            static_cast<float*>(d.domain()),
            static_cast<float*>(d.domain()) + 2);
        if (d.tile_extent() != nullptr)
          j["tile_extent"] = *((float*)d.tile_extent());
        break;
      case tiledb::sm::Datatype::FLOAT64:
        j["domain"] = std::vector<double>(
            static_cast<double*>(d.domain()),
            static_cast<double*>(d.domain()) + 2);
        if (d.tile_extent() != nullptr)
          j["tile_extent"] = *((double*)d.tile_extent());
        break;
      default:
        break;
    }
    STATS_FUNC_VOID_OUT(serialization_dimension_to_json);
  }

  /*
   * Implement json de-serialization for dimension
   *
   * @param j json object
   * @param d dimension object to store de-serializated object in
   */
  static void from_json(const json& j, tiledb::sm::Dimension& d) {
    STATS_FUNC_VOID_IN(serialization_dimension_from_json);
    // Datatype to parse, set default to avoid compiler uninitialized warnings
    tiledb::sm::Datatype datatype = tiledb::sm::Datatype::INT32;
    tiledb::sm::Status status =
        tiledb::sm::datatype_enum(j.at("type"), &datatype);
    if (!status.ok())
      throw std::runtime_error(status.to_string());
    d = tiledb::sm::Dimension(j.at("name"), datatype);

    tiledb::sm::Status st;
    switch (d.type()) {
      case tiledb::sm::Datatype::INT32: {
        std::array<int32_t, 2> tmpDomain = j.at("domain");
        st = d.set_domain((void*)tmpDomain.data());
        if (!st.ok())
          LOG_STATUS(st);
        if (!j.at("null_tile_extent")) {
          int32_t extent = j.at("tile_extent").get<int32_t>();
          st = d.set_tile_extent((void*)&extent);
          if (!st.ok())
            LOG_STATUS(st);
        }
        break;
      }
      case tiledb::sm::Datatype::INT64: {
        std::vector<int64_t> tmpDomain = j.at("domain");
        st = d.set_domain((void*)tmpDomain.data());
        if (!st.ok())
          LOG_STATUS(st);
        if (!j.at("null_tile_extent")) {
          int64_t extent = j.at("tile_extent").get<int64_t>();
          st = d.set_tile_extent((void*)&extent);
          if (!st.ok())
            LOG_STATUS(st);
        }
        break;
      }
      case tiledb::sm::Datatype::INT8: {
        std::vector<int8_t> tmpDomain = j.at("domain");
        st = d.set_domain((void*)tmpDomain.data());
        if (!st.ok())
          LOG_STATUS(st);
        if (!j.at("null_tile_extent")) {
          int8_t extent = j.at("tile_extent").get<int8_t>();
          st = d.set_tile_extent((void*)&extent);
          if (!st.ok())
            LOG_STATUS(st);
        }
        break;
      }
      case tiledb::sm::Datatype::UINT8: {
        std::vector<uint8_t> tmpDomain = j.at("domain");
        st = d.set_domain((void*)tmpDomain.data());
        if (!st.ok())
          LOG_STATUS(st);
        if (!j.at("null_tile_extent")) {
          uint8_t extent = j.at("tile_extent").get<uint8_t>();
          st = d.set_tile_extent((void*)&extent);
          if (!st.ok())
            LOG_STATUS(st);
        }
        break;
      }
      case tiledb::sm::Datatype::INT16: {
        std::vector<int16_t> tmpDomain = j.at("domain");
        st = d.set_domain((void*)tmpDomain.data());
        if (!st.ok())
          LOG_STATUS(st);
        if (!j.at("null_tile_extent")) {
          int16_t extent = j.at("tile_extent").get<int16_t>();
          st = d.set_tile_extent((void*)&extent);
          if (!st.ok())
            LOG_STATUS(st);
        }
        break;
      }
      case tiledb::sm::Datatype::UINT16: {
        std::vector<uint16_t> tmpDomain = j.at("domain");
        st = d.set_domain((void*)tmpDomain.data());
        if (!st.ok())
          LOG_STATUS(st);
        if (!j.at("null_tile_extent")) {
          uint16_t extent = j.at("tile_extent").get<uint16_t>();
          st = d.set_tile_extent((void*)&extent);
          if (!st.ok())
            LOG_STATUS(st);
        }
        break;
      }
      case tiledb::sm::Datatype::UINT32: {
        std::vector<uint32_t> tmpDomain = j.at("domain");
        st = d.set_domain((void*)tmpDomain.data());
        if (!st.ok())
          LOG_STATUS(st);
        if (!j.at("null_tile_extent")) {
          uint32_t extent = j.at("tile_extent").get<uint32_t>();
          st = d.set_tile_extent((void*)&extent);
          if (!st.ok())
            LOG_STATUS(st);
        }
        break;
      }
      case tiledb::sm::Datatype::UINT64: {
        std::vector<uint64_t> tmpDomain = j.at("domain");
        st = d.set_domain((void*)tmpDomain.data());
        if (!st.ok())
          LOG_STATUS(st);
        if (!j.at("null_tile_extent")) {
          uint64_t extent = j.at("tile_extent").get<uint64_t>();
          st = d.set_tile_extent((void*)&extent);
          if (!st.ok())
            LOG_STATUS(st);
        }
        break;
      }
      case tiledb::sm::Datatype::FLOAT32: {
        std::vector<float> tmpDomain = j.at("domain");
        st = d.set_domain((void*)tmpDomain.data());
        if (!st.ok())
          LOG_STATUS(st);
        if (!j.at("null_tile_extent")) {
          float extent = j.at("tile_extent").get<float>();
          st = d.set_tile_extent((void*)&extent);
          if (!st.ok())
            LOG_STATUS(st);
        }
        break;
      }
      case tiledb::sm::Datatype::FLOAT64: {
        std::vector<double> tmpDomain = j.at("domain");
        st = d.set_domain((void*)tmpDomain.data());
        if (!st.ok())
          LOG_STATUS(st);
        if (!j.at("null_tile_extent")) {
          double extent = j.at("tile_extent").get<double>();
          st = d.set_tile_extent((void*)&extent);
          if (!st.ok())
            LOG_STATUS(st);
        }
        break;
      }
      default:
        break;
    }
    if (!st.ok())
      LOG_STATUS(st);
    STATS_FUNC_VOID_OUT(serialization_dimension_from_json);
  }
};

template <>
struct adl_serializer<const tiledb::sm::Dimension*> {
  /*
   * Implement json serialization for dimension
   *
   * @param j json object to store serialized data in
   * @param d dimension pointer to serialize
   */
  static void to_json(json& j, const tiledb::sm::Dimension* d) {
    STATS_FUNC_VOID_IN(serialization_dimension_to_json);
    if (d != nullptr)
      j = *d;
    else
      j = nullptr;
    STATS_FUNC_VOID_OUT(serialization_dimension_to_json);
  }
};

template <>
struct adl_serializer<tiledb::sm::Domain> {
  /*
   * Implement json serialization for domain
   *
   * @param j json object to store serialized data in
   * @param d domain object to serialize
   */
  static void to_json(json& j, const tiledb::sm::Domain& d) {
    STATS_FUNC_VOID_IN(serialization_domain_to_json);
    j = json{
        {"type", datatype_str(d.type())},
        {"tile_order", layout_str(d.tile_order())},
        {"cell_order", layout_str(d.cell_order())},
    };
    auto dims = json::array();
    for (unsigned int i = 0; i < d.dim_num(); i++) {
      dims.push_back(d.dimension(i));
    }
    j["dimensions"] = dims;
    STATS_FUNC_VOID_OUT(serialization_domain_to_json);
  }

  /*
   * Implement json de-serialization for domain
   *
   * @param j json object
   * @param d domain object to store de-serializated object in
   */
  static void from_json(const json& j, tiledb::sm::Domain& d) {
    STATS_FUNC_VOID_IN(serialization_domain_from_json);
    for (auto it : j.at("dimensions").items()) {
      tiledb::sm::Dimension tmp = it.value().get<tiledb::sm::Dimension>();
      auto st = d.add_dimension(&tmp);
      if (!st.ok())
        LOG_STATUS(st);
    }
    STATS_FUNC_VOID_OUT(serialization_domain_from_json);
  }
};

template <>
struct adl_serializer<const tiledb::sm::Domain*> {
  /*
   * Implement json serialization for domain
   *
   * @param j json object to store serialized data in
   * @param d domain pointer to serialize
   */
  static void to_json(json& j, const tiledb::sm::Domain* d) {
    STATS_FUNC_VOID_IN(serialization_domain_to_json);
    if (d != nullptr)
      j = *d;
    else
      j = nullptr;
    STATS_FUNC_VOID_OUT(serialization_domain_to_json);
  }
};

template <>
struct adl_serializer<tiledb::sm::ArraySchema> {
  /*
   * Implement json serialization for ArraySchema
   *
   * @param j json object to store serialized data in
   * @param a ArraySchema object to serialize
   */
  static void to_json(json& j, const tiledb::sm::ArraySchema& a) {
    STATS_FUNC_VOID_IN(serialization_array_schema_to_json);
    j = json{
        {"version", tiledb::sm::constants::version},
        {"array_type", array_type_str(a.array_type())},
        {"tile_order", layout_str(a.tile_order())},
        {"cell_order", layout_str(a.cell_order())},
        {"capacity", a.capacity()},
        {"coords_compression", compressor_str(a.coords_compression())},
        {"coords_compression_level", a.coords_compression_level()},
        {"domain", a.domain()},
        {"offset_compression",
         compressor_str(a.cell_var_offsets_compression())},
        {"offset_compression_level", a.cell_var_offsets_compression_level()},
        {"uri", a.array_uri().to_string()}};

    auto attrs = json::array();
    for (size_t i = 0; i < a.attributes().size(); i++) {
      attrs.push_back(a.attribute(i));
    }
    j["attributes"] = attrs;
    STATS_FUNC_VOID_OUT(serialization_array_schema_to_json);
  }

  /*
   * Implement json de-serialization for ArraySchema
   *
   * @param j json object
   * @param a ArraySchema object to store de-serializated object in
   */
  static void from_json(const json& j, tiledb::sm::ArraySchema& a) {
    STATS_FUNC_VOID_IN(serialization_array_schema_from_json);
    tiledb::sm::Status st;

    // ArrayType to parse, set default to avoid compiler uninitialized warnings
    tiledb::sm::ArrayType array_type = tiledb::sm::ArrayType::DENSE;
    st = tiledb::sm::array_type_enum(j.at("array_type"), &array_type);
    if (!st.ok())
      throw std::runtime_error(st.to_string());
    // Initialize ArraySchema
    a = tiledb::sm::ArraySchema(array_type);
    a.set_array_uri(tiledb::sm::URI(j.at("uri").get<std::string>()));
    tiledb::sm::Domain d = j.at("domain").get<tiledb::sm::Domain>();
    st = a.set_domain(&d);
    // Log if domain did not get set correctly
    if (!st.ok())
      LOG_STATUS(st);

    // Layout to parse, set default to avoid compiler uninitialized warnings
    tiledb::sm::Layout layout = tiledb::sm::Layout::ROW_MAJOR;
    st = tiledb::sm::layout_enum(j.at("tile_order"), &layout);
    if (!st.ok())
      throw std::runtime_error(st.to_string());
    a.set_tile_order(layout);

    st = tiledb::sm::layout_enum(j.at("cell_order"), &layout);
    if (!st.ok())
      throw std::runtime_error(st.to_string());
    a.set_cell_order(layout);
    a.set_capacity(j.at("capacity"));

    // Compressor to parse, set default to avoid compiler uninitialized warnings
    tiledb::sm::Compressor compressor = tiledb::sm::Compressor::NO_COMPRESSION;
    st = tiledb::sm::compressor_enum(j.at("coords_compression"), &compressor);
    if (!st.ok())
      throw std::runtime_error(st.to_string());
    a.set_coords_compressor(compressor);
    a.set_coords_compression_level(j.at("coords_compression_level"));

    st = tiledb::sm::compressor_enum(j.at("offset_compression"), &compressor);
    if (!st.ok())
      throw std::runtime_error(st.to_string());
    a.set_cell_var_offsets_compressor(compressor);
    a.set_cell_var_offsets_compression_level(j.at("offset_compression_level"));
    for (auto it : j.at("attributes").items()) {
      tiledb::sm::Attribute attr = it.value().get<tiledb::sm::Attribute>();
      // TODO: Should special attributes be excluded from export?
      if (attr.name().substr(0, 2) !=
          tiledb::sm::constants::special_name_prefix) {
        st = a.add_attribute(&attr);
        if (!st.ok()) {
          LOG_STATUS(st);
        }
      }
    }
    st = a.init();
    if (!st.ok())
      LOG_STATUS(st);
    STATS_FUNC_VOID_OUT(serialization_array_schema_from_json);
  }
};

template <>
struct adl_serializer<tiledb::sm::ArraySchema*> {
  /*
   * Implement json serialization for ArraySchema
   *
   * @param j json object to store serialized data in
   * @param a ArraySchema pointer to serialize
   */
  static void to_json(json& j, const tiledb::sm::ArraySchema* a) {
    STATS_FUNC_VOID_IN(serialization_array_schema_to_json);
    if (a != nullptr)
      j = *a;
    else
      j = nullptr;
    STATS_FUNC_VOID_OUT(serialization_array_schema_to_json);
  }
};

template <>
struct adl_serializer<const tiledb::sm::ArraySchema*> {
  /*
   * Implement json serialization for ArraySchema
   *
   * @param j json object to store serialized data in
   * @param a ArraySchema pointer to serialize
   */
  static void to_json(json& j, const tiledb::sm::ArraySchema* a) {
    STATS_FUNC_VOID_IN(serialization_array_schema_to_json);
    if (a != nullptr)
      j = *a;
    else
      j = nullptr;
    STATS_FUNC_VOID_OUT(serialization_array_schema_to_json);
  }
};

}  // namespace nlohmann
#endif  // TILEDB_JSON_ARRAY_H
