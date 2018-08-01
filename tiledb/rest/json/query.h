/**
 * @file   query.h
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

#ifndef TILEDB_JSON_QUERY_H
#define TILEDB_JSON_QUERY_H

#include <iostream>

#include "nlohmann/json.hpp"
#include "tiledb/rest/json/array.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/query/query.h"

#include <vector>

namespace nlohmann {

template <>
struct adl_serializer<tiledb::sm::Query> {
  /*
   * Implement json serialization for Query
   *
   * @param j json object to store serialized data in
   * @param d dimension object to serialize
   */
  static void to_json(json& j, const tiledb::sm::Query& q) {
    STATS_FUNC_VOID_IN(serialization_query_to_json);
    const tiledb::sm::ArraySchema* array_schema = q.array_schema();
    j = json{{"type", tiledb::sm::query_type_str(q.type())},
             {"array_schema", array_schema},
             {"layout", tiledb::sm::layout_str(q.layout())},
             {"status", tiledb::sm::query_Status_str(q.status())}};
    if (array_schema != nullptr) {
      switch (array_schema->domain()->type()) {
        case tiledb::sm::Datatype::INT8: {
          auto subarray = q.subarray<int8_t>();
          if (subarray.size() > 0)
            j["subarray"] = subarray;
          break;
        }
        case tiledb::sm::Datatype::UINT8: {
          auto subarray = q.subarray<uint8_t>();
          if (subarray.size() > 0)
            j["subarray"] = subarray;
          break;
        }
        case tiledb::sm::Datatype::INT16: {
          auto subarray = q.subarray<int16_t>();
          if (subarray.size() > 0)
            j["subarray"] = subarray;
          break;
        }
        case tiledb::sm::Datatype::UINT16: {
          auto subarray = q.subarray<uint16_t>();
          if (subarray.size() > 0)
            j["subarray"] = subarray;
          break;
        }
        case tiledb::sm::Datatype::INT32: {
          auto subarray = q.subarray<int32_t>();
          if (subarray.size() > 0)
            j["subarray"] = subarray;
          break;
        }
        case tiledb::sm::Datatype::UINT32: {
          auto subarray = q.subarray<uint32_t>();
          if (subarray.size() > 0)
            j["subarray"] = subarray;
          break;
        }
        case tiledb::sm::Datatype::INT64: {
          auto subarray = q.subarray<int64_t>();
          if (subarray.size() > 0)
            j["subarray"] = subarray;
          break;
        }
        case tiledb::sm::Datatype::UINT64: {
          auto subarray = q.subarray<uint64_t>();
          if (subarray.size() > 0)
            j["subarray"] = subarray;
          break;
        }
        case tiledb::sm::Datatype::FLOAT32: {
          auto subarray = q.subarray<float>();
          if (subarray.size() > 0)
            j["subarray"] = subarray;
          break;
        }
        case tiledb::sm::Datatype::FLOAT64: {
          auto subarray = q.subarray<double>();
          if (subarray.size() > 0)
            j["subarray"] = subarray;
          break;
        }
        case tiledb::sm::Datatype::CHAR:
        case tiledb::sm::Datatype::STRING_ASCII:
        case tiledb::sm::Datatype::STRING_UTF8:
        case tiledb::sm::Datatype::STRING_UTF16:
        case tiledb::sm::Datatype::STRING_UTF32:
        case tiledb::sm::Datatype::STRING_UCS2:
        case tiledb::sm::Datatype::STRING_UCS4:
        case tiledb::sm::Datatype::ANY:
          // Not supported domain type
          assert(false);
          break;
      }
    }

    std::vector<std::string> attributes = q.attributes();

    nlohmann::json buffers;
    for (auto attribute_name : attributes) {
      // TODO: We have to skip special attrs, which include anonymous ones
      // because we can't call add_attribute() for a special attr name, we need
      // to figure out how to add these to a array schema nicely.
      if (attribute_name.substr(0, 2) ==
          tiledb::sm::constants::special_name_prefix) {
        continue;
      }
      const tiledb::sm::Attribute* attribute =
          array_schema->attribute(attribute_name);
      if (attribute != nullptr) {
        switch (attribute->type()) {
          case tiledb::sm::Datatype::INT8: {
            auto buffer = q.buffer<int8_t>(attribute_name);
            // If buffer is null, skip
            if (buffer.first.first == nullptr || buffer.first.second == 0)
              break;
            buffers[attribute_name] = {
                {"buffer",
                 std::vector<int8_t>(
                     buffer.first.first,
                     buffer.first.first + buffer.first.second)}};
            if (buffer.second.first != nullptr)
              buffers[attribute_name]["buffer_offset"] = std::vector<uint64_t>(
                  buffer.second.first,
                  buffer.second.first + buffer.second.second);
            break;
          }
          case tiledb::sm::Datatype::STRING_ASCII:
          case tiledb::sm::Datatype::STRING_UTF8:
          case tiledb::sm::Datatype::UINT8: {
            auto buffer = q.buffer<uint8_t>(attribute_name);
            // If buffer is null, skip
            if (buffer.first.first == nullptr || buffer.first.second == 0)
              break;
            buffers[attribute_name] = {
                {"buffer",
                 std::vector<uint8_t>(
                     buffer.first.first,
                     buffer.first.first + buffer.first.second)}};
            if (buffer.second.first != nullptr)
              buffers[attribute_name]["buffer_offset"] = std::vector<uint64_t>(
                  buffer.second.first,
                  buffer.second.first + buffer.second.second);
            break;
          }
          case tiledb::sm::Datatype::INT16: {
            auto buffer = q.buffer<int16_t>(attribute_name);
            // If buffer is null, skip
            if (buffer.first.first == nullptr || buffer.first.second == 0)
              break;
            buffers[attribute_name] = {
                {"buffer",
                 std::vector<int16_t>(
                     buffer.first.first,
                     buffer.first.first + buffer.first.second)}};
            if (buffer.second.first != nullptr)
              buffers[attribute_name]["buffer_offset"] = std::vector<uint64_t>(
                  buffer.second.first,
                  buffer.second.first + buffer.second.second);
            break;
          }
          case tiledb::sm::Datatype::STRING_UTF16:
          case tiledb::sm::Datatype::STRING_UCS2:
          case tiledb::sm::Datatype::UINT16: {
            auto buffer = q.buffer<uint16_t>(attribute_name);
            // If buffer is null, skip
            if (buffer.first.first == nullptr || buffer.first.second == 0)
              break;
            buffers[attribute_name] = {
                {"buffer",
                 std::vector<uint16_t>(
                     buffer.first.first,
                     buffer.first.first + buffer.first.second)}};
            if (buffer.second.first != nullptr)
              buffers[attribute_name]["buffer_offset"] = std::vector<uint64_t>(
                  buffer.second.first,
                  buffer.second.first + buffer.second.second);
            break;
          }
          case tiledb::sm::Datatype::INT32: {
            auto buffer = q.buffer<int32_t>(attribute_name);
            // If buffer is null, skip
            if (buffer.first.first == nullptr || buffer.first.second == 0)
              break;
            buffers[attribute_name] = {
                {"buffer",
                 std::vector<int32_t>(
                     buffer.first.first,
                     buffer.first.first + buffer.first.second)}};
            if (buffer.second.first != nullptr)
              buffers[attribute_name]["buffer_offset"] = std::vector<uint64_t>(
                  buffer.second.first,
                  buffer.second.first + buffer.second.second);
            break;
          }
          case tiledb::sm::Datatype::STRING_UTF32:
          case tiledb::sm::Datatype::STRING_UCS4:
          case tiledb::sm::Datatype::UINT32: {
            auto buffer = q.buffer<uint32_t>(attribute_name);
            // If buffer is null, skip
            if (buffer.first.first == nullptr || buffer.first.second == 0)
              break;
            buffers[attribute_name] = {
                {"buffer",
                 std::vector<uint32_t>(
                     buffer.first.first,
                     buffer.first.first + buffer.first.second)}};
            if (buffer.second.first != nullptr)
              buffers[attribute_name]["buffer_offset"] = std::vector<uint64_t>(
                  buffer.second.first,
                  buffer.second.first + buffer.second.second);
            break;
          }
          case tiledb::sm::Datatype::INT64: {
            auto buffer = q.buffer<int64_t>(attribute_name);
            // If buffer is null, skip
            if (buffer.first.first == nullptr || buffer.first.second == 0)
              break;
            buffers[attribute_name] = {
                {"buffer",
                 std::vector<int64_t>(
                     buffer.first.first,
                     buffer.first.first + buffer.first.second)}};
            if (buffer.second.first != nullptr)
              buffers[attribute_name]["buffer_offset"] = std::vector<uint64_t>(
                  buffer.second.first,
                  buffer.second.first + buffer.second.second);
            break;
          }
          case tiledb::sm::Datatype::UINT64: {
            auto buffer = q.buffer<uint64_t>(attribute_name);
            // If buffer is null, skip
            if (buffer.first.first == nullptr || buffer.first.second == 0)
              break;
            buffers[attribute_name] = {
                {"buffer",
                 std::vector<uint64_t>(
                     buffer.first.first,
                     buffer.first.first + buffer.first.second)}};
            if (buffer.second.first != nullptr)
              buffers[attribute_name]["buffer_offset"] = std::vector<uint64_t>(
                  buffer.second.first,
                  buffer.second.first + buffer.second.second);
            break;
          }
          case tiledb::sm::Datatype::FLOAT32: {
            auto buffer = q.buffer<float>(attribute_name);
            // If buffer is null, skip
            if (buffer.first.first == nullptr || buffer.first.second == 0)
              break;
            buffers[attribute_name] = {
                {"buffer",
                 std::vector<float>(
                     buffer.first.first,
                     buffer.first.first + buffer.first.second)}};
            if (buffer.second.first != nullptr)
              buffers[attribute_name]["buffer_offset"] = std::vector<uint64_t>(
                  buffer.second.first,
                  buffer.second.first + buffer.second.second);
            break;
          }
          case tiledb::sm::Datatype::FLOAT64: {
            auto buffer = q.buffer<double>(attribute_name);
            // If buffer is null, skip
            if (buffer.first.first == nullptr || buffer.first.second == 0)
              break;
            buffers[attribute_name] = {
                {"buffer",
                 std::vector<double>(
                     buffer.first.first,
                     buffer.first.first + buffer.first.second)}};
            if (buffer.second.first != nullptr)
              buffers[attribute_name]["buffer_offset"] = std::vector<uint64_t>(
                  buffer.second.first,
                  buffer.second.first + buffer.second.second);
            break;
          }
          case tiledb::sm::Datatype::CHAR: {
            auto buffer = q.buffer<char>(attribute_name);
            // If buffer is null, skip
            if (buffer.first.first == nullptr || buffer.first.second == 0)
              break;
            buffers[attribute_name] = {
                {"buffer",
                 std::vector<char>(
                     buffer.first.first,
                     buffer.first.first + buffer.first.second)}};
            if (buffer.second.first != nullptr)
              buffers[attribute_name]["buffer_offset"] = std::vector<uint64_t>(
                  buffer.second.first,
                  buffer.second.first + buffer.second.second);
            break;
          }
          case tiledb::sm::Datatype::ANY:
            // Not supported datatype for buffer
            assert(false);
            break;
        }
      }
    }
    j["buffers"] = buffers;

    if (q.layout() == tiledb::sm::Layout::GLOBAL_ORDER &&
        q.type() == tiledb::sm::QueryType::WRITE) {
      nlohmann::json writer = q.writer_to_json();
      if (!writer.empty())
        j["writer"] = writer;
    }
    STATS_FUNC_VOID_OUT(serialization_query_to_json);
  }

  /*
   * Implement json de-serialization for query, does not produce a viable query
   * object but one that buffers can be copied from
   *
   * @param j json object
   * @param d dimension object to store de-serializated object in
   */
  static void from_json(const json& j, tiledb::sm::Query& q) {
    STATS_FUNC_VOID_IN(serialization_query_from_json);
    tiledb::sm::ArraySchema tmp_array_schema =
        j.at("array_schema").get<tiledb::sm::ArraySchema>();
    // Memory leak!
    tiledb::sm::ArraySchema* array_schema =
        new tiledb::sm::ArraySchema(&tmp_array_schema);
    // QueryType to parse, set default to avoid compiler uninitialized warnings
    tiledb::sm::QueryType querytype = tiledb::sm::QueryType::READ;
    tiledb::sm::Status status =
        tiledb::sm::query_type_enum(j.at("type"), &querytype);
    if (!status.ok())
      throw std::runtime_error(status.to_string());
    q = tiledb::sm::Query(nullptr, querytype, array_schema, {});

    // Layout to parse, set default to avoid compiler uninitialized warnings
    tiledb::sm::Layout layout = tiledb::sm::Layout::ROW_MAJOR;
    status = tiledb::sm::layout_enum(j.at("layout"), &layout);
    if (!status.ok())
      throw std::runtime_error(status.to_string());
    q.set_layout(layout);

    // TODO: do this differently
    if (layout == tiledb::sm::Layout::GLOBAL_ORDER &&
        querytype == tiledb::sm::QueryType::WRITE && j.count("writer"))
      q.set_writer(j.at("writer"));

    // QueryStatus to parse, set default to avoid compiler uninitialized
    // warnings
    tiledb::sm::QueryStatus query_status =
        tiledb::sm::QueryStatus::UNINITIALIZED;
    status = tiledb::sm::query_status_enum(j.at("status"), &query_status);
    if (!status.ok())
      throw std::runtime_error(status.to_string());
    q.set_status(query_status);

    // Set subarray
    if (j.count("subarray")) {
      switch (array_schema->domain()->type()) {
        case tiledb::sm::Datatype::INT8: {
          std::vector<int8_t> subarray = j.at("subarray");
          status = q.set_subarray(subarray.data());
          break;
        }
        case tiledb::sm::Datatype::UINT8: {
          std::vector<uint8_t> subarray = j.at("subarray");
          status = q.set_subarray(subarray.data());
          break;
        }
        case tiledb::sm::Datatype::INT16: {
          std::vector<int16_t> subarray = j.at("subarray");
          status = q.set_subarray(subarray.data());
          break;
        }
        case tiledb::sm::Datatype::UINT16: {
          std::vector<uint16_t> subarray = j.at("subarray");
          status = q.set_subarray(subarray.data());
          break;
        }
        case tiledb::sm::Datatype::INT32: {
          std::vector<int32_t> subarray = j.at("subarray");
          status = q.set_subarray(subarray.data());
          break;
        }
        case tiledb::sm::Datatype::UINT32: {
          std::vector<uint32_t> subarray = j.at("subarray");
          status = q.set_subarray(subarray.data());
          break;
        }
        case tiledb::sm::Datatype::INT64: {
          std::vector<int64_t> subarray = j.at("subarray");
          status = q.set_subarray(subarray.data());
          break;
        }
        case tiledb::sm::Datatype::UINT64: {
          std::vector<uint64_t> subarray = j.at("subarray");
          status = q.set_subarray(subarray.data());
          break;
        }
        case tiledb::sm::Datatype::FLOAT32: {
          std::vector<float> subarray = j.at("subarray");
          status = q.set_subarray(subarray.data());
          break;
        }
        case tiledb::sm::Datatype::FLOAT64: {
          std::vector<double> subarray = j.at("subarray");
          status = q.set_subarray(subarray.data());
          break;
        }
        case tiledb::sm::Datatype::CHAR:
        case tiledb::sm::Datatype::STRING_ASCII:
        case tiledb::sm::Datatype::STRING_UTF8:
        case tiledb::sm::Datatype::STRING_UTF16:
        case tiledb::sm::Datatype::STRING_UTF32:
        case tiledb::sm::Datatype::STRING_UCS2:
        case tiledb::sm::Datatype::STRING_UCS4:
        case tiledb::sm::Datatype::ANY:
          // Not supported domain type
          assert(false);
          break;
      }
      if (!status.ok()) {
        LOG_STATUS(status);
        throw std::runtime_error(status.to_string());
      }
    }

    std::unordered_map<std::string, nlohmann::json> buffers = j.at("buffers");
    for (auto buffer_json : buffers) {
      // TODO: We have to skip special attrs, which include anonymous ones
      // because we can't call add_attribute() for a special attr name, we need
      // to figure out how to add these to a array schema nicely.
      if (buffer_json.first.substr(0, 2) ==
          tiledb::sm::constants::special_name_prefix) {
        continue;
      }
      const tiledb::sm::Attribute* attr =
          array_schema->attribute(buffer_json.first);
      if (attr == nullptr) {
        throw std::runtime_error(
            "Attribute " + buffer_json.first + " is null in query from_json");
      }
      uint64_t type_size = tiledb::sm::datatype_size(attr->type());
      switch (attr->type()) {
        case tiledb::sm::Datatype::INT8: {
          // Get buffer
          std::vector<int8_t>* buffer = new std::vector<int8_t>(
              buffer_json.second.at("buffer").get<std::vector<int8_t>>());
          uint64_t* buffer_size = new uint64_t(buffer->size() * type_size);

          // Check for offset buffer
          if (buffer_json.second.count("buffer_offset")) {
            std::vector<uint64_t>* buffer_offset =
                new std::vector<uint64_t>(buffer_json.second.at("buffer_offset")
                                              .get<std::vector<uint64_t>>());
            uint64_t* buffer_offset_size =
                new uint64_t(buffer_offset->size() * sizeof(uint64_t));
            q.set_buffer(
                attr->name(),
                buffer_offset->data(),
                buffer_offset_size,
                buffer->data(),
                buffer_size);
          } else
            q.set_buffer(attr->name(), buffer->data(), buffer_size);
          break;
        }
        case tiledb::sm::Datatype::STRING_ASCII:
        case tiledb::sm::Datatype::STRING_UTF8:
        case tiledb::sm::Datatype::UINT8: {
          // Get buffer
          std::vector<uint8_t>* buffer = new std::vector<uint8_t>(
              buffer_json.second.at("buffer").get<std::vector<uint8_t>>());
          uint64_t* buffer_size = new uint64_t(buffer->size() * type_size);

          // Check for offset buffer
          if (buffer_json.second.count("buffer_offset")) {
            std::vector<uint64_t>* buffer_offset =
                new std::vector<uint64_t>(buffer_json.second.at("buffer_offset")
                                              .get<std::vector<uint64_t>>());
            uint64_t* buffer_offset_size =
                new uint64_t(buffer_offset->size() * sizeof(uint64_t));
            q.set_buffer(
                attr->name(),
                buffer_offset->data(),
                buffer_offset_size,
                buffer->data(),
                buffer_size);
          } else
            q.set_buffer(attr->name(), buffer->data(), buffer_size);
          break;
        }
        case tiledb::sm::Datatype::INT16: {
          // Get buffer
          std::vector<int16_t>* buffer = new std::vector<int16_t>(
              buffer_json.second.at("buffer").get<std::vector<int16_t>>());
          uint64_t* buffer_size = new uint64_t(buffer->size() * type_size);

          // Check for offset buffer
          if (buffer_json.second.count("buffer_offset")) {
            std::vector<uint64_t>* buffer_offset =
                new std::vector<uint64_t>(buffer_json.second.at("buffer_offset")
                                              .get<std::vector<uint64_t>>());
            uint64_t* buffer_offset_size =
                new uint64_t(buffer_offset->size() * sizeof(uint64_t));
            q.set_buffer(
                attr->name(),
                buffer_offset->data(),
                buffer_offset_size,
                buffer->data(),
                buffer_size);
          } else
            q.set_buffer(attr->name(), buffer->data(), buffer_size);
          break;
        }
        case tiledb::sm::Datatype::STRING_UTF16:
        case tiledb::sm::Datatype::STRING_UCS2:
        case tiledb::sm::Datatype::UINT16: {
          // Get buffer
          std::vector<uint16_t>* buffer = new std::vector<uint16_t>(
              buffer_json.second.at("buffer").get<std::vector<uint16_t>>());
          uint64_t* buffer_size = new uint64_t(buffer->size() * type_size);

          // Check for offset buffer
          if (buffer_json.second.count("buffer_offset")) {
            std::vector<uint64_t>* buffer_offset =
                new std::vector<uint64_t>(buffer_json.second.at("buffer_offset")
                                              .get<std::vector<uint64_t>>());
            uint64_t* buffer_offset_size =
                new uint64_t(buffer_offset->size() * sizeof(uint64_t));
            q.set_buffer(
                attr->name(),
                buffer_offset->data(),
                buffer_offset_size,
                buffer->data(),
                buffer_size);
          } else
            q.set_buffer(attr->name(), buffer->data(), buffer_size);
          break;
        }
        case tiledb::sm::Datatype::INT32: {
          // Get buffer
          std::vector<int32_t>* buffer = new std::vector<int32_t>(
              buffer_json.second.at("buffer").get<std::vector<int32_t>>());
          uint64_t* buffer_size = new uint64_t(buffer->size() * type_size);

          // Check for offset buffer
          if (buffer_json.second.count("buffer_offset")) {
            std::vector<uint64_t>* buffer_offset =
                new std::vector<uint64_t>(buffer_json.second.at("buffer_offset")
                                              .get<std::vector<uint64_t>>());
            uint64_t* buffer_offset_size =
                new uint64_t(buffer_offset->size() * sizeof(uint64_t));
            q.set_buffer(
                attr->name(),
                buffer_offset->data(),
                buffer_offset_size,
                buffer->data(),
                buffer_size);
          } else {
            q.set_buffer(attr->name(), buffer->data(), buffer_size);
          }
          break;
        }
        case tiledb::sm::Datatype::STRING_UTF32:
        case tiledb::sm::Datatype::STRING_UCS4:
        case tiledb::sm::Datatype::UINT32: {
          // Get buffer
          std::vector<uint32_t>* buffer = new std::vector<uint32_t>(
              buffer_json.second.at("buffer").get<std::vector<uint32_t>>());
          uint64_t* buffer_size = new uint64_t(buffer->size() * type_size);

          // Check for offset buffer
          if (buffer_json.second.count("buffer_offset")) {
            std::vector<uint64_t>* buffer_offset =
                new std::vector<uint64_t>(buffer_json.second.at("buffer_offset")
                                              .get<std::vector<uint64_t>>());
            uint64_t* buffer_offset_size =
                new uint64_t(buffer_offset->size() * sizeof(uint64_t));
            q.set_buffer(
                attr->name(),
                buffer_offset->data(),
                buffer_offset_size,
                buffer->data(),
                buffer_size);
          } else
            q.set_buffer(attr->name(), buffer->data(), buffer_size);
          break;
        }
        case tiledb::sm::Datatype::INT64: {
          // Get buffer
          std::vector<int64_t>* buffer = new std::vector<int64_t>(
              buffer_json.second.at("buffer").get<std::vector<int64_t>>());
          uint64_t* buffer_size = new uint64_t(buffer->size() * type_size);

          // Check for offset buffer
          if (buffer_json.second.count("buffer_offset")) {
            std::vector<uint64_t>* buffer_offset =
                new std::vector<uint64_t>(buffer_json.second.at("buffer_offset")
                                              .get<std::vector<uint64_t>>());
            uint64_t* buffer_offset_size =
                new uint64_t(buffer_offset->size() * sizeof(uint64_t));
            q.set_buffer(
                attr->name(),
                buffer_offset->data(),
                buffer_offset_size,
                buffer->data(),
                buffer_size);
          } else
            q.set_buffer(attr->name(), buffer->data(), buffer_size);
          break;
        }
        case tiledb::sm::Datatype::UINT64: {
          // Get buffer
          std::vector<uint64_t>* buffer = new std::vector<uint64_t>(
              buffer_json.second.at("buffer").get<std::vector<uint64_t>>());
          uint64_t* buffer_size = new uint64_t(buffer->size() * type_size);

          // Check for offset buffer
          if (buffer_json.second.count("buffer_offset")) {
            std::vector<uint64_t>* buffer_offset =
                new std::vector<uint64_t>(buffer_json.second.at("buffer_offset")
                                              .get<std::vector<uint64_t>>());
            uint64_t* buffer_offset_size =
                new uint64_t(buffer_offset->size() * sizeof(uint64_t));
            q.set_buffer(
                attr->name(),
                buffer_offset->data(),
                buffer_offset_size,
                buffer->data(),
                buffer_size);
          } else
            q.set_buffer(attr->name(), buffer->data(), buffer_size);
          break;
        }
        case tiledb::sm::Datatype::FLOAT32: {
          // Get buffer
          std::vector<float>* buffer = new std::vector<float>(
              buffer_json.second.at("buffer").get<std::vector<float>>());
          uint64_t* buffer_size = new uint64_t(buffer->size() * type_size);

          // Check for offset buffer
          if (buffer_json.second.count("buffer_offset")) {
            std::vector<uint64_t>* buffer_offset =
                new std::vector<uint64_t>(buffer_json.second.at("buffer_offset")
                                              .get<std::vector<uint64_t>>());
            uint64_t* buffer_offset_size =
                new uint64_t(buffer_offset->size() * sizeof(uint64_t));
            q.set_buffer(
                attr->name(),
                buffer_offset->data(),
                buffer_offset_size,
                buffer->data(),
                buffer_size);
          } else
            q.set_buffer(attr->name(), buffer->data(), buffer_size);
          break;
        }
        case tiledb::sm::Datatype::FLOAT64: {
          // Get buffer
          std::vector<double>* buffer = new std::vector<double>(
              buffer_json.second.at("buffer").get<std::vector<double>>());
          uint64_t* buffer_size = new uint64_t(buffer->size() * type_size);

          // Check for offset buffer
          if (buffer_json.second.count("buffer_offset")) {
            std::vector<uint64_t>* buffer_offset =
                new std::vector<uint64_t>(buffer_json.second.at("buffer_offset")
                                              .get<std::vector<uint64_t>>());
            uint64_t* buffer_offset_size =
                new uint64_t(buffer_offset->size() * sizeof(uint64_t));
            q.set_buffer(
                attr->name(),
                buffer_offset->data(),
                buffer_offset_size,
                buffer->data(),
                buffer_size);
          } else
            q.set_buffer(attr->name(), buffer->data(), buffer_size);
          break;
        }
        case tiledb::sm::Datatype::CHAR: {
          // Get buffer
          std::vector<char>* buffer = new std::vector<char>(
              buffer_json.second.at("buffer").get<std::vector<char>>());
          uint64_t* buffer_size = new uint64_t(buffer->size() * type_size);

          // Check for offset buffer
          if (buffer_json.second.count("buffer_offset")) {
            std::vector<uint64_t>* buffer_offset =
                new std::vector<uint64_t>(buffer_json.second.at("buffer_offset")
                                              .get<std::vector<uint64_t>>());
            uint64_t* buffer_offset_size =
                new uint64_t(buffer_offset->size() * sizeof(uint64_t));
            q.set_buffer(
                attr->name(),
                buffer_offset->data(),
                buffer_offset_size,
                buffer->data(),
                buffer_size);
          } else
            q.set_buffer(attr->name(), buffer->data(), buffer_size);
          break;
        }
        case tiledb::sm::Datatype::ANY:
          // Not supported datatype for buffer
          assert(false);
          break;
      }
    }
    STATS_FUNC_VOID_OUT(serialization_query_from_json);
  }
};

template <>
struct adl_serializer<tiledb::sm::Query*> {
  /*
   * Implement json serialization for Query
   *
   * @param j json object to store serialized data in
   * @param d dimension pointer to serialize
   */
  static void to_json(json& j, const tiledb::sm::Query* q) {
    STATS_FUNC_VOID_IN(serialization_query_to_json);
    if (q != nullptr)
      j = *q;
    else
      j = nullptr;
    STATS_FUNC_VOID_OUT(serialization_query_to_json);
  }
};

}  // namespace nlohmann
#endif  // TILEDB_JSON_QUERY_H
