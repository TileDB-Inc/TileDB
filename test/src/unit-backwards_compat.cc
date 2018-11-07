/**
 * @file   unit-backwards_compat.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
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
 * Tests of backwards compatibility for opening/reading arrays.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb;

static const std::string arrays_dir =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays";

TEST_CASE(
    "Backwards compatibility: Test error opening 1.3.0 array",
    "[backwards-compat]") {
  Context ctx;
  std::string array_uri(arrays_dir + "/dense_array_v1_3_0");
  try {
    Array array(ctx, array_uri, TILEDB_READ);
    REQUIRE(false);
  } catch (const TileDBError& e) {
    // Check correct exception type and error message.
    std::string msg(e.what());
    REQUIRE(msg.find("Error: Read buffer overflow") != std::string::npos);
  } catch (const std::exception& e) {
    REQUIRE(false);
  }
}

TEST_CASE(
    "Backwards compatibility: Test reading 1.4.0 array with non-split coords",
    "[backwards-compat]") {
  Context ctx;
  std::string array_uri(arrays_dir + "/non_split_coords_v1_4_0");
  Array array(ctx, array_uri, TILEDB_READ);
  std::vector<int> subarray = {1, 4, 10, 10};
  auto max_el = array.max_buffer_elements(subarray);
  std::vector<int> a_read;
  a_read.resize(max_el["a"].second);
  std::vector<int> coords_read;
  coords_read.resize(max_el[TILEDB_COORDS].second);

  Query query_r(ctx, array);
  query_r.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", a_read)
      .set_coordinates(coords_read);
  query_r.submit();
  array.close();

  for (int i = 0; i < 4; i++)
    REQUIRE(a_read[i] == i + 1);
}

TEST_CASE(
    "Backwards compatibility: Test reading arrays written with previous "
    "version of tiledb",
    "[backwards-compat]") {
  Context ctx;
  std::string compat_folder(arrays_dir + "/read_compatibility_test");
  if (Object::object(ctx, compat_folder).type() != Object::Type::Group)
    return;

  std::string encryption_key = "unittestunittestunittestunittest";

  tiledb::ObjectIter versions_iter(ctx, compat_folder);
  for (const auto& groupVersions : versions_iter) {
    tiledb::ObjectIter obj_iter(ctx, groupVersions.uri());
    for (const auto& object : obj_iter) {
      // REQUIRE(read_array(object.uri()))
      Array* array;
      // Check for if array is encrypted based on name for now
      if (object.uri().find("_encryption_AES_256_GCM") != std::string::npos) {
        array = new Array(
            ctx,
            object.uri(),
            TILEDB_READ,
            TILEDB_AES_256_GCM,
            encryption_key.c_str(),
            static_cast<uint32_t>(encryption_key.size() * sizeof(char)));
      } else {
        array = new Array(ctx, object.uri(), TILEDB_READ);
      }
      Query query(ctx, *array);

      std::unordered_map<std::string, std::pair<uint64_t*, void*>> buffers;
      for (auto attr : array->schema().attributes()) {
        std::string attributeName = attr.first;
        uint64_t* offsets = static_cast<uint64_t*>(malloc(sizeof(uint64_t)));
        void* values = malloc(tiledb_datatype_size(attr.second.type()));
        if (attr.second.variable_sized()) {
          buffers.emplace(attributeName, std::make_pair(offsets, values));
        } else {
          buffers.emplace(attributeName, std::make_pair(nullptr, values));
        }

        switch (attr.second.type()) {
          case TILEDB_INT8: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attributeName, offsets, 1, static_cast<int8_t*>(values), 1);
              buffers.emplace(attributeName, std::make_pair(offsets, values));
            } else {
              query.set_buffer(attributeName, static_cast<int8_t*>(values), 1);
              buffers.emplace(attributeName, std::make_pair(nullptr, values));
            }
            break;
          }
          case TILEDB_UINT8: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attributeName, offsets, 1, static_cast<uint8_t*>(values), 1);
            } else {
              query.set_buffer(attributeName, static_cast<uint8_t*>(values), 1);
            }
            break;
          }
          case TILEDB_INT16: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attributeName, offsets, 1, static_cast<int16_t*>(values), 1);
            } else {
              query.set_buffer(attributeName, static_cast<int16_t*>(values), 1);
            }
            break;
          }
          case TILEDB_UINT16: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attributeName, offsets, 1, static_cast<uint16_t*>(values), 1);
            } else {
              query.set_buffer(
                  attributeName, static_cast<uint16_t*>(values), 1);
            }
            break;
          }
          case TILEDB_INT32: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attributeName, offsets, 1, static_cast<int32_t*>(values), 1);
            } else {
              query.set_buffer(attributeName, static_cast<int32_t*>(values), 1);
            }
            break;
          }
          case TILEDB_UINT32: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attributeName, offsets, 1, static_cast<uint32_t*>(values), 1);
            } else {
              query.set_buffer(
                  attributeName, static_cast<uint32_t*>(values), 1);
            }
            break;
          }
          case TILEDB_INT64: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attributeName, offsets, 1, static_cast<int64_t*>(values), 1);
            } else {
              query.set_buffer(attributeName, static_cast<int64_t*>(values), 1);
            }
            break;
          }
          case TILEDB_UINT64: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attributeName, offsets, 1, static_cast<uint64_t*>(values), 1);
            } else {
              query.set_buffer(
                  attributeName, static_cast<uint64_t*>(values), 1);
            }
            break;
          }
          case TILEDB_FLOAT32: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attributeName, offsets, 1, static_cast<float*>(values), 1);
            } else {
              query.set_buffer(attributeName, static_cast<float*>(values), 1);
            }
            break;
          }
          case TILEDB_FLOAT64: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attributeName, offsets, 1, static_cast<double*>(values), 1);
            } else {
              query.set_buffer(attributeName, static_cast<double*>(values), 1);
            }
            break;
          }
          case TILEDB_CHAR:
          case TILEDB_STRING_ASCII:
          case TILEDB_STRING_UTF8:
          case TILEDB_STRING_UTF16:
          case TILEDB_STRING_UTF32:
          case TILEDB_STRING_UCS2:
          case TILEDB_STRING_UCS4:
          case TILEDB_ANY: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attributeName, offsets, 1, static_cast<char*>(values), 1);
            } else {
              query.set_buffer(attributeName, static_cast<char*>(values), 1);
            }
            break;
          }
        }
      }

      // Get domain to build coordinates
      Domain domain = array->schema().domain();
      uint64_t coordLength = domain.ndim();
      uint64_t coordsSize = tiledb_datatype_size(domain.type()) * coordLength;
      void* coordinates = malloc(coordLength);
      void* expectedCoordinates =
          malloc(tiledb_datatype_size(domain.type()) * coordLength);
      switch (domain.type()) {
        case TILEDB_INT8: {
          query.set_coordinates(static_cast<int8_t*>(coordinates), coordLength);
          for (size_t i = 0; i < coordLength; i++)
            static_cast<int8_t*>(expectedCoordinates)[i] = 1;
          break;
        }
        case TILEDB_UINT8: {
          query.set_coordinates(
              static_cast<uint8_t*>(coordinates), coordLength);
          for (size_t i = 0; i < coordLength; i++)
            static_cast<uint8_t*>(expectedCoordinates)[i] = 1;
          break;
        }
        case TILEDB_INT16: {
          query.set_coordinates(
              static_cast<int16_t*>(coordinates), coordLength);
          for (size_t i = 0; i < coordLength; i++)
            static_cast<int16_t*>(expectedCoordinates)[i] = 1;
          break;
        }
        case TILEDB_UINT16: {
          query.set_coordinates(
              static_cast<uint16_t*>(coordinates), coordLength);
          for (size_t i = 0; i < coordLength; i++)
            static_cast<uint16_t*>(expectedCoordinates)[i] = 1;
          break;
        }
        case TILEDB_INT32: {
          query.set_coordinates(
              static_cast<int32_t*>(coordinates), coordLength);
          for (size_t i = 0; i < coordLength; i++)
            static_cast<uint32_t*>(expectedCoordinates)[i] = 1;
          break;
        }
        case TILEDB_UINT32: {
          query.set_coordinates(
              static_cast<uint32_t*>(coordinates), coordLength);
          for (size_t i = 0; i < coordLength; i++)
            static_cast<uint32_t*>(expectedCoordinates)[i] = 1;
          break;
        }
        case TILEDB_INT64: {
          query.set_coordinates(
              static_cast<int64_t*>(coordinates), coordLength);
          for (size_t i = 0; i < coordLength; i++)
            static_cast<int64_t*>(expectedCoordinates)[i] = 1;
          break;
        }
        case TILEDB_UINT64: {
          query.set_coordinates(
              static_cast<uint64_t*>(coordinates), coordLength);
          for (size_t i = 0; i < coordLength; i++)
            static_cast<uint64_t*>(expectedCoordinates)[i] = 1;
          break;
        }
        case TILEDB_FLOAT32: {
          query.set_coordinates(static_cast<float*>(coordinates), coordLength);
          for (size_t i = 0; i < coordLength; i++)
            static_cast<float*>(expectedCoordinates)[i] = 1;
          break;
        }
        case TILEDB_FLOAT64: {
          query.set_coordinates(static_cast<double*>(coordinates), coordLength);
          for (size_t i = 0; i < coordLength; i++)
            static_cast<double*>(expectedCoordinates)[i] = 1;
          break;
        }
        default:
          REQUIRE(false);
      }

      // Submit query
      query.submit();

      REQUIRE(memcmp(coordinates, expectedCoordinates, coordsSize) == 0);

      // Check the results to make sure all values are set to 1
      for (auto buff : buffers) {
        std::pair<uint64_t*, void*> buffer = buff.second;
        if (buffer.first != nullptr) {
          REQUIRE(buffer.first[0] == 0);
        }

        Attribute attribute = array->schema().attribute(buff.first);
        switch (attribute.type()) {
          case TILEDB_INT8: {
            REQUIRE(static_cast<int8_t*>(buffer.second)[0] == 1);
            break;
          }
          case TILEDB_UINT8: {
            REQUIRE(static_cast<int8_t*>(buffer.second)[0] == 1);
            break;
          }
          case TILEDB_INT16: {
            REQUIRE(static_cast<int16_t*>(buffer.second)[0] == 1);
            break;
          }
          case TILEDB_UINT16: {
            REQUIRE(static_cast<uint16_t*>(buffer.second)[0] == 1);
            break;
          }
          case TILEDB_INT32: {
            REQUIRE(static_cast<int32_t*>(buffer.second)[0] == 1);
            break;
          }
          case TILEDB_UINT32: {
            REQUIRE(static_cast<uint32_t*>(buffer.second)[0] == 1);
            break;
          }
          case TILEDB_INT64: {
            REQUIRE(static_cast<int64_t*>(buffer.second)[0] == 1);
            break;
          }
          case TILEDB_UINT64: {
            REQUIRE(static_cast<uint64_t*>(buffer.second)[0] == 1);
            break;
          }
          case TILEDB_FLOAT32: {
            REQUIRE(static_cast<float*>(buffer.second)[0] == 1);
            break;
          }
          case TILEDB_FLOAT64: {
            REQUIRE(static_cast<double*>(buffer.second)[0] == 1);
            break;
          }
          case TILEDB_CHAR:
          case TILEDB_STRING_ASCII:
          case TILEDB_STRING_UTF8:
          case TILEDB_STRING_UTF16:
          case TILEDB_STRING_UTF32:
          case TILEDB_STRING_UCS2:
          case TILEDB_STRING_UCS4:
          case TILEDB_ANY: {
            REQUIRE(static_cast<char*>(buffer.second)[0] == '1');
            break;
          }
        }

        // Free buffers
        if (buffer.first != nullptr) {
          free(buffer.first);
        }
        if (buffer.second != nullptr) {
          free(buffer.second);
        }
      }
      delete array;
    }
  }
}
