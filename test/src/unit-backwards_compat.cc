/**
 * @file   unit-backwards_compat.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB Inc.
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

namespace {

static const std::string arrays_dir =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays";

template <typename T>
void set_query_coords(
    const Domain& domain,
    Query* query,
    void** coordinates,
    void** expected_coordinates) {
  const uint64_t ndim = domain.ndim();
  const uint64_t coords_size = tiledb_datatype_size(domain.type()) * ndim;
  *coordinates = std::malloc(coords_size);
  *expected_coordinates = std::malloc(coords_size);

  void* subarray = std::malloc(2 * coords_size);
  auto dimensions = domain.dimensions();
  for (size_t i = 0; i < ndim; i++) {
    auto dom = dimensions[i].domain<T>();
    static_cast<T*>(subarray)[2 * i] = dom.first;
    static_cast<T*>(subarray)[2 * i + 1] = dom.second;
    static_cast<T*>(*expected_coordinates)[i] = 1;
  }

  query->set_coordinates<T>(static_cast<T*>(*coordinates), ndim);
  query->set_subarray<T>(static_cast<T*>(subarray), 2 * ndim);

  std::free(subarray);
}

template <typename T>
void set_query_dimension_buffer(
    const Domain& domain,
    const uint64_t dim_idx,
    Query* query,
    void** buffer,
    void** expected_buffer) {
  Dimension dimension = domain.dimension(dim_idx);
  // Make the buffer size a bit larger because the estimator
  // works on the zipped coords size
  const uint64_t buffer_size =
      tiledb_datatype_size(dimension.type()) * domain.ndim();
  *buffer = std::malloc(buffer_size);
  *expected_buffer = std::malloc(buffer_size);
  static_cast<T*>(*expected_buffer)[0] = 1;

  auto dom = dimension.domain<T>();
  query->set_buffer<T>(dimension.name(), static_cast<T*>(*buffer), buffer_size);
  query->add_range(dim_idx, dom.first, dom.second);
}

template <typename T>
void set_query_var_dimension_buffer(
    const Domain& domain,
    const uint64_t dim_idx,
    Query* query,
    uint64_t** offsets,
    void** buffer,
    uint64_t** expected_offsets,
    void** expected_buffer) {
  Dimension dimension = domain.dimension(dim_idx);
  // Make the buffer size a bit larger because the estimator
  // works on the zipped coords size
  const uint64_t buffer_size =
      1 * tiledb_datatype_size(dimension.type()) * domain.ndim();
  *offsets = static_cast<uint64_t*>(std::malloc(sizeof(uint64_t)));
  *buffer = std::malloc(buffer_size);
  *expected_offsets = static_cast<uint64_t*>(std::malloc(sizeof(uint64_t)));
  (*expected_offsets)[0] = 0;
  *expected_buffer = std::malloc(buffer_size);
  static_cast<T*>(*expected_buffer)[0] = '1';

  memset(*offsets, 0, sizeof(uint64_t));
  memset(*buffer, 0, buffer_size);
  query->set_buffer<T>(
      dimension.name(), *offsets, 1, static_cast<T*>(*buffer), buffer_size);
  query->add_range(dim_idx, std::string("1"), std::string("1"));
}

}  // namespace

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
    "[backwards-compat][non-split-coords]") {
  Context ctx;
  std::string array_uri(arrays_dir + "/non_split_coords_v1_4_0");
  Array array(ctx, array_uri, TILEDB_READ);
  std::vector<int> subarray = {1, 4, 10, 10};
  std::vector<int> a_read;
  a_read.resize(4);
  std::vector<int> coords_read;
  coords_read.resize(8);

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
    "[backwards-compat][coords]") {
  Context ctx;
  std::string compat_folder(arrays_dir + "/read_compatibility_test");
  if (Object::object(ctx, compat_folder).type() != Object::Type::Group)
    return;

  std::string encryption_key = "unittestunittestunittestunittest";

  tiledb::ObjectIter versions_iter(ctx, compat_folder);
  for (const auto& group_versions : versions_iter) {
    tiledb::ObjectIter obj_iter(ctx, group_versions.uri());
    for (const auto& object : obj_iter) {
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

      // Skip domain types that are unsupported with zipped coordinates.
      Domain domain = array->schema().domain();
      if (domain.type() == TILEDB_STRING_ASCII) {
        continue;
      }

      Query query(ctx, *array);

      std::unordered_map<std::string, std::pair<uint64_t*, void*>> buffers;
      for (auto attr : array->schema().attributes()) {
        std::string attribute_name = attr.first;
        uint64_t* offsets = static_cast<uint64_t*>(malloc(sizeof(uint64_t)));
        void* values = malloc(tiledb_datatype_size(attr.second.type()));
        if (attr.second.variable_sized()) {
          buffers.emplace(attribute_name, std::make_pair(offsets, values));
        } else {
          buffers.emplace(attribute_name, std::make_pair(nullptr, values));
        }

        switch (attr.second.type()) {
          case TILEDB_INT8: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<int8_t*>(values), 1);
              buffers.emplace(attribute_name, std::make_pair(offsets, values));
            } else {
              query.set_buffer(attribute_name, static_cast<int8_t*>(values), 1);
              buffers.emplace(attribute_name, std::make_pair(nullptr, values));
            }
            break;
          }
          case TILEDB_UINT8: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<uint8_t*>(values), 1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<uint8_t*>(values), 1);
            }
            break;
          }
          case TILEDB_INT16: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<int16_t*>(values), 1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<int16_t*>(values), 1);
            }
            break;
          }
          case TILEDB_UINT16: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name,
                  offsets,
                  1,
                  static_cast<uint16_t*>(values),
                  1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<uint16_t*>(values), 1);
            }
            break;
          }
          case TILEDB_INT32: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<int32_t*>(values), 1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<int32_t*>(values), 1);
            }
            break;
          }
          case TILEDB_UINT32: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name,
                  offsets,
                  1,
                  static_cast<uint32_t*>(values),
                  1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<uint32_t*>(values), 1);
            }
            break;
          }
          case TILEDB_INT64: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<int64_t*>(values), 1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<int64_t*>(values), 1);
            }
            break;
          }
          case TILEDB_UINT64: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name,
                  offsets,
                  1,
                  static_cast<uint64_t*>(values),
                  1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<uint64_t*>(values), 1);
            }
            break;
          }
          case TILEDB_FLOAT32: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<float*>(values), 1);
            } else {
              query.set_buffer(attribute_name, static_cast<float*>(values), 1);
            }
            break;
          }
          case TILEDB_FLOAT64: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<double*>(values), 1);
            } else {
              query.set_buffer(attribute_name, static_cast<double*>(values), 1);
            }
            break;
          }
          case TILEDB_DATETIME_YEAR:
          case TILEDB_DATETIME_MONTH:
          case TILEDB_DATETIME_WEEK:
          case TILEDB_DATETIME_DAY:
          case TILEDB_DATETIME_HR:
          case TILEDB_DATETIME_MIN:
          case TILEDB_DATETIME_SEC:
          case TILEDB_DATETIME_MS:
          case TILEDB_DATETIME_US:
          case TILEDB_DATETIME_NS:
          case TILEDB_DATETIME_PS:
          case TILEDB_DATETIME_FS:
          case TILEDB_DATETIME_AS: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<int64_t*>(values), 1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<int64_t*>(values), 1);
            }
            break;
          }
          case TILEDB_CHAR:
          case TILEDB_STRING_ASCII:
          case TILEDB_STRING_UTF8:
          case TILEDB_ANY: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<char*>(values), 1);
            } else {
              query.set_buffer(attribute_name, static_cast<char*>(values), 1);
            }
            break;
          }
          case TILEDB_STRING_UTF16:
          case TILEDB_STRING_UCS2: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name,
                  offsets,
                  1,
                  static_cast<char16_t*>(values),
                  1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<char16_t*>(values), 1);
            }
            break;
          }
          case TILEDB_STRING_UTF32:
          case TILEDB_STRING_UCS4: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name,
                  offsets,
                  1,
                  static_cast<char32_t*>(values),
                  1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<char32_t*>(values), 1);
            }
            break;
          }
        }
      }

      // Build coordinates from the domain.
      uint64_t ndim = domain.ndim();
      uint64_t coords_size = tiledb_datatype_size(domain.type()) * ndim;
      void *coordinates = nullptr, *expected_coordinates = nullptr;

      switch (domain.type()) {
        case TILEDB_INT8:
          set_query_coords<int8_t>(
              domain, &query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_UINT8:
          set_query_coords<uint8_t>(
              domain, &query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_INT16:
          set_query_coords<int16_t>(
              domain, &query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_UINT16:
          set_query_coords<uint16_t>(
              domain, &query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_INT32:
          set_query_coords<int32_t>(
              domain, &query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_UINT32:
          set_query_coords<uint32_t>(
              domain, &query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_INT64:
          set_query_coords<int64_t>(
              domain, &query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_UINT64:
          set_query_coords<uint64_t>(
              domain, &query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_FLOAT32:
          set_query_coords<float>(
              domain, &query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_FLOAT64:
          set_query_coords<double>(
              domain, &query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_DATETIME_YEAR:
        case TILEDB_DATETIME_MONTH:
        case TILEDB_DATETIME_WEEK:
        case TILEDB_DATETIME_DAY:
        case TILEDB_DATETIME_HR:
        case TILEDB_DATETIME_MIN:
        case TILEDB_DATETIME_SEC:
        case TILEDB_DATETIME_MS:
        case TILEDB_DATETIME_US:
        case TILEDB_DATETIME_NS:
        case TILEDB_DATETIME_PS:
        case TILEDB_DATETIME_FS:
        case TILEDB_DATETIME_AS:
          set_query_coords<int64_t>(
              domain, &query, &coordinates, &expected_coordinates);
          break;
        default:
          REQUIRE(false);
      }

      // Submit query
      query.submit();

      REQUIRE(memcmp(coordinates, expected_coordinates, coords_size) == 0);
      std::free(coordinates);
      std::free(expected_coordinates);

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
            REQUIRE(static_cast<uint8_t*>(buffer.second)[0] == 1);
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
          case TILEDB_DATETIME_YEAR:
          case TILEDB_DATETIME_MONTH:
          case TILEDB_DATETIME_WEEK:
          case TILEDB_DATETIME_DAY:
          case TILEDB_DATETIME_HR:
          case TILEDB_DATETIME_MIN:
          case TILEDB_DATETIME_SEC:
          case TILEDB_DATETIME_MS:
          case TILEDB_DATETIME_US:
          case TILEDB_DATETIME_NS:
          case TILEDB_DATETIME_PS:
          case TILEDB_DATETIME_FS:
          case TILEDB_DATETIME_AS: {
            REQUIRE(static_cast<int64_t*>(buffer.second)[0] == 1);
            break;
          }
          case TILEDB_CHAR:
          case TILEDB_STRING_ASCII:
          case TILEDB_STRING_UTF8:
          case TILEDB_ANY: {
            REQUIRE(static_cast<char*>(buffer.second)[0] == '1');
            break;
          }
          case TILEDB_STRING_UTF16:
          case TILEDB_STRING_UCS2: {
            REQUIRE(static_cast<char16_t*>(buffer.second)[0] == u'1');
            break;
          }
          case TILEDB_STRING_UTF32:
          case TILEDB_STRING_UCS4: {
            REQUIRE(static_cast<char32_t*>(buffer.second)[0] == U'1');
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

TEST_CASE(
    "Backwards compatibility: Write to an array of older version",
    "[backwards-compat][write-to-older-version]") {
  std::string old_array_name(arrays_dir + "/non_split_coords_v1_4_0");
  Context ctx;
  std::string fragment_uri;

  try {
    // Write
    Array old_array(ctx, old_array_name, TILEDB_WRITE);
    std::vector<int> a_write = {100};
    std::vector<int> coords_write = {1, 10};
    Query query_w(ctx, old_array);
    query_w.set_layout(TILEDB_UNORDERED)
        .set_buffer("a", a_write)
        .set_coordinates(coords_write);
    query_w.submit();
    fragment_uri = query_w.fragment_uri(0);
    old_array.close();

    // Read
    std::vector<int> subarray = {1, 4, 10, 10};
    std::vector<int> a_read(50);
    std::vector<int> coords_read(50);

    Array array(ctx, old_array_name, TILEDB_READ);
    Query query_r(ctx, array);
    query_r.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_buffer("a", a_read)
        .set_coordinates(coords_read);
    query_r.submit();
    array.close();

    // Remove created fragment and ok file
    VFS vfs(ctx);
    vfs.remove_dir(fragment_uri);
    vfs.remove_file(fragment_uri + ".ok");

    REQUIRE(a_read[0] == 100);
    for (int i = 1; i < 4; i++) {
      REQUIRE(a_read[i] == i + 1);
    }
  } catch (const std::exception& e) {
    CHECK(false);
  }
}

TEST_CASE(
    "Backwards compatibility: Test reading arrays written with previous "
    "version of tiledb using split buffers",
    "[backwards-compat][split-buffers]") {
  Context ctx;
  std::string compat_folder(arrays_dir + "/read_compatibility_test");
  if (Object::object(ctx, compat_folder).type() != Object::Type::Group)
    return;

  std::string encryption_key = "unittestunittestunittestunittest";

  tiledb::ObjectIter versions_iter(ctx, compat_folder);
  for (const auto& group_versions : versions_iter) {
    tiledb::ObjectIter obj_iter(ctx, group_versions.uri());
    for (const auto& object : obj_iter) {
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
        std::string attribute_name = attr.first;
        uint64_t* offsets = static_cast<uint64_t*>(malloc(sizeof(uint64_t)));
        void* values = malloc(tiledb_datatype_size(attr.second.type()));
        if (attr.second.variable_sized()) {
          buffers.emplace(attribute_name, std::make_pair(offsets, values));
        } else {
          buffers.emplace(attribute_name, std::make_pair(nullptr, values));
        }

        switch (attr.second.type()) {
          case TILEDB_INT8: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<int8_t*>(values), 1);
              buffers.emplace(attribute_name, std::make_pair(offsets, values));
            } else {
              query.set_buffer(attribute_name, static_cast<int8_t*>(values), 1);
              buffers.emplace(attribute_name, std::make_pair(nullptr, values));
            }
            break;
          }
          case TILEDB_UINT8: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<uint8_t*>(values), 1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<uint8_t*>(values), 1);
            }
            break;
          }
          case TILEDB_INT16: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<int16_t*>(values), 1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<int16_t*>(values), 1);
            }
            break;
          }
          case TILEDB_UINT16: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name,
                  offsets,
                  1,
                  static_cast<uint16_t*>(values),
                  1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<uint16_t*>(values), 1);
            }
            break;
          }
          case TILEDB_INT32: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<int32_t*>(values), 1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<int32_t*>(values), 1);
            }
            break;
          }
          case TILEDB_UINT32: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name,
                  offsets,
                  1,
                  static_cast<uint32_t*>(values),
                  1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<uint32_t*>(values), 1);
            }
            break;
          }
          case TILEDB_INT64: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<int64_t*>(values), 1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<int64_t*>(values), 1);
            }
            break;
          }
          case TILEDB_UINT64: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name,
                  offsets,
                  1,
                  static_cast<uint64_t*>(values),
                  1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<uint64_t*>(values), 1);
            }
            break;
          }
          case TILEDB_FLOAT32: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<float*>(values), 1);
            } else {
              query.set_buffer(attribute_name, static_cast<float*>(values), 1);
            }
            break;
          }
          case TILEDB_FLOAT64: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<double*>(values), 1);
            } else {
              query.set_buffer(attribute_name, static_cast<double*>(values), 1);
            }
            break;
          }
          case TILEDB_DATETIME_YEAR:
          case TILEDB_DATETIME_MONTH:
          case TILEDB_DATETIME_WEEK:
          case TILEDB_DATETIME_DAY:
          case TILEDB_DATETIME_HR:
          case TILEDB_DATETIME_MIN:
          case TILEDB_DATETIME_SEC:
          case TILEDB_DATETIME_MS:
          case TILEDB_DATETIME_US:
          case TILEDB_DATETIME_NS:
          case TILEDB_DATETIME_PS:
          case TILEDB_DATETIME_FS:
          case TILEDB_DATETIME_AS: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<int64_t*>(values), 1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<int64_t*>(values), 1);
            }
            break;
          }

          case TILEDB_CHAR:
          case TILEDB_STRING_ASCII:
          case TILEDB_STRING_UTF8:
          case TILEDB_ANY: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name, offsets, 1, static_cast<char*>(values), 1);
            } else {
              query.set_buffer(attribute_name, static_cast<char*>(values), 1);
            }
            break;
          }
          case TILEDB_STRING_UTF16:
          case TILEDB_STRING_UCS2: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name,
                  offsets,
                  1,
                  static_cast<char16_t*>(values),
                  1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<char16_t*>(values), 1);
            }
            break;
          }
          case TILEDB_STRING_UTF32:
          case TILEDB_STRING_UCS4: {
            if (attr.second.variable_sized()) {
              query.set_buffer(
                  attribute_name,
                  offsets,
                  1,
                  static_cast<char32_t*>(values),
                  1);
            } else {
              query.set_buffer(
                  attribute_name, static_cast<char32_t*>(values), 1);
            }
            break;
          }
        }
      }

      // Get domain to build dimension buffers
      Domain domain = array->schema().domain();
      uint64_t ndim = domain.ndim();
      // Store one buffer per dimension
      std::vector<void*> dim_buffers(ndim);
      std::vector<uint64_t*> dim_offsets(ndim);
      std::vector<void*> dim_expected_buffers(ndim);
      std::vector<uint64_t*> dim_expected_offsets(ndim);
      for (uint64_t i = 0; i < ndim; ++i) {
        Dimension dim = domain.dimension(i);
        uint64_t* offsets = nullptr;
        void* buffer = nullptr;
        uint64_t* expected_offsets = nullptr;
        void* expected_results = nullptr;
        switch (dim.type()) {
          case TILEDB_INT8:
            set_query_dimension_buffer<int8_t>(
                domain, i, &query, &buffer, &expected_results);
            break;
          case TILEDB_UINT8:
            set_query_dimension_buffer<uint8_t>(
                domain, i, &query, &buffer, &expected_results);
            break;
          case TILEDB_INT16:
            set_query_dimension_buffer<int16_t>(
                domain, i, &query, &buffer, &expected_results);
            break;
          case TILEDB_UINT16:
            set_query_dimension_buffer<uint16_t>(
                domain, i, &query, &buffer, &expected_results);
            break;
          case TILEDB_INT32:
            set_query_dimension_buffer<int32_t>(
                domain, i, &query, &buffer, &expected_results);
            break;
          case TILEDB_UINT32:
            set_query_dimension_buffer<uint32_t>(
                domain, i, &query, &buffer, &expected_results);
            break;
          case TILEDB_INT64:
            set_query_dimension_buffer<int64_t>(
                domain, i, &query, &buffer, &expected_results);
            break;
          case TILEDB_UINT64:
            set_query_dimension_buffer<uint64_t>(
                domain, i, &query, &buffer, &expected_results);
            break;
          case TILEDB_FLOAT32:
            set_query_dimension_buffer<float>(
                domain, i, &query, &buffer, &expected_results);
            break;
          case TILEDB_FLOAT64:
            set_query_dimension_buffer<double>(
                domain, i, &query, &buffer, &expected_results);
            break;
          case TILEDB_DATETIME_YEAR:
          case TILEDB_DATETIME_MONTH:
          case TILEDB_DATETIME_WEEK:
          case TILEDB_DATETIME_DAY:
          case TILEDB_DATETIME_HR:
          case TILEDB_DATETIME_MIN:
          case TILEDB_DATETIME_SEC:
          case TILEDB_DATETIME_MS:
          case TILEDB_DATETIME_US:
          case TILEDB_DATETIME_NS:
          case TILEDB_DATETIME_PS:
          case TILEDB_DATETIME_FS:
          case TILEDB_DATETIME_AS:
            set_query_dimension_buffer<int64_t>(
                domain, i, &query, &buffer, &expected_results);
            break;
          case TILEDB_STRING_ASCII: {
            set_query_var_dimension_buffer<char>(
                domain,
                i,
                &query,
                &offsets,
                &buffer,
                &expected_offsets,
                &expected_results);
            break;
          }
          default:
            REQUIRE(false);
        }
        dim_offsets[i] = offsets;
        dim_buffers[i] = buffer;
        dim_expected_offsets[i] = expected_offsets;
        dim_expected_buffers[i] = expected_results;
      }

      // Submit query
      query.submit();

      for (uint64_t i = 0; i < ndim; ++i) {
        auto& offsets = dim_offsets[i];
        auto& buff = dim_buffers[i];
        auto& expected_offsets = dim_expected_offsets[i];
        auto& expected_results = dim_expected_buffers[i];
        Dimension dimension = domain.dimension(i);
        const uint64_t buffer_size = tiledb_datatype_size(dimension.type());
        REQUIRE(memcmp(buff, expected_results, buffer_size) == 0);
        std::free(offsets);
        std::free(buff);
        std::free(expected_offsets);
        std::free(expected_results);
      }

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
            REQUIRE(static_cast<uint8_t*>(buffer.second)[0] == 1);
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
          case TILEDB_DATETIME_YEAR:
          case TILEDB_DATETIME_MONTH:
          case TILEDB_DATETIME_WEEK:
          case TILEDB_DATETIME_DAY:
          case TILEDB_DATETIME_HR:
          case TILEDB_DATETIME_MIN:
          case TILEDB_DATETIME_SEC:
          case TILEDB_DATETIME_MS:
          case TILEDB_DATETIME_US:
          case TILEDB_DATETIME_NS:
          case TILEDB_DATETIME_PS:
          case TILEDB_DATETIME_FS:
          case TILEDB_DATETIME_AS: {
            REQUIRE(static_cast<int64_t*>(buffer.second)[0] == 1);
            break;
          }
          case TILEDB_CHAR:
          case TILEDB_STRING_ASCII:
          case TILEDB_STRING_UTF8:
          case TILEDB_ANY: {
            REQUIRE(static_cast<char*>(buffer.second)[0] == '1');
            break;
          }
          case TILEDB_STRING_UTF16:
          case TILEDB_STRING_UCS2: {
            REQUIRE(static_cast<char16_t*>(buffer.second)[0] == u'1');
            break;
          }
          case TILEDB_STRING_UTF32:
          case TILEDB_STRING_UCS4: {
            REQUIRE(static_cast<char32_t*>(buffer.second)[0] == U'1');
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