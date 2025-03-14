/**
 * @file   unit-backwards_compat.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB Inc.
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

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/serialization_wrappers.h"
#include "test/support/src/temporary_local_directory.h"
#include "tiledb/common/common.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/misc/constants.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb;
using namespace tiledb::test;

namespace {

static const std::string arrays_dir =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays";

static const std::string groups_dir =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/groups";

template <typename T>
void set_query_coords(
    Context& ctx,
    Array* array,
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

  Subarray sub(ctx, *array);
  sub.set_subarray<T>(static_cast<T*>(subarray), 2 * ndim);

  query->set_data_buffer<T>("__coords", static_cast<T*>(*coordinates), ndim);
  query->set_subarray(sub);

  std::free(subarray);
}

template <typename T>
void set_query_dimension_buffer(
    const Domain& domain,
    const uint64_t dim_idx,
    Query* query,
    Subarray* subarray,
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
  query->set_data_buffer<T>(
      dimension.name(), static_cast<T*>(*buffer), buffer_size);
  subarray->add_range(dim_idx, dom.first, dom.second);
}

template <typename T>
void set_query_var_dimension_buffer(
    const Domain& domain,
    const uint64_t dim_idx,
    Query* query,
    Subarray* subarray,
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
  query->set_data_buffer<T>(
      dimension.name(), static_cast<T*>(*buffer), buffer_size);
  query->set_offsets_buffer(dimension.name(), *offsets, 1);
  subarray->add_range(dim_idx, std::string("1"), std::string("1"));
}

}  // namespace

TEST_CASE(
    "Backwards compatibility: Test error opening 1.3.0 array",
    "[backwards-compat]") {
  Context& ctx = vanilla_context_cpp();
  std::string array_uri(arrays_dir + "/dense_array_v1_3_0");
  REQUIRE_THROWS_WITH(
      Array(ctx, array_uri, TILEDB_READ),
      Catch::Matchers::EndsWith(
          "Reading data past end of serialized data size."));
}

template <typename T>
void set_buffer_wrapper(
    Query* const query,
    const std::string& attribute_name,
    const bool var_sized,
    const bool nullable,
    uint64_t* const offsets,
    void* const values,
    uint8_t* const validity,
    std::unordered_map<std::string, tuple<uint64_t*, void*, uint8_t*>>* const
        buffers) {
  if (var_sized) {
    if (!nullable) {
      query->set_data_buffer(attribute_name, static_cast<T*>(values), 1);
      query->set_offsets_buffer(attribute_name, offsets, 1);
      buffers->emplace(
          attribute_name, std::make_tuple(offsets, values, nullptr));
    } else {
      query->set_data_buffer(attribute_name, static_cast<T*>(values), 1);
      query->set_offsets_buffer(attribute_name, offsets, 1);
      query->set_validity_buffer(attribute_name, validity, 1);
      buffers->emplace(
          attribute_name, std::make_tuple(offsets, values, validity));
    }
  } else {
    if (!nullable) {
      query->set_data_buffer(attribute_name, static_cast<T*>(values), 1);
      buffers->emplace(
          attribute_name, std::make_tuple(nullptr, values, nullptr));
    } else {
      query->set_data_buffer(attribute_name, static_cast<T*>(values), 1);
      query->set_validity_buffer(attribute_name, validity, 1);
      buffers->emplace(
          attribute_name, std::make_tuple(nullptr, values, validity));
    }
  }
}

TEST_CASE(
    "Backwards compatibility: Test reading 1.4.0 array with non-split coords",
    "[backwards-compat][non-split-coords]") {
  Context& ctx = vanilla_context_cpp();
  std::string array_uri(arrays_dir + "/non_split_coords_v1_4_0");
  Array array(ctx, array_uri, TILEDB_READ);
  std::vector<int> subarray = {1, 4, 10, 10};
  std::vector<int> a_read(4);
  std::vector<int> coords_read(8);

  Query query_r(ctx, array);
  Subarray sub(ctx, array);
  sub.set_subarray(subarray);
  query_r.set_subarray(sub)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_read)
      .set_data_buffer("__coords", coords_read);
  query_r.submit();

  // Note: If you encounter a failure here, in particular with a_read[0] == 100
  // (instead of 1), be sure non_split_coords_v1_4_0 has not become 'corrupt',
  // possibly from a previous aborted run, as there is also a test elsewhere
  // which expects a_read[0] == 100, if non_split_coords_v1_4_0 may have become
  // corrupt can refresh from repository to correct initial state.
  for (int i = 0; i < 4; i++)
    REQUIRE(a_read[i] == i + 1);

  array.close();
}

TEST_CASE(
    "Backwards compatibility: Test reading arrays written with previous "
    "version of tiledb",
    "[backwards-compat][coords]") {
  std::string encryption_key = "unittestunittestunittestunittest";

  Context& ctx = vanilla_context_cpp();
  tiledb::Config cfg;
  cfg["sm.encryption_type"] = "AES_256_GCM";
  cfg["sm.encryption_key"] = encryption_key.c_str();
  Context ctx_encrypt(cfg);
  std::string compat_folder(arrays_dir + "/read_compatibility_test");
  if (Object::object(ctx, compat_folder).type() != Object::Type::Group) {
    return;
  }

  tiledb::ObjectIter versions_iter(ctx, compat_folder);
  for (const auto& group_versions : versions_iter) {
    tiledb::ObjectIter obj_iter(ctx, group_versions.uri());
    for (const auto& object : obj_iter) {
      bool encrypted = false;
      Array* array;

      if (object.type() != Object::Type::Array) {
        continue;
      }

      // Check for if array is encrypted based on name for now
      if (object.uri().find("_encryption_AES_256_GCM") != std::string::npos) {
        encrypted = true;
        array = new Array(ctx_encrypt, object.uri(), TILEDB_READ);
      } else {
        array = new Array(ctx, object.uri(), TILEDB_READ);
      }

      // Skip arrays with heterogeneous dimension types.
      Domain domain = array->schema().domain();
      REQUIRE(domain.ndim() > 0);
      tiledb_datatype_t last_datatype = domain.dimension(0).type();
      bool heterogeneous = false;
      for (uint32_t i = 1; i < domain.ndim(); ++i) {
        if (domain.dimension(i).type() != last_datatype) {
          heterogeneous = true;
          break;
        }
      }

      if (heterogeneous) {
        continue;
      }

      // Skip domain types that are unsupported with zipped coordinates.
      if (domain.type() == TILEDB_STRING_ASCII) {
        continue;
      }

      auto query = new Query(encrypted ? ctx_encrypt : ctx, *array);

      std::unordered_map<std::string, tuple<uint64_t*, void*, uint8_t*>>
          buffers;
      for (auto attr : array->schema().attributes()) {
        std::string attribute_name = attr.first;
        const bool var_sized = attr.second.variable_sized();
        const bool nullable = attr.second.nullable();
        uint64_t* offsets = static_cast<uint64_t*>(malloc(sizeof(uint64_t)));
        void* values = malloc(tiledb_datatype_size(attr.second.type()));
        uint8_t* validity = static_cast<uint8_t*>(malloc(sizeof(uint8_t)));

        switch (attr.second.type()) {
          case TILEDB_BLOB:
          case TILEDB_GEOM_WKB:
          case TILEDB_GEOM_WKT: {
            set_buffer_wrapper<std::byte>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_INT8: {
            set_buffer_wrapper<int8_t>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_BOOL:
          case TILEDB_UINT8: {
            set_buffer_wrapper<uint8_t>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_INT16: {
            set_buffer_wrapper<int16_t>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_UINT16: {
            set_buffer_wrapper<uint16_t>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_INT32: {
            set_buffer_wrapper<int32_t>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_UINT32: {
            set_buffer_wrapper<uint32_t>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_INT64: {
            set_buffer_wrapper<int64_t>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_UINT64: {
            set_buffer_wrapper<uint64_t>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_FLOAT32: {
            set_buffer_wrapper<float>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_FLOAT64: {
            set_buffer_wrapper<double>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
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
          case TILEDB_DATETIME_AS:
          case TILEDB_TIME_HR:
          case TILEDB_TIME_MIN:
          case TILEDB_TIME_SEC:
          case TILEDB_TIME_MS:
          case TILEDB_TIME_US:
          case TILEDB_TIME_NS:
          case TILEDB_TIME_PS:
          case TILEDB_TIME_FS:
          case TILEDB_TIME_AS: {
            set_buffer_wrapper<int64_t>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_CHAR:
          case TILEDB_STRING_ASCII:
          case TILEDB_STRING_UTF8:
          case TILEDB_ANY: {
            set_buffer_wrapper<char>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_STRING_UTF16:
          case TILEDB_STRING_UCS2: {
            set_buffer_wrapper<char16_t>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_STRING_UTF32:
          case TILEDB_STRING_UCS4: {
            set_buffer_wrapper<char32_t>(
                query,
                attribute_name,
                var_sized,
                nullable,
                offsets,
                values,
                validity,
                &buffers);
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
              ctx, array, domain, query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_UINT8:
          set_query_coords<uint8_t>(
              ctx, array, domain, query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_INT16:
          set_query_coords<int16_t>(
              ctx, array, domain, query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_UINT16:
          set_query_coords<uint16_t>(
              ctx, array, domain, query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_INT32:
          set_query_coords<int32_t>(
              ctx, array, domain, query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_UINT32:
          set_query_coords<uint32_t>(
              ctx, array, domain, query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_INT64:
          set_query_coords<int64_t>(
              ctx, array, domain, query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_UINT64:
          set_query_coords<uint64_t>(
              ctx, array, domain, query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_FLOAT32:
          set_query_coords<float>(
              ctx, array, domain, query, &coordinates, &expected_coordinates);
          break;
        case TILEDB_FLOAT64:
          set_query_coords<double>(
              ctx, array, domain, query, &coordinates, &expected_coordinates);
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
        case TILEDB_TIME_HR:
        case TILEDB_TIME_MIN:
        case TILEDB_TIME_SEC:
        case TILEDB_TIME_MS:
        case TILEDB_TIME_US:
        case TILEDB_TIME_NS:
        case TILEDB_TIME_PS:
        case TILEDB_TIME_FS:
        case TILEDB_TIME_AS:
          set_query_coords<int64_t>(
              ctx, array, domain, query, &coordinates, &expected_coordinates);
          break;
        default:
          REQUIRE(false);
      }

      // Submit query
      query->submit();
      delete query;

      REQUIRE(memcmp(coordinates, expected_coordinates, coords_size) == 0);
      std::free(coordinates);
      std::free(expected_coordinates);

      // Check the results to make sure all values are set to 1
      for (auto buff : buffers) {
        tuple<uint64_t*, void*, uint8_t*> buffer = buff.second;
        if (std::get<0>(buffer) != nullptr) {
          REQUIRE(std::get<0>(buffer)[0] == 0);
        }

        Attribute attribute = array->schema().attribute(buff.first);
        switch (attribute.type()) {
          case TILEDB_BLOB:
          case TILEDB_GEOM_WKB:
          case TILEDB_GEOM_WKT: {
            REQUIRE(
                static_cast<std::byte*>(std::get<1>(buffer))[0] ==
                static_cast<std::byte>(1));
            break;
          }
          case TILEDB_INT8: {
            REQUIRE(static_cast<int8_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_BOOL:
          case TILEDB_UINT8: {
            REQUIRE(static_cast<uint8_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_INT16: {
            REQUIRE(static_cast<int16_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_UINT16: {
            REQUIRE(static_cast<uint16_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_INT32: {
            REQUIRE(static_cast<int32_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_UINT32: {
            REQUIRE(static_cast<uint32_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_INT64: {
            REQUIRE(static_cast<int64_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_UINT64: {
            REQUIRE(static_cast<uint64_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_FLOAT32: {
            REQUIRE(static_cast<float*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_FLOAT64: {
            REQUIRE(static_cast<double*>(std::get<1>(buffer))[0] == 1);
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
          case TILEDB_DATETIME_AS:
          case TILEDB_TIME_HR:
          case TILEDB_TIME_MIN:
          case TILEDB_TIME_SEC:
          case TILEDB_TIME_MS:
          case TILEDB_TIME_US:
          case TILEDB_TIME_NS:
          case TILEDB_TIME_PS:
          case TILEDB_TIME_FS:
          case TILEDB_TIME_AS: {
            REQUIRE(static_cast<int64_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_CHAR:
          case TILEDB_STRING_ASCII:
          case TILEDB_STRING_UTF8:
          case TILEDB_ANY: {
            REQUIRE(static_cast<char*>(std::get<1>(buffer))[0] == '1');
            break;
          }
          case TILEDB_STRING_UTF16:
          case TILEDB_STRING_UCS2: {
            REQUIRE(static_cast<char16_t*>(std::get<1>(buffer))[0] == u'1');
            break;
          }
          case TILEDB_STRING_UTF32:
          case TILEDB_STRING_UCS4: {
            REQUIRE(static_cast<char32_t*>(std::get<1>(buffer))[0] == U'1');
            break;
          }
        }

        // Free buffers
        if (std::get<0>(buffer) != nullptr) {
          free(std::get<0>(buffer));
        }
        if (std::get<1>(buffer) != nullptr) {
          free(std::get<1>(buffer));
        }
      }
      delete array;
    }
  }
}

TEST_CASE(
    "Backwards compatibility: Write to an array of older version",
    "[backwards-compat][write-to-older-version]") {
  if constexpr (is_experimental_build) {
    return;
  }

  std::string old_array_name(arrays_dir + "/non_split_coords_v1_4_0");
  Context& ctx = vanilla_context_cpp();
  std::string fragment_uri;

  try {
    // Write
    Array old_array(ctx, old_array_name, TILEDB_WRITE);
    std::vector<int> a_write = {100};
    std::vector<int> coords_write = {1, 10};
    Query query_w(ctx, old_array);
    query_w.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", a_write)
        .set_data_buffer("__coords", coords_write);
    query_w.submit();
    old_array.close();

    // Read
    std::vector<int> subarray = {1, 4, 10, 10};
    std::vector<int> a_read(50);
    std::vector<int> coords_read(50);

    Array array(ctx, old_array_name, TILEDB_READ);
    Query query_r(ctx, array);
    Subarray subarray_r(ctx, array);
    subarray_r.set_subarray(subarray);
    query_r.set_subarray(subarray_r)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_read)
        .set_data_buffer("__coords", coords_read);
    query_r.submit();
    array.close();

    // Remove created fragment and ok file
    VFS vfs(ctx);
    vfs.remove_dir(get_fragment_dir(old_array_name));
    vfs.remove_dir(get_commit_dir(old_array_name));

    REQUIRE(a_read[0] == 100);
    for (int i = 1; i < 4; i++) {
      REQUIRE(a_read[i] == i + 1);
    }
  } catch (const std::exception& e) {
    std::cerr << "Unexpected exception in unit-backwards_compat: " << e.what()
              << std::endl;
    CHECK(false);
  }
}

TEST_CASE(
    "Backwards compatibility: Test reading arrays written with previous "
    "version of tiledb using split buffers",
    "[backwards-compat][split-buffers]") {
  std::string encryption_key = "unittestunittestunittestunittest";

  Context& ctx = vanilla_context_cpp();
  tiledb::Config cfg;
  cfg["sm.encryption_type"] = "AES_256_GCM";
  cfg["sm.encryption_key"] = encryption_key.c_str();
  Context ctx_encrypt(cfg);
  std::string compat_folder(arrays_dir + "/read_compatibility_test");
  if (Object::object(ctx, compat_folder).type() != Object::Type::Group) {
    return;
  }

  tiledb::ObjectIter versions_iter(ctx, compat_folder);
  for (const auto& group_versions : versions_iter) {
    tiledb::ObjectIter obj_iter(ctx, group_versions.uri());
    for (const auto& object : obj_iter) {
      bool encrypted = false;
      Array* array;

      if (object.type() != Object::Type::Array) {
        continue;
      }

      // Check for if array is encrypted based on name for now
      if (object.uri().find("_encryption_AES_256_GCM") != std::string::npos) {
        encrypted = true;
        array = new Array(ctx_encrypt, object.uri(), TILEDB_READ);
      } else {
        array = new Array(ctx, object.uri(), TILEDB_READ);
      }

      auto query = new Query(encrypted ? ctx_encrypt : ctx, *array);

      auto subarray = new Subarray(ctx, *array);

      std::unordered_map<std::string, tuple<uint64_t*, void*, uint8_t*>>
          buffers;
      for (auto attr : array->schema().attributes()) {
        std::string attribute_name = attr.first;
        uint64_t* offsets = static_cast<uint64_t*>(malloc(sizeof(uint64_t)));
        void* values = malloc(tiledb_datatype_size(attr.second.type()));
        uint8_t* validity = static_cast<uint8_t*>(malloc(sizeof(uint8_t)));

        switch (attr.second.type()) {
          case TILEDB_BLOB:
          case TILEDB_GEOM_WKB:
          case TILEDB_GEOM_WKT: {
            set_buffer_wrapper<std::byte>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_INT8: {
            set_buffer_wrapper<int8_t>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_BOOL:
          case TILEDB_UINT8: {
            set_buffer_wrapper<uint8_t>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_INT16: {
            set_buffer_wrapper<int16_t>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_UINT16: {
            set_buffer_wrapper<uint16_t>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_INT32: {
            set_buffer_wrapper<int32_t>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_UINT32: {
            set_buffer_wrapper<uint32_t>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_INT64: {
            set_buffer_wrapper<int64_t>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_UINT64: {
            set_buffer_wrapper<uint64_t>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_FLOAT32: {
            set_buffer_wrapper<float>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_FLOAT64: {
            set_buffer_wrapper<double>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
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
          case TILEDB_DATETIME_AS:
          case TILEDB_TIME_HR:
          case TILEDB_TIME_MIN:
          case TILEDB_TIME_SEC:
          case TILEDB_TIME_MS:
          case TILEDB_TIME_US:
          case TILEDB_TIME_NS:
          case TILEDB_TIME_PS:
          case TILEDB_TIME_FS:
          case TILEDB_TIME_AS: {
            set_buffer_wrapper<int64_t>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }

          case TILEDB_CHAR:
          case TILEDB_STRING_ASCII:
          case TILEDB_STRING_UTF8:
          case TILEDB_ANY: {
            set_buffer_wrapper<char>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_STRING_UTF16:
          case TILEDB_STRING_UCS2: {
            set_buffer_wrapper<char16_t>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
            break;
          }
          case TILEDB_STRING_UTF32:
          case TILEDB_STRING_UCS4: {
            set_buffer_wrapper<char32_t>(
                query,
                attribute_name,
                attr.second.variable_sized(),
                attr.second.nullable(),
                offsets,
                values,
                validity,
                &buffers);
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
                domain, i, query, subarray, &buffer, &expected_results);
            break;
          case TILEDB_UINT8:
            set_query_dimension_buffer<uint8_t>(
                domain, i, query, subarray, &buffer, &expected_results);
            break;
          case TILEDB_INT16:
            set_query_dimension_buffer<int16_t>(
                domain, i, query, subarray, &buffer, &expected_results);
            break;
          case TILEDB_UINT16:
            set_query_dimension_buffer<uint16_t>(
                domain, i, query, subarray, &buffer, &expected_results);
            break;
          case TILEDB_INT32:
            set_query_dimension_buffer<int32_t>(
                domain, i, query, subarray, &buffer, &expected_results);
            break;
          case TILEDB_UINT32:
            set_query_dimension_buffer<uint32_t>(
                domain, i, query, subarray, &buffer, &expected_results);
            break;
          case TILEDB_INT64:
            set_query_dimension_buffer<int64_t>(
                domain, i, query, subarray, &buffer, &expected_results);
            break;
          case TILEDB_UINT64:
            set_query_dimension_buffer<uint64_t>(
                domain, i, query, subarray, &buffer, &expected_results);
            break;
          case TILEDB_FLOAT32:
            set_query_dimension_buffer<float>(
                domain, i, query, subarray, &buffer, &expected_results);
            break;
          case TILEDB_FLOAT64:
            set_query_dimension_buffer<double>(
                domain, i, query, subarray, &buffer, &expected_results);
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
          case TILEDB_TIME_HR:
          case TILEDB_TIME_MIN:
          case TILEDB_TIME_SEC:
          case TILEDB_TIME_MS:
          case TILEDB_TIME_US:
          case TILEDB_TIME_NS:
          case TILEDB_TIME_PS:
          case TILEDB_TIME_FS:
          case TILEDB_TIME_AS:
            set_query_dimension_buffer<int64_t>(
                domain, i, query, subarray, &buffer, &expected_results);
            break;
          case TILEDB_STRING_ASCII: {
            set_query_var_dimension_buffer<char>(
                domain,
                i,
                query,
                subarray,
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
      query->set_subarray(*subarray);
      query->submit();
      delete query;

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
        tuple<uint64_t*, void*, uint8_t*> buffer = buff.second;
        if (std::get<0>(buffer) != nullptr) {
          REQUIRE(std::get<0>(buffer)[0] == 0);
        }

        Attribute attribute = array->schema().attribute(buff.first);
        switch (attribute.type()) {
          case TILEDB_BLOB:
          case TILEDB_GEOM_WKB:
          case TILEDB_GEOM_WKT: {
            REQUIRE(
                static_cast<std::byte*>(std::get<1>(buffer))[0] ==
                static_cast<std::byte>(1));
            break;
          }
          case TILEDB_INT8: {
            REQUIRE(static_cast<int8_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_BOOL:
          case TILEDB_UINT8: {
            REQUIRE(static_cast<uint8_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_INT16: {
            REQUIRE(static_cast<int16_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_UINT16: {
            REQUIRE(static_cast<uint16_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_INT32: {
            REQUIRE(static_cast<int32_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_UINT32: {
            REQUIRE(static_cast<uint32_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_INT64: {
            REQUIRE(static_cast<int64_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_UINT64: {
            REQUIRE(static_cast<uint64_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_FLOAT32: {
            REQUIRE(static_cast<float*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_FLOAT64: {
            REQUIRE(static_cast<double*>(std::get<1>(buffer))[0] == 1);
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
          case TILEDB_DATETIME_AS:
          case TILEDB_TIME_HR:
          case TILEDB_TIME_MIN:
          case TILEDB_TIME_SEC:
          case TILEDB_TIME_MS:
          case TILEDB_TIME_US:
          case TILEDB_TIME_NS:
          case TILEDB_TIME_PS:
          case TILEDB_TIME_FS:
          case TILEDB_TIME_AS: {
            REQUIRE(static_cast<int64_t*>(std::get<1>(buffer))[0] == 1);
            break;
          }
          case TILEDB_CHAR:
          case TILEDB_STRING_ASCII:
          case TILEDB_STRING_UTF8:
          case TILEDB_ANY: {
            REQUIRE(static_cast<char*>(std::get<1>(buffer))[0] == '1');
            break;
          }
          case TILEDB_STRING_UTF16:
          case TILEDB_STRING_UCS2: {
            REQUIRE(static_cast<char16_t*>(std::get<1>(buffer))[0] == u'1');
            break;
          }
          case TILEDB_STRING_UTF32:
          case TILEDB_STRING_UCS4: {
            REQUIRE(static_cast<char32_t*>(std::get<1>(buffer))[0] == U'1');
            break;
          }
        }

        if (std::get<2>(buffer) != nullptr) {
          REQUIRE(std::get<2>(buffer)[0] == 1);
        }

        // Free buffers
        if (std::get<0>(buffer) != nullptr) {
          free(std::get<0>(buffer));
        }
        if (std::get<1>(buffer) != nullptr) {
          free(std::get<1>(buffer));
        }
        if (std::get<2>(buffer) != nullptr) {
          free(std::get<2>(buffer));
        }
      }
      delete array;
    }
  }
}

TEST_CASE(
    "Backwards compatibility: Upgrades an array of older version and "
    "write/read it",
    "[backwards-compat][upgrade-version][write-read-new-version]") {
  std::string array_name(arrays_dir + "/non_split_coords_v1_4_0");
  Context& ctx = vanilla_context_cpp();
  std::string schema_folder;
  std::string fragment_uri;

  // Upgrades version
  Array::upgrade_version(ctx, array_name);

  // Read using upgraded version
  Array array_read1(ctx, array_name, TILEDB_READ);
  std::vector<int> subarray_read1 = {1, 4, 10, 10};
  std::vector<int> a_read1(4);
  std::vector<int> d1_read1(4);
  std::vector<int> d2_read1(4);

  Query query_read1(ctx, array_read1);
  Subarray subarray_r(ctx, array_read1);
  subarray_r.set_subarray(subarray_read1);
  query_read1.set_subarray(subarray_r)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_read1)
      .set_data_buffer("d1", d1_read1)
      .set_data_buffer("d2", d2_read1);

  ServerQueryBuffers server_buffers;
  query_read1.submit();
  array_read1.close();

  for (int i = 0; i < 4; i++) {
    REQUIRE(a_read1[i] == i + 1);
  }

  // Write
  Array array_write(ctx, array_name, TILEDB_WRITE);
  std::vector<int> subarray_write = {1, 2, 10, 10};
  std::vector<int> a_write = {11, 12};
  std::vector<int> d1_write = {1, 2};
  std::vector<int> d2_write = {10, 10};

  Query query_write(ctx, array_write, TILEDB_WRITE);

  query_write.set_layout(TILEDB_GLOBAL_ORDER);
  query_write.set_data_buffer("a", a_write);
  query_write.set_data_buffer("d1", d1_write);
  query_write.set_data_buffer("d2", d2_write);

  query_write.submit_and_finalize();

  array_write.close();

  FragmentInfo fragment_info(ctx, array_name);
  fragment_info.load();
  fragment_uri = fragment_info.fragment_uri(1);

  // old version fragment
  CHECK(fragment_info.version(0) == 1);
  // new version fragment
  CHECK(fragment_info.version(1) == tiledb::sm::constants::format_version);

  // Read again
  Array array_read2(ctx, array_name, TILEDB_READ);
  std::vector<int> subarray_read2 = {1, 4, 10, 10};
  std::vector<int> a_read2(4);
  std::vector<int> d1_read2(4);
  std::vector<int> d2_read2(4);

  Query query_read2(ctx, array_read2);
  Subarray subarray_r2(ctx, array_read2);
  subarray_r2.set_subarray(subarray_read2);
  query_read2.set_subarray(subarray_r2)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_read2)
      .set_data_buffer("d1", d1_read2)
      .set_data_buffer("d2", d2_read2);

  query_read2.submit();
  array_read2.close();

  for (int i = 0; i < 2; i++) {
    REQUIRE(a_read2[i] == i + 11);
  }
  for (int i = 2; i < 3; i++) {
    REQUIRE(a_read2[i] == i + 1);
  }

  // Clean up
  schema_folder = array_read2.uri() + "/__schema";

  VFS vfs(ctx);
  vfs.remove_dir(get_fragment_dir(array_read2.uri()));
  vfs.remove_dir(get_commit_dir(array_read2.uri()));
  vfs.remove_dir(schema_folder);
}

template <class T>
void assert_group_metadata(
    Group& g,
    const std::string& key,
    tiledb_datatype_t expected_datatype,
    T expected_value) {
  tiledb_datatype_t datatype;
  uint32_t value_num;
  const void* value;

  g.get_metadata(key, &datatype, &value_num, &value);
  REQUIRE(datatype == expected_datatype);
  REQUIRE(value_num == 1);
  REQUIRE(value != nullptr);
  REQUIRE(*static_cast<const T*>(value) == expected_value);

  std::vector<T> expected_values(7);
  std::fill(expected_values.begin(), expected_values.end(), expected_value);
  g.get_metadata(key + "_multi", &datatype, &value_num, &value);
  REQUIRE(datatype == expected_datatype);
  REQUIRE(value != nullptr);
  const T* valueT = static_cast<const T*>(value);
  REQUIRE(std::vector<T>(valueT, valueT + value_num) == expected_values);
}

TEST_CASE(
    "Backwards compatibility: Test reading group metadata written with "
    "previous version of tiledb",
    "[backwards-compat][group][metadata]") {
  Context& ctx = vanilla_context_cpp();
  std::string compat_folder(arrays_dir + "/read_compatibility_test");
  if (Object::object(ctx, compat_folder).type() != Object::Type::Group) {
    return;
  }

  std::string encryption_key = "unittestunittestunittestunittest";

  tiledb::ObjectIter versions_iter(ctx, compat_folder);
  for (const auto& group_versions : versions_iter) {
    if (group_versions.type() != Object::Type::Group) {
      continue;
    }

    Group g{ctx, group_versions.uri(), TILEDB_READ};

    if (g.metadata_num() == 0) {
      continue;
    }

    assert_group_metadata<uint8_t>(g, "u8", TILEDB_UINT8, 0x77);
    assert_group_metadata<uint16_t>(g, "u16", TILEDB_UINT16, 0x7777);
    assert_group_metadata<uint32_t>(g, "u32", TILEDB_UINT32, 0x77777777);
    assert_group_metadata<uint64_t>(
        g, "u64", TILEDB_UINT64, 0x7777777777777777);
  }
}

TEST_CASE(
    "Backwards compatibility: Test v1 groups",
    "[backwards-compat][group][v1]") {
  Context& ctx = vanilla_context_cpp();
  VFS vfs(ctx);

  // Copy the group to a temporary directory because we will be modifying it.
  tiledb::sm::TemporaryLocalDirectory temp_dir;
  std::filesystem::copy(
      groups_dir + "/group_v1",
      temp_dir.path(),
      std::filesystem::copy_options::recursive);

  // Read the group
  {
    Group g{ctx, temp_dir.path(), TILEDB_READ};

    CHECK(g.dump(false) != "");
    CHECK(g.member_count() == 1);
  }

  // Add a member to the group
  {
    Group g{ctx, temp_dir.path(), TILEDB_WRITE};

    Group::create(ctx, temp_dir.path() + "/subgroup2");

    g.add_member("subgroup2", true, "subgroup2");

    g.close();
  }

  // Read the group again
  {
    Group g{ctx, temp_dir.path(), TILEDB_READ};

    CHECK(g.dump(false) != "");
    CHECK(g.member_count() == 2);
    CHECK(g.member(1).name() == "subgroup2");
  }

  // Read the raw group details files
  auto children = vfs.ls(temp_dir.path() + "/__group");
  CHECK(children.size() == 2);
  std::sort(children.begin(), children.end());
  CHECK(!tiledb::sm::utils::parse::ends_with(children[0], "_1"));
  // This is the file written by this test.
  CHECK(tiledb::sm::utils::parse::ends_with(children[1], "_1"));
}

TEST_CASE("VanillaContext static option") {
  static std::optional<Context> vanilla_impl;
  vanilla_impl.emplace();
}

TEST_CASE("VanillaContext static non-option") {
  static Context vanilla_impl;
}
