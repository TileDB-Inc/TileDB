/**
 * @file   serialization_wrappers.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file implements some test suite wrapper functions which wrap c-api calls
 * through serialization.
 */

#include <test/support/tdb_catch.h>
#include <string>
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"

int tiledb_array_create_serialization_wrapper(
    tiledb_ctx_t* ctx,
    const std::string& path,
    tiledb_array_schema_t* array_schema,
    bool serialize_array_schema) {
#ifndef TILEDB_SERIALIZATION
  return tiledb_array_create(ctx, path.c_str(), array_schema);
#endif

  if (!serialize_array_schema) {
    return tiledb_array_create(ctx, path.c_str(), array_schema);
  }

  // Serialize the array
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx, array_schema, TILEDB_CAPNP, 1, &buff) == TILEDB_OK);

  // Load array schema from the rest server
  tiledb_array_schema_t* new_array_schema = nullptr;
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx, buff, TILEDB_CAPNP, 0, &new_array_schema) == TILEDB_OK);

  // Create array from new schema
  int rc = tiledb_array_create(ctx, path.c_str(), new_array_schema);

  // Serialize the new array schema and deserialize into the original array
  // schema.
  tiledb_buffer_t* buff2;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx, new_array_schema, TILEDB_CAPNP, 0, &buff2) == TILEDB_OK);
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx, buff2, TILEDB_CAPNP, 1, &array_schema) == TILEDB_OK);

  // Clean up.
  tiledb_array_schema_free(&array_schema);
  tiledb_array_schema_free(&new_array_schema);
  tiledb_buffer_free(&buff);
  tiledb_buffer_free(&buff2);

  return rc;
}

int tiledb_group_serialize(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group_serialized,
    tiledb_group_t* group_deserialized,
    tiledb_serialization_type_t serialize_type) {
  // Serialize and Deserialize
  tiledb_buffer_t* buffer;
  int rc =
      tiledb_serialize_group(ctx, group_serialized, serialize_type, 1, &buffer);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_deserialize_group(
      ctx, buffer, serialize_type, 0, group_deserialized);
  REQUIRE(rc == TILEDB_OK);

  tiledb_buffer_free(&buffer);
  return rc;
}

int tiledb_array_open_serialize(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array_open_serialized,
    tiledb_array_t** array_open_deserialized,
    tiledb_serialization_type_t serialize_type) {
  // Serialize and Deserialize
  tiledb_buffer_t* buffer;
  int rc = tiledb_serialize_array_open(
      ctx, array_open_serialized, serialize_type, 1, &buffer);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_deserialize_array_open(
      ctx, buffer, serialize_type, 0, array_open_deserialized);
  REQUIRE(rc == TILEDB_OK);

  tiledb_buffer_free(&buffer);
  return rc;
}

int tiledb_fragment_info_request_serialize(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info_before_serialization,
    tiledb_fragment_info_t* fragment_info_deserialized,
    tiledb_serialization_type_t serialize_type) {
  // Serialize and Deserialize
  tiledb_buffer_t* buffer;
  int rc = tiledb_serialize_fragment_info_request(
      ctx, fragment_info_before_serialization, serialize_type, 1, &buffer);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_deserialize_fragment_info_request(
      ctx, buffer, serialize_type, 0, fragment_info_deserialized);
  REQUIRE(rc == TILEDB_OK);

  tiledb_buffer_free(&buffer);
  return rc;
}

int tiledb_fragment_info_serialize(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_fragment_info_t* fragment_info_before_serialization,
    tiledb_fragment_info_t* fragment_info_deserialized,
    tiledb_serialization_type_t serialize_type) {
  // Serialize and Deserialize
  tiledb_buffer_t* buffer;
  int rc = tiledb_serialize_fragment_info(
      ctx, fragment_info_before_serialization, serialize_type, 1, &buffer);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_deserialize_fragment_info(
      ctx, buffer, serialize_type, array_uri, 0, fragment_info_deserialized);
  REQUIRE(rc == TILEDB_OK);

  tiledb_buffer_free(&buffer);
  return rc;
}