/**
 * @file   serialization_wrappers.h
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
 * This file declares some test suite wrapper functions which wrap c-api calls
 * through serialization.
 */

#ifndef TILEDB_TEST_SERIALIZATION_WRAPPERS_H
#define TILEDB_TEST_SERIALIZATION_WRAPPERS_H

#include "tiledb/sm/c_api/tiledb.h"

/**
 * Wrap creating the array by round tripping through array schema serialization
 *
 * @param ctx tiledb context
 * @param path path to create the array at
 * @param array_schema array schema to create
 * @param serialize_array_schema should the creation of the schema be
 * round-tripped through serialization or not
 * @return status
 */
int tiledb_array_create_serialization_wrapper(
    tiledb_ctx_t* ctx,
    const std::string& path,
    tiledb_array_schema_t* array_schema,
    bool serialize_array_schema);

/**
 * Wrap an array open serialize/deserialize call
 *
 * @param ctx tiledb context
 * @param array_open_serialized array to serialize
 * @param array_open_deserialized array to deserialize into
 * @param serialize_type serialization format
 * @return status
 */
int tiledb_array_open_serialize(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array_open_serialized,
    tiledb_array_t** array_open_deserialized,
    tiledb_serialization_type_t serialize_type);

/**
 * Wrap a group in serialize/deserialize call
 *
 * @param ctx tiledb context
 * @param group_serialized group to serialize
 * @param group_deserialized group to deserialize into
 * @param serialize_type serialization format
 * @return status
 */
int tiledb_group_serialize(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group_serialized,
    tiledb_group_t* group_deserialized,
    tiledb_serialization_type_t serialize_type);

/**
 * Wrap a fragment info request serialize/deserialize call
 *
 * @param ctx tiledb context
 * @param fragment_info_before_serialization fragment info to serialize from
 * @param fragment_info_deserialized fragment info to deserialize into
 * @param serialize_type serialization format
 * @return status
 */
int tiledb_fragment_info_request_serialize(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info_before_serialization,
    tiledb_fragment_info_t* fragment_info_deserialized,
    tiledb_serialization_type_t serialize_type);

/**
 * Wrap a fragment info serialize/deserialize call
 *
 * @param ctx tiledb context
 * @param array_uri array that fragment info belongs to
 * @param fragment_info_before_serialization fragment info to serialize
 * @param fragment_info_deserialized fragment info to deserialize into
 * @param serialize_type serialization format
 * @return status
 */
int tiledb_fragment_info_serialize(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_fragment_info_t* fragment_info_before_serialization,
    tiledb_fragment_info_t* fragment_info_deserialized,
    tiledb_serialization_type_t serialize_type);

#endif  // TILEDB_TEST_SERIALIZATION_WRAPPERS_H