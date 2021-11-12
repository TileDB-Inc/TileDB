/*
 * @file   tiledb_serialization.h
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
 * This file declares the TileDB C API for serialization.
 */

#ifndef TILEDB_EXPERIMENTAL_SERIALIZATION_H
#define TILEDB_EXPERIMENTAL_SERIALIZATION_H

#include "tiledb.h"
#include "tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ****************************** */
/*          Serialization         */
/* ****************************** */

/**
 * Serializes the given array schema evolution.
 *
 * @note The caller must free the returned `tiledb_buffer_t`.
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The array schema evolution to serialize.
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, serialize from "client-side" perspective.
 *    Else, "server-side."
 * @param buffer Will be set to a newly allocated buffer containing the
 *      serialized max buffer sizes.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_array_schema_evolution(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer);

/**
 * Deserializes a new array schema evolution object from the given buffer.
 *
 * @param ctx The TileDB context.
 * @param buffer Buffer to deserialize from
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, deserialize from "client-side" perspective.
 *    Else, "server-side."
 * @param array_schema_evolution Will be set to a newly allocated array schema
 * evolution.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_array_schema_evolution(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_array_schema_evolution_t** array_schema_evolution);

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_EXPERIMENTAL_SERIALIZATION_H
