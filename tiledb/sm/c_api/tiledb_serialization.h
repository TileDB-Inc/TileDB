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

#ifndef TILEDB_SERIALIZATION_H
#define TILEDB_SERIALIZATION_H

#include "tiledb.h"
#include "tiledb_experimental.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Serialization type. */
typedef enum {
/** Helper macro for defining array type enums. */
#define TILEDB_SERIALIZATION_TYPE_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_SERIALIZATION_TYPE_ENUM
} tiledb_serialization_type_t;

/**
 * Returns a string representation of the given serialization type.
 *
 * @param serialization_type Serialization type
 * @param str Set to point to a constant string representation of the
 * serialization type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialization_type_to_str(
    tiledb_serialization_type_t serialization_type,
    const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a serialization type from the given string.
 *
 * @param str String representation to parse
 * @param serialization_type Set to the parsed serialization type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialization_type_from_str(
    const char* str,
    tiledb_serialization_type_t* serialization_type) TILEDB_NOEXCEPT;

/* ****************************** */
/*          Serialization         */
/* ****************************** */

/**
 * Serializes the given array.
 *
 * @note The caller must free the returned `tiledb_buffer_t`.
 *
 * @param ctx The TileDB context.
 * @param array The array to serialize.
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, serialize from "client-side" perspective.
 *    Else, "server-side."
 * @param buffer Will be set to a newly allocated buffer containing the
 *      serialized max buffer sizes.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_array(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Deserializes a new array from the given buffer.
 *
 * @param ctx The TileDB context.
 * @param buffer Buffer to deserialize from
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, deserialize from "client-side" perspective.
 *    Else, "server-side."
 * @param array Will be set to a newly allocated array.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_array(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_array_t** array) TILEDB_NOEXCEPT;

/**
 * Serializes the given array schema.
 *
 * @note The caller must free the returned `tiledb_buffer_t`.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema to serialize.
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, serialize from "client-side" perspective.
 *    Else, "server-side."
 * @param buffer Will be set to a newly allocated buffer containing the
 *      serialized max buffer sizes.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_array_schema(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Deserializes a new array schema from the given buffer.
 *
 * @param ctx The TileDB context.
 * @param buffer Buffer to deserialize from
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, deserialize from "client-side" perspective.
 *    Else, "server-side."
 * @param array_schema Will be set to a newly allocated array schema.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_array_schema(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT;

/**
 * Serializes the given array open information into the given buffer.
 *
 * @note The caller must free the returned `tiledb_buffer_t`.
 *
 * @param ctx The TileDB context.
 * @param array The array to get the information to serialize from.
 * @param serialization_type Type of serialization to use.
 * @param client_side Allows to specify different behavior depending on who is
 * serializing, the client (1) or the Cloud server (0). This is sometimes needed
 * since they are both using the same Core library APIs for serialization.
 * @param tiledb_buffer_t Will be set to a newly allocated buffer containing
 *    the serialized array open information.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_array_open(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer_list) TILEDB_NOEXCEPT;

/**
 * Deserializes into an existing array from the given buffer.
 *
 * @note The deserialization is zero-copy, so the source buffer must exceed
 * the lifetime of the array being deserialized to.
 *
 * @param ctx The TileDB context.
 * @param buffer Buffer to deserialize from.
 * @param serialization_type Type of deserialization to use.
 * @param client_side Allows to specify different behavior depending on who is
 * serializing, the client (1) or the Cloud server (0). This is sometimes needed
 * since they are both using the same Core library APIs for serialization.
 * @param array The array object to deserialize into (must be pre-allocated).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_array_open(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_array_t** array) TILEDB_NOEXCEPT;

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
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

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
    tiledb_array_schema_evolution_t** array_schema_evolution) TILEDB_NOEXCEPT;

/**
 * Serializes the given query.
 *
 * Where possible the serialization is zero-copy. The returned buffer list
 * contains an ordered list of pointers to buffers that logically contain the
 * entire serialized query when concatenated.
 *
 * @note The caller must free the returned `tiledb_buffer_list_t`.
 *
 * @param ctx The TileDB context.
 * @param query The query.
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, deserialize from "client-side" perspective.
 *    Else, "server-side."
 * @param buffer_list Will be set to a newly allocated buffer list containing
 *    the serialized query.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_query(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_list_t** buffer_list) TILEDB_NOEXCEPT;

/**
 * Deserializes into an existing query from the given buffer.
 *
 * @note The deserialization is zero-copy, so the source buffer must exceed
 * the lifetime of the query being deserialized to.
 *
 * @param ctx The TileDB context.
 * @param buffer Buffer to deserialize from
 * @param serialization_type Type of deserialization to use
 * @param client_side If set to 1, deserialize from "client-side" perspective.
 *    Else, "server-side."
 * @param query The query object to deserialize into (must be pre-allocated).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_query(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_query_t* query) TILEDB_NOEXCEPT;

/**
 * Serializes the given non-empty domain information into the given buffer.
 *
 * @note The caller must free the returned `tiledb_buffer_t`.
 *
 * @param ctx The TileDB context.
 * @param array Array to which the domain belongs to
 * @param nonempty_domain The domain to serialize
 * @param is_empty 1 if the domain is empty
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, serialize from "client-side" perspective.
 *    Else, "server-side."
 * @param buffer Will be set to a newly allocated buffer containing the
 *      serialized max buffer sizes.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_array_nonempty_domain(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const void* nonempty_domain,
    int32_t is_empty,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Deserializes non-empty domain information from the given buffer.
 *
 * @param ctx The TileDB context.
 * @param array Array to which the domain belongs to
 * @param buffer Buffer to deserialize from
 * @param serialization_type Type of deserialization to use
 * @param client_side If set to 1, deserialize from "client-side" perspective.
 *    Else, "server-side."
 * @param nonempty_domain The buffer to deserialize into
 * @param is_empty Will be set to 1 if the domain is empty
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_array_nonempty_domain(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    void* nonempty_domain,
    int32_t* is_empty) TILEDB_NOEXCEPT;

/**
 * Serializes the given non-empty domain information into the given buffer.
 *
 * @note The caller must free the returned `tiledb_buffer_t`.
 *
 * @param ctx The TileDB context.
 * @param Array array to which the domain belongs to
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, serialize from "client-side" perspective.
 *    Else, "server-side."
 * @param buffer Will be set to a newly allocated buffer containing the
 *      serialized max buffer sizes.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_array_non_empty_domain_all_dimensions(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Deserializes non-empty domain information from the given buffer.
 *
 * @param ctx The TileDB context.
 * @param array Array to which the domain belongs to
 * @param buffer Buffer to deserialize from
 * @param serialization_type Type of deserialization to use
 * @param client_side If set to 1, deserialize from "client-side" perspective.
 *    Else, "server-side."
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_array_non_empty_domain_all_dimensions(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side) TILEDB_NOEXCEPT;

/**
 * Serializes the array max buffer sizes information into the given buffer.
 *
 * @note The caller must free the returned `tiledb_buffer_t`.
 *
 * @param ctx The TileDB context.
 * @param array Array to which the subarray belongs to
 * @param subarray The subarray to use for max buffer size calculation.
 * @param serialization_type Type of serialization to use
 * @param buffer Will be set to a newly allocated buffer containing the
 *      serialized max buffer sizes.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_array_max_buffer_sizes(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const void* subarray,
    tiledb_serialization_type_t serialize_type,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Serializes the array metadata into the given buffer.
 *
 * @note The caller must free the returned `tiledb_buffer_t`.
 *
 * @param ctx The TileDB context.
 * @param array Array whose metadata to serialize.
 * @param serialization_type Type of serialization to use
 * @param buffer Will be set to a newly allocated buffer containing the
 *      serialized array metadata.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_array_metadata(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Sets the array metadata on the given array instance by deserializing the
 * array metadata from the given buffer.
 *
 * @param ctx The TileDB context.
 * @param array Array whose metadata to set.
 * @param serialization_type Type of serialization to use
 * @param buffer Buffer containing serialized array metadata.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_array_metadata(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* buffer) TILEDB_NOEXCEPT;

/**
 * Serializes the given query's estimated result sizes map.
 *
 * @note The caller must free the returned `tiledb_buffer_list_t`.
 *
 * @param ctx The TileDB context.
 * @param query The query to get the estimated result sizes of.
 * @param serialization_type Type of serialization to use
 * @param client_side currently unused
 * @param buffer Will be set to a newly allocated buffer containing
 *    the serialized query estimated result sizes.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_query_est_result_sizes(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Deserializes into an existing query from the given buffer.
 *
 *
 * @param ctx The TileDB context.
 * @param query The query object to deserialize into (must be pre-allocated).
 * @param serialization_type Type of deserialization to use
 * @param client_side currently unused
 * @param buffer Buffer to deserialize from
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_query_est_result_sizes(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    const tiledb_buffer_t* buffer) TILEDB_NOEXCEPT;

/**
 * Serializes the given config.
 *
 * @note The caller must free the returned `tiledb_buffer_t`.
 *
 * @param ctx The TileDB context.
 * @param config The config to serialize.
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, serialize from "client-side" perspective.
 *    Else, "server-side.". Currently unused for config
 * @param buffer Will be set to a newly allocated buffer containing the
 *      serialized max buffer sizes.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_config(
    tiledb_ctx_t* ctx,
    const tiledb_config_t* config,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Deserializes a new config from the given buffer.
 *
 * @param ctx The TileDB context.
 * @param buffer Buffer to deserialize from
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, deserialize from "client-side" perspective.
 *    Else, "server-side.". Currently unused for config
 * @param config Will be set to a newly allocated config.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_config(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_config_t** config) TILEDB_NOEXCEPT;

/**
 * Serializes the fragment info into the given buffer.
 *
 * @note The caller must free the returned `tiledb_buffer_t`.
 *
 * @param ctx The TileDB context.
 * @param fragment_info Fragment info to serialize.
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, serialize from "client-side" perspective.
 *    Else, "server-side."
 * @param buffer Will be set to a newly allocated buffer containing the
 *      serialized fragment info.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_fragment_info(
    tiledb_ctx_t* ctx,
    const tiledb_fragment_info_t* fragment_info,
    tiledb_serialization_type_t serialization_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Populates the fragment info by deserializing from the given buffer.
 *
 * @param ctx The TileDB context.
 * @param buffer Buffer containing serialized fragment info.
 * @param serialization_type Type of serialization to use
 * @param fragment_info Fragment info to deserialize into.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_fragment_info(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    tiledb_fragment_info_t* fragment_info) TILEDB_NOEXCEPT;

/**
 * Serializes the given group.
 *
 * Where possible the serialization is zero-copy. The returned buffer list
 * contains an ordered list of pointers to buffers that logically contain the
 * entire serialized group when concatenated.
 *
 * @note The caller must free the returned `tiledb_buffer_list_t`.
 *
 * @param ctx The TileDB context.
 * @param group The group.
 * @param serialization_type Type of serialization to use
 * @param client_side If set to 1, deserialize from "client-side" perspective.
 *    Else, "server-side."
 * @param tiledb_buffer_t Will be set to a newly allocated buffer containing
 *    the serialized group.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_group(
    tiledb_ctx_t* ctx,
    const tiledb_group_t* group,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer_list) TILEDB_NOEXCEPT;

/**
 * Deserializes into an existing group from the given buffer.
 *
 * @note The deserialization is zero-copy, so the source buffer must exceed
 * the lifetime of the group being deserialized to.
 *
 * @param ctx The TileDB context.
 * @param buffer Buffer to deserialize from
 * @param serialization_type Type of deserialization to use
 * @param client_side If set to 1, deserialize from "client-side" perspective.
 *    Else, "server-side."
 * @param group The group object to deserialize into (must be pre-allocated).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_group(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_group_t* group) TILEDB_NOEXCEPT;

/**
 * Serializes the group metadata into the given buffer.
 *
 * @note The caller must free the returned `tiledb_buffer_t`.
 *
 * @param ctx The TileDB context.
 * @param group Group whose metadata to serialize.
 * @param serialization_type Type of serialization to use
 * @param buffer Will be set to a newly allocated buffer containing the
 *      serialized group metadata.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_serialize_group_metadata(
    tiledb_ctx_t* ctx,
    const tiledb_group_t* group,
    tiledb_serialization_type_t serialization_type,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT;

/**
 * Sets the group metadata on the given group instance by deserializing the
 * group metadata from the given buffer.
 *
 * @param ctx The TileDB context.
 * @param group Group whose metadata to set.
 * @param serialization_type Type of serialization to use
 * @param buffer Buffer containing serialized group metadata.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_deserialize_group_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* buffer) TILEDB_NOEXCEPT;
#ifdef __cplusplus
}
#endif

#endif  // TILEDB_SERIALIZATION_H
