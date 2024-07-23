/**
 * @file   consolidation.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2023 TileDB, Inc.
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
 * This file declares serialization for the Consolidation class
 */

#ifndef TILEDB_SERIALIZATION_CONSOLIDATION_H
#define TILEDB_SERIALIZATION_CONSOLIDATION_H

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/consolidation_plan/consolidation_plan.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class Config;
enum class SerializationType : uint8_t;

namespace serialization {

#ifdef TILEDB_SERIALIZATION

/**
 * Convert Cap'n Proto message to Consolidation request
 *
 * @param array_uri URI of the array
 * @param consolidation_req_reader cap'n proto class.
 * @return {config, fragment_uris} config object to deserialize into, and the
 * uris of the fragments to be consolidated if any
 */
std::pair<Config, std::optional<std::vector<std::string>>>
array_consolidation_request_from_capnp(
    const std::string& array_uri,
    const capnp::ArrayConsolidationRequest::Reader& consolidation_req_reader);

/**
 * Convert Consolidation Request to Cap'n Proto message.
 *
 * @param config config to serialize info from
 * @param fragment_uris The uris of the fragments to be consolidated if this is
 * a request for fragment list consolidation
 * @param consolidation_req_builder cap'n proto class.
 */
void array_consolidation_request_to_capnp(
    const Config& config,
    const std::vector<std::string>* fragment_uris,
    capnp::ArrayConsolidationRequest::Builder* consolidation_req_builder);
#endif

/**
 * Serialize a consolidation request via Cap'n Proto.
 *
 * @param config config object to get info to serialize.
 * @param fragment_uris The uris of the fragments to be consolidated if this is
 * a request for fragment list consolidation
 * @param serialize_type format to serialize into Cap'n Proto or JSON.
 * @param serialized_buffer buffer to store serialized bytes in.
 */
void array_consolidation_request_serialize(
    const Config& config,
    const std::vector<std::string>* fragment_uris,
    SerializationType serialize_type,
    Buffer* serialized_buffer);

/**
 * Deserialize consolidation request via Cap'n Proto
 *
 * @param array_uri URI of the array
 * @param serialize_type format the data is serialized in: Cap'n Proto of JSON.
 * @param serialized_buffer buffer to read serialized bytes from.
 * @return {config, fragment_uris} config object to deserialize into, and the
 * uris of the fragments to be consolidated if any
 */
std::pair<Config, std::optional<std::vector<std::string>>>
array_consolidation_request_deserialize(
    const std::string& array_uri,
    SerializationType serialize_type,
    const Buffer& serialized_buffer);

/**
 * Serialize a consolidation plan request via Cap'n Proto.
 *
 * @param fragment_size maximum fragment size for the cosnolidateion plan.
 * @param config config object to serialize.
 * @param serialization_type format to serialize into Cap'n Proto or JSON.
 * @param request buffer to store serialized bytes in.
 */
void serialize_consolidation_plan_request(
    uint64_t fragment_size,
    const Config& config,
    SerializationType serialization_type,
    Buffer& request);

/**
 * Deserialize a consolidation plan request via Cap'n Proto.
 *
 * @param serialization_type format the data is serialized in: Cap'n Proto of
 * JSON.
 * @param response buffer to read serialized bytes from.
 * @return the deserialized maximum fragment size
 */
uint64_t deserialize_consolidation_plan_request(
    SerializationType serialization_type, const Buffer& request);

/**
 * Serialize a consolidation plan response via Cap'n Proto.
 *
 * @param consolidation_plan consolidation plan to serialize.
 * @param serialization_type format to serialize into Cap'n Proto or JSON.
 * @param response buffer to store serialized bytes in.
 */
void serialize_consolidation_plan_response(
    const ConsolidationPlan& consolidation_plan,
    SerializationType serialization_type,
    Buffer& response);

/**
 * Deserialize a consolidation plan response via Cap'n Proto.
 *
 * @param serialization_type format the data is serialized in: Cap'n Proto of
 * JSON.
 * @param response buffer to read serialized bytes from.
 * @return the deserialized consolidation plan info
 */
std::vector<std::vector<std::string>> deserialize_consolidation_plan_response(
    SerializationType serialization_type, const Buffer& response);

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
#endif  // TILEDB_SERIALIZATION_CONSOLIDATION_H
