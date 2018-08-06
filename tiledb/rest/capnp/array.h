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
 * This file declares serialization for
 * tiledb::sm::Attribute/Dimension/Domain/ArraySchema
 */

#ifndef TILEDB_REST_CAPNP_ARRAY_H
#define TILEDB_REST_CAPNP_ARRAY_H

#include "capnp/message.h"

#include "tiledb/rest/capnp/tiledb-rest.capnp.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/serialization_type.h"

#include <memory>

namespace tiledb {
namespace rest {
tiledb::sm::Status filter_to_capnp(
    const tiledb::sm::Filter* f, Filter::Builder* filterBuilder);
std::unique_ptr<tiledb::sm::Filter> filter_from_capnp(
    const Filter::Reader* filterReader);
tiledb::sm::Status filter_pipeline_to_capnp(
    const tiledb::sm::FilterPipeline* f,
    FilterPipeline::Builder* filterPipelineBuilder);
std::unique_ptr<tiledb::sm::FilterPipeline> filter_pipeline_from_capnp(
    const FilterPipeline::Reader* filterPipelineReader);

/**
 * Conversion of a internal tiledb::sm::Attribute to an capnp format
 *
 * @param a attribute to convert
 * @param attributeBuilder capnp builder
 * @return unique_ptr of capnp
 */
tiledb::sm::Status attribute_to_capnp(
    const tiledb::sm::Attribute* a, Attribute::Builder* attributeBuilder);

/**
 * Conversion of a capnp to an internal tiledb::sm::Attribute
 *
 * @param attribute
 * @return unique_ptr to internal tiledb::sm::Attribute
 */
std::unique_ptr<tiledb::sm::Attribute> attribute_from_capnp(
    Attribute::Reader* attribute);

/**
 * Conversion of a internal tiledb::sm::Dimension to an capnp format
 *
 * @param a dimension to convert
 * @param dimensionBuilder capnp builder
 * @return unique_ptr of capnp
 */
tiledb::sm::Status dimension_to_capnp(
    const tiledb::sm::Dimension* d, Dimension::Builder* dimensionBuilder);

/**
 * Conversion of a capnp to an internal tiledb::sm::Dimension
 *
 * @param dimension
 * @return unique_ptr to internal tiledb::sm::Dimension
 */
std::unique_ptr<tiledb::sm::Dimension> dimension_from_capnp(
    Dimension::Reader* dimension);

/**
 * Conversion of a internal tiledb::sm::Domain to an capnp format
 *
 * @param a domain to convert
 * @param domainBuilder capnp builder
 * @return unique_ptr of capnp
 */
tiledb::sm::Status domain_to_capnp(
    const tiledb::sm::Domain* d, Domain::Builder* domainBuilder);

/**
 * Conversion of a capnp to an internal tiledb::sm::Domain
 *
 * @param domain
 * @return unique_ptr to internal tiledb::sm::Domain
 */
std::unique_ptr<tiledb::sm::Domain> domain_from_capnp(Domain::Reader* domain);

/**
 * Conversion of a internal tiledb::sm::ArraySchema to an capnp format
 *
 * @param a arraySchema to convert
 * @param arraySchemaBuilder capnp builder
 * @return unique_ptr of capnp
 */
tiledb::sm::Status array_schema_to_capnp(
    const tiledb::sm::ArraySchema* a, ArraySchema::Builder* arraySchemaBuilder);

/**
 * Conversion of a capnp to an internal tiledb::sm::ArraySchema
 *
 * @param arraySchema
 * @return unique_ptr to internal tiledb::sm::ArraySchema
 */
std::unique_ptr<tiledb::sm::ArraySchema> array_schema_from_capnp(
    ArraySchema::Reader* arraySchema);

tiledb::sm::Status array_schema_serialize(
    tiledb::sm::ArraySchema* array_schema,
    tiledb::sm::SerializationType serialize_type,
    char** serialized_string,
    uint64_t* serialized_string_length);

tiledb::sm::Status array_schema_deserialize(
    tiledb::sm::ArraySchema** array_schema,
    tiledb::sm::SerializationType serialize_type,
    const char* serialized_string,
    const uint64_t serialized_string_length);
}  // namespace rest
}  // namespace tiledb
#endif  // TILEDB_REST_CAPNP_ARRAY_H
