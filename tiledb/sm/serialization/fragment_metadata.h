/**
 * @file   fragment_metadata.h
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
 * This file declares serialization functions for fragment metadata.
 */

#ifndef TILEDB_SERIALIZATION_FRAGMENT_METADATA_H
#define TILEDB_SERIALIZATION_FRAGMENT_METADATA_H

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/fragment/fragment_metadata.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class FragmentMetadata;
enum class SerializationType : uint8_t;

namespace serialization {

#ifdef TILEDB_SERIALIZATION

/**
 * Convert Cap'n Proto message to Fragment Metadata
 *
 * @param array_schema the schema of the array the metadata belongs
 * @param frag_meta_reader cap'n proto class
 * @param frag_meta fragment metadata object to deserialize into
 * @return Status
 */
Status fragment_metadata_from_capnp(
    const shared_ptr<const ArraySchema>& array_schema,
    const capnp::FragmentMetadata::Reader& frag_meta_reader,
    shared_ptr<FragmentMetadata> frag_meta);

/**
 * Convert Fragment Metadata to Cap'n Proto message
 *
 * @param frag_meta fragment metadata to serialize
 * @param frag_meta_builder cap'n proto class
 * @return Status
 */
Status fragment_metadata_to_capnp(
    const FragmentMetadata& frag_meta,
    capnp::FragmentMetadata::Builder* frag_meta_builder);

#endif

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION_FRAGMENT_METADATA_H