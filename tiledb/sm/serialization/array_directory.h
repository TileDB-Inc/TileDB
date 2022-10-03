/**
 * @file   array_directory.h
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
 * This file declares serialization functions for Array Directory.
 */

#ifndef TILEDB_SERIALIZATION_ARRAY_DIRECTORY_H
#define TILEDB_SERIALIZATION_ARRAY_DIRECTORY_H

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/array/array_directory.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class ArrayDirectory;
enum class SerializationType : uint8_t;

namespace serialization {

#ifdef TILEDB_SERIALIZATION

/**
 * Convert Cap'n Proto message to Array Directory
 *
 * @param array_dir_reader cap'n proto class
 * @param array_uri uri of the array that the directory belongs to
 * @param array_directory array directory object to deserialize into
 * @return Status
 */
Status array_directory_from_capnp(
    const capnp::ArrayDirectory::Reader& array_directory_reader,
    const URI& array_uri,
    ArrayDirectory* array_directory);

/**
 * Convert Array Directory to Cap'n Proto message
 *
 * @param array_dir array directory to serialize
 * @param array_dir_builder cap'n proto class
 * @return Status
 */
Status array_directory_to_capnp(
    const ArrayDirectory& array_directory,
    capnp::ArrayDirectory::Builder* array_directory_builder);

#endif

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION_ARRAY_DIRECTORY_H