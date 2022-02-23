/**
 * @file   unit_interval.h
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
 * This file defines common test functions for class BlobArray/BlobArraySchema.
 */

#ifndef TILEDB_UNIT_BLOB_ARRAY_H
#define TILEDB_UNIT_BLOB_ARRAY_H

#include <catch.hpp>

#include "../blob_array.h"
#include "../blob_array_schema.h"
#include "tiledb/common/common.h"

// using namespace tiledb::common;

namespace tiledb {
namespace appl {

class WhiteboxBlobArray : public BlobArray {
  typedef BlobArray Base;

 public:
  WhiteboxBlobArray(const BlobArray& src)
      : BlobArray(src) {
  }

  Status get_mime_type(
      // const char* key,
      Datatype* value_type,
      uint32_t* value_size,
      const void** value) /* const */ {
    return this->Base::get_metadata(
        constants::blob_array_metadata_mime_type_key.c_str(),
        // key,     // constants::blob_array_metadata_mime_type_key.c_str(),
        value_type,  //&datatype, //Datatype::STRING_ASCII,
        value_size,  //&mime_size,
        value);      //&mime);
  }
  Status get_mime_encoding(
      Datatype* value_type,
      uint32_t* value_size,
      const void** value) /* const */ {
    return this->Base::get_metadata(
        constants::blob_array_metadata_mime_encoding_key.c_str(),
        // key,     // constants::blob_array_metadata_mime_type_key.c_str(),
        value_type,  //&datatype, //Datatype::STRING_ASCII,
        value_size,  //&mime_size,
        value);      //&mime);
  }
};

}  // namespace appl
}  // namespace tiledb

#endif  //