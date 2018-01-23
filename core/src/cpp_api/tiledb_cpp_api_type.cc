/**
 * @file   tiledb_cpp_api_type.cc
 *
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This defines TileDB datatypes for the C++ API.
 */

#include "tiledb_cpp_api_type.h"

namespace tdb {

namespace impl {

std::string to_str(const tiledb_datatype_t &type) {
  switch (type) {
    case TILEDB_CHAR:
      return "char";
    case TILEDB_INT8:
      return "int8";
    case TILEDB_UINT8:
      return "uint8";
    case TILEDB_INT16:
      return "int16";
    case TILEDB_UINT16:
      return "uint16";
    case TILEDB_INT32:
      return "int32";
    case TILEDB_UINT32:
      return "uint32";
    case TILEDB_INT64:
      return "int64";
    case TILEDB_UINT64:
      return "uint64";
    case TILEDB_FLOAT32:
      return "float32";
    case TILEDB_FLOAT64:
      return "float64";
  }
  return "";
}

size_t type_size(tiledb_datatype_t type) {
  switch (type) {
    case TILEDB_CHAR: return sizeof(typename CHAR::type);
    case TILEDB_INT8: return sizeof(typename INT8::type);
    case TILEDB_UINT8: return sizeof(typename UINT8::type);
    case TILEDB_INT16: return sizeof(typename INT16::type);
    case TILEDB_UINT16: return sizeof(typename UINT16::type);
    case TILEDB_INT32: return sizeof(typename INT32::type);
    case TILEDB_UINT32: return sizeof(typename UINT32::type);
    case TILEDB_INT64: return sizeof(typename INT64::type);
    case TILEDB_UINT64: return sizeof(typename UINT64::type);
    case TILEDB_FLOAT32: return sizeof(typename FLOAT32::type);
    case TILEDB_FLOAT64: return sizeof(typename FLOAT64::type);
  }
  return 0;
}

}  // namespace impl

}  // namespace tdb
