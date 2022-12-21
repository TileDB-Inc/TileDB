/**
 * @file query_remote_buffer_storage.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2022 TileDB, Inc.
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
 * This file defines class query_remote_buffer_storage.
 */

#ifndef TILEDB_QUERY_REMOTE_BUFFER_H
#define TILEDB_QUERY_REMOTE_BUFFER_H

#include "tiledb/common/common.h"
#include "tiledb/common/macros.h"
#include "tiledb/common/types/dynamic_typed_datum.h"
#include "tiledb/common/types/untyped_datum.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/query/validity_vector.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

struct QueryRemoteBufferStorage {
  Buffer buffer;
  Buffer buffer_var;
  Buffer buffer_validity;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_REMOTE_BUFFER_H
