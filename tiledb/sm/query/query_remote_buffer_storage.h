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

#include <numeric>
#include "tiledb/common/common.h"
#include "tiledb/common/macros.h"
#include "tiledb/common/types/dynamic_typed_datum.h"
#include "tiledb/common/types/untyped_datum.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/query/validity_vector.h"

using namespace tiledb::common;

namespace tiledb::sm {
class QueryRemoteBufferStorageException : public StatusException {
 public:
  explicit QueryRemoteBufferStorageException(const std::string& msg)
      : StatusException("QueryRemoteBufferStorage", msg) {
  }
};

struct QueryRemoteBufferStorage {
  Buffer buffer;
  Buffer buffer_var;
  Buffer buffer_validity;

  /** Size of a single cell stored in fixed buffer. */
  uint8_t cell_size;

  /**
   * Offset position to use for submission. Data after this offset will be held.
   * Used to preserve the order of cached data.
   */
  uint64_t byte_offset;

  /**
   * Offset position used to partition var-size data buffer.
   * Var size data up to this byte will prepend user buffers on submit.
   * Data after this position will be held for next submit to REST.
   */
  uint64_t var_data_offset;

  /** True if the cache has been submit to REST up to byte_offset position. */
  bool submit = false;

  /**
   * Retrieve an offset at a certain cell index.
   *
   * @param cell_index Index of offset cell to retrieve.
   * @return Pointer to the offset at cell_index.
   */
  uint64_t* get_cell_offset(uint64_t cell_index) {
    if (buffer.size() < cell_index * constants::cell_var_offset_size) {
      throw QueryRemoteBufferStorageException(
          "Attempt to retrieve offset beyond cache bounds.");
    }
    return (uint64_t*)buffer.data(cell_index * constants::cell_var_offset_size);
  }

  /**
   * Shifts data remaining in the cache to the front and adjusts byte offsets.
   * Data at the front of the cache will prepend user buffers.
   *
   * @param is_var True if the cache is for var-size data.
   */
  void shift_cache(bool is_var) {
    uint64_t shift_bytes = buffer.size() - byte_offset;
    uint64_t shift_cells = shift_bytes / cell_size;
    // Shift cached data to overwrite already submitted data.
    throw_if_not_ok(buffer.write(buffer.data(byte_offset), 0, shift_bytes));

    if (is_var) {
      uint64_t shift_var_bytes = buffer_var.size() - var_data_offset;
      throw_if_not_ok(buffer_var.write(
          buffer_var.data(var_data_offset), 0, shift_var_bytes));

      buffer_var.set_offset(shift_var_bytes);
      buffer_var.set_size(shift_var_bytes);
      var_data_offset = shift_var_bytes;

      // If offsets have been shifted to front of cache, make absolute from 0.
      // Helps to simplify appending user buffer data during serialization.
      if (shift_bytes > 0) {
        auto* data = buffer.data_as<uint64_t>();
        std::adjacent_difference(data, data + shift_cells, data);
        // Set the first offset to 0 and ensure others are in ascending order.
        data[0] = 0;
        for (uint64_t j = 1; j < shift_cells; j++) {
          data[j] += data[j - 1];
        }
      }
    }
    buffer.set_offset(shift_bytes);
    buffer.set_size(shift_bytes);

    if (buffer_validity.alloced_size() > 0) {
      throw_if_not_ok(buffer_validity.write(
          buffer_validity.data(byte_offset / cell_size), 0, shift_cells));
      buffer_validity.set_offset(shift_cells);
      buffer_validity.set_size(shift_cells);
    }

    // Any cached data after this position will be held for next submits.
    // Helps to preserve order of cached data between submits.
    byte_offset = shift_bytes;
    submit = false;
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_QUERY_REMOTE_BUFFER_H
