/**
 * @file   lidar_filter.h
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
 * This file defines the lidar compressor class.
 */

#ifndef TILEDB_LIDAR_FILTER_H
#define TILEDB_LIDAR_FILTER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filter/xor_filter.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class PreallocatedBuffer;

/** Handles compression/decompression of lidar data (similar to LASzip). 
 * TODO: comment more
*/
class Lidar {
 public:
  /**
   * Compression function.
   *
   * @param level Compression level.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write to the compressed data.
   * @return Status
   */
  static Status compress(
      Datatype type, int level, ConstBuffer* input_buffer, Buffer* output_buffer);

  /**
   * Overloaded compression function with default compression level.
   *
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write to the compressed data.
   * @return Status
   */
  static Status compress(Datatype type, ConstBuffer* input_buffer, Buffer* output_buffer);

  /**
   * Decompression function.
   *
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write the decompressed data to.
   * @return Status
   */
  static Status decompress(
      Datatype type, ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);

  /** Returns the default compression level. */
  static int default_level() {
    return default_level_;
  }

  /** Returns the compression overhead for the given input. */
  static uint64_t overhead(uint64_t nbytes);

  private:
    template<typename W>
    static Status compress(Datatype type, int level, ConstBuffer* input_buffer, Buffer* output_buffer);

    template<typename W>
    static Status decompress(ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);

    /** The default filter compression level. */
    static constexpr int default_level_ = -1;

    static XORFilter xor_filter_;

    /** Thread pool for compute-bound tasks. */
    static ThreadPool* compute_tp_;
};

}; // namespace sm
}  // namespace tiledb

#endif  // TILEDB_LIDAR_FILTER_H