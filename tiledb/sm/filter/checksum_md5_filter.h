/**
 * @file   checksum_md5_filter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file declares class ChecksumMD5Filter.
 */

#ifndef TILEDB_CHECKSUM_MD5_FILTER_H
#define TILEDB_CHECKSUM_MD5_FILTER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/filter/filter.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * A filter that computes a checksum of the input data into the output data
 * buffer with user specified algorithm.
 *
 * If the input comes in multiple FilterBuffer parts, each part is checksummed
 * independently in the forward direction. Input metadata is checksummed as
 * well.
 *
 * The forward output metadata has the format:
 *   uint32_t - number of metadata checksums
 *   uint32_t - number of data checksum
 *   metadata_checksum_part0
 *   ...
 *   metadata_checksum__partN
 *   data_checksum_part0
 *   ...
 *   data_checksum__partN
 *   input_metadata
 *
 *   Where checksum_part is
 *   uint64_t size of part that checksum is computed over
 *   uint8_t[16] checksum

 * The forward output data format is just the input bytes forwarded untouched
 *
 * The reverse output data format is simply:
 *   uint8_t[] - Original input data
 */
class ChecksumMD5Filter : public Filter {
 public:
  /**
   * Constructor.
   *
   * @param filter_data_type Datatype the filter will operate on.
   */
  ChecksumMD5Filter(Datatype filter_data_type);

  /**
   * Encrypt the bytes of the input data into the output data buffer.
   */
  void run_forward(
      const WriterTile& tile,
      WriterTile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Decrypt the bytes of the input data into the output data buffer.
   */
  Status run_reverse(
      const Tile& tile,
      Tile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

 protected:
  /** Dumps the filter details in ASCII format in the selected output string. */
  std::ostream& output(std::ostream& os) const override;

 private:
  /** Returns a new clone of this filter. */
  ChecksumMD5Filter* clone_impl() const override;

  /**
   * Compares a passed checksum to a computed on for the part passed
   *
   * @param part Plaintext to checksum
   * @param bytes_to_compared size of bytes to checksum
   * @param checksum checksum to compare against
   * @return Status
   */
  Status compare_checksum_part(
      FilterBuffer* part, uint64_t bytes_to_compared, void* checksum) const;

  /**
   * Compute and store the checksum
   *
   * @param part Plaintext to checksum
   * @param output_metadata Metadata to store checksum in
   * @return Status
   */
  Status checksum_part(ConstBuffer* part, FilterBuffer* output_metadata) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CHECKSUM_MD5_FILTER_H
