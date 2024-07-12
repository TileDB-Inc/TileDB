/**
 * @file   encryption_aes256gcm_filter.h
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
 * This file declares class EncryptionAES256GCMFilter.
 */

#ifndef TILEDB_ENCRYPTION_AES256GCM_FILTER_H
#define TILEDB_ENCRYPTION_AES256GCM_FILTER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/filter/filter.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class EncryptionKey;

/**
 * A filter that encrypts the input data into the output data buffer with
 * AES-256-GCM.
 *
 * If the input comes in multiple FilterBuffer parts, each part is encrypted
 * independently in the forward direction. Input metadata is encrypted as well.
 *
 * The forward output metadata has the format:
 *   uint32_t - Number of encrypted metadata parts
 *   uint32_t - Number of encrypted data parts
 *   metadata_part0
 *   ...
 *   metadata_partN
 *   data_part0
 *   ...
 *   data_partN
 * Where each metadata_part/data_part has the format:
 *   uint32_t - part plaintext (unencrypted) length
 *   uint32_t - part encrypted length
 *   uint8_t[12] - AES-256-GCM IV bytes
 *   uint8_t[16] - AES-256-GCM tag bytes
 *
 * The forward output data format is just the concatenated encrypted bytes:
 *   uint8_t[] - metadata_part0's array of encrypted bytes
 *   ...
 *   uint8_t[] - metadata_partN's array of encrypted bytes
 *   uint8_t[] - data_part0's array of encrypted bytes
 *   ...
 *   uint8_t[] - data_partN's array of encrypted bytes
 *
 * The reverse output data format is simply:
 *   uint8_t[] - Original input data
 */
class EncryptionAES256GCMFilter : public Filter {
 public:
  /**
   * Constructor.
   *
   * @param filter_data_type Datatype the filter will operate on.
   */
  EncryptionAES256GCMFilter(Datatype filter_data_type);

  /**
   * Constructor with explicit key.
   *
   * @param key Key to use for the filter.
   * @param filter_data_type Datatype the filter will operate on.
   */
  explicit EncryptionAES256GCMFilter(
      const EncryptionKey& key, Datatype filter_data_type);

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

  /**
   * Return a pointer to the secret key set on this filter.
   *
   * @param key_bytes Will store a pointer to the key.
   * @return Status
   */
  Status get_key(const void** key_bytes) const;

  /**
   * Sets the secret key on this filter to a pointer to the given key.
   *
   * @param key Encryption key, expected to hold `uint8_t[32]`.
   */
  void set_key(const EncryptionKey& key);

  /**
   * Sets the secret key on this filter to a pointer to the given key.
   *
   * @param key_bytes Buffer holding the key, expected to be `uint8_t[32]`.
   */
  void set_key(const void* key_bytes);

 protected:
  /** Dumps the filter details in ASCII format in the selected output string. */
  std::ostream& output(std::ostream& os) const override;

 private:
  /** Pointer to a buffer storing the secret key. */
  const void* key_bytes_;

  /** Returns a new clone of this filter. */
  EncryptionAES256GCMFilter* clone_impl() const override;

  /**
   * Decrypt the given input into the given output buffer.
   *
   * @param input Ciphertext to decrypt
   * @param output Buffer to hold decrypted bytes
   * @param input_metadata Metadata about ciphertext
   * @return Status
   */
  Status decrypt_part(
      FilterBuffer* input, Buffer* output, FilterBuffer* input_metadata) const;

  /**
   * Encrypt the given input into the given output buffer.
   *
   * @param input Plaintext to encrypt
   * @param output Buffer to hold encrypted bytes
   * @param output_metadata Metadata about ciphertext
   * @return Status
   */
  Status encrypt_part(
      ConstBuffer* part, Buffer* output, FilterBuffer* output_metadata) const;

  /**
   * Returns the filter output type
   *
   * @param input_type Expected type used for input. Used for filters which
   * change output type based on input data. e.g. XORFilter output type is
   * based on byte width of input type.
   */
  Datatype output_datatype(Datatype input_type) const override;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ENCRYPTION_AES256GCM_FILTER_H
