/**
 * @file   encryption.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 * This file declares a platform-independent encryption interface.
 */

#ifndef TILEDB_ENCRYPTION_H
#define TILEDB_ENCRYPTION_H

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/buffer/preallocated_buffer.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/** Class implementing encryption methods. */
class Encryption {
 public:
  /** Size of an AES-256-GCM block in bytes. */
  static const unsigned AES256GCM_BLOCK_BYTES = 16;
  /** Size of an AES-256-GCM key in bytes. */
  static const unsigned AES256GCM_KEY_BYTES = 32;
  /** Size of an AES-256-GCM IV in bytes. */
  static const unsigned AES256GCM_IV_BYTES = 12;
  /** Size of an AES-256-GCM tag in bytes. */
  static const unsigned AES256GCM_TAG_BYTES = 16;

  /**
   * Encrypt the given data using AES-256-GCM.
   *
   * @param key Secret key.
   * @param iv If non-null, the initialization vector to use. It is recommended
   *    to always leave this null.
   * @param input Plaintext to encrypt.
   * @param output Buffer to store encrypted bytes.
   * @param output_iv Buffer to store the IV that was used.
   * @param output_tag Buffer to store the GCM tag that was computed.
   * @return Status
   */
  static Status encrypt_aes256gcm(
      ConstBuffer* key,
      ConstBuffer* iv,
      ConstBuffer* input,
      Buffer* output,
      PreallocatedBuffer* output_iv,
      PreallocatedBuffer* output_tag);

  /**
   * Decrypt the given data using AES-256-GCM.
   *
   * @param key Secret key.
   * @param iv The initialization vector to use.
   * @param tag The GCM tag to use.
   * @param input Ciphertext to decrypt.
   * @param output Buffer to store decrypted bytes.
   * @return Status
   */
  static Status decrypt_aes256gcm(
      ConstBuffer* key,
      ConstBuffer* iv,
      ConstBuffer* tag,
      ConstBuffer* input,
      Buffer* output);
};

}  // namespace sm
}  // namespace tiledb

#endif