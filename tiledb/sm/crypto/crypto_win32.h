/**
 * @file   crypto_win32.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file declares a Win32 crypto interface.
 */

#ifndef TILEDB_CRYPTO_WIN32_H
#define TILEDB_CRYPTO_WIN32_H

#ifdef _WIN32

#if !defined(NOMINMAX)
#define NOMINMAX
#endif
#include <windows.h>

#include <bcrypt.h>

#include "tiledb/common/status.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class PreallocatedBuffer;

/** Class encapsulating encryption/decryption using the Win32 CNG interface. */
class Win32CNG {
 public:
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

  /**
   * Compute md5 checksum of data
   *
   * @param input Plaintext to compute hash of
   * @param input_read_size size of input to read for hash
   * @param output Buffer to store store hash bytes.
   * @return Status
   */
  static Status md5(
      const void* input, uint64_t input_read_size, Buffer* output) {
    return hash_bytes(input, input_read_size, output, BCRYPT_MD5_ALGORITHM);
  }

  /**
   * Compute sha256 checksum of data
   *
   * @param input Plaintext to compute hash of
   * @param input_read_size size of input to read for hash
   * @param output Buffer to store store hash bytes.
   * @return Status
   */
  static Status sha256(
      const void* input, uint64_t input_read_size, Buffer* output) {
    return hash_bytes(input, input_read_size, output, BCRYPT_SHA256_ALGORITHM);
  }

 private:
  /**
   *
   * Compute a hash using Win32CNG functions
   *
   * @param input Plaintext to compute hash of
   * @param input_read_size size of input to read for hash
   * @param output Buffer to store store hash bytes.
   * @param alg_handle hash algorithm handle
   * @return Status
   */
  static Status hash_bytes(
      const void* input,
      uint64_t input_read_size,
      Buffer* output,
      LPCWSTR hash_algorithm);
};

}  // namespace sm
}  // namespace tiledb

#endif  // _WIN32

#endif
