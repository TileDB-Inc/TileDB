/**
 * @file   crypto.cc
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
 * This file defines a platform-independent crypto interface.
 */

#include "tiledb/sm/crypto/crypto.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"

#ifdef _WIN32
#include "tiledb/sm/crypto/crypto_win32.h"
#else
#include "tiledb/sm/crypto/crypto_openssl.h"
#endif

using namespace tiledb::common;

namespace tiledb {
namespace sm {

Status Crypto::encrypt_aes256gcm(
    ConstBuffer* key,
    ConstBuffer* iv,
    ConstBuffer* input,
    Buffer* output,
    PreallocatedBuffer* output_iv,
    PreallocatedBuffer* output_tag) {
  if (key->size() != AES256GCM_KEY_BYTES)
    return LOG_STATUS(
        Status_EncryptionError("AES-256-GCM error; unexpected key length."));
  if (iv != nullptr && iv->size() != AES256GCM_IV_BYTES)
    return LOG_STATUS(
        Status_EncryptionError("AES-256-GCM error; unexpected IV length."));
  if (output_iv == nullptr || output_iv->size() != AES256GCM_IV_BYTES)
    return LOG_STATUS(
        Status_EncryptionError("AES-256-GCM error; invalid output IV buffer."));
  if (output_tag == nullptr || output_tag->size() != AES256GCM_TAG_BYTES)
    return LOG_STATUS(Status_EncryptionError(
        "AES-256-GCM error; invalid output tag buffer."));

#ifdef _WIN32
  return Win32CNG::encrypt_aes256gcm(
      key, iv, input, output, output_iv, output_tag);
#else
  return OpenSSL::encrypt_aes256gcm(
      key, iv, input, output, output_iv, output_tag);
#endif
}

Status Crypto::decrypt_aes256gcm(
    ConstBuffer* key,
    ConstBuffer* iv,
    ConstBuffer* tag,
    ConstBuffer* input,
    Buffer* output) {
  if (key == nullptr || key->size() != AES256GCM_KEY_BYTES)
    return LOG_STATUS(
        Status_EncryptionError("AES-256-GCM error; invalid key."));
  if (iv == nullptr || iv->size() != AES256GCM_IV_BYTES)
    return LOG_STATUS(Status_EncryptionError("AES-256-GCM error; invalid IV."));
  if (tag == nullptr || tag->size() != AES256GCM_TAG_BYTES)
    return LOG_STATUS(
        Status_EncryptionError("AES-256-GCM error; invalid tag."));

#ifdef _WIN32
  return Win32CNG::decrypt_aes256gcm(key, iv, tag, input, output);
#else
  return OpenSSL::decrypt_aes256gcm(key, iv, tag, input, output);
#endif
}

Status Crypto::md5(ConstBuffer* input, Buffer* output) {
  return md5(input, input->size(), output);
}

Status Crypto::md5(
    ConstBuffer* input, uint64_t input_read_size, Buffer* output) {
  return md5(input->data(), input_read_size, output);
}

Status Crypto::md5(
    const void* input, uint64_t input_read_size, Buffer* output) {
#ifdef _WIN32
  return Win32CNG::md5(input, input_read_size, output);
#else
  return OpenSSL::md5(input, input_read_size, output);
#endif
}

Status Crypto::sha256(ConstBuffer* input, Buffer* output) {
  return sha256(input, input->size(), output);
}

Status Crypto::sha256(
    ConstBuffer* input, uint64_t input_read_size, Buffer* output) {
  return sha256(input->data(), input_read_size, output);
}

Status Crypto::sha256(
    const void* input, uint64_t input_read_size, Buffer* output) {
#ifdef _WIN32
  return Win32CNG::sha256(input, input_read_size, output);
#else
  return OpenSSL::sha256(input, input_read_size, output);
#endif
}

}  // namespace sm
}  // namespace tiledb
