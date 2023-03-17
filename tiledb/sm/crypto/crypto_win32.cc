/**
 * @file   encryption_win32.cc
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
 * This file defines a Win32 crypto interface.
 */

#ifdef _WIN32

#include "tiledb/sm/crypto/crypto_win32.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/crypto/crypto.h"

#ifndef NT_SUCCESS
#define NT_SUCCESS(Status) (((NTSTATUS)(Status)) >= 0)
#endif

#ifndef _MSC_VER
// The MinGW headers do not include the pseudo-handles.
// Constants taken from
// https://docs.rs/winapi/0.3.6/src/winapi/shared/bcrypt.rs.html#424, licensed
// under the MIT or Apache 2.0 licenses.
#define BCRYPT_MD5_ALG_HANDLE 0x00000021
#define BCRYPT_SHA256_ALG_HANDLE 0x00000041
#endif

using namespace tiledb::common;

namespace tiledb {
namespace sm {

Status Win32CNG::get_random_bytes(unsigned num_bytes, Buffer* output) {
  if (output->free_space() < num_bytes)
    RETURN_NOT_OK(output->realloc(output->alloced_size() + num_bytes));

  BCRYPT_ALG_HANDLE alg_handle;
  if (!NT_SUCCESS(BCryptOpenAlgorithmProvider(
          &alg_handle, BCRYPT_RNG_ALGORITHM, nullptr, 0)))
    return Status_EncryptionError(
        "Win32CNG error; generating random bytes: error opening algorithm.");

  if (!NT_SUCCESS(BCryptGenRandom(
          alg_handle, (unsigned char*)output->cur_data(), num_bytes, 0))) {
    BCryptCloseAlgorithmProvider(alg_handle, 0);
    return Status_EncryptionError(
        "Win32CNG error; generating random bytes: error generating bytes.");
  }

  BCryptCloseAlgorithmProvider(alg_handle, 0);

  output->advance_size(num_bytes);
  output->advance_offset(num_bytes);

  return Status::Ok();
}

Status Win32CNG::encrypt_aes256gcm(
    ConstBuffer* key,
    ConstBuffer* iv,
    ConstBuffer* input,
    Buffer* output,
    PreallocatedBuffer* output_iv,
    PreallocatedBuffer* output_tag) {
  // Ensure sufficient space in output buffer.
  auto required_space = input->size() + 2 * Crypto::AES256GCM_BLOCK_BYTES;
  if (output->free_space() < required_space)
    RETURN_NOT_OK(output->realloc(output->alloced_size() + required_space));

  // Generate IV if the given IV buffer is null.
  ULONG iv_len;
  unsigned char* iv_buf;
  Buffer generated_iv;
  if (iv == nullptr || iv->data() == nullptr) {
    RETURN_NOT_OK(get_random_bytes(Crypto::AES256GCM_IV_BYTES, &generated_iv));
    iv_len = (ULONG)generated_iv.size();
    iv_buf = (unsigned char*)generated_iv.data();
  } else {
    iv_len = (ULONG)iv->size();
    iv_buf = (unsigned char*)iv->data();
  }
  // Copy IV to output arg.
  std::memcpy(output_iv->cur_data(), iv_buf, iv_len);

  // Initialize algorithm.
  BCRYPT_ALG_HANDLE alg_handle;
  if (!NT_SUCCESS(BCryptOpenAlgorithmProvider(
          &alg_handle, BCRYPT_AES_ALGORITHM, nullptr, 0))) {
    return LOG_STATUS(Status_EncryptionError(
        "Win32CNG error; error opening algorithm provider."));
  }
  if (!NT_SUCCESS(BCryptSetProperty(
          alg_handle,
          BCRYPT_CHAINING_MODE,
          (PUCHAR)BCRYPT_CHAIN_MODE_GCM,
          sizeof(BCRYPT_CHAIN_MODE_GCM),
          0))) {
    BCryptCloseAlgorithmProvider(alg_handle, 0);
    return LOG_STATUS(
        Status_EncryptionError("Win32CNG error; error setting chaining mode."));
  }

  // Initialize authentication info struct.
  BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO auth_info;
  BCRYPT_INIT_AUTH_MODE_INFO(auth_info);
  auth_info.pbNonce = iv_buf;
  auth_info.cbNonce = iv_len;
  auth_info.pbTag = (unsigned char*)output_tag->data();
  auth_info.cbTag = output_tag->size();
  auth_info.cbData = 0;
  auth_info.dwFlags = 0;

  // Initialize key.
  BCRYPT_KEY_DATA_BLOB_HEADER key_data_header;
  key_data_header.dwMagic = BCRYPT_KEY_DATA_BLOB_MAGIC;
  key_data_header.dwVersion = BCRYPT_KEY_DATA_BLOB_VERSION1;
  key_data_header.cbKeyData = key->size();
  // The key header must be immediately followed in memory by the key bytes,
  // so we have to copy into a new buffer.
  Buffer key_buffer;
  RETURN_NOT_OK(key_buffer.realloc(sizeof(key_data_header) + key->size()));
  std::memcpy(key_buffer.data(), &key_data_header, sizeof(key_data_header));
  std::memcpy(
      (char*)key_buffer.data() + sizeof(key_data_header),
      key->data(),
      key->size());
  key_buffer.advance_size(sizeof(key_data_header) + key->size());

  BCRYPT_KEY_HANDLE key_handle;
  if (!NT_SUCCESS(BCryptImportKey(
          alg_handle,
          nullptr,
          BCRYPT_KEY_DATA_BLOB,
          &key_handle,
          nullptr,
          0,
          (unsigned char*)key_buffer.data(),
          key_buffer.size(),
          0))) {
    BCryptCloseAlgorithmProvider(alg_handle, 0);
    return LOG_STATUS(
        Status_EncryptionError("Win32CNG error; error importing key blob."));
  }

  // Encrypt the input.
  ULONG output_len;
  if (!NT_SUCCESS(BCryptEncrypt(
          key_handle,
          (unsigned char*)input->data(),
          input->size(),
          &auth_info,
          nullptr,
          0,
          (unsigned char*)output->cur_data(),
          required_space,
          &output_len,
          0))) {
    BCryptDestroyKey(key_handle);
    BCryptCloseAlgorithmProvider(alg_handle, 0);
    return LOG_STATUS(
        Status_EncryptionError("Win32CNG error; error encrypting."));
  }

  output->advance_size(output_len);
  output->advance_offset(output_len);

  // Clean up.
  BCryptDestroyKey(key_handle);
  BCryptCloseAlgorithmProvider(alg_handle, 0);

  return Status::Ok();
}

Status Win32CNG::decrypt_aes256gcm(
    ConstBuffer* key,
    ConstBuffer* iv,
    ConstBuffer* tag,
    ConstBuffer* input,
    Buffer* output) {
  // Ensure sufficient space in output buffer.
  auto required_space = input->size();
  if (output->owns_data()) {
    if (output->free_space() < required_space)
      RETURN_NOT_OK(output->realloc(output->alloced_size() + required_space));
  } else if (output->size() < required_space) {
    return LOG_STATUS(Status_EncryptionError(
        "Win32CNG error; cannot decrypt: output buffer too small."));
  }

  // Initialize algorithm.
  BCRYPT_ALG_HANDLE alg_handle;
  if (!NT_SUCCESS(BCryptOpenAlgorithmProvider(
          &alg_handle, BCRYPT_AES_ALGORITHM, nullptr, 0))) {
    return LOG_STATUS(Status_EncryptionError(
        "Win32CNG error; error opening algorithm provider."));
  }
  if (!NT_SUCCESS(BCryptSetProperty(
          alg_handle,
          BCRYPT_CHAINING_MODE,
          (PUCHAR)BCRYPT_CHAIN_MODE_GCM,
          sizeof(BCRYPT_CHAIN_MODE_GCM),
          0))) {
    BCryptCloseAlgorithmProvider(alg_handle, 0);
    return LOG_STATUS(
        Status_EncryptionError("Win32CNG error; error setting chaining mode."));
  }

  // Initialize authentication info struct.
  BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO auth_info;
  BCRYPT_INIT_AUTH_MODE_INFO(auth_info);
  auth_info.pbNonce = (unsigned char*)iv->data();
  auth_info.cbNonce = iv->size();
  auth_info.pbTag = (unsigned char*)tag->data();
  auth_info.cbTag = tag->size();
  auth_info.cbData = 0;
  auth_info.dwFlags = 0;

  // Initialize key.
  BCRYPT_KEY_DATA_BLOB_HEADER key_data_header;
  key_data_header.dwMagic = BCRYPT_KEY_DATA_BLOB_MAGIC;
  key_data_header.dwVersion = BCRYPT_KEY_DATA_BLOB_VERSION1;
  key_data_header.cbKeyData = key->size();
  // The key header must be immediately followed in memory by the key bytes,
  // so we have to copy into a new buffer.
  Buffer key_buffer;
  RETURN_NOT_OK(key_buffer.realloc(sizeof(key_data_header) + key->size()));
  std::memcpy(key_buffer.data(), &key_data_header, sizeof(key_data_header));
  std::memcpy(
      (char*)key_buffer.data() + sizeof(key_data_header),
      key->data(),
      key->size());
  key_buffer.advance_size(sizeof(key_data_header) + key->size());

  BCRYPT_KEY_HANDLE key_handle;
  if (!NT_SUCCESS(BCryptImportKey(
          alg_handle,
          nullptr,
          BCRYPT_KEY_DATA_BLOB,
          &key_handle,
          nullptr,
          0,
          (unsigned char*)key_buffer.data(),
          key_buffer.size(),
          0))) {
    BCryptCloseAlgorithmProvider(alg_handle, 0);
    return LOG_STATUS(
        Status_EncryptionError("Win32CNG error; error importing key blob."));
  }

  // Decrypt the input.
  ULONG output_len;
  if (!NT_SUCCESS(BCryptDecrypt(
          key_handle,
          (unsigned char*)input->data(),
          input->size(),
          &auth_info,
          nullptr,
          0,
          (unsigned char*)output->cur_data(),
          required_space,
          &output_len,
          0))) {
    BCryptDestroyKey(key_handle);
    BCryptCloseAlgorithmProvider(alg_handle, 0);
    return LOG_STATUS(
        Status_EncryptionError("Win32CNG error; error decrypting."));
  }

  if (output->owns_data())
    output->advance_size(output_len);
  output->advance_offset(output_len);

  // Clean up.
  BCryptDestroyKey(key_handle);
  BCryptCloseAlgorithmProvider(alg_handle, 0);

  return Status::Ok();
}

Status Win32CNG::md5(
    const void* input, uint64_t input_read_size, Buffer* output) {
  return hash_bytes(input, input_read_size, output, BCRYPT_MD5_ALG_HANDLE);
}

Status Win32CNG::sha256(
    const void* input, uint64_t input_read_size, Buffer* output) {
  return hash_bytes(input, input_read_size, output, BCRYPT_SHA256_ALG_HANDLE);
}

Status Win32CNG::hash_bytes(
    const void* input,
    uint64_t input_read_size,
    Buffer* output,
    BCRYPT_ALG_HANDLE hash_algorithm) {
  if (!NT_SUCCESS(BCryptHash(
          hash_algorithm,
          nullptr,
          0,
          (PBYTE)input,
          input_read_size,
          (PBYTE)output->data(),
          output->alloced_size()))) {
    return Status_ChecksumError("Win32CNG error; could not hash data.");
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // _WIN32
