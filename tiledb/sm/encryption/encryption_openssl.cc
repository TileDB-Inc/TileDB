/**
 * @file   encryption_openssl.cc
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
 * This file defines an OpenSSL encryption interface.
 */

#ifndef _WIN32

#include "tiledb/sm/encryption/encryption_openssl.h"
#include "tiledb/sm/misc/logger.h"

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

namespace tiledb {
namespace sm {

Status OpenSSL::get_random_bytes(unsigned num_bytes, Buffer* output) {
  if (output->free_space() < num_bytes)
    RETURN_NOT_OK(output->realloc(output->alloced_size() + num_bytes));

  int rc = RAND_bytes((unsigned char*)output->cur_data(), num_bytes);
  if (rc < 1) {
    char err_msg[256];
    ERR_error_string_n(ERR_get_error(), err_msg, sizeof(err_msg));
    return Status::EncryptionError(
        "Cannot generate random bytes with OpenSSL: " + std::string(err_msg));
  }
  output->advance_size(num_bytes);
  output->advance_offset(num_bytes);

  return Status::Ok();
}

Status OpenSSL::encrypt_aes256gcm(
    ConstBuffer* key,
    ConstBuffer* iv,
    ConstBuffer* input,
    Buffer* output,
    PreallocatedBuffer* output_iv,
    PreallocatedBuffer* output_tag) {
  // Check input size for int datatype used by OpenSSL.
  if (input->size() > std::numeric_limits<int>::max())
    return LOG_STATUS(Status::EncryptionError(
        "OpenSSL error; cannot encrypt: input too large"));

  // Ensure sufficient space in output buffer.
  auto required_space = input->size() + 2 * EVP_MAX_BLOCK_LENGTH;
  if (output->free_space() < required_space)
    RETURN_NOT_OK(output->realloc(output->alloced_size() + required_space));

  // Generate IV if the given IV buffer is null.
  Buffer generated_iv;
  int iv_len;
  unsigned char* iv_buf;
  if (iv == nullptr || iv->data() == nullptr) {
    RETURN_NOT_OK(
        get_random_bytes(Encryption::AES256GCM_IV_BYTES, &generated_iv));
    iv_len = (int)generated_iv.size();
    iv_buf = (unsigned char*)generated_iv.data();
  } else {
    iv_len = (int)iv->size();
    iv_buf = (unsigned char*)iv->data();
  }
  // Copy IV to output arg.
  std::memcpy(output_iv->cur_data(), iv_buf, iv_len);

  EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
  if (ctx == nullptr)
    return LOG_STATUS(Status::EncryptionError(
        "OpenSSL error; cannot encrypt: context allocation failed."));

  // Initialize the cipher. We use the default parameter lengths for the IV and
  // tag, so no further configuration is needed for the cipher.
  EVP_CIPHER_CTX_init(ctx);
  if (EVP_EncryptInit_ex(
          ctx,
          EVP_aes_256_gcm(),
          nullptr,
          (unsigned char*)key->data(),
          iv_buf) == 0) {
    EVP_CIPHER_CTX_free(ctx);
    return LOG_STATUS(
        Status::EncryptionError("OpenSSL error; error initializing cipher."));
  }

  // Encrypt the input.
  int output_len;
  if (EVP_EncryptUpdate(
          ctx,
          (unsigned char*)output->cur_data(),
          &output_len,
          (const unsigned char*)input->data(),
          (int)input->size()) == 0) {
    EVP_CIPHER_CTX_free(ctx);
    return LOG_STATUS(
        Status::EncryptionError("OpenSSL error; error encrypting data."));
  }
  output->advance_size((uint64_t)output_len);
  output->advance_offset((uint64_t)output_len);

  // Finalize encryption.
  if (EVP_EncryptFinal_ex(
          ctx, (unsigned char*)output->cur_data(), &output_len) == 0) {
    EVP_CIPHER_CTX_free(ctx);
    return LOG_STATUS(
        Status::EncryptionError("OpenSSL error; error finalizing encryption."));
  }
  output->advance_size((uint64_t)output_len);
  output->advance_offset((uint64_t)output_len);

  // Get the tag.
  if (EVP_CIPHER_CTX_ctrl(
          ctx,
          EVP_CTRL_GCM_GET_TAG,
          Encryption::AES256GCM_TAG_BYTES,
          (char*)output_tag->data()) == 0) {
    EVP_CIPHER_CTX_free(ctx);
    return LOG_STATUS(
        Status::EncryptionError("OpenSSL error; error getting tag."));
  }

  // Clean up.
  EVP_CIPHER_CTX_free(ctx);

  return Status::Ok();
}

Status OpenSSL::decrypt_aes256gcm(
    ConstBuffer* key,
    ConstBuffer* iv,
    ConstBuffer* tag,
    ConstBuffer* input,
    Buffer* output) {
  // Check input size for int datatype used by OpenSSL.
  if (input->size() > std::numeric_limits<int>::max())
    return LOG_STATUS(Status::EncryptionError(
        "OpenSSL error; cannot decrypt: input too large"));

  // Ensure sufficient space in output buffer.
  auto required_space = input->size();
  if (output->owns_data()) {
    if (output->free_space() < required_space)
      RETURN_NOT_OK(output->realloc(output->alloced_size() + required_space));
  } else if (output->size() < required_space) {
    return LOG_STATUS(Status::EncryptionError(
        "OpenSSL error; cannot decrypt: output buffer too small."));
  }

  EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
  if (ctx == nullptr)
    return LOG_STATUS(Status::EncryptionError(
        "OpenSSL error; cannot decrypt: context allocation failed."));

  // Initialize the cipher. We use the default parameter lengths for the IV and
  // tag, so no further configuration is needed for the cipher.
  EVP_CIPHER_CTX_init(ctx);
  if (EVP_DecryptInit_ex(
          ctx,
          EVP_aes_256_gcm(),
          nullptr,
          (unsigned char*)key->data(),
          (unsigned char*)iv->data()) == 0) {
    EVP_CIPHER_CTX_free(ctx);
    return LOG_STATUS(
        Status::EncryptionError("OpenSSL error; error initializing cipher."));
  }

  // Decrypt the input.
  int output_len;
  if (EVP_DecryptUpdate(
          ctx,
          (unsigned char*)output->cur_data(),
          &output_len,
          (const unsigned char*)input->data(),
          (int)input->size()) == 0) {
    EVP_CIPHER_CTX_free(ctx);
    return LOG_STATUS(
        Status::EncryptionError("OpenSSL error; error decrypting data."));
  }
  if (output->owns_data())
    output->advance_size((uint64_t)output_len);
  output->advance_offset((uint64_t)output_len);

  // Set the tag (which will be checked during finalization).
  if (EVP_CIPHER_CTX_ctrl(
          ctx,
          EVP_CTRL_GCM_SET_TAG,
          Encryption::AES256GCM_TAG_BYTES,
          (char*)tag->data()) == 0) {
    EVP_CIPHER_CTX_free(ctx);
    return LOG_STATUS(
        Status::EncryptionError("OpenSSL error; error setting tag."));
  }

  // Finalize decryption.
  if (EVP_DecryptFinal_ex(
          ctx, (unsigned char*)output->cur_data(), &output_len) == 0) {
    EVP_CIPHER_CTX_free(ctx);
    return LOG_STATUS(
        Status::EncryptionError("OpenSSL error; error finalizing decryption."));
  }
  if (output->owns_data())
    output->advance_size((uint64_t)output_len);
  output->advance_offset((uint64_t)output_len);

  // Clean up.
  EVP_CIPHER_CTX_free(ctx);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // !_WIN32