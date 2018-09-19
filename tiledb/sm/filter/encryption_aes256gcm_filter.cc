/**
 * @file   encryption_aes256gcm_filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file defines class EncryptionAES256GCMFilter.
 */

#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/encryption/encryption.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/tile/tile.h"

namespace tiledb {
namespace sm {

EncryptionAES256GCMFilter::EncryptionAES256GCMFilter()
    : Filter(FilterType::INTERNAL_FILTER_AES_256_GCM) {
  set_key(nullptr);
}

EncryptionAES256GCMFilter::EncryptionAES256GCMFilter(
    const EncryptionKey& encryption_key)
    : Filter(FilterType::INTERNAL_FILTER_AES_256_GCM) {
  auto buff = encryption_key.key();
  set_key(buff.data());
}

EncryptionAES256GCMFilter* EncryptionAES256GCMFilter::clone_impl() const {
  auto clone = new EncryptionAES256GCMFilter;
  // Copy key bytes buffer.
  clone->key_bytes_ = key_bytes_;
  return clone;
}

Status EncryptionAES256GCMFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  if (key_bytes_ == nullptr)
    return LOG_STATUS(Status::FilterError("Encryption error; bad key."));

  // Allocate an initial output buffer.
  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  assert(output_buf != nullptr);

  // Compute and write the metadata
  std::vector<ConstBuffer> data_parts = input->buffers(),
                           metadata_parts = input_metadata->buffers();
  auto num_data_parts = (uint32_t)data_parts.size(),
       num_metadata_parts = (uint32_t)metadata_parts.size(),
       total_num_parts = num_data_parts + num_metadata_parts;
  uint32_t part_md_size = 2 * sizeof(uint32_t) +
                          Encryption::AES256GCM_TAG_BYTES +
                          Encryption::AES256GCM_IV_BYTES;
  uint32_t metadata_size =
      2 * sizeof(uint32_t) + total_num_parts * part_md_size;
  RETURN_NOT_OK(output_metadata->prepend_buffer(metadata_size));
  RETURN_NOT_OK(output_metadata->write(&num_metadata_parts, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata->write(&num_data_parts, sizeof(uint32_t)));

  // Encrypt all parts
  for (auto& part : metadata_parts)
    RETURN_NOT_OK(encrypt_part(&part, output_buf, output_metadata));
  for (auto& part : data_parts)
    RETURN_NOT_OK(encrypt_part(&part, output_buf, output_metadata));

  return Status::Ok();
}

Status EncryptionAES256GCMFilter::encrypt_part(
    ConstBuffer* part, Buffer* output, FilterBuffer* output_metadata) const {
  // Set up the key buffer.
  ConstBuffer key(key_bytes_, Encryption::AES256GCM_KEY_BYTES);

  // Set up the IV and tag metadata buffers.
  uint8_t iv[Encryption::AES256GCM_IV_BYTES],
      tag[Encryption::AES256GCM_TAG_BYTES];
  PreallocatedBuffer output_iv(iv, Encryption::AES256GCM_IV_BYTES),
      output_tag(tag, Encryption::AES256GCM_TAG_BYTES);

  // Encrypt.
  auto orig_size = (uint32_t)output->size();

  RETURN_NOT_OK(Encryption::encrypt_aes256gcm(
      &key, nullptr, part, output, &output_iv, &output_tag));

  if (output->size() > std::numeric_limits<uint32_t>::max())
    return LOG_STATUS(
        Status::FilterError("Encrypted output exceeds uint32 max."));

  // Write metadata.
  uint32_t input_size = (uint32_t)part->size(),
           encrypted_size = (uint32_t)output->size() - orig_size;
  RETURN_NOT_OK(output_metadata->write(&input_size, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata->write(&encrypted_size, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata->write(iv, Encryption::AES256GCM_IV_BYTES));
  RETURN_NOT_OK(output_metadata->write(tag, Encryption::AES256GCM_TAG_BYTES));

  return Status::Ok();
}

Status EncryptionAES256GCMFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  if (key_bytes_ == nullptr)
    return LOG_STATUS(Status::FilterError("Encryption error; bad key."));

  // Read the number of parts from input metadata.
  uint32_t num_metadata_parts, num_data_parts;
  RETURN_NOT_OK(input_metadata->read(&num_metadata_parts, sizeof(uint32_t)));
  RETURN_NOT_OK(input_metadata->read(&num_data_parts, sizeof(uint32_t)));

  // Get buffers for output.
  RETURN_NOT_OK(output->prepend_buffer(0));
  Buffer* data_buffer = output->buffer_ptr(0);
  assert(data_buffer != nullptr);
  RETURN_NOT_OK(output_metadata->prepend_buffer(0));
  Buffer* metadata_buffer = output_metadata->buffer_ptr(0);
  assert(metadata_buffer != nullptr);

  // Decrypt all parts
  for (uint32_t i = 0; i < num_metadata_parts; i++)
    RETURN_NOT_OK(decrypt_part(input, metadata_buffer, input_metadata));
  for (uint32_t i = 0; i < num_data_parts; i++)
    RETURN_NOT_OK(decrypt_part(input, data_buffer, input_metadata));

  return Status::Ok();
}

Status EncryptionAES256GCMFilter::decrypt_part(
    FilterBuffer* input, Buffer* output, FilterBuffer* input_metadata) const {
  // Get original (plaintext) and encrypted sizes.
  uint32_t encrypted_size, plaintext_size;
  RETURN_NOT_OK(input_metadata->read(&plaintext_size, sizeof(uint32_t)));
  RETURN_NOT_OK(input_metadata->read(&encrypted_size, sizeof(uint32_t)));

  // Set up the key buffer.
  ConstBuffer key(key_bytes_, Encryption::AES256GCM_KEY_BYTES);

  // Set up the IV and tag metadata buffers.
  uint8_t iv_bytes[Encryption::AES256GCM_IV_BYTES],
      tag_bytes[Encryption::AES256GCM_TAG_BYTES];
  RETURN_NOT_OK(input_metadata->read(iv_bytes, Encryption::AES256GCM_IV_BYTES));
  RETURN_NOT_OK(
      input_metadata->read(tag_bytes, Encryption::AES256GCM_TAG_BYTES));
  ConstBuffer iv(iv_bytes, Encryption::AES256GCM_IV_BYTES),
      tag(tag_bytes, Encryption::AES256GCM_TAG_BYTES);

  // Ensure space in the output buffer if possible.
  if (output->owns_data()) {
    RETURN_NOT_OK(output->realloc(output->alloced_size() + plaintext_size));
  } else if (output->offset() + plaintext_size > output->size()) {
    return LOG_STATUS(
        Status::FilterError("Encryption error; output buffer too small."));
  }

  // Set up the input buffer.
  ConstBuffer input_buffer(nullptr, 0);
  RETURN_NOT_OK(input->get_const_buffer(encrypted_size, &input_buffer));

  // Decrypt.
  RETURN_NOT_OK(
      Encryption::decrypt_aes256gcm(&key, &iv, &tag, &input_buffer, output));

  input->advance_offset(encrypted_size);

  return Status::Ok();
}

Status EncryptionAES256GCMFilter::set_key(const EncryptionKey& key) {
  auto key_buff = key.key();

  if (key.encryption_type() != EncryptionType::AES_256_GCM)
    return LOG_STATUS(
        Status::FilterError("Encryption error; invalid key encryption type."));

  if (key_buff.data() == nullptr ||
      key_buff.size() != Encryption::AES256GCM_KEY_BYTES)
    return LOG_STATUS(
        Status::FilterError("Encryption error; invalid key for AES-256-GCM."));

  key_bytes_ = key_buff.data();

  return Status::Ok();
}

Status EncryptionAES256GCMFilter::set_key(const void* key_bytes) {
  key_bytes_ = key_bytes;

  return Status::Ok();
}

Status EncryptionAES256GCMFilter::get_key(const void** key_bytes) const {
  *key_bytes = key_bytes_;
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb