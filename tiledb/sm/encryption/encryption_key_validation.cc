/**
 * @file   encryption_key_validation.h
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
 * This file defines a class for encryption key validation.
 */

#include "tiledb/sm/encryption/encryption_key_validation.h"
#include "tiledb/sm/misc/logger.h"

namespace tiledb {
namespace sm {

const std::string EncryptionKeyValidation::ENCRYPTION_KEY_CHECK_DATA =
    "TILEDB_ENCRYPTION_KEY_CHECK_DATA";

Status EncryptionKeyValidation::check_encryption_key(
    const EncryptionKey& encryption_key) {
  // First time: encrypt the check data.
  if (encryption_key_check_data_.size() == 0)
    RETURN_NOT_OK(init_encryption_key_check_data(encryption_key));

  // Decrypt the data and check that it is the same.
  Buffer output;
  ConstBuffer input(
      encryption_key_check_data_.data(), encryption_key_check_data_.size());
  switch (encryption_key.encryption_type()) {
    case EncryptionType::NO_ENCRYPTION:
      RETURN_NOT_OK(output.write(&input, input.size()));
      break;
    case EncryptionType::AES_256_GCM: {
      ConstBuffer iv(
          encryption_key_check_data_iv_.data(),
          encryption_key_check_data_iv_.size());
      ConstBuffer tag(
          encryption_key_check_data_tag_.data(),
          encryption_key_check_data_tag_.size());
      ConstBuffer key = encryption_key.key();
      RETURN_NOT_OK(
          Encryption::decrypt_aes256gcm(&key, &iv, &tag, &input, &output));
      break;
    }
    default:
      return LOG_STATUS(Status::EncryptionError(
          "Invalid encryption key; invalid encryption type."));
  }

  if (output.size() != ENCRYPTION_KEY_CHECK_DATA.size())
    return LOG_STATUS(Status::EncryptionError("Invalid encryption key."));
  for (uint64_t i = 0; i < output.size(); i++) {
    if (output.value<char>(i * sizeof(char)) != ENCRYPTION_KEY_CHECK_DATA[i])
      return LOG_STATUS(Status::EncryptionError("Invalid encryption key."));
  }

  return Status::Ok();
}

Status EncryptionKeyValidation::init_encryption_key_check_data(
    const EncryptionKey& encryption_key) {
  encryption_key_check_data_.clear();
  encryption_key_check_data_tag_.clear();
  encryption_key_check_data_iv_.clear();

  ConstBuffer input(
      ENCRYPTION_KEY_CHECK_DATA.data(), ENCRYPTION_KEY_CHECK_DATA.size());

  switch (encryption_key.encryption_type()) {
    case EncryptionType::NO_ENCRYPTION:
      RETURN_NOT_OK(encryption_key_check_data_.write(&input, input.size()));
      break;
    case EncryptionType::AES_256_GCM: {
      RETURN_NOT_OK(encryption_key_check_data_iv_.realloc(
          Encryption::AES256GCM_IV_BYTES));
      RETURN_NOT_OK(encryption_key_check_data_tag_.realloc(
          Encryption::AES256GCM_TAG_BYTES));
      ConstBuffer key = encryption_key.key();
      PreallocatedBuffer iv(
          encryption_key_check_data_iv_.data(),
          encryption_key_check_data_iv_.alloced_size());
      PreallocatedBuffer tag(
          encryption_key_check_data_tag_.data(),
          encryption_key_check_data_tag_.alloced_size());
      RETURN_NOT_OK(Encryption::encrypt_aes256gcm(
          &key, nullptr, &input, &encryption_key_check_data_, &iv, &tag));
      encryption_key_check_data_iv_.advance_size(
          Encryption::AES256GCM_IV_BYTES);
      encryption_key_check_data_tag_.advance_size(
          Encryption::AES256GCM_TAG_BYTES);
      break;
    }
    default:
      return LOG_STATUS(Status::EncryptionError(
          "Invalid encryption key; invalid encryption type."));
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb