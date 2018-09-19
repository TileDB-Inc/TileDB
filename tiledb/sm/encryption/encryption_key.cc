/**
 * @file   encryption_key.h
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
 * This file defines a class representing an encryption key.
 */

#include "tiledb/sm/encryption/encryption_key.h"
#include "tiledb/sm/misc/logger.h"

namespace tiledb {
namespace sm {

EncryptionKey::EncryptionKey()
    : encryption_type_(EncryptionType::NO_ENCRYPTION) {
}

EncryptionKey::~EncryptionKey() {
  if (key_.data() != nullptr)
    std::memset(key_.data(), 0, key_.alloced_size());
}

EncryptionType EncryptionKey::encryption_type() const {
  return encryption_type_;
}

Status EncryptionKey::set_key(
    EncryptionType encryption_type,
    const void* key_bytes,
    uint32_t key_length) {
  // Clear (and free) old key
  if (key_.data() != nullptr)
    std::memset(key_.data(), 0, key_.alloced_size());
  key_.clear();

  if (!is_valid_key_length(encryption_type, key_length))
    return LOG_STATUS(Status::EncryptionError(
        "Cannot create key; invalid key length for encryption type."));

  encryption_type_ = encryption_type;

  if (key_bytes != nullptr && key_length > 0) {
    // Realloc and copy.
    if (key_.alloced_size() < key_length)
      RETURN_NOT_OK(key_.realloc(key_length));
    RETURN_NOT_OK(key_.write(key_bytes, key_length));
    key_.reset_offset();
  }

  return Status::Ok();
}

bool EncryptionKey::is_valid_key_length(
    EncryptionType encryption_type, uint32_t key_length) {
  switch (encryption_type) {
    case EncryptionType::NO_ENCRYPTION:
      return key_length == 0;
    case EncryptionType::AES_256_GCM:
      return key_length == Encryption::AES256GCM_KEY_BYTES;
    default:
      return false;
  }
}

ConstBuffer EncryptionKey::key() const {
  return ConstBuffer(key_.data(), key_.size());
}

}  // namespace sm
}  // namespace tiledb