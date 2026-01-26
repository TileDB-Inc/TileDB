/**
 * @file   encryption_key.h
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
 * This file defines a class representing an encryption key.
 */

#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/crypto/crypto.h"
#include "tiledb/sm/enums/encryption_type.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

EncryptionKey::EncryptionKey()
    : encryption_type_(EncryptionType::NO_ENCRYPTION)
    , key_length_(0) {
  std::memset(key_, 0, max_key_length_);
}

EncryptionKey::EncryptionKey(const Config& cfg) {
  auto enc_type = cfg.get<std::string_view>("sm.encryption_type");
  if (enc_type.has_value()) {
    auto [st, opt_type] = encryption_type_enum(*enc_type);
    throw_if_not_ok(st);
    encryption_type_ = *opt_type;
  }

  auto enc_key = cfg.get<std::string_view>("sm.encryption_key");
  if (enc_key.has_value()) {
    auto key_length = static_cast<uint32_t>(enc_key->size());
    if (!is_valid_key_length(encryption_type_, key_length)) {
      throw StatusException(Status_EncryptionError(
          "Cannot create key; invalid key length for encryption type."));
    }
    key_length_ = key_length;
    std::memcpy(key_, enc_key->data(), key_length);
  } else {
    key_length_ = 0;
  }
}

EncryptionKey::~EncryptionKey() {
  std::memset(key_, 0, max_key_length_);
}

EncryptionType EncryptionKey::encryption_type() const {
  return encryption_type_;
}

Status EncryptionKey::set_key(
    EncryptionType encryption_type,
    const void* key_bytes,
    uint32_t key_length) {
  if (!is_valid_key_length(encryption_type, key_length)) {
    throw StatusException(Status_EncryptionError(
        "Cannot create key; invalid key length for encryption type."));
  }

  encryption_type_ = encryption_type;
  key_length_ = key_length;

  std::memset(key_, 0, max_key_length_);

  if (key_bytes != nullptr && key_length > 0) {
    std::memcpy(key_, key_bytes, key_length);
  }

  return Status::Ok();
}

bool EncryptionKey::is_valid_key_length(
    EncryptionType encryption_type, uint32_t key_length) {
  switch (encryption_type) {
    case EncryptionType::NO_ENCRYPTION:
      return key_length == 0;
    case EncryptionType::AES_256_GCM:
      return key_length == Crypto::AES256GCM_KEY_BYTES;
    default:
      return false;
  }
}

ConstBuffer EncryptionKey::key() const {
  return ConstBuffer(key_, key_length_);
}

}  // namespace sm
}  // namespace tiledb
