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
 * This file declares a class representing an encryption key.
 */

#ifndef TILEDB_ENCRYPTION_KEY_H
#define TILEDB_ENCRYPTION_KEY_H

#include "tiledb/common/status.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"

#include <cstddef>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

enum class EncryptionType : uint8_t;

/**
 * Class that holds an encryption key, and zeroes the buffer in the destructor.
 */
class EncryptionKey {
 public:
  /** Constructor. */
  EncryptionKey();

  /** Constructor from Config data. */
  EncryptionKey(const Config& config);

  /** Destructor. This zeroes the underlying key buffer. */
  ~EncryptionKey();

  /** Deleted rule-of-5 functions to prevent unintended copies/moves. */
  EncryptionKey(const EncryptionKey& other) = delete;
  EncryptionKey(EncryptionKey&& other) = delete;
  EncryptionKey& operator=(const EncryptionKey& other) = delete;
  EncryptionKey& operator=(EncryptionKey&& other) = delete;

  /** Returns the encryption type. */
  EncryptionType encryption_type() const;

  /**
   * Returns true if the given key length (in bytes) is valid for the encryption
   * type.
   */
  static bool is_valid_key_length(
      EncryptionType encryption_type, uint32_t key_length);

  /** Returns a ConstBuffer holding a pointer to the key bytes. */
  ConstBuffer key() const;

  /**
   * Copies the given key into the buffer.
   *
   * @param encryption_type The encryption type
   * @param key_bytes Pointer to key bytes
   * @param key_length Length of key in bytes
   * @return Status
   */
  Status set_key(
      EncryptionType encryption_type,
      const void* key_bytes,
      uint32_t key_length);

 private:
  /** The encryption type. */
  EncryptionType encryption_type_;

  /** Size of the array storing the encryption key. */
  constexpr static size_t max_key_length_ = 32;

  /** Array holding the actual encryption key. */
  std::byte key_[max_key_length_];

  /** Length of the stored encryption key. */
  size_t key_length_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ENCRYPTION_KEY_H
