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
 * This file declares a class representing an encryption key.
 */

#ifndef TILEDB_ENCRYPTION_KEY_H
#define TILEDB_ENCRYPTION_KEY_H

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/encryption/encryption.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/**
 * Class that holds an encryption key, and zeroes the buffer in the destructor.
 */
class EncryptionKey {
 public:
  /** Constructor. */
  EncryptionKey();

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
  /** Buffer holding the encryption key. */
  Buffer key_;

  /** The encryption type. */
  EncryptionType encryption_type_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ENCRYPTION_KEY_H