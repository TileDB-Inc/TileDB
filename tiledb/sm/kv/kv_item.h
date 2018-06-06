/**
 * @file   kv_item.h
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
 This file defines class KVItem.
 */

#ifndef TILEDB_KV_ITEM_H
#define TILEDB_KV_ITEM_H

#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/status.h"

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace tiledb {
namespace sm {

class KVItem {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** Represents a hash. */
  typedef std::pair<uint64_t, uint64_t> Hash;

  /** Represents a key. */
  struct Key {
    void* key_;
    Datatype key_type_;
    uint64_t key_size_;
    Hash hash_;
  };

  /** Represents a value. */
  struct Value {
    std::string attribute_;
    void* value_;
    Datatype value_type_;
    uint64_t value_size_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  KVItem();

  /** Destructor. */
  ~KVItem();

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Assignment operator. Copies all members. */
  KVItem& operator=(const KVItem& kv_item);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Clears the members of this item. */
  void clear();

  /**
   * Checks the goodness of the key-value item. Specifically, it checks
   * whether the set of attributes stored in the item is equivalent to
   * `attributes`, and have the same types as `types` (there is a one-to-one
   * correspondence. Returns `false` when the key is `nullptr`, or a set
   * value is `nullptr`.
   *
   * @param attributes The attributes to check.
   * @param types The types to be checked.
   * @return Status
   */
  Status good(
      const std::vector<std::string>& attributes,
      const std::vector<Datatype>& types) const;

  /** Returns the hash of the key-value item. */
  const Hash& hash() const;

  /** Retrieves the key, along with its type and size. */
  const Key* key() const;

  /**
   * Retrieves a value and its size for a particular attribute. If the attribute
   * does not exist, it returns `nullptr`.
   */
  const Value* value(const std::string& attribute) const;

  /**
   * Sets the key of the key-value item. This function also computes and
   * sets the hash of the key.
   *
   * @param key The key to be set.
   * @param key_type The key type to be set.
   * @param key_size The key size (in bytes).
   * @return Status
   */
  Status set_key(const void* key, Datatype key_type, uint64_t key_size);

  /**
   * Sets the key of the key-value item.
   *
   * @param key The key to be set.
   * @param key_type The key type to be set.
   * @param key_size The key size (in bytes).
   * @param hash The key hash.
   * @return Status
   */
  Status set_key(
      const void* key, Datatype key_type, uint64_t key_size, const Hash& hash);

  /**
   * Sets the value for a particular attribute of the key-value item.
   *
   * @param attribute The name of the attribute.
   * @param value The value to be set.
   * @param value_size The value size (in bytes).
   * @return Status
   */
  Status set_value(
      const std::string& attribute,
      const void* value,
      Datatype value_type,
      uint64_t value_size);

  /* ********************************* */
  /*         STATIC FUNCTIONS          */
  /* ********************************* */

  /**
   * Computes and returns a hash on a key, key type and key size tuple as a
   * pair of `uint64_t` values.
   */
  static Hash compute_hash(
      const void* key, Datatype key_type, uint64_t key_size);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The key. */
  Key key_;

  /** Map of values: (attrbute name) -> (value). */
  std::unordered_map<std::string, Value*> values_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Copies the input hash into the object hash. */
  void copy_hash(const Hash& hash);

  /** Copies the input key into the object key. */
  void copy_key(const Key& key);

  /** Copies and adds the input value to the object values. */
  void copy_value(const Value& value);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_KV_ITEM_H
