/**
 * @file   kv_item.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 This file defines class KV.
 */

#ifndef TILEDB_KV_H
#define TILEDB_KV_H

#include "array_type.h"
#include "buffer.h"
#include "datatype.h"
#include "query_type.h"
#include "status.h"

#include <map>
#include <string>
#include <vector>

namespace tiledb {

/**
 * This class enables storing key-value items, which are used for writing to or
 * reading from a TileDB array. The keys can have arbitrary types and sizes.
 * Each value can have an arbitrary number of attributes, with arbitrary types.
 * The underlying TileDB array that store such a key-value store is a 2D sparse
 * array, where the coordinates are computed on the keys as a 16-byte
 * (2 * uint64_t) MD5 digest.
 */
class KV {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Empty constructor. */
  KV() = default;

  /**
   * Constructor.
   *
   * @param attributes The attributes in the key-value store.
   * @param types The types of the attributes.
   * @param nitems The number of items that each attribute value can store
   *     (can be variable).
   */
  KV(const std::vector<std::string>& attributes,
     const std::vector<tiledb::Datatype>& types,
     const std::vector<unsigned int>& nitems);

  /** Destructor. */
  ~KV();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Adds a key to the store.
   *
   * @param key The key to be stored.
   * @param key_type The key type.
   * @param key_size The key size.
   * @return Status
   */
  Status add_key(const void* key, Datatype key_type, uint64_t key_size);

  /**
   * Adds a (fixed-sized) value to the store on a particular attribute.
   * The function will return an error if the attribute index is out
   * of bounds, or if the attribute is variable-sized.
   *
   * @param attribute_idx The index of the attribute that will store the value.
   * @param value The value to be stored.
   * @return Status
   */
  Status add_value(unsigned int attribute_idx, const void* value);

  /**
   * Adds a variable-sized value to the store on a particular attribute.
   * The function will return an error if the attribute index is out
   * of bounds, or if the attribute is fixed-sized.
   *
   * @param attribute_idx The index of the attribute that will store the value.
   * @param value The value to be stored.
   * @param value_size The value size.
   * @return Status
   */
  Status add_value(
      unsigned int attribute_idx, const void* value, uint64_t value_size);

  /**
   * This function is used to retrieve the attribute names and number
   * that will be used when writing the key-value store to a TileDB
   * array, or reading a key-value store from a TileDB array. The
   * calculation depends on whether the key attributes and/or coordinates
   * will participate in the query or not.
   *
   * @param attributes The attributes names to be retrieved.
   * @param attribute_num The number of attributes retrieved.
   * @param get_coords It should be `true` if the coordinates will participate
   *     in the query (this happens when the query is a write query).
   * @param get_key It should be `true` if the key attributes will participate
   *     in the query.
   * @return Status
   */
  Status get_array_attributes(
      const char*** attributes,
      unsigned int* attribute_num,
      bool get_coords,
      bool get_key);

  /**
   * Retrieves the buffers and buffer sizes to be used when writing the
   * key-value store into a TileDB array, or reading the key-value store
   * from a TileDB array. The buffers follow the
   * order of attributes returned from `get_array_attributes`. There will be
   * one buffer per fixed-sized attribute, and two buffers per variable-sized
   * attribute.
   *
   * @param buffers The buffers to be retrieved.
   * @param buffer_sizes the buffer sizes to be retrieved.
   * @return Status
   *
   * @note `get_array_attributes` must be invoked before this function to
   *     calculate the corresponding attributes involved, otherwise an error
   *     will be returned.
   */
  Status get_array_buffers(void*** buffers, uint64_t** buffer_sizes);

  /**
   * Retrieves a key based on a provided index.
   *
   * @param idx The index of the key to be retrieved.
   * @param key The key to be retrieved. Note that essentially only a pointer
   *     to the internal key buffers is retrieved.
   * @param key_type They key type.
   * @param key_size The key size.
   * @return Status
   *
   * @note No particular order is assumed on the keys.
   */
  Status get_key(
      uint64_t idx, void** key, Datatype* key_type, uint64_t* key_size) const;

  /**
   * Retrieves a value based on an object and attribute index.
   *
   * @param obj_idx The object index.
   * @param attr_idx The attribute index. The order of the attributes is the
   *     same as that used in the constructor.
   * @param value The value to be retrieved. Note that only a pointer to the
   *     internal value buffer is retrieved.
   * @return Status
   *
   * @note No particular order is assumed on the values.
   */
  Status get_value(uint64_t obj_idx, unsigned int attr_idx, void** value) const;

  /**
   * Retrieves a variable-sized value based on an object and attribute index.
   *
   * @param obj_idx The object index.
   * @param attr_idx The attribute index. The order of the attributes is the
   *     same as that used in the constructor.
   * @param value The value to be retrieved. Note that obly a pointer to the
   *     internal value buffer is retrieved.
   * @param value_size The size of the value to be retrieved.
   * @return Status
   *
   * @note No particular order is assumed on the values.
   */
  Status get_value(
      uint64_t obj_idx,
      unsigned int attr_idx,
      void** value,
      uint64_t* value_size) const;

  /** Returns the number of keys in the key-value store. */
  uint64_t key_num() const;

  /**
   * Sets the size to be allocated for the internal buffers. This is applicable
   * when the key-value store is read from a TileDB array, so pre-allocation
   * provides control over memory management.
   *
   * @param nbytes Size to be set.
   */
  void set_buffer_alloc_size(uint64_t nbytes);

  /**
   * Retrieves the number of values on a particular attribute in the key-value
   * store.
   */
  Status value_num(unsigned int attribute_idx, uint64_t* num) const;

  /* ********************************* */
  /*         STATIC FUNCTIONS          */
  /* ********************************* */

  /**
   * Computes the (unary) subarray for a particular key. The single pair of
   * coordinates that defines this subarray is computed as the 16-byte
   * (2 * uint64_t) MD5 digest of the <key_type | key_size | key> tuple.
   * The subarray result is will be stored
   * in the user-provided uint64_t[4] array `subarray`.
   *
   * @param key The key.
   * @param key_type The key type.
   * @param key_size The key size.
   * @param subarray This will store the result. The user must make sure that
   *     this is a 4-element uint64_t array.
   * @return void
   */
  static void compute_subarray(
      const void* key,
      Datatype key_type,
      uint64_t key_size,
      uint64_t* subarray);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * Stores the attribute names to be used when this key-value store is written
   * into or read from a TileDB array.
   */
  const char** array_attributes_;

  /**
   * Stores the number of attributes to be used when this key-value store is
   * written into or read from a TileDB array.
   */
  unsigned int array_attribute_num_;

  /**
   * Stores the buffers to be used when this key-value store is written
   * into or read from a TileDB array.
   */
  void** array_buffers_;

  /**
   * Stores the buffer sizes to be used when this key-value store is written
   * into or read from a TileDB array.
   */
  uint64_t* array_buffer_sizes_;

  /** The number of array buffers created. */
  unsigned int array_buffer_num_;

  /**
   * For each attribute, it stores the index of its corresponding buffer
   * in `array_buffers_`. For variable-sized attributes, this is the index
   * to the corresponding offsets buffer.
   */
  std::vector<unsigned int> array_buffer_idx_;

  /** The attributes of the key-value store. */
  std::vector<std::string> attributes_;

  /** Number of attributes. */
  unsigned int attribute_num_;

  /** The size to be allocated for the internal buffers upon reads. */
  uint64_t buffer_alloc_size_;

  /**
   * The buffer for the coordinates to be calculated on the keys and
   * key types upon writes.
   */
  Buffer buff_coords_;

  /**
   * Buffers for the attribute value offsets (one per attribute). This
   * is applicable only to variable-sized attributes. For the fixed-sized
   * ones, the corresponding buffer will just be empty.
   */
  std::vector<Buffer> buff_value_offsets_;

  /** Buffers for the attribute values (one per attribute). */
  std::vector<Buffer> buff_values_;

  /** The buffer for the key offsets. */
  Buffer buff_key_offsets_;

  /** The buffer for the keys. */
  Buffer buff_keys_;

  /** The buffer for the key types. */
  Buffer buff_key_types_;

  /** The number of keys in the store. */
  uint64_t key_num_;

  /** The number of items stored in a single value for each attribute. */
  std::vector<unsigned int> nitems_;

  /** The attribute types. */
  std::vector<Datatype> types_;

  /** Stores the number of values written in each attribute buffer. */
  std::vector<uint64_t> value_num_;

  /** The value sizes for each attribute. */
  std::vector<uint64_t> value_sizes_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Allocates memory for the internal buffers in preparation for reading. */
  Status alloc_buffers(bool has_keys);

  /** Computes the coordinates from the keys (using MD5) upon writing. */
  Status compute_coords();

  /**
   * Returns `true` if the coordinates will participate in an underlying
   * TileDB array read or write query.
   */
  bool has_coords() const;

  /**
   * Returns `true` if the key attribites will participate in an underlying
   * TileDB array read or write query.
   */
  bool has_keys() const;
};

}  // namespace tiledb

#endif  // TILEDB_KV_H
