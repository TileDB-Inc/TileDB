/**
 * @file dictionary.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file defines class Dictionary.
 */

#ifndef TILEDB_DICTIONARY_H
#define TILEDB_DICTIONARY_H

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/storage_format/serialization/serializers.h"


namespace tiledb::sm {

/** Manipulates a TileDB dictionary. */
class Dictionary {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param type The type of the dictionary.
   * @param cell_val_num The cell number of the dictionary.
   * @param nullable The nullable of the dictionary.
   * @param order Whether dictionary values are ordered.
   *
   * @note The default number of values per cell is 1 for all datatypes except
  *     `ANY`, which is always variable-sized.
   */
  Dictionary(
      Datatype type = Datatype::CHAR,
      uint32_t cell_val_num = 0,
      bool nullable = false,
      bool ordered = false);

  Dictionary(
      Datatype type,
      uint32_t cell_val_num,
      bool nullable,
      bool ordered,
      Buffer data,
      Buffer offsets,
      Buffer validity);

  Dictionary(const Dictionary& dict);
  Dictionary(Dictionary&& dict) noexcept;
  Dictionary& operator=(const Dictionary& dict);
  Dictionary& operator=(Dictionary&& dict);

  /** Destructor. */
  ~Dictionary() = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the dictionary type. */
  Datatype type() const;

  /**
   * Sets the dictionary number of values per cell. Note that if the dictionary
   * datatype is `ANY` this function returns an error, since `ANY` datatype
   * must always be variable-sized.
   */
  void set_cell_val_num(uint32_t cv_num);

  /** Returns the number of values per cell. */
  uint32_t cell_val_num() const;

  /**
   * Returns *true* if this is a variable-sized dictionary, and *false*
   * otherwise.
   */
  bool var_size() const;

  /** Sets the nullability for this dictionary. */
  void set_nullable(bool nullable);

  /**
   * Returns *true* if this is a nullable dictionary, and *false*
   * otherwise.
   */
  bool nullable() const;

  /** Sets the ordered flag for this dictionary. */
  void set_ordered(bool ordered);

  /** Returns whether dictionary values are considered ordered. */
  bool ordered() const;

  /** Set the data buffer for this dictionary. */
  void set_data_buffer(void* const buffer, uint64_t buffer_size);

  /** Retrieve the data buffer for this dictionary. */
  void get_data_buffer(void** buffer, uint64_t* buffer_size);

  /** Set the offsets buffer for this dictionary. */
  void set_offsets_buffer(void* const buffer, uint64_t buffer_size);

  /** Get the offsets buffer for this dictionary. */
  void get_offsets_buffer(void** buffer, uint64_t* buffer_size);

  /** Set the validity buffer for this dictionary. */
  void set_validity_buffer(void* const buffer, uint64_t buffer_size);

  /** Get the validity buffer for this dictionary. */
  void get_validity_buffer(void** buffer, uint64_t* buffer_size);

  /**
   * Populates the object members from the data in the input binary buffer.
   *
   * @param deserializer The deserializer to deserialize from.
   * @param version The format spec version.
   * @return Dictionary
   */
  static shared_ptr<Dictionary> deserialize(Deserializer& deserializer, uint32_t version);

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param serializer The object the dictionary is serialized into.
   * @param version The format spec version.
   */
  void serialize(Serializer& serializer, uint32_t version) const;

  /** Swap two dictionaries. */
  void swap(Dictionary& other);

  /** Dumps the dictionary contents in ASCII form in the selected output. */
  void dump(FILE* out) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The dictionary type. */
  Datatype type_;

  /** The dictionary number of values per cell. */
  unsigned cell_val_num_;

  /** True if this dictionary may be null. */
  bool nullable_;

  /** Whether dictionary values are considered ordered */
  bool ordered_;

  /** The data buffer for this dictionary. */
  Buffer data_;

  /** The offsets buffer for this dictionary. */
  Buffer offsets_;

  /** The validity vector for this dictionary. */
  Buffer validity_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_DICTIONARY_H
