/**
 * @file tiledb/sm/dimension_label/dimension_label.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Defines the DimensionLabel object
 */

#ifndef TILEDB_DIMENSION_LABEL_H
#define TILEDB_DIMENSION_LABEL_H

#include "tiledb/common/common.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/dimension_label_schema.h"
#include "tiledb/sm/enums/label_order.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Attribute;
class Dimension;
class StorageManager;
class URI;

/** Return a Status_DimensionLabelError error class Status with a given
 * message **/
inline Status Status_DimensionLabelError(const std::string& msg) {
  return {"[TileDB::DimensionLabel] Error", msg};
};

/**
 * A dual-array object to be opened for reads/writes. The two one-dimensional
 * arrays make up the arrays needed for a dimension label.
 **/
class DimensionLabel {
 public:
  /**
   * Constructor.
   *
   * @param indexed_array_uri URI for the array with indices stored on the
   * dimension.
   * @param labelled_array_uri URI for the array with labels stored on the
   * dimension.
   * @param storage_manager Storage manager object to use when opening the
   * arrays.
   * @param label_order The ordering of the labels relative to the indices.
   * @param label_attr_id The integer ID of the label attribute in the indexed
   * array.
   * @param index_attr_id The integer ID of the index attribute in the labelled
   * array.
   */
  DimensionLabel(
      const URI& indexed_array_uri,
      const URI& labelled_array_uri,
      StorageManager* storage_manager,
      LabelOrder label_order,
      const DimensionLabelSchema::attribute_size_type label_attr_id = 0,
      const DimensionLabelSchema::attribute_size_type index_attr_id = 0);

  /** Closes the array and frees all memory. */
  Status close();

  /** Returns the index attribute in the labelled array. */
  const Attribute* index_attribute() const;

  /** Returns the index dimension in the indexed array. */
  const Dimension* index_dimension() const;

  /** Returns the array with indices stored on the dimension. */
  inline shared_ptr<Array> indexed_array() const {
    return indexed_array_;
  }

  /** Returns the label attribute in the indexed array. */
  const Attribute* label_attribute() const;

  /** Returns the label dimension in the labelled array. */
  const Dimension* label_dimension() const;

  /** Returns the array with labels stored on the dimension. */
  inline shared_ptr<Array> labelled_array() const {
    return labelled_array_;
  }

  /** Returns the order of the dimension label. */
  inline LabelOrder label_order() const {
    return label_order_;
  }

  /**
   * Opens the dimension label for reading at a timestamp retrieved from the
   * config or for writing.
   *
   * @param query_type The mode in which the dimension label is opened.
   * @param encryption_type The encryption type of the dimension label
   * @param encryption_key If the dimension label is encrypted, the private
   * encryption key. For unencrypted axes, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   */
  Status open(
      QueryType query_type,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Opens the dimension label for reading.
   *
   * @param query_type The query type. This should always be READ. It
   *    is here only for sanity check.
   * @param timestamp_start The start timestamp at which to open the
   * dimension label.
   * @param timestamp_end The end timestamp at which to open the
   * dimension label.
   * @param encryption_type The encryption type of the dimension label
   * @param encryption_key If the dimension label is encrypted, the private
   * encryption key. For unencrypted axes, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   *
   * @note Applicable only to reads.
   */
  Status open(
      QueryType query_type,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Opens the dimension label for reading without fragments.
   *
   * @param encryption_type The encryption type of the dimension label
   * @param encryption_key If the dimension label is encrypted, the private
   * encryption key. For unencrypted axes, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   *
   * @note Applicable only to reads.
   */
  Status open_without_fragments(
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

 private:
  /**********************/
  /* Private attributes */
  /**********************/

  /** Array with index dimension */
  shared_ptr<Array> indexed_array_;

  /** Array with label dimension */
  shared_ptr<Array> labelled_array_;

  /** Label order type of the dimension label */
  LabelOrder label_order_;

  /** Name of the label attribute in the indexed array. */
  DimensionLabelSchema::attribute_size_type label_attr_id_;

  /** Name of the index attribute in the labelled array. */
  DimensionLabelSchema::attribute_size_type index_attr_id_;

  /** Latest dimension label schema  */
  shared_ptr<DimensionLabelSchema> schema_;

  /*******************/
  /* Private methods */
  /*******************/

  /** Loads and checks the dimension label schema. */
  Status load_schema();
};

}  // namespace tiledb::sm

#endif
