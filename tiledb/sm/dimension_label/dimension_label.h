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
  inline const static std::string indexed_array_name{"indexed"};
  inline const static std::string labelled_array_name{"labelled"};
  /**
   * Constructor.
   *
   * @param uri URI for the group containing the dimension labeli arrays.
   * @param storage_manager Storage manager object to use when opening the
   * arrays.
   */
  DimensionLabel(const URI& uri, StorageManager* storage_manager);

  /** Closes the arrays and frees all memory. */
  void close();

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
  LabelOrder label_order() const;

  /**
   * Opens the dimension label for reading at a timestamp retrieved from the
   * config or for writing.
   *
   * @param query_type The mode in which the dimension label is opened.
   * @param encryption_type The encryption type of the dimension label
   * @param encryption_key If the dimension label is encrypted, the private
   *     encryption key. For unencrypted axes, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   */
  void open(
      QueryType query_type,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Opens the dimension label.
   *
   * @param query_type The query type. This should always be READ. It
   *     is here only for sanity check.
   * @param timestamp_start The start timestamp at which to open the
   *     dimension label.
   * @param timestamp_end The end timestamp at which to open the
   *     dimension label.
   * @param encryption_type The encryption type of the dimension label
   * @param encryption_key If the dimension label is encrypted, the private
   *     encryption key. For unencrypted axes, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   */
  void open(
      QueryType query_type,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /** Returns the query type the dimension label was opened with. */
  QueryType query_type() const;

  /** Returns a reference to the dimension label schema. */
  const DimensionLabelSchema& schema() const;

 private:
  /**********************/
  /* Private attributes */
  /**********************/

  /** The URI of the dimension label group. */
  URI uri_;

  /** Storage manager */
  StorageManager* storage_manager_;

  /** Array with index dimension */
  shared_ptr<Array> indexed_array_;

  /** Array with label dimension */
  shared_ptr<Array> labelled_array_;

  /** Latest dimension label schema  */
  shared_ptr<DimensionLabelSchema> schema_;

  /** The query type the dimension label is opened as. */
  optional<QueryType> query_type_;

  /*******************/
  /* Private methods */
  /*******************/

  /**
   * Loads and checks the dimension label schema.
   *
   * @param indexed_array_schema Array schema for the indexed array
   * @param labelled_array_schema Array schema for the labelled array
   **/
  void load_schema(
      shared_ptr<ArraySchema> indexed_array_schema,
      shared_ptr<ArraySchema> labelled_array_schema);
};

/**
 * Creates a dimension label from a dimension label schema.
 *
 * @param uri URI to create the dimension label at.
 * @param storage_manager Storage manager object to use when opening the
 *     arrays.
 * @param schema The schema of the dimension label that will be created.
 **/
void create_dimension_label(
    const URI& uri,
    StorageManager& storage_manager,
    const DimensionLabelSchema& schema);

/**
 * Removes all fragments URIs from a fragment list that do not have a similarly
 * named fragment in a comparison fragments list.
 *
 * The comparison here is only on the base name of the fragment URIs. Fragments
 * may be re-ordered upon return.
 *
 * @param comparison_fragment_uris The list of fragment URIs to check against.
 * @param fragment_list The list of fragment URIs to prune.
 */
void intersect_fragments(
    const std::vector<TimestampedURI>& comparison_fragment_list,
    std::vector<TimestampedURI>& fragment_list);

}  // namespace tiledb::sm

#endif
