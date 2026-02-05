/**
 * @file   column_fragment_writer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2026 TileDB, Inc.
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
 * This file defines class ColumnFragmentWriter, which writes one field
 * (column) at a time to a fragment.
 */

#ifndef TILEDB_COLUMN_FRAGMENT_WRITER_H
#define TILEDB_COLUMN_FRAGMENT_WRITER_H

#include <string>
#include <vector>

#include "tiledb/common/common.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/types.h"

using namespace tiledb::common;

namespace tiledb::sm {

class ArraySchema;
class ContextResources;
class EncryptionKey;
class FragmentMetadata;
class WriterTileTuple;

class ColumnFragmentWriterException : public StatusException {
 public:
  explicit ColumnFragmentWriterException(const std::string& message)
      : StatusException("ColumnFragmentWriter", message) {
  }
};

/**
 * A fragment writer that writes one field (column) at a time.
 *
 * Usage:
 *   1. Create a ColumnFragmentWriter with domain and tile count
 *   2. For each field:
 *      a. Call open_field(name)
 *      b. Call write_tile() for each pre-filtered tile
 *      c. Call close_field()
 *   3. For sparse arrays, call set_mbrs() after processing dimensions
 *   4. Call finalize(key)
 */
class ColumnFragmentWriter {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor. Creates fragment directory and FragmentMetadata.
   *
   * Derived automatically:
   * - timestamp_range from fragment_uri
   * - memory_tracker from resources
   * - dense/sparse from array_schema
   * - tile_count from non_empty_domain (dense only)
   *
   * @param resources A context resources instance.
   * @param array_schema The schema of the array the fragment belongs to.
   * @param fragment_uri The fragment URI (must contain valid timestamps).
   * @param non_empty_domain The non-empty domain for this fragment.
   * @param tile_count Number of tiles for sparse arrays. If 0, tile count is
   *        determined dynamically by the first field written (for streaming).
   *        Ignored for dense arrays (computed from domain).
   */
  ColumnFragmentWriter(
      ContextResources* resources,
      shared_ptr<const ArraySchema> array_schema,
      const URI& fragment_uri,
      const NDRange& non_empty_domain,
      uint64_t tile_count = 0);

  ~ColumnFragmentWriter() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(ColumnFragmentWriter);
  DISABLE_MOVE_AND_MOVE_ASSIGN(ColumnFragmentWriter);

  /* ********************************* */
  /*          FIELD OPERATIONS         */
  /* ********************************* */

  /**
   * Opens a field for writing. Must be called before write_tile().
   *
   * @param name The name of the field (attribute or dimension).
   * @throws ColumnFragmentWriterException if field doesn't exist in schema,
   *         or if another field is already open.
   */
  void open_field(const std::string& name);

  /**
   * Writes a pre-filtered tile for the currently open field.
   *
   * @param tile The tile to write. Must already be filtered
   *        (tile.filtered_size().has_value() must be true).
   * @throws ColumnFragmentWriterException if no field is open,
   *         tile is not filtered, or tile count limit reached.
   */
  void write_tile(WriterTileTuple& tile);

  /**
   * Closes the currently open field. Flushes file buffers.
   *
   * @throws ColumnFragmentWriterException if no field is open.
   */
  void close_field();

  /**
   * Sets the MBRs for a sparse fragment. Should be called after processing
   * dimensions and before finalize(). This allows freeing intermediate MBR
   * computation memory before processing attributes.
   *
   * @param mbrs MBRs for sparse arrays (one per tile). Ownership is
   * transferred.
   * @throws ColumnFragmentWriterException if this is a dense array,
   *         or if MBR count doesn't match tile count.
   */
  void set_mbrs(std::vector<NDRange>&& mbrs);

  /* ********************************* */
  /*       FRAGMENT-LEVEL OPERATIONS   */
  /* ********************************* */

  /**
   * Finalizes the fragment. Stores metadata and creates commit file.
   *
   * For sparse arrays, set_mbrs() must be called before finalize().
   *
   * @param encryption_key The encryption key for storing metadata.
   * @throws ColumnFragmentWriterException if a field is still open,
   *         or if this is a sparse array without MBRs set.
   */
  void finalize(const EncryptionKey& encryption_key);

  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Returns the fragment URI. */
  const URI& fragment_uri() const;

  /** Returns the fragment metadata. */
  shared_ptr<FragmentMetadata> fragment_metadata() const;

 private:
  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /** Creates the fragment directory structure. */
  void create_fragment_directory();

  /** Internal finalize implementation. */
  void finalize_internal(const EncryptionKey& encryption_key);

  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The context resources. */
  ContextResources* resources_;

  /** The array schema. */
  shared_ptr<const ArraySchema> array_schema_;

  /** The fragment URI. */
  URI fragment_uri_;

  /** The fragment metadata. */
  shared_ptr<FragmentMetadata> frag_meta_;

  /** Whether this is a dense fragment. */
  bool dense_;

  /** Currently open field name (empty if no field is open). */
  std::string current_field_;

  /** Current tile index for the open field. */
  uint64_t current_tile_idx_;

  /** Number of tiles to be written. */
  uint64_t tile_num_;

  /** Whether the first field has been closed (for sparse dynamic tile count).
   */
  bool first_field_closed_;

  /** Whether MBRs have been set. */
  bool mbrs_set_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_COLUMN_FRAGMENT_WRITER_H
