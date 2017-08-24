/**
 * @file  fragment_metadata.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines class BookKeeping.
 */

#ifndef TILEDB_FRAGMENTMETADATA_H
#define TILEDB_FRAGMENTMETADATA_H

#include <zlib.h>
#include <vector>
#include "array_schema.h"
#include "buffer.h"
#include "query_mode.h"
#include "status.h"

namespace tiledb {

/** Stores the metadata structures of a fragment. */
class FragmentMetadata {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  FragmentMetadata(
      const ArraySchema* array_schema,
      bool dense,
      const uri::URI& fragment_uri);

  /** Destructor. */
  ~FragmentMetadata();

  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Returns the bounding coordinates. */
  const std::vector<void*>& bounding_coords() const;

  /** Returns the number of cells in the tile at the input position. */
  int64_t cell_num(int64_t tile_pos) const;

  /**
   * Returns ture if the corresponding fragment is dense, and false if it
   * is sparse.
   */
  bool dense() const;

  /** Returns the (expanded) domain in which the fragment is constrained. */
  const void* domain() const;

  /** Returns the number of cells in the last tile. */
  int64_t last_tile_cell_num() const;

  /** Returns the MBRs. */
  const std::vector<void*>& mbrs() const;

  /** Returns the non-empty domain in which the fragment is constrained. */
  const void* non_empty_domain() const;

  /** Returns the number of tiles in the fragment. */
  int64_t tile_num() const;

  /** Returns the tile offsets. */
  // TODO: use either uint64_t or int64_t
  const std::vector<std::vector<off_t>>& tile_offsets() const;

  /** Returns the variable tile offsets. */
  // TODO: use either uint64_t or int64_t
  const std::vector<std::vector<off_t>>& tile_var_offsets() const;

  /** Returns the variable tile sizes. */
  // TODO: use either uint64_t or int64_t
  const std::vector<std::vector<size_t>>& tile_var_sizes() const;

  /* ********************************* */
  /*             MUTATORS              */
  /* ********************************* */

  /**
   * Appends the tile bounding coordinates to the fragment metadata.
   *
   * @param bounding_coords The bounding coordinates to be appended.
   * @return void
   */
  void append_bounding_coords(const void* bounding_coords);

  /**
   * Appends the input MBR to the fragment metadata.
   *
   * @param mbr The MBR to be appended.
   * @return void
   */
  void append_mbr(const void* mbr);

  /**
   * Appends a tile offset for the input attribute.
   *
   * @param attribute_id The id of the attribute for which the offset is
   *     appended.
   * @param step This is essentially the step by which the previous
   *     offset will be expanded. It is practically the last tile size.
   * @return void
   */
  void append_tile_offset(int attribute_id, size_t step);

  /**
   * Appends a variable tile offset for the input attribute.
   *
   * @param attribute_id The id of the attribute for which the offset is
   *     appended.
   * @param step This is essentially the step by which the previous
   *     offset will be expanded. It is practically the last variable tile size.
   * @return void
   */
  void append_tile_var_offset(int attribute_id, size_t step);

  /**
   * Appends a variable tile size for the input attribute.
   *
   * @param attribute_id The id of the attribute for which the size is appended.
   * @param size The size to be appended.
   * @return void
   */
  void append_tile_var_size(int attribute_id, size_t size);

  /**
   * Finalizes fragment metadata, properly flushing them to the disk.
   *
   * @return TILEDB_BK_OK on success and TILEDB_BK_ERR on error.
   */
  Status flush();

  /**
   * Initializes the fragment metadata structures.
   *
   * @param non_empty_domain The non-empty domain in which the array read/write
   *     will be constrained.
   * @return TILEDB_BK_OK for success, and TILEDB_OK_ERR for error.
   */
  Status init(const void* non_empty_domain);

  /**
   * Loads the fragment metadata structures from the disk.
   *
   * @return TILEDB_BK_OK for success, and TILEDB_OK_ERR for error.
   */
  Status load();

  /**
   * Simply sets the number of cells for the last tile.
   *
   * @param cell_num The number of cells for the last tile.
   * @return void
   */
  void set_last_tile_cell_num(int64_t cell_num);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array schema */
  const ArraySchema* array_schema_;

  /** The first and last coordinates of each tile. */
  std::vector<void*> bounding_coords_;

  /** True if the fragment is dense, and false if it is sparse. */
  bool dense_;

  /**
   * The (expanded) domain in which the fragment is constrained. "Expanded"
   * means that the domain is enlarged minimally to coincide with tile
   * boundaries (if there is a tile grid imposed by tile extents). Note that the
   * type of the domain must be the same as the type of the array coordinates.
   */
  void* domain_;

  /** The uri of the fragment the metadata belongs to. */
  uri::URI fragment_uri_;

  /** Number of cells in the last tile (meaningful only in the sparse case). */
  int64_t last_tile_cell_num_;

  /** The MBRs (applicable only to the sparse case with irregular tiles). */
  std::vector<void*> mbrs_;

  /** The offsets of the next tile for each attribute. */
  std::vector<off_t> next_tile_offsets_;

  /** The offsets of the next variable tile for each attribute. */
  std::vector<off_t> next_tile_var_offsets_;

  /**
   * The non-empty domain in which the fragment is constrained. Note that the
   * type of the domain must be the same as the type of the array coordinates.
   */
  void* non_empty_domain_;

  /**
   * The tile offsets in their corresponding attribute files. Meaningful only
   * when there is compression.
   */
  std::vector<std::vector<off_t>> tile_offsets_;

  /**
   * The variable tile offsets in their corresponding attribute files.
   * Meaningful only for variable-sized tiles.
   */
  std::vector<std::vector<off_t>> tile_var_offsets_;

  /**
   * The sizes of the uncompressed variable tiles.
   * Meaningful only when there is compression for variable tiles.
   */
  std::vector<std::vector<size_t>> tile_var_sizes_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Writes the bounding coordinates to the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status write_bounding_coords(Buffer* buff);

  /**
   * Writes the cell number of the last tile to the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status write_last_tile_cell_num(Buffer* buff);

  /**
   * Writes the MBRs to the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status write_mbrs(Buffer* buff);

  /**
   * Writes the non-empty domain to the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status write_non_empty_domain(Buffer* buff);

  /**
   * Writes the tile offsets to the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status write_tile_offsets(Buffer* buff);

  /**
   * Writes the variable tile offsets to the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status write_tile_var_offsets(Buffer* buff);

  /**
   * Writes the variable tile sizes to the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status write_tile_var_sizes(Buffer* buff);

  /**
   * Loads the bounding coordinates from the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status load_bounding_coords(Buffer* buff);

  /**
   * Loads the cell number of the last tile from the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status load_last_tile_cell_num(Buffer* buff);

  /**
   * Loads the MBRs from the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status load_mbrs(Buffer* buff);

  /**
   * Loads the non-empty domain from the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status load_non_empty_domain(Buffer* buff);

  /**
   * Loads the tile offsets from the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status load_tile_offsets(Buffer* buff);

  /**
   * Loads the variable tile offsets from the fragment metadata buffer.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status load_tile_var_offsets(Buffer* buff);

  /**
   * Loads the variable tile sizes from the fragment metadata.
   *
   * @param buff Buffer pointer
   * @return Status
   */
  Status load_tile_var_sizes(Buffer* buff);
};

}  // namespace tiledb

#endif  // TILEDB_FRAGMENTMETADATA_H
