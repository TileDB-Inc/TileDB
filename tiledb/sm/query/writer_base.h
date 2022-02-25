/**
 * @file   writer_base.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file defines class WriterBase.
 */

#ifndef TILEDB_WRITER_BASE_H
#define TILEDB_WRITER_BASE_H

#include <atomic>

#include "tiledb/common/status.h"
#include "tiledb/sm/fragment/written_fragment_info.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/dense_tiler.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/strategy_base.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/tile/writer_tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class FragmentMetadata;
class TileMetadataGenerator;
class StorageManager;

/** Processes write queries. */
class WriterBase : public StrategyBase, public IQueryStrategy {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  WriterBase(
      stats::Stats* stats,
      tdb_shared_ptr<Logger> logger,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout,
      std::vector<WrittenFragmentInfo>& written_fragment_info,
      bool disable_check_global_order,
      Query::CoordsInfo& coords_info_,
      URI fragment_uri = URI(""));

  /** Destructor. */
  ~WriterBase();

  DISABLE_COPY_AND_COPY_ASSIGN(WriterBase);
  DISABLE_MOVE_AND_MOVE_ASSIGN(WriterBase);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the names of the buffers set by the user for the write query. */
  std::vector<std::string> buffer_names() const;

  /** Writer is never in an imcomplete state. */
  bool incomplete() const {
    return false;
  }

  /** Writer is never in an imcomplete state. */
  QueryStatusDetailsReason status_incomplete_reason() const {
    return QueryStatusDetailsReason::REASON_NONE;
  }

  /** Returns current setting of check_coord_dups_ */
  bool get_check_coord_dups() const;

  /** Returns current setting of check_coord_oob_ */
  bool get_check_coord_oob() const;

  /** Returns current setting of dedup_coords_ */
  bool get_dedup_coords() const;

  /** Initializes the writer. */
  Status init();

  /** Initialize the memory budget variables. */
  Status initialize_memory_budget();

  /** Sets current setting of check_coord_dups_ */
  void set_check_coord_dups(bool b);

  /** Sets current setting of check_coord_oob_ */
  void set_check_coord_oob(bool b);

  /** Sets current setting of dedup_coords_ */
  void set_dedup_coords(bool b);

 protected:
  /* ********************************* */
  /*        PROTECTED ATTRIBUTES       */
  /* ********************************* */

  /**
   * The sizes of the coordinate buffers in a map (dimension -> size).
   * Needed separate storage since QueryBuffer stores a pointer to the buffer
   * sizes.
   */
  std::unordered_map<std::string, uint64_t> coord_buffer_sizes_;

  /**
   * If `true`, it will not check if the written coordinates are
   * in the global order. This supercedes the config.
   */
  bool disable_check_global_order_;

  /** Keeps track of the coords data. */
  Query::CoordsInfo& coords_info_;

  /**
   * Meaningful only when `dedup_coords_` is `false`.
   * If `true`, a check for duplicate coordinates will be performed upon
   * sparse writes and appropriate errors will be thrown in case
   * duplicates are found.
   */
  bool check_coord_dups_;

  /**
   * If `true`, a check for coordinates lying out-of-bounds (i.e.,
   * outside the array domain) will be performed upon
   * sparse writes and appropriate errors will be thrown in case
   * such coordinates are found.
   */
  bool check_coord_oob_;

  /**
   * If `true`, the coordinates will be checked whether the
   * obey the global array order and appropriate errors will be thrown.
   */
  bool check_global_order_;

  /**
   * If `true`, deduplication of coordinates/cells will happen upon
   * sparse writes. Ties are broken arbitrarily.
   *
   */
  bool dedup_coords_;

  /** The name of the new fragment to be created. */
  URI fragment_uri_;

  /** True if the writer has been initialized. */
  bool initialized_;

  /** Stores information about the written fragments. */
  std::vector<WrittenFragmentInfo>& written_fragment_info_;

  /** Allocated buffers that neeed to be cleaned upon destruction. */
  std::vector<void*> to_clean_;

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /** Adss a fragment to `written_fragment_info_`. */
  Status add_written_fragment_info(const URI& uri);

  /** Calculates the hilbert values of the input coordinate buffers. */
  Status calculate_hilbert_values(
      const std::vector<const QueryBuffer*>& buffs,
      std::vector<uint64_t>* hilbert_values) const;

  /** Correctness checks for buffer sizes. */
  Status check_buffer_sizes() const;

  /**
   * Throws an error if there are coordinates falling out-of-bounds, i.e.,
   * outside the array domain.
   *
   * @return Status
   */
  Status check_coord_oob() const;

  /** Correctness checks for `subarray_`. */
  Status check_subarray() const;

  /**
   * Check the validity of the provided buffer offsets for a variable attribute.
   *
   * @return Status
   */
  Status check_var_attr_offsets() const;

  /**
   * Cleans up the coordinate buffers. Applicable only if the coordinate
   * buffers were allocated by TileDB (not the user)
   */
  void clear_coord_buffers();

  /** Closes all attribute files, flushing their state to storage. */
  Status close_files(tdb_shared_ptr<FragmentMetadata> meta) const;

  /**
   * Computes the coordinates metadata (e.g., MBRs).
   *
   * @param tiles The tiles to calculate the coords metadata from. It is
   *     a map of vectors, one vector of tiles per dimension.
   * @param meta The fragment metadata that will store the coords metadata.
   * @return Status
   */
  Status compute_coords_metadata(
      const std::unordered_map<std::string, std::vector<WriterTile>>& tiles,
      tdb_shared_ptr<FragmentMetadata> meta) const;

  /**
   * Computes the tiles metadata (min/max/sum/null count).
   *
   * @param tile_num The number of tiles.
   * @param tiles The tiles to calculate the tile metadata from. It is
   *     a map of vectors, one vector of tiles per dimension.
   * @return Status
   */
  Status compute_tiles_metadata(
      uint64_t tile_num,
      std::unordered_map<std::string, std::vector<WriterTile>>& tiles) const;

  /**
   * Returns the i-th coordinates in the coordinate buffers in string
   * format.
   */
  std::string coords_to_str(uint64_t i) const;

  /**
   * Creates a new fragment.
   *
   * @param dense Whether the fragment is dense or not.
   * @param frag_meta The fragment metadata to be generated.
   * @return Status
   */
  Status create_fragment(
      bool dense, tdb_shared_ptr<FragmentMetadata>& frag_meta) const;

  /**
   * Runs the input coordinate and attribute tiles through their
   * filter pipelines. The tile buffers are modified to contain the output
   * of the pipeline.
   */
  Status filter_tiles(
      std::unordered_map<std::string, std::vector<WriterTile>>* tiles);

  /**
   * Runs the input tiles for the input attribute through the filter pipeline.
   * The tile buffers are modified to contain the output of the pipeline.
   *
   * @param name The attribute/dimension the tiles belong to.
   * @param tile The tiles to be filtered.
   * @return Status
   */
  Status filter_tiles(const std::string& name, std::vector<WriterTile>* tiles);

  /**
   * Runs the input tile for the input attribute/dimension through the filter
   * pipeline. The tile buffer is modified to contain the output of the
   * pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The tile to be filtered.
   * @param offsets_tile The offsets tile in case of a var tile, or null.
   * @param offsets True if the tile to be filtered contains offsets for a
   *    var-sized attribute/dimension.
   * @param offsets True if the tile to be filtered contains validity values.
   * @return Status
   */
  Status filter_tile(
      const std::string& name,
      WriterTile* tile,
      WriterTile* offsets_tile,
      bool offsets,
      bool nullable);

  /**
   * Determines if an attribute has min max metadata.
   *
   * @param name Attribute/dimension name.
   * @param var_size Is the attribute/dimension var size.
   * @return true if the atribute has min max metadata.
   */
  bool has_min_max_metadata(const std::string& name, const bool var_size);

  /**
   * Determines if an attribute has sum metadata.
   *
   * @param name Attribute/dimension name.
   * @param var_size Is the attribute/dimension var size.
   * @return true if the atribute has sum metadata.
   */
  bool has_sum_metadata(const std::string& name, const bool var_size);

  /**
   * Initializes a fixed-sized tile.
   *
   * @param name The attribute/dimension the tile belongs to.
   * @param tile The tile to be initialized.
   * @return Status
   */
  Status init_tile(const std::string& name, WriterTile* tile) const;

  /**
   * Initializes a var-sized tile.
   *
   * @param name The attribute/dimension the tile belongs to.
   * @param tile The offsets tile to be initialized.
   * @param tile_var The var-sized data tile to be initialized.
   * @return Status
   */
  Status init_tile(
      const std::string& name, WriterTile* tile, WriterTile* tile_var) const;

  /**
   * Initializes a fixed-sized, nullable tile.
   *
   * @param name The attribute the tile belongs to.
   * @param tile The tile to be initialized.
   * @param tile_validity The validity tile to be initialized.
   * @return Status
   */
  Status init_tile_nullable(
      const std::string& name,
      WriterTile* tile,
      WriterTile* tile_validity) const;

  /**
   * Initializes a var-sized, nullable tile.
   *
   * @param name The attribute the tile belongs to.
   * @param tile The offsets tile to be initialized.
   * @param tile_var The var-sized data tile to be initialized.
   * @param tile_validity The validity tile to be initialized.
   * @return Status
   */
  Status init_tile_nullable(
      const std::string& name,
      WriterTile* tile,
      WriterTile* tile_var,
      WriterTile* tile_validity) const;

  /**
   * Initializes the tiles for writing for the input attribute/dimension.
   *
   * @param name The attribute/dimension the tiles belong to.
   * @param tile_num The number of tiles.
   * @param tiles The tiles to be initialized. Note that the vector
   *     has been already preallocated.
   * @return Status
   */
  Status init_tiles(
      const std::string& name,
      uint64_t tile_num,
      std::vector<WriterTile>* tiles) const;

  /**
   * Generates a new fragment name, which is in the form: <br>
   * `__t_t_uuid_v`, where `t` is the input timestamp and `v` is the current
   * format version. For instance,
   * `__1458759561320_1458759561320_6ba7b8129dad11d180b400c04fd430c8_3`.
   *
   * If `timestamp` is 0, then it is set to the current time.
   *
   * @param timestamp The timestamp of when the array got opened for writes. It
   *     is in ms since 1970-01-01 00:00:00 +0000 (UTC).
   * @param frag_uri Will store the new special fragment name
   * @return Status
   */
  Status new_fragment_name(
      uint64_t timestamp, uint32_t format_version, std::string* frag_uri) const;

  /**
   * Optimize the layout for 1D arrays. Specifically, if the array
   * is 1D and the query layout is not global or unordered, the layout
   * should be the same as the cell order of the array. This produces
   * equivalent results offering faster processing.
   */
  void optimize_layout_for_1D();

  /**
   * Checks the validity of the extra element from var-sized offsets of
   * attributes
   */
  Status check_extra_element();

  /**
   * Return an element of the offsets buffer at a certain position
   * taking into account the configured bitsize
   */
  inline uint64_t get_offset_buffer_element(
      const void* buffer, const uint64_t pos) const {
    if (offsets_bitsize_ == 32) {
      const uint32_t* buffer_32bit = reinterpret_cast<const uint32_t*>(buffer);
      return static_cast<uint64_t>(buffer_32bit[pos]);
    } else {
      return reinterpret_cast<const uint64_t*>(buffer)[pos];
    }
  }

  /**
   * Return the size of an offsets buffer according to the configured
   * options for variable-sized attributes
   */
  inline uint64_t get_offset_buffer_size(const uint64_t buffer_size) const {
    return offsets_extra_element_ ?
               buffer_size - constants::cell_var_offset_size :
               buffer_size;
  }

  /**
   * Return a buffer offset according to the configured options for
   * variable-sized attributes (e.g. transform a byte offset to element offset)
   */
  inline uint64_t prepare_buffer_offset(
      const void* buffer, const uint64_t pos, const uint64_t datasize) const {
    uint64_t offset = get_offset_buffer_element(buffer, pos);
    return offsets_format_mode_ == "elements" ? offset * datasize : offset;
  }

  /**
   * Splits the coordinates buffer into separate coordinate
   * buffers, one per dimension. Note that this will require extra memory
   * allocation, which will be cleaned up in the class destructor.
   *
   * @return Status
   */
  Status split_coords_buffer();

  /**
   * Writes all the input tiles to storage.
   *
   * @param tiles Attribute/Coordinate tiles to be written, one element per
   *     attribute or dimension.
   * @param tiles Attribute/Coordinate tiles to be written.
   * @return Status
   */
  Status write_all_tiles(
      tdb_shared_ptr<FragmentMetadata> frag_meta,
      std::unordered_map<std::string, std::vector<WriterTile>>* tiles);

  /**
   * Writes the input tiles for the input attribute/dimension to storage.
   *
   * @param name The attribute/dimension the tiles belong to.
   * @param frag_meta The fragment metadata.
   * @param start_tile_id The function will start writing tiles
   *     with ids in the fragment that start with this value.
   * @param tiles The tiles to be written.
   * @param close_files Whether to close the attribute/coordinate
   *     file in the end of the function call.
   * @return Status
   */
  Status write_tiles(
      const std::string& name,
      tdb_shared_ptr<FragmentMetadata> frag_meta,
      uint64_t start_tile_id,
      std::vector<WriterTile>* tiles,
      bool close_files = true);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_WRITER_BASE_H
