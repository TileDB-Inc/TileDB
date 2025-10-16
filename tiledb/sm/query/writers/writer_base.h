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

#include "tiledb/common/common.h"
#include "tiledb/common/indexed_list.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/fragment/written_fragment_info.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/strategy_base.h"
#include "tiledb/sm/query/writers/dense_tiler.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/tile/writer_tile_tuple.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Array;
class DomainBuffersView;
class FragmentMetadata;
class TileMetadataGenerator;

using WriterTileTupleVector = IndexedList<WriterTileTuple>;

/** Processes write queries. */
class WriterBase : public StrategyBase, public IQueryStrategy {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  WriterBase(
      stats::Stats* stats,
      shared_ptr<Logger> logger,
      StrategyParams& params,
      std::vector<WrittenFragmentInfo>& written_fragment_info,
      bool disable_checks_consolidation,
      Query::CoordsInfo& coords_info_,
      bool remote_query,
      optional<std::string> fragment_name = nullopt);

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

  /** Initialize the memory budget variables. */
  void refresh_config();

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
   * in the global order or have duplicates. This supercedes the config.
   */
  bool disable_checks_consolidation_;

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

  /** Timestamps for the new fragment to be created. */
  std::pair<uint64_t, uint64_t> fragment_timestamp_range_;

  /** Stores information about the written fragments. */
  std::vector<WrittenFragmentInfo>& written_fragment_info_;

  /** Allocated buffers that neeed to be cleaned upon destruction. */
  std::vector<void*> to_clean_;

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /** Used in serialization to track if the writer belongs to a remote query */
  bool remote_query_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /** Utility function for constructing new FragmentMetadata instances. */
  shared_ptr<FragmentMetadata> create_fragment_metadata();

  /** Adss a fragment to `written_fragment_info_`. */
  Status add_written_fragment_info(const URI& uri);

  /** Correctness checks for buffer sizes. */
  void check_buffer_sizes() const;

  /**
   * Throws an error if there are coordinates falling out-of-bounds, i.e.,
   * outside the array domain.
   *
   * @return Status
   */
  Status check_coord_oob() const;

  /** Correctness checks for `subarray_`. */
  void check_subarray() const;

  /**
   * Check the validity of the provided buffer offsets for a variable attribute.
   *
   * @return Status
   */
  void check_var_attr_offsets() const;

  /**
   * Throws an error if ordered data buffers do not have the expected sort.
   *
   * This method only checks currently loaded data. It does not check the
   * sort of data in subsequent writes for the global order writer.
   *
   * For unordered writes, this method will need to be modified to take into
   * account the sort order.
   */
  void check_attr_order() const;

  /**
   * Cleans up the coordinate buffers. Applicable only if the coordinate
   * buffers were allocated by TileDB (not the user)
   */
  void clear_coord_buffers();

  /** Closes all attribute files, flushing their state to storage. */
  Status close_files(shared_ptr<FragmentMetadata> meta) const;

  /**
   * Computes the MBRs.
   *
   * @param start_tile_idx The index of the first tile to compute MBR for
   * @param end_tile_idx The index of the last tile to compute MBR for
   * @param tiles The tiles to calculate the MBRs from. It is a map of vectors,
   * one vector of tiles per dimension/coordinates.
   * @return MBRs.
   */
  std::vector<NDRange> compute_mbrs(
      uint64_t start_tile_idx,
      uint64_t end_tile_idx,
      const tdb::pmr::unordered_map<std::string, WriterTileTupleVector>& tiles)
      const;

  /**
   * Computes the MBRs for all of the requested tiles. See above.
   */
  std::vector<NDRange> compute_mbrs(
      const tdb::pmr::unordered_map<std::string, WriterTileTupleVector>& tiles)
      const {
    return compute_mbrs(0, tiles.begin()->second.size(), tiles);
  }

  /**
   * Set the coordinates metadata (e.g., MBRs).
   *
   * @param start_tile_idx Index of the first tile to set metadata for.
   * @param end_tile_idx Index of the last tile to set metadata for.
   * @param tiles The tiles to calculate the coords metadata from. It is
   *     a map of vectors, one vector of tiles per dimension/coordinates.
   * @param mbrs The MBRs.
   * @param meta The fragment metadata that will store the coords metadata.
   */
  void set_coords_metadata(
      const uint64_t start_tile_idx,
      const uint64_t end_tile_idx,
      const tdb::pmr::unordered_map<std::string, WriterTileTupleVector>& tiles,
      const std::vector<NDRange>& mbrs,
      shared_ptr<FragmentMetadata> meta) const;

  /**
   * Computes the tiles metadata (min/max/sum/null count).
   *
   * @param start_tile_idx The index of the first tile to compute metadata for
   * @param end_tile_idx The index of the last tile to compute metadata for
   * @param tiles The tiles to calculate the tile metadata from. It is
   *     a map of vectors, one vector of tiles per dimension.
   * @return Status
   */
  Status compute_tiles_metadata(
      uint64_t start_tile_idx,
      uint64_t end_tile_idx,
      tdb::pmr::unordered_map<std::string, WriterTileTupleVector>& tiles) const;

  /**
   * Computes the tiles metadata for each tile in the provided list. See above.
   */
  Status compute_tiles_metadata(
      tdb::pmr::unordered_map<std::string, WriterTileTupleVector>& tiles)
      const {
    return compute_tiles_metadata(0, tiles.begin()->second.size(), tiles);
  }

  /**
   * Returns the i-th coordinates in the coordinate buffers in string
   * format.
   */
  std::string coords_to_str(uint64_t i) const;

  /**
   * Creates a new fragment.
   *
   * This will create the fragment directory, fragment URI directory, and commit
   * directory (if they do not already exist) the first time it is called.
   *
   * @param dense Whether the fragment is dense or not.
   * @param frag_meta The fragment metadata to be generated.
   * @param domain Optional domain for the fragment, uses subarray 0th range if
   *               not provided
   * @return Status
   */
  Status create_fragment(
      bool dense,
      shared_ptr<FragmentMetadata>& frag_meta,
      const NDRange* domain = nullptr);

  /**
   * Runs the input coordinate and attribute tiles through their
   * filter pipelines. The tile buffers are modified to contain the output
   * of the pipeline.
   *
   * @param start_tile_idx The index of the first tile to filter
   * @param end_tile_idx The index of the last tile to filter
   */
  Status filter_tiles(
      uint64_t start_tile_idx,
      uint64_t end_tile_idx,
      tdb::pmr::unordered_map<std::string, WriterTileTupleVector>* tiles);

  /**
   * See above, filtering all of the provided tiles.
   */
  Status filter_tiles(
      tdb::pmr::unordered_map<std::string, WriterTileTupleVector>* tiles) {
    return filter_tiles(0, tiles->begin()->second.size(), tiles);
  }

  /**
   * Runs the input tiles for the input attribute through the filter pipeline.
   * The tile buffers are modified to contain the output of the pipeline.
   *
   * @param start_tile_idx The index of the first tile to filter
   * @param end_tile_idx The index of the last tile to filter
   * @param name The attribute/dimension the tiles belong to.
   * @param tile The tiles to be filtered.
   * @return Status
   */
  Status filter_tiles(
      uint64_t start_tile_idx,
      uint64_t end_tile_idx,
      const std::string& name,
      WriterTileTupleVector* tiles);

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
   * @param nullable True if the tile to be filtered contains validity values.
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
      WriterTileTupleVector* tiles) const;

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
  void check_extra_element();

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
   * Writes a number of the input tiles to storage for all
   * dimensions/attributes.
   *
   * @param start_tile_idx Index of the first tile to write.
   * @param end_tile_idx Index of the last tile to write.
   * @param frag_meta Current fragment metadata.
   * @param tiles Attribute/Coordinate tiles to be written, one element per
   *     attribute or dimension.
   * @return Status
   */
  Status write_tiles(
      const uint64_t start_tile_idx,
      const uint64_t end_tile_idx,
      shared_ptr<FragmentMetadata> frag_meta,
      tdb::pmr::unordered_map<std::string, WriterTileTupleVector>* tiles);

  /**
   * Writes the input tiles for the input attribute/dimension to storage.
   *
   * @param start_tile_idx Index of the first tile to write.
   * @param end_tile_idx Index of the last tile to write.
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
      const uint64_t start_tile_idx,
      const uint64_t end_tile_idx,
      const std::string& name,
      shared_ptr<FragmentMetadata> frag_meta,
      uint64_t start_tile_id,
      WriterTileTupleVector* tiles,
      bool close_files = true);

  /**
   * Calculates the hilbert values of the input coordinate buffers.
   *
   * @param[in] domain_buffers QueryBuffers for which to calculate values
   * @param[out] hilbert_values Output values written into caller-defined vector
   */
  Status calculate_hilbert_values(
      const DomainBuffersView& domain_buffers,
      std::vector<uint64_t>& hilbert_values) const;

  /**
   * Prepares, filters and writes dense tiles for the given attribute.
   *
   * @tparam T The array domain datatype.
   * @param name The attribute name.
   * @param tile_batches The attribute tile batches.
   * @param frag_meta The metadata of the new fragment.
   * @param dense_tiler The dense tiler that will prepare the tiles.
   * @param thread_num The number of threads to be used for the function.
   * @param stats Statistics to gather in the function.
   */
  template <class T>
  Status prepare_filter_and_write_tiles(
      const std::string& name,
      IndexedList<WriterTileTupleVector>& tile_batches,
      tdb_shared_ptr<FragmentMetadata> frag_meta,
      DenseTiler<T>* dense_tiler,
      uint64_t thread_num);

  /**
   * Returns true if this write strategy is part of a remote query
   */
  bool remote_query() const;
};

}  // namespace tiledb::sm

#endif  // TILEDB_WRITER_BASE_H
