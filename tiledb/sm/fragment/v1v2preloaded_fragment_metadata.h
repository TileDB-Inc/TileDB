/**
 * @file  v1v2preloaded_fragment_metadata.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines class V1V2PreloadedFragmentMetadata.
 */

#ifndef TILEDB_V1V2PRELOADED_FRAGMENT_METADATA_H
#define TILEDB_V1V2PRELOADED_FRAGMENT_METADATA_H

#include "tiledb/common/common.h"
#include "tiledb/common/pmr.h"
#include "tiledb/sm/fragment/loaded_fragment_metadata.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/rtree/rtree.h"
#include "tiledb/sm/storage_manager/context_resources.h"

namespace tiledb {
namespace sm {

/** Collection of lazily loaded fragment metadata */
class V1V2PreloadedFragmentMetadata : public LoadedFragmentMetadata {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param resources A context resources instance.
   * @param memory_tracker The memory tracker of the array this fragment
   *     metadata corresponds to.
   */
  V1V2PreloadedFragmentMetadata(
      FragmentMetadata& parent, shared_ptr<MemoryTracker> memory_tracker);

  /* Destructor */
  ~V1V2PreloadedFragmentMetadata() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Loads the tile offsets for the input attribute from the input buffer.
   * Applicable to versions 1 and 2
   *
   * @param deserializer A deserializer to read data from
   */
  void load_tile_offsets(Deserializer& deserializer);

  /**
   * Loads the variable tile offsets from the input buffer.
   * Applicable to versions 1 and 2
   *
   * @param deserializer A deserializer to read data from
   */
  void load_tile_var_offsets(Deserializer& deserializer);

  /**
   * Loads the variable tile sizes from the input buffer.
   *
   * @param deserializer A deserializer to read data from
   */
  void load_tile_var_sizes(Deserializer& deserializer);

  /**
   * Loads the R-tree from storage.
   *
   * @param deserializer A deserializer to read data from
   */
  virtual void load_rtree(const EncryptionKey& encryption_key) override;

  /**
   * Loads the min max sum null count values for the fragment.
   *
   * @param encryption_key The key the array got opened with.
   */
  virtual void load_fragment_min_max_sum_null_count(
      const EncryptionKey& encryption_key) override;

  /**
   * Loads the tile global order bounds for the fragment.
   *
   * @param encrpytion_key The key the array was opened with.
   */
  virtual void load_fragment_tile_global_order_bounds(
      const EncryptionKey& encryption_key) override;

  /**
   * Loads the processed conditions for the fragment. The processed conditions
   * is the list of delete/update conditions that have already been applied for
   * this fragment and don't need to be applied again.
   *
   * @param encryption_key The key the array got opened with.
   */
  virtual void load_processed_conditions(
      const EncryptionKey& encryption_key) override;

  /**
   * Retrieves the overlap of all MBRs with the input ND range.
   *
   * @param range The range to use
   * @param is_default If default range should be used
   * @param tile_overlap The resulted tile overlap
   */
  virtual void get_tile_overlap(
      const NDRange& range,
      std::vector<bool>& is_default,
      TileOverlap* tile_overlap) override;

  /**
   * Compute tile bitmap for the curent fragment/range/dimension.
   *
   * @param range The range to use
   * @param d The dimension index
   * @param tile_bitmap The resulted tile bitmap
   */
  virtual void compute_tile_bitmap(
      const Range& range,
      unsigned d,
      std::vector<uint8_t>* tile_bitmap) override;

 private:
  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Loads the tile offsets for the input attribute or dimension idx
   * from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_offsets(
      const EncryptionKey& encryption_key, unsigned idx) override;

  /**
   * Loads the variable tile offsets for the input attribute or dimension idx
   * from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_var_offsets(
      const EncryptionKey& encryption_key, unsigned idx) override;

  /**
   * Loads the variable tile sizes for the input attribute or dimension idx
   * from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_var_sizes(
      const EncryptionKey& encryption_key, unsigned idx) override;

  /**
   * Loads the validity tile offsets for the input attribute idx from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_validity_offsets(
      const EncryptionKey& encryption_key, unsigned idx) override;

  /**
   * Loads the min values for the input attribute idx from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_min_values(
      const EncryptionKey& encryption_key, unsigned idx) override;

  /**
   * Loads the max values for the input attribute idx from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_max_values(
      const EncryptionKey& encryption_key, unsigned idx) override;

  /**
   * Loads the global order minimum values for the given dimension from storage.
   *
   * This is a no-op because this field does not exist in format versions
   * using this class.
   *
   * @param encryption_key The encryption key
   * @param dimension Dimension index
   */
  virtual void load_tile_global_order_min_values(
      const EncryptionKey& encryption_key, unsigned dimension) override;

  /**
   * Loads the global order maximum values for the given dimension from storage.
   *
   * This is a no-op because this field does not exist in format versions
   * using this class.
   *
   * @param encryption_key The encryption key
   * @param dimension Dimension index
   */
  virtual void load_tile_global_order_max_values(
      const EncryptionKey& encryption_key, unsigned dimension) override;

  /**
   * Loads the sum values for the input attribute idx from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_sum_values(
      const EncryptionKey& encryption_key, unsigned idx) override;

  /**
   * Loads the null count values for the input attribute idx from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_null_count_values(
      const EncryptionKey& encryption_key, unsigned idx) override;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_V1V2PRELOADED_FRAGMENT_METADATA_H
