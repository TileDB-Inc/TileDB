/**
 * @file  ondemand_fragment_metadata.h
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
 * This file defines class OndemandFragmentMetadata.
 */

#ifndef TILEDB_ONDEMAND_FRAGMENT_METADATA_H
#define TILEDB_ONDEMAND_FRAGMENT_METADATA_H

#include <deque>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "tiledb/common/common.h"
#include "tiledb/common/pmr.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/fragment/offsets_fragment_metadata.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/rtree/rtree.h"
#include "tiledb/sm/storage_manager/context_resources.h"

namespace tiledb {
namespace sm {

/** Collection of lazily loaded fragment metadata */
class OndemandFragmentMetadata : public OffsetsFragmentMetadata {
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
  OndemandFragmentMetadata(
      FragmentMetadata& parent, shared_ptr<MemoryTracker> memory_tracker);

  /* Destructor */
  virtual ~OndemandFragmentMetadata() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Loads the tile offsets for the input attribute or dimension idx
   * from storage.
   */
  virtual void load_tile_offsets(
      const EncryptionKey& encryption_key, unsigned idx) override;

  /**
   * Loads the variable tile offsets for the input attribute or dimension idx
   * from storage.
   */
  virtual void load_tile_var_offsets(
      const EncryptionKey& encryption_key, unsigned idx) override;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ONDEMAND_FRAGMENT_METADATA_H
