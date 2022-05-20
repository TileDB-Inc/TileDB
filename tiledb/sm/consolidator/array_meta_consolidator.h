/**
 * @file   array_meta_consolidator.h
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
 * This file defines class ArrayMetaConsolidator.
 */

#ifndef TILEDB_ARRAY_META_CONSOLIDATOR_H
#define TILEDB_ARRAY_META_CONSOLIDATOR_H

#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/consolidator/consolidator.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class StorageManager;

/** Handles array metadata consolidation. */
class ArrayMetaConsolidator : public Consolidator {
  // Declare Consolidator as friend so it can use the private constructor.
  friend class Consolidator;

 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param config Config.
   * @param storage_manager Storage manager.
   */
  explicit ArrayMetaConsolidator(
      const Config* config, StorageManager* storage_manager);

  /** Destructor. */
  ~ArrayMetaConsolidator() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(ArrayMetaConsolidator);
  DISABLE_MOVE_AND_MOVE_ASSIGN(ArrayMetaConsolidator);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Performs the consolidation operation.
   *
   * @param array_name URI of array to consolidate.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   */
  Status consolidate(
      const char* array_name,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Performs the vacuuming operation.
   *
   * @param array_name URI of array to consolidate.
   * @return Status
   */
  Status vacuum(const char* array_name);

 private:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /** Consolidation configuration parameters. */
  struct ConsolidationConfig {
    /** Start time for consolidation. */
    uint64_t timestamp_start_;
    /** End time for consolidation. */
    uint64_t timestamp_end_;
    /** Start time for vacuuming. */
    uint64_t vacuum_timestamp_start_;
    /** End time for vacuuming. */
    uint64_t vacuum_timestamp_end_;
  };

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Checks and sets the input configuration parameters. */
  Status set_config(const Config* config);

  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** Consolidation configuration parameters. */
  ConsolidationConfig config_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ARRAY_META_CONSOLIDATOR_H
