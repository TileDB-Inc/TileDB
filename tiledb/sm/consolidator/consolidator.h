/**
 * @file   consolidator.h
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
 * This file defines class Consolidator.
 */

#ifndef TILEDB_CONSOLIDATOR_H
#define TILEDB_CONSOLIDATOR_H

#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/resource/resource.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"

#include <vector>

using namespace tiledb::common;

namespace tiledb::sm {

class ArraySchema;
class Config;
class Query;
class URI;

/** Mode for the consolidator class. */
enum class ConsolidationMode {
  FRAGMENT,       // Fragment mode.
  FRAGMENT_META,  // Fragment metadata mode.
  ARRAY_META,     // Array metadata mode.
  COMMITS,        // Commits mode.
  GROUP_META      // Group metadata mode.
};

/** Handles array consolidation. */
class Consolidator {
 public:
  /* ********************************* */
  /*          FACTORY METHODS          */
  /* ********************************* */

  /**
   * Factory method to make a new Consolidator instance given the config mode.
   *
   * @param mode Consolidation mode.
   * @param config Configuration parameters for the consolidation
   *     (`nullptr` means default).
   * @param storage_manager Storage manager.
   * @return New Consolidator instance or nullptr on error.
   */
  static shared_ptr<Consolidator> create(
      tdb::RM& rm,
      const ConsolidationMode mode,
      const Config& config,
      StorageManager* storage_manager);

  /**
   * Returns the ConsolidationMode from the config.
   *
   * @param config Config.
   * @param vacuum_mode true for vacuum mode, false otherwise.
   * @return Consolidation mode.
   */
  static ConsolidationMode mode_from_config(
      const Config& config, const bool vacuum_mode = false);

  /* ********************************* */
  /*            DESTRUCTORS            */
  /* ********************************* */

  /** Destructor. */
  virtual ~Consolidator();

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
  virtual Status consolidate(
      const char* array_name,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Performs the vacuuming operation.
   *
   * @param array_name URI of array to vacuum.
   */
  virtual void vacuum(const char* array_name);

 protected:
  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param storage_manager Storage manager.
   */
  explicit Consolidator(StorageManager* storage_manager);

  /**
   * Checks if the array is remote.
   *
   * @param array_name Name of the array to check.
   */
  void check_array_uri(const char* array_name);

  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /** Consolidation configuration parameters. */
  struct ConsolidationConfigBase {
    /** Start time for consolidation. */
    uint64_t timestamp_start_;
    /** End time for consolidation. */
    uint64_t timestamp_end_;
  };

  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The class stats. */
  stats::Stats* stats_;

  /** The class logger. */
  shared_ptr<Logger> logger_;

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;
};

}  // namespace tiledb::sm

#endif  // TILEDB_FRAGMENT_H
