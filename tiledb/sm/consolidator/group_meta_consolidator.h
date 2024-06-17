/**
 * @file   group_meta_consolidator.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file defines class GroupMetaConsolidator.
 */

#ifndef TILEDB_GROUP_META_CONSOLIDATOR
#define TILEDB_GROUP_META_CONSOLIDATOR

#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/consolidator/consolidator.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"

using namespace tiledb::common;

namespace tiledb::sm {

/** Handles group metadata consolidation. */
class GroupMetaConsolidator : public Consolidator {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * This is a transitional constructor in the sense that we are working
   * on removing the dependency of all Consolidator classes on StorageManager.
   * For now we still need to keep the storage_manager argument, but once the
   * dependency is gone the signature will be
   * GroupMetaConsolidator(ContextResources&, const Config&).
   *
   * @param resources The context resources.
   * @param config Config.
   * @param storage_manager A StorageManager pointer.
   *    (this will go away in the near future)
   */
  explicit GroupMetaConsolidator(
      ContextResources& resources,
      const Config& config,
      StorageManager* storage_manager);

  /** Destructor. */
  ~GroupMetaConsolidator() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(GroupMetaConsolidator);
  DISABLE_MOVE_AND_MOVE_ASSIGN(GroupMetaConsolidator);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Performs the consolidation operation.
   *
   * @param group_name URI of group to consolidate.
   * @param encryption_type The encryption type of the group
   * @param encryption_key If the group is encrypted, the private encryption
   *    key. For unencrypted groups, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   */
  Status consolidate(
      const char* group_name,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length) override;

  /**
   * Performs the vacuuming operation.
   *
   * @param group_name URI of group to consolidate.
   */
  void vacuum(const char* group_name) override;

 private:
  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Checks and sets the input configuration parameters. */
  Status set_config(const Config& config);

  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** Consolidation configuration parameters. */
  Consolidator::ConsolidationConfigBase config_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_GROUP_META_CONSOLIDATOR
