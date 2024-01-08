/**
 * @file   fragment_consolidator.h
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
 * This file defines class FragmentConsolidator.
 */

#ifndef TILEDB_FRAGMENT_CONSOLIDATOR_H
#define TILEDB_FRAGMENT_CONSOLIDATOR_H

#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/consolidator/consolidator.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"

#include <vector>

using namespace tiledb::common;

namespace tiledb::sm {

class ArraySchema;
class Config;
class Query;
class URI;

/** Handles fragment consolidation. */
class FragmentConsolidator : public Consolidator {
  friend class WhiteboxFragmentConsolidator;

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
  explicit FragmentConsolidator(
      tdb::RM& rm, const Config& config, StorageManager* storage_manager);

  /** Destructor. */
  ~FragmentConsolidator() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(FragmentConsolidator);
  DISABLE_MOVE_AND_MOVE_ASSIGN(FragmentConsolidator);

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
   * Consolidates only the fragments of the input array using a list of
   * fragments. Note that this might change ordering of fragments and
   * currently does no checks for non-empty domains. It must be used
   * carefully.
   *
   * @param array_name URI of array to consolidate.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @param fragment_uris The list of the fragments to consolidate.
   * @param config Configuration parameters for the consolidation
   *     (`nullptr` means default).
   * @return Status
   */
  Status consolidate_fragments(
      const char* array_name,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length,
      const std::vector<std::string>& fragment_uris);

  /**
   * Performs the vacuuming operation.
   *
   * @param array_name URI of array to vacuum.
   */
  void vacuum(const char* array_name);

 private:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /** Consolidation configuration parameters. */
  struct ConsolidationConfig : Consolidator::ConsolidationConfigBase {
    /**
     * Include timestamps in the consolidated fragment or not.
     */
    bool with_timestamps_;
    /**
     * Include delete metadata in the consolidated fragment or not.
     */
    bool with_delete_meta_;
    /**
     * The factor by which the size of the dense fragment resulting
     * from consolidating a set of fragments (containing at least one
     * dense fragment) can be amplified. This is important when
     * the union of the non-empty domains of the fragments to be
     * consolidated have a lot of empty cells, which the consolidated
     * fragment will have to fill with the special fill value
     * (since the resulting fragments is dense).
     */
    float amplification_;
    /** Attribute buffer size. */
    uint64_t buffer_size_;
    /** Total buffer size for all attributes. */
    uint64_t total_buffer_size_;
    /** Max fragment size. */
    uint64_t max_fragment_size_;
    /**
     * Number of consolidation steps performed in a single
     * consolidation invocation.
     */
    uint32_t steps_;
    /** Minimum number of fragments to consolidate in a single step. */
    uint32_t min_frags_;
    /** Maximum number of fragments to consolidate in a single step. */
    uint32_t max_frags_;
    /**
     * Minimum size ratio for two fragments to be considered for
     * consolidation.
     */
    float size_ratio_;
    /** Is the refactored reader in use or not */
    bool use_refactored_reader_;
    /** Purge deleted cells or not. */
    bool purge_deleted_cells_;
  };

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Checks if the fragments between `start` and `end` (inclusive)
   * in `fragments` are allowed to be consolidated. A set of fragments
   * is allowed to be consolidated if all fragments are sparse,
   * or (i) no earlier fragment's (before `start`) non-empty domain
   * overlaps with the union of the non-empty domains of the
   * fragments, and (ii) the union of the non-empty domains of the
   * fragments is more than an amplification factor larger than the
   * sum of sizes of the separate fragment non-empty domains.
   *
   * @param domain The array domain.
   * @param fragment_info The input fragment info.
   * @param start The function will focus on fragments between
   *     positions `start` and `end`.
   * @param end The function will focus on fragments between
   *     positions `start` and `end`.
   * @param union_non_empty_domains The union of the non-empty domains of
   *    the fragments between `start` and `end`.
   * @return `True` if the fragments between `start` and `end` can be
   *     consolidated based on the above definition.
   */
  bool are_consolidatable(
      const Domain& domain,
      const FragmentInfo& fragment_info,
      size_t start,
      size_t end,
      const NDRange& union_non_empty_domains) const;

  /**
   * Consolidates the input fragments of the input array. This function
   * implements a single consolidation step.
   *
   * @param array_for_reads Array used for reads.
   * @param array_for_writes Array used for writes.
   * @param to_consolidate The fragments to consolidate in this consolidation
   *     step.
   * @param union_non_empty_domains The union of the non-empty domains of
   *     the fragments in `to_consolidate`. Applicable only when those
   *     fragments are *not* all sparse.
   * @param new_fragment_uri The URI of the fragment created after
   *     consolidating the `to_consolidate` fragments.
   * @return Status
   */
  Status consolidate_internal(
      shared_ptr<Array> array_for_reads,
      shared_ptr<Array> array_for_writes,
      const std::vector<TimestampedURI>& to_consolidate,
      const NDRange& union_non_empty_domains,
      URI* new_fragment_uri);

  /**
   * Copies the array by reading from the fragments to be consolidated
   * with `query_r` and writing to the new fragment with `query_w`.
   * It also appropriately sets the query buffers.
   *
   * @param query_r The read query.
   * @param query_w The write query.
   * @return Status
   */
  Status copy_array(
      Query* query_r,
      Query* query_w,
      std::vector<ByteVec>* buffers,
      std::vector<uint64_t>* buffer_sizes);

  /**
   * Creates the buffers that will be used upon reading the input fragments and
   * writing into the new fragment. It also retrieves the number of buffers
   * created.
   *
   * @param stats The stats.
   * @param config The consolidation config.
   * @param array_schema The array schema.
   * @param avg_cell_sizes The average cell sizes.
   * @return Buffers, Buffer sizes.
   */
  static tuple<std::vector<ByteVec>, std::vector<uint64_t>> create_buffers(
      stats::Stats* stats,
      const ConsolidationConfig& config,
      const ArraySchema& array_schema,
      std::unordered_map<std::string, uint64_t>& avg_cell_sizes);

  /**
   * Creates the queries needed for consolidation. It also retrieves
   * the number of fragments to be consolidated and the URI of the
   * new fragment to be created.
   *
   * @param array_for_reads The opened array for reading the fragments
   *     to be consolidated.
   * @param array_for_writes The opened array for writing the
   *     consolidated fragments.
   * @param subarray The subarray to read from (the fragments to consolidate)
   *     and write to (the new fragment).
   * @param query_r This query reads from the fragments to be consolidated.
   * @param query_w This query writes to the new consolidated fragment.
   * @param new_fragment_uri The URI of the new fragment to be created.
   * @return Status
   */
  Status create_queries(
      shared_ptr<Array> array_for_reads,
      shared_ptr<Array> array_for_writes,
      const NDRange& subarray,
      Query** query_r,
      Query** query_w,
      URI* new_fragment_uri);

  /**
   * Based on the input fragment info, this algorithm decides the (sorted) list
   * of fragments to be consolidated in the next consolidation step.
   *
   * @param array_schema The array schema.
   * @param fragment_info Information about all the fragments.
   * @param to_consolidate The fragments to consolidate in the next step.
   * @param union_non_empty_domains The function will return here the
   *     union of the non-empty domains of the fragments in `to_consolidate`.
   * @return Status
   */
  Status compute_next_to_consolidate(
      const ArraySchema& array_schema,
      const FragmentInfo& fragment_info,
      std::vector<TimestampedURI>* to_consolidate,
      NDRange* union_non_empty_domains) const;

  /** Checks and sets the input configuration parameters. */
  Status set_config(const Config& config);

  /**
   * Sets the buffers to the query, using all the attributes in the
   * query schema. There is a 1-1 correspondence between the input `buffers`
   * and the attributes in the schema, considering also the coordinates
   * if the array is sparse in the end.
   *
   * @param query The query to set the buffers to.
   * @param The buffers to set.
   * @param buffer_sizes The buffer sizes.
   */
  Status set_query_buffers(
      Query* query,
      std::vector<ByteVec>* buffers,
      std::vector<uint64_t>* buffer_sizes) const;

  /** Writes the vacuum file that contains the URIs of the consolidated
   * fragments. */
  Status write_vacuum_file(
      const format_version_t write_version,
      const URI& array_uri,
      const URI& vac_uri,
      const std::vector<TimestampedURI>& to_consolidate) const;

  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** Consolidation configuration parameters. */
  ConsolidationConfig config_;

  /**
   * Resource manager
   */
  tdb::RM& rm_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_FRAGMENT_CONSOLIDATOR_H
