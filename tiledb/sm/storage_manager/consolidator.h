/**
 * @file   consolidator.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/filesystem/filelock.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/storage_manager/open_array.h"

#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class ArraySchema;
class Config;
class Query;
class StorageManager;
class URI;

/** Handles array consolidation. */
class Consolidator {
 public:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /** Consolidation configuration parameters. */
  struct ConsolidationConfig {
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
    /**
     * The consolidation mode. It can be one of:
     *     - "fragments": only the fragments will be consoidated
     *     - "fragment_meta": only the fragment metadata will be consolidated
     *     - "array_meta": only the array metadata will be consolidated
     */
    std::string mode_;
    /** Start time for consolidation. */
    uint64_t timestamp_start_;
    /** End time for consolidation. */
    uint64_t timestamp_end_;
    /** Are the refactored readers in use or not */
    bool use_refactored_readers_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param storage_manager The storage manager.
   */
  explicit Consolidator(StorageManager* storage_manager);

  /** Destructor. */
  ~Consolidator();

  DISABLE_COPY_AND_COPY_ASSIGN(Consolidator);
  DISABLE_MOVE_AND_MOVE_ASSIGN(Consolidator);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Consolidates the fragments, fragment metadata or array metadata of
   * the input array, based on the consolidation mode set in the configuration.
   *
   * @param array_name URI of array to consolidate.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @param config Configuration parameters for the consolidation
   *     (`nullptr` means default).
   * @return Status
   */
  Status consolidate(
      const char* array_name,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length,
      const Config* config);

  /**
   * Consolidates only the array metadata of the input array.
   *
   * @param array_name URI of array whose metadata to consolidate.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   */
  Status consolidate_array_meta(
      const char* array_name,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** Connsolidation configuration parameters. */
  ConsolidationConfig config_;

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The class stats. */
  stats::Stats* stats_;

  /* ********************************* */
  /*          PRIVATE METHODS           */
  /* ********************************* */

  /**
   * Consolidates only the fragments of the input array.
   *
   * @param array_name URI of array to consolidate.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   */
  Status consolidate_fragments(
      const char* array_name,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Returns `true` if all fragments between `start` and `end`
   * (inclusive) in `fragments` are sparse.
   */
  bool all_sparse(
      const FragmentInfo& fragment_info, size_t start, size_t end) const;

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
      const Domain* domain,
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
   * @param all_sparse If all the fragments to consolidate are sparse.
   * @param union_non_empty_domains The union of the non-empty domains of
   *     the fragments in `to_consolidate`. Applicable only when those
   *     fragments are *not* all sparse.
   * @param new_fragment_uri The URI of the fragment created after
   *     consolidating the `to_consolidate` fragments.
   * @return Status
   */
  Status consolidate(
      Array& array_for_reads,
      Array& array_for_writes,
      const std::vector<TimestampedURI>& to_consolidate,
      bool all_sparse,
      const NDRange& union_non_empty_domains,
      URI* new_fragment_uri);

  /**
   * Consolidates the fragment metadata of the input array.
   *
   * This function will write a file with format `__t1_t2_v`,
   * where `t1` (`t2`) is the timestamp of the first (last)
   * fragment whose footers it consolidates, and `v` is the
   * format version.
   *
   * The file format is as follows:
   * <number of fragments whose footers are consolidated in the file>
   * <framgment #1 name size> <fragment #1 name> <fragment #1 footer offset>
   * <framgment #2 name size> <fragment #2 name> <fragment #2 footer offset>
   * ...
   * <framgment #N name size> <fragment #N name> <fragment #N footer offset>
   * <serialized footer for fragment #1>
   * <serialized footer for fragment #2>
   * ...
   * <serialized footer for fragment #N>
   *
   * @param array_uri The array URI.
   * @param enc_key If the array is encrypted, the private encryption
   *    key.
   * @return Status
   */
  Status consolidate_fragment_meta(
      const URI& array_uri,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

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
      std::vector<uint64_t>* buffer_sizes,
      bool sparse_mode);

  /**
   * Creates the buffers that will be used upon reading the input fragments and
   * writing into the new fragment. It also retrieves the number of buffers
   * created.
   *
   * @param array_schema The array schema.
   * @param sparse_mode This indicates whether a dense array must be opened
   *     in special sparse mode. This is ignored for sparse arrays.
   * @param buffers The buffers to be created.
   * @param buffer_sizes The corresponding buffer sizes.
   * @return Status
   */
  Status create_buffers(
      const ArraySchema* array_schema,
      bool sparse_mode,
      std::vector<ByteVec>* buffers,
      std::vector<uint64_t>* buffer_sizes);

  /**
   * Creates the queries needed for consolidation. It also retrieves
   * the number of fragments to be consolidated and the URI of the
   * new fragment to be created.
   *
   * @param array_for_reads The opened array for reading the fragments
   *     to be consolidated.
   * @param array_for_writes The opened array for writing the
   *     consolidated fragments.
   * @param sparse_mode This indicates whether a dense array must be opened
   *     in special sparse mode. This is ignored for sparse arrays.
   * @param subarray The subarray to read from (the fragments to consolidate)
   *     and write to (the new fragment).
   * @param query_r This query reads from the fragments to be consolidated.
   * @param query_w This query writes to the new consolidated fragment.
   * @param new_fragment_uri The URI of the new fragment to be created.
   * @return Status
   */
  Status create_queries(
      Array* array_for_reads,
      Array* array_for_writes,
      bool sparse_mode,
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
   * @param all_sparse The function will return here if all fragments to
   *     consolidate are all sparse.
   * @param union_non_empty_domains The function will return here the
   *     union of the non-empty domains of the fragments in `to_consolidate`.
   * @return Status
   */
  Status compute_next_to_consolidate(
      const ArraySchema* array_schema,
      const FragmentInfo& fragment_info,
      std::vector<TimestampedURI>* to_consolidate,
      bool* all_sparse,
      NDRange* union_non_empty_domains) const;

  /**
   * The new fragment URI is computed
   * as `__<first_URI_timestamp>_<last_URI_timestamp>_<uuid>`.
   */
  Status compute_new_fragment_uri(
      const URI& first,
      const URI& last,
      uint32_t format_version,
      URI* new_uri) const;

  /** Checks and sets the input configuration parameters. */
  Status set_config(const Config* config);

  /**
   * Sets the buffers to the query, using all the attributes in the
   * query schema. There is a 1-1 correspondence between the input `buffers`
   * and the attributes in the schema, considering also the coordinates
   * if the array is sparse in the end.
   *
   * @param query The query to set the buffers to.
   * @param sparse_mode This indicates whether a dense array must be opened
   *     in special sparse mode. This is ignored for sparse arrays.
   * @param The buffers to set.
   * @param buffer_sizes The buffer sizes.
   */
  Status set_query_buffers(
      Query* query,
      bool sparse_mode,
      std::vector<ByteVec>* buffers,
      std::vector<uint64_t>* buffer_sizes) const;

  /**
   * Updates the `fragment_info` by removing `to_consolidate` and
   * replacing those fragment info objects with `new_fragment_info`.
   *
   * @param to_consolidate Fragment info objects to remove from `fragment_info`.
   * @param new_fragment_info The new object that replaces `to_consolidate`
   *     in `fragment_info`.
   * @param fragment_info The fragment info vector to be updated.
   * @return void
   *
   * @note The algorithm assumes that to_consolidate is a sub vector of
   *     `fragment_info` of size >=0.
   */
  void update_fragment_info(
      const std::vector<TimestampedURI>& to_consolidate,
      const SingleFragmentInfo& new_fragment_info,
      FragmentInfo* fragment_info) const;

  /** Writes the vacuum file that contains the URIs of the consolidated
   * fragments. */
  Status write_vacuum_file(
      const URI& new_uri,
      const std::vector<TimestampedURI>& to_consolidate) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FRAGMENT_H
