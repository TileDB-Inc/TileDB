/**
 * @file   consolidator.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "tiledb/sm/array/array.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/storage_manager/open_array.h"

#include <vector>

namespace tiledb {
namespace sm {

class ArraySchema;
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

    /** Constructor. */
    ConsolidationConfig() {
      amplification_ = constants::consolidation_amplification;
      steps_ = constants::consolidation_steps;
      buffer_size_ = constants::consolidation_buffer_size;
      min_frags_ = constants::consolidation_step_min_frags;
      max_frags_ = constants::consolidation_step_max_frags;
      size_ratio_ = constants::consolidation_step_size_ratio;
    }
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

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Consolidates the fragments of the input array.
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

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** Connsolidation configuration parameters. */
  ConsolidationConfig config_;

  /** The storage manager. */
  StorageManager* storage_manager_;

  /* ********************************* */
  /*          PRIVATE METHODS           */
  /* ********************************* */

  /**
   * Returns `true` if all fragments between `start` and `end`
   * (inclusive) in `fragments` are sparse.
   */
  bool all_sparse(
      const std::vector<FragmentInfo>& fragments,
      size_t start,
      size_t end) const;

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
   * @tparam T The domain type.
   * @param fragments The input fragments.
   * @param start The function will focus on fragments between
   *     positions `start` and `end`.
   * @param end The function will focus on fragments between
   *     positions `start` and `end`.
   * @param union_non_empty_domains The union of the non-empty domains of
   *    the fragments between `start` and `end`.
   * @param dim_num The number of domain dimensions.
   * @return `True` if the fragments between `start` and `end` can be
   *     consolidated based on the above definition.
   */
  template <class T>
  bool are_consolidatable(
      const std::vector<FragmentInfo>& fragments,
      size_t start,
      size_t end,
      const T* union_non_empty_domains,
      unsigned dim_num) const;

  /**
   * Consolidates the fragments of the input array.
   *
   * @param array_schema The schema of the array to consolidate.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   */
  Status consolidate(
      const ArraySchema* array_schema,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Consolidates the fragments of the input array.
   *
   * @param T The domain type.
   * @param array_schema The schema of the array to consolidate.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   */
  template <class T>
  Status consolidate(
      const ArraySchema* array_schema,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Consolidates the input fragments of the input array. This function
   * implements a single consolidation step.
   *
   * @tparam T The domain type.
   * @param array_uri URI of array to consolidate.
   * @param to_consolidate The fragments to consolidate in this consolidation
   *     step.
   * @param union_non_empty_domains The union of the non-empty domains of
   *     the fragments in `to_consolidate`. Applicable only when those
   *     fragments are *not* all sparse.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @param new_fragment_uri The URI of the fragment created after
   *     consolidating the `to_consolidate` fragments.
   * @return Status
   */
  template <class T>
  Status consolidate(
      const URI& array_uri,
      const std::vector<FragmentInfo>& to_consolidate,
      T* union_non_empty_domains,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length,
      URI* new_fragment_uri);

  /**
   * Copies the array by reading from the fragments to be consolidated
   * (with `query_r`) and writing to the new fragment (with `query_w`).
   *
   * @param query_r The read query.
   * @param query_w The write query.
   * @return Status
   */
  Status copy_array(Query* query_r, Query* query_w);

  /** Cleans up the inputs. */
  void clean_up(
      unsigned buffer_num,
      void** buffers,
      uint64_t* buffer_sizes,
      Query* query_r,
      Query* query_w) const;

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
   * @param buffer_num The number of buffers to be retrieved.
   * @return Status
   */
  Status create_buffers(
      const ArraySchema* array_schema,
      bool sparse_mode,
      void*** buffers,
      uint64_t** buffer_sizes,
      unsigned int* buffer_num);

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
   * @param buffers The buffers to be passed in the queries.
   * @param buffer_sizes The corresponding buffer sizes.
   * @param query_r This query reads from the fragments to be consolidated.
   * @param query_w This query writes to the new consolidated fragment.
   * @param new_fragment_uri The URI of the new fragment to be created.
   * @return Status
   */
  Status create_queries(
      Array* array_for_reads,
      Array* array_for_writes,
      bool sparse_mode,
      void* subarray,
      void** buffers,
      uint64_t* buffer_sizes,
      Query** query_r,
      Query** query_w,
      URI* new_fragment_uri);

  /**
   * Deletes the fragment metadata files of the input fragments.
   * This renders the fragments "invisible".
   *
   * @param fragments The URIs of the fragments to be deleted.
   * @return Status
   */
  Status delete_fragment_metadata(const std::vector<URI>& fragments);

  /**
   * Deletes the entire directories of the input fragments.
   *
   * @param fragments The URIs of the fragments to be deleted.
   * @return Status
   */
  Status delete_fragments(const std::vector<URI>& fragments);

  /**
   * This function will delete all fragments that are completely
   * overwritten by more recent fragments, i.e., all fragments
   * (dense or sparse) whose non-empty domain is completely
   * included in the non-empty domain of a later dense fragment.
   * This is applicable only to dense arrays.
   *
   * @tparam T The domain type.
   * @param array_schema The array schema.
   * @param fragments Fragment information that will help in identifying
   *     which fragments to delete. If a fragment gets deleted by the
   *     function, its corresponding fragment info will get evicted
   *     from this vector.
   * @return Status
   */
  template <class T>
  Status delete_overwritten_fragments(
      const ArraySchema* array_schema, std::vector<FragmentInfo>* fragments);

  /**
   * Frees the input buffers.
   *
   * @param buffer_num The number of buffers.
   * @param buffers The buffers to be freed.
   * @param buffer_sizes The corresponding buffer sizes.
   */
  void free_buffers(
      unsigned int buffer_num, void** buffers, uint64_t* buffer_sizes) const;

  /**
   * Based on the input fragment info, this algorithm decides the (sorted) list
   * of fragments to be consolidated in the next consolidation step.
   *
   * @tparam T The domain type.
   * @param array_schema The array schema.
   * @param fragments Information about all the fragments.
   * @param to_consolidate The fragments to consolidate in the next step.
   * @param union_non_empty_domains The function will return here the
   *     union of the non-empty domains of the fragments in `to_consolidate`.
   * @return Status
   */
  template <class T>
  Status compute_next_to_consolidate(
      const ArraySchema* array_schema,
      const std::vector<FragmentInfo>& fragments,
      std::vector<FragmentInfo>* to_consolidate,
      T* union_non_empty_domains) const;

  /**
   * Renames the new fragment URI. The new name has the format
   * `__<thread_id>_<timestamp>_<last_fragment_timestamp>`, where
   * `<thread_id>` is the id of the thread that performs the consolidation,
   * `<timestamp>` is the current timestamp in milliseconds, and
   * `<last_fragment_timestamp>` is the timestamp of the last of the
   * consolidated fragments.
   */
  Status rename_new_fragment_uri(URI* uri) const;

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
      void** buffers,
      uint64_t* buffer_sizes) const;

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
      const std::vector<FragmentInfo>& to_consolidate,
      const FragmentInfo& new_fragment_info,
      std::vector<FragmentInfo>* fragment_info) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FRAGMENT_H
