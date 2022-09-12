/**
 * @file  fragment_info.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2022 TileDB, Inc.
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
 * This file defines struct FragmentInfo.
 */

#ifndef TILEDB_FRAGMENT_INFO_H
#define TILEDB_FRAGMENT_INFO_H

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/fragment/single_fragment_info.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Stores basic information about fragments in an array. */
class FragmentInfo {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  FragmentInfo();

  /** Constructor. */
  FragmentInfo(const URI& array_uri, StorageManager* storage_manager);

  /** Destructor. */
  ~FragmentInfo();

  /** Copy constructor. */
  FragmentInfo(const FragmentInfo& fragment_info);

  /** Move constructor. */
  FragmentInfo(FragmentInfo&& fragment_info);

  /** Copy-assign operator. */
  FragmentInfo& operator=(const FragmentInfo& fragment_info);

  /** Move-assign operator. */
  FragmentInfo& operator=(FragmentInfo&& fragment_info);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Sets a config to the fragment info. Useful for retrieving timestamps
   * and encryption key.
   */
  Status set_config(const Config& config);

  /** Expand the non empty domain before start with a new range */
  void expand_anterior_ndrange(const Domain& domain, const NDRange& range);

  /** Dumps the fragment info in ASCII format in the selected output. */
  void dump(FILE* out) const;

  /** Retrieves whether the fragment with the given index is dense. */
  Status get_dense(uint32_t fid, int32_t* dense) const;

  /** Retrieves whether the fragment with the given index is sparse. */
  Status get_sparse(uint32_t fid, int32_t* sparse) const;

  /** Returns the number of fragments described in this object. */
  uint32_t fragment_num() const;

  /** Retrieves the number of cells in the fragment with the given index. */
  Status get_cell_num(uint32_t fid, uint64_t* cell_num) const;

  /** Retrieves the number of cells in all currently loaded fragments. */
  Status get_total_cell_num(uint64_t* cell_num) const;

  /** Retrieves the name of the fragment with the given index. */
  Status get_fragment_name(uint32_t fid, const char** name) const;

  /** Retrieves the size of the fragment with the given index. */
  Status get_fragment_size(uint32_t fid, uint64_t* size) const;

  /** Retrieves the URI of the fragment with the given index. */
  Status get_fragment_uri(uint32_t fid, const char** uri) const;

  /** Retrieves the URI of the fragment to vacuum with the given index. */
  Status get_to_vacuum_uri(uint32_t fid, const char** uri) const;

  /** Retrieves the timestamp range of the fragment with the given index. */
  Status get_timestamp_range(
      uint32_t fid, uint64_t* start, uint64_t* end) const;

  /**
   * Retrieves the non-empty domain of the fragment with the given index
   * on the given dimension index.
   */
  Status get_non_empty_domain(uint32_t fid, uint32_t did, void* domain) const;

  /**
   * Retrieves the non-empty domain of the fragment with the given index
   * on the given dimension name.
   */
  Status get_non_empty_domain(
      uint32_t fid, const char* dim_name, void* domain) const;

  /**
   * Retrieves the sizes of the start and end values of the non-empty domain
   * of the fragment with the given index on the given dimension index.
   * Applicable only to var-sized dimensions.
   */
  Status get_non_empty_domain_var_size(
      uint32_t fid,
      uint32_t did,
      uint64_t* start_size,
      uint64_t* end_size) const;

  /**
   * Retrieves the sizes of the start and end values of the non-empty domain
   * of the fragment with the given index on the given dimension name.
   * Applicable only to var-sized dimensions.
   */
  Status get_non_empty_domain_var_size(
      uint32_t fid,
      const char* dim_name,
      uint64_t* start_size,
      uint64_t* end_size) const;

  /**
   * Retrieves the non-empty domain of the fragment with the given index
   * on the given dimension index. Applicable to var-sized dimensions.
   */
  Status get_non_empty_domain_var(
      uint32_t fid, uint32_t did, void* start, void* end) const;

  /**
   * Retrieves the non-empty domain of the fragment with the given index
   * on the given dimension name. Applicable to var-sized dimensions.
   */
  Status get_non_empty_domain_var(
      uint32_t fid, const char* dim_name, void* start, void* end) const;

  /** Retrieves the number of MBRs in the fragment with the given index. */
  Status get_mbr_num(uint32_t fid, uint64_t* mbr_num);

  /**
   * Retrieves the MBR of the fragment with the given index on the given
   * dimension index.
   */
  Status get_mbr(uint32_t fid, uint32_t mid, uint32_t did, void* mbr);

  /**
   * Retrieves the MBR of the fragment with the given index on the given
   * dimension name.
   */
  Status get_mbr(uint32_t fid, uint32_t mid, const char* dim_name, void* mbr);

  /**
   * Retrieves the sizes of the start and end values of the MBR of the fragment
   * with the given index on the given dimension index. Applicable only to
   * var-sized dimensions.
   */
  Status get_mbr_var_size(
      uint32_t fid,
      uint32_t mid,
      uint32_t did,
      uint64_t* start_size,
      uint64_t* end_size);

  /**
   * Retrieves the sizes of the start and end values of the MBR of the fragment
   * with the given index on the given dimension name. Applicable only to
   * var-sized dimensions.
   */
  Status get_mbr_var_size(
      uint32_t fid,
      uint32_t mid,
      const char* dim_name,
      uint64_t* start_size,
      uint64_t* end_size);

  /**
   * Retrieves the MBR of the fragment with the given index on the given
   * dimension index. Applicable to var-sized dimensions.
   */
  Status get_mbr_var(
      uint32_t fid, uint32_t mid, uint32_t did, void* start, void* end);

  /**
   * Retrieves the MBR of the fragment with the given index on the given
   * dimension name. Applicable to var-sized dimensions.
   */
  Status get_mbr_var(
      uint32_t fid, uint32_t mid, const char* dim_name, void* start, void* end);

  /** Retrieves the version of the fragment with the given index. */
  Status get_version(uint32_t fid, uint32_t* version) const;

  /** Retrieves the array schema of the fragment with the given index. */
  tuple<Status, optional<shared_ptr<ArraySchema>>> get_array_schema(
      uint32_t fid);

  /** Retrieves the array schema name of the fragment with the given index. */
  Status get_array_schema_name(uint32_t fid, const char** schema_name);

  /**
   * Checks if the fragment with the given index has consolidated metadata.
   */
  Status has_consolidated_metadata(uint32_t fid, int32_t* has) const;

  /**
   * Loads the fragment info from an array.
   *
   * @return Status
   */
  Status load();

  /**
   * Loads the fragment info from an array using the input key.
   *
   * @param encryption_type The encryption type.
   * @param encryption_key The encryption key.
   * @param key_length The length of `encryption_key`.
   * @return Status
   */
  Status load(
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Loads the fragment info from an array using the input key
   * and timestamps.
   *
   * @param array_dir The array directory to load the fragments.
   * @param timestamp_start This function will load fragments with
   *      whose timestamps are within [timestamp_start, timestamp_end].
   * @param timestamp_end This function will load fragments with
   *      whose timestamps are within [timestamp_start, timestamp_end].
   * @param encryption_type The encryption type.
   * @param encryption_key The encryption key.
   * @param key_length The length of `encryption_key`.
   * @return Status
   */
  Status load(
      const ArrayDirectory& array_dir,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * It replaces a sequence of SingleFragmentInfo elements in
   * `single_fragment_info_vec_` which are determined by `to_replace`.
   * It then loads a SingleFragmentInfo object for the `new_fragment_uri`
   * fragment, and adds it in `single_fragment_info_vec_` at the postion
   * of the first element of the corresponding `to_replace` object.
   *
   * @param new_fragment_uri The new fragment to be loaded as a
   *     SingleFragmentInfo object.
   * @param to_replace The SingleFragmentInfo elements to be replaced
   *     in `single_fragment_info_vec_` by the new SingleFragmentInfo object.
   * @return Status
   */
  Status load_and_replace(
      const URI& new_fragment_uri,
      const std::vector<TimestampedURI>& to_replace);

  /** Returns the vector with the info about individual fragments. */
  const std::vector<SingleFragmentInfo>& single_fragment_info_vec() const;

  /** Returns the non empty domain of the fragments before start time. */
  const NDRange& anterior_ndrange() const;

  /** Returns the number of fragments to vacuum. */
  uint32_t to_vacuum_num() const;

  /** Returns the number of fragments with unconsolidated metadata. */
  uint32_t unconsolidated_metadata_num() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The URI of the array the fragments belong to. */
  URI array_uri_;

  /** The config. */
  Config config_;

  /** The encryption key used if the array is encrypted. */
  EncryptionKey enc_key_;

  /**
   * All the array schemas relevant to the loaded fragment metadata
   * keyed by their file name.
   */
  std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas_all_;

  /** Information about fragments in the array. */
  std::vector<SingleFragmentInfo> single_fragment_info_vec_;

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The URIs of the fragments to vacuum. */
  std::vector<URI> to_vacuum_;

  /** The number of fragments with unconsolidated metadata. */
  uint32_t unconsolidated_metadata_num_;

  /** Non empty domain before the start time specified. */
  NDRange anterior_ndrange_;

  /** Timestamp start used in load. */
  uint64_t timestamp_start_;

  /** Timestamp end used in load. */
  uint64_t timestamp_end_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Sets the encryption key (if present) from config_. */
  Status set_enc_key_from_config();

  /**
   * Sets the timestamp range to [0, now].
   */
  Status set_default_timestamp_range();

  /**
   * Loads the fragment info from an array using the array directory.
   */
  Status load(const ArrayDirectory& array_directory);

  /**
   * Loads the fragment metadata of the input URI and returns a
   * SingleFragmentInfo object that wraps it.
   *
   * @param fragment_uri The URI of the fragment whose metadata
   *     will be loaded into the returned `SingleFragmentInfo` object.
   * @return Status, a `SingleFragmentInfo` object
   */
  tuple<Status, optional<SingleFragmentInfo>> load(
      const URI& fragment_uri) const;

  /**
   * Replaces the SingleFragmentInfo objects determined by `to_replace`
   * with `new_single_fragment_info`.
   */
  Status replace(
      const SingleFragmentInfo& new_single_fragment_info,
      const std::vector<TimestampedURI>& to_replace);

  /** Returns a copy of this object. */
  FragmentInfo clone() const;

  /**
   * Swaps the contents (all field values) of this object with the
   * given object.
   */
  void swap(FragmentInfo& fragment_info);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FRAGMENT_INFO_H
