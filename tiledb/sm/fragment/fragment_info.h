/**
 * @file  fragment_info.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB, Inc.
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

#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/fragment/single_fragment_info.h"
#include "tiledb/sm/misc/uri.h"
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

  /** Appends the info about a single fragment at the end of `fragments_`. */
  void append(const SingleFragmentInfo& fragment);

  /** Expand the non empty domain before start with a new range */
  void expand_anterior_ndrange(const Domain* domain, const NDRange& range);

  /** Clears the object. */
  void clear();

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
  Status get_mbr_num(uint32_t fid, uint64_t* mbr_num) const;

  /**
   * Retrieves the MBR of the fragment with the given index on the given
   * dimension index.
   */
  Status get_mbr(uint32_t fid, uint32_t mid, uint32_t did, void* mbr) const;

  /**
   * Retrieves the MBR of the fragment with the given index on the given
   * dimension name.
   */
  Status get_mbr(
      uint32_t fid, uint32_t mid, const char* dim_name, void* mbr) const;

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
      uint64_t* end_size) const;

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
      uint64_t* end_size) const;

  /**
   * Retrieves the MBR of the fragment with the given index on the given
   * dimension index. Applicable to var-sized dimensions.
   */
  Status get_mbr_var(
      uint32_t fid, uint32_t mid, uint32_t did, void* start, void* end) const;

  /**
   * Retrieves the MBR of the fragment with the given index on the given
   * dimension name. Applicable to var-sized dimensions.
   */
  Status get_mbr_var(
      uint32_t fid,
      uint32_t mid,
      const char* dim_name,
      void* start,
      void* end) const;

  /** Retrieves the version of the fragment with the given index. */
  Status get_version(uint32_t fid, uint32_t* version) const;

  /**
   * Checks if the fragment with the given index has consolidated metadata.
   */
  Status has_consolidated_metadata(uint32_t fid, int32_t* has) const;

  /**
   * Loads the fragment info from an array. The encryption key is used
   * only if the array is encrypted.
   */
  Status load(
      const Config config,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /** Set the dimension names and types to the object. */
  void set_dim_info(
      const std::vector<std::string>& dim_names,
      const std::vector<Datatype>& dim_types_);

  /** Sets the URIs of the fragments to vacuum. */
  void set_to_vacuum(const std::vector<URI>& to_vacuum);

  /** Returns the vector with the info about individual fragments. */
  const std::vector<SingleFragmentInfo>& fragments() const;

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

  /** Information about fragments in the array. */
  std::vector<SingleFragmentInfo> fragments_;

  /** The dimension names of the array. */
  std::vector<std::string> dim_names_;

  /** The dimension types of the array. */
  std::vector<Datatype> dim_types_;

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The pointer to the opened array that the fragments belong to. */
  Array* array_;

  /** The URIs of the fragments to vacuum. */
  std::vector<URI> to_vacuum_;

  /** The number of fragments with unconsolidated metadata. */
  uint32_t unconsolidated_metadata_num_;

  /** Non empty domain before the start time specified */
  NDRange anterior_ndrange_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

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
