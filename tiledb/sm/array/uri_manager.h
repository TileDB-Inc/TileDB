/**
 * @file   uri_manager.h
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
 This file defines class URIManager.
 */

#ifndef TILEDB_URI_MANAGER_H
#define TILEDB_URI_MANAGER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <unordered_map>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * Manages the various URIs inside an array directory, considering
 * various versions of the on-disk TileDB format.
 */
class URIManager {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param storage_manager A pointer to a StorageManager object.
   * @param array_uri The URI of the array this URI manager will be
   *     associated with.
   * @param timestamp_start Only array fragments, metadata, etc. that
   *     were created within timestamp range
   *    [`timestamp_start`, `timestamp_end`] will be considered when
   *     fetching URIs.
   * @param timestamp_end Only array fragments, metadata, etc. that
   *     were created within timestamp range
   *    [`timestamp_start`, `timestamp_end`] will be considered when
   *     fetching URIs.
   * @param only_schema If `true`, only the array schema URIs
   *     will be loaded.
   */
  URIManager(
      StorageManager* storage_manager,
      const URI& array_uri,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      bool only_schemas = false);

  /** Destructor. */
  ~URIManager() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the array URI. */
  const URI& array_uri() const;

  /** Returns the URIs of the array schema files. */
  const std::vector<URI>& array_schema_uris() const;

  /** Returns the latest array schema URI. */
  const URI& latest_array_schema_uri() const;

  /** Returns the URIs of the array metadata files to vacuum. */
  const std::vector<URI>& array_meta_uris_to_vacuum() const;

  /** Returns the URIs of the array metadata vacuum files to vacuum. */
  const std::vector<URI>& array_meta_vac_uris_to_vacuum() const;

  /** Returns the filtered array metadata URIs. */
  const std::vector<TimestampedURI>& array_meta_uris() const;

  /** Loads the URIs from the various array subdirectories. */
  Status load();

  /** Returns the fragment URIs to vacuum. */
  const std::vector<URI>& fragment_uris_to_vacuum() const;

  /** Returns the vacuum file URIs to vacuum for fragments. */
  const std::vector<URI>& fragment_vac_uris_to_vacuum() const;

  /** Returns the filtered fragment URIs. */
  const std::vector<TimestampedURI>& fragment_uris() const;

  /** Returns the URIs of the consolidated fragment metadata files. */
  const std::vector<URI>& fragment_meta_uris() const;

  /** Returns the latest consolidated fragment metadata URI. */
  const URI& latest_fragment_meta_uri() const;

  /** Returns `true` if `load` has been run. */
  bool loaded() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array URI. */
  URI array_uri_;

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The URIs of all the array schema files. */
  std::vector<URI> array_schema_uris_;

  /** The latest array schema URI. */
  URI latest_array_schema_uri_;

  /** The URIs of the array metadata files to vacuum. */
  std::vector<URI> array_meta_uris_to_vacuum_;

  /** The URIs of the array metadata vac files to vacuum. */
  std::vector<URI> array_meta_vac_uris_to_vacuum_;

  /**
   * The filtered array metadata URIs, after removing the ones that
   * need to be vacuum and those that do not fall inside range
   * [`timestamp_start_`, `timestamp_end_`].
   */
  std::vector<TimestampedURI> array_meta_uris_;

  /** The fragment URIs to vacuum. */
  std::vector<URI> fragment_uris_to_vacuum_;

  /** The URIs of the fragment vac files to vacuum. */
  std::vector<URI> fragment_vac_uris_to_vacuum_;

  /**
   * The filtered fragment URIs, after removing the ones that
   * need to be vacuum and those that do not fall inside range
   * [`timestamp_start_`, `timestamp_end_`].
   */
  std::vector<TimestampedURI> fragment_uris_;

  /** The URIs of the consolidated fragment metadata files. */
  std::vector<URI> fragment_meta_uris_;

  /**
   * The latest fragment metadata URI.
   */
  URI latest_fragment_meta_uri_;

  /**
   * Only array fragments, metadata, etc. that
   * were created within timestamp range
   *    [`timestamp_start`, `timestamp_end`] will be considered when
   *     fetching URIs.
   */
  uint64_t timestamp_start_;

  /**
   * Only array fragments, metadata, etc. that
   * were created within timestamp range
   *    [`timestamp_start`, `timestamp_end`] will be considered when
   *     fetching URIs.
   */
  uint64_t timestamp_end_;

  /** If `ture`, only the array schemas will be loaded. */
  bool only_schemas_;

  /** True if `load` has been run. */
  bool loaded_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Loads the fragment URIs. */
  Status load_fragment_uris();

  /** Loads the array metadata URIs. */
  Status load_array_meta_uris();

  /** Loads the array schema URIs. */
  Status load_array_schema_uris();

  /**
   * Computes the fragment URIs from the input array directory URIs.
   */
  std::tuple<Status, std::optional<std::vector<URI>>> compute_fragment_uris(
      const std::vector<URI>& array_dir_uris);

  /**
   * Computes the fragment URIs and vacuum URIs to vacuum.
   *
   * @param uris The URIs to calculate the URIs to vacuum from.
   * @return Status, a vector of the URIs to vacuum, a vector of
   *     the vac file URIs to vacuum.
   */
  std::tuple<
      Status,
      std::optional<std::vector<URI>>,
      std::optional<std::vector<URI>>>
  compute_uris_to_vacuum(const std::vector<URI>& uris);

  /**
   * Computes the filtered URIs based on the input, which fall
   * in the timestamp range [`timestamp_start_`, `timestamp_end_`].
   *
   * @param uris The URIs to filter.
   * @param to_ignore The URIs to ignore (because they are vacuumed).
   * @return Status, vector of filtered timestamped URIs.
   */
  std::tuple<Status, std::optional<std::vector<TimestampedURI>>>
  compute_filtered_uris(
      const std::vector<URI>& uris, const std::vector<URI>& to_ignore);

  /**
   * Computes and sets the final vector of array schema URIs, and the
   * latest array schema URI, given `array_schema_dir_uris`.
   *
   * @return Status.
   */
  Status compute_array_schema_uris(
      const std::vector<URI>& array_schema_dir_uris);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_URI_MANAGER_H
