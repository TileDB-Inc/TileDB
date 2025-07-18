/**
 * @file   group_directory.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 This file defines class GroupDirectory.
 */

#ifndef TILEDB_GROUP_DIRECTORY_H
#define TILEDB_GROUP_DIRECTORY_H

#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/filesystem/vfs.h"

#include <unordered_map>

using namespace tiledb::common;

namespace tiledb::sm {

class GroupNotFoundException : public StatusException {
 public:
  explicit GroupNotFoundException(const std::string& message)
      : StatusException("Group", message) {
  }
};

/** Mode for the GroupDirectory class. */
enum class GroupDirectoryMode {
  READ,         // Read mode.
  CONSOLIDATE,  // Consolidation mode.
  VACUUM,       // Vacuum mode.
};

/**
 * Manages the various URIs inside an group directory, considering
 * various versions of the on-disk TileDB format.
 */
class GroupDirectory {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  GroupDirectory() = delete;

  /**
   * Constructor.
   *
   * @param vfs A reference to a VFS object for all IO.
   * @param tp A thread pool used for parallelism.
   * @param uri The URI of the group directory.
   * @param timestamp_start Only group fragments, metadata, etc. that
   *     were created within timestamp range
   *    [`timestamp_start`, `timestamp_end`] will be considered when
   *     fetching URIs.
   * @param timestamp_end Only group fragments, metadata, etc. that
   *     were created within timestamp range
   *    [`timestamp_start`, `timestamp_end`] will be considered when
   *     fetching URIs.
   * @param mode The mode to load the group directory in.
   */
  GroupDirectory(
      VFS& vfs,
      ThreadPool& tp,
      const URI& uri,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      GroupDirectoryMode mode = GroupDirectoryMode::READ);

  /** Destructor. */
  ~GroupDirectory() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the group URI. */
  const URI& uri() const;

  /** Returns the URIs of all group files. */
  const std::vector<URI>& group_file_uris() const;

  /** Returns the URIs of the group metadata files to vacuum. */
  const std::vector<URI>& group_meta_uris_to_vacuum() const;

  /** Returns the URIs of the group metadata vacuum files to vacuum. */
  const std::vector<URI>& group_meta_vac_uris_to_vacuum() const;

  /** Returns the filtered group metadata URIs. */
  const std::vector<TimestampedURI>& group_meta_uris() const;

  /** Returns the URIs of the group metadata files to vacuum. */
  const std::vector<URI>& group_detail_uris_to_vacuum() const;

  /** Returns the URIs of the group metadata vacuum files to vacuum. */
  const std::vector<URI>& group_detail_vac_uris_to_vacuum() const;

  /** Returns the filtered group metadata URIs. */
  const std::vector<TimestampedURI>& group_detail_uris() const;

  /** Return the latest group details URI. */
  const URI& latest_group_details_uri() const;

  /** Returns `true` if `load` has been run. */
  bool loaded() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The group URI. */
  URI uri_;

  /** The storage manager. */
  VFS& vfs_;

  /** A thread pool used for parallelism. */
  ThreadPool& tp_;

  /** The URIs of all group files. */
  std::vector<URI> group_file_uris_;

  /** Latest group details URI. */
  URI latest_group_details_uri_;

  /** The URIs of the group metadata files to vacuum. */
  std::vector<URI> group_meta_uris_to_vacuum_;

  /** The URIs of the group metadata vac files to vacuum. */
  std::vector<URI> group_meta_vac_uris_to_vacuum_;

  /**
   * The filtered group metadata URIs, after removing the ones that
   * need to be vacuum and those that do not fall inside range
   * [`timestamp_start_`, `timestamp_end_`].
   */
  std::vector<TimestampedURI> group_meta_uris_;

  /** The URIs of the group details files to vacuum. */
  std::vector<URI> group_detail_uris_to_vacuum_;

  /** The URIs of the group details vac files to vacuum. */
  std::vector<URI> group_detail_vac_uris_to_vacuum_;

  /**
   * The filtered group file URIs, after removing the ones that
   * need to be vacuum and those that do not fall inside range
   * [`timestamp_start_`, `timestamp_end_`].
   */
  std::vector<TimestampedURI> group_detail_uris_;

  /**
   * Only group fragments, metadata, etc. that
   * were created within timestamp range
   *    [`timestamp_start`, `timestamp_end`] will be considered when
   *     fetching URIs.
   */
  uint64_t timestamp_start_;

  /**
   * Only group fragments, metadata, etc. that
   * were created within timestamp range
   *    [`timestamp_start`, `timestamp_end`] will be considered when
   *     fetching URIs.
   */
  uint64_t timestamp_end_;

  /** True if `load` has been run. */
  bool loaded_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Loads the URIs from the various group subdirectories. */
  Status load();

  /** Loads the group metadata URIs. */
  Status load_group_meta_uris();

  /** Loads the group details URIs. */
  Status load_group_detail_uris();

  /**
   * Computes the fragment URIs and vacuum URIs to vacuum.
   *
   * @param uris The URIs to calculate the URIs to vacuum from.
   * @return Status, a vector of the URIs to vacuum, a vector of
   *     the vac file URIs to vacuum.
   */
  tuple<Status, optional<std::vector<URI>>, optional<std::vector<URI>>>
  compute_uris_to_vacuum(const std::vector<URI>& uris) const;

  /**
   * Computes the filtered URIs based on the input, which fall
   * in the timestamp range [`timestamp_start_`, `timestamp_end_`].
   *
   * @param uris The URIs to filter.
   * @param to_ignore The URIs to ignore (because they are vacuumed).
   * @return Status, vector of filtered timestamped URIs.
   */
  tuple<Status, optional<std::vector<TimestampedURI>>> compute_filtered_uris(
      const std::vector<URI>& uris, const std::vector<URI>& to_ignore) const;

  /** Returns true if the input URI is a vacuum file. */
  bool is_vacuum_file(const URI& uri) const;
};

}  // namespace tiledb::sm

#endif  // TILEDB_GROUP_DIRECTORY_H
