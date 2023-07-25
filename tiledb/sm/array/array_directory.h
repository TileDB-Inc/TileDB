/**
 * @file   array_directory.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * This file defines class ArrayDirectory.
 */

#ifndef TILEDB_ARRAY_DIRECTORY_H
#define TILEDB_ARRAY_DIRECTORY_H

#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/storage_format/uri/parse_uri.h"

#include <functional>
#include <unordered_map>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Mode for the ArrayDirectory class. */
enum class ArrayDirectoryMode {
  READ,             // Read mode.
  SCHEMA_ONLY,      // Used when we only load schemas.
  COMMITS,          // Used for opening the array directory for commits
                    // consolidation or vacuuming.
  VACUUM_FRAGMENTS  // Used when opening the array for fragment vacuuming.
};

// Forward declaration
class WhiteboxArrayDirectory;

/**
 * Manages the various URIs inside an array directory, considering
 * various versions of the on-disk TileDB format.
 */
class ArrayDirectory {
  /**
   * Friends with its whitebox testing class.
   */
  friend class WhiteboxArrayDirectory;

 public:
  /**
   * Class to return the URIs that need to be processed after the schema have
   * been created.
   */
  class FilteredFragmentUris {
   public:
    /**
     * Type alias for a set of fragment_uris.
     */
    using FragmentSet = std::set<URI>;

    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */

    /**
     * Constructor.
     */
    FilteredFragmentUris(
        std::vector<URI> fragment_uris_to_vacuum,
        std::vector<URI> commit_uris_to_vacuum,
        std::vector<URI> commit_uris_to_ignore,
        std::vector<URI> fragment_vac_uris_to_vacuum,
        std::vector<TimestampedURI> fragment_uris)
        : fragment_uris_to_vacuum_(fragment_uris_to_vacuum)
        , commit_uris_to_vacuum_(commit_uris_to_vacuum)
        , commit_uris_to_ignore_(commit_uris_to_ignore)
        , fragment_vac_uris_to_vacuum_(fragment_vac_uris_to_vacuum)
        , fragment_uris_(fragment_uris) {
    }

    /** Destructor. */
    ~FilteredFragmentUris() = default;

    /* ********************************* */
    /*                API                */
    /* ********************************* */

    /** Returns the fragment URIs to vacuum. */
    const std::vector<URI>& fragment_uris_to_vacuum() const {
      return fragment_uris_to_vacuum_;
    };

    /** Returns the commit URIs to vacuum for fragment vacuuming. */
    const std::vector<URI>& commit_uris_to_vacuum() const {
      return commit_uris_to_vacuum_;
    }

    /** Returns the commit URIs to ignore for fragment vacuuming. */
    const std::vector<URI>& commit_uris_to_ignore() const {
      return commit_uris_to_ignore_;
    };

    /** Returns the vacuum file URIs to vacuum for fragments. */
    const std::vector<URI>& fragment_vac_uris_to_vacuum() const {
      return fragment_vac_uris_to_vacuum_;
    };

    /** Returns the filtered fragment URIs. */
    const std::vector<TimestampedURI>& fragment_uris() const {
      return fragment_uris_;
    };

    /**
     * Returns the filtered fragment URIs matching the given fragments_list.
     *
     * Note: though fragment_uris_ is of type TimestampedURI,
     * this API will return non-timestamped URIs.
     */
    std::vector<URI> fragment_uris(
        const std::vector<URI>& fragments_list) const {
      std::vector<URI> uris;
      FragmentSet fragment_set = fragment_uris_to_set();
      for (auto fragment : fragments_list) {
        auto iter = fragment_set.find(fragment);
        if (iter == fragment_set.end()) {
          throw std::runtime_error(
              "[ArrayDirectory::fragment_uris] " + fragment.to_string() +
              " is not a fragment of the ArrayDirectory.");
        } else {
          uris.emplace_back(*iter);
        }
      }
      return uris;
    };

   private:
    /* ********************************* */
    /*         PRIVATE ATTRIBUTES        */
    /* ********************************* */

    /** The fragment URIs to vacuum. */
    std::vector<URI> fragment_uris_to_vacuum_;

    /** The URIs of the commits files to vacuum. */
    std::vector<URI> commit_uris_to_vacuum_;

    /** The commit URIs to ignore after fragment vacuum. */
    std::vector<URI> commit_uris_to_ignore_;

    /** The URIs of the fragment vac files to vacuum. */
    std::vector<URI> fragment_vac_uris_to_vacuum_;

    /**
     * The filtered fragment URIs, after removing the ones that
     * need to be vacuum and those that do not fall inside range
     * [`timestamp_start_`, `timestamp_end_`].
     */
    std::vector<TimestampedURI> fragment_uris_;

    /* ********************************* */
    /*             PRIVATE API           */
    /* ********************************* */

    /**
     * Convert the filtered fragment URIs into a FragmentSet to ease parsing.
     *
     * Note: this function is private and may only be called internally.
     */
    inline const FragmentSet fragment_uris_to_set() const {
      FragmentSet fragment_set;
      for (auto timestamped_uri : fragment_uris_) {
        fragment_set.insert(timestamped_uri.uri_);
      }
      return fragment_set;
    }
  };

  /**
   * Class to return a location of a delete or update tile, which is file
   * URI/offset.
   */
  class DeleteAndUpdateTileLocation {
   public:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */

    /**
     * Constructor.
     */
    DeleteAndUpdateTileLocation(
        const URI& uri,
        const std::string condition_marker,
        const storage_size_t offset)
        : uri_(uri)
        , condition_marker_(condition_marker)
        , offset_(offset) {
      std::pair<uint64_t, uint64_t> timestamps;
      if (!utils::parse::get_timestamp_range(URI(condition_marker), &timestamps)
               .ok()) {
        throw std::logic_error("Error parsing uri.");
      }

      timestamp_ = timestamps.first;
    }

    /** Destructor. */
    ~DeleteAndUpdateTileLocation() = default;

    bool operator<(const DeleteAndUpdateTileLocation& rhs) const {
      return (timestamp_ < rhs.timestamp_);
    }

    bool operator==(const DeleteAndUpdateTileLocation& other) const {
      return (
          uri() == other.uri() &&
          condition_marker() == other.condition_marker() &&
          offset() == other.offset() && timestamp() == other.timestamp());
    }

    /* ********************************* */
    /*                API                */
    /* ********************************* */
    inline const URI& uri() const {
      return uri_;
    }

    inline const std::string& condition_marker() const {
      return condition_marker_;
    }

    inline uint64_t offset() const {
      return offset_;
    }

    inline uint64_t timestamp() const {
      return timestamp_;
    }

   private:
    /* ********************************* */
    /*         PRIVATE ATTRIBUTES        */
    /* ********************************* */

    /** The URIs of the file. */
    URI uri_;

    /** The condition marker. */
    std::string condition_marker_;

    /** The offset within the file. */
    uint64_t offset_;

    /** Stores the timestamp of the delete for sorting. */
    uint64_t timestamp_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param resources A reference to a ContextResources instance
   * @param uri The URI of the array directory
   */
  ArrayDirectory(ContextResources& resources, const URI& uri);

  /**
   * Constructor.
   *
   * @param resources A reference to the ContextResources instance
   * @param uri The URI of the array directory.
   * @param timestamp_start Only array fragments, metadata, etc. that
   *     were created within timestamp range
   *    [`timestamp_start`, `timestamp_end`] will be considered when
   *     fetching URIs.
   * @param timestamp_end Only array fragments, metadata, etc. that
   *     were created within timestamp range
   *    [`timestamp_start`, `timestamp_end`] will be considered when
   *     fetching URIs.
   * @param mode The mode to load the array directory in.
   */
  ArrayDirectory(
      ContextResources& resources,
      const URI& uri,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      ArrayDirectoryMode mode = ArrayDirectoryMode::READ);

  /** Destructor. */
  ~ArrayDirectory() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Loads the schema of a schema uri from persistent storage into memory.
   *
   * @param array_schema_uri The URI path of the array schema.
   * @param encryption_key The encryption key to use.
   * @return Status, the loaded array schema
   */
  static shared_ptr<ArraySchema> load_array_schema_from_uri(
      ContextResources& resources,
      const URI& array_schema_uri,
      const EncryptionKey& encryption_key);

  /**
   * Get the full vac uri using the base URI and a vac uri that might be
   * relative or not. It also fixes URIs that were not relative and the array
   * has moved since consolidation.
   *
   * @param base Base URI for the array.
   * @param vac_uri The vac URI that might or not be relative to the array.
   * @return std::string Full vac URI.
   */
  static std::string get_full_vac_uri(std::string base, std::string vac_uri);

  /**
   * Loads the latest schema of an array from persistent storage into memory.
   *
   * @param array_dir The ArrayDirectory object used to retrieve the
   *     various URIs in the array directory.
   * @param encryption_key The encryption key to use.
   * @return Status, a new ArraySchema
   */
  shared_ptr<ArraySchema> load_array_schema_latest(
      const EncryptionKey& encryption_key) const;

  /**
   * It loads and returns the latest schema and all the array schemas
   * (in the presence of schema evolution).
   *
   * @param array_dir The ArrayDirectory object used to retrieve the
   *     various URIs in the array directory.
   * @param encryption_key The encryption key to use.
   * @return tuple of Status, latest array schema and all array schemas.
   *   Status Ok on success, else error
   *   ArraySchema The latest array schema.
   *   ArraySchemaMap Map of all array schemas loaded, keyed by name
   */
  tuple<
      shared_ptr<ArraySchema>,
      std::unordered_map<std::string, shared_ptr<ArraySchema>>>
  load_array_schemas(const EncryptionKey& encryption_key) const;

  /**
   * Loads all schemas of an array from persistent storage into memory.
   *
   * @param encryption_key The encryption key to use.
   * @return tuple of Status and optional unordered map. If Status is an error
   * the unordered_map will be nullopt
   *        Status Ok on success, else error
   *        ArraySchemaMap Map of all array schemas found keyed by name
   */
  std::unordered_map<std::string, shared_ptr<ArraySchema>>
  load_all_array_schemas(const EncryptionKey& encryption_key) const;

  /**
   * Load an enumeration from schema with the given name.
   *
   * @param schema The ArraySchema that references the enumeration name.
   * @param enumeration_name The name of the enumeration to load.
   * @param encryption_key The encryption key to use.
   * @return shared_ptr<Enumeration> The loaded enumeration.
   */
  shared_ptr<const Enumeration> load_enumeration(
      shared_ptr<ArraySchema> schema,
      const std::string& enumeration_name,
      const EncryptionKey& encryption_key) const;

  /**
   * Load all enumerations for the given schema.
   *
   * @param schema The ArraySchema to load Enumerations for
   * @param encryption_key The encryption key to use.
   */
  void load_all_enumerations(
      shared_ptr<ArraySchema> schema,
      const EncryptionKey& encryption_key) const;

  /** Returns the array URI. */
  const URI& uri() const;

  /** Returns the URIs of the array schema files. */
  const std::vector<URI>& array_schema_uris() const;

  /** Returns the latest array schema URI. */
  const URI& latest_array_schema_uri() const;

  /** Returns the unfiltered fragment uris. */
  const std::vector<URI>& unfiltered_fragment_uris() const;

  /** Returns the URIs of the array metadata files to vacuum. */
  const std::vector<URI>& array_meta_uris_to_vacuum() const;

  /** Returns the URIs of the array metadata vacuum files to vacuum. */
  const std::vector<URI>& array_meta_vac_uris_to_vacuum() const;

  /** Returns the URIs of the commit files to consolidate. */
  const std::vector<URI>& commit_uris_to_consolidate() const;

  /** Returns the URIs of the commit files to vacuum. */
  const std::vector<URI>& commit_uris_to_vacuum() const;

  /** Returns the consolidated commit URI set. */
  const std::unordered_set<std::string>& consolidated_commit_uris_set() const;

  /** Returns the URIs of the consolidated commit files to vacuum. */
  const std::vector<URI>& consolidated_commits_uris_to_vacuum() const;

  /** Returns the filtered array metadata URIs. */
  const std::vector<TimestampedURI>& array_meta_uris() const;

  /** Returns the URIs of the consolidated fragment metadata files. */
  const std::vector<URI>& fragment_meta_uris() const;

  /** Returns the location of delete tiles. */
  const std::vector<DeleteAndUpdateTileLocation>&
  delete_and_update_tiles_location() const;

  /** Returns the fragment absolute path given an array URI and a version */
  static URI generate_fragment_dir_uri(uint32_t write_version, URI array_uri);

  /** Returns the URI to store fragments. */
  URI get_fragments_dir(uint32_t write_version) const;

  /** Returns the URI to store fragment metadata. */
  URI get_fragment_metadata_dir(uint32_t write_version) const;

  /** Returns the URI to store commit files. */
  URI get_commits_dir(uint32_t write_version) const;

  /** Returns the URI for either an ok file or wrt file. */
  URI get_commit_uri(const URI& fragment_uri) const;

  /** Returns the URI for a vacuum file. */
  URI get_vacuum_uri(const URI& fragment_uri) const;

  /**
   * The new fragment name is computed
   * as `__<first_URI_timestamp>_<last_URI_timestamp>_<uuid>`.
   */
  std::string compute_new_fragment_name(
      const URI& first, const URI& last, format_version_t format_version) const;

  /** Returns `true` if `load` has been run. */
  bool loaded() const;

  /** Returns the filtered fragment URIs struct. */
  const FilteredFragmentUris filtered_fragment_uris(
      const bool full_overlap_only) const;

  /** Returns the start timestamp of the files to be considered */
  const uint64_t& timestamp_start() const;

  /** Returns the end timestamp of the files to be considered */
  const uint64_t& timestamp_end() const;

  /** Writes a commit ignore file. */
  void write_commit_ignore_file(const std::vector<URI>& commit_uris_to_ignore);

  /** Deletes the array fragments at the given fragment URIs. */
  void delete_fragments_list(const std::vector<URI>& fragment_uris);

  /* ACCESSORS */

  /** Accessor to array uri_ */
  inline URI& uri() {
    return uri_;
  }

  /** Accessor to array_schema_uris_ */
  inline std::vector<URI>& array_schema_uris() {
    return array_schema_uris_;
  }

  /** Accessor to latest_array_schema_uri_ */
  inline URI& latest_array_schema_uri() {
    return latest_array_schema_uri_;
  }

  /** Accessor to unfiltered_fragment_uris_ */
  inline std::vector<URI>& unfiltered_fragment_uris() {
    return unfiltered_fragment_uris_;
  }

  /** Accessor to consolidated_commit_uris_set_ */
  inline std::unordered_set<std::string>& consolidated_commit_uris_set() {
    return consolidated_commit_uris_set_;
  }

  /** Accessor to array_meta_uris_to_vacuum_ */
  inline std::vector<URI>& array_meta_uris_to_vacuum() {
    return array_meta_uris_to_vacuum_;
  }

  /** Accessor to array_meta_vac_uris_to_vacuum_ */
  inline std::vector<URI>& array_meta_vac_uris_to_vacuum() {
    return array_meta_vac_uris_to_vacuum_;
  }

  /** Accessor to commit_uris_to_consolidate_ */
  inline std::vector<URI>& commit_uris_to_consolidate() {
    return commit_uris_to_consolidate_;
  }

  /** Accessor to commit_uris_to_vacuum_ */
  inline std::vector<URI>& commit_uris_to_vacuum() {
    return commit_uris_to_vacuum_;
  }

  /** Accessor to consolidated_commits_uris_to_vacuum_ */
  inline std::vector<URI>& consolidated_commits_uris_to_vacuum() {
    return consolidated_commits_uris_to_vacuum_;
  }

  /** Accessor to array_meta_uris_ */
  inline std::vector<TimestampedURI>& array_meta_uris() {
    return array_meta_uris_;
  }

  /** Accessor to timestamp_start_ */
  inline uint64_t& timestamp_start() {
    return timestamp_start_;
  }

  /** Accessor to timestamp_end_ */
  inline uint64_t& timestamp_end() {
    return timestamp_end_;
  }

  /** Accessor to fragment_meta_uris_ */
  inline std::vector<URI>& fragment_meta_uris() {
    return fragment_meta_uris_;
  }

  /** Accessor to delete_and_update_tiles_location_ */
  inline std::vector<DeleteAndUpdateTileLocation>&
  delete_and_update_tiles_location() {
    return delete_and_update_tiles_location_;
  }

  /** Accessor to loaded_ */
  inline bool& loaded() {
    return loaded_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The ContextResources class. */
  std::reference_wrapper<ContextResources> resources_;

  /** The array URI. */
  URI uri_;

  /** The class stats. */
  stats::Stats* stats_;

  /** Fragment URIs. */
  std::vector<URI> unfiltered_fragment_uris_;

  /** Consolidated commit URI set. */
  std::unordered_set<std::string> consolidated_commit_uris_set_;

  /** The URIs of all the array schema files. */
  std::vector<URI> array_schema_uris_;

  /** The latest array schema URI. */
  URI latest_array_schema_uri_;

  /** The URIs of the array metadata files to vacuum. */
  std::vector<URI> array_meta_uris_to_vacuum_;

  /** The URIs of the array metadata vac files to vacuum. */
  std::vector<URI> array_meta_vac_uris_to_vacuum_;

  /** The URIs of the commits files to consolidate. */
  std::vector<URI> commit_uris_to_consolidate_;

  /** The URIs of the commits files to vacuum. */
  std::vector<URI> commit_uris_to_vacuum_;

  /** The URIs of the consolidated commits files to vacuum. */
  std::vector<URI> consolidated_commits_uris_to_vacuum_;

  /**
   * The filtered array metadata URIs, after removing the ones that
   * need to be vacuum and those that do not fall inside range
   * [`timestamp_start_`, `timestamp_end_`].
   */
  std::vector<TimestampedURI> array_meta_uris_;

  /** The URIs of the consolidated fragment metadata files. */
  std::vector<URI> fragment_meta_uris_;

  /** The location of delete and update tiles. */
  std::vector<DeleteAndUpdateTileLocation> delete_and_update_tiles_location_;

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

  /** Mode for the array directory. */
  ArrayDirectoryMode mode_;

  /** True if `load` has been run. */
  bool loaded_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Loads the URIs from the various array subdirectories. */
  Status load();

  /** Returns a set of the known TileDB array directory names. */
  static const std::set<std::string>& dir_names();

  /**
   * Lists the given URI and returns filtered results.
   *
   * @param uri The URI to list
   * @return vector of URIs
   */
  std::vector<URI> ls(const URI& uri) const;

  /**
   * List the root directory uris for v1 to v11.
   *
   * @return vector of URIs.
   */
  std::vector<URI> list_root_dir_uris();

  /**
   * Loads the root directory uris for v1 to v11.
   *
   * @return Status, vector of fragment URIs.
   */
  tuple<Status, optional<std::vector<URI>>> load_root_dir_uris_v1_v11(
      const std::vector<URI>& root_dir_uris);

  /**
   * List the commits directory uris for v12 or higher.
   *
   * @return vector of commit URIs.
   */
  std::vector<URI> list_commits_dir_uris();

  /**
   * Loads the commit directory uris for v12 or higher.
   *
   * @return Status, vector of fragment URIs.
   */
  tuple<Status, optional<std::vector<URI>>> load_commits_dir_uris_v12_or_higher(
      const std::vector<URI>& commits_dir_uris,
      const std::vector<URI>& consolidated_uris);

  /**
   * Loads the fragment metadata directory uris for v12 or higher.
   *
   * @return fragment metadata URIs.
   */
  std::vector<URI> list_fragment_metadata_dir_uris_v12_or_higher();

  /**
   * Loads the commits URIs to consolidate.
   */
  void load_commits_uris_to_consolidate(
      const std::vector<URI>& array_dir_uris,
      const std::vector<URI>& commits_dir_uris,
      const std::vector<URI>& consolidated_uris,
      const std::unordered_set<std::string>& consolidated_uris_set);

  /**
   * Loads the consolidated commit URI from the commit directory and the files
   * to vacuum for the commits directory.
   *
   * @return Status, consolidated uris, set of all consolidated uris.
   */
  tuple<
      Status,
      optional<std::vector<URI>>,
      optional<std::unordered_set<std::string>>>
  load_consolidated_commit_uris(const std::vector<URI>& commits_dir_uris);

  /** Loads the array metadata URIs. */
  void load_array_meta_uris();

  /** Loads the array schema URIs. */
  void load_array_schema_uris();

  /**
   * Computes the fragment URIs from the input array directory URIs, for
   * versions 1 to 11.
   */
  tuple<Status, optional<std::vector<URI>>> compute_fragment_uris_v1_v11(
      const std::vector<URI>& array_dir_uris) const;

  /**
   * Computes the fragment meta URIs from the input array directory.
   */
  std::vector<URI> compute_fragment_meta_uris(
      const std::vector<URI>& array_dir_uris);

  /**
   * Computes the fragment URIs and vacuum URIs to vacuum.
   *
   * @param full_overlap_only Only enable full overlap.
   * @param uris The URIs to calculate the URIs to vacuum from.
   * @return Status, a vector of the URIs to vacuum, a vector of
   *     the vac file URIs to vacuum.
   */
  tuple<Status, optional<std::vector<URI>>, optional<std::vector<URI>>>
  compute_uris_to_vacuum(
      const bool full_overlap_only, const std::vector<URI>& uris) const;

  /**
   * Computes the filtered URIs based on the input, which fall
   * in the timestamp range [`timestamp_start_`, `timestamp_end_`].
   *
   * @param full_overlap_only Only enable full overlap.
   * @param uris The URIs to filter.
   * @param to_ignore The URIs to ignore (because they are vacuumed).
   * @return Status, vector of filtered timestamped URIs.
   */
  tuple<Status, optional<std::vector<TimestampedURI>>> compute_filtered_uris(
      const bool full_overlap_only,
      const std::vector<URI>& uris,
      const std::vector<URI>& to_ignore) const;

  /**
   * Computes and sets the final vector of array schema URIs, and the
   * latest array schema URI, given `array_schema_dir_uris`.
   *
   * @return Status.
   */
  Status compute_array_schema_uris(
      const std::vector<URI>& array_schema_dir_uris);

  /**
   * Checks if a fragment overlaps with the array directory timestamp
   * range. Overlap is partial or full depending on the consolidation
   * type (with timestamps or not).
   *
   * @param fragment_timestamp_range Timestamp range of a given fragment.
   * @param consolidation_with_timestamps True if consolidation includes
   * timestamps, false otherwise.
   * @return True if there is overlap, false otherwise.
   */
  bool timestamps_overlap(
      const std::pair<uint64_t, uint64_t> fragment_timestamp_range,
      const bool consolidation_with_timestamps) const;

  /** Returns true if the input URI is a vacuum file. */
  bool is_vacuum_file(const URI& uri) const;

  /**
   * Checks if the input URI represents a fragment. The functions takes into
   * account the fragment version, which is retrived directly from the URI.
   * For versions >= 5, the function checks whether the URI is included
   * in `ok_uris`. For versions < 5, `ok_uris` is empty so the function
   * checks for the existence of the fragment metadata file in the fragment
   * URI directory. Therefore, the function is more expensive for earlier
   * fragment versions.
   *
   * @param uri The URI to be checked.
   * @param ok_uris For checking URI existence of versions >= 5.
   * @param consolidated_uris_set For checking URI existence of versions >= 5.
   * @param is_fragment Set to `1` if the URI is a fragment and `0`
   *     otherwise.
   * @return Status
   */
  Status is_fragment(
      const URI& uri,
      const std::unordered_set<std::string>& ok_uris_set,
      const std::unordered_set<std::string>& consolidated_uris_set,
      int32_t* is_fragment) const;

  /**
   * Checks if consolidation with timestamps is supported for a fragment
   *
   * @param uri The fragment URI to be checked.
   * @return True if supported, false otherwise
   */
  bool consolidation_with_timestamps_supported(const URI& uri) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ARRAY_DIRECTORY_H
