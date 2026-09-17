/**
 * @file tile_ai.h
 *
 * TileDB VFS backend that routes all S3 access through presigned URLs.
 * URI format: tile://{array_id}/{relative_key}
 *
 * Credential resolution (highest precedence first). The `vfs.tile.*` config
 * is primary; the SDK env-var pair sits below it so a tile.ai-aware
 * environment is honored without users having to touch TileDB-specific
 * config:
 *   1. `vfs.tile.{server_url,api_key}` — config keys (user-set,
 *                                        `TILEDB_VFS_TILE_*` env, or profile).
 *   2. `TILE_API_URL` / `TILE_API_KEY` — tile.ai SDK env-var convention,
 *                                        shared with the tile.ai Python SDK,
 *                                        the Go `tile-fuse` tool, and the
 *                                        env vars that tile-ai injects into
 *                                        containers it spawns.
 */

#ifndef TILEDB_TILE_AI_H
#define TILEDB_TILE_AI_H

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/filesystem_base.h"
#include "tiledb/sm/rest/tile_ai_client.h"
#include "tiledb/sm/filesystem/uri.h"

namespace tiledb::sm {

/**
 * VFS backend that mediates all S3 access through presigned URLs issued by the
 * tile.ai tile.ai server. Accepts URIs of the form
 * `tile://{teamspace_ref}/{name}[/{relative_key}]`, where `teamspace_ref`
 * is either the canonical teamspace id (`{name-slug}-{uuid}` PK) or the
 * human-readable teamspace name. Name resolution is scoped by the
 * caller's workspace memberships server-side; ambiguous names on a
 * multi-workspace API key require `vfs.tile.workspace` to disambiguate.
 * The id-form `tile://{id}` is also accepted for direct lookups by
 * resource id.
 *
 * Most methods throw TileAiException on transport errors or unexpected
 * HTTP statuses, and FilesystemException for unsupported operations (notably
 * file deletion, which the server does not currently expose).
 */
class TileAi : public FilesystemBase {
 public:
  TileAi();
  ~TileAi();

  TileAi(const TileAi&) = delete;
  TileAi& operator=(const TileAi&) = delete;

  /**
   * Initializes the backend from config. Must be called before any other
   * method; subsequent calls overwrite prior state.
   *
   * Reads:
   *  - `vfs.tile.server_url` and `vfs.tile.api_key`. Resolved through
   *    the standard Config chain (user set, `TILEDB_VFS_TILE_*` env,
   *    SDK env alias `TILE_API_URL` / `TILE_API_KEY`, profile, default);
   *    the SDK alias step lives in `Config::get_from_env`.
   *  - `vfs.tile.workspace`. Same chain, with SDK alias
   *    `TILE_API_WORKSPACE`. Optional in the common case: when empty
   *    the client omits `?workspaceId=` and the server applies its
   *    three-tier resolution (single-workspace auto-resolve, then
   *    derive from entity ids in the URL path or request body).
   *    Required only when the API key has memberships in two or more
   *    workspaces and the URI uses a teamspace NAME that exists in
   *    more than one of those workspaces; without a pin the server
   *    returns 400 "Ambiguous teamspace name 'X'".
   *  - `vfs.tile.create_storage_uri` — optional storage URI consulted
   *    by `TileAiClient` (peer of `RestClient`) when it registers a
   *    new array or group with the tile.ai catalog. This VFS reads
   *    the key only for parity with the catalog client; it does not
   *    perform catalog registration itself.
   *  - `vfs.tile.multipart_threshold_bytes` (default 5 MiB) — switch from
   *    simple PUT to multipart upload once the buffered write reaches this
   *    size.
   *  - `vfs.tile.multipart_part_size_bytes` (default 5 MiB) — size of each
   *    multipart part; must be >= 5 MiB (S3 minimum).
   *
   * Type routing for reads is resolved via the catalog:
   * `canonicalize_resource` calls `lookup_resource{,_by_name}` and stamps
   * `parsed.effective_entity_type` from the response. Catalog mutations
   * (creating an array or group, committing write sessions, writing
   * group membership) all live in `TileAiClient` and are dispatched
   * directly from `Array::create` / `Group::create` / `Group::close`,
   * not through the VFS. This backend handles only the presigned-URL
   * I/O for `tile://` URIs.
   *
   * @param config TileDB config consulted for the keys above.
   * @throws TileAiException if `server_url` or `api_key` resolves empty,
   *   if `multipart_part_size_bytes` is below the 5 MiB S3 minimum, or if
   *   `multipart_threshold_bytes` is less than `multipart_part_size_bytes`.
   */
  void init(const Config& config);

  /**
   * @return `true` for `tile://` URIs; `false` otherwise.
   */
  bool supports_uri(const URI& uri) const override;

  /**
   * No-op for valid `tile://` URIs: S3 has no real directories, and
   * catalog registration for top-level resources happens through
   * `TileAiClient` (called from `Array::create` / `Group::create`),
   * not via the VFS. Validates that the URI is hierarchical; throws
   * for `tile://{id}` forms since ids are server-generated.
   */
  void create_dir(const URI& uri) const override;

  /** Implemented as a zero-byte `write` + `flush` pair. */
  void touch(const URI& uri) const override;

  /**
   * For top-level array URIs, returns whether the array is registered with
   * the server. For URIs with a relative key, returns whether
   * `ls_with_sizes` produces any entries under that prefix.
   */
  bool is_dir(const URI& uri) const override;

  /** Probes via `file_size`; returns `false` (not throws) if not found. */
  bool is_file(const URI& uri) const override;

  /**
   * No-op. The tile.ai server does not currently expose deletion APIs;
   * TileDB's create/write rollback paths still call this, so it is silently
   * tolerated.
   */
  void remove_dir(const URI& uri) const override;

  /** Always throws FilesystemException — deletion is unsupported. */
  void remove_file(const URI& uri) const override;

  /** Throws TileAiException with HTTP status 404 if the key is unknown. */
  uint64_t file_size(const URI& uri) const override;

  std::vector<tiledb::common::filesystem::directory_entry> ls_with_sizes(
      const URI& parent) const override;

  /**
   * Reads via a cached presigned GET URL. On HTTP 403 (presumed expired URL),
   * the cache entry is evicted and the fetch is retried once with a fresh
   * URL.
   *
   * @return Number of bytes actually written into `buffer`. May be less than
   *   `nbytes` near EOF.
   */
  uint64_t read(
      const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const override;

  /**
   * Buffers `buffer` in memory keyed on `uri`. When the accumulated buffer
   * for that URI reaches `multipart_threshold_bytes`, the backend creates a
   * server-side multipart upload and starts uploading parts of size
   * `multipart_part_size_bytes`. Below threshold, no network I/O happens
   * until `flush`.
   *
   * @param remote_global_order_write Ignored — applies only to the S3 backend.
   */
  void write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size,
      bool remote_global_order_write = false) override;

  /**
   * Finalizes the in-memory write for `uri`. For below-threshold writes, this
   * issues a single presigned PUT; for multipart writes, it uploads any
   * remaining buffered data and calls `multipart_complete`. Either path then
   * commits the server-side write session for this single key.
   *
   * To commit multiple keys' writes atomically (with a shared
   * fragments_written / metadata_updated payload), use `commit_writes`
   * instead of relying on per-`flush` commits.
   *
   * @param finalize Ignored — applies only to the S3 backend.
   */
  void flush(const URI& uri, bool finalize = false) override;

  /**
   * Result of decomposing a `tile://` URI into the shape every backend
   * method consumes. Two forms:
   *
   *  - Hierarchical (one or more slashes after the scheme prefix):
   *      `array_id = "{teamspace}/{name}"`, `relative_key` = subpath
   *      below the resource root (empty for top-level URIs). `id_form`
   *      is `false`, `id` is empty. The catalog (via
   *      `lookup_resource_by_name`) decides whether the resource is
   *      treated as an array or a group.
   *
   *  - Id-form (zero slashes after the scheme prefix):
   *      `id` = the single segment (e.g. `tiledb-{uuid}`), `array_id`
   *      and `relative_key` are empty until canonicalized via
   *      `lookup_resource`. `id_form` is `true`. Type is also
   *      catalog-self-describing.
   *
   * Public for testing; treat as an internal type.
   */
  struct ParsedUri {
    /**
     * Server-side resource address — `"{teamspace}/{name}"`. Populated
     * directly by `parse_uri` for hierarchical URIs; left empty for
     * id-form URIs until `canonicalize_resource` resolves them.
     */
    std::string array_id;
    /** Sub-key under the resource root. Always empty for id-form URIs. */
    std::string relative_key;
    /**
     * `true` when the URI was a single segment (`tile://{id}`); `false`
     * for hierarchical URIs. Set by `parse_uri`.
     */
    bool id_form = false;
    /** Raw id segment for id-form URIs; empty for hierarchical. */
    std::string id;
    /**
     * `"array"` or `"group"` — the type the per-resource endpoints
     * should route to. Empty after `parse_uri`; populated by
     * `canonicalize_resource` from the catalog
     * (`lookup_resource{,_by_name}`). The dispatch helper
     * `client_for(parsed.effective_entity_type)` reads this field
     * to pick the right pre-bound client, so a single op against
     * an id-form URI of a different type still routes correctly.
     */
    std::string effective_entity_type;
  };

  /**
   * Decompose a `tile://` URI into a `ParsedUri`.
   * **Path structure alone is the disambiguator**: zero slashes after the
   * scheme prefix → id-form; one or more slashes → hierarchical. No
   * prefix gate on the id segment — the catalog (`lookup_resource`) is
   * the authoritative arbiter of "is this a real id?", so a static
   * regex on the C++ side would only couple us to a server-side
   * convention without buying us anything.
   *
   * Pure parsing — no network calls. For id-form URIs the result has
   * `id_form=true` and only `id` populated; call `canonicalize_resource`
   * to fill in the hierarchical fields.
   *
   * Public for testing; treat as an internal helper.
   *
   * @throws TileAiException if the URI doesn't start with one of
   *   the two supported scheme prefixes.
   */
  static ParsedUri parse_uri(const URI& uri);

  /**
   * Resolve a `ParsedUri` to its canonical hierarchical address and
   * type via the catalog. Calls `lookup_resource(id)` for id-form
   * URIs and `lookup_resource_by_name(teamspace, name)` for
   * hierarchical ones — both fall through `/tiles` → `/groups`, so
   * the type comes from whichever endpoint family answered.
   * Populates `parsed.array_id` and `parsed.effective_entity_type`.
   * Caches successful lookups by URI key (id for id-form,
   * `array_id` for hierarchical) for the lifetime of this backend.
   *
   * Public for testing; treat as an internal helper.
   *
   * @throws TileAiException with HTTP 404 if the id (or
   *   teamspace/name pair) is not registered as either an array or
   *   a group on the server.
   */
  ParsedUri canonicalize_resource(ParsedUri parsed) const;

 private:

  std::string server_url_;
  std::string api_key_;
  // Workspace context resolved from `vfs.tile.workspace` at init.
  // Empty string when the user didn't set it (server-side handling: 400
  // on creates, accept-and-skip-validation on reads/writes).
  std::string workspace_;
  std::string create_storage_uri_;
  uint64_t multipart_threshold_bytes_;
  uint64_t multipart_part_size_bytes_;
  bool initialized_ = false;

  // Single HTTP client; per-resource ops pass an EntityType arg at
  // call time (derived from `parsed.effective_entity_type`, which the
  // catalog stamps via `canonicalize_resource`). Create paths pass
  // the type directly from the call site (`create_dir` → Array,
  // `create_group_dir` → Group).
  std::unique_ptr<TileAiClient> client_;

  struct CachedUrl {
    std::string url;
    std::chrono::system_clock::time_point expires_at;
  };
  mutable std::mutex url_cache_mutex_;
  mutable std::unordered_map<std::string, CachedUrl> url_cache_;

  // Single mutex protects both the array and group component caches —
  // each cache key (the resource's `{teamspace}/{name}` string) only ever
  // lives in one of the two, so contention between them is a non-issue.
  mutable std::mutex components_cache_mutex_;
  mutable std::unordered_map<std::string, ArrayComponents> components_cache_;
  mutable std::unordered_map<std::string, GroupComponents>
      group_components_cache_;

  // Cache for `canonicalize_resource`: id → resolved locator. The catalog
  // is the source of truth for id-form URIs; this just memoises the
  // resolution so repeated ops against the same `tile://{id}` URI don't
  // refetch. Tile renames or deletes during a session would invalidate
  // entries here, but tile-ai already routes mutations through the
  // hierarchical endpoints so id mappings are stable for the lifetime
  // of this backend in normal usage.
  mutable std::mutex id_cache_mutex_;
  mutable std::unordered_map<std::string, ResourceLocator> id_cache_;

  struct MultipartState {
    std::string array_id;
    std::string relative_key;
    std::string upload_id;
    std::string write_session_id;
    // Resolved type ("array" or "group") at the moment write started.
    // Captured here so `flush_part` can dispatch to the right client
    // without re-resolving via the catalog on every part.
    std::string entity_type;
    int next_part_number = 2;
    std::vector<CompletedPart> completed_parts;
    std::vector<uint8_t> part_buffer;
    PresignedPartUrl next_part_url;
  };

  mutable std::mutex write_state_mutex_;
  std::unordered_map<std::string, MultipartState> multipart_state_;

  uint64_t read_impl(
      const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const;
  std::vector<std::string> ls_impl(const URI& uri) const;
  uint64_t file_size_impl(const URI& uri) const;
  void write_impl(const URI& uri, const void* buffer, uint64_t nbytes);
  void flush_impl(const URI& uri);

  static std::string cache_key(
      const std::string& array_id, const std::string& relative_key);
  void ensure_initialized() const;

  std::string get_read_url(
      const std::string& array_id,
      const std::string& relative_key,
      const std::string& type) const;
  /**
   * Perform an HTTP GET for a byte range. Throws on transport-level errors;
   * @p http_status returns the HTTP status so callers can branch on it.
   */
  void do_http_get(
      const std::string& url,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes,
      long* http_status,
      uint64_t* bytes_read) const;
  /**
   * Perform an HTTP PUT. Throws on transport-level errors; @p http_status
   * returns the HTTP status so callers can branch on it.
   */
  void do_http_put(
      const std::string& url,
      const void* buffer,
      uint64_t nbytes,
      std::string* etag,
      long* http_status) const;
  void flush_part(MultipartState& state);
};

}  // namespace tiledb::sm

#endif  // TILEDB_TILE_AI_H
