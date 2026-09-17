/**
 * @file tile_ai_client.h
 *
 * Client for the tile.ai server. Peer of `RestClient`: handles all
 * server-side operations against the tile.ai HTTP API for `tile://`
 * URIs, including catalog registration and group membership in
 * addition to the wire-level presigned-URL machinery used by the
 * `TileAi` VFS backend.
 */

#ifndef TILEDB_TILE_AI_CLIENT_H
#define TILEDB_TILE_AI_CLIENT_H

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "tiledb/common/exception/exception.h"

namespace tiledb::sm {

class Config;
class URI;

class TileAiException : public common::StatusException {
 public:
  explicit TileAiException(const std::string& msg, long http_status = -1)
      : StatusException("TileAi", msg)
      , http_status_(http_status) {
  }

  /** The HTTP status code associated with the error, or -1 if not applicable. */
  long http_status() const {
    return http_status_;
  }

 private:
  long http_status_;
};

/**
 * Selects which family of endpoints a per-resource call hits.
 * `Array` routes to `/api/v1/tiles/...`, `Group` routes to
 * `/api/v1/groups/...`. Used as a parameter on every `TileAiClient`
 * method whose URL shape is `/api/v1/{tiles|groups}/{base}/...`; the
 * client is otherwise type-agnostic.
 */
enum class EntityType { Array, Group };

/** Server-returned record for one registered tile array. */
struct TileInfo {
  /**
   * Server-generated id (e.g. `tiledb-{uuid}` for arrays,
   * `tiledb-group-{uuid}` for groups). Used to address the resource
   * via the `tile://{id}` URI form and the parallel
   * `/api/v1/{tiles,groups}/{id}` lookup routes.
   */
  std::string id;
  /** "{teamspace}/{name}" identifying the resource. */
  std::string base;
  /** Backing object-store URI (e.g. `s3://bucket/prefix`). */
  std::string storage_uri;
  std::string created_at;
  std::string updated_at;
};

/** Server-returned record for one teamspace accessible to the API-key caller. */
struct TeamspaceInfo {
  /** Stable id used in `tile://{id}/{name}` URIs. */
  std::string id;
  /** Human-readable name. Not unique across workspaces. */
  std::string name;
  /** Workspace this teamspace belongs to. */
  std::string workspace_id;
};

/** A single object owned by an array (relative key + size in bytes). */
struct ObjectEntry {
  std::string key;
  int64_t size_bytes;
};

/** One TileDB fragment listing — `name` is the fragment directory. */
struct PresignedFragmentInfo {
  std::string name;
  std::vector<ObjectEntry> objects;
};

/**
 * The full inventory of an array: its identity, backing storage, and every
 * object grouped by category. Returned by `list_array_components`.
 */
struct ArrayComponents {
  std::string array_id;
  std::string s3_uri;
  std::vector<ObjectEntry> schema;
  std::vector<ObjectEntry> array_metadata;
  std::vector<PresignedFragmentInfo> fragments;
  std::vector<ObjectEntry> commits;
  std::vector<ObjectEntry> other;
};

/**
 * The full inventory of a group: its identity, backing storage, and every
 * object grouped by category. Returned by `list_group_components`. Distinct
 * from `ArrayComponents` because the storage layouts diverge (group has a
 * marker file and group_metadata where an array would have schema +
 * fragments). Membership is NOT in this struct — members live in a separate
 * `tiledb_group_members` table on the server, queried via `list_members`.
 */
struct GroupComponents {
  std::string group_id;
  std::string s3_uri;
  std::vector<ObjectEntry> marker;          // __tiledb_group.tdb
  std::vector<ObjectEntry> commits;         // __group/ commit files
  std::vector<ObjectEntry> group_metadata;  // __meta/ files
  std::vector<ObjectEntry> other;
};

/**
 * Server-returned record for `lookup_resource{,_by_name}`: a tile id (or
 * a hierarchical teamspace/name pair) resolved to its full address and
 * type. Used by `TileAi` to canonicalize URIs into the
 * `(teamspace, name)` shape every other client method consumes, and to
 * stamp `parsed.effective_entity_type` for downstream dispatch — the
 * catalog is the source of truth for type, so neither id-form nor
 * hierarchical URIs need a per-context type override.
 */
struct ResourceLocator {
  std::string id;
  std::string type;          // "array" | "group"
  std::string teamspace_id;
  std::string name;
  std::string storage_uri;
};

/**
 * One entry in a group's membership list. Members can be addressed by an
 * absolute `tile://` URI or by a path relative to the group; the
 * `relative` flag distinguishes the two.
 */
struct MemberEntry {
  std::string uri;
  std::string name;      // optional alias; empty if not set
  std::string type;      // "array" | "group" | "unknown"
  bool relative = false;
};

/** A presigned GET/PUT URL for a single key, with its expiration timestamp. */
struct PresignedUrl {
  std::string key;
  std::string url;
  std::chrono::system_clock::time_point expires_at;
};

/** A presigned URL for one part of a multipart upload. */
struct PresignedPartUrl {
  int part_number;
  std::string url;
  std::chrono::system_clock::time_point expires_at;
};

/**
 * Server response for `presign_write`: a presigned PUT URL per requested
 * key, plus a `write_session_id` that must be passed to
 * `commit_write_session` to publish the upload.
 */
struct WritePresignResult {
  std::vector<PresignedUrl> urls;
  std::string write_session_id;
};

/**
 * Server response for `multipart_create`: the multipart `upload_id`, the
 * `write_session_id` for the eventual commit, and one initial presigned
 * part URL (subsequent parts are fetched via `multipart_parts`).
 */
struct MultipartCreateResult {
  std::string upload_id;
  std::string key;
  std::string write_session_id;
  std::vector<PresignedPartUrl> parts;
};

/** A part-number / ETag pair to pass to `multipart_complete`. */
struct CompletedPart {
  int part_number;
  std::string etag;
};

/**
 * HTTP client for the TileDB tile.ai Server. All methods are
 * synchronous, perform JSON-over-HTTP via libcurl with a 30-second timeout,
 * and authenticate with `Authorization: Bearer {api_key}`. Every method
 * throws TileAiException on transport-level errors or when the server
 * returns an HTTP status other than the per-endpoint expected status; the
 * exception carries the actual HTTP status (or -1 for transport errors).
 *
 * Routing between `/api/v1/tiles/...` (arrays) and `/api/v1/groups/...`
 * (groups) is selected per-call via an `EntityType` argument on each
 * method whose URL shape is `/api/v1/{tiles|groups}/{base}/...`. Every
 * such method also has a `std::string_view` overload for callers
 * holding the type as a string (e.g. `parsed.effective_entity_type`);
 * the string overload validates and forwards to the enum overload,
 * throwing on anything other than `"array"` / `"group"`.
 *
 * Endpoints that are inherently type-agnostic — `list_teamspaces`,
 * `lookup_resource{,_by_name}`, `list_{array,group}_components`,
 * `list_members`, and `put_members` — do not take an `EntityType`.
 */
class TileAiClient {
 public:
  /**
   * @param server_url Base URL of the tile-ai deployment, e.g.
   *   `https://app.tile.ai` or `http://localhost:3000` for local dev.
   *   The client appends `/api/v1/...` paths automatically; do not include
   *   that prefix here. A trailing slash is stripped.
   * @param api_key Bearer token sent on every request.
   * @param workspace Optional workspace identifier supplied at connect
   *   time via `vfs.tile.workspace`. A workspace id (`home-…`). When
   *   non-empty, the client appends `?workspaceId=<urlencoded>` (or
   *   `&workspaceId=…`) to every request URL; the server validates
   *   membership and, on routes that operate on a specific tile,
   *   verifies the resolved workspace matches the tile's actual
   *   workspace. Empty string omits the query parameter — the server
   *   falls back to its documented three-tier resolution (single-
   *   workspace auto-resolve, then derive from entity ids in the
   *   request).
   */
  TileAiClient(
      const std::string& server_url,
      const std::string& api_key,
      const std::string& workspace = "");
  ~TileAiClient() = default;

  TileAiClient(const TileAiClient&) = delete;
  TileAiClient& operator=(const TileAiClient&) = delete;
  TileAiClient(TileAiClient&&) = default;
  TileAiClient& operator=(TileAiClient&&) = default;

  /**
   * `GET /api/v1/{tiles|groups}` — list every resource of the given
   * family visible to this api_key.
   *
   * @param entity_type Array → `/api/v1/tiles`; Group → `/api/v1/groups`.
   * @return One TileInfo per registered resource. Empty vector if none exist.
   */
  std::vector<TileInfo> list_resources(EntityType entity_type);
  std::vector<TileInfo> list_resources(std::string_view entity_type);

  /**
   * `GET /api/v1/teamspaces` — list every teamspace accessible to this
   * api_key. Type-agnostic; bypasses `base_path()`.
   *
   * Used by tests to resolve a teamspace by name without requiring developers
   * to hardcode the server-side id.
   *
   * @return One TeamspaceInfo per accessible teamspace. Empty vector if the
   *   caller has no workspace memberships.
   */
  std::vector<TeamspaceInfo> list_teamspaces();

  /**
   * `GET /api/v1/tiles/{id}` with fallthrough to `GET /api/v1/groups/{id}`
   * — resolve an opaque tile id to its hierarchical address and type.
   *
   * Type-agnostic at the call-site by construction: the resolver tries
   * the array endpoint first, falls back to the group endpoint on 404.
   * Bypasses `base_path()` for the same reason — the whole point of
   * id-form is that the catalog tells you the type, so we don't need it
   * configured in advance.
   *
   * @param id The tile id (single segment from a `tile://{id}` URI).
   * @return Locator with `{id, type, teamspace_id, name, storage_uri}`.
   * @throws TileAiException with HTTP 404 if the id is not a tile
   *   under either endpoint family. Other HTTP statuses (auth/permission
   *   errors) propagate verbatim.
   */
  ResourceLocator lookup_resource(const std::string& id);

  /**
   * `GET /api/v1/tiles/{teamspace}/{name}` with fallthrough to
   * `GET /api/v1/groups/{teamspace}/{name}` — resolve a hierarchical
   * `tile://{teamspace}/{name}` URI to its type and storage uri.
   *
   * Same fall-through pattern as `lookup_resource(id)`: try /tiles
   * first, fall back to /groups on 404. Bypasses `base_path()` —
   * the whole point is to discover the type from the catalog rather
   * than have the client assume one.
   *
   * @param teamspace_id The teamspace segment of the URI.
   * @param name The resource segment of the URI.
   * @return Locator with `{id, type, teamspace_id, name, storage_uri}`.
   * @throws TileAiException with HTTP 404 if the (teamspace, name)
   *   pair is not registered as either an array or a group. Other HTTP
   *   statuses (auth/permission) propagate verbatim.
   */
  ResourceLocator lookup_resource_by_name(
      const std::string& teamspace_id, const std::string& name);

  /**
   * `POST /api/v1/{tiles|groups}` — register a new resource with
   * caller-chosen storage.
   *
   * @param entity_type Selects the endpoint family.
   * @param base "{teamspace}/{name}" for the new resource.
   * @param storage_uri Backing object-store URI (e.g. `s3://bucket/prefix`).
   * @return The created TileInfo as echoed back by the server.
   * @throws TileAiException if the server returns anything other than
   *   HTTP 201 (notably 409 if the resource already exists).
   */
  TileInfo create_resource(
      EntityType entity_type,
      const std::string& base,
      const std::string& storage_uri);
  TileInfo create_resource(
      std::string_view entity_type,
      const std::string& base,
      const std::string& storage_uri);

  /**
   * `POST /api/v1/{tiles|groups}` — register a new resource, letting the
   * server pick the backing storage URI.
   *
   * @param entity_type Selects the endpoint family.
   * @param base "{teamspace}/{name}" for the new resource.
   * @return The created TileInfo (server-chosen storage_uri).
   * @throws TileAiException if the server returns anything other than
   *   HTTP 201.
   */
  TileInfo create_resource(EntityType entity_type, const std::string& base);
  TileInfo create_resource(
      std::string_view entity_type, const std::string& base);

  /**
   * `GET /api/v1/tiles/{base}/components` — fetch the full object inventory
   * for an array. Use only when the resource is known to be an array; for
   * group resources call `list_group_components` instead.
   *
   * @param base "{teamspace}/{name}".
   * @return ArrayComponents with schema/metadata/fragments/commits/other
   *   populated.
   * @throws TileAiException with HTTP 404 if the array is unknown.
   */
  ArrayComponents list_array_components(const std::string& base);

  /**
   * `GET /api/v1/groups/{base}/components` — fetch the full object inventory
   * for a group. The response shape differs from arrays: marker file and
   * group_metadata replace schema and fragments; membership is NOT included
   * here (use `list_members`).
   *
   * @param base "{teamspace}/{name}".
   * @return GroupComponents with marker/commits/group_metadata/other.
   * @throws TileAiException with HTTP 404 if the group is unknown.
   */
  GroupComponents list_group_components(const std::string& base);

  /**
   * `GET /api/v1/groups/{base}/members` — fetch the membership list for a
   * group. The server is the source of truth; libtiledb calls this on
   * group open-for-reads.
   *
   * @param base "{teamspace}/{name}".
   * @return One MemberEntry per registered member. Empty vector if the
   *   group has no members.
   * @throws TileAiException with HTTP 404 if the group is unknown.
   */
  std::vector<MemberEntry> list_members(const std::string& base);

  /**
   * `PUT /api/v1/groups/{base}/members` — replace the entire membership
   * list for a group. The server diffs the new set against existing rows
   * and atomically upserts/deletes. libtiledb calls this on group
   * close-for-writes after computing the updated member list from the
   * in-memory diff.
   *
   * @param base "{teamspace}/{name}".
   * @param members The complete intended membership. Pass an empty vector
   *   to clear all members.
   */
  void put_members(
      const std::string& base, const std::vector<MemberEntry>& members);

  /**
   * `POST /api/v1/{tiles|groups}/{base}/presign/read` — get presigned GET
   * URLs for one or more keys.
   *
   * @param entity_type Selects the endpoint family.
   * @param base "{teamspace}/{name}".
   * @param keys Relative keys within the resource. Order is not guaranteed in
   *   the response — callers should match by `PresignedUrl::key`.
   * @return One PresignedUrl per accessible key.
   */
  std::vector<PresignedUrl> presign_read(
      EntityType entity_type,
      const std::string& base,
      const std::vector<std::string>& keys);
  std::vector<PresignedUrl> presign_read(
      std::string_view entity_type,
      const std::string& base,
      const std::vector<std::string>& keys);

  /**
   * `POST /api/v1/{tiles|groups}/{base}/presign/write` — get presigned PUT
   * URLs for one or more keys (single-shot upload, not multipart).
   *
   * @param entity_type Selects the endpoint family.
   * @param base "{teamspace}/{name}".
   * @param keys Relative keys to write.
   * @return WritePresignResult containing the URLs and the
   *   `write_session_id` to pass to `commit_write_session` after every PUT
   *   succeeds.
   */
  WritePresignResult presign_write(
      EntityType entity_type,
      const std::string& base,
      const std::vector<std::string>& keys);
  WritePresignResult presign_write(
      std::string_view entity_type,
      const std::string& base,
      const std::vector<std::string>& keys);

  /**
   * `POST /api/v1/{tiles|groups}/{base}/presign/multipart/create` — initiate
   * a multipart upload for a single large key.
   *
   * @param entity_type Selects the endpoint family.
   * @param base "{teamspace}/{name}".
   * @param key Relative key being uploaded.
   * @return The `upload_id`, `write_session_id`, and one initial presigned
   *   part URL (typically part 1).
   */
  MultipartCreateResult multipart_create(
      EntityType entity_type,
      const std::string& base,
      const std::string& key);
  MultipartCreateResult multipart_create(
      std::string_view entity_type,
      const std::string& base,
      const std::string& key);

  /**
   * `POST /api/v1/{tiles|groups}/{base}/presign/multipart/parts` — request
   * additional presigned URLs for specific part numbers of an in-flight
   * multipart upload.
   *
   * @param entity_type Selects the endpoint family.
   * @param base "{teamspace}/{name}".
   * @param key Relative key being uploaded.
   * @param upload_id The `upload_id` from `multipart_create`.
   * @param part_numbers Part numbers (1-based) to presign.
   * @return One PresignedPartUrl per requested part.
   */
  std::vector<PresignedPartUrl> multipart_parts(
      EntityType entity_type,
      const std::string& base,
      const std::string& key,
      const std::string& upload_id,
      const std::vector<int>& part_numbers);
  std::vector<PresignedPartUrl> multipart_parts(
      std::string_view entity_type,
      const std::string& base,
      const std::string& key,
      const std::string& upload_id,
      const std::vector<int>& part_numbers);

  /**
   * `POST /api/v1/{tiles|groups}/{base}/presign/multipart/complete` — finalize
   * a multipart upload by handing the server the part numbers and ETags.
   *
   * @param entity_type Selects the endpoint family.
   * @param base "{teamspace}/{name}".
   * @param key Relative key being uploaded.
   * @param upload_id The `upload_id` from `multipart_create`.
   * @param parts Every successfully uploaded part's number and ETag, in any
   *   order.
   */
  void multipart_complete(
      EntityType entity_type,
      const std::string& base,
      const std::string& key,
      const std::string& upload_id,
      const std::vector<CompletedPart>& parts);
  void multipart_complete(
      std::string_view entity_type,
      const std::string& base,
      const std::string& key,
      const std::string& upload_id,
      const std::vector<CompletedPart>& parts);

  /**
   * `POST /api/v1/{tiles|groups}/{base}/presign/multipart/abort` — discard an
   * in-flight multipart upload.
   *
   * @param entity_type Selects the endpoint family.
   * @param base "{teamspace}/{name}".
   * @param key Relative key whose upload to abort.
   * @param upload_id The `upload_id` from `multipart_create`.
   */
  void multipart_abort(
      EntityType entity_type,
      const std::string& base,
      const std::string& key,
      const std::string& upload_id);
  void multipart_abort(
      std::string_view entity_type,
      const std::string& base,
      const std::string& key,
      const std::string& upload_id);

  /**
   * `POST /api/v1/{tiles|groups}/{base}/writes/{write_session_id}/commit` —
   * publish a write session: tells the server which fragments were
   * materialized and whether resource metadata changed, making the writes
   * visible.
   *
   * @param entity_type Selects the endpoint family.
   * @param base "{teamspace}/{name}".
   * @param write_session_id Session id returned by `presign_write` or
   *   `multipart_create`.
   * @param fragments_written Names of fragment directories produced by this
   *   session; passed through verbatim.
   * @param metadata_updated `true` if resource metadata was modified during
   *   the session.
   */
  void commit_write_session(
      EntityType entity_type,
      const std::string& base,
      const std::string& write_session_id,
      const std::vector<std::string>& fragments_written,
      bool metadata_updated);
  void commit_write_session(
      std::string_view entity_type,
      const std::string& base,
      const std::string& write_session_id,
      const std::vector<std::string>& fragments_written,
      bool metadata_updated);

 private:
  std::string server_url_;
  std::string api_key_;
  // Workspace identifier set via `vfs.tile.workspace`. Appended as
  // `?workspaceId=<urlencoded>` (or `&workspaceId=…` when the path
  // already contains a query string) on every request when non-empty;
  // omitted entirely when empty.
  std::string workspace_;

  /**
   * Returns the route prefix for per-resource calls — `/api/v1/tiles` for
   * `EntityType::Array`, `/api/v1/groups` for `EntityType::Group`. Used by
   * every method that addresses a specific resource via `{base}` in the
   * path; type-agnostic endpoints (`/api/v1/teamspaces`,
   * `/api/v1/{tiles,groups}/{id}` lookup) bypass this.
   */
  static std::string base_path(EntityType entity_type);

  /**
   * Return @p path with `?workspaceId=<urlencoded value>` (or
   * `&workspaceId=…`) appended when `workspace_` is non-empty; unchanged
   * otherwise. Used by `http_request` so every server call carries the
   * workspace context the user supplied via `vfs.tile.workspace`.
   */
  std::string with_workspace_query(const std::string& path) const;

  /**
   * Perform an HTTP request and return the response body. Supports GET,
   * POST, and PUT. Throws TileAiException on transport-level errors
   * or when the server returns an HTTP status other than @p expected_status;
   * the exception message includes the method, path, and returned status.
   */
  std::string http_request(
      const std::string& method,
      const std::string& path,
      const std::string& request_body = "",
      long expected_status = 200);

  static std::chrono::system_clock::time_point parse_timestamp(
      const std::string& ts);
};

// ----------------------------------------------------------------------
// High-level URI-taking helpers
//
// These add a thin layer of URI parsing + per-call endpoint family
// selection on top of the wire-level methods above. Called from
// Array::create, Group::create, and Group::close for `tile://` URIs,
// in the same spirit as the corresponding `rest_client()` calls used
// by the `is_tiledb()` branches in those functions.
// ----------------------------------------------------------------------

namespace tile_ai {

/**
 * One entry in a group's membership list, in the language-agnostic
 * form `Group::close` hands the client when committing a write
 * session's member modifications. Translated to `MemberEntry` inside
 * `put_members(client, uri, members)`.
 */
struct GroupMember {
  std::string uri;
  std::optional<std::string> name;
  std::string type;  // "array" | "group" | "unknown"
  bool relative = false;
};

/**
 * Register an array with the tile.ai catalog. Idempotent: on
 * conflict, falls back to `list_resources(EntityType::Array)` to
 * confirm the resource exists at the same `{teamspace}/{name}` base.
 * Passes `EntityType::Array` to the wire-level call.
 *
 * @param client A TileAiClient.
 * @param uri A `tile://{teamspace}/{name}` URI.
 * @param create_storage_uri Optional storage URI hint; empty lets
 *   the server pick.
 */
void create_array(
    TileAiClient& client,
    const URI& uri,
    const std::string& create_storage_uri = "");

/**
 * Register a group with the tile.ai catalog. Same idempotency
 * pattern as `create_array`, with `EntityType::Group`.
 *
 * @param client A TileAiClient.
 * @param uri A `tile://{teamspace}/{name}` URI.
 * @param create_storage_uri Optional storage URI hint.
 */
void create_group(
    TileAiClient& client,
    const URI& uri,
    const std::string& create_storage_uri = "");

/**
 * Replace a group's membership list. Wraps the wire-level
 * `TileAiClient::put_members(base, members)` (group-only, no
 * `EntityType` needed) with URI parsing.
 *
 * @param client A TileAiClient.
 * @param uri A `tile://{teamspace}/{name}` URI.
 * @param members Full intended membership at close time.
 */
void put_members(
    TileAiClient& client,
    const URI& uri,
    const std::vector<GroupMember>& members);

/**
 * Publish one or more write sessions for an array. Each session_id
 * was previously returned by `presign_write` or `multipart_create`
 * during the actual data write (done by the `TileAi` VFS
 * backend). Passes `EntityType::Array` to the wire-level call.
 *
 * @param client A TileAiClient.
 * @param uri Array URI (tile://{teamspace}/{name}).
 * @param session_ids Write session IDs to publish. Empty vector is
 *   a no-op (no HTTP calls).
 * @param fragments_written Fragment directory names produced by the
 *   write batch; passed verbatim to every per-session commit.
 * @param metadata_updated true if array metadata changed.
 */
void commit_array_writes(
    TileAiClient& client,
    const URI& uri,
    const std::vector<std::string>& session_ids,
    const std::vector<std::string>& fragments_written,
    bool metadata_updated);

}  // namespace tile_ai

}  // namespace tiledb::sm

#endif  // TILEDB_TILE_AI_CLIENT_H
