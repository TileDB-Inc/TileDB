/**
 * @file unit-cppapi-tile-ai.cc
 *
 * Public C++ API smoke tests for the tile:// backend.
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/filesystem/tile_ai.h"

using namespace tiledb;
// Disambiguate from `tiledb::Config` (the public C++ API class). The
// internal `tiledb::sm::Config` is what implements the env-var
// resolution chain we test below.
using SmConfig = tiledb::sm::Config;

namespace {

// Read a tile.ai credential / config key as a string, returning empty
// when no source provides a value. Trusts `Config::get_from_env` to
// walk the full chain (user set, TILEDB_VFS_TILE_* env, TILE_API_*
// SDK env alias, profile, default).
std::string config_string(const SmConfig& config, std::string_view key) {
  return std::string(
      config.get<std::string_view>(std::string(key)).value_or(""));
}

std::optional<std::string> env_string(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || value[0] == '\0') {
    return std::nullopt;
  }
  return std::string(value);
}

// Per-process run id used to namespace test-created arrays/groups.
// Honours TILEDB_TEST_TILE_RUN_ID when set; otherwise falls back to the
// process start time as a unix timestamp so back-to-back invocations
// don't collide on the (teamspace, name) unique constraint server-side.
// Cached in a function-local static so every call within one binary
// invocation sees the same id.
const std::string& test_run_id() {
  static const std::string id = []() -> std::string {
    if (auto env = env_string("TILEDB_TEST_TILE_RUN_ID")) {
      return *env;
    }
    return std::to_string(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
  }();
  return id;
}

std::vector<int32_t> make_random_int_data(uint64_t seed, size_t count) {
  std::mt19937 rng(static_cast<uint32_t>(seed));
  std::uniform_int_distribution<int32_t> dist(-5000, 5000);

  std::vector<int32_t> data(count);
  std::generate(data.begin(), data.end(), [&]() { return dist(rng); });
  return data;
}

std::vector<float> make_random_float_data(uint64_t seed, size_t count) {
  std::mt19937 rng(static_cast<uint32_t>(seed));
  std::uniform_real_distribution<float> dist(-1000.0f, 1000.0f);

  std::vector<float> data(count);
  std::generate(data.begin(), data.end(), [&]() { return dist(rng); });
  return data;
}

// Build a standalone TileAiClient for in-test server-side assertions,
// drawing credentials from the library's resolution chain. Use this
// instead of constructing TileAiClient inline with `env_string("TILE_API_*")`
// reads — the test code does not need to know the SDK env-var names.
tiledb::sm::TileAiClient make_tile_ai_client() {
  SmConfig config;
  auto url = config_string(config, "vfs.tile.server_url");
  auto key = config_string(config, "vfs.tile.api_key");
  REQUIRE(!url.empty());
  REQUIRE(!key.empty());
  return tiledb::sm::TileAiClient(url, key);
}

Config make_presigned_config() {
  // Walk the library's credential resolution chain so the test inherits
  // every fallback `TileAi::init` would use at runtime (Config + Config's
  // TILEDB_VFS_TILE_* env, plus the SDK env-var convention TILE_API_URL /
  // TILE_API_KEY shared with the tile.ai Python SDK and tile-fuse). Tests no
  // longer hardcode SDK env-var names — those live entirely in the library.
  SmConfig sm_config;
  if (config_string(sm_config, "vfs.tile.server_url").empty() ||
      config_string(sm_config, "vfs.tile.api_key").empty()) {
    SKIP(
        "tile:// C++ tests require vfs.tile.{server_url,api_key} "
        "(set via TILEDB_VFS_TILE_* or the SDK env vars TILE_API_URL / "
        "TILE_API_KEY)");
  }

  return Config{};
}

struct DenseArrayShape {
  int32_t rows;
  int32_t cols;
  int32_t row_tile_extent;
  int32_t col_tile_extent;

  size_t cell_num() const {
    return static_cast<size_t>(rows) * static_cast<size_t>(cols);
  }
};

struct TileAiCppApiFx {
  static constexpr uint64_t kSeedBase = 20260323;
  static constexpr const char* kDefaultTeamspaceName = "unit-teamspace";

  TileAiCppApiFx()
      : config_(make_presigned_config())
      , ctx_(config_)
      , teamspace_id_(resolve_teamspace_id()) {
  }

  // Resolves the teamspace to use for test arrays by hitting the live
  // /api/v1/teamspaces endpoint with credentials drawn from the library's
  // resolution chain (Config + TILEDB_VFS_TILE_* env + SDK env-var
  // fallback). Defaults to looking for a teamspace literally named
  // "unit-teamspace" so developers don't have to look up server-side ids;
  // override via TILEDB_TEST_TILE_TEAMSPACE_NAME if your dev workspace uses
  // a different name. Errors loudly if zero or multiple teamspaces match
  // — name is not unique across workspaces.
  static std::string resolve_teamspace_id() {
    SmConfig config;
    auto api_url = config_string(config, "vfs.tile.server_url");
    auto api_key = config_string(config, "vfs.tile.api_key");
    REQUIRE(!api_url.empty());
    REQUIRE(!api_key.empty());

    const auto target_name = env_string("TILEDB_TEST_TILE_TEAMSPACE_NAME")
                                 .value_or(kDefaultTeamspaceName);

    tiledb::sm::TileAiClient client(api_url, api_key);
    auto teamspaces = client.list_teamspaces();

    std::vector<std::string> matched_ids;
    for (auto& ts : teamspaces) {
      if (ts.name == target_name) {
        matched_ids.push_back(ts.id);
      }
    }

    if (matched_ids.empty()) {
      FAIL(
          "No teamspace named '" + target_name +
          "' is accessible to this API key. Create one in your tile.ai dev "
          "workspace, or set TILEDB_TEST_TILE_TEAMSPACE_NAME to a teamspace "
          "name your API key can access.");
    }
    if (matched_ids.size() > 1) {
      FAIL(
          "Multiple teamspaces named '" + target_name +
          "' are accessible — name is ambiguous. Rename one, or set "
          "TILEDB_TEST_TILE_TEAMSPACE_NAME to a unique name.");
    }
    return matched_ids[0];
  }

  std::string array_uri(const std::string& suffix) const {
    return "tile://" + teamspace_id_ + "/" + test_run_id() + "-" + suffix;
  }

  // Group equivalent of `array_uri`. Uses the same teamspace + run_id
  // scheme. URI shape is identical for arrays and groups; dispatch is
  // by call site (`Group::create` → `create_group_dir` hook for the
  // create path) and by the catalog (`lookup_resource_by_name` for
  // the read path) — neither needs a config knob.
  std::string group_uri(const std::string& suffix) const {
    return "tile://" + teamspace_id_ + "/" + test_run_id() + "-grp-" + suffix;
  }

  // Returns a `tiledb::Context` for group operations. Currently
  // identical to `ctx_` — group creates route via the
  // `create_group_dir` VFS hook that `Group::create` calls, and
  // group reads resolve type via the catalog
  // (`lookup_resource_by_name`), so no per-context config overlay is
  // needed. The helper is kept as a hook in case future tests need
  // to override per-context tile.* settings for group flows.
  tiledb::Context group_ctx() const {
    return tiledb::Context(config_);
  }

  void register_array_uri(const std::string& uri) const {
    VFS vfs(ctx_);
    vfs.create_dir(uri);
  }

  void create_dense_array(
      const std::string& uri,
      const DenseArrayShape& shape = {4, 3, 4, 3}) const {
    register_array_uri(uri);

    Domain domain(ctx_);
    domain.add_dimension(Dimension::create<int32_t>(
        ctx_, "rows", {{1, shape.rows}}, shape.row_tile_extent));
    domain.add_dimension(Dimension::create<int32_t>(
        ctx_, "cols", {{1, shape.cols}}, shape.col_tile_extent));

    ArraySchema schema(ctx_, TILEDB_DENSE);
    schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
    schema.set_domain(domain);
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "values"));

    Array::create(ctx_, uri, schema);
  }

  void write_dense_array(
      const std::string& uri, std::vector<int32_t>& data) const {
    Array array(ctx_, uri, TILEDB_WRITE);
    Query query(ctx_, array);
    query.set_layout(TILEDB_ROW_MAJOR);
    query.set_data_buffer("values", data);
    query.submit();
    query.finalize();
    array.close();
  }

  std::vector<int32_t> read_dense_array(
      const std::string& uri,
      const DenseArrayShape& shape = {4, 3, 4, 3}) const {
    Array array(ctx_, uri, TILEDB_READ);
    Query query(ctx_, array);
    query.set_layout(TILEDB_ROW_MAJOR);
    Subarray subarray(ctx_, array);
    subarray.add_range<int32_t>(0, 1, shape.rows);
    subarray.add_range<int32_t>(1, 1, shape.cols);
    query.set_subarray(subarray);

    std::vector<int32_t> data(shape.cell_num());
    query.set_data_buffer("values", data);
    query.submit();
    array.close();
    return data;
  }

  void create_sparse_array(const std::string& uri) const {
    register_array_uri(uri);

    Domain domain(ctx_);
    domain.add_dimension(Dimension::create<int32_t>(ctx_, "x", {{1, 20}}, 5));
    domain.add_dimension(Dimension::create<int32_t>(ctx_, "y", {{1, 20}}, 5));

    ArraySchema schema(ctx_, TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.set_allows_dups(false);
    schema.add_attribute(Attribute::create<float>(ctx_, "a"));

    Array::create(ctx_, uri, schema);
  }

  void write_sparse_array(
      const std::string& uri,
      std::vector<int32_t>& xs,
      std::vector<int32_t>& ys,
      std::vector<float>& values) const {
    Array array(ctx_, uri, TILEDB_WRITE);
    Query query(ctx_, array);
    query.set_layout(TILEDB_UNORDERED);
    query.set_data_buffer("x", xs);
    query.set_data_buffer("y", ys);
    query.set_data_buffer("a", values);
    query.submit();
    query.finalize();
    array.close();
  }

  void write_metadata(
      const std::string& uri,
      const std::string& key,
      const std::vector<int32_t>& values) const {
    Array array(ctx_, uri, TILEDB_WRITE);
    array.put_metadata(key, TILEDB_INT32, values.size(), values.data());
    array.close();
  }

  std::vector<int32_t> read_metadata(
      const std::string& uri, const std::string& key) const {
    Array array(ctx_, uri, TILEDB_READ);
    tiledb_datatype_t type = TILEDB_ANY;
    uint32_t value_num = 0;
    const void* value = nullptr;
    array.get_metadata(key, &type, &value_num, &value);

    REQUIRE(type == TILEDB_INT32);
    const auto* ptr = static_cast<const int32_t*>(value);
    auto result = std::vector<int32_t>(ptr, ptr + value_num);
    array.close();
    return result;
  }

  Config config_;
  Context ctx_;
  std::string teamspace_id_;
};

}  // namespace

TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi registered dense arrays round-trip through create and "
    "read",
    "[cppapi][tile][dense]") {
  const auto uri = array_uri("dense");
  auto data = make_random_int_data(kSeedBase + 1, 12);

  create_dense_array(uri);

  write_dense_array(uri, data);
  CHECK(read_dense_array(uri) == data);
}

TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi sparse read round-trips",
    "[cppapi][tile][sparse]") {
  const auto uri = array_uri("sparse");
  auto values = make_random_float_data(kSeedBase + 2, 5);
  std::vector<int32_t> xs = {2, 4, 6, 8, 10};
  std::vector<int32_t> ys = {1, 3, 5, 7, 9};

  create_sparse_array(uri);
  write_sparse_array(uri, xs, ys, values);

  Array array(ctx_, uri, TILEDB_READ);
  Query query(ctx_, array);
  query.set_layout(TILEDB_UNORDERED);

  std::vector<int32_t> out_x(5);
  std::vector<int32_t> out_y(5);
  std::vector<float> out_values(5);
  query.set_data_buffer("x", out_x);
  query.set_data_buffer("y", out_y);
  query.set_data_buffer("a", out_values);
  query.submit();
  array.close();

  CHECK(out_x == xs);
  CHECK(out_y == ys);
  CHECK(out_values == values);
}

TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi metadata read round-trips",
    "[cppapi][tile][metadata]") {
  const auto uri = array_uri("metadata");
  auto data = make_random_int_data(kSeedBase + 3, 12);
  const auto metadata = make_random_int_data(kSeedBase + 4, 4);

  create_dense_array(uri);
  write_dense_array(uri, data);
  write_metadata(uri, "seeded-metadata", metadata);

  CHECK(read_metadata(uri, "seeded-metadata") == metadata);
}

TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi multipart dense writes round-trip",
    "[cppapi][tile][multipart]") {
  const auto uri = array_uri("multipart-dense");
  const DenseArrayShape shape{1536, 1024, 256, 256};
  auto data = make_random_int_data(kSeedBase + 5, shape.cell_num());

  create_dense_array(uri, shape);
  write_dense_array(uri, data);

  CHECK(read_dense_array(uri, shape) == data);
}

// Verifies `tiledb::Group::create` registers the group server-side
// under `/api/v1/groups/{base}` (not `/api/v1/tiles/{base}`) by
// listing resources via a group-typed client and asserting the
// group's base shows up. The end-to-end `Group::open(READ)`
// round-trip is covered separately in `[group][open]`.
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi group create registers under /api/v1/groups",
    "[cppapi][tile][group][create]") {
  const auto uri = group_uri("phase1-create");

  tiledb::Group::create(group_ctx(), uri);

  // Verify by listing groups directly via the catalog client.
  auto group_client = make_tile_ai_client();
  auto resources = group_client.list_resources(tiledb::sm::EntityType::Group);

  // The URI's path component (everything after `tile://`) is the server
  // `base`. Match by suffix on `name` to avoid coupling the test to the
  // run_id / teamspace embedded in the full base.
  const auto expected_suffix =
      std::string("/") + test_run_id() + "-grp-phase1-create";
  bool found = false;
  for (const auto& r : resources) {
    if (r.base.size() >= expected_suffix.size() &&
        r.base.compare(
            r.base.size() - expected_suffix.size(),
            expected_suffix.size(),
            expected_suffix) == 0) {
      found = true;
      break;
    }
  }
  INFO("expected group base ending in '" << expected_suffix << "'");
  INFO("got " << resources.size() << " group(s) from /api/v1/groups");
  CHECK(found);

  // Negative assertion: the group must NOT show up under /api/v1/tiles
  // (would mean the routing fell through to the array path).
  auto array_client = make_tile_ai_client();
  auto array_resources =
      array_client.list_resources(tiledb::sm::EntityType::Array);
  bool spurious_in_tiles = false;
  for (const auto& r : array_resources) {
    if (r.base.size() >= expected_suffix.size() &&
        r.base.compare(
            r.base.size() - expected_suffix.size(),
            expected_suffix.size(),
            expected_suffix) == 0) {
      spurious_in_tiles = true;
      break;
    }
  }
  CHECK_FALSE(spurious_in_tiles);
}

// Full Group::create → add_member → close → reopen round-trip.
// Verifies both the C++ Group API surface (member is visible after
// reopen) AND the server-side authority (member appears in
// /api/v1/groups/.../members via direct client.list_members()).
//
// Covers the C++-developer critical path; nested groups,
// relative-path members, and exotic type discrimination are
// delegated to the Python harness in
// `examples/tiledb-presigned/tests/test_groups.py`.
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi group with absolute array member round-trips through "
    "close+reopen",
    "[cppapi][tile][group][members]") {
  // First materialize an array to use as the group's member.
  const auto member_array_uri = array_uri("phase2-group-member");
  auto member_data = make_random_int_data(kSeedBase + 10, 12);
  create_dense_array(member_array_uri);
  write_dense_array(member_array_uri, member_data);

  const auto group_uri_str = group_uri("phase2-with-member");
  tiledb::Group::create(group_ctx(), group_uri_str);

  // WRITE: open, add member, close. The close fires
  // FilesystemBase::commit_group_writes via the new group.cc hook,
  // which translates to a PUT /api/v1/groups/.../members.
  // The Context must outlive the Group — `tiledb::Group` stores
  // `std::reference_wrapper<const Context>`, so a temporary from
  // `group_ctx()` would dangle past the constructor's full expression.
  //
  // The explicit `TILEDB_ARRAY` type avoids `Group::add_member`'s
  // auto-detect path, which under a group-typed Context would route
  // the array URI's type-probe through the group endpoint and 404.
  // Self-describing tile-id URIs remove that limitation; until then,
  // callers in a group context pass the type explicitly when adding
  // cross-type members (which they typically know — they just
  // materialised the array).
  {
    auto gctx = group_ctx();
    tiledb::Group g(gctx, group_uri_str, TILEDB_WRITE);
    g.add_member(
        member_array_uri,
        /*relative=*/false,
        std::string("my_array"),
        TILEDB_ARRAY);
    g.close();
  }

  // READ: reopen and verify the member is visible via the C++ Group API.
  {
    auto gctx = group_ctx();
    tiledb::Group g(gctx, group_uri_str, TILEDB_READ);
    REQUIRE(g.member_count() == 1);
    auto m = g.member(uint64_t{0});
    CHECK(m.uri() == member_array_uri);
    REQUIRE(m.name().has_value());
    CHECK(*m.name() == "my_array");
    g.close();
  }

  // Server-side cross-check: the member must show up in the
  // /api/v1/groups/.../members list, which is the source of truth.
  {
    auto group_client = make_tile_ai_client();

    // Strip "tile://" to get the server-side base.
    REQUIRE(group_uri_str.rfind("tile://", 0) == 0);
    const std::string base = group_uri_str.substr(7);
    auto members = group_client.list_members(base);

    REQUIRE(members.size() == 1);
    CHECK(members[0].uri == member_array_uri);
    CHECK(members[0].name == "my_array");
    CHECK(members[0].type == "array");
    CHECK(members[0].relative == false);
  }

  // End-to-end: the URI returned by `Group::member` must be a usable
  // tile:// array URI. Open it and read back the data we seeded
  // before adding it as a member — proves the group→member→array
  // handoff isn't just metadata.
  {
    std::string member_uri_from_group;
    {
      auto gctx = group_ctx();
      tiledb::Group g(gctx, group_uri_str, TILEDB_READ);
      member_uri_from_group = g.member(uint64_t{0}).uri();
      g.close();
    }
    REQUIRE(member_uri_from_group == member_array_uri);
    CHECK(read_dense_array(member_uri_from_group) == member_data);
  }
}

// The tile.ai backend resolves members through the group's own catalog
// prefix, so the only members it can address are relative paths and
// tile:// cross-references. Any other absolute URI is rejected at add
// time with the policy error rather than surfacing later as an
// unresolvable open.
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi group rejects absolute non-tile members",
    "[cppapi][tile][group][members]") {
  const auto group_uri_str = group_uri("abs-member-reject");
  tiledb::Group::create(group_ctx(), group_uri_str);

  auto gctx = group_ctx();
  tiledb::Group g(gctx, group_uri_str, TILEDB_WRITE);
  REQUIRE_THROWS_WITH(
      g.add_member(
          "s3://external-bucket/relocated/ms",
          /*relative=*/false,
          std::string("ms"),
          TILEDB_GROUP),
      Catch::Matchers::ContainsSubstring("Absolute group member") &&
          Catch::Matchers::ContainsSubstring(
              "must be relative to the group prefix"));
  // Anything flagged relative that is not a plain path under the prefix
  // (schemes, rooted paths, dot segments) would join into a garbage path,
  // alias the group itself, or escape it at resolution time; rejected.
  const auto plain_path =
      Catch::Matchers::ContainsSubstring("must be a plain path");
  for (const char* uri :
       {"s3://external-bucket/relocated/ms",
        "/mnt/data/ms",
        "../other-group/ms",
        "ms/..",
        "."}) {
    REQUIRE_THROWS_WITH(
        g.add_member(uri, /*relative=*/true, std::string("bad"), TILEDB_GROUP),
        plain_path);
  }

  // Relative members remain legal on the same handle after the rejections,
  // and none of the rejected members leaked into the registration.
  g.add_member("sub", /*relative=*/true, std::string("sub"), TILEDB_GROUP);
  g.close();
  {
    tiledb::Group r(gctx, group_uri_str, TILEDB_READ);
    CHECK(r.member_count() == 1);
    r.close();
  }
}

// Group metadata write/read round-trip. Exercises
// `Group::close_for_writes` → `unsafe_metadata()->store(...)` against
// the tile:// backend (a `__meta/...` write under the group's S3
// prefix), and the metadata reload path on `Group::open(READ)`.
// Distinct from `[group][members]` which only round-trips the
// server-side member ledger — group metadata is a real payload write
// through the storage backend.
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi group metadata round-trips through close+reopen",
    "[cppapi][tile][group][metadata]") {
  const auto group_uri_str = group_uri("phase2-metadata");
  tiledb::Group::create(group_ctx(), group_uri_str);

  const auto values = make_random_int_data(kSeedBase + 11, 4);

  // WRITE: open, set metadata, close. close_for_writes flushes the
  // group's metadata blob via the VFS-backed Metadata::store path.
  {
    auto gctx = group_ctx();
    tiledb::Group g(gctx, group_uri_str, TILEDB_WRITE);
    g.put_metadata(
        "seeded-group-metadata",
        TILEDB_INT32,
        static_cast<uint32_t>(values.size()),
        values.data());
    g.close();
  }

  // READ: reopen and verify the value round-trips byte-for-byte.
  {
    auto gctx = group_ctx();
    tiledb::Group g(gctx, group_uri_str, TILEDB_READ);
    tiledb_datatype_t type = TILEDB_ANY;
    uint32_t value_num = 0;
    const void* value = nullptr;
    g.get_metadata("seeded-group-metadata", &type, &value_num, &value);

    REQUIRE(type == TILEDB_INT32);
    REQUIRE(value_num == values.size());
    REQUIRE(value != nullptr);
    const auto* ptr = static_cast<const int32_t*>(value);
    auto roundtrip = std::vector<int32_t>(ptr, ptr + value_num);
    CHECK(roundtrip == values);
    g.close();
  }
}

// `parse_uri` behavior. No network calls; pure disambiguation logic
// over the URI string. Path structure alone is the disambiguator:
// zero slashes after the scheme prefix → id-form, any slash →
// hierarchical, regardless of how the first segment looks.
TEST_CASE(
    "C++ API: TileAi::parse_uri uses path structure to disambiguate "
    "id-form vs hierarchical",
    "[cppapi][tile][parse_uri]") {
  using ParsedUri = tiledb::sm::TileAi::ParsedUri;

  SECTION("zero slashes → id_form, opaque id captured verbatim") {
    auto p =
        tiledb::sm::TileAi::parse_uri(tiledb::sm::URI("tile://tiledb-abc"));
    CHECK(p.id_form == true);
    CHECK(p.id == "tiledb-abc");
    CHECK(p.array_id.empty());
    CHECK(p.relative_key.empty());
  }

  SECTION("zero slashes, no tiledb- prefix → still id-form (no parser gate)") {
    // Existence is the catalog's job (`lookup_resource` 404s if `foo`
    // isn't a real id). The parser only cares about path shape.
    auto p = tiledb::sm::TileAi::parse_uri(tiledb::sm::URI("tile://foo"));
    CHECK(p.id_form == true);
    CHECK(p.id == "foo");
  }

  SECTION("one slash → hierarchical, even when first segment starts tiledb-") {
    // Regression guard for the rejected "tiledb-prefix-as-id-marker" rule:
    // teamspace IDs share the same `{label}-{uuid}` shape as tile IDs
    // (a teamspace named "TileDB" gets ID `tiledb-{uuid}`), so this URI
    // is a perfectly valid hierarchical reference.
    auto p = tiledb::sm::TileAi::parse_uri(
        tiledb::sm::URI("tile://tiledb-abc/my-array"));
    CHECK(p.id_form == false);
    CHECK(p.array_id == "tiledb-abc/my-array");
    CHECK(p.relative_key.empty());
    CHECK(p.id.empty());
  }

  SECTION("two slashes → hierarchical with relative key") {
    auto p = tiledb::sm::TileAi::parse_uri(
        tiledb::sm::URI("tile://teamspace/my-array/__schema/0.tdb"));
    CHECK(p.id_form == false);
    CHECK(p.array_id == "teamspace/my-array");
    CHECK(p.relative_key == "__schema/0.tdb");
  }

  SECTION(
      "three+ slashes → relative_key captures the full tail past slash #2") {
    // The parser doesn't cap relative key depth; everything after the
    // second slash is preserved verbatim so the libtiledb caller can
    // address fragment objects nested arbitrarily deep
    // (e.g. `__fragments/<frag-uuid>/a0.tdb` is the common shape).
    auto p = tiledb::sm::TileAi::parse_uri(tiledb::sm::URI(
        "tile://teamspace/my-array/__fragments/abc-uuid/a0.tdb"));
    CHECK(p.id_form == false);
    CHECK(p.array_id == "teamspace/my-array");
    CHECK(p.relative_key == "__fragments/abc-uuid/a0.tdb");
  }

  SECTION("empty payload → throws (no resource segment to look up)") {
    // `tile://` with nothing after carries no handle at all. Fail at the
    // parser rather than letting it propagate to the catalog as a
    // guaranteed 404 — a round-trip with no possible upside.
    CHECK_THROWS_AS(
        tiledb::sm::TileAi::parse_uri(tiledb::sm::URI("tile://")),
        tiledb::sm::TileAiException);
  }

  SECTION("trailing slash is normalized away (id-form)") {
    // TileDB appends `/` to URIs when treating them as directories
    // (e.g. `Array::open` does `is_dir(uri + "/")`). The trailing
    // slash doesn't change the resource identity; without normalization
    // `tile://{id}/` parses as hierarchical with an empty `name` and
    // the catalog client 308s on the resulting `…/{id}/` URL.
    auto p =
        tiledb::sm::TileAi::parse_uri(tiledb::sm::URI("tile://tiledb-abc/"));
    CHECK(p.id_form == true);
    CHECK(p.id == "tiledb-abc");
    CHECK(p.array_id.empty());
    CHECK(p.relative_key.empty());
  }

  SECTION("trailing slash is normalized away (hierarchical, no rel)") {
    auto p = tiledb::sm::TileAi::parse_uri(
        tiledb::sm::URI("tile://teamspace/my-array/"));
    CHECK(p.id_form == false);
    CHECK(p.array_id == "teamspace/my-array");
    CHECK(p.relative_key.empty());
  }

  SECTION("trailing slash is normalized away (hierarchical with rel)") {
    auto p = tiledb::sm::TileAi::parse_uri(
        tiledb::sm::URI("tile://teamspace/my-array/__schema/"));
    CHECK(p.id_form == false);
    CHECK(p.array_id == "teamspace/my-array");
    CHECK(p.relative_key == "__schema");
  }

  SECTION("only-slash payload (tile:///) → throws") {
    // After trailing-slash strip the remainder is empty; equivalent to
    // bare `tile://`.
    CHECK_THROWS_AS(
        tiledb::sm::TileAi::parse_uri(tiledb::sm::URI("tile:///")),
        tiledb::sm::TileAiException);
  }

  SECTION("unsupported scheme throws") {
    CHECK_THROWS_AS(
        tiledb::sm::TileAi::parse_uri(tiledb::sm::URI("s3://bucket/key")),
        tiledb::sm::TileAiException);
  }

  // Suppress unused-type warning on builds that elide the SECTIONs.
  (void)ParsedUri{};
}

// End-to-end coverage for the two URI forms the libtiledb C++ caller
// can hand the VFS that aren't already exercised transitively by the
// hierarchical-no-key fixture tests above:
//
//   1. Id-form `tile://{id}` — used when a caller persists a stable
//      server id rather than the human path. The catalog round-trips
//      via `lookup_resource(id)` and the operation runs against the
//      resolved `(teamspace, name)`.
//   2. Hierarchical-with-relative-key `tile://{ts}/{name}/{rel}` — used
//      whenever a caller addresses an object inside an array (schema,
//      fragments, metadata). Exercised constantly inside libtiledb
//      reads but never as a direct user-facing URI in the tests above.
//
// Top-level hierarchical (`tile://{ts}/{name}`) is the dominant form
// and is covered by every existing test in this file.
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi round-trips through an id-form tile:// URI",
    "[cppapi][tile][uri_forms]") {
  // Seed an array via the hierarchical helper so the catalog has a
  // known resource. Write data so the round-trip via id-form actually
  // proves data path works, not just metadata.
  const auto hierarchical = array_uri("uri-form-id");
  auto data = make_random_int_data(kSeedBase + 90, 12);
  create_dense_array(hierarchical);
  write_dense_array(hierarchical, data);

  // Scrape the server-generated id by matching `base`. Same pattern as
  // the [lookup_resource] test below — kept inline so this test reads
  // standalone.
  auto client = make_tile_ai_client();
  REQUIRE(hierarchical.rfind("tile://", 0) == 0);
  const std::string expected_base = hierarchical.substr(7);
  std::string id;
  for (const auto& r : client.list_resources(tiledb::sm::EntityType::Array)) {
    if (r.base == expected_base) {
      id = r.id;
      break;
    }
  }
  REQUIRE_FALSE(id.empty());

  // Open the array via `tile://{id}` and read it back. TileDB will
  // internally append `/`, `/__schema/...`, `/__fragments/...` etc. to
  // the user-supplied URI when probing the array layout — every one of
  // those sub-URIs has to make it through `parse_uri` +
  // `canonicalize_resource` and resolve back to the same (teamspace,
  // name) the user originally registered.
  const std::string id_uri = "tile://" + id;
  CHECK(read_dense_array(id_uri) == data);
}

TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi resolves tile://{ts}/{name}/{relative_key} URIs",
    "[cppapi][tile][uri_forms]") {
  // Every TileDB array on creation lays out `__schema/{schema-id}.tdb`
  // at the array's prefix. Listing `tile://{ts}/{name}/__schema`
  // through the VFS exercises the hierarchical-with-relative-key form
  // end-to-end: the parser routes `__schema` into `relative_key`,
  // canonicalisation looks up the array, and `ls_with_sizes` presigns
  // the listing against the server.
  const auto hierarchical = array_uri("uri-form-rel-key");
  create_dense_array(hierarchical);

  VFS vfs(ctx_);
  const std::string schema_uri = hierarchical + "/__schema";
  auto entries = vfs.ls(schema_uri);
  CHECK_FALSE(entries.empty());
}

// `lookup_resource` end-to-end. Seeds an array and a group via the
// existing fixture helpers, scrapes their server-generated ids out of
// `list_resources`, then verifies that `lookup_resource(id)` resolves
// each correctly and that an unknown id surfaces a 404-status
// TileAiException (which the C++ side can route on without
// parsing strings).
//
// Single TEST_CASE_METHOD with linear assertions (no SECTIONs) so
// `create_dense_array` runs exactly once — Catch2 re-enters the test
// body per leaf SECTION, which would 409 on the second `Array::create`.
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAiClient::lookup_resource resolves array/group ids",
    "[cppapi][tile][lookup_resource]") {
  // Materialize one array and one group so the catalog has known ids.
  const auto array_uri_str = array_uri("phase3-lookup-array");
  create_dense_array(array_uri_str);

  const auto group_uri_str = group_uri("phase3-lookup-group");
  tiledb::Group::create(group_ctx(), group_uri_str);

  // Find the seeded array's id by matching `base` in the array list.
  auto array_client = make_tile_ai_client();
  std::string array_id;
  {
    REQUIRE(array_uri_str.rfind("tile://", 0) == 0);
    const std::string expected_base = array_uri_str.substr(7);
    for (const auto& r :
         array_client.list_resources(tiledb::sm::EntityType::Array)) {
      if (r.base == expected_base) {
        array_id = r.id;
        break;
      }
    }
  }
  REQUIRE_FALSE(array_id.empty());

  // Same for the group; pass EntityType::Group to hit `/api/v1/groups`.
  auto group_client = make_tile_ai_client();
  std::string group_id;
  {
    REQUIRE(group_uri_str.rfind("tile://", 0) == 0);
    const std::string expected_base = group_uri_str.substr(7);
    for (const auto& r :
         group_client.list_resources(tiledb::sm::EntityType::Group)) {
      if (r.base == expected_base) {
        group_id = r.id;
        break;
      }
    }
  }
  REQUIRE_FALSE(group_id.empty());

  // Array lookup returns the correct hierarchical address and type.
  {
    auto loc = array_client.lookup_resource(array_id);
    CHECK(loc.id == array_id);
    CHECK(loc.type == "array");
    CHECK(loc.teamspace_id == teamspace_id_);
    CHECK(
        loc.name == array_uri_str.substr(
                        std::string("tile://" + teamspace_id_ + "/").size()));
    CHECK(loc.storage_uri.rfind("s3://", 0) == 0);
  }

  // Group lookup works through the array-typed client too — the client
  // tries /tiles/{id} first and falls back to /groups/{id} on 404, so
  // running this against the array client proves the fall-through path.
  {
    auto loc = array_client.lookup_resource(group_id);
    CHECK(loc.id == group_id);
    CHECK(loc.type == "group");
    CHECK(loc.teamspace_id == teamspace_id_);
    CHECK(
        loc.name == group_uri_str.substr(
                        std::string("tile://" + teamspace_id_ + "/").size()));
  }

  // Unknown id surfaces HTTP 404 — both with the `tiledb-` prefix and
  // without (existence is catalog-driven, no parser-level prefix check).
  for (const char* bogus :
       {"tiledb-doesnotexist-0000-0000-0000-000000", "foo"}) {
    try {
      (void)array_client.lookup_resource(bogus);
      FAIL(
          std::string("expected lookup_resource to throw on unknown id: ") +
          bogus);
    } catch (const tiledb::sm::TileAiException& e) {
      INFO("bogus id was: " << bogus);
      CHECK(e.http_status() == 404);
    }
  }
}

// End-to-end "lookup id → build hierarchical → open + read"
// round-trip. This is the documented usage pattern from
// `tile_ai.h`'s ParsedUri docs: id-form URIs are resource
// handles, not full substitutes for hierarchical URIs in
// `Array::open`. Also confirms the resolver stamps the correct type
// from the catalog — the array is opened via the fixture's plain
// `ctx_` with no per-context type configuration.
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: lookup_resource → hierarchical URI → Array::open round-trip",
    "[cppapi][tile][id_form][open]") {
  const auto array_uri_str = array_uri("phase3-id-form-roundtrip");
  auto data = make_random_int_data(kSeedBase + 12, 12);
  create_dense_array(array_uri_str);
  write_dense_array(array_uri_str, data);

  // Pretend the caller only knows the id. (Realistic SDK pattern: an
  // app records the id at create time, drops the hierarchical
  // address, and resolves on demand later.)
  auto client = make_tile_ai_client();
  REQUIRE(array_uri_str.rfind("tile://", 0) == 0);
  const std::string expected_base = array_uri_str.substr(7);
  std::string id;
  for (const auto& r : client.list_resources(tiledb::sm::EntityType::Array)) {
    if (r.base == expected_base) {
      id = r.id;
      break;
    }
  }
  REQUIRE_FALSE(id.empty());

  // Resolve, build hierarchical URI, open + read. ctx_ is the
  // fixture's plain default context; no per-context type override.
  auto loc = client.lookup_resource(id);
  REQUIRE(loc.type == "array");
  const std::string hierarchical =
      "tile://" + loc.teamspace_id + "/" + loc.name;
  CHECK(hierarchical == array_uri_str);
  CHECK(read_dense_array(hierarchical) == data);
}

// Group operations route without any per-context config. Reads
// resolve type via the catalog (lookup_resource_by_name); creates
// route via the call site (Group::create → create_group_dir hook).
// This test exercises the full group create → add_member → close →
// reopen cycle using the fixture's plain `ctx_` — regression guard
// for the dispatch staying out of the config layer.
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: group lifecycle works without per-context tile.* config",
    "[cppapi][tile][group][no_config]") {
  // Materialize an array via the fixture's default context to use
  // as the group's member.
  const auto member_array_uri = array_uri("phase3-no-config-member");
  auto member_data = make_random_int_data(kSeedBase + 13, 12);
  create_dense_array(member_array_uri);
  write_dense_array(member_array_uri, member_data);

  const auto group_uri_str = group_uri("phase3-no-config-group");

  // Plain `ctx_` — no per-context type override. The
  // `create_group_dir` virtual signals "this is a group create" at the
  // call site, so the backend POSTs to /api/v1/groups instead of
  // /api/v1/tiles.
  tiledb::Group::create(ctx_, group_uri_str);

  // WRITE: add a member, close. Same plain ctx_.
  {
    tiledb::Group g(ctx_, group_uri_str, TILEDB_WRITE);
    g.add_member(
        member_array_uri,
        /*relative=*/false,
        std::string("my_array"),
        TILEDB_ARRAY);
    g.close();
  }

  // READ: reopen, verify member is visible. Server-resolve via
  // lookup_resource_by_name during canonicalize_resource is what
  // makes this work — the backend learns the type from the catalog,
  // not from a config knob.
  {
    tiledb::Group g(ctx_, group_uri_str, TILEDB_READ);
    REQUIRE(g.member_count() == 1);
    auto m = g.member(uint64_t{0});
    CHECK(m.uri() == member_array_uri);
    REQUIRE(m.name().has_value());
    CHECK(*m.name() == "my_array");
    g.close();
  }
}

// `Group::create` followed by `Group::open(READ)` end-to-end —
// confirms a fresh empty group is fully usable through the public C++
// API. `[group][create]` only verifies registration via
// `list_resources`; this exercises the components-fetch path through
// `list_group_components` on open.
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi group create+open round-trips end-to-end",
    "[cppapi][tile][group][open]") {
  const auto group_uri_str = group_uri("phase1-finalize-open");
  tiledb::Group::create(group_ctx(), group_uri_str);

  // Open for read: exercises the components-fetch path through
  // list_group_components. Should succeed even on a fresh empty group.
  // Context must outlive the Group (see [group][members] note above).
  auto gctx = group_ctx();
  tiledb::Group g(gctx, group_uri_str, TILEDB_READ);
  CHECK(g.member_count() == 0);
  g.close();
}

// Group-as-member: parent registers another group as a member with
// TILEDB_GROUP type. Validates that the member type round-trips
// (libtiledb → /api/v1/groups/.../members → list_members) and that
// the URI returned by `Group::member` is itself openable as a Group.
// This is the primitive the tile-ai `tiledb_group` tile relies on for
// nested-group views (`list_tiledb_group_members` + `resolveGroupPath`).
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi group registers another group as a member",
    "[cppapi][tile][group][nested]") {
  // Two top-level groups; one becomes a member of the other.
  const auto child_group_uri = group_uri("nested-child");
  const auto parent_group_uri = group_uri("nested-parent");
  tiledb::Group::create(group_ctx(), child_group_uri);
  tiledb::Group::create(group_ctx(), parent_group_uri);

  // WRITE: add the child group as a member of the parent.
  // Pass TILEDB_GROUP explicitly: the auto-detect path under a
  // group-typed context would type-probe via /api/v1/groups, which
  // would happen to succeed here, but the tile feature always
  // supplies the type at the call site, so mirror that.
  {
    auto gctx = group_ctx();
    tiledb::Group g(gctx, parent_group_uri, TILEDB_WRITE);
    g.add_member(
        child_group_uri,
        /*relative=*/false,
        std::string("child"),
        TILEDB_GROUP);
    g.close();
  }

  // READ: parent surfaces the child as a Group-typed member.
  {
    auto gctx = group_ctx();
    tiledb::Group g(gctx, parent_group_uri, TILEDB_READ);
    REQUIRE(g.member_count() == 1);
    auto m = g.member(uint64_t{0});
    CHECK(m.type() == tiledb::Object::Type::Group);
    CHECK(m.uri() == child_group_uri);
    REQUIRE(m.name().has_value());
    CHECK(*m.name() == "child");
    g.close();
  }

  // Server-side cross-check: the membership row carries type="group".
  {
    auto group_client = make_tile_ai_client();

    REQUIRE(parent_group_uri.rfind("tile://", 0) == 0);
    const std::string base = parent_group_uri.substr(7);
    auto members = group_client.list_members(base);

    REQUIRE(members.size() == 1);
    CHECK(members[0].uri == child_group_uri);
    CHECK(members[0].name == "child");
    CHECK(members[0].type == "group");
    CHECK(members[0].relative == false);
  }

  // The URI returned by `Group::member` must itself be openable as a
  // Group — proves the parent→child hand-off isn't just metadata.
  {
    std::string child_uri_from_parent;
    {
      auto gctx = group_ctx();
      tiledb::Group p(gctx, parent_group_uri, TILEDB_READ);
      child_uri_from_parent = p.member(uint64_t{0}).uri();
      p.close();
    }
    REQUIRE(child_uri_from_parent == child_group_uri);

    auto gctx = group_ctx();
    tiledb::Group child(gctx, child_uri_from_parent, TILEDB_READ);
    CHECK(child.member_count() == 0);
    child.close();
  }
}

// Two-level nesting: grandparent → parent → grandchild via member
// references. Mirrors `resolveGroupPath`'s walk: at each level take
// the group-typed member with a given name and recurse. Catches any
// regression where a depth-2 traversal stops working even though
// depth-1 does (e.g. open-as-read of a group-typed member URI hits
// a wrong dispatch branch).
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi group two-level nested member traversal",
    "[cppapi][tile][group][nested][deep]") {
  const auto grandparent_uri = group_uri("nested-gp");
  const auto parent_uri = group_uri("nested-p");
  const auto child_uri = group_uri("nested-c");
  tiledb::Group::create(group_ctx(), grandparent_uri);
  tiledb::Group::create(group_ctx(), parent_uri);
  tiledb::Group::create(group_ctx(), child_uri);

  // Wire up the chain: parent has child as "child"; grandparent has
  // parent as "parent". Each `close` flushes one PUT /members.
  {
    auto gctx = group_ctx();
    tiledb::Group p(gctx, parent_uri, TILEDB_WRITE);
    p.add_member(
        child_uri, /*relative=*/false, std::string("child"), TILEDB_GROUP);
    p.close();
  }
  {
    auto gctx = group_ctx();
    tiledb::Group gp(gctx, grandparent_uri, TILEDB_WRITE);
    gp.add_member(
        parent_uri, /*relative=*/false, std::string("parent"), TILEDB_GROUP);
    gp.close();
  }

  // Walk: grandparent → parent (by name) → child (by name).
  std::string resolved_parent_uri;
  std::string resolved_child_uri;
  {
    auto gctx = group_ctx();
    tiledb::Group gp(gctx, grandparent_uri, TILEDB_READ);
    REQUIRE(gp.member_count() == 1);
    auto step1 = gp.member(std::string("parent"));
    CHECK(step1.type() == tiledb::Object::Type::Group);
    resolved_parent_uri = step1.uri();
    gp.close();
  }
  REQUIRE(resolved_parent_uri == parent_uri);

  {
    auto gctx = group_ctx();
    tiledb::Group p(gctx, resolved_parent_uri, TILEDB_READ);
    REQUIRE(p.member_count() == 1);
    auto step2 = p.member(std::string("child"));
    CHECK(step2.type() == tiledb::Object::Type::Group);
    resolved_child_uri = step2.uri();
    p.close();
  }
  REQUIRE(resolved_child_uri == child_uri);

  // The walked URI is a fully-functional group, not just a string.
  {
    auto gctx = group_ctx();
    tiledb::Group c(gctx, resolved_child_uri, TILEDB_READ);
    CHECK(c.member_count() == 0);
    c.close();
  }
}

// Mixed-type members: one group containing both array and group
// members. The tile UI distinguishes them by `type` to render
// different icons / row affordances and to gate which children are
// traversable. Regression guard: any code path that flattens type
// to a single value on the wire (or in libtiledb's internal model)
// would break the tile's Members tab.
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi group with mixed array and group members",
    "[cppapi][tile][group][nested][mixed]") {
  // Materialize two arrays and one sub-group to register as members.
  const auto array_a_uri = array_uri("mixed-array-a");
  const auto array_b_uri = array_uri("mixed-array-b");
  const auto sub_group_uri = group_uri("mixed-sub");
  create_dense_array(array_a_uri);
  create_dense_array(array_b_uri);
  tiledb::Group::create(group_ctx(), sub_group_uri);

  // Parent group that hosts all three.
  const auto parent_uri = group_uri("mixed-parent");
  tiledb::Group::create(group_ctx(), parent_uri);

  // WRITE: 2 arrays + 1 group, each with an explicit name.
  {
    auto gctx = group_ctx();
    tiledb::Group g(gctx, parent_uri, TILEDB_WRITE);
    g.add_member(
        array_a_uri, /*relative=*/false, std::string("data_a"), TILEDB_ARRAY);
    g.add_member(
        array_b_uri, /*relative=*/false, std::string("data_b"), TILEDB_ARRAY);
    g.add_member(
        sub_group_uri, /*relative=*/false, std::string("sub"), TILEDB_GROUP);
    g.close();
  }

  // READ: each member resolves to the right type via name lookup.
  {
    auto gctx = group_ctx();
    tiledb::Group g(gctx, parent_uri, TILEDB_READ);
    REQUIRE(g.member_count() == 3);

    auto a = g.member(std::string("data_a"));
    CHECK(a.type() == tiledb::Object::Type::Array);
    CHECK(a.uri() == array_a_uri);

    auto b = g.member(std::string("data_b"));
    CHECK(b.type() == tiledb::Object::Type::Array);
    CHECK(b.uri() == array_b_uri);

    auto s = g.member(std::string("sub"));
    CHECK(s.type() == tiledb::Object::Type::Group);
    CHECK(s.uri() == sub_group_uri);

    g.close();
  }

  // Server-side cross-check: types are preserved per-row in the DB.
  {
    auto group_client = make_tile_ai_client();

    REQUIRE(parent_uri.rfind("tile://", 0) == 0);
    auto members = group_client.list_members(parent_uri.substr(7));
    REQUIRE(members.size() == 3);

    // Build a name → entry map so we can assert without depending on
    // server-side ordering (which is `member_key`-sorted, but the
    // test shouldn't care about that detail).
    auto by_name = std::map<std::string, tiledb::sm::MemberEntry>{};
    for (const auto& m : members) {
      by_name[m.name] = m;
    }
    REQUIRE(by_name.count("data_a") == 1);
    REQUIRE(by_name.count("data_b") == 1);
    REQUIRE(by_name.count("sub") == 1);
    CHECK(by_name["data_a"].type == "array");
    CHECK(by_name["data_b"].type == "array");
    CHECK(by_name["sub"].type == "group");
  }
}

// Remove-member round-trip. The tile's `remove_tiledb_group_member`
// tool unregisters a member without touching the underlying data,
// so two invariants must hold: (1) the parent's member list shrinks
// by exactly the removed entry, and (2) the unlinked target remains
// independently openable. Both are verified here.
TEST_CASE_METHOD(
    TileAiCppApiFx,
    "C++ API: TileAi group remove_member unlinks without deleting target",
    "[cppapi][tile][group][nested][remove]") {
  // Two members: one we'll keep, one we'll drop.
  const auto keep_array_uri = array_uri("remove-keep");
  const auto drop_array_uri = array_uri("remove-drop");
  create_dense_array(keep_array_uri);
  create_dense_array(drop_array_uri);

  const auto parent_uri = group_uri("remove-parent");
  tiledb::Group::create(group_ctx(), parent_uri);

  // WRITE: register both arrays.
  {
    auto gctx = group_ctx();
    tiledb::Group g(gctx, parent_uri, TILEDB_WRITE);
    g.add_member(
        keep_array_uri,
        /*relative=*/false,
        std::string("keep"),
        TILEDB_ARRAY);
    g.add_member(
        drop_array_uri,
        /*relative=*/false,
        std::string("drop"),
        TILEDB_ARRAY);
    g.close();
  }

  // Sanity: parent has both before the remove.
  {
    auto gctx = group_ctx();
    tiledb::Group g(gctx, parent_uri, TILEDB_READ);
    CHECK(g.member_count() == 2);
    g.close();
  }

  // WRITE: drop the "drop" member by name.
  {
    auto gctx = group_ctx();
    tiledb::Group g(gctx, parent_uri, TILEDB_WRITE);
    g.remove_member("drop");
    g.close();
  }

  // READ: only "keep" survives in the parent's member list.
  {
    auto gctx = group_ctx();
    tiledb::Group g(gctx, parent_uri, TILEDB_READ);
    REQUIRE(g.member_count() == 1);
    auto m = g.member(uint64_t{0});
    REQUIRE(m.name().has_value());
    CHECK(*m.name() == "keep");
    CHECK(m.uri() == keep_array_uri);
    g.close();
  }

  // Server-side: members table has one row, with name="keep".
  {
    auto group_client = make_tile_ai_client();

    REQUIRE(parent_uri.rfind("tile://", 0) == 0);
    auto members = group_client.list_members(parent_uri.substr(7));
    REQUIRE(members.size() == 1);
    CHECK(members[0].name == "keep");
  }

  // The unlinked array is still a real, queryable array. The tile's
  // "remove member" path is not "delete data" — that's a separate
  // destructive flow (`deleteTileDBData` / vfs.remove_dir).
  {
    Array a(ctx_, drop_array_uri, TILEDB_READ);
    CHECK(a.is_open());
    a.close();
  }
}

// ── teamspace name resolution ─────────────────────────────────────────────

TEST_CASE(
    "TileAiClient: name-based teamspace reference resolves server-side",
    "[tile_ai][teamspace]") {
  SmConfig config;
  auto api_url = config_string(config, "vfs.tile.server_url");
  auto api_key = config_string(config, "vfs.tile.api_key");
  if (api_url.empty() || api_key.empty()) {
    SKIP("tile:// C++ tests require vfs.tile.{server_url,api_key}");
  }

  // Look up the canonical teamspace id by name so we can both (a) pass
  // the human-readable name in the URI segment (verifying the
  // server-side `resolveTeamspaceRef` name fallback) and (b) verify the
  // server responds with the canonical id, proving the lookup succeeded.
  auto lookup = make_tile_ai_client();
  auto teamspaces = lookup.list_teamspaces();
  REQUIRE(!teamspaces.empty());
  const auto target_name = env_string("TILEDB_TEST_TILE_TEAMSPACE_NAME")
                               .value_or(TileAiCppApiFx::kDefaultTeamspaceName);
  std::string expected_id;
  for (const auto& ts : teamspaces) {
    if (ts.name == target_name) {
      REQUIRE(expected_id.empty());  // assume name is unique in this workspace
      expected_id = ts.id;
    }
  }
  REQUIRE(!expected_id.empty());

  tiledb::sm::TileAiClient client(api_url, api_key);

  // Use the NAME (not the id) as the URI's teamspace segment. Server-side
  // resolution should look it up by name and return the canonical id.
  const std::string base = target_name + "/" + test_run_id() + "-by-name";
  tiledb::sm::TileInfo created;
  REQUIRE_NOTHROW(
      created = client.create_resource(tiledb::sm::EntityType::Array, base));
  REQUIRE(!created.id.empty());

  // The server's response and any subsequent lookups should report the
  // canonical teamspace_id (the id PK), not the name we sent. This
  // confirms the name fallback in resolveTeamspaceRef found the right row.
  auto loc = lookup.lookup_resource(created.id);
  CHECK(loc.teamspace_id == expected_id);
}

// ── tile:// credential resolution (no server required) ────────────────────
//
// These tests directly exercise `tiledb::sm::TileAi::init()` to verify
// the credential lookup chain.  They are deterministic, independent of the
// host's env state (all relevant vars are masked at the top), and run in
// every CI configuration (no SKIP).  A network call is never made — init()
// only stores the resolved server_url/api_key into the backend instance.
//
// The integration tests above (gated on TILEDB_TEST_TILE_*) cover the full
// auth round-trip end-to-end against a live tile:// service.

TEST_CASE(
    "C++ API: tile:// backend credential resolution", "[cppapi][tile][env]") {
  using tiledb::sm::TileAi;
  using tiledb::sm::TileAiException;
  // Disambiguate from the public `tiledb::Config` brought in by the
  // file-level `using namespace tiledb;` at the top.
  using SmConfig = tiledb::sm::Config;

  // Mask any inherited credential env vars so each SECTION starts from a
  // clean slate regardless of host environment.
  auto _vfs_tile_url = setenv_local("TILEDB_VFS_TILE_SERVER_URL", "");
  auto _vfs_tile_key = setenv_local("TILEDB_VFS_TILE_API_KEY", "");
  auto _tile_url = setenv_local("TILE_API_URL", "");
  auto _tile_key = setenv_local("TILE_API_KEY", "");

  SECTION("init throws when no credentials are set anywhere") {
    TileAi backend;
    SmConfig config;
    REQUIRE_THROWS_AS(backend.init(config), TileAiException);
  }

  SECTION("init succeeds with explicit vfs.tile.* config") {
    TileAi backend;
    SmConfig config;
    REQUIRE(config.set("vfs.tile.server_url", "http://localhost:3000").ok());
    REQUIRE(config.set("vfs.tile.api_key", "tla_test").ok());
    REQUIRE_NOTHROW(backend.init(config));
  }

  SECTION("init succeeds with TILEDB_VFS_TILE_* env (canonical TileDB)") {
    auto u =
        setenv_local("TILEDB_VFS_TILE_SERVER_URL", "http://localhost:3000");
    auto k = setenv_local("TILEDB_VFS_TILE_API_KEY", "tla_test");
    TileAi backend;
    SmConfig config;
    REQUIRE_NOTHROW(backend.init(config));
  }

  SECTION("init succeeds with TILE_API_URL + TILE_API_KEY (SDK convention)") {
    auto u = setenv_local("TILE_API_URL", "http://localhost:3000");
    auto k = setenv_local("TILE_API_KEY", "tla_test");
    TileAi backend;
    SmConfig config;
    REQUIRE_NOTHROW(backend.init(config));
  }

  SECTION("init throws when TILE_API_URL is set but no api_key anywhere") {
    auto u = setenv_local("TILE_API_URL", "http://localhost:3000");
    TileAi backend;
    SmConfig config;
    REQUIRE_THROWS_WITH(
        backend.init(config), Catch::Matchers::ContainsSubstring("api_key"));
  }

  SECTION("init throws when TILE_API_KEY is set but no server_url anywhere") {
    auto k = setenv_local("TILE_API_KEY", "tla_test");
    TileAi backend;
    SmConfig config;
    REQUIRE_THROWS_WITH(
        backend.init(config), Catch::Matchers::ContainsSubstring("server_url"));
  }

  SECTION("error message names TILE_API_URL when server_url missing") {
    TileAi backend;
    SmConfig config;
    REQUIRE(config.set("vfs.tile.api_key", "tla_test").ok());
    REQUIRE_THROWS_WITH(
        backend.init(config),
        Catch::Matchers::ContainsSubstring("TILE_API_URL"));
  }

  SECTION("error message names TILE_API_KEY when api_key missing") {
    TileAi backend;
    SmConfig config;
    REQUIRE(config.set("vfs.tile.server_url", "http://localhost:3000").ok());
    REQUIRE_THROWS_WITH(
        backend.init(config),
        Catch::Matchers::ContainsSubstring("TILE_API_KEY"));
  }

  SECTION("partial fallback: explicit url + TILE_API_KEY env") {
    // server_url from explicit config, api_key from SDK env — both lookup
    // paths exercised in a single init() call.
    auto k = setenv_local("TILE_API_KEY", "tla_test");
    TileAi backend;
    SmConfig config;
    REQUIRE(config.set("vfs.tile.server_url", "http://localhost:3000").ok());
    REQUIRE_NOTHROW(backend.init(config));
  }

  SECTION("partial fallback: TILE_API_URL env + explicit api_key") {
    auto u = setenv_local("TILE_API_URL", "http://localhost:3000");
    TileAi backend;
    SmConfig config;
    REQUIRE(config.set("vfs.tile.api_key", "tla_test").ok());
    REQUIRE_NOTHROW(backend.init(config));
  }

  SECTION("explicit config + TILE_API_* env both present, init succeeds") {
    auto u = setenv_local("TILE_API_URL", "http://env-url");
    auto k = setenv_local("TILE_API_KEY", "env-key");
    TileAi backend;
    SmConfig config;
    REQUIRE(config.set("vfs.tile.server_url", "http://config-url").ok());
    REQUIRE(config.set("vfs.tile.api_key", "config-key").ok());
    REQUIRE_NOTHROW(backend.init(config));
    // Precedence is verified directly below via Config::get.
  }
}

TEST_CASE(
    "C++ API: tile:// config resolution precedence (Config + SDK env alias)",
    "[cppapi][tile][env]") {
  // The SDK env-var aliases (TILE_API_URL / TILE_API_KEY) live inside
  // `Config::get_from_env`, so the
  // resolution chain for `vfs.tile.*` keys is whatever Config provides
  // out of the box. These tests pin that contract.

  // Mask all relevant env vars so the test starts deterministic.
  auto _vfs_tile = setenv_local("TILEDB_VFS_TILE_API_KEY", "");
  auto _tile = setenv_local("TILE_API_KEY", "");

  SECTION("returns empty when nothing is set") {
    SmConfig config;
    CHECK(config_string(config, "vfs.tile.api_key") == "");
  }

  SECTION("explicit config wins over both envs") {
    auto vfs = setenv_local("TILEDB_VFS_TILE_API_KEY", "config-env-value");
    auto sdk = setenv_local("TILE_API_KEY", "sdk-env-value");
    SmConfig config;
    REQUIRE(config.set("vfs.tile.api_key", "user-set-value").ok());
    CHECK(config_string(config, "vfs.tile.api_key") == "user-set-value");
  }

  SECTION("TILEDB_VFS_TILE_* env wins over SDK env") {
    auto vfs = setenv_local("TILEDB_VFS_TILE_API_KEY", "config-env-value");
    auto sdk = setenv_local("TILE_API_KEY", "sdk-env-value");
    SmConfig config;
    CHECK(config_string(config, "vfs.tile.api_key") == "config-env-value");
  }

  SECTION("SDK env (TILE_API_KEY) used when no TILEDB_VFS_TILE_* env is set") {
    auto sdk = setenv_local("TILE_API_KEY", "sdk-env-value");
    SmConfig config;
    CHECK(config_string(config, "vfs.tile.api_key") == "sdk-env-value");
  }

  SECTION("SDK env is ignored when explicitly set to empty string") {
    // setenv to "" is the standard way to mask an env var without
    // unsetting; the alias check treats it as absent.
    auto sdk = setenv_local("TILE_API_KEY", "");
    SmConfig config;
    CHECK(config_string(config, "vfs.tile.api_key") == "");
  }

  SECTION("server_url variant: TILE_API_URL fallback works the same") {
    auto sdk = setenv_local("TILE_API_URL", "http://sdk-server");
    SmConfig config;
    CHECK(config_string(config, "vfs.tile.server_url") == "http://sdk-server");
  }
}
