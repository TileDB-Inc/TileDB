/**
 * @file   unit-cppapi-consolidation.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2026 TileDB Inc.
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
 * Consolidation tests with the C++ API.
 */
#include "tiledb/sm/cpp_api/tiledb_experimental"

#include <test/support/tdb_catch.h>
#include "test/support/src/array_helpers.h"
#include "test/support/src/array_templates.h"
#include "test/support/src/fragment_info_helpers.h"
#include "test/support/src/helpers.h"
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/comparators.h"

using namespace tiledb;
using namespace tiledb::test;

void remove_array(const std::string& array_name) {
  Context ctx;
  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

void create_array(const std::string& array_name) {
  Context ctx;
  Domain domain(ctx);
  auto d = Dimension::create<int>(ctx, "d", {{1, 3}}, 2);
  domain.add_dimensions(d);
  auto a = Attribute::create<int>(ctx, "a");
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attributes(a);
  Array::create(array_name, schema);
}

void create_array_2d(const std::string& array_name) {
  Context ctx;
  Domain domain(ctx);
  auto d1 = Dimension::create<int>(ctx, "d1", {{1, 10}}, 2);
  auto d2 = Dimension::create<int>(ctx, "d2", {{1, 10}}, 2);
  domain.add_dimensions(d1);
  domain.add_dimensions(d2);
  auto a = Attribute::create<int>(ctx, "a");
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attributes(a);
  Array::create(array_name, schema);
}

void write_array(
    const std::string& array_name,
    const std::vector<int>& subarray,
    std::vector<int> values) {
  Context ctx;
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR);
  Subarray sub(ctx, array);
  sub.set_subarray(subarray);
  query.set_subarray(sub);
  query.set_data_buffer("a", values);
  query.submit();
  array.close();
}

void read_array(
    const std::string& array_name,
    const std::vector<int>& subarray,
    const std::vector<int>& c_values) {
  Context ctx;
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);
  query.set_layout(TILEDB_ROW_MAJOR);
  Subarray sub(ctx, array);
  sub.set_subarray(subarray);
  query.set_subarray(sub);
  std::vector<int> values(10);
  query.set_data_buffer("a", values);
  query.submit();
  array.close();
  values.resize(query.result_buffer_elements()["a"].second);
  CHECK(values.size() == c_values.size());
  CHECK(values == c_values);
}

TEST_CASE(
    "C++ API: Test consolidation with partial tiles",
    "[cppapi][consolidation]") {
  std::string array_name = "cppapi_consolidation";
  remove_array(array_name);

  create_array(array_name);
  write_array(array_name, {1, 2}, {1, 2});
  write_array(array_name, {3, 3}, {3});
  CHECK(tiledb::test::num_fragments(array_name) == 2);

  read_array(array_name, {1, 3}, {1, 2, 3});

  Context ctx;
  REQUIRE_NOTHROW(Array::consolidate(ctx, array_name, nullptr));
  CHECK(tiledb::test::num_fragments(array_name) == 3);
  REQUIRE_NOTHROW(Array::vacuum(ctx, array_name, nullptr));
  CHECK(tiledb::test::num_fragments(array_name) == 1);

  read_array(array_name, {1, 3}, {1, 2, 3});

  remove_array(array_name);
}

TEST_CASE(
    "C++ API: Test consolidation with domain expansion",
    "[cppapi][consolidation][expand-domain]") {
  std::string array_name = "cppapi_consolidation_domain_exp";
  remove_array(array_name);

  // Create array
  tiledb::Config cfg;
  cfg["sm.mem.consolidation.buffers_weight"] = "1";
  cfg["sm.mem.consolidation.reader_weight"] = "5000";
  cfg["sm.mem.consolidation.writer_weight"] = "5000";
  Context ctx(cfg);
  Domain domain(ctx);
  auto d = Dimension::create<int>(ctx, "d1", {{10, 110}}, 50);
  domain.add_dimensions(d);
  auto a = Attribute::create<float>(ctx, "a");
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attributes(a);
  Array::create(array_name, schema);

  // Write
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);

  std::vector<float> a1(100);
  std::fill(a1.begin(), a1.end(), 1.0);
  std::vector<float> a2({2.0});

  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray(Subarray(ctx, array).set_subarray({10, 109}));
  query.set_data_buffer("a", a1);
  query.submit();

  query = Query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray(Subarray(ctx, array).set_subarray({110, 110}));
  query.set_data_buffer("a", a2);
  query.submit();
  array.close();

  // Read
  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  query_r.set_layout(TILEDB_ROW_MAJOR);
  query_r.set_subarray(Subarray(ctx, array_r).set_subarray({10, 110}));
  std::vector<float> a_r(101);
  query_r.set_data_buffer("a", a_r);
  query_r.submit();
  array_r.close();

  std::vector<float> c_a(100, 1.0f);
  c_a.push_back(2.0f);
  CHECK(a_r == c_a);

  // Consolidate
  REQUIRE_NOTHROW(Array::consolidate(ctx, array_name, nullptr));

  // Read again
  Array array_c(ctx, array_name, TILEDB_READ);
  query_r = Query(ctx, array_c, TILEDB_READ);
  query_r.set_layout(TILEDB_ROW_MAJOR);
  query_r.set_subarray(Subarray(ctx, array_c).set_subarray({10, 110}));
  query_r.set_data_buffer("a", a_r);
  query_r.submit();
  array_c.close();
  CHECK(a_r == c_a);

  remove_array(array_name);
}

TEST_CASE(
    "C++ API: Test consolidation without vacuum", "[cppapi][consolidation]") {
  std::string array_name = "cppapi_consolidation";
  remove_array(array_name);

  create_array(array_name);
  write_array(array_name, {1, 2}, {1, 2});
  write_array(array_name, {3, 3}, {3});
  CHECK(tiledb::test::num_fragments(array_name) == 2);

  read_array(array_name, {1, 3}, {1, 2, 3});

  Context ctx;
  REQUIRE_NOTHROW(Array::consolidate(ctx, array_name, nullptr));
  CHECK(tiledb::test::num_fragments(array_name) == 3);

  read_array(array_name, {1, 3}, {1, 2, 3});

  remove_array(array_name);
}

TEST_CASE(
    "C++ API: Test consolidation with fragment list",
    "[cppapi][consolidation]") {
  std::string array_name = "cppapi_consolidation";
  remove_array(array_name);

  create_array(array_name);
  write_array(array_name, {1, 2}, {1, 2});
  write_array(array_name, {3, 3}, {3});
  CHECK(tiledb::test::num_fragments(array_name) == 2);

  read_array(array_name, {1, 3}, {1, 2, 3});

  Context ctx;

  FragmentInfo fragment_info(ctx, array_name);
  fragment_info.load();
  std::string fragment_name1 = fragment_info.fragment_uri(0);
  std::string fragment_name2 = fragment_info.fragment_uri(1);
  std::string short_fragment_name1 =
      fragment_name1.substr(fragment_name1.find_last_of('/') + 1);
  std::string short_fragment_name2 =
      fragment_name2.substr(fragment_name2.find_last_of('/') + 1);

  const char* fragment_uris[2] = {
      short_fragment_name1.c_str(), short_fragment_name2.c_str()};

  REQUIRE_NOTHROW(
      Array::consolidate(ctx, array_name, fragment_uris, 2, nullptr));
  CHECK(tiledb::test::num_fragments(array_name) == 3);

  read_array(array_name, {1, 3}, {1, 2, 3});

  remove_array(array_name);
}

TEST_CASE(
    "C++ API: Test consolidation with wrong fragment list",
    "[cppapi][consolidation][fragment-list-consolidation]") {
  std::string array_name = "cppapi_consolidation";
  remove_array(array_name);

  Context ctx;
  Config config;
  std::string fragment_name1;
  std::string fragment_name2;
  bool throws = false;
  int32_t number_of_fragments_before_consolidation = 0;

  SECTION("Throws exception") {
    throws = true;
    create_array_2d(array_name);
    // In this case we request to consolidate frag2 and frag4. We can see that
    // frag1 has been created prior to frag3 so the first condition to abort
    // the consolidation is satisfied. Additionally, frag1's
    // domain intersects with the union of the domains of the
    // selected fragments for consolidation(frag2, frag4), so the second
    // condition is also satisfied. An exception is expected.
    write_array(array_name, {1, 3, 7, 9}, {1, 2, 3, 4, 5, 6, 7, 8, 9});
    write_array(array_name, {2, 4, 2, 3}, {10, 11, 12, 13, 14, 15});
    write_array(array_name, {3, 5, 4, 5}, {16, 17, 18, 19, 20, 21});
    write_array(array_name, {7, 9, 6, 8}, {22, 23, 24, 25, 26, 27, 28, 29, 30});

    number_of_fragments_before_consolidation =
        tiledb::test::num_fragments(array_name);
    CHECK(number_of_fragments_before_consolidation == 4);

    FragmentInfo fragment_info(ctx, array_name);
    fragment_info.load();
    fragment_name1 = fragment_info.fragment_uri(1);
    fragment_name2 = fragment_info.fragment_uri(3);
  }

  SECTION("Throws exception because of overlap in extended domain") {
    throws = true;
    create_array_2d(array_name);
    // In this case we request to consolidate frag1 and frag3. We can see that
    // frag2 has been created prior to frag3 so the first condition to abort
    // the consolidation is satisfied. Additionally, even though frag2's
    // domain does not directly intersect with the union of the domains of the
    // selected fragments for consolidation(frag1, frag3), it intersects with
    // their expanded domain(Full tiles) because the tile extend is set to 2 and
    // the domain range is 1-10.
    write_array(array_name, {2, 4, 2, 3}, {1, 2, 3, 4, 5, 6});
    write_array(array_name, {10, 10, 4, 4}, {16});
    write_array(array_name, {7, 9, 6, 8}, {7, 8, 9, 10, 11, 12, 13, 14, 15});

    number_of_fragments_before_consolidation =
        tiledb::test::num_fragments(array_name);
    CHECK(number_of_fragments_before_consolidation == 3);

    FragmentInfo fragment_info(ctx, array_name);
    fragment_info.load();
    fragment_name1 = fragment_info.fragment_uri(0);
    fragment_name2 = fragment_info.fragment_uri(2);
  }

  SECTION(
      "Throws exception because of overlap with already consolidated "
      "fragment") {
    throws = true;
    create_array_2d(array_name);
    // In this case we request to consolidate frag1 and frag3. Before this main
    // consolidation we run another secondary consolidation between frag2 and
    // frag4. The consolidated frag2_frag4 has been created after frag3 but its
    // start timestamp is older than frag3's start timestamp so the first
    // condition to abort the consolidation is satisfied. Frag2_frag4's domain
    // intersects with the union of the domains of the selected fragments for
    // consolidation(frag1, frag3), so the second condition is also satisfied.
    // An exception is expected.
    write_array(array_name, {2, 4, 2, 3}, {10, 11, 12, 13, 14, 15});
    write_array(array_name, {8, 9, 3, 4}, {32, 33, 34, 35});
    write_array(array_name, {7, 9, 6, 8}, {22, 23, 24, 25, 26, 27, 28, 29, 30});
    write_array(array_name, {7, 8, 3, 4}, {31, 32, 33, 34});

    FragmentInfo fragment_info(ctx, array_name);
    fragment_info.load();
    fragment_name1 = fragment_info.fragment_uri(1);
    fragment_name2 = fragment_info.fragment_uri(3);

    std::string short_fragment_name1 =
        fragment_name1.substr(fragment_name1.find_last_of('/') + 1);
    std::string short_fragment_name2 =
        fragment_name2.substr(fragment_name2.find_last_of('/') + 1);

    const char* fragment_uris[2] = {
        short_fragment_name1.c_str(), short_fragment_name2.c_str()};

    REQUIRE_NOTHROW(
        Array::consolidate(ctx, array_name, fragment_uris, 2, &config));

    fragment_name1 = fragment_info.fragment_uri(0);
    fragment_name2 = fragment_info.fragment_uri(2);

    number_of_fragments_before_consolidation =
        tiledb::test::num_fragments(array_name);
    CHECK(number_of_fragments_before_consolidation == 5);
  }

  SECTION("Does not throw exception") {
    create_array_2d(array_name);
    // In this case we request to consolidate frag1 and frag2. We can see that
    // frag3 has not been created prior to frag3 so the first condition to abort
    // the consolidation is not satisfied. Frag3's domain intersects with the
    // union of the domains of the selected fragments for consolidation(frag1,
    // frag2), so the second condition is satisfied. An exception is expected.
    write_array(array_name, {2, 4, 2, 3}, {10, 11, 12, 13, 14, 15});
    write_array(array_name, {7, 9, 6, 8}, {22, 23, 24, 25, 26, 27, 28, 29, 30});
    write_array(array_name, {7, 8, 3, 4}, {31, 32, 33, 34});

    number_of_fragments_before_consolidation =
        tiledb::test::num_fragments(array_name);
    CHECK(number_of_fragments_before_consolidation == 3);

    FragmentInfo fragment_info(ctx, array_name);
    fragment_info.load();
    fragment_name1 = fragment_info.fragment_uri(0);
    fragment_name2 = fragment_info.fragment_uri(1);
  }

  std::string short_fragment_name1 =
      fragment_name1.substr(fragment_name1.find_last_of('/') + 1);
  std::string short_fragment_name2 =
      fragment_name2.substr(fragment_name2.find_last_of('/') + 1);

  const char* fragment_uris[2] = {
      short_fragment_name1.c_str(), short_fragment_name2.c_str()};

  if (throws) {
    REQUIRE_THROWS_WITH(
        Array::consolidate(ctx, array_name, fragment_uris, 2, &config),
        Catch::Matchers::ContainsSubstring(
            "Cannot consolidate; The non-empty domain of the fragment"));
  } else {
    REQUIRE_NOTHROW(
        Array::consolidate(ctx, array_name, fragment_uris, 2, &config));

    CHECK(
        tiledb::test::num_fragments(array_name) ==
        number_of_fragments_before_consolidation + 1);
  }

  remove_array(array_name);
}

TEST_CASE(
    "C++ API: Test consolidation with timestamp and max domain",
    "[cppapi][consolidation][timestamp][maxdomain]") {
  Context ctx;
  VFS vfs(ctx);
  const std::string array_name = "consolidate_timestamp_max_domain";

  int64_t domain1[] = {
      std::numeric_limits<int64_t>::min() + 1,
      std::numeric_limits<int64_t>::max()};
  const uint8_t domain2[2] = {0, 1};
  Domain domain(ctx);
  domain
      .add_dimension(
          Dimension::create(ctx, "d1", TILEDB_DATETIME_MS, domain1, nullptr))
      .add_dimension(
          Dimension::create(ctx, "d2", TILEDB_INT8, domain2, nullptr));

  // The array will be dense.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);

  schema.add_attribute(Attribute::create<int64_t>(ctx, "a1"));

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
  Array::create(array_name, schema);

  std::vector<int64_t> d1 = {0};
  std::vector<int8_t> d2 = {0};
  std::vector<int64_t> a1 = {0};

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("d1", d1)
      .set_data_buffer("d2", d2)
      .set_data_buffer("a1", a1);
  query.submit();

  d2[0] = 1;
  a1[0] = 1;
  Query query2(ctx, array, TILEDB_WRITE);
  query2.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("d1", d1)
      .set_data_buffer("d2", d2)
      .set_data_buffer("a1", a1);
  query2.submit();

  array.close();

  tiledb::Array::consolidate(ctx, array_name);

  std::vector<int64_t> d1_r(2);
  std::vector<int8_t> d2_r(2);
  std::vector<int64_t> a1_r(2);
  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r);
  query_r.set_data_buffer("d1", d1_r)
      .set_data_buffer("d2", d2_r)
      .set_data_buffer("a1", a1_r);
  REQUIRE(query_r.submit() == Query::Status::COMPLETE);
  array_r.close();

  REQUIRE(d1_r[0] == 0);
  REQUIRE(d1_r[1] == 0);
  REQUIRE(d2_r[0] == 0);
  REQUIRE(d2_r[1] == 1);
  REQUIRE(a1_r[0] == 0);
  REQUIRE(a1_r[1] == 1);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test consolidation that respects the current domain",
    "[cppapi][consolidation][current_domain]") {
  std::string array_name = "cppapi_consolidation_current_domain";
  remove_array(array_name);

  Context ctx;

  Domain domain(ctx);
  auto d1 = Dimension::create<int32_t>(ctx, "d1", {{0, 1000000000}}, 50);
  auto d2 = Dimension::create<int32_t>(ctx, "d2", {{0, 1000000000}}, 50);
  domain.add_dimensions(d1, d2);

  auto a = Attribute::create<int>(ctx, "a");

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attributes(a);

  tiledb::NDRectangle ndrect(ctx, domain);
  int32_t range_one[] = {0, 2};
  int32_t range_two[] = {0, 3};
  ndrect.set_range(0, range_one[0], range_one[1]);
  ndrect.set_range(1, range_two[0], range_two[1]);

  tiledb::CurrentDomain current_domain(ctx);
  current_domain.set_ndrectangle(ndrect);

  tiledb::ArraySchemaExperimental::set_current_domain(
      ctx, schema, current_domain);

  Array::create(array_name, schema);

  std::vector<int> data = {
      -60, 79, -8, 100, 88, -19, -100, -111, -72, -85, 58, -41};

  // Write it twice so there is something to consolidate
  write_array(array_name, {0, 2, 0, 3}, data);
  write_array(array_name, {0, 2, 0, 3}, data);

  CHECK(tiledb::test::num_fragments(array_name) == 2);
  Context ctx2;
  Config config;
  REQUIRE_NOTHROW(Array::consolidate(ctx2, array_name, &config));
  CHECK(tiledb::test::num_fragments(array_name) == 3);

  remove_array(array_name);
}

template <sm::Datatype DT, templates::FragmentType F>
void instance_dense_consolidation_create_array(
    Context& ctx,
    const std::string& array_name,
    const std::vector<templates::Dimension<DT>>& domain) {
  using Coord = templates::Dimension<DT>::value_type;

  // create array
  Domain arraydomain(ctx);
  for (uint64_t d = 0; d < domain.size(); d++) {
    const std::string dname = "d" + std::to_string(d + 1);
    auto dd = Dimension::create<Coord>(
        ctx,
        dname,
        {domain[d].domain.lower_bound, domain[d].domain.upper_bound},
        domain[d].extent);
    arraydomain.add_dimension(dd);
  }

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(arraydomain);

  const std::vector<std::tuple<Datatype, uint32_t, bool>> attributes =
      templates::ddl::physical_type_attributes<F>();
  for (uint64_t a = 0; a < attributes.size(); a++) {
    const std::string aname = "a" + std::to_string(a + 1);
    auto aa = Attribute::create(
                  ctx,
                  aname,
                  static_cast<tiledb_datatype_t>(std::get<0>(attributes[a])))
                  .set_cell_val_num(std::get<1>(attributes[a]))
                  .set_nullable(std::get<2>(attributes[a]));
    schema.add_attribute(aa);
  }

  Array::create(array_name, schema);
}

/**
 * Runs an instance of a dense consolidation test.
 * The `fragments` are written in ascending order from the beginning of the
 * array domain.
 *
 * Asserts that after consolidation we get fragments which appropriately satisfy
 * `max_fragment_size`:
 * 1) no fragment is larger than that size
 * 2) if the union of two adjacent fragments can form a rectangular domain, then
 *    the sum of their sizes must exceed the maximum fragment size (else they
 *    should be one fragment)
 *
 * @precondition the `fragments` each have a number of cells which is an
 * integral number of tiles
 */
template <
    sm::Datatype DT,
    templates::FragmentType F,
    typename Asserter = AsserterCatch>
std::vector<std::vector<typename templates::Dimension<DT>::domain_type>>
instance_dense_consolidation(
    Context& ctx,
    const std::string& array_name,
    const std::vector<templates::Dimension<DT>>& domain,
    std::vector<F>& fragments,
    uint64_t max_fragment_size) {
  using Coord = templates::Dimension<DT>::value_type;

  static constexpr sm::Layout tile_order = sm::Layout::ROW_MAJOR;

  // create array
  instance_dense_consolidation_create_array<DT, F>(ctx, array_name, domain);

  DeleteArrayGuard arrayguard(ctx.ptr().get(), array_name.c_str());

  sm::NDRange array_domain;
  for (const auto& dim : domain) {
    array_domain.push_back(
        Range(dim.domain.lower_bound, dim.domain.upper_bound));
  }

  uint64_t num_cells_per_tile = 1;
  std::vector<Coord> tile_extents;
  for (const auto& dim : domain) {
    tile_extents.push_back(dim.extent);
    num_cells_per_tile *= static_cast<uint64_t>(dim.extent);
  }

  // populate array
  uint64_t start_tile = 0;
  {
    Array forwrite(ctx, array_name, TILEDB_WRITE);
    for (auto& f : fragments) {
      const uint64_t f_num_tiles = f.num_cells() / num_cells_per_tile;

      const std::optional<sm::NDRange> subarray = domain_tile_offset<Coord>(
          tile_order, tile_extents, array_domain, start_tile, f_num_tiles);
      ASSERTER(subarray.has_value());

      templates::query::write_fragment<Asserter, F, Coord>(
          f, forwrite, subarray.value());

      start_tile += f_num_tiles;
    }
  }

  sm::NDRange non_empty_domain;
  {
    std::optional<sm::NDRange> maybe = domain_tile_offset<Coord>(
        tile_order, tile_extents, array_domain, 0, start_tile);
    ASSERTER(maybe.has_value());
    non_empty_domain = maybe.value();
  }

  // consolidate
  Config cconfig;
  cconfig["sm.consolidation.max_fragment_size"] =
      std::to_string(max_fragment_size);
  Array::consolidate(ctx, array_name, &cconfig);

  Array forread(ctx, array_name, TILEDB_READ);

  // sanity check the non-empty domain
  // NB: cannot use `==` for some reason, the array `non_empty_domain` method
  // returns `range_start_size_` zero
  {
    const auto actual_domain = forread.ptr()->array()->non_empty_domain();
    for (uint64_t d = 0; d < domain.size(); d++) {
      ASSERTER(
          non_empty_domain[d].start_as<Coord>() ==
          actual_domain[d].start_as<Coord>());
      ASSERTER(
          non_empty_domain[d].end_as<Coord>() ==
          actual_domain[d].end_as<Coord>());
    }
  }

  // check fragment info
  FragmentInfo finfo(ctx, array_name);
  finfo.load();

  const auto fragment_domains =
      collect_and_validate_fragment_domains<Coord, Asserter>(
          ctx,
          tile_order,
          array_name,
          tile_extents,
          non_empty_domain,
          max_fragment_size);

  // read back fragments to check contents
  std::vector<Coord> api_subarray;
  api_subarray.reserve(2 * domain.size());
  for (uint64_t d = 0; d < domain.size(); d++) {
    api_subarray.push_back(non_empty_domain[d].start_as<Coord>());
    api_subarray.push_back(non_empty_domain[d].end_as<Coord>());
  }

  F input_concatenated, output;
  for (const auto& f : fragments) {
    input_concatenated.extend(f);
  }

  // sort in global order
  {
    std::vector<uint64_t> idxs(input_concatenated.size());
    std::iota(idxs.begin(), idxs.end(), 0);

    std::vector<Coord> next_coord;
    next_coord.reserve(domain.size());
    for (uint64_t d = 0; d < domain.size(); d++) {
      next_coord.push_back(domain[d].domain.lower_bound);
    }

    std::vector<std::vector<Coord>> coords;
    coords.reserve(input_concatenated.size());
    for (uint64_t i = 0; i < input_concatenated.size(); i++) {
      coords.push_back(next_coord);
      for (uint64_t di = 0; di < domain.size(); di++) {
        const uint64_t d = domain.size() - di - 1;
        if (next_coord[d] < domain[d].domain.upper_bound) {
          ++next_coord[d];
          break;
        } else {
          next_coord[d] = 0;
        }
      }
    }

    sm::GlobalCellCmp globalcmp(
        forread.ptr()->array()->array_schema_latest().domain());

    auto icmp = [&](uint64_t ia, uint64_t ib) -> bool {
      const auto sa = templates::global_cell_cmp_span<Coord>(coords[ia]);
      const auto sb = templates::global_cell_cmp_span<Coord>(coords[ib]);
      return globalcmp(sa, sb);
    };

    std::sort(idxs.begin(), idxs.end(), icmp);

    input_concatenated.attributes() = stdx::select(
        stdx::reference_tuple(input_concatenated.attributes()),
        std::span(idxs));
  }

  output = input_concatenated;

  Subarray sub(ctx, forread);
  sub.set_subarray(api_subarray);

  Query query(forread.context(), forread);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_subarray(sub);

  // make field size locations
  templates::query::fragment_field_sizes_t<F> field_sizes =
      templates::query::make_field_sizes<Asserter>(output, output.num_cells());

  // add fields to query
  auto outcursor = templates::query::fragment_field_sizes_t<F>();
  templates::query::set_fields<Asserter, F>(
      ctx.ptr().get(),
      query.ptr().get(),
      field_sizes,
      output,
      [](unsigned d) { return "d" + std::to_string(d + 1); },
      [](unsigned a) { return "a" + std::to_string(a + 1); },
      outcursor);

  const auto status = query.submit();
  ASSERTER(status == Query::Status::COMPLETE);

  // resize according to what was found
  templates::query::apply_cursor<F>(output, outcursor, field_sizes);

  ASSERTER(output == input_concatenated);

  return fragment_domains;
}

/**
 * Test case inspired by CORE-290.
 *
 */
TEST_CASE(
    "C++ API: Test consolidation dense array with max fragment size",
    "[cppapi][consolidation]") {
  using Dim64 = templates::Dimension<sm::Datatype::UINT64>;
  using Dom64 = Dim64::domain_type;
  using DenseFragmentFixed = templates::Fragment<std::tuple<>, std::tuple<int>>;

  const std::string array_name = "cppapi_consolidation_dense";

  Context ctx;

  SECTION("2D") {
    SECTION("Row tiles") {
      const Dim64 row(0, std::numeric_limits<uint64_t>::max() - 1, 1);
      const Dim64 col(0, 99999, 100000);

      const uint64_t num_fragments = 32;

      // each input fragment is a single row
      std::vector<DenseFragmentFixed> input_fragments;
      for (uint64_t f = 0; f < num_fragments; f++) {
        DenseFragmentFixed fdata;
        fdata.resize(row.extent * col.domain.num_cells());

        auto& att = std::get<0>(fdata.attributes());
        std::iota(
            att.begin(), att.end(), static_cast<int>(f) * fdata.num_cells());

        input_fragments.push_back(fdata);
      }

      // unfiltered, each row takes `100000 * sizeof(int)` bytes, plus some
      // padding
      const uint64_t tile_size = (row.extent * col.extent * sizeof(int)) + 92;
      const uint64_t max_fragment_size = GENERATE_COPY(
          tile_size - 1, tile_size, (2 * tile_size) - 1, 2 * tile_size);

      const uint64_t rows_per_fragment = max_fragment_size / tile_size;
      DYNAMIC_SECTION(
          "max_fragment_size = " + std::to_string(max_fragment_size)) {
        if (rows_per_fragment == 0) {
          const auto expect = Catch::Matchers::ContainsSubstring(
              "Fragment size is too small to subdivide dense subarray into "
              "multiple fragments");
          REQUIRE_THROWS(
              instance_dense_consolidation<
                  sm::Datatype::UINT64,
                  DenseFragmentFixed>(
                  ctx,
                  array_name,
                  {row, col},
                  input_fragments,
                  max_fragment_size),
              expect);
        } else {
          const auto output_fragments = instance_dense_consolidation<
              sm::Datatype::UINT64,
              DenseFragmentFixed>(
              ctx, array_name, {row, col}, input_fragments, max_fragment_size);

          std::vector<std::vector<Dom64>> expect;
          for (uint64_t r = 0; r < num_fragments; r += rows_per_fragment) {
            expect.push_back({Dom64(r, r + rows_per_fragment - 1), col.domain});
          }
          CHECK(output_fragments == expect);
        }
      }
    }

    SECTION("Rectangle tiles") {
      const Dim64 row(0, std::numeric_limits<uint64_t>::max() - 1, 4);
      const Dim64 col(0, 99999, 100000 / row.extent);

      const uint64_t num_fragments = 32;

      // each input fragment is 4 tiles, covering 4 rows of cells
      std::vector<DenseFragmentFixed> input_fragments;
      for (uint64_t f = 0; f < num_fragments; f++) {
        DenseFragmentFixed fdata;
        fdata.resize(row.extent * col.extent * row.extent);

        auto& att = std::get<0>(fdata.attributes());
        std::iota(
            att.begin(), att.end(), static_cast<int>(f) * fdata.num_cells());

        input_fragments.push_back(fdata);
      }

      // unfiltered, each row takes `100000 * sizeof(int)` bytes, plus some
      // padding
      const uint64_t tile_size = (row.extent * col.extent * sizeof(int)) + 92;

      SECTION("Too small") {
        const auto expect = Catch::Matchers::ContainsSubstring(
            "Fragment size is too small to subdivide dense subarray into "
            "multiple fragments");
        REQUIRE_THROWS(
            instance_dense_consolidation<
                sm::Datatype::UINT64,
                DenseFragmentFixed>(
                ctx, array_name, {row, col}, input_fragments, tile_size - 1),
            expect);
      }
      SECTION("One tile") {
        std::vector<std::vector<Dom64>> expect;
        for (uint64_t r = 0; r < num_fragments; r++) {
          for (uint64_t c = 0; c < 4; c++) {
            expect.push_back(
                {Dom64(r * 4, r * 4 + 3),
                 Dom64(col.extent * c, (col.extent * (c + 1)) - 1)});
          }
        }
        const auto output_fragments = instance_dense_consolidation<
            sm::Datatype::UINT64,
            DenseFragmentFixed>(
            ctx, array_name, {row, col}, input_fragments, tile_size);
        CHECK(output_fragments == expect);
      }
      SECTION("Two tiles") {
        std::vector<std::vector<Dom64>> expect;
        for (uint64_t r = 0; r < num_fragments; r++) {
          expect.push_back(
              {Dom64(r * 4, r * 4 + 3), Dom64(0, (col.extent * 2) - 1)});
          expect.push_back(
              {Dom64(r * 4, r * 4 + 3),
               Dom64(col.extent * 2, (col.extent * 4) - 1)});
        }
        const auto output_fragments = instance_dense_consolidation<
            sm::Datatype::UINT64,
            DenseFragmentFixed>(
            ctx, array_name, {row, col}, input_fragments, 2 * tile_size);
      }
      SECTION("Three tiles") {
        // now we have some trouble, each row is 4 tiles, 3 of them fit,
        // so we will alternate fragments with 3 tiles and fragments with 1
        // tile to fill out the row, yikes
        std::vector<std::vector<Dom64>> expect;
        for (uint64_t r = 0; r < num_fragments; r++) {
          expect.push_back(
              {Dom64(r * 4, r * 4 + 3), Dom64(0, (col.extent * 3) - 1)});
          expect.push_back(
              {Dom64(r * 4, r * 4 + 3),
               Dom64(col.extent * 3, (col.extent * 4) - 1)});
        }
        const auto output_fragments = instance_dense_consolidation<
            sm::Datatype::UINT64,
            DenseFragmentFixed>(
            ctx, array_name, {row, col}, input_fragments, 3 * tile_size);
        CHECK(output_fragments == expect);
      }
      SECTION("Four tiles") {
        std::vector<std::vector<Dom64>> expect;
        for (uint64_t f = 0; f < num_fragments; f++) {
          expect.push_back({Dom64(f * 4, f * 4 + 3), col.domain});
        }
        const auto output_fragments = instance_dense_consolidation<
            sm::Datatype::UINT64,
            DenseFragmentFixed>(
            ctx, array_name, {row, col}, input_fragments, 4 * tile_size);
        CHECK(output_fragments == expect);
      }
      SECTION("Five tiles") {
        // since we need rectangle domains this is the same as four tiles
        std::vector<std::vector<Dom64>> expect;
        for (uint64_t f = 0; f < num_fragments; f++) {
          expect.push_back({Dom64(f * 4, f * 4 + 3), col.domain});
        }
        const auto output_fragments = instance_dense_consolidation<
            sm::Datatype::UINT64,
            DenseFragmentFixed>(
            ctx, array_name, {row, col}, input_fragments, 5 * tile_size);
        CHECK(output_fragments == expect);
      }
    }
  }
}
