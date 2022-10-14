/**
 * @file tiledb/sm/subarray/unit_add_ranges_list.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file defines unit tests for the add_ranges_list method of Subarray
 * class.
 */

#include <test/support/tdb_catch.h>
#if defined(DELETE)
#undef DELETE
#endif
#include <tiledb/sm/array/array.h>
#include <tiledb/sm/array_schema/dimension.h>
#include <tiledb/sm/enums/encryption_type.h>
#include <tiledb/sm/storage_manager/context.h>
#include <tiledb/sm/subarray/subarray.h>

#include <test/src/helpers.h>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::type;

TEST_CASE("Subarray::add_ranges_list", "[subarray]") {
  // Setup an Array needed to construct the Subarray for testing
  // add_ranges_list.
  std::shared_ptr<tiledb::sm::Dimension> sp_dim1 =
      make_shared<tiledb::sm::Dimension>(HERE(), "d1", Datatype::INT64);
  std::shared_ptr<tiledb::sm::Dimension> sp_dim2 =
      make_shared<tiledb::sm::Dimension>(HERE(), "d2", Datatype::INT64);
  uint64_t tile_extents[] = {2, 2};
  std::vector<std::shared_ptr<tiledb::sm::Dimension>> dims{sp_dim1, sp_dim2};
  std::shared_ptr<tiledb::sm::Domain> sp_dom = make_shared<tiledb::sm::Domain>(
      HERE(), Layout::ROW_MAJOR, dims, Layout::ROW_MAJOR);
  uint64_t local_DIM_DOMAIN[4] = {1, 12, 1, 12};
  CHECK(sp_dim1->set_domain(&local_DIM_DOMAIN[0]).ok());
  CHECK(sp_dim2->set_domain(&local_DIM_DOMAIN[2]).ok());
  CHECK(sp_dim1->set_tile_extent(&tile_extents[0]).ok());
  CHECK(sp_dim2->set_tile_extent(&tile_extents[1]).ok());
  std::shared_ptr<tiledb::sm::Attribute> sp_attrib =
      make_shared<tiledb::sm::Attribute>(HERE(), "a1", Datatype::INT32);
  tiledb::sm::Domain dom{Layout::ROW_MAJOR, dims, Layout::ROW_MAJOR};
  std::shared_ptr<tiledb::sm::ArraySchema> sp_as =
      make_shared<tiledb::sm::ArraySchema>(HERE());
  CHECK(sp_as->set_domain(sp_dom).ok());
  CHECK(sp_as->add_attribute(sp_attrib).ok());
  // sp_as->add_dimension();
  tiledb::sm::Config cfg;
  tiledb::sm::Context ctx(cfg);
  tiledb::sm::Array a(tiledb::sm::URI{"mem://junk"}, ctx.storage_manager());
  a.set_array_schema_latest(sp_as);
  // a.create();
  tiledb::sm::EncryptionKey ek;
  CHECK(ek.set_key(tiledb::sm::EncryptionType::NO_ENCRYPTION, nullptr, 0).ok());
  CHECK(ctx.storage_manager()->array_create(a.array_uri(), sp_as, ek).ok());
  CHECK(a.open(
             tiledb::sm::QueryType::READ,
             tiledb::sm::EncryptionType::NO_ENCRYPTION,
             nullptr,
             0)
            .ok());

  // The Subarray used to test add_ranges_list.
  tiledb::sm::Subarray sa(
      &a,
      &test::g_helper_stats,
      test::g_helper_logger(),
      true,
      ctx.storage_manager());

  // Add ranges
  // NOTE: The type used here for the range needs to match the type used for the
  // dimensions in the array above, as the coord_size of the dimension is used
  // underneath add_ranges_list to determine the size of the values being
  // iterated over.
  uint64_t ranges[] = {1, 2, 4, 5, 7, 8, 10, 11};
  CHECK(sa.add_ranges_list(0, ranges, 8).ok());
  CHECK(sa.add_ranges_list(1, ranges, 8).ok());
  uint64_t range_num;
  CHECK(sa.get_range_num(0, &range_num).ok());
  CHECK(range_num == 4);

  // Check ranges
  for (uint32_t dim_idx = 0; dim_idx < 1; dim_idx++) {
    for (uint32_t idx = 0; idx < range_num; idx++) {
      const void *start, *end;
      CHECK(sa.get_range(dim_idx, idx, &start, &end).ok());
      CHECK(*(uint64_t*)start == ranges[idx * 2]);
      CHECK(*(uint64_t*)end == ranges[idx * 2 + 1]);
    }
  }
}
