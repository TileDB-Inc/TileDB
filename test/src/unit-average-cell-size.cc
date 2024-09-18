/**
 * @file   unit-average-cell-size.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB Inc.
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
 * Tests the average cell size computations.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/fragment/fragment_identifier.h"
#include "tiledb/sm/misc/constants.h"

#include <numeric>

using namespace tiledb;

struct CPPAverageCellSizeFx {
  const char* array_name = "cpp_average_cell_size";

  CPPAverageCellSizeFx()
      : vfs_(ctx_) {
    if (vfs_.is_dir(array_name)) {
      vfs_.remove_dir(array_name);
    }
  }

  /**
   * Create a schema with one int dimension, one var dimension and one int
   * attribute and one var attribute.
   */
  void create_array() {
    Domain domain(ctx_);
    auto d1 = Dimension::create<int>(ctx_, "d1", {{0, 1000}}, 2);
    domain.add_dimensions(d1);

    auto d2 =
        Dimension::create(ctx_, "d2", TILEDB_STRING_ASCII, nullptr, nullptr);
    domain.add_dimensions(d2);

    auto a1 = Attribute::create<int>(ctx_, "a1");
    auto a2 = Attribute::create<std::string>(ctx_, "a2");

    ArraySchema schema(ctx_, TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.add_attributes(a1, a2);
    schema.set_capacity(2);

    Array::create(array_name, schema);
  }

  void evolve_array() {
    auto evolution = ArraySchemaEvolution(ctx_);

    // add a new attribute a3
    auto a3 = Attribute::create<std::string>(ctx_, "a3");
    evolution.add_attribute(a3);

    uint64_t now = tiledb_timestamp_now_ms();
    now = now + 1;
    evolution.set_timestamp_range({now, now});

    // evolve array
    evolution.array_evolve(array_name);
  }

  /**
   * Write to the array, specifying cell sizes for the var dimension and var
   * attribute.
   *
   * @param d2_sizes Sizes for each cells for the var dimension.
   * @param a2_sizes Sizes for each cells for the var attribute.
   * @param a3_sizes Optional sizes for each cells for the var attribute.
   *
   * @return TimestampedURI for the written fragment.
   */
  sm::TimestampedURI write_array(
      std::vector<uint64_t> d2_sizes,
      std::vector<uint64_t> a2_sizes,
      optional<std::vector<uint64_t>> a3_sizes = std::nullopt) {
    REQUIRE(d2_sizes.size() == a2_sizes.size());
    if (a3_sizes.has_value()) {
      REQUIRE(d2_sizes.size() == a3_sizes->size());
    }

    // Open array and create query.
    Array array(ctx_, array_name, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);

    // Perform writes of the requested sizes.
    std::vector<int> d1(d2_sizes.size());
    std::iota(d1.begin(), d1.end(), 0);
    std::vector<int> a1(d2_sizes.size());
    std::iota(a1.begin(), a1.end(), 1);
    std::vector<uint64_t> d2_offs(d2_sizes.size());
    std::vector<uint64_t> a2_offs(d2_sizes.size());
    std::vector<uint64_t> a3_offs(d2_sizes.size());

    uint64_t d2_off = 0;
    uint64_t a2_off = 0;
    uint64_t a3_off = 0;
    for (uint64_t c = 0; c < d2_sizes.size(); c++) {
      d2_offs[c] = d2_off;
      d2_off += d2_sizes[c];

      a2_offs[c] = a2_off;
      a2_off += a2_sizes[c];

      if (a3_sizes.has_value()) {
        a3_offs[c] = a3_off;
        a3_off += a3_sizes->at(c);
      }
    }

    std::string d2;
    d2.resize(d2_off);

    std::string a2;
    a2.resize(a2_off);

    std::string a3;
    a3.resize(a3_off);

    // Perform the write.
    query.set_data_buffer("d1", d1);
    query.set_data_buffer("d2", d2);
    query.set_offsets_buffer("d2", d2_offs);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_offsets_buffer("a2", a2_offs);
    if (a3_sizes.has_value()) {
      query.set_data_buffer("a3", a3);
      query.set_offsets_buffer("a3", a3_offs);
    }
    query.set_layout(TILEDB_UNORDERED);
    REQUIRE(query.submit() == Query::Status::COMPLETE);

    auto uri = sm::URI(query.fragment_uri(0));
    sm::FragmentID fragment_id{uri};
    return {uri, fragment_id.timestamp_range()};
  }

  /**
   * Validate the cell size for all fragments of the array.
   *
   * @param d2_size Expected size for the average cell size for d2.
   * @param a2_size Expected size for the average cell size for a2.
   * @param a3_size Expected size for the average cell size for a3.
   */
  // void check_avg_cell_size(
  //     uint64_t d2_size,
  //     uint64_t a2_size,
  //     optional<uint64_t> a3_size = std::nullopt) {
  //   Array array(ctx_, array_name, TILEDB_READ);
  //   auto avg_cell_sizes = array.ptr()->array_->get_average_var_cell_sizes();

  //   CHECK(avg_cell_sizes["d2"] == d2_size);
  //   CHECK(avg_cell_sizes["a2"] == a2_size);
  //   if (a3_size.has_value()) {
  //     CHECK(avg_cell_sizes["a3"] == a3_size);
  //   }
  // }

  /**
   * Validate the cell size for some fragments of the array the same way it
   * will be computed for consolidation.
   *
   * @param uris Timestamped URIs for the fragments to consider.
   * @param d2_size Expected size for the average cell size for d2.
   * @param a2_size Expected size for the average cell size for a2.
   * @param a3_size Expected size for the average cell size for a3.
   */
  // void check_avg_cell_size_for_fragments(
  //     std::vector<sm::TimestampedURI> uris,
  //     uint64_t d2_size,
  //     uint64_t a2_size,
  //     optional<uint64_t> a3_size = std::nullopt) {
  //   auto array_for_reads{make_shared<sm::Array>(
  //       HERE(), ctx_.ptr().get()->resources(), sm::URI(array_name))};
  //   REQUIRE(array_for_reads
  //               ->open_without_fragments(
  //                   sm::EncryptionType::NO_ENCRYPTION, nullptr, 0)
  //               .ok());
  //   array_for_reads->load_fragments(uris);
  //   auto avg_cell_sizes = array_for_reads->get_average_var_cell_sizes();

  //   CHECK(avg_cell_sizes["d2"] == d2_size);
  //   CHECK(avg_cell_sizes["a2"] == a2_size);
  //   if (a3_size.has_value()) {
  //     CHECK(avg_cell_sizes["a3"] == a3_size);
  //   }
  // }

  ~CPPAverageCellSizeFx() {
    if (vfs_.is_dir(array_name)) {
      vfs_.remove_dir(array_name);
    }
  }

  Context ctx_;
  VFS vfs_;
};

TEST_CASE_METHOD(
    CPPAverageCellSizeFx, "Average cell size", "[average-cell-size]") {
  create_array();
  auto frag1 = write_array({10, 4}, {4, 5});
  // check_avg_cell_size(7, 4);
  // auto frag2 = write_array({10, 400}, {12, 15});
  // check_avg_cell_size(106, 9);
  // auto frag3 = write_array({10, 10, 10, 10}, {400, 15, 400, 15});
  // check_avg_cell_size(58, 108);

  // check_avg_cell_size_for_fragments({frag1}, 7, 4);
  // check_avg_cell_size_for_fragments({frag2}, 205, 13);
  // check_avg_cell_size_for_fragments({frag3}, 10, 207);
  // check_avg_cell_size_for_fragments({frag1, frag2}, 106, 9);
  // check_avg_cell_size_for_fragments({frag2, frag3}, 75, 142);
  // check_avg_cell_size_for_fragments({frag1, frag3}, 9, 139);
  // check_avg_cell_size_for_fragments({frag1, frag2, frag3}, 58, 108);

  // // Validate schema evolution works with average cell sizes.
  // evolve_array();
  // auto frag4 =
  //     write_array({10, 10, 10, 10}, {400, 15, 400, 15}, {{4, 9, 14, 19}});
  // check_avg_cell_size_for_fragments({frag4}, 10, 207, 11);
  // check_avg_cell_size(42, 141, 11);
}
