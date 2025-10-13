/**
 * @file tiledb/sm/serialization/test/unit_capnp_nonempty_domain.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file contains serialization tests for nonempty domain
 */

#include <capnp/message.h>

#include <test/support/tdb_catch.h>

#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/serialization/array_schema.h"

using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE(
    "Check serialization correctly marks nonempty domain as "
    "var/fixed size",
    "[nonemptydomain][serialization]") {
  auto dim = make_shared<Dimension>(
      HERE(),
      "index",
      Datatype::UINT32,
      tiledb::test::get_test_memory_tracker());
  uint32_t domain1[2]{1, 64};
  dim->set_domain(&domain1[0]);

  NDRange nd_list = {dim->domain()};

  // Serialize
  ::capnp::MallocMessageBuilder message;
  tiledb::sm::serialization::capnp::NonEmptyDomainList::Builder builder =
      message.initRoot<tiledb::sm::serialization::capnp::NonEmptyDomainList>();
  auto st = tiledb::sm::serialization::utils::serialize_non_empty_domain_rv(
      builder, nd_list, 1);
  REQUIRE(st.ok());

  auto&& [status, clone] =
      tiledb::sm::serialization::utils::deserialize_non_empty_domain_rv(
          builder);

  REQUIRE(status.ok());
  REQUIRE(clone.value()[0].var_size() == false);
}

TEST_CASE(
    "Check serialization correctly handles empty sizes",
    "[nonemptydomain][serialization]") {
  auto dim = make_shared<Dimension>(
      HERE(),
      "index",
      Datatype::UINT32,
      tiledb::test::get_test_memory_tracker());
  uint32_t domain1[2]{1, 64};
  dim->set_domain(&domain1[0]);

  NDRange nd_list = {dim->domain()};

  // Serialize
  ::capnp::MallocMessageBuilder message;
  tiledb::sm::serialization::capnp::NonEmptyDomainList::Builder builder =
      message.initRoot<tiledb::sm::serialization::capnp::NonEmptyDomainList>();
  Status st;

  auto nonEmptyDomainListBuilder = builder.initNonEmptyDomains(1);

  for (uint64_t dimIdx = 0; dimIdx < nd_list.size(); ++dimIdx) {
    const auto& dimNonEmptyDomain = nd_list[dimIdx];

    auto dim_builder = nonEmptyDomainListBuilder[dimIdx];
    dim_builder.setIsEmpty(dimNonEmptyDomain.empty());

    if (!dimNonEmptyDomain.empty()) {
      auto subarray_builder = dim_builder.initNonEmptyDomain();
      st = tiledb::sm::serialization::utils::set_capnp_array_ptr(
          subarray_builder,
          tiledb::sm::Datatype::UINT8,
          dimNonEmptyDomain.data(),
          dimNonEmptyDomain.size());

      if (dimNonEmptyDomain.start_size() != 0) {
        // start_size() is non-zero for var-size dimensions
        auto range_start_sizes = dim_builder.initSizes(1);
        range_start_sizes.set(0, dimNonEmptyDomain.start_size());
      } else {
        // sizes can be empty for non var-size dimensions
        dim_builder.initSizes(0);
      }
    }
  }

  REQUIRE(st.ok());

  auto&& [status, clone] =
      tiledb::sm::serialization::utils::deserialize_non_empty_domain_rv(
          builder);

  REQUIRE(status.ok());
  REQUIRE(clone.value()[0].var_size() == false);
}
