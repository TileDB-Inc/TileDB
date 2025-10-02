/**
 * @file tiledb/sm/serialization/test/unit_capnp_group.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file contains serialization tests for groups
 */

#include <capnp/message.h>

#include <test/support/tdb_catch.h>
#include "test/support/src/mem_helpers.h"

#include "tiledb/sm/serialization/group.h"

using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE(
    "Check group member serialization correctly handles relative uris",
    "[group][serialization][relative_uri][regression]") {
  auto group_member = tdb::make_shared<GroupMember>(
      HERE(),
      URI("relative_member", false),
      ObjectType::ARRAY,
      2,
      true,
      std::nullopt,
      false);

  // Serialize
  ::capnp::MallocMessageBuilder message;
  tiledb::sm::serialization::capnp::GroupMember::Builder builder =
      message.initRoot<tiledb::sm::serialization::capnp::GroupMember>();

  auto rc =
      tiledb::sm::serialization::group_member_to_capnp(group_member, &builder);
  REQUIRE(rc.ok());

  tiledb::sm::serialization::capnp::GroupMember::Reader reader =
      (tiledb::sm::serialization::capnp::GroupMember::Reader)builder;
  auto&& [status, clone] =
      tiledb::sm::serialization::group_member_from_capnp(&reader);
  REQUIRE(status.ok());

  REQUIRE(clone.value()->uri().to_string() == "relative_member");
}
