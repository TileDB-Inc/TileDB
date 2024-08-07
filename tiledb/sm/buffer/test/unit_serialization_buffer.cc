/**
 * @file unit_buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests the `SerializationBuffer` class.
 */

#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/buffer/buffer.h"

#include <catch2/catch_test_macros.hpp>

using namespace tiledb::sm;

TEST_CASE(
    "SerializationBuffer: Test default constructor",
    "[serialization_buffer][default_constructor]") {
  SerializationBuffer buff;
  CHECK(static_cast<span<const char>>(buff).empty());
}

TEST_CASE(
    "SerializationBuffer: Test owned buffer",
    "[serialization_buffer][owned_buffer]") {
  SerializationBuffer buff;
  auto data = {'a', 'b', 'c', 'd'};
  buff.assign(data);
  CHECK(buff.is_owned());
  span<const char> buff_span = buff;
  CHECK(memcmp(buff_span.data(), data.begin(), data.size()) == 0);
  // In owned buffers the data is copied, so the pointers should differ.
  CHECK(buff_span.data() != data.begin());
  // The owned mutable span should point to the same memory location as the
  // buffer's read-only span.
  CHECK(buff.owned_mutable_span().data() == buff_span.data());
  CHECK(buff.owned_mutable_span().size() == buff_span.size());

  // Test copying
  auto copied = buff;
  span<const char> copied_span = copied;
  // The copied buffer should have the same size as the original buffer.
  CHECK(copied.size() == buff.size());
  // The copied buffer should point to a different memory location.
  CHECK(copied_span.data() != buff_span.data());
  // The copied buffer should contain the same data as the original buffer.
  CHECK(memcmp(copied_span.data(), buff_span.data(), buff_span.size()) == 0);

  // Test moving
  auto moved = std::move(buff);
  span<const char> moved_span = moved;
  // The moved buffer should have the same size as the original buffer.
  CHECK(moved.size() == data.size());
  // The original buffer should be left empty after the move.
  CHECK(buff.size() == 0);
  // The moved buffer should point to the same memory location as the original
  // was pointing to.
  CHECK(moved_span.data() == buff_span.data());
}

TEST_CASE(
    "SerializationBuffer: Test non-owned buffer",
    "[serialization_buffer][non_owned]") {
  SerializationBuffer buff;
  auto data = {'a', 'b', 'c', 'd'};
  buff.assign(SerializationBuffer::NonOwned, data);
  CHECK_FALSE(buff.is_owned());
  span<const char> buff_span = buff;
  CHECK(memcmp(buff_span.data(), data.begin(), data.size()) == 0);
  // In non-owned buffers the data is not copied, so the pointers should be the
  // same.
  CHECK(buff_span.data() == data.begin());
  // Accessing the owned mutable span should throw.
  CHECK_THROWS(buff.owned_mutable_span());

  // Test copying
  auto copied = buff;
  span<const char> copied_span = copied;
  // The copied buffer should have the same size as the original buffer.
  CHECK(copied.size() == buff.size());
  // The copied buffer should point to the same memory location.
  CHECK(copied_span.data() == buff_span.data());

  // Test moving
  auto moved = std::move(buff);
  span<const char> moved_span = moved;
  // The moved buffer should have the same size as the original buffer.
  CHECK(moved.size() == data.size());
  // The original buffer should be left empty after the move.
  CHECK(buff.size() == 0);
  // The moved buffer should point to the same memory location as the original
  // was pointing to.
  CHECK(moved_span.data() == buff_span.data());
}

TEST_CASE(
    "SerializationBuffer: Test owned null-terminated buffer",
    "[serialization_buffer][owned][null_terminated]") {
  SerializationBuffer buff;
  std::string_view data = "abcd";
  buff.assign_null_terminated(data);
  span<const char> buff_span = buff;
  CHECK(buff.size() == data.size() + 1);
  CHECK(memcmp(buff_span.data(), data.data(), data.size()) == 0);
  CHECK(static_cast<span<const char>>(buff)[buff.size() - 1] == '\0');
}

TEST_CASE(
    "SerializationBuffer: Test memory tracking",
    "[serialization_buffer][memory_tracking]") {
  tiledb::sm::MemoryTrackerManager memory_tracker_manager;
  auto memory_tracker = memory_tracker_manager.create_tracker();
  auto resource = dynamic_cast<tiledb::sm::MemoryTrackerResource*>(
      memory_tracker->get_resource(MemoryType::CONSOLIDATION_BUFFERS));
  REQUIRE(resource != nullptr);
  SerializationBuffer buff(resource);

  // Get the current memory usage, after accounting for the size of the vector
  // itself.
  auto existing_usage = resource->get_count();
  auto data = {'a', 'b', 'c', 'd'};
  SECTION("Owned buffer") {
    buff.assign(data);
    CHECK(resource->get_count() - existing_usage == data.size());
    // Clearing the buffer should bring the memory usage back to the original
    buff.assign(std::string_view(""));
    CHECK(existing_usage == resource->get_count());
  }

  SECTION("Non-owned buffer") {
    buff.assign(SerializationBuffer::NonOwned, data);
    CHECK(existing_usage == resource->get_count());
  }
}
