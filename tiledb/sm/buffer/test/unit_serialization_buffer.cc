/**
 * @file unit_serialization_buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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

#include "test/support/src/mem_helpers.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/buffer/buffer.h"

#include <catch2/catch_test_macros.hpp>

using namespace tiledb::sm;

TEST_CASE(
    "SerializationBuffer: Test default constructor",
    "[serialization_buffer][default_constructor]") {
  SerializationBuffer buff{
      tiledb::test::get_test_memory_tracker()->get_resource(
          MemoryType::SERIALIZATION_BUFFER)};
  CHECK(static_cast<span<const char>>(buff).empty());
}

TEST_CASE(
    "SerializationBuffer: Test owned buffer",
    "[serialization_buffer][owned_buffer]") {
  SerializationBuffer buff{
      tiledb::test::get_test_memory_tracker()->get_resource(
          MemoryType::SERIALIZATION_BUFFER)};
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
}

TEST_CASE(
    "SerializationBuffer: Test non-owned buffer",
    "[serialization_buffer][non_owned]") {
  SerializationBuffer buff{
      tiledb::test::get_test_memory_tracker()->get_resource(
          MemoryType::SERIALIZATION_BUFFER)};
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
}

TEST_CASE(
    "SerializationBuffer: Test owned null-terminated buffer",
    "[serialization_buffer][owned][null_terminated]") {
  SerializationBuffer buff{
      tiledb::test::get_test_memory_tracker()->get_resource(
          MemoryType::SERIALIZATION_BUFFER)};
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
      memory_tracker->get_resource(MemoryType::SERIALIZATION_BUFFER));
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
