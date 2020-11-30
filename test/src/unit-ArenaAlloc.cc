/**
 * @file unit-ArenaAlloc.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
 * Tests the `ArenaAlloc` class.
 */

#include "tiledb/common/arena_alloc.h"

#include <catch.hpp>
#include <iostream>
#include <math.h>

using namespace tiledb::common;

TEST_CASE(
    "ArenaAlloc: Test default constructor",
    "[ArenaAlloc][default_constructor]") {
  // Test the default constructor and verify the empty state.
  ArenaAlloc arena_alloc;
  REQUIRE(arena_alloc.capacity() == 0);
}

TEST_CASE(
    "ArenaAlloc: Test init",
    "[ArenaAlloc][init]") {
  ArenaAlloc arena_alloc;

  // Check that we are unable to initialize with a capacity
  // of 0.
  REQUIRE(!arena_alloc.init(0).ok());

  // Check that we unable to initialize with a capacity that
  // is not a power-of-two.
  REQUIRE(!arena_alloc.init(static_cast<uint64_t>(pow(2, 1)) - 1).ok());
  REQUIRE(!arena_alloc.init(static_cast<uint64_t>(pow(2, 2)) - 1).ok());
  REQUIRE(!arena_alloc.init(static_cast<uint64_t>(pow(2, 3)) - 1).ok());
  REQUIRE(!arena_alloc.init(static_cast<uint64_t>(pow(2, 4)) - 1).ok());
  REQUIRE(!arena_alloc.init(static_cast<uint64_t>(pow(2, 5)) - 1).ok());
  REQUIRE(!arena_alloc.init(static_cast<uint64_t>(pow(2, 6)) - 1).ok());
  REQUIRE(!arena_alloc.init(static_cast<uint64_t>(pow(2, 7)) - 1).ok());
  REQUIRE(!arena_alloc.init(static_cast<uint64_t>(pow(2, 8)) - 1).ok());
  REQUIRE(!arena_alloc.init(static_cast<uint64_t>(pow(2, 9)) - 1).ok());
  REQUIRE(!arena_alloc.init(static_cast<uint64_t>(pow(2, 10)) - 1).ok());

  // Check that we are able to initialize with a valid capacity.
  const uint64_t capacity = 1024;
  REQUIRE(arena_alloc.init(capacity).ok());
  REQUIRE(arena_alloc.capacity() == capacity);

  // Check that we are unable to initialize twice.
  REQUIRE(!arena_alloc.init(capacity).ok());
}

TEST_CASE(
    "ArenaAlloc: Test destroy",
    "[ArenaAlloc][destroy]") {
  ArenaAlloc arena_alloc;

  // Check that `destroy` fails if the instance has not
  // been initialized.
  REQUIRE(!arena_alloc.destroy().ok());

  const uint64_t capacity = 1024;
  REQUIRE(arena_alloc.init(capacity).ok());
  REQUIRE(arena_alloc.capacity() == capacity);

  // Check that we can destroy the instance after a
  // successful initialization.
  REQUIRE(arena_alloc.destroy().ok());
  REQUIRE(arena_alloc.capacity() == 0);

  // Check that we can initialize after a successful
  // destruction.
  const uint64_t capacity2 = 2048;
  REQUIRE(arena_alloc.init(capacity2).ok());
  REQUIRE(arena_alloc.capacity() == capacity2);

  // Check that we can destroy the instance after a
  // second, successful initialization.
  REQUIRE(arena_alloc.destroy().ok());
  REQUIRE(arena_alloc.capacity() == 0);
}

TEST_CASE(
    "ArenaAlloc: Test malloc",
    "[ArenaAlloc][malloc]") {
  ArenaAlloc arena_alloc;

  // Ensure we are unable to allocate even a single byte
  // before calling `init`.
  REQUIRE(arena_alloc.malloc(1) == nullptr);

  const uint64_t capacity = 1024;
  REQUIRE(arena_alloc.init(capacity).ok());

  // Ensure we are unable to allocate 0 bytes.
  REQUIRE(arena_alloc.malloc(0) == nullptr);

  // Ensure we are unable to allocate `capacity + 1` bytes.
  REQUIRE(arena_alloc.malloc(capacity + 1) == nullptr);

  // Allocate 100 bytes, which will consume a 128 byte block.
  // This leaves 896 bytes unallocated.
  void* ptr1 = arena_alloc.malloc(100);
  REQUIRE(ptr1 != nullptr);

  // Allocate 500 bytes, which will consume a 512 byte block.
  // This leaves 384 bytes unallocated.
  void* ptr2 = arena_alloc.malloc(500);
  REQUIRE(ptr2 != nullptr);

  // Allocate 64 bytes, which will consume a 64 byte block.
  // This leaves 320 bytes unallocated.
  void* ptr3 = arena_alloc.malloc(64);
  REQUIRE(ptr3 != nullptr);

  // Allocate 20 bytes, which will consume a 32 byte block.
  // This leaves 256 bytes unallocated.
  void* ptr4 = arena_alloc.malloc(20);
  REQUIRE(ptr4 != nullptr);

  // Allocate 32 bytes, which will consume a 32 byte block.
  // This leaves 256 bytes unallocated.
  void* ptr5 = arena_alloc.malloc(32);
  REQUIRE(ptr5 != nullptr);

  // Allocate 65 bytes, which will consume a 128 byte block.
  // This leaves 128 bytes unallocated.
  void* ptr6 = arena_alloc.malloc(65);
  REQUIRE(ptr6 != nullptr);

  // Allocate 65 bytes, which will consume a 128 byte block.
  // This leaves 0 bytes unallocated.
  void* ptr7 = arena_alloc.malloc(65);
  REQUIRE(ptr7 != nullptr);

  // The arena is entirely allocated. Ensure all allocations
  // fails in the range of [0, capacity].
  for (uint64_t i = 0; i <= capacity; ++i)
    REQUIRE(arena_alloc.malloc(i) == nullptr);

  // Free `ptr4`, which is a 32 byte block. This leaves
  // 32 bytes unallocated.
  arena_alloc.free(ptr4);

  // Allocate 10 bytes, which will consume a 16 byte block.
  // This leaves 16 bytes unallocated.
  void* ptr4_1 = arena_alloc.malloc(10);
  REQUIRE(ptr4_1 != nullptr);

  // Allocate 6 bytes, which will consume an 8 byte block.
  // This leaves 8 bytes unallocated.
  void* ptr4_2 = arena_alloc.malloc(6);
  REQUIRE(ptr4_2 != nullptr);

  // Allocate 7 bytes, which will consume an 8 byte block.
  // This leaves 0 bytes unallocated.
  void* ptr4_3 = arena_alloc.malloc(7);
  REQUIRE(ptr4_3 != nullptr);

  // The arena is entirely allocated. Ensure all allocations
  // fails in the range of [0, capacity].
  for (uint64_t i = 0; i <= capacity; ++i)
    REQUIRE(arena_alloc.malloc(i) == nullptr);

  // Free all memory.
  arena_alloc.free(ptr1);
  arena_alloc.free(ptr2);
  arena_alloc.free(ptr3);
  arena_alloc.free(ptr4_1);
  arena_alloc.free(ptr4_2);
  arena_alloc.free(ptr4_3);
  arena_alloc.free(ptr5);
  arena_alloc.free(ptr6);
  arena_alloc.free(ptr7);
}

