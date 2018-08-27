/**
 * @file unit-filter-buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * Tests the `FilterBuffer` class.
 */

#include "tiledb/sm/filter/filter_buffer.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

static void check_buf(const char* check, std::initializer_list<char> answer) {
  int idx = 0;
  for (char c : answer) {
    CHECK(check[idx++] == c);
  }
}

static void check_buf(const int* check, std::initializer_list<int> answer) {
  int idx = 0;
  for (int val : answer) {
    CHECK(check[idx++] == val);
  }
}

TEST_CASE("FilterBuffer: Test init", "[filter], [filter-buffer]") {
  FilterStorage storage;
  FilterBuffer fbuf(&storage);

  // Check reads and writes with a buffer not owned by the FilterBuffer.
  const char data[] = {0, 1, 2, 3, 4, 5};
  Buffer buff;
  CHECK(buff.write(data, sizeof(data)).ok());
  CHECK(buff.size() == 6);
  CHECK(fbuf.size() == 0);
  CHECK(fbuf.init(buff.data(), buff.size()).ok());
  CHECK(fbuf.size() == 6);

  // Check reads
  char data_r[sizeof(data)];
  CHECK(fbuf.read(data_r, 3).ok());
  check_buf(data_r, {0, 1, 2});
  CHECK(fbuf.read(&data_r[3], 2).ok());
  check_buf(data_r, {0, 1, 2, 3, 4});
  CHECK(!fbuf.read(&data_r[3], 2).ok());
  std::memset(data_r, (char)0, sizeof(data_r));
  fbuf.set_offset(4);
  CHECK(fbuf.read(data_r, 2).ok());
  check_buf(data_r, {4, 5, 0, 0, 0, 0});
  CHECK(fbuf.size() == 6);

  CHECK(!fbuf.init(buff.data(), buff.size()).ok());
  CHECK(fbuf.size() == 6);

  // Overwrite in middle
  fbuf.set_offset(2);
  char data2[] = {6, 7, 8};
  CHECK(fbuf.write(data2, sizeof(data2)).ok());
  CHECK(fbuf.size() == 6);
  char data_r2[8];
  CHECK(!fbuf.read(data_r2, 6).ok());
  fbuf.set_offset(0);
  CHECK(fbuf.read(data_r2, 6).ok());
  check_buf(data_r2, {0, 1, 6, 7, 8, 5});

  // Check can't write past end.
  fbuf.set_offset(5);
  CHECK(!fbuf.write(data2, sizeof(data2)).ok());
}

TEST_CASE("FilterBuffer: Test prepend", "[filter], [filter-buffer]") {
  FilterStorage storage;
  FilterBuffer fbuf(&storage);

  const char data[] = {0, 1, 2, 3, 4, 5};

  // Prepend a first buffer and write to it.
  CHECK(fbuf.prepend_buffer(sizeof(data)).ok());
  CHECK(fbuf.size() == 0);
  fbuf.reset_offset();
  CHECK(fbuf.write(data, sizeof(data)).ok());
  CHECK(fbuf.size() == 6);

  char data_r[sizeof(data)];
  fbuf.reset_offset();
  CHECK(fbuf.read(data_r, sizeof(data)).ok());
  check_buf(data_r, {0, 1, 2, 3, 4, 5});

  // Overwrite in-place
  fbuf.reset_offset();
  char c = 6;
  CHECK(fbuf.write(&c, sizeof(char)).ok());
  CHECK(fbuf.size() == 6);
  fbuf.reset_offset();
  CHECK(fbuf.read(data_r, sizeof(data)).ok());
  check_buf(data_r, {6, 1, 2, 3, 4, 5});

  // Prepend a buffer of 2 bytes and overwrite in place, spanning both buffers.
  CHECK(fbuf.prepend_buffer(2).ok());
  CHECK(fbuf.size() == 6);
  fbuf.set_offset(0);
  char data2[] = {7, 8, 9, 10, 11};
  CHECK(fbuf.write(data2, sizeof(data2)).ok());
  CHECK(fbuf.size() == 8);
  char data_r2[8];
  fbuf.set_offset(0);
  CHECK(fbuf.read(data_r2, 8).ok());
  check_buf(data_r2, {7, 8, 9, 10, 11, 3, 4, 5});

  // Prepend another buffer of 3 bytes, and only write to it partially.
  CHECK(fbuf.prepend_buffer(3).ok());
  CHECK(fbuf.size() == 8);
  fbuf.set_offset(0);
  char data3[] = {12};
  CHECK(fbuf.write(data3, sizeof(data3)).ok());
  CHECK(fbuf.size() == 9);
  char data_r3[9];
  fbuf.set_offset(0);
  CHECK(fbuf.read(data_r3, 7).ok());
  check_buf(data_r3, {12, 7, 8, 9, 10, 11, 3});

  // At this point fbuf has three buffers with the contents:
  // 12 _ _ | 7 8 | 9 10 11 3 4 5
  // where _ indicates unused space.

  // Check getting a pointer to a buffer and modifying it directly.
  // Note that this expands the explicit buffer in-place and doesn't spill
  // over into the next buffer (since we're not writing via fbuf).
  CHECK(fbuf.num_buffers() == 3);
  Buffer* b1 = fbuf.buffer_ptr(1);
  CHECK(b1 != nullptr);
  CHECK(b1->size() == 2);
  CHECK(b1->value<char>(0) == 7);
  CHECK(b1->value<char>(1) == 8);
  CHECK(fbuf.size() == 9);
  const char data4[] = {13, 14, 15, 16, 17};
  CHECK(b1->write(data4, sizeof(data4)).ok());
  CHECK(b1->size() == 5);
  CHECK(fbuf.size() == 12);
  char data_r4[100];
  fbuf.reset_offset();
  CHECK(fbuf.read(data_r4, fbuf.size()).ok());
  check_buf(data_r4, {12, 13, 14, 15, 16, 17, 9, 10, 11, 3, 4, 5});
}

TEST_CASE(
    "FilterBuffer: Test multiple reads/writes", "[filter], [filter-buffer]") {
  FilterStorage storage;
  FilterBuffer fbuf(&storage);

  CHECK(fbuf.prepend_buffer(sizeof(int)).ok());
  CHECK(fbuf.prepend_buffer(sizeof(int)).ok());

  const int data1[] = {1}, data2[] = {2};
  fbuf.reset_offset();
  CHECK(fbuf.write(data1, sizeof(int)).ok());
  CHECK(fbuf.write(data2, sizeof(int)).ok());

  int data_r[2] = {0};
  fbuf.reset_offset();
  CHECK(fbuf.read(data_r, 2 * sizeof(int)).ok());
  check_buf(data_r, {1, 2});

  data_r[0] = data_r[1] = 0;
  fbuf.reset_offset();
  CHECK(fbuf.read(data_r, sizeof(int)).ok());
  check_buf(data_r, {1, 0});
  CHECK(fbuf.read(&data_r[1], sizeof(int)).ok());
  check_buf(data_r, {1, 2});
}

TEST_CASE("FilterBuffer: Test clear", "[filter], [filter-buffer]") {
  FilterStorage storage;
  FilterBuffer fbuf(&storage);

  const char init_data[] = {0, 1, 2};
  Buffer buff;
  CHECK(buff.write(init_data, sizeof(init_data)).ok());
  CHECK(buff.size() == 3);
  CHECK(fbuf.size() == 0);
  CHECK(fbuf.init(buff.data(), buff.size()).ok());

  CHECK(fbuf.prepend_buffer(3).ok());
  CHECK(fbuf.prepend_buffer(3).ok());
  CHECK(fbuf.prepend_buffer(3).ok());
  CHECK(fbuf.size() == 3);

  // Write to prepended buffers
  const char data[] = {3, 4, 5, 6, 7, 8, 9, 10, 11};
  fbuf.reset_offset();
  CHECK(fbuf.write(data, sizeof(data)).ok());
  CHECK(fbuf.size() == 12);
  char data_r[12];
  fbuf.reset_offset();
  CHECK(fbuf.read(data_r, 12).ok());
  check_buf(data_r, {3, 4, 5, 6, 7, 8, 9, 10, 11, 0, 1, 2});

  // Clear everything
  CHECK(fbuf.clear().ok());
  CHECK(fbuf.size() == 0);

  // No buffers to write into
  CHECK(!fbuf.write(data, sizeof(data)).ok());

  // Prepend a new buffer and write again.
  fbuf.prepend_buffer(9);
  fbuf.reset_offset();
  CHECK(fbuf.write(data, sizeof(data)).ok());
  CHECK(fbuf.size() == 9);
}

TEST_CASE("FilterBuffer: Test copy_to", "[filter], [filter-buffer]") {
  FilterStorage storage;
  FilterBuffer fbuf(&storage);

  const char init_data[] = {0, 1, 2};
  Buffer buff;
  CHECK(buff.write(init_data, sizeof(init_data)).ok());
  CHECK(fbuf.init(buff.data(), buff.size()).ok());

  CHECK(fbuf.prepend_buffer(3).ok());
  CHECK(fbuf.prepend_buffer(3).ok());
  CHECK(fbuf.prepend_buffer(3).ok());

  // Write to prepended buffers
  const char data[] = {3, 4, 5, 6, 7, 8, 9, 10, 11};
  fbuf.reset_offset();
  CHECK(fbuf.write(data, sizeof(data)).ok());
  char data_r[12];
  fbuf.reset_offset();
  CHECK(fbuf.read(data_r, 12).ok());
  check_buf(data_r, {3, 4, 5, 6, 7, 8, 9, 10, 11, 0, 1, 2});

  Buffer buff2;
  buff2.write(data_r, 1);
  CHECK(fbuf.copy_to(&buff2).ok());
  CHECK(buff2.size() == 13);
  CHECK(fbuf.size() == 12);
  char data_r2[13];
  buff2.reset_offset();
  buff2.read(data_r2, 13);
  check_buf(data_r2, {3, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0, 1, 2});
}

TEST_CASE("FilterBuffer: Test append_view", "[filter], [filter-buffer]") {
  FilterStorage storage;
  FilterBuffer fbuf(&storage);

  const char init_data[] = {0, 1, 2};
  Buffer buff;
  CHECK(buff.write(init_data, sizeof(init_data)).ok());
  CHECK(fbuf.init(buff.data(), buff.size()).ok());

  FilterBuffer fbuf2(&storage);
  CHECK(fbuf2.append_view(&fbuf, 1, 2).ok());
  char data_r[100];
  fbuf2.reset_offset();
  CHECK(fbuf2.read(data_r, 2).ok());
  check_buf(data_r, {1, 2});

  // Write to prepended buffers
  CHECK(fbuf.prepend_buffer(3).ok());
  CHECK(fbuf.prepend_buffer(3).ok());
  CHECK(fbuf.prepend_buffer(3).ok());
  const char data[] = {3, 4, 5, 6, 7, 8, 9, 10, 11};
  fbuf.reset_offset();
  CHECK(fbuf.write(data, sizeof(data)).ok());

  // Check view unaffected
  fbuf2.reset_offset();
  std::memset(data_r, 0, sizeof(data_r));
  CHECK(fbuf2.read(data_r, 2).ok());
  check_buf(data_r, {1, 2});

  // Write overlapping the view, and check that reading from the view sees it.
  fbuf.set_offset(8);
  CHECK(fbuf.write(data, 3).ok());
  fbuf2.reset_offset();
  std::memset(data_r, 0, sizeof(data_r));
  CHECK(fbuf2.read(data_r, 2).ok());
  check_buf(data_r, {5, 2});

  CHECK(fbuf.clear().ok());
  CHECK(fbuf2.clear().ok());
}

TEST_CASE("FilterBuffer: Test view reclaim", "[filter], [filter-buffer]") {
  FilterStorage storage;
  FilterBuffer fbuf(&storage), fbuf2(&storage);

  CHECK(storage.num_available() == 0);
  CHECK(storage.num_in_use() == 0);

  CHECK(fbuf.prepend_buffer(sizeof(uint64_t)).ok());
  CHECK(storage.num_available() == 0);
  CHECK(storage.num_in_use() == 1);
  uint64_t val = 100;
  CHECK(fbuf.write(&val, sizeof(uint64_t)).ok());

  CHECK(fbuf2.append_view(&fbuf, 0, sizeof(uint64_t)).ok());
  CHECK(storage.num_available() == 0);
  CHECK(storage.num_in_use() == 1);

  // This should not reclaim the original buffer due to the view on it.
  fbuf.clear();
  CHECK(storage.num_available() == 0);
  CHECK(storage.num_in_use() == 1);

  // Now it should reclaim.
  fbuf2.clear();
  CHECK(storage.num_available() == 1);
  CHECK(storage.num_in_use() == 0);
}

TEST_CASE("FilterBuffer: Test fixed allocation", "[filter], [filter-buffer]") {
  FilterStorage storage;
  FilterBuffer fbuf(&storage);
  Buffer fixed;
  const unsigned nelts = 100;
  CHECK(fixed.realloc(nelts * sizeof(unsigned)).ok());
  std::memset(fixed.data(), 0, fixed.alloced_size());
  CHECK(fbuf.set_fixed_allocation(fixed.data(), fixed.alloced_size()).ok());
  CHECK(fbuf.size() == fixed.alloced_size());

  // Error setting multiple fixed allocations
  CHECK(!fbuf.set_fixed_allocation(nullptr, 0).ok());

  SECTION("- Prepend buffer") {
    // Any size smaller than the fixed alloc will have the same effect.
    CHECK(fbuf.prepend_buffer(0).ok());
    fbuf.reset_offset();
    for (unsigned i = 0; i < nelts; i++)
      CHECK(fbuf.write(&i, sizeof(unsigned)).ok());
    for (unsigned i = 0; i < nelts; i++)
      CHECK(fixed.value<unsigned>(i * sizeof(unsigned)) == i);

    // Error writing past fixed alloc
    unsigned v = 101;
    CHECK(!fbuf.write(&v, sizeof(unsigned)).ok());
    CHECK(fbuf.size() == nelts * sizeof(unsigned));

    // Error prepending again
    CHECK(!fbuf.prepend_buffer(0).ok());
    // Error append after prepend
    CHECK(!fbuf.append_view(&fbuf, 0, 0).ok());

    // Prepend allowed after clear.
    CHECK(fbuf.clear().ok());
    CHECK(fbuf.prepend_buffer(0).ok());
  }

  SECTION("- Prepend too large buffer") {
    CHECK(!fbuf.prepend_buffer(nelts * sizeof(unsigned) + 1).ok());
  }

  SECTION("- Append view") {
    // Set up data to view
    FilterBuffer fbuf2(&storage);
    CHECK(fbuf2.prepend_buffer(fixed.alloced_size()).ok());
    fbuf2.reset_offset();
    for (unsigned i = 0; i < nelts; i++)
      CHECK(fbuf2.write(&i, sizeof(unsigned)).ok());

    // Check that append copies data from the view.
    CHECK(fbuf.append_view(&fbuf2, 0, (nelts / 2) * sizeof(unsigned)).ok());
    for (unsigned i = 0; i < nelts; i++) {
      if (i < nelts / 2)
        CHECK(fixed.value<unsigned>(i * sizeof(unsigned)) == i);
      else
        CHECK(fixed.value<unsigned>(i * sizeof(unsigned)) == 0);
    }

    // Error appending multiple times
    CHECK(!fbuf.append_view(&fbuf2, 0, (nelts / 2) * sizeof(unsigned)).ok());
    // Error prepend after append
    CHECK(!fbuf.prepend_buffer(0).ok());

    // Append allowed after clear.
    CHECK(fbuf.clear().ok());
    CHECK(fbuf.append_view(&fbuf2, 0, (nelts / 2) * sizeof(unsigned)).ok());
  }

  SECTION("- Append too large view") {
    CHECK(!fbuf.append_view(&fbuf, 0, nelts * sizeof(unsigned) + 1).ok());
  }
}

TEST_CASE("FilterBuffer: Test read-only", "[filter], [filter-buffer]") {
  FilterStorage storage;
  FilterBuffer fbuf(&storage);

  const char init_data[] = {0, 1, 2};
  Buffer buff;
  CHECK(buff.write(init_data, sizeof(init_data)).ok());
  CHECK(fbuf.init(buff.data(), buff.size()).ok());
  fbuf.reset_offset();

  fbuf.set_read_only(true);
  CHECK(fbuf.read_only());
  CHECK(!fbuf.prepend_buffer(0).ok());
  CHECK(!fbuf.append_view(&fbuf, 0, 0).ok());
  CHECK(!fbuf.write(init_data, 1).ok());
  CHECK(!fbuf.clear().ok());
  CHECK(!fbuf.swap(fbuf).ok());
  CHECK(!fbuf.set_fixed_allocation(nullptr, 0).ok());

  char data[3] = {0};
  CHECK(fbuf.read(data, 3).ok());
  check_buf(data, {0, 1, 2});
  std::memset(data, 0, sizeof(data));
  fbuf.set_offset(1);
  CHECK(fbuf.read(data, 2).ok());
  check_buf(data, {1, 2});
  CHECK(!fbuf.read(data, 1).ok());
}