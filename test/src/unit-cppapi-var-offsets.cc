/**
 * @file   unit-var_offsets.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB Inc.
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
 * Tests the different configurations of var-sized attribute offsets using the
 * C++ API.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

void create_sparse_array(const std::string& array_name) {
  Context ctx;
  VFS vfs(ctx);

  // Create the array
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  Domain dom(ctx);
  dom.add_dimension(Dimension::create<int64_t>(ctx, "d1", {{1, 4}}, 2))
      .add_dimension(Dimension::create<int64_t>(ctx, "d2", {{1, 4}}, 2));

  ArraySchema schema(ctx, TILEDB_SPARSE);
  Attribute attr(ctx, "attr", TILEDB_INT32);
  attr.set_cell_val_num(TILEDB_VAR_NUM);
  schema.add_attribute(attr);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_domain(dom);

  Array::create(array_name, schema);
}

void write_sparse_array(
    Context ctx,
    const std::string& array_name,
    std::vector<int32_t>& data,
    std::vector<uint64_t>& data_offsets,
    tiledb_layout_t layout) {
  std::vector<int64_t> d1 = {1, 2, 3, 4};
  std::vector<int64_t> d2 = {2, 1, 3, 4};

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(layout);
  query.set_buffer("d1", d1);
  query.set_buffer("d2", d2);
  query.set_buffer("attr", data_offsets, data);
  CHECK_NOTHROW(query.submit());

  // Finalize is necessary in global writes, otherwise a no-op
  query.finalize();

  array.close();
}

void write_sparse_array(
    Context ctx,
    const std::string& array_name,
    std::vector<int32_t>& data,
    std::vector<uint32_t>& data_offsets,
    tiledb_layout_t layout) {
  std::vector<int64_t> d1 = {1, 2, 3, 4};
  std::vector<int64_t> d2 = {2, 1, 3, 4};

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(layout);
  query.set_buffer("d1", d1);
  query.set_buffer("d2", d2);
  query.set_buffer(
      "attr",
      reinterpret_cast<uint64_t*>(data_offsets.data()),
      data_offsets.size(),
      data.data(),
      data.size());
  CHECK_NOTHROW(query.submit());

  // Finalize is necessary in global writes, otherwise a no-op
  query.finalize();

  array.close();
}

void read_and_check_sparse_array(
    Context ctx,
    const std::string& array_name,
    std::vector<int32_t>& expected_data,
    std::vector<uint64_t>& expected_offsets) {
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);

  std::vector<int32_t> attr_val(expected_data.size());
  std::vector<uint64_t> attr_off(expected_offsets.size());

  query.set_buffer("attr", attr_off, attr_val);

  CHECK_NOTHROW(query.submit());

  // Check the element offsets are properly returned
  CHECK(attr_val == expected_data);
  CHECK(attr_off == expected_offsets);

  array.close();
}

void read_and_check_sparse_array(
    Context ctx,
    const std::string& array_name,
    std::vector<int32_t>& expected_data,
    std::vector<uint32_t>& expected_offsets) {
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);

  std::vector<int32_t> attr_val(expected_data.size());
  std::vector<uint32_t> attr_off(expected_offsets.size());
  // Read using a 32-bit vector, but cast it to 64-bit pointer so that the API
  // accepts it
  query.set_buffer(
      "attr",
      reinterpret_cast<uint64_t*>(attr_off.data()),
      attr_off.size(),
      attr_val.data(),
      attr_val.size());
  CHECK_NOTHROW(query.submit());

  // Check the element offsets are properly returned
  CHECK(attr_val == expected_data);
  CHECK(attr_off == expected_offsets);

  array.close();
}

void reset_read_buffers(
    std::vector<int32_t>& data, std::vector<uint64_t>& offsets) {
  data.assign(data.size(), 0);
  offsets.assign(offsets.size(), 0);
}

void partial_read_and_check_sparse_array(
    Context ctx,
    const std::string& array_name,
    std::vector<int32_t>& exp_data_part1,
    std::vector<uint64_t>& exp_off_part1,
    std::vector<int32_t>& exp_data_part2,
    std::vector<uint64_t>& exp_off_part2) {
  // The size of read buffers is smaller than the size
  // of all the data, so we'll do partial reads
  std::vector<int32_t> attr_val(exp_data_part1.size());
  std::vector<uint64_t> attr_off(exp_off_part1.size());

  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);
  query.set_buffer("attr", attr_off, attr_val);

  // Check that first partial read returns expected results
  CHECK_NOTHROW(query.submit());
  Query::Status status = query.query_status();
  CHECK(status == Query::Status::INCOMPLETE);
  CHECK(attr_val == exp_data_part1);
  CHECK(attr_off == exp_off_part1);

  // Check that second partial read returns expected results
  CHECK_NOTHROW(query.submit());
  status = query.query_status();
  CHECK(status == Query::Status::COMPLETE);
  CHECK(attr_val == exp_data_part2);
  CHECK(attr_off == exp_off_part2);

  array.close();
}

void create_dense_array(const std::string& array_name) {
  Context ctx;
  VFS vfs(ctx);

  // Create the array
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  Domain dom(ctx);
  dom.add_dimension(Dimension::create<int64_t>(ctx, "d1", {{1, 4}}, 2))
      .add_dimension(Dimension::create<int64_t>(ctx, "d2", {{1, 4}}, 2));

  ArraySchema schema(ctx, TILEDB_DENSE);
  Attribute attr(ctx, "attr", TILEDB_INT32);
  attr.set_cell_val_num(TILEDB_VAR_NUM);
  schema.add_attribute(attr);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_domain(dom);

  Array::create(array_name, schema);
}

void write_dense_array(
    Context ctx,
    const std::string& array_name,
    std::vector<int32_t>& data,
    std::vector<uint64_t>& data_offsets,
    tiledb_layout_t layout,
    std::shared_ptr<Config> config = nullptr) {
  std::vector<int64_t> d1 = {1, 1, 2, 2};
  std::vector<int64_t> d2 = {1, 2, 1, 2};

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);

  if (config != nullptr) {
    query.set_config(*config);
  }

  query.set_buffer("attr", data_offsets, data);
  query.set_layout(layout);
  if (layout == TILEDB_UNORDERED) {
    // sparse write to dense array
    query.set_buffer("d1", d1);
    query.set_buffer("d2", d2);
  } else {
    query.set_subarray<int64_t>({1, 2, 1, 2});
  }

  CHECK_NOTHROW(query.submit());

  // Finalize is necessary in global writes, otherwise a no-op
  query.finalize();

  array.close();
}

void write_dense_array(
    Context ctx,
    const std::string& array_name,
    std::vector<int32_t>& data,
    std::vector<uint32_t>& data_offsets,
    tiledb_layout_t layout,
    std::shared_ptr<Config> config = nullptr) {
  std::vector<int64_t> d1 = {1, 1, 2, 2};
  std::vector<int64_t> d2 = {1, 2, 1, 2};

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);

  if (config != nullptr) {
    query.set_config(*config);
  }

  // Write using a 32-bit vector, but cast it to 64-bit pointer so that the API
  // accepts it
  query.set_buffer(
      "attr",
      reinterpret_cast<uint64_t*>(data_offsets.data()),
      data_offsets.size(),
      data.data(),
      data.size());
  query.set_layout(layout);
  if (layout == TILEDB_UNORDERED) {
    // sparse write to dense array
    query.set_buffer("d1", d1);
    query.set_buffer("d2", d2);
  } else {
    query.set_subarray<int64_t>({1, 2, 1, 2});
  }

  CHECK_NOTHROW(query.submit());
  // Finalize is necessary in global writes, otherwise a no-op
  query.finalize();

  array.close();
}

void read_and_check_dense_array(
    Context ctx,
    const std::string& array_name,
    std::vector<int32_t>& expected_data,
    std::vector<uint64_t>& expected_offsets,
    std::shared_ptr<Config> config = nullptr) {
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);

  if (config != nullptr) {
    query.set_config(*config);
  }

  std::vector<int32_t> attr_val(expected_data.size());
  std::vector<uint64_t> attr_off(expected_offsets.size());
  query.set_subarray<int64_t>({1, 2, 1, 2});
  query.set_buffer("attr", attr_off, attr_val);
  CHECK_NOTHROW(query.submit());

  // Check the element offsets are properly returned
  CHECK(attr_val == expected_data);
  CHECK(attr_off == expected_offsets);

  array.close();
}

void read_and_check_dense_array(
    Context ctx,
    const std::string& array_name,
    std::vector<int32_t>& expected_data,
    std::vector<uint32_t>& expected_offsets,
    std::shared_ptr<Config> config = nullptr) {
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);

  if (config != nullptr) {
    query.set_config(*config);
  }

  std::vector<int32_t> attr_val(expected_data.size());
  std::vector<uint32_t> attr_off(expected_offsets.size());
  query.set_subarray<int64_t>({1, 2, 1, 2});
  // Read using a 32-bit vector, but cast it to 64-bit pointer so that the API
  // accepts it
  query.set_buffer(
      "attr",
      reinterpret_cast<uint64_t*>(attr_off.data()),
      attr_off.size(),
      attr_val.data(),
      attr_val.size());
  CHECK_NOTHROW(query.submit());

  // Check the element offsets are properly returned
  CHECK(attr_val == expected_data);
  CHECK(attr_off == expected_offsets);

  array.close();
}

void partial_read_and_check_dense_array(
    Context ctx,
    const std::string& array_name,
    std::vector<int32_t>& exp_data_part1,
    std::vector<uint64_t>& exp_off_part1,
    std::vector<int32_t>& exp_data_part2,
    std::vector<uint64_t>& exp_off_part2) {
  // The size of read buffers is smaller than the size
  // of all the data, so we'll do partial reads
  std::vector<int32_t> attr_val(exp_data_part1.size());
  std::vector<uint64_t> attr_off(exp_off_part1.size());

  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);
  query.set_subarray<int64_t>({1, 2, 1, 2});
  query.set_buffer("attr", attr_off, attr_val);

  // Check that first partial read returns expected results
  CHECK_NOTHROW(query.submit());
  CHECK(attr_val == exp_data_part1);
  CHECK(attr_off == exp_off_part1);

  // Check that second partial read returns expected results
  CHECK_NOTHROW(query.submit());
  CHECK(attr_val == exp_data_part2);
  CHECK(attr_off == exp_off_part2);

  array.close();
}

TEST_CASE(
    "C++ API: Test element offsets : sparse array",
    "[var-offsets][element-offset]") {
  std::string array_name = "test_element_offset";
  create_sparse_array(array_name);

  std::vector<int32_t> data = {1, 2, 3, 4, 5, 6};
  Context ctx;

  SECTION("Byte offsets (default case)") {
    Config config = ctx.config();
    CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");

    std::vector<uint64_t> byte_offsets = {0, 4, 12, 20};

    SECTION("Unordered write") {
      write_sparse_array(ctx, array_name, data, byte_offsets, TILEDB_UNORDERED);
      read_and_check_sparse_array(ctx, array_name, data, byte_offsets);
    }
    SECTION("Global order write") {
      write_sparse_array(
          ctx, array_name, data, byte_offsets, TILEDB_GLOBAL_ORDER);
      read_and_check_sparse_array(ctx, array_name, data, byte_offsets);
    }
  }

  SECTION("Element offsets") {
    Config config;
    // Change config of offsets format from bytes to elements
    config["sm.var_offsets.mode"] = "elements";
    Context ctx(config);

    std::vector<uint64_t> element_offsets = {0, 1, 3, 5};

    SECTION("Unordered write") {
      write_sparse_array(
          ctx, array_name, data, element_offsets, TILEDB_UNORDERED);
      read_and_check_sparse_array(ctx, array_name, data, element_offsets);
    }
    SECTION("Global order write") {
      write_sparse_array(
          ctx, array_name, data, element_offsets, TILEDB_GLOBAL_ORDER);
      read_and_check_sparse_array(ctx, array_name, data, element_offsets);
    }
  }

  // Clean up
  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test element offsets : dense array",
    "[var-offsets][element-offset]") {
  std::string array_name = "test_element_offset";
  create_dense_array(array_name);

  std::vector<int32_t> data = {1, 2, 3, 4, 5, 6};
  Context ctx;

  SECTION("Byte offsets (default case)") {
    Config config = ctx.config();
    CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");

    std::vector<uint64_t> byte_offsets = {0, 4, 12, 20};

    SECTION("Unordered write") {
      write_dense_array(ctx, array_name, data, byte_offsets, TILEDB_UNORDERED);
      read_and_check_dense_array(ctx, array_name, data, byte_offsets);
    }
    SECTION("Ordered write") {
      write_dense_array(ctx, array_name, data, byte_offsets, TILEDB_ROW_MAJOR);
      read_and_check_dense_array(ctx, array_name, data, byte_offsets);
    }
    SECTION("Global order write") {
      write_dense_array(
          ctx, array_name, data, byte_offsets, TILEDB_GLOBAL_ORDER);
      read_and_check_dense_array(ctx, array_name, data, byte_offsets);
    }
  }

  SECTION("Element offsets") {
    Config config;
    // Change config of offsets format from bytes to elements
    config["sm.var_offsets.mode"] = "elements";
    Context ctx(config);

    std::vector<uint64_t> element_offsets = {0, 1, 3, 5};

    SECTION("Unordered write") {
      write_dense_array(
          ctx, array_name, data, element_offsets, TILEDB_UNORDERED);
      read_and_check_dense_array(ctx, array_name, data, element_offsets);
    }
    SECTION("Ordered write") {
      write_dense_array(
          ctx, array_name, data, element_offsets, TILEDB_ROW_MAJOR);
      read_and_check_dense_array(ctx, array_name, data, element_offsets);
    }
    SECTION("Global order write") {
      write_dense_array(
          ctx, array_name, data, element_offsets, TILEDB_GLOBAL_ORDER);
      read_and_check_dense_array(ctx, array_name, data, element_offsets);
    }
  }

  // Clean up
  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test offsets extra element: sparse array",
    "[var-offsets][extra-offset]") {
  std::string array_name = "test_extra_offset";
  create_sparse_array(array_name);

  Context ctx;
  std::vector<int32_t> data = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> data_offsets = {0, 4, 12, 20};
  std::vector<uint64_t> element_offsets = {0, 1, 3, 5};

  SECTION("Full read") {
    Config config;

    SECTION("No extra element (default case)") {
      config = ctx.config();
      CHECK((std::string)config["sm.var_offsets.extra_element"] == "false");

      write_sparse_array(ctx, array_name, data, data_offsets, TILEDB_UNORDERED);
      read_and_check_sparse_array(ctx, array_name, data, data_offsets);
    }

    SECTION("Extra element") {
      config["sm.var_offsets.extra_element"] = "true";

      SECTION("Byte offsets (default config)") {
        CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");
        Context ctx(config);

        // Write data with extra element indicating total number of bytes
        data_offsets.push_back(sizeof(data[0]) * data.size());

        SECTION("Unordered write") {
          write_sparse_array(
              ctx, array_name, data, data_offsets, TILEDB_UNORDERED);
          read_and_check_sparse_array(ctx, array_name, data, data_offsets);
        }
        SECTION("Global order write") {
          write_sparse_array(
              ctx, array_name, data, data_offsets, TILEDB_GLOBAL_ORDER);
          read_and_check_sparse_array(ctx, array_name, data, data_offsets);
        }
      }

      SECTION("Element offsets") {
        config["sm.var_offsets.mode"] = "elements";
        Context ctx(config);

        // Write data with extra element indicating the total number of elements
        element_offsets.push_back(data.size());

        SECTION("Unordered write") {
          write_sparse_array(
              ctx, array_name, data, element_offsets, TILEDB_UNORDERED);
          read_and_check_sparse_array(ctx, array_name, data, element_offsets);
        }
        SECTION("Global order write") {
          write_sparse_array(
              ctx, array_name, data, element_offsets, TILEDB_GLOBAL_ORDER);
          read_and_check_sparse_array(ctx, array_name, data, element_offsets);
        }
      }

      SECTION("User offsets buffer too small") {
        Context ctx(config);

        Array array_w(ctx, array_name, TILEDB_WRITE);
        std::vector<int64_t> d1 = {1, 2, 3, 4};
        std::vector<int64_t> d2 = {2, 1, 3, 4};
        Query query_w(ctx, array_w, TILEDB_WRITE);
        query_w.set_layout(TILEDB_UNORDERED)
            .set_buffer("d1", d1)
            .set_buffer("d2", d2);

        // Try to write without allocating memory for the extra element
        query_w.set_buffer("attr", data_offsets, data);
        CHECK_THROWS(query_w.submit());

        // Write data with extra element
        data_offsets.push_back(sizeof(data[0]) * data.size());
        query_w.set_buffer("attr", data_offsets, data);
        CHECK_NOTHROW(query_w.submit());
        array_w.close();

        // Submit read query
        Array array_r(ctx, array_name, TILEDB_READ);
        Query query_r(ctx, array_r, TILEDB_READ);

        // Assume no size for the extra element
        std::vector<int32_t> attr_val(data.size());
        std::vector<uint64_t> attr_off(data_offsets.size() - 1);
        query_r.set_buffer("attr", attr_off, attr_val);

        // First partial read because offsets don't fit
        CHECK_NOTHROW(query_r.submit());
        CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
        // check returned data
        auto data_num = query_r.result_buffer_elements()["attr"].second;
        CHECK(data_num == 3);
        std::vector<int32_t> data_exp1 = {1, 2, 3, 0, 0, 0};
        CHECK(attr_val == data_exp1);
        // check returned offsets
        auto offset_num = query_r.result_buffer_elements()["attr"].first;
        CHECK(offset_num == 3);
        std::vector<uint64_t> data_off_exp1 = {0, 4, 12, 0};
        CHECK(attr_off == data_off_exp1);

        // Second partial read
        reset_read_buffers(attr_val, attr_off);
        CHECK_NOTHROW(query_r.submit());
        CHECK(query_r.query_status() == Query::Status::COMPLETE);
        // check returned data
        data_num = query_r.result_buffer_elements()["attr"].second;
        CHECK(data_num == 3);
        std::vector<int32_t> data_exp2 = {4, 5, 6, 0, 0, 0};
        CHECK(attr_val == data_exp2);
        // check returned offsets
        offset_num = query_r.result_buffer_elements()["attr"].first;
        CHECK(offset_num == 3);
        std::vector<uint64_t> data_off_exp2 = {0, 8, 12, 0};
        CHECK(attr_off == data_off_exp2);

        array_r.close();
      }
    }
  }

  SECTION("Partial read") {
    Config config;

    // The expected buffers to be returned after 2 partial reads with
    // read buffers of size data.size() / 2
    std::vector<int32_t> data_part1 = {1, 2, 3};
    std::vector<uint64_t> data_off_part1 = {0, 4};
    std::vector<uint64_t> data_elem_off_part1 = {0, 1};
    std::vector<int32_t> data_part2 = {4, 5, 6};
    std::vector<uint64_t> data_off_part2 = {0, 8};
    std::vector<uint64_t> data_elem_off_part2 = {0, 2};

    SECTION("No extra element (default case)") {
      config = ctx.config();
      CHECK((std::string)config["sm.var_offsets.extra_element"] == "false");

      write_sparse_array(ctx, array_name, data, data_offsets, TILEDB_UNORDERED);
      partial_read_and_check_sparse_array(
          ctx,
          array_name,
          data_part1,
          data_off_part1,
          data_part2,
          data_off_part2);
    }

    SECTION("Extra element") {
      config["sm.var_offsets.extra_element"] = "true";

      SECTION("Byte offsets (default config)") {
        CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");
        Context ctx(config);

        // Write data with extra element indicating total number of bytes
        data_offsets.push_back(sizeof(data[0]) * data.size());

        // Expect an extra element offset on each read
        data_off_part1.push_back(sizeof(data_part1[0]) * data_part1.size());
        data_off_part2.push_back(sizeof(data_part2[0]) * data_part2.size());

        SECTION("Unordered write") {
          write_sparse_array(
              ctx, array_name, data, data_offsets, TILEDB_UNORDERED);
          partial_read_and_check_sparse_array(
              ctx,
              array_name,
              data_part1,
              data_off_part1,
              data_part2,
              data_off_part2);
        }
        SECTION("Global order write") {
          write_sparse_array(
              ctx, array_name, data, data_offsets, TILEDB_GLOBAL_ORDER);
          partial_read_and_check_sparse_array(
              ctx,
              array_name,
              data_part1,
              data_off_part1,
              data_part2,
              data_off_part2);
        }
      }

      SECTION("Element offsets") {
        config["sm.var_offsets.mode"] = "elements";
        Context ctx(config);

        // Write data with extra element indicating total number of elements
        element_offsets.push_back(data.size());

        // Expect an extra element offset on each read
        data_elem_off_part1.push_back(data_part1.size());
        data_elem_off_part2.push_back(data_part2.size());

        SECTION("Unordered write") {
          write_sparse_array(
              ctx, array_name, data, element_offsets, TILEDB_UNORDERED);
          partial_read_and_check_sparse_array(
              ctx,
              array_name,
              data_part1,
              data_elem_off_part1,
              data_part2,
              data_elem_off_part2);
        }
        SECTION("Global order write") {
          write_sparse_array(
              ctx, array_name, data, element_offsets, TILEDB_GLOBAL_ORDER);
          partial_read_and_check_sparse_array(
              ctx,
              array_name,
              data_part1,
              data_elem_off_part1,
              data_part2,
              data_elem_off_part2);
        }
      }

      SECTION("User offsets buffer too small") {
        // Write data with extra element
        data_offsets.push_back(sizeof(data[0]) * data.size());
        write_sparse_array(
            ctx, array_name, data, data_offsets, TILEDB_UNORDERED);

        // Submit read query
        Context ctx(config);
        Array array(ctx, array_name, TILEDB_READ);
        Query query(ctx, array, TILEDB_READ);

        // Assume no size for the extra element
        std::vector<int32_t> attr_val(data_part1.size());
        std::vector<uint64_t> attr_off(data_off_part1.size());
        query.set_buffer("attr", attr_off, attr_val);

        // First partial read
        CHECK_NOTHROW(query.submit());
        CHECK(query.query_status() == Query::Status::INCOMPLETE);
        std::vector<int32_t> data_exp1 = {1, 0, 0};
        std::vector<uint64_t> data_off_exp1 = {0, 4};
        // check returned data
        auto data_num = query.result_buffer_elements()["attr"].second;
        CHECK(data_num == 1);
        CHECK(attr_val == data_exp1);
        // check returned offsets
        auto offset_num = query.result_buffer_elements()["attr"].first;
        CHECK(offset_num == 2);
        CHECK(attr_off == data_off_exp1);

        // Second partial read
        reset_read_buffers(attr_val, attr_off);
        CHECK_NOTHROW(query.submit());
        CHECK(query.query_status() == Query::Status::INCOMPLETE);
        std::vector<int32_t> data_exp2 = {2, 3, 0};
        std::vector<uint64_t> data_off_exp2 = {0, 8};
        // check returned data
        data_num = query.result_buffer_elements()["attr"].second;
        CHECK(data_num == 2);
        CHECK(attr_val == data_exp2);
        // check returned offsets
        offset_num = query.result_buffer_elements()["attr"].first;
        CHECK(offset_num == 2);
        CHECK(attr_off == data_off_exp2);

        // Third partial read
        reset_read_buffers(attr_val, attr_off);
        CHECK_NOTHROW(query.submit());
        CHECK(query.query_status() == Query::Status::INCOMPLETE);
        std::vector<int32_t> data_exp3 = {4, 5, 0};
        std::vector<uint64_t> data_off_exp3 = {0, 8};
        // check returned data
        data_num = query.result_buffer_elements()["attr"].second;
        CHECK(data_num == 2);
        CHECK(attr_val == data_exp3);
        // check returned offsets
        offset_num = query.result_buffer_elements()["attr"].first;
        CHECK(offset_num == 2);
        CHECK(attr_off == data_off_exp3);

        // Last partial read
        reset_read_buffers(attr_val, attr_off);
        CHECK_NOTHROW(query.submit());
        CHECK(query.query_status() == Query::Status::COMPLETE);
        std::vector<int32_t> data_exp4 = {6, 0, 0};
        std::vector<uint64_t> data_off_exp4 = {0, 4};
        // check returned data
        data_num = query.result_buffer_elements()["attr"].second;
        CHECK(data_num == 1);
        CHECK(attr_val == data_exp4);
        // check returned offsets
        offset_num = query.result_buffer_elements()["attr"].first;
        CHECK(offset_num == 2);
        CHECK(attr_off == data_off_exp4);

        array.close();
      }
    }
  }

  // Clean up
  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test offsets extra element: dense array",
    "[var-offsets][extra-offset]") {
  std::string array_name = "test_extra_offset";
  create_dense_array(array_name);

  Context ctx;
  std::vector<int32_t> data = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> data_offsets = {0, 4, 12, 20};
  std::vector<uint64_t> element_offsets = {0, 1, 3, 5};

  SECTION("Full read") {
    Config config;

    SECTION("No extra element (default case)") {
      config = ctx.config();
      CHECK((std::string)config["sm.var_offsets.extra_element"] == "false");

      write_dense_array(ctx, array_name, data, data_offsets, TILEDB_ROW_MAJOR);
      read_and_check_dense_array(ctx, array_name, data, data_offsets);
    }

    SECTION("Extra element") {
      config["sm.var_offsets.extra_element"] = "true";

      SECTION("Byte offsets (default config)") {
        CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");
        Context ctx(config);

        // Write data with extra element indicating total number of bytes
        data_offsets.push_back(sizeof(data[0]) * data.size());

        SECTION("Unordered write") {
          write_dense_array(
              ctx, array_name, data, data_offsets, TILEDB_UNORDERED);
          read_and_check_dense_array(ctx, array_name, data, data_offsets);
        }
        SECTION("Ordered write") {
          write_dense_array(
              ctx, array_name, data, data_offsets, TILEDB_ROW_MAJOR);
          read_and_check_dense_array(ctx, array_name, data, data_offsets);
        }
        SECTION("Global order write") {
          write_dense_array(
              ctx, array_name, data, data_offsets, TILEDB_GLOBAL_ORDER);
          read_and_check_dense_array(ctx, array_name, data, data_offsets);
        }
      }

      SECTION("Element offsets") {
        config["sm.var_offsets.mode"] = "elements";
        Context ctx(config);

        // Write data with extra element indicating the total number of elements
        element_offsets.push_back(data.size());

        SECTION("Unordered write") {
          write_dense_array(
              ctx, array_name, data, element_offsets, TILEDB_UNORDERED);
          read_and_check_dense_array(ctx, array_name, data, element_offsets);
        }
        SECTION("Ordered write") {
          write_dense_array(
              ctx, array_name, data, element_offsets, TILEDB_ROW_MAJOR);
          read_and_check_dense_array(ctx, array_name, data, element_offsets);
        }
        SECTION("Global order write") {
          write_dense_array(
              ctx, array_name, data, element_offsets, TILEDB_GLOBAL_ORDER);
          read_and_check_dense_array(ctx, array_name, data, element_offsets);
        }
      }

      SECTION("User offsets buffer too small") {
        // Use element offsets to cover this code path as well
        config["sm.var_offsets.mode"] = "elements";
        Context ctx(config);

        Array array_w(ctx, array_name, TILEDB_WRITE);
        Query query_w(ctx, array_w, TILEDB_WRITE);
        query_w.set_layout(TILEDB_ROW_MAJOR)
            .set_subarray<int64_t>({1, 2, 1, 2});

        // Try to write without allocating memory for the extra element
        query_w.set_buffer("attr", element_offsets, data);
        CHECK_THROWS(query_w.submit());

        // Write data with extra element
        element_offsets.push_back(data.size());
        query_w.set_buffer("attr", element_offsets, data);
        CHECK_NOTHROW(query_w.submit());
        array_w.close();

        // Submit read query
        Array array_r(ctx, array_name, TILEDB_READ);
        Query query_r(ctx, array_r, TILEDB_READ);

        // Assume no size for the extra element
        std::vector<int32_t> attr_val(data.size());
        std::vector<uint64_t> attr_off(element_offsets.size() - 1);
        query_r.set_buffer("attr", attr_off, attr_val);
        query_r.set_subarray<int64_t>({1, 2, 1, 2});

        // First partial read because offsets don't fit
        CHECK_NOTHROW(query_r.submit());
        CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
        std::vector<int32_t> data_exp1 = {1, 2, 3, 0, 0, 0};
        std::vector<uint64_t> data_off_exp1 = {0, 1, 3, 0};
        // check returned data
        auto data_num = query_r.result_buffer_elements()["attr"].second;
        CHECK(data_num == 3);
        CHECK(attr_val == data_exp1);
        // check returned offsets
        auto offset_num = query_r.result_buffer_elements()["attr"].first;
        CHECK(offset_num == 3);
        CHECK(attr_off == data_off_exp1);

        // Second partial read
        reset_read_buffers(attr_val, attr_off);
        CHECK_NOTHROW(query_r.submit());
        CHECK(query_r.query_status() == Query::Status::COMPLETE);
        std::vector<int32_t> data_exp2 = {4, 5, 6, 0, 0, 0};
        std::vector<uint64_t> data_off_exp2 = {0, 2, 3, 0};
        // check returned data
        data_num = query_r.result_buffer_elements()["attr"].second;
        CHECK(data_num == 3);
        CHECK(attr_val == data_exp2);
        // check returned offsets
        offset_num = query_r.result_buffer_elements()["attr"].first;
        CHECK(offset_num == 3);
        CHECK(attr_off == data_off_exp2);

        array_r.close();
      }
    }
  }

  SECTION("Partial read") {
    Config config;

    // The expected buffers to be returned after 2 partial reads with
    // read buffers of size data.size() / 2
    std::vector<int32_t> data_part1 = {1, 2, 3};
    std::vector<uint64_t> data_off_part1 = {0, 4};
    std::vector<uint64_t> data_elem_off_part1 = {0, 1};
    std::vector<int32_t> data_part2 = {4, 5, 6};
    std::vector<uint64_t> data_off_part2 = {0, 8};
    std::vector<uint64_t> data_elem_off_part2 = {0, 2};

    SECTION("No extra element (default case)") {
      config = ctx.config();
      CHECK((std::string)config["sm.var_offsets.extra_element"] == "false");

      write_dense_array(ctx, array_name, data, data_offsets, TILEDB_UNORDERED);
      partial_read_and_check_dense_array(
          ctx,
          array_name,
          data_part1,
          data_off_part1,
          data_part2,
          data_off_part2);
    }

    SECTION("Extra element") {
      config["sm.var_offsets.extra_element"] = "true";

      SECTION("Byte offsets (default config)") {
        CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");
        Context ctx(config);

        // Write data with extra element indicating total number of bytes
        data_offsets.push_back(sizeof(data[0]) * data.size());

        // Expect an extra element offset on each read
        data_off_part1.push_back(sizeof(data_part1[0]) * data_part1.size());
        data_off_part2.push_back(sizeof(data_part2[0]) * data_part2.size());

        SECTION("Unordered write") {
          write_dense_array(
              ctx, array_name, data, data_offsets, TILEDB_UNORDERED);
          partial_read_and_check_dense_array(
              ctx,
              array_name,
              data_part1,
              data_off_part1,
              data_part2,
              data_off_part2);
        }
        SECTION("Ordered write") {
          write_dense_array(
              ctx, array_name, data, data_offsets, TILEDB_ROW_MAJOR);
          partial_read_and_check_dense_array(
              ctx,
              array_name,
              data_part1,
              data_off_part1,
              data_part2,
              data_off_part2);
        }
        SECTION("Global order write") {
          write_dense_array(
              ctx, array_name, data, data_offsets, TILEDB_GLOBAL_ORDER);
          partial_read_and_check_dense_array(
              ctx,
              array_name,
              data_part1,
              data_off_part1,
              data_part2,
              data_off_part2);
        }
      }

      SECTION("Element offsets") {
        config["sm.var_offsets.mode"] = "elements";
        Context ctx(config);

        // Write data with extra element indicating total number of elements
        element_offsets.push_back(data.size());

        // Expect an extra element offset on each read
        data_elem_off_part1.push_back(data_part1.size());
        data_elem_off_part2.push_back(data_part2.size());

        SECTION("Unordered write") {
          write_dense_array(
              ctx, array_name, data, element_offsets, TILEDB_UNORDERED);
          partial_read_and_check_dense_array(
              ctx,
              array_name,
              data_part1,
              data_elem_off_part1,
              data_part2,
              data_elem_off_part2);
        }
        SECTION("Ordered write") {
          write_dense_array(
              ctx, array_name, data, element_offsets, TILEDB_ROW_MAJOR);
          partial_read_and_check_dense_array(
              ctx,
              array_name,
              data_part1,
              data_elem_off_part1,
              data_part2,
              data_elem_off_part2);
        }
        SECTION("Global order write") {
          write_dense_array(
              ctx, array_name, data, element_offsets, TILEDB_GLOBAL_ORDER);
          partial_read_and_check_dense_array(
              ctx,
              array_name,
              data_part1,
              data_elem_off_part1,
              data_part2,
              data_elem_off_part2);
        }
      }

      SECTION("User offsets buffer too small") {
        Context ctx(config);
        // Write data with extra element
        data_offsets.push_back(sizeof(data[0]) * data.size());
        write_dense_array(
            ctx, array_name, data, data_offsets, TILEDB_ROW_MAJOR);

        // Submit read query
        Array array(ctx, array_name, TILEDB_READ);
        Query query(ctx, array, TILEDB_READ);

        // Assume smaller offset buffer than data buffer
        std::vector<int32_t> attr_val(data_part1.size());
        std::vector<uint64_t> attr_off(data_off_part1.size());
        query.set_buffer("attr", attr_off, attr_val);
        query.set_subarray<int64_t>({1, 2, 1, 2});

        // First partial read
        CHECK_NOTHROW(query.submit());
        CHECK(query.query_status() == Query::Status::INCOMPLETE);
        std::vector<int32_t> data_exp1 = {1, 0, 0};
        std::vector<uint64_t> data_off_exp1 = {0, 4};
        // check returned data
        auto data_num = query.result_buffer_elements()["attr"].second;
        CHECK(data_num == 1);
        CHECK(attr_val == data_exp1);
        // check returned offsets
        auto offset_num = query.result_buffer_elements()["attr"].first;
        CHECK(offset_num == 2);
        CHECK(attr_off == data_off_exp1);

        // Second partial read
        reset_read_buffers(attr_val, attr_off);
        CHECK_NOTHROW(query.submit());
        CHECK(query.query_status() == Query::Status::INCOMPLETE);
        std::vector<int32_t> data_exp2 = {2, 3, 0};
        std::vector<uint64_t> data_off_exp2 = {0, 8};
        // check returned data
        data_num = query.result_buffer_elements()["attr"].second;
        CHECK(data_num == 2);
        CHECK(attr_val == data_exp2);
        // check returned offsets
        offset_num = query.result_buffer_elements()["attr"].first;
        CHECK(offset_num == 2);
        CHECK(attr_off == data_off_exp2);

        // Third partial read
        reset_read_buffers(attr_val, attr_off);
        CHECK_NOTHROW(query.submit());
        CHECK(query.query_status() == Query::Status::INCOMPLETE);
        std::vector<int32_t> data_exp3 = {4, 5, 0};
        std::vector<uint64_t> data_off_exp3 = {0, 8};
        // check returned data
        data_num = query.result_buffer_elements()["attr"].second;
        CHECK(data_num == 2);
        CHECK(attr_val == data_exp3);
        // check returned offsets
        offset_num = query.result_buffer_elements()["attr"].first;
        CHECK(offset_num == 2);
        CHECK(attr_off == data_off_exp3);

        // Last partial read
        reset_read_buffers(attr_val, attr_off);
        CHECK_NOTHROW(query.submit());
        CHECK(query.query_status() == Query::Status::COMPLETE);
        std::vector<int32_t> data_exp4 = {6, 0, 0};
        std::vector<uint64_t> data_off_exp4 = {0, 4};
        // check returned data
        data_num = query.result_buffer_elements()["attr"].second;
        CHECK(data_num == 1);
        CHECK(attr_val == data_exp4);
        // check returned offsets
        offset_num = query.result_buffer_elements()["attr"].first;
        CHECK(offset_num == 2);
        CHECK(attr_off == data_off_exp4);

        array.close();
      }
    }
  }

  // Clean up
  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test 32-bit offsets: sparse array",
    "[var-offsets][32bit-offset]") {
  std::string array_name = "test_32bit_offset";
  create_sparse_array(array_name);

  std::vector<int32_t> data = {1, 2, 3, 4, 5, 6};
  // Create 32 bit byte offsets buffer to use
  std::vector<uint32_t> data_byte_offsets = {0, 4, 12, 20};

  Config config;
  // Change config of offsets bitsize from 64 to 32
  config["sm.var_offsets.bitsize"] = 32;
  Context ctx(config);

  SECTION("Byte offsets (default case)") {
    CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");

    SECTION("Unordered write") {
      write_sparse_array(
          ctx, array_name, data, data_byte_offsets, TILEDB_UNORDERED);
      read_and_check_sparse_array(ctx, array_name, data, data_byte_offsets);
    }
    SECTION("Global order write") {
      write_sparse_array(
          ctx, array_name, data, data_byte_offsets, TILEDB_GLOBAL_ORDER);
      read_and_check_sparse_array(ctx, array_name, data, data_byte_offsets);
    }
  }

  SECTION("Element offsets") {
    // Change config of offsets format from bytes to elements
    config["sm.var_offsets.mode"] = "elements";
    Context ctx(config);

    // Create 32 bit element offsets buffer to use
    std::vector<uint32_t> data_element_offsets = {0, 1, 3, 5};

    SECTION("Unordered write") {
      write_sparse_array(
          ctx, array_name, data, data_element_offsets, TILEDB_UNORDERED);
      read_and_check_sparse_array(ctx, array_name, data, data_element_offsets);
    }
    SECTION("Global order write") {
      write_sparse_array(
          ctx, array_name, data, data_element_offsets, TILEDB_GLOBAL_ORDER);
      read_and_check_sparse_array(ctx, array_name, data, data_element_offsets);
    }
  }

  SECTION("Extra element") {
    config["sm.var_offsets.extra_element"] = "true";
    Context ctx(config);

    // Check the extra element is included in the offsets
    uint32_t data_size = static_cast<uint32_t>(sizeof(data[0]) * data.size());
    std::vector<uint32_t> data_byte_offsets = {0, 4, 12, 20, data_size};

    SECTION("Unordered write") {
      write_sparse_array(
          ctx, array_name, data, data_byte_offsets, TILEDB_UNORDERED);
      read_and_check_sparse_array(ctx, array_name, data, data_byte_offsets);
    }
    SECTION("Global order write") {
      write_sparse_array(
          ctx, array_name, data, data_byte_offsets, TILEDB_GLOBAL_ORDER);
      read_and_check_sparse_array(ctx, array_name, data, data_byte_offsets);
    }
  }

  // Clean up
  config["sm.var_offsets.extra_element"] = "false";
  config["sm.var_offsets.mode"] = "bytes";
  config["sm.var_offsets.bitsize"] = 64;
  Context ctx2(config);
  VFS vfs(ctx2);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test 32-bit offsets: dense array",
    "[var-offsets][32bit-offset]") {
  std::string array_name = "test_32bit_offset";
  create_dense_array(array_name);

  std::vector<int32_t> data = {1, 2, 3, 4, 5, 6};
  // Create 32 bit offsets byte buffer to use
  std::vector<uint32_t> data_byte_offsets = {0, 4, 12, 20};

  Config config;
  // Change config of offsets bitsize from 64 to 32
  config["sm.var_offsets.bitsize"] = 32;
  Context ctx(config);

  SECTION("Byte offsets (default case)") {
    CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");

    SECTION("Unordered write") {
      write_dense_array(
          ctx, array_name, data, data_byte_offsets, TILEDB_UNORDERED);
      read_and_check_dense_array(ctx, array_name, data, data_byte_offsets);
    }
    SECTION("Ordered write") {
      write_dense_array(
          ctx, array_name, data, data_byte_offsets, TILEDB_ROW_MAJOR);
      read_and_check_dense_array(ctx, array_name, data, data_byte_offsets);
    }
    SECTION("Global order write") {
      write_dense_array(
          ctx, array_name, data, data_byte_offsets, TILEDB_GLOBAL_ORDER);
      read_and_check_dense_array(ctx, array_name, data, data_byte_offsets);
    }
  }

  SECTION("Element offsets") {
    // Change config of offsets format from bytes to elements
    config["sm.var_offsets.mode"] = "elements";
    Context ctx(config);

    // Create 32 bit element offsets buffer to use
    std::vector<uint32_t> data_element_offsets = {0, 1, 3, 5};

    SECTION("Unordered write") {
      write_dense_array(
          ctx, array_name, data, data_element_offsets, TILEDB_UNORDERED);
      read_and_check_dense_array(ctx, array_name, data, data_element_offsets);
    }
    SECTION("Ordered write") {
      write_dense_array(
          ctx, array_name, data, data_element_offsets, TILEDB_ROW_MAJOR);
      read_and_check_dense_array(ctx, array_name, data, data_element_offsets);
    }
    SECTION("Global order write") {
      write_dense_array(
          ctx, array_name, data, data_element_offsets, TILEDB_GLOBAL_ORDER);
      read_and_check_dense_array(ctx, array_name, data, data_element_offsets);
    }
  }

  SECTION("Extra element") {
    config["sm.var_offsets.extra_element"] = "true";
    Context ctx(config);

    // Check the extra element is included in the offsets
    uint32_t data_size = static_cast<uint32_t>(sizeof(data[0]) * data.size());
    std::vector<uint32_t> data_byte_offsets = {0, 4, 12, 20, data_size};

    SECTION("Unordered write") {
      write_dense_array(
          ctx, array_name, data, data_byte_offsets, TILEDB_UNORDERED);
      read_and_check_dense_array(ctx, array_name, data, data_byte_offsets);
    }
    SECTION("Ordered write") {
      write_dense_array(
          ctx, array_name, data, data_byte_offsets, TILEDB_ROW_MAJOR);
      read_and_check_dense_array(ctx, array_name, data, data_byte_offsets);
    }
    SECTION("Global order write") {
      write_dense_array(
          ctx, array_name, data, data_byte_offsets, TILEDB_GLOBAL_ORDER);
      read_and_check_dense_array(ctx, array_name, data, data_byte_offsets);
    }
  }

  SECTION("Test direct Query configuration") {
    config["sm.var_offsets.extra_element"] = "true";
    auto config_sp = std::make_shared<Config>(config);
    Context ctx;

    // Check the extra element is included in the offsets
    uint32_t data_size = static_cast<uint32_t>(sizeof(data[0]) * data.size());
    std::vector<uint32_t> data_byte_offsets = {0, 4, 12, 20, data_size};

    SECTION("Unordered write") {
      write_dense_array(
          ctx,
          array_name,
          data,
          data_byte_offsets,
          TILEDB_UNORDERED,
          config_sp);
      read_and_check_dense_array(
          ctx, array_name, data, data_byte_offsets, config_sp);
    }
  }

  // Clean up
  config["sm.var_offsets.extra_element"] = "false";
  config["sm.var_offsets.mode"] = "bytes";
  config["sm.var_offsets.bitsize"] = 64;
  Context ctx2(config);
  VFS vfs(ctx2);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test 32-bit offsets: sparse array with string dimension",
    "[var-offsets-dim][32bit-offset]") {
  std::string array_name = "test_32bit_offset_string_dim";

  /*
    Write an array with string dimension and make sure we get back
    proper offsets along with extra element in read.
  */

  // Create data buffer to use
  std::string data = "aabbbcdddd";
  // Create 32 bit offsets byte buffer to use
  std::vector<uint64_t> data_elem_offsets = {0, 2, 5, 6};

  // Create and write array
  {
    Context ctx;
    Domain domain(ctx);
    domain.add_dimension(
        Dimension::create(ctx, "dim1", TILEDB_STRING_ASCII, nullptr, nullptr));

    ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(domain);

    tiledb::Array::create(array_name, schema);

    auto array = tiledb::Array(ctx, array_name, TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);
    query.set_buffer(
        "dim1",
        data_elem_offsets.data(),
        data_elem_offsets.size(),
        (char*)data.data(),
        data.size());

    query.set_layout(TILEDB_UNORDERED);
    query.submit();
    query.finalize();
    array.close();
  }

  {
    Config config;
    // Change config of offsets bitsize from 64 to 32
    config["sm.var_offsets.bitsize"] = 32;
    // Add extra element
    config["sm.var_offsets.extra_element"] = "true";
    Context ctx(config);

    std::vector<uint32_t> offsets_back(5);
    std::string data_back;
    data_back.resize(data.size());

    auto array = tiledb::Array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);
    query.add_range(0, std::string("aa"), std::string("dddd"));
    query.set_buffer(
        "dim1",
        (uint64_t*)offsets_back.data(),
        offsets_back.size(),
        (char*)data_back.data(),
        data_back.size());

    query.submit();

    CHECK(query.query_status() == Query::Status::COMPLETE);
    CHECK(offsets_back[4] == data.size());
  }

  Context ctx;
  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}
