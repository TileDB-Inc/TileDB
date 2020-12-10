/**
 * @file   unit-var_offsets.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 TileDB Inc.
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
  query.set_layout(layout).set_buffer("d1", d1).set_buffer("d2", d2).set_buffer(
      "attr", data_offsets, data);
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
    tiledb_layout_t layout) {
  std::vector<int64_t> d1 = {1, 1, 2, 2};
  std::vector<int64_t> d2 = {1, 2, 1, 2};

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
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

void read_and_check_dense_array(
    Context ctx,
    const std::string& array_name,
    std::vector<int32_t>& expected_data,
    std::vector<uint64_t>& expected_offsets) {
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);

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

  std::vector<int32_t> data = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> data_offsets = {0, 4, 12, 20};
  Context ctx;
  write_sparse_array(ctx, array_name, data, data_offsets, TILEDB_UNORDERED);

  SECTION("Full read") {
    Config config;
    Array array(ctx, array_name, TILEDB_READ);

    // Buffers to store the read results
    std::vector<int32_t> attr_val(data.size());
    std::vector<uint64_t> attr_off(data_offsets.size());

    SECTION("No extra element (default case)") {
      config = ctx.config();
      CHECK((std::string)config["sm.var_offsets.extra_element"] == "false");

      // Submit read query
      Query query_r(ctx, array, TILEDB_READ);
      query_r.set_buffer("attr", attr_off, attr_val);
      CHECK_NOTHROW(query_r.submit());

      // Check that bytes offsets are properly returned
      CHECK(attr_val == data);
      CHECK(attr_off == data_offsets);
    }

    SECTION("Extra element") {
      config["sm.var_offsets.extra_element"] = "true";
      // Extend offsets buffer to accomodate for the extra element
      attr_off.resize(attr_off.size() + 1);

      SECTION("Byte offsets (default config)") {
        CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");
        Context ctx(config);

        // Submit read query
        Query query_r(ctx, array, TILEDB_READ);
        query_r.set_buffer("attr", attr_off, attr_val);
        CHECK_NOTHROW(query_r.submit());

        // Check the extra element is included in the offsets
        data_offsets.push_back(sizeof(data[0]) * data.size());
        CHECK(attr_val == data);
        CHECK(attr_off == data_offsets);
      }

      SECTION("Element offsets") {
        config["sm.var_offsets.mode"] = "elements";
        Context ctx(config);

        // Submit read query
        Query query_r(ctx, array, TILEDB_READ);
        query_r.set_buffer("attr", attr_off, attr_val);
        CHECK_NOTHROW(query_r.submit());

        // Check the extra element is included in the offsets
        data_offsets = {0, 1, 3, 5, 6};
        CHECK(attr_val == data);
        CHECK(attr_off == data_offsets);
      }

      SECTION("User offsets buffer too small") {
        // Assume no size for the extra element
        attr_off.resize(attr_off.size() - 1);

        // Submit read query
        Context ctx(config);
        Query query_r(ctx, array, TILEDB_READ);
        query_r.set_buffer("attr", attr_off, attr_val);
        CHECK_THROWS(query_r.submit());
      }
    }
    array.close();
  }

  SECTION("Partial read") {
    Array array(ctx, array_name, TILEDB_READ);
    Config config;

    // Assume the user buffers can only store half the data
    std::vector<int32_t> attr_val(data.size() / 2);
    std::vector<uint64_t> attr_off(data_offsets.size() - 2);

    // The expected buffers to be returned after 2 partial reads
    std::vector<int32_t> data_part1 = {1, 2, 3};
    std::vector<uint64_t> data_off_part1 = {0, 4};
    std::vector<int32_t> data_part2 = {4, 5, 6};
    std::vector<uint64_t> data_off_part2 = {0, 8};

    SECTION("No extra element (default case)") {
      config = ctx.config();
      CHECK((std::string)config["sm.var_offsets.extra_element"] == "false");

      // Submit read query
      Query query_r(ctx, array, TILEDB_READ);
      query_r.set_buffer("attr", attr_off, attr_val);

      // Check that first partial read returns expected results
      CHECK_NOTHROW(query_r.submit());
      CHECK(attr_val == data_part1);
      CHECK(attr_off == data_off_part1);

      // Check that second partial read returns expected results
      CHECK_NOTHROW(query_r.submit());
      CHECK(attr_val == data_part2);
      CHECK(attr_off == data_off_part2);
    }

    SECTION("Extra element") {
      config["sm.var_offsets.extra_element"] = "true";
      // Extend offsets buffer to accomodate for the extra element
      attr_off.resize(attr_off.size() + 1);

      SECTION("Byte offsets (default config)") {
        CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");
        Context ctx(config);

        // Submit read query
        Query query_r(ctx, array, TILEDB_READ);
        query_r.set_buffer("attr", attr_off, attr_val);

        // Check the extra element is included in the offsets part1
        CHECK_NOTHROW(query_r.submit());
        data_off_part1.push_back(sizeof(data_part1[0]) * data_part1.size());
        CHECK(attr_val == data_part1);
        CHECK(attr_off == data_off_part1);

        // Check the extra element is included in the offsets part2
        CHECK_NOTHROW(query_r.submit());
        data_off_part2.push_back(sizeof(data_part2[0]) * data_part2.size());
        CHECK(attr_val == data_part2);
        CHECK(attr_off == data_off_part2);
      }

      SECTION("Element offsets") {
        config["sm.var_offsets.mode"] = "elements";
        Context ctx(config);

        // Submit read query
        Query query_r(ctx, array, TILEDB_READ);
        query_r.set_buffer("attr", attr_off, attr_val);

        // Check the extra element is included in the offsets part1
        CHECK_NOTHROW(query_r.submit());
        data_off_part1 = {0, 1, 3};
        CHECK(attr_val == data_part1);
        CHECK(attr_off == data_off_part1);

        // Check the extra element is included in the offsets part2
        CHECK_NOTHROW(query_r.submit());
        data_off_part2 = {0, 2, 3};
        CHECK(attr_val == data_part2);
        CHECK(attr_off == data_off_part2);
      }

      SECTION("User offsets buffer too small") {
        // Assume no size for the extra element
        attr_off.resize(attr_off.size() - 1);

        // Submit read query
        Context ctx(config);
        Query query_r(ctx, array, TILEDB_READ);
        query_r.set_buffer("attr", attr_off, attr_val);
        CHECK_THROWS(query_r.submit());
      }
    }
    array.close();
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
  write_dense_array(ctx, array_name, data, data_offsets, TILEDB_ROW_MAJOR);

  SECTION("Full read") {
    Array array(ctx, array_name, TILEDB_READ);
    Config config;

    // Buffers to store the read results
    std::vector<int32_t> attr_val(data.size());
    std::vector<uint64_t> attr_off(data_offsets.size());

    SECTION("No extra element (default case)") {
      config = ctx.config();
      CHECK((std::string)config["sm.var_offsets.extra_element"] == "false");

      // Submit read query
      Query query_r(ctx, array, TILEDB_READ);
      query_r.set_buffer("attr", attr_off, attr_val);
      query_r.set_subarray<int64_t>({1, 2, 1, 2});
      CHECK_NOTHROW(query_r.submit());

      // Check that bytes offsets are properly returned
      CHECK(attr_val == data);
      CHECK(attr_off == data_offsets);
    }

    SECTION("Extra element") {
      config["sm.var_offsets.extra_element"] = "true";
      // Extend offsets buffer to accomodate for the extra element
      attr_off.resize(attr_off.size() + 1);

      SECTION("Byte offsets (default config)") {
        CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");
        Context ctx(config);

        // Submit read query
        Query query_r(ctx, array, TILEDB_READ);
        query_r.set_buffer("attr", attr_off, attr_val);
        query_r.set_subarray<int64_t>({1, 2, 1, 2});
        CHECK_NOTHROW(query_r.submit());

        // Check the extra element is included in the offsets
        data_offsets.push_back(sizeof(data[0]) * data.size());
        CHECK(attr_val == data);
        CHECK(attr_off == data_offsets);
      }

      SECTION("Element offsets") {
        config["sm.var_offsets.mode"] = "elements";
        Context ctx(config);

        // Submit read query
        Query query_r(ctx, array, TILEDB_READ);
        query_r.set_buffer("attr", attr_off, attr_val);
        query_r.set_subarray<int64_t>({1, 2, 1, 2});
        CHECK_NOTHROW(query_r.submit());

        // Check the extra element is included in the offsets
        data_offsets = {0, 1, 3, 5, 6};
        CHECK(attr_val == data);
        CHECK(attr_off == data_offsets);
      }

      SECTION("User offsets buffer too small") {
        // Assume no size for the extra element
        attr_off.resize(attr_off.size() - 1);

        // Submit read query
        Context ctx(config);
        Query query_r(ctx, array, TILEDB_READ);
        query_r.set_buffer("attr", attr_off, attr_val);
        query_r.set_subarray<int64_t>({1, 2, 1, 2});
        CHECK_THROWS(query_r.submit());
      }
    }
    array.close();
  }

  SECTION("Partial read") {
    Array array(ctx, array_name, TILEDB_READ);
    Config config;

    // Assume the user buffers can only store half the data
    std::vector<int32_t> attr_val(data.size() / 2);
    std::vector<uint64_t> attr_off(data_offsets.size() - 2);

    // The expected buffers to be returned after 2 partial reads
    std::vector<int32_t> data_part1 = {1, 2, 3};
    std::vector<uint64_t> data_off_part1 = {0, 4};
    std::vector<int32_t> data_part2 = {4, 5, 6};
    std::vector<uint64_t> data_off_part2 = {0, 8};

    SECTION("Extra element") {
      config["sm.var_offsets.extra_element"] = "true";
      // Extend offsets buffer to accomodate for the extra element
      attr_off.resize(attr_off.size() + 1);

      SECTION("Byte offsets (default config)") {
        CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");
        Context ctx(config);

        // Submit read query
        Query query_r(ctx, array, TILEDB_READ);
        query_r.set_buffer("attr", attr_off, attr_val);
        query_r.set_subarray<int64_t>({1, 2, 1, 2});

        // Check the extra element is included in the offsets part1
        CHECK_NOTHROW(query_r.submit());
        data_off_part1.push_back(sizeof(data_part1[0]) * data_part1.size());
        CHECK(attr_val == data_part1);
        CHECK(attr_off == data_off_part1);

        // Check the extra element is included in the offsets part2
        CHECK_NOTHROW(query_r.submit());
        data_off_part2.push_back(sizeof(data_part2[0]) * data_part2.size());
        CHECK(attr_val == data_part2);
        CHECK(attr_off == data_off_part2);
      }

      SECTION("Element offsets") {
        config["sm.var_offsets.mode"] = "elements";
        Context ctx(config);

        // Submit read query
        Query query_r(ctx, array, TILEDB_READ);
        query_r.set_buffer("attr", attr_off, attr_val);
        query_r.set_subarray<int64_t>({1, 2, 1, 2});

        // Check the extra element is included in the offsets part1
        CHECK_NOTHROW(query_r.submit());
        data_off_part1 = {0, 1, 3};
        CHECK(attr_val == data_part1);
        CHECK(attr_off == data_off_part1);

        // Check the extra element is included in the offsets part2
        CHECK_NOTHROW(query_r.submit());
        data_off_part2 = {0, 2, 3};
        CHECK(attr_val == data_part2);
        CHECK(attr_off == data_off_part2);
      }

      SECTION("User offsets buffer too small") {
        // Assume no size for the extra element
        attr_off.resize(attr_off.size() - 1);

        // Submit read query
        Context ctx(config);
        Query query_r(ctx, array, TILEDB_READ);
        query_r.set_buffer("attr", attr_off, attr_val);
        query_r.set_subarray<int64_t>({1, 2, 1, 2});
        CHECK_THROWS(query_r.submit());
      }
    }

    SECTION("No extra element (default case)") {
      config["sm.var_offsets.extra_element"] = "false";
      Context ctx(config);

      // Submit read query
      Query query_r(ctx, array, TILEDB_READ);
      query_r.set_buffer("attr", attr_off, attr_val);
      query_r.set_subarray<int64_t>({1, 2, 1, 2});

      // Check that first partial read returns expected results
      CHECK_NOTHROW(query_r.submit());
      CHECK(attr_val == data_part1);
      CHECK(attr_off == data_off_part1);

      // Check that second partial read returns expected results
      CHECK_NOTHROW(query_r.submit());
      CHECK(attr_val == data_part2);
      CHECK(attr_off == data_off_part2);
    }
    array.close();
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
  // TODO: use a 32-bit offset vector when it's supported on the write path
  std::vector<uint64_t> data_offsets = {0, 4, 12, 20};
  Context ctx;
  write_sparse_array(ctx, array_name, data, data_offsets, TILEDB_UNORDERED);

  Config config;
  // Change config of offsets bitsize from 64 to 32
  config["sm.var_offsets.bitsize"] = 32;

  std::vector<int32_t> attr_val(data.size());
  std::vector<uint32_t> attr_off(data_offsets.size());

  SECTION("Byte offsets (default case)") {
    CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");
    Context ctx(config);

    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);

    // Read using a 32-bit vector, but cast it to 64-bit pointer so that the API
    // accepts it
    query.set_buffer(
        "attr",
        reinterpret_cast<uint64_t*>(attr_off.data()),
        attr_off.size(),
        attr_val.data(),
        attr_val.size());
    CHECK_NOTHROW(query.submit());

    // Check that byte offsets are properly returned
    std::vector<uint32_t> data_offsets_exp = {0, 4, 12, 20};
    CHECK(attr_val == data);
    CHECK(attr_off == data_offsets_exp);
  }

  SECTION("Element offsets") {
    // Change config of offsets format from bytes to elements
    config["sm.var_offsets.mode"] = "elements";
    Context ctx(config);

    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);

    // Read using a 32-bit vector, but cast it to 64-bit pointer so that the API
    // accepts it
    query.set_buffer(
        "attr",
        reinterpret_cast<uint64_t*>(attr_off.data()),
        attr_off.size(),
        attr_val.data(),
        attr_val.size());
    CHECK_NOTHROW(query.submit());

    // Check that element offsets are properly returned
    std::vector<uint32_t> data_offsets_exp = {0, 1, 3, 5};
    CHECK(attr_val == data);
    CHECK(attr_off == data_offsets_exp);

    array.close();
  }

  SECTION("Extra element") {
    config["sm.var_offsets.extra_element"] = "true";
    Context ctx(config);

    // Extend offsets buffer to accomodate for the extra element
    attr_off.resize(attr_off.size() + 1);

    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);

    // Read using a 32-bit vector, but cast it to 64-bit pointer so that the API
    // accepts it
    query.set_buffer(
        "attr",
        reinterpret_cast<uint64_t*>(attr_off.data()),
        attr_off.size(),
        attr_val.data(),
        attr_val.size());
    CHECK_NOTHROW(query.submit());

    // Check the extra element is included in the offsets
    uint32_t data_size = static_cast<uint32_t>(sizeof(data[0]) * data.size());
    std::vector<uint32_t> data_offsets_exp = {0, 4, 12, 20, data_size};
    CHECK(attr_val == data);
    CHECK(attr_off == data_offsets_exp);
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

  Config config;
  Context ctx;
  std::vector<int32_t> data = {1, 2, 3, 4, 5, 6};
  // TODO: use a 32-bit offset vector when it's supported on the write path
  std::vector<uint64_t> data_offsets = {0, 4, 12, 20};
  write_dense_array(ctx, array_name, data, data_offsets, TILEDB_ROW_MAJOR);

  // Change config of offsets bitsize from 64 to 32
  config["sm.var_offsets.bitsize"] = 32;

  std::vector<int32_t> attr_val(data.size());
  std::vector<uint32_t> attr_off(data_offsets.size());

  SECTION("Byte offsets (default case)") {
    CHECK((std::string)config["sm.var_offsets.mode"] == "bytes");
    Context ctx(config);

    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);

    // Read using a 32-bit vector, but cast it to 64-bit pointer so that the API
    // accepts it
    query.set_buffer(
        "attr",
        reinterpret_cast<uint64_t*>(attr_off.data()),
        attr_off.size(),
        attr_val.data(),
        attr_val.size());
    query.set_subarray<int64_t>({1, 2, 1, 2});
    CHECK_NOTHROW(query.submit());

    // Check that byte offsets are properly returned
    std::vector<uint32_t> data_offsets_exp = {0, 4, 12, 20};
    CHECK(attr_val == data);
    CHECK(attr_off == data_offsets_exp);
  }

  SECTION("Element offsets") {
    // Change config of offsets format from bytes to elements
    config["sm.var_offsets.mode"] = "elements";
    Context ctx(config);

    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);

    // Read using a 32-bit vector, but cast it to 64-bit pointer so that the API
    // accepts it
    query.set_buffer(
        "attr",
        reinterpret_cast<uint64_t*>(attr_off.data()),
        attr_off.size(),
        attr_val.data(),
        attr_val.size());
    query.set_subarray<int64_t>({1, 2, 1, 2});
    CHECK_NOTHROW(query.submit());

    // Check that element offsets are properly returned
    std::vector<uint32_t> data_offsets_exp = {0, 1, 3, 5};
    CHECK(attr_val == data);
    CHECK(attr_off == data_offsets_exp);

    array.close();
  }

  SECTION("Extra element") {
    config["sm.var_offsets.extra_element"] = "true";
    Context ctx(config);

    // Extend offsets buffer to accomodate for the extra element
    attr_off.resize(attr_off.size() + 1);

    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);

    // Read using a 32-bit vector, but cast it to 64-bit pointer so that the API
    // accepts it
    query.set_buffer(
        "attr",
        reinterpret_cast<uint64_t*>(attr_off.data()),
        attr_off.size(),
        attr_val.data(),
        attr_val.size());
    query.set_subarray<int64_t>({1, 2, 1, 2});
    CHECK_NOTHROW(query.submit());

    // Check the extra element is included in the offsets
    uint32_t data_size = static_cast<uint32_t>(sizeof(data[0]) * data.size());
    std::vector<uint32_t> data_offsets_exp = {0, 4, 12, 20, data_size};
    CHECK(attr_val == data);
    CHECK(attr_off == data_offsets_exp);
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