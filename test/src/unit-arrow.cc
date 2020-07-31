/**
 * @file   unit-arrow_io.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB Inc.
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
 * Tests for the ThleDB Arrow integration.
 */

#include "catch.hpp"
#include "helpers.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/arrow_cdefs.h"
#include "tiledb/sm/misc/arrow_io.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/misc/utils.h"

#include <pybind11/embed.h>
#include <pybind11/pybind11.h>

using namespace tiledb;
namespace py = pybind11;

namespace {
struct CPPArrayFx {
 public:
  CPPArrayFx(std::string uri, const uint64_t col_size = 10)
      : vfs(ctx)
      , uri_(uri) {
    using namespace tiledb;

    if (vfs.is_dir(uri))
      vfs.remove_dir(uri);

    const uint64_t d1_tile = 10;

    Domain domain(ctx);
    auto d1 = Dimension::create<int>(ctx, "d1", {{0, (int)col_size-1}}, d1_tile);
    domain.add_dimensions(d1);

    std::vector<Attribute> attrs;
    attrs.insert(
        attrs.end(),
        {
            Attribute::create<int8_t>(ctx, "int8"),
            Attribute::create<int16_t>(ctx, "int16"),
            Attribute::create<int32_t>(ctx, "int32"),
            Attribute::create<int64_t>(ctx, "int64"),

            Attribute::create<uint8_t>(ctx, "uint8"),
            Attribute::create<uint16_t>(ctx, "uint16"),
            Attribute::create<uint32_t>(ctx, "uint32"),
            Attribute::create<uint64_t>(ctx, "uint64"),

            Attribute::create<float>(ctx, "float32"),
            Attribute::create<double>(ctx, "float64"),
            Attribute::create<std::string>(ctx, "utf_string"),
        });

    FilterList filters(ctx);
    filters.add_filter({ctx, TILEDB_FILTER_LZ4});

    ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(domain);
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_ROW_MAJOR);
    for (auto attr : attrs) {
      attr.set_filter_list(filters);
      schema.add_attribute(attr);
    }

    Array::create(uri, schema);
  }

  ~CPPArrayFx() {
    if (vfs.is_dir(uri_))
      vfs.remove_dir(uri_);
  }

  Context ctx;
  VFS vfs;
  std::string uri_;
};
};  // namespace

TEST_CASE("Arrow IO integration tests", "[arrow]") {
  std::string uri("test_arrow_io");
  const uint64_t col_size = 10;

  CPPArrayFx _fx(uri, col_size);

  py::scoped_interpreter guard{};
  py::object py_data_source;
  py::object py_data_arrays;
  py::object py_data_names;
  size_t data_len;

  // create (arrow) schema and array from pyarrow RecordBatch
  try {
    // use python to get current tiledb_unit path portably
    py::module py_sys = py::module::import("sys");
    auto exe_path = py_sys.attr("executable");
    py::module py_os = py::module::import("os");
    auto exe_dir = py_os.attr("path").attr("dirname")(exe_path);

    // append the tiledb_unit exe dir so that we can import the helper
    py_sys.attr("path").attr("append")(exe_dir);
    // import the arrow helper
    py::module unit_arrow = py::module::import("unit_arrow");

    auto h_data_source = unit_arrow.attr("DataFactory");
    py_data_source = h_data_source(py::int_(col_size));
    py_data_names = py_data_source.attr("names");
    py_data_arrays = py_data_source.attr("arrays");
    data_len = py::len(py_data_arrays);
  } catch (std::exception& e) {
    std::cerr << std::string("[pybind11 error] ") + e.what() << std::endl;
    CHECK(false);
  } catch (...) {
    std::cerr << "[pybind11 error] unknown exception!" << std::endl;
    CHECK(false);
  }

  ////////////////////////////////////////////////////////////////////////////
  using namespace tiledb;

  {
    /*
     * Test write
     */

    Context ctx;
    Array array(ctx, uri, TILEDB_WRITE);
    std::shared_ptr<Query> query(new Query(ctx, array));
    query->set_layout(TILEDB_COL_MAJOR);
    query->add_range(0, 0, 9);

    std::vector<ArrowSchema> vec_schema;
    std::vector<ArrowArray> vec_array;

    vec_schema.resize(data_len);
    vec_array.resize(data_len);

    tiledb::arrow::ArrowAdapter adapter(query);

    for (size_t i = 0; i < data_len; i++) {
      auto py_i = py::int_(i);
      auto pa_name = py_data_names[py_i].cast<std::string>();
      auto pa_array = py_data_arrays[py_i];

      try {
        pa_array.attr("_export_to_c")(
          py::int_((uint64_t)&(vec_array.at(i))), py::int_((uint64_t)&(vec_schema.at(i))));
      } catch (std::exception& e) {
        std::cerr << "unexpected error: " << e.what() << std::endl;
      }
      adapter.import_buffer(
          pa_name.c_str(),
          &(vec_schema.at(i)),
          &(vec_array.at(i)));
    }
    query->submit();
  }

  {
    /*
     * Test read
     */
    Context ctx;
    Array array(ctx, uri, TILEDB_WRITE);
    std::shared_ptr<Query> query(new Query(ctx, array));
    query->set_layout(TILEDB_COL_MAJOR);
    query->add_range(0, 0, 9);
    query->submit();

    std::vector<ArrowSchema> vec_schema;
    std::vector<ArrowArray> vec_array;

    vec_schema.resize(data_len);
    vec_array.resize(data_len);

    tiledb::arrow::ArrowAdapter adapter(query);

    for (size_t i = 0; i < data_len; i++) {
      auto py_i = py::int_(i);
      auto pa_name = py_data_names[py_i].cast<std::string>();
      auto pa_array = py_data_arrays[py_i];

      adapter.export_buffer(
          pa_name.c_str(),
          (void**)&(vec_schema.at(i)),
          (void**)&(vec_array.at(i)));
    }
  }
}