/**
 * @file   unit-arrow.cc
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
 * Tests for the TileDB Arrow integration.
 */

#include "catch.hpp"
#include "helpers.h"
#include "tiledb/sm/cpp_api/arrowio"
#include "tiledb/sm/cpp_api/tiledb"
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
  CPPArrayFx(std::string uri, const uint64_t col_size)
      : vfs(ctx)
      , uri(uri) {
    if (vfs.is_dir(uri))
      vfs.remove_dir(uri);

    Domain domain(ctx);
    auto d1 =
        Dimension::create<int>(ctx, "d1", {{0, (int)col_size - 1}}, col_size);
    domain.add_dimensions(d1);

    std::vector<Attribute> attrs;
    attrs.insert(
        attrs.end(),
        {Attribute::create<int8_t>(ctx, "int8"),
         Attribute::create<int16_t>(ctx, "int16"),
         Attribute::create<int32_t>(ctx, "int32"),
         Attribute::create<int64_t>(ctx, "int64"),

         Attribute::create<uint8_t>(ctx, "uint8"),
         Attribute::create<uint16_t>(ctx, "uint16"),
         Attribute::create<uint32_t>(ctx, "uint32"),
         Attribute::create<uint64_t>(ctx, "uint64"),

         Attribute::create<float>(ctx, "float32"),
         Attribute::create<double>(ctx, "float64")});

    // must be constructed manually to get TILEDB_STRING_UTF8 type
    {
      auto str_attr = Attribute(ctx, "utf_string1", TILEDB_STRING_UTF8);
      str_attr.set_cell_val_num(TILEDB_VAR_NUM);
      attrs.push_back(str_attr);
    }
    {
      auto str_attr = Attribute(ctx, "utf_string2", TILEDB_STRING_UTF8);
      str_attr.set_cell_val_num(TILEDB_VAR_NUM);
      attrs.push_back(str_attr);
    }

    // must be constructed manually to get TILEDB_DATETIME_NS type
    auto datetimens_attr = Attribute(ctx, "datetime_ns", TILEDB_DATETIME_NS);
    attrs.push_back(datetimens_attr);

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
    if (vfs.is_dir(uri)) {
      vfs.remove_dir(uri);
    }
  }

 private:
  Context ctx;
  VFS vfs;
  std::string uri;
};

void free_query_buffers(tiledb::Query* const query) {
  auto schema = query->array().schema();
  void* data = nullptr;
  uint64_t data_nelem = 0;
  uint64_t elem_size = 0;
  uint64_t* offsets = nullptr;
  uint64_t offset_nelem = 0;

  for (auto attr_iter : schema.attributes()) {
    auto name = attr_iter.first;
    auto attr = attr_iter.second;

    if (attr.cell_val_num() != TILEDB_VAR_NUM) {
      query->get_buffer(name, &data, &data_nelem, &elem_size);
      std::free(data);
    } else {
      query->get_buffer(
          name, &offsets, &offset_nelem, &data, &data_nelem, &elem_size);
      std::free(data);
      std::free(offsets);
    }
  }

  for (auto dim : schema.domain().dimensions()) {
    auto name = dim.name();

    if (dim.cell_val_num() != TILEDB_VAR_NUM) {
      query->get_buffer(name, &data, &data_nelem, &elem_size);
      std::free(data);
    } else {
      query->get_buffer(
          name, &offsets, &offset_nelem, &data, &data_nelem, &elem_size);
      std::free(data);
      std::free(offsets);
    }
  }
}

void allocate_query_buffers(tiledb::Query* const query) {
  auto schema = query->array().schema();

  bool has_ranges = false;
  for (uint64_t dim_idx = 0; dim_idx < schema.domain().ndim(); dim_idx++) {
    if (query->range_num(dim_idx) > 0) {
      has_ranges = true;
      break;
    }
  }
  if (!has_ranges) {
    throw tiledb::TileDBError("No ranges set for query!");
  }

  for (auto attr_iter : schema.attributes()) {
    auto name = attr_iter.first;
    auto attr = attr_iter.second;

    if (attr.cell_val_num() != TILEDB_VAR_NUM) {
      auto est_size = query->est_result_size(attr.name());
      void* data = std::malloc(est_size);
      query->set_buffer(name, data, est_size);
    } else {
      auto est_size_var = query->est_result_size_var(attr.name());
      void* data = std::malloc(std::get<1>(est_size_var));
      uint64_t* offsets = static_cast<uint64_t*>(
          std::malloc(std::get<0>(est_size_var) * sizeof(uint64_t)));
      query->set_buffer(
          name,
          offsets,
          std::get<0>(est_size_var),
          data,
          std::get<1>(est_size_var));
    }
  }

  for (auto dim : schema.domain().dimensions()) {
    auto name = dim.name();

    if (dim.cell_val_num() != TILEDB_VAR_NUM) {
      auto est_size = query->est_result_size(dim.name());
      void* data = std::malloc(est_size);
      query->set_buffer(name, data, est_size);
    } else {
      auto est_size_var = query->est_result_size_var(dim.name());
      void* data = std::malloc(std::get<1>(est_size_var));
      uint64_t* offsets = static_cast<uint64_t*>(
          std::malloc(std::get<0>(est_size_var) * sizeof(uint64_t)));
      query->set_buffer(
          name,
          offsets,
          std::get<0>(est_size_var),
          data,
          std::get<1>(est_size_var));
    }
  }
}

};  // namespace

TEST_CASE("Arrow IO integration tests", "[arrow]") {
  std::string uri("test_arrow_io");
  const uint64_t col_size = 111;

  CPPArrayFx _fx(uri, col_size);

  py::scoped_interpreter guard{};
  py::object py_data_source;
  py::object py_data_arrays;
  py::object py_data_names;
  py::object ds_import;
  py::module unit_arrow;
  size_t data_len = 0;

  // create (arrow) schema and array from pyarrow RecordBatch
  py::module py_sys = py::module::import("sys");

#ifdef TILEDB_PYTHON_UNIT_PATH
  // append the tiledb_unit exe dir so that we can import the helper
  py_sys.attr("path").attr("insert")(1, TILEDB_PYTHON_UNIT_PATH);
#endif
#ifdef TILEDB_PYTHON_SITELIB_PATH
  // append the site-packages path from cmake
  // this is not necessary with conda
  py_sys.attr("path").attr("insert")(1, TILEDB_PYTHON_SITELIB_PATH);
#endif

  // import the arrow helper
  unit_arrow = py::module::import("unit_arrow");

  // this class generates random test data for each attribute
  auto h_data_source = unit_arrow.attr("DataFactory");
  py_data_source = h_data_source(py::int_(col_size));
  py_data_names = py_data_source.attr("names");
  py_data_arrays = py_data_source.attr("arrays");
  data_len = py::len(py_data_arrays);
  ds_import = py_data_source.attr("import_result");

  // SECTION("Test writing data via ArrowAdapter from pyarrow arrays")
  {
    /*
     * Test write
     */
    Config config;
    config["sm.var_offsets.bitsize"] = 32;
    config["sm.var_offsets.mode"] = "elements";
    config["sm.var_offsets.extra_element"] = "true";
    Context ctx(config);
    Array array(ctx, uri, TILEDB_WRITE);
    Query query(ctx, array);
    query.set_layout(TILEDB_COL_MAJOR);
    query.add_range(0, (int32_t)0, (int32_t)(col_size - 1));

    std::vector<ArrowArray*> vec_array;
    std::vector<ArrowSchema*> vec_schema;
    vec_array.resize(data_len);
    vec_schema.resize(data_len);

    for (size_t i = 0; i < data_len; i++) {
      vec_array[i] = static_cast<ArrowArray*>(std::malloc(sizeof(ArrowArray)));
      vec_schema[i] =
          static_cast<ArrowSchema*>(std::malloc(sizeof(ArrowSchema)));
    }

    tiledb::arrow::ArrowAdapter adapter(&ctx, &query);

    for (size_t i = 0; i < data_len; i++) {
      auto py_i = py::int_(i);
      auto pa_name = py_data_names[py_i].cast<std::string>();
      auto pa_array = py_data_arrays[py_i];

      try {
        pa_array.attr("_export_to_c")(
            py::int_(reinterpret_cast<uintptr_t>(vec_array.at(i))),
            py::int_(reinterpret_cast<uintptr_t>(vec_schema.at(i))));
      } catch (std::exception& e) {
        std::cerr << "unexpected error: " << e.what() << std::endl;
      }
      adapter.import_buffer(
          pa_name.c_str(),
          static_cast<void*>(vec_array.at(i)),
          static_cast<void*>(vec_schema.at(i)));
    }
    query.submit();
    CHECK(query.query_status() == Query::Status::COMPLETE);

    for (size_t i = 0; i < data_len; i++) {
      std::free(vec_array[i]);
      std::free(vec_schema[i]);
    }
  }

  // TODO: Ideally these scopes should be separate Catch2 SECTION blocks.
  // However, there is an unexplained crash due to an early destructor
  // when both brace scopes are converted to SECTIONs.
  // SECTION("Test reading data back via ArrowAdapter into pyarrow arrays")

  // test both bitsize read modes
  for (auto bitsize : {32, 64}) {
    /*
     * Test read
     */
    Config config;
    config["sm.var_offsets.bitsize"] = bitsize;
    config["sm.var_offsets.mode"] = "elements";
    config["sm.var_offsets.extra_element"] = "true";
    Context ctx(config);
    Array array(ctx, uri, TILEDB_READ);
    Query query(ctx, array);
    query.set_layout(TILEDB_COL_MAJOR);
    query.add_range(
        0, static_cast<int32_t>(0), static_cast<int32_t>(col_size - 1));

    allocate_query_buffers(&query);
    query.submit();
    assert(query.query_status() == Query::Status::COMPLETE);

    std::vector<ArrowArray*> vec_array;
    std::vector<ArrowSchema*> vec_schema;
    vec_array.resize(data_len);
    vec_schema.resize(data_len);

    for (size_t i = 0; i < data_len; i++) {
      vec_array[i] = static_cast<ArrowArray*>(std::malloc(sizeof(ArrowArray)));
      vec_schema[i] =
          static_cast<ArrowSchema*>(std::malloc(sizeof(ArrowSchema)));
    }

    tiledb::arrow::ArrowAdapter adapter(&ctx, &query);

    for (size_t i = 0; i < data_len; i++) {
      auto py_i = py::int_(i);
      auto pa_name = py_data_names[py_i].cast<std::string>();
      auto pa_array = py_data_arrays[py_i];

      adapter.export_buffer(
          pa_name.c_str(),
          static_cast<void*>(vec_array.at(i)),
          static_cast<void*>(vec_schema.at(i)));

      ds_import(
          pa_name,
          py::int_((ptrdiff_t)(vec_array.at(i))),
          py::int_((ptrdiff_t)(vec_schema.at(i))));
    }

    // check the data
    CHECK(py_data_source.attr("check")().cast<bool>());

    for (size_t i = 0; i < data_len; i++) {
      auto arw_array = vec_array.at(i);
      auto arw_schema = vec_schema.at(i);
      // ensure released
      CHECK(arw_array->release == nullptr);
      CHECK(arw_schema->release == nullptr);
    }

    // free structs
    for (auto array_p : vec_array)
      std::free(array_p);

    for (auto schema_p : vec_schema)
      std::free(schema_p);

    free_query_buffers(&query);
  }
}
