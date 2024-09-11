/**
 * @file   unit-ordered-dim-label-reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB Inc.
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
 * Tests the ordered dimension label reader.
 */

#include <catch2/matchers/catch_matchers_string.hpp>
using namespace Catch::Matchers;

#include <test/support/src/helpers.h>
#include <test/support/tdb_catch.h>
#include "test/support/src/serialization_wrappers.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/query/readers/ordered_dim_label_reader.h"

#include <numeric>

using namespace tiledb;

template <typename LabelT, typename IndexT>
struct CPPOrderedDimLabelReaderFixedFx {
  const char* array_name = "cpp_ordered_dim_label_reader";

  CPPOrderedDimLabelReaderFixedFx(IndexT tile_size = 10)
      : vfs_(ctx_)
      , labels_(100, std::numeric_limits<LabelT>::lowest())
      , min_index_(std::numeric_limits<IndexT>::max())
      , max_index_(std::numeric_limits<IndexT>::min()) {
    Config config;
    config["sm.query.dense.qc_coords_mode"] = "true";
    ctx_ = Context(config);
    vfs_ = VFS(ctx_);
    increasing_labels_ = true;

    using namespace tiledb;

    if (vfs_.is_dir(array_name)) {
      vfs_.remove_dir(array_name);
    }

    Domain domain(ctx_);
    auto d = Dimension::create<IndexT>(ctx_, "index", {{0, 99}}, tile_size);
    domain.add_dimensions(d);

    auto a = Attribute::create<LabelT>(ctx_, "labels");

    ArraySchema schema(ctx_, TILEDB_DENSE);
    schema.set_domain(domain);
    schema.add_attributes(a);

    Array::create(array_name, schema);
  }

  ~CPPOrderedDimLabelReaderFixedFx() {
    if (vfs_.is_dir(array_name)) {
      vfs_.remove_dir(array_name);
    }
  }

  void write_labels(
      IndexT min_index, IndexT max_index, std::vector<LabelT> labels) {
    Array array(ctx_, array_name, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);
    Subarray subarray(ctx_, array);
    subarray.add_range(0, min_index, max_index);
    if (serialize_) {
      auto subarray_ptr = subarray.ptr().get();
      tiledb_subarray_serialize(
          ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
    }

    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("labels", labels);
    query.submit();
    array.close();

    // Update the labels_ vector. It will contain the full dataset.
    IndexT i = min_index;
    for (auto& label : labels) {
      labels_[i++] = label;
    }

    min_index_ = std::min(min_index_, min_index);
    max_index_ = std::max(max_index_, max_index);
  }

  std::vector<IndexT> read_labels(std::vector<LabelT> ranges) {
    std::vector<IndexT> index(ranges.size());
    Array array(ctx_, array_name, TILEDB_READ);
    Query query(ctx_, array, TILEDB_READ);

    // Set attribute ranges.
    std::vector<Range> input_ranges;
    for (uint64_t r = 0; r < ranges.size() / 2; r++) {
      input_ranges.emplace_back(
          &ranges[r * 2], &ranges[r * 2 + 1], sizeof(LabelT));
    }

    Subarray subarray(ctx_, array);
    subarray.ptr()->subarray_->set_attribute_ranges("labels", input_ranges);
    if (serialize_) {
      auto subarray_ptr = subarray.ptr().get();
      tiledb_subarray_serialize(
          ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
    }

    query.ptr()->query_->set_dimension_label_ordered_read(increasing_labels_);
    query.set_data_buffer("index", index);
    query.set_subarray(subarray);
    query.submit();
    array.close();

    return index;
  }

  void read_all_possible_labels() {
    for (IndexT first = min_index_; first <= max_index_; first++) {
      for (IndexT second = first; second <= max_index_; second++) {
        std::vector<IndexT> index(2);
        Array array(ctx_, array_name, TILEDB_READ);
        Query query(ctx_, array, TILEDB_READ);

        // Set attribute ranges.
        std::vector<Range> input_ranges;
        LabelT boundary_modifier = increasing_labels_ ? 1 : -1;

        // Get the value in between the labels we are testing for or a label
        // before the first one.
        LabelT first_label = first == min_index_ ?
                                 labels_[first] - boundary_modifier :
                                 (labels_[first] + labels_[first - 1]) / 2;

        // Get the value in between the labels we are testing for or a label
        // after the last one.
        LabelT second_label = second == max_index_ ?
                                  labels_[second] + boundary_modifier :
                                  (labels_[second] + labels_[second + 1]) / 2;

        // Always add range so that the lower bound is less than or equal to the
        // upper bound.
        increasing_labels_ ? input_ranges.emplace_back(
                                 &first_label, &second_label, sizeof(LabelT)) :
                             input_ranges.emplace_back(
                                 &second_label, &first_label, sizeof(LabelT));

        Subarray subarray(ctx_, array);
        subarray.ptr()->subarray_->set_attribute_ranges("labels", input_ranges);
        if (serialize_) {
          auto subarray_ptr = subarray.ptr().get();
          tiledb_subarray_serialize(
              ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
        }

        query.ptr()->query_->set_dimension_label_ordered_read(
            increasing_labels_);
        query.set_data_buffer("index", index);
        query.set_subarray(subarray);
        query.submit();
        array.close();

        CHECK(index[0] == first);
        CHECK(index[1] == second);
      }
    }
  }

  Context ctx_;
  VFS vfs_;
  std::vector<LabelT> labels_;
  IndexT min_index_;
  IndexT max_index_;
  bool increasing_labels_;
  bool serialize_ = false;
};

struct CPPOrderedDimLabelReaderFixedDoubleFx
    : public CPPOrderedDimLabelReaderFixedFx<double, int> {};

struct CPPOrderedDimLabelReaderFixedIntFx
    : public CPPOrderedDimLabelReaderFixedFx<int, int> {};

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: Invalid no ranges",
    "[ordered-dim-label-reader][invalid][no-ranges]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif

  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});

  std::vector<int> index(2);

  Array array(ctx_, array_name, TILEDB_READ);
  Query query(ctx_, array, TILEDB_READ);
  Subarray subarray(ctx_, array);
  if (serialize_) {
    auto subarray_ptr = subarray.ptr().get();
    tiledb_subarray_serialize(
        ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
  }

  query.ptr()->query_->set_dimension_label_ordered_read(increasing_labels_);
  query.set_subarray(subarray);
  query.set_data_buffer("index", index);

  REQUIRE_THROWS_AS(query.submit(), tiledb::TileDBError);

  array.close();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: Invalid no buffers",
    "[ordered-dim-label-reader][invalid][no-buffers]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});

  Array array(ctx_, array_name, TILEDB_READ);
  Query query(ctx_, array, TILEDB_READ);

  // Set attribute ranges.
  double val = 0;
  std::vector<Range> input_ranges;
  input_ranges.emplace_back(&val, &val, sizeof(double));

  Subarray subarray(ctx_, array);
  subarray.ptr()->subarray_->set_attribute_ranges("labels", input_ranges);
  if (serialize_) {
    auto subarray_ptr = subarray.ptr().get();
    tiledb_subarray_serialize(
        ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
  }

  query.ptr()->query_->set_dimension_label_ordered_read(increasing_labels_);
  query.set_subarray(subarray);

  REQUIRE_THROWS_WITH(
      query.submit(),
      ContainsSubstring(
          "OrderedDimLabelReader: "
          "Cannot initialize ordered dim label reader; Buffers not set"));

  array.close();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: Invalid wrong buffer name",
    "[ordered-dim-label-reader][invalid][wrong-buffer-name]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});

  std::vector<double> labels(2);

  Array array(ctx_, array_name, TILEDB_READ);
  Query query(ctx_, array, TILEDB_READ);

  // Set attribute ranges.
  double val = 0;
  std::vector<Range> input_ranges;
  input_ranges.emplace_back(&val, &val, sizeof(double));

  Subarray subarray(ctx_, array);
  subarray.ptr()->subarray_->set_attribute_ranges("labels", input_ranges);
  if (serialize_) {
    auto subarray_ptr = subarray.ptr().get();
    tiledb_subarray_serialize(
        ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
  }

  query.ptr()->query_->set_dimension_label_ordered_read(increasing_labels_);
  query.set_subarray(subarray);
  query.set_data_buffer("labels", labels);

  REQUIRE_THROWS_WITH(
      query.submit(),
      ContainsSubstring(
          "OrderedDimLabelReader: "
          "Cannot initialize ordered dim label reader; Wrong buffer set"));

  array.close();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: Invalid wrong buffer size",
    "[ordered-dim-label-reader][invalid][wrong-buffer-size]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});

  std::vector<int> index(3);

  Array array(ctx_, array_name, TILEDB_READ);
  Query query(ctx_, array, TILEDB_READ);

  // Set attribute ranges.
  double val = 0;
  std::vector<Range> input_ranges;
  input_ranges.emplace_back(&val, &val, sizeof(double));

  Subarray subarray(ctx_, array);
  subarray.ptr()->subarray_->set_attribute_ranges("labels", input_ranges);
  if (serialize_) {
    auto subarray_ptr = subarray.ptr().get();
    tiledb_subarray_serialize(
        ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
  }

  query.ptr()->query_->set_dimension_label_ordered_read(increasing_labels_);
  query.set_subarray(subarray);
  query.set_data_buffer("index", index);

  REQUIRE_THROWS_WITH(
      query.submit(),
      ContainsSubstring(
          "OrderedDimLabelReader: "
          "Cannot initialize ordered dim label reader; Wrong buffer size"));

  array.close();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: Invalid ranges set",
    "[ordered-dim-label-reader][invalid][ranges-set]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});

  std::vector<int> index(2);

  Array array(ctx_, array_name, TILEDB_READ);
  Query query(ctx_, array, TILEDB_READ);

  // Set attribute ranges.
  double val = 0;
  std::vector<Range> input_ranges;
  input_ranges.emplace_back(&val, &val, sizeof(double));

  Subarray subarray(ctx_, array);
  subarray.add_range(0, 1, 1);
  subarray.ptr()->subarray_->set_attribute_ranges("labels", input_ranges);
  if (serialize_) {
    auto subarray_ptr = subarray.ptr().get();
    tiledb_subarray_serialize(
        ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
  }

  query.ptr()->query_->set_dimension_label_ordered_read(increasing_labels_);
  query.set_subarray(subarray);
  query.set_data_buffer("index", index);

  REQUIRE_THROWS_WITH(
      query.submit(),
      ContainsSubstring(
          "OrderedDimLabelReader: "
          "Cannot initialize ordered dim label reader; Subarray is set"));

  array.close();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: Invalid no data",
    "[ordered-dim-label-reader][invalid][no-data]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  Array array(ctx_, array_name, TILEDB_READ);
  Query query(ctx_, array, TILEDB_READ);

  // Set attribute ranges.
  std::vector<double> ranges{0.4, 0.8};
  std::vector<Range> input_ranges;
  input_ranges.emplace_back(&ranges[0], &ranges[1], sizeof(double));

  Subarray subarray(ctx_, array);
  subarray.ptr()->subarray_->set_attribute_ranges("labels", input_ranges);
  if (serialize_) {
    auto subarray_ptr = subarray.ptr().get();
    tiledb_subarray_serialize(
        ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
  }

  query.ptr()->query_->set_dimension_label_ordered_read(increasing_labels_);
  std::vector<int> index(ranges.size());
  query.set_data_buffer("index", index);
  query.set_subarray(subarray);
  REQUIRE_THROWS_WITH(
      query.submit(),
      ContainsSubstring("OrderedDimLabelReader: Cannot read dim label; "
                        "Dimension label is empty"));

  array.close();
}

TEST_CASE(
    "Ordered dimension label reader: fixed double labels, single fragment, "
    "increasing, unsigned dimension with tile size 1",
    "[ordered-dim-label-reader][fixed][double][single-fragment]["
    "increasing]") {
  // This test catches an error where a while loop in the implementation
  // never ended.
  CPPOrderedDimLabelReaderFixedFx<double, uint32_t> fixture{1};
#ifdef TILEDB_SERIALIZATION
  fixture.serialize_ = GENERATE(true, false);
#endif
  fixture.write_labels(
      0, 9, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  fixture.read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: fixed double labels, single fragment, "
    "increasing",
    "[ordered-dim-label-reader][fixed][double][single-fragment][increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: fixed double labels, single fragment, "
    "decreasing",
    "[ordered-dim-label-reader][fixed][double][single-fragment][decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(16, 25, {1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: fixed double labels, multiple fragments, "
    "increasing",
    "[ordered-dim-label-reader][fixed][double][multiple-fragments]["
    "increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  write_labels(19, 22, {0.45, 0.55, 0.65, 0.75});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: fixed double labels, multiple fragments, "
    "decreasing",
    "[ordered-dim-label-reader][fixed][double][multiple-fragments]["
    "decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(16, 25, {1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1});
  write_labels(19, 22, {0.75, 0.65, 0.55, 0.45});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: fixed double labels, lots of fragment, "
    "increasing",
    "[ordered-dim-label-reader][fixed][double][lots-of-fragment][increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  write_labels(26, 35, {1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0});
  write_labels(36, 45, {2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0});
  write_labels(46, 55, {3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 4.0});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: fixed double labels, lots of fragment, "
    "decreasing",
    "[ordered-dim-label-reader][fixed][double][lots-of-fragment][decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(36, 45, {4.0, 3.9, 3.8, 3.7, 3.6, 3.5, 3.4, 3.3, 3.2, 3.1});
  write_labels(46, 55, {3.0, 2.9, 2.8, 2.7, 2.6, 2.5, 2.4, 2.3, 2.2, 2.1});
  write_labels(56, 65, {2.0, 1.9, 1.8, 1.7, 1.6, 1.5, 1.4, 1.3, 1.2, 1.1});
  write_labels(66, 75, {1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: fixed labels, empty range, increasing",
    "[ordered-dim-label-reader][fixed][double][empty-range][increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(0, 3, {1.0, 2.0, 3.0, 4.0});
  REQUIRE_THROWS_WITH(
      read_labels({2.1, 2.8}),
      ContainsSubstring("OrderedDimLabelReader: Range contained no values"));
  REQUIRE_THROWS_WITH(
      read_labels({-2.0, 0.0}),
      ContainsSubstring("OrderedDimLabelReader: Range contained no values"));
  REQUIRE_THROWS_WITH(
      read_labels({5.0, 6.0}),
      ContainsSubstring("OrderedDimLabelReader: Range contained no values"));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: fixed labels, empty range, decreasing",
    "[ordered-dim-label-reader][fixed][double][empty-range][decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(0, 3, {4.0, 3.0, 2.0, 1.0});
  std::vector<double> ranges{2.1, 2.8};
  REQUIRE_THROWS_WITH(
      read_labels({2.1, 2.8}),
      ContainsSubstring("OrderedDimLabelReader: Range contained no values"));
  REQUIRE_THROWS_WITH(
      read_labels({-2.0, 0.0}),
      ContainsSubstring("OrderedDimLabelReader: Range contained no values"));
  REQUIRE_THROWS_WITH(
      read_labels({5.0, 6.0}),
      ContainsSubstring("OrderedDimLabelReader: Range contained no values"));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedIntFx,
    "Ordered dimension label reader: fixed int labels, single fragment, "
    "increasing",
    "[ordered-dim-label-reader][fixed][int][single-fragment][increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {1, 3, 5, 7, 9, 11, 13, 15, 17, 19});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedIntFx,
    "Ordered dimension label reader: fixed int labels, single fragment, "
    "decreasing",
    "[ordered-dim-label-reader][fixed][int][single-fragment][decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(16, 25, {19, 17, 15, 13, 11, 9, 7, 5, 3, 1});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedIntFx,
    "Ordered dimension label reader: fixed int labels, multiple fragments, "
    "increasing",
    "[ordered-dim-label-reader][fixed][int][multiple-fragments]["
    "increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {10, 20, 30, 40, 50, 60, 70, 80, 90, 100});
  write_labels(19, 22, {45, 55, 65, 75});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedIntFx,
    "Ordered dimension label reader: fixed int labels, multiple fragments, "
    "decreasing",
    "[ordered-dim-label-reader][fixed][int][multiple-fragments]["
    "decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(16, 25, {100, 90, 80, 70, 60, 50, 40, 30, 20, 10});
  write_labels(19, 22, {75, 65, 55, 45});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedIntFx,
    "Ordered dimension label reader: fixed int labels, boundary conditions in "
    "binary search, increasing",
    "[ordered-dim-label-reader][fixed][int][boundary][binary-search]["
    "increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
  CHECK(read_labels({2, 3}) == std::vector({17, 18}));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedIntFx,
    "Ordered dimension label reader: fixed int labels, boundary conditions in "
    "binary search, decreasing",
    "[ordered-dim-label-reader][fixed][int][boundary][binary-search]["
    "decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(16, 25, {10, 9, 8, 7, 6, 5, 4, 3, 2, 1});
  CHECK(read_labels({8, 9}) == std::vector({17, 18}));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedIntFx,
    "Ordered dimension label reader: fixed int labels, boundary conditions in "
    "tile search, increasing",
    "[ordered-dim-label-reader][fixed][int][boundary][tile-search]["
    "increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
  CHECK(read_labels({5, 6}) == std::vector({20, 21}));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedIntFx,
    "Ordered dimension label reader: fixed int labels, boundary conditions in "
    "tile search, decreasing",
    "[ordered-dim-label-reader][fixed][int][boundary][tile-search]["
    "decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(16, 25, {10, 9, 8, 7, 6, 5, 4, 3, 2, 1});
  CHECK(read_labels({5, 6}) == std::vector({20, 21}));
}

struct CPPOrderedDimLabelReaderVarFx {
  const char* array_name = "cpp_ordered_dim_label_reader";

  CPPOrderedDimLabelReaderVarFx()
      : vfs_(ctx_)
      , labels_(100, std::numeric_limits<double>::lowest())
      , min_index_(std::numeric_limits<int>::max())
      , max_index_(std::numeric_limits<int>::min()) {
    Config config;
    config["sm.query.dense.qc_coords_mode"] = "true";
    ctx_ = Context(config);
    vfs_ = VFS(ctx_);
    increasing_labels_ = true;

    using namespace tiledb;

    if (vfs_.is_dir(array_name)) {
      vfs_.remove_dir(array_name);
    }

    Domain domain(ctx_);
    auto d = Dimension::create<int>(ctx_, "index", {{1, 100}}, 10);
    domain.add_dimensions(d);

    auto a = Attribute::create<std::string>(ctx_, "labels");

    ArraySchema schema(ctx_, TILEDB_DENSE);
    schema.set_domain(domain);
    schema.add_attributes(a);

    Array::create(array_name, schema);
  }

  ~CPPOrderedDimLabelReaderVarFx() {
    if (vfs_.is_dir(array_name)) {
      vfs_.remove_dir(array_name);
    }
  }

  void write_labels(int min_index, int max_index, std::vector<double> labels) {
    // Generate string labels from doubles.
    std::vector<uint64_t> offsets;
    std::stringstream stream;
    uint64_t offset = 0;
    for (auto& label : labels) {
      stream << std::fixed << std::setprecision(2) << label;
      offsets.emplace_back(offset);
      offset += 4;
    }
    auto labels_data = stream.str();

    Array array(ctx_, array_name, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);
    Subarray subarray(ctx_, array);
    subarray.add_range(0, min_index, max_index);
    if (serialize_) {
      auto subarray_ptr = subarray.ptr().get();
      tiledb_subarray_serialize(
          ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
    }

    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("labels", labels_data)
        .set_offsets_buffer("labels", offsets);
    query.submit();
    array.close();

    // Update the labels_ vector. It will contain the full dataset.
    int i = min_index;
    for (auto& label : labels) {
      labels_[i++] = label;
    }

    min_index_ = std::min(min_index_, min_index);
    max_index_ = std::max(max_index_, max_index);
  }

  std::vector<int> read_labels(std::vector<double> ranges) {
    std::vector<int> index(ranges.size());
    Array array(ctx_, array_name, TILEDB_READ);
    Query query(ctx_, array, TILEDB_READ);

    // Set attribute ranges.
    std::vector<Range> input_ranges;
    for (uint64_t r = 0; r < ranges.size() / 2; r++) {
      // Set attribute ranges.
      std::stringstream stream;
      stream << std::fixed << std::setprecision(2) << ranges[r * 2]
             << ranges[r * 2 + 1];
      auto labels_data = stream.str();

      input_ranges.emplace_back(
          labels_data.data(), 4, labels_data.data() + 4, 4);
    }

    Subarray subarray(ctx_, array);
    subarray.ptr()->subarray_->set_attribute_ranges("labels", input_ranges);
    if (serialize_) {
      auto subarray_ptr = subarray.ptr().get();
      tiledb_subarray_serialize(
          ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
    }

    query.ptr()->query_->set_dimension_label_ordered_read(increasing_labels_);
    query.set_data_buffer("index", index);
    query.set_subarray(subarray);
    query.submit();
    array.close();

    return index;
  }

  void read_all_possible_labels() {
    for (int first = min_index_; first <= max_index_; first++) {
      for (int second = first; second <= max_index_; second++) {
        std::vector<int> index(2);
        Array array(ctx_, array_name, TILEDB_READ);
        Query query(ctx_, array, TILEDB_READ);
        double boundary_modifier = increasing_labels_ ? 0.01 : -0.01;

        // Set attribute ranges.
        std::vector<Range> input_ranges;

        // Get the value in between the labels we are testing for or a label
        // before the first one.
        double first_label = first == min_index_ ?
                                 labels_[first] - boundary_modifier :
                                 (labels_[first] + labels_[first - 1]) / 2;

        // Get the value in between the labels we are testing for or a label
        // after the last one.
        double second_label = second == max_index_ ?
                                  labels_[second] + boundary_modifier :
                                  (labels_[second] + labels_[second + 1]) / 2;

        // Set attribute ranges.
        std::stringstream stream;
        stream << std::fixed << std::setprecision(2) << first_label
               << second_label;
        auto labels_data = stream.str();

        // Always add range so that the lower bound is less than or equal to the
        // upper bound.
        increasing_labels_ ?
            input_ranges.emplace_back(
                labels_data.data(), 4, labels_data.data() + 4, 4) :
            input_ranges.emplace_back(
                labels_data.data() + 4, 4, labels_data.data(), 4);

        Subarray subarray(ctx_, array);
        subarray.ptr()->subarray_->set_attribute_ranges("labels", input_ranges);
        if (serialize_) {
          auto subarray_ptr = subarray.ptr().get();
          tiledb_subarray_serialize(
              ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
        }

        query.ptr()->query_->set_dimension_label_ordered_read(
            increasing_labels_);
        query.set_data_buffer("index", index);
        query.set_subarray(subarray);
        query.submit();
        array.close();

        CHECK(index[0] == first);
        CHECK(index[1] == second);
      }
    }
  }

  Context ctx_;
  VFS vfs_;
  std::vector<double> labels_;
  int min_index_;
  int max_index_;
  bool increasing_labels_;
  bool serialize_ = false;
};

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, single fragment, increasing",
    "[ordered-dim-label-reader][var][single-fragment][increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, single fragment, decreasing",
    "[ordered-dim-label-reader][var][single-fragment][decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(16, 25, {1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, multiple fragments, "
    "increasing",
    "[ordered-dim-label-reader][var][multiple-fragments][increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  write_labels(19, 22, {0.45, 0.55, 0.65, 0.75});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, multiple fragments, "
    "decreasing",
    "[ordered-dim-label-reader][var][multiple-fragments][decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(16, 25, {1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1});
  write_labels(19, 22, {0.75, 0.65, 0.55, 0.45});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, lots of fragment, increasing",
    "[ordered-dim-label-reader][var][lots-of-fragment][increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  write_labels(26, 35, {1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0});
  write_labels(36, 45, {2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0});
  write_labels(46, 55, {3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 4.0});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, lots of fragment, decreasing",
    "[ordered-dim-label-reader][var][lots-of-fragment][decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(36, 45, {4.0, 3.9, 3.8, 3.7, 3.6, 3.5, 3.4, 3.3, 3.2, 3.1});
  write_labels(46, 55, {3.0, 2.9, 2.8, 2.7, 2.6, 2.5, 2.4, 2.3, 2.2, 2.1});
  write_labels(56, 65, {2.0, 1.9, 1.8, 1.7, 1.6, 1.5, 1.4, 1.3, 1.2, 1.1});
  write_labels(66, 75, {1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1});
  read_all_possible_labels();
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, boundary conditions in binary "
    "search, increasing",
    "[ordered-dim-label-reader][var][boundary][binary-search][increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  CHECK(read_labels({0.2, 0.3}) == std::vector({17, 18}));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, boundary conditions in binary "
    "search, decreasing",
    "[ordered-dim-label-reader][var][boundary][binary-search][decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(16, 25, {1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1});
  CHECK(read_labels({0.8, 0.9}) == std::vector({17, 18}));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, boundary conditions in tile "
    "search, increasing",
    "[ordered-dim-label-reader][var][boundary][tile-search][increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  CHECK(read_labels({0.5, 0.6}) == std::vector({20, 21}));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, boundary conditions in tile "
    "search, decreasing",
    "[ordered-dim-label-reader][var][boundary][tile-search][decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(16, 25, {1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1});
  CHECK(read_labels({0.5, 0.6}) == std::vector({20, 21}));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, empty range, increasing",
    "[ordered-dim-label-reader][var][empty-range][increasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(1, 4, {1.0, 2.0, 3.0, 4.0});
  REQUIRE_THROWS_WITH(
      read_labels({2.1, 2.8}),
      ContainsSubstring("OrderedDimLabelReader: Range contained no values"));
  REQUIRE_THROWS_WITH(
      read_labels({-2.0, 0.0}),
      ContainsSubstring("OrderedDimLabelReader: Range contained no values"));
  REQUIRE_THROWS_WITH(
      read_labels({5.0, 6.0}),
      ContainsSubstring("OrderedDimLabelReader: Range contained no values"));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, empty range, decreasing",
    "[ordered-dim-label-reader][var][empty-range][decreasing]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  increasing_labels_ = false;
  write_labels(1, 4, {4.0, 3.0, 2.0, 1.0});
  std::vector<double> ranges{2.1, 2.8};
  REQUIRE_THROWS_WITH(
      read_labels({2.1, 2.8}),
      ContainsSubstring("OrderedDimLabelReader: Range contained no values"));
  REQUIRE_THROWS_WITH(
      read_labels({-2.0, 0.0}),
      ContainsSubstring("OrderedDimLabelReader: Range contained no values"));
  REQUIRE_THROWS_WITH(
      read_labels({5.0, 5.0}),
      ContainsSubstring("OrderedDimLabelReader: Range contained no values"));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: memory budget forcing internal loops",
    "[ordered-dim-label-reader][memory-budget]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  // Budget should only allow to load one tile in memory.
  Config cfg;
  cfg.set("sm.mem.total_budget", "100");
  ctx_ = Context(cfg);

  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  Array array(ctx_, array_name, TILEDB_READ);
  Query query(ctx_, array, TILEDB_READ);

  // First range is in the first tile, second range in the second one.
  std::vector<double> ranges{0.15, 0.35, 0.75, 0.85};
  std::vector<int> index(ranges.size());

  // Set attribute ranges.
  std::vector<Range> input_ranges;
  for (uint64_t r = 0; r < ranges.size() / 2; r++) {
    input_ranges.emplace_back(
        &ranges[r * 2], &ranges[r * 2 + 1], sizeof(double));
  }

  Subarray subarray(ctx_, array);
  subarray.ptr()->subarray_->set_attribute_ranges("labels", input_ranges);
  if (serialize_) {
    auto subarray_ptr = subarray.ptr().get();
    tiledb_subarray_serialize(
        ctx_.ptr().get(), array.ptr().get(), &subarray_ptr);
  }

  query.ptr()->query_->set_dimension_label_ordered_read(increasing_labels_);
  query.set_data_buffer("index", index);
  query.set_subarray(subarray);
  query.submit();
  array.close();

  CHECK(index == std::vector({17, 18, 23, 23}));

  // Check the internal loop count against expected value.
  auto stats =
      ((sm::OrderedDimLabelReader*)query.ptr()->query_->strategy())->stats();
  REQUIRE(stats != nullptr);
  auto counters = stats->counters();
  REQUIRE(counters != nullptr);
  auto loop_num = counters->find("Context.Query.Reader.loop_num");
  CHECK(2 == loop_num->second);
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: fixed labels, multiple ranges",
    "[ordered-dim-label-reader][fixed][double][multi-range]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  write_labels(16, 25, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  write_labels(26, 35, {1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0});
  auto index = read_labels({0.85, 1.25, 0.15, 0.75, 1.75, 2.05});
  CHECK(index == std::vector({24, 27, 17, 22, 33, 35}));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedIntFx,
    "Ordered dimension label reader: fixed int labels, discountinuity",
    "[ordered-dim-label-reader][fixed][int][discountinuity]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  SECTION("Test 1") {
    write_labels(16, 20, {1, 2, 3, 4, 5});
    write_labels(22, 25, {7, 8, 9, 10});
  }

  SECTION("Test 2") {
    write_labels(16, 20, {1, 2, 3, 4, 5});
    write_labels(21, 25, {6, 7, 8, 9, 10});
    write_labels(27, 27, {12});
  }

  REQUIRE_THROWS_WITH(
      read_labels({8, 9}),
      ContainsSubstring("ReaderBase: Discontinuity found in array domain"));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedIntFx,
    "Ordered dimension label reader: fixed int labels, out of order",
    "[ordered-dim-label-reader][fixed][int][out-of-order]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  SECTION("Non tile aligned, overlapped 1") {
    write_labels(11, 20, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    write_labels(15, 16, {3, 6});
  }

  SECTION("Non tile aligned, overlapped 2") {
    write_labels(11, 20, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    write_labels(15, 16, {5, 8});
  }

  SECTION("Non tile aligned, overlapped 3") {
    write_labels(11, 30, {1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                          11, 12, 13, 14, 15, 16, 17, 18, 19, 20});
    write_labels(15, 26, {3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
  }

  SECTION("Non tile aligned, overlapped 4") {
    write_labels(11, 30, {1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                          11, 12, 13, 14, 15, 16, 17, 18, 19, 20});
    write_labels(15, 26, {5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 18});
  }

  SECTION("Non tile aligned, overlapped, equality 1") {
    write_labels(11, 20, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    write_labels(15, 16, {4, 6});
  }

  SECTION("Non tile aligned, overlapped, equality 2") {
    write_labels(11, 20, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    write_labels(15, 16, {5, 7});
  }

  SECTION("Tile aligned validate min") {
    write_labels(16, 20, {1, 2, 3, 4, 5});
    write_labels(21, 25, {4, 7, 8, 9, 10});
  }

  SECTION("Tile aligned validate min, equality") {
    write_labels(16, 20, {1, 2, 3, 4, 5});
    write_labels(21, 25, {5, 7, 8, 9, 10});
  }

  SECTION("Non tile aligned, contiguous, validate min") {
    write_labels(16, 21, {1, 2, 3, 4, 5, 6});
    write_labels(22, 25, {5, 8, 9, 10});
  }

  SECTION("Non tile aligned, contiguous, validate min, equality") {
    write_labels(16, 21, {1, 2, 3, 4, 5, 6});
    write_labels(22, 25, {6, 8, 9, 10});
  }

  SECTION("Tile aligned, overlapped, validate min") {
    write_labels(6, 15, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    write_labels(11, 20, {4, 5, 6, 7, 8, 9, 10, 11, 12, 13});
  }

  SECTION("Tile aligned validate max") {
    write_labels(21, 25, {4, 7, 8, 9, 10});
    write_labels(16, 20, {1, 2, 3, 4, 5});
  }

  SECTION("Tile aligned validate max, equality") {
    write_labels(21, 25, {5, 7, 8, 9, 10});
    write_labels(16, 20, {1, 2, 3, 4, 5});
  }

  SECTION("Non tile aligned, contiguous, validate max") {
    write_labels(22, 25, {5, 8, 9, 10});
    write_labels(16, 21, {1, 2, 3, 4, 5, 6});
  }

  SECTION("Non tile aligned, contiguous, validate max, equality") {
    write_labels(22, 25, {6, 8, 9, 10});
    write_labels(16, 21, {1, 2, 3, 4, 5, 6});
  }

  SECTION("Tile aligned, overlapped, validate max") {
    write_labels(16, 25, {4, 5, 6, 7, 8, 9, 10, 11, 12, 13});
    write_labels(11, 20, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
  }

  REQUIRE_THROWS_WITH(
      read_labels({8, 9}),
      ContainsSubstring("ReaderBase: Attribute out of order"));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderFixedDoubleFx,
    "Ordered dimension label reader: fixed double labels, out of order",
    "[ordered-dim-label-reader][fixed][double][out-of-order]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  SECTION("Non tile aligned, overlapped 1") {
    write_labels(11, 20, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
    write_labels(15, 16, {0.3, 0.6});
  }

  SECTION("Non tile aligned, overlapped 2") {
    write_labels(11, 20, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
    write_labels(15, 16, {0.5, 0.8});
  }

  SECTION("Non tile aligned, overlapped 3") {
    write_labels(11, 30, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
                          1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0});
    write_labels(
        15, 26, {0.3, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6});
  }

  SECTION("Non tile aligned, overlapped 4") {
    write_labels(11, 30, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
                          1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0});
    write_labels(
        15, 26, {0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.8});
  }

  SECTION("Non tile aligned, overlapped, equality 1") {
    write_labels(11, 20, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
    write_labels(15, 16, {0.4, 0.6});
  }

  SECTION("Non tile aligned, overlapped, equality 2") {
    write_labels(11, 20, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
    write_labels(15, 16, {0.5, 0.7});
  }

  SECTION("Tile aligned validate min") {
    write_labels(16, 20, {0.1, 0.2, 0.3, 0.4, 0.5});
    write_labels(21, 25, {0.4, 0.7, 0.8, 0.9, 1.0});
  }

  SECTION("Tile aligned validate min, equality") {
    write_labels(16, 20, {0.1, 0.2, 0.3, 0.4, 0.5});
    write_labels(21, 25, {0.5, 0.7, 0.8, 0.9, 1.0});
  }

  SECTION("Non tile aligned, contiguous, validate min") {
    write_labels(16, 21, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6});
    write_labels(22, 25, {0.5, 0.8, 0.9, 1.0});
  }

  SECTION("Non tile aligned, contiguous, validate min, equality") {
    write_labels(16, 21, {0.1, 0.2, 0.3, 0.4, 0.45, 0.5});
    write_labels(22, 25, {0.5, 0.8, 0.9, 1.0});
  }

  SECTION("Tile aligned, overlapped, validate min") {
    write_labels(6, 15, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
    write_labels(11, 20, {0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3});
  }

  SECTION("Tile aligned validate max") {
    write_labels(21, 25, {0.4, 0.7, 0.8, 0.9, 1.0});
    write_labels(16, 20, {0.1, 0.2, 0.3, 0.4, 0.5});
  }

  SECTION("Tile aligned validate max, equality") {
    write_labels(21, 25, {0.5, 0.7, 0.8, 0.9, 1.0});
    write_labels(16, 20, {0.1, 0.2, 0.3, 0.4, 0.5});
  }

  SECTION("Non tile aligned, contiguous, validate max") {
    write_labels(22, 25, {0.5, 0.8, 0.9, 1.0});
    write_labels(16, 21, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6});
  }

  SECTION("Non tile aligned, contiguous, validate max, equality") {
    write_labels(22, 25, {0.5, 0.8, 0.9, 1.0});
    write_labels(16, 21, {0.1, 0.2, 0.3, 0.4, 0.45, 0.5});
  }

  SECTION("Tile aligned, overlapped, validate max") {
    write_labels(16, 25, {0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3});
    write_labels(11, 20, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  }

  REQUIRE_THROWS_WITH(
      read_labels({0.8, 0.9}),
      ContainsSubstring("ReaderBase: Attribute out of order"));
}

TEST_CASE_METHOD(
    CPPOrderedDimLabelReaderVarFx,
    "Ordered dimension label reader: var labels, out of order",
    "[ordered-dim-label-reader][var][out-of-order]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif
  SECTION("Non tile aligned, overlapped 1") {
    write_labels(11, 20, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
    write_labels(15, 16, {0.3, 0.6});
  }

  SECTION("Non tile aligned, overlapped 2") {
    write_labels(11, 20, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
    write_labels(15, 16, {0.5, 0.8});
  }

  SECTION("Non tile aligned, overlapped 3") {
    write_labels(11, 30, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
                          1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0});
    write_labels(
        15, 26, {0.3, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6});
  }

  SECTION("Non tile aligned, overlapped 4") {
    write_labels(11, 30, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
                          1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0});
    write_labels(
        15, 26, {0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.8});
  }

  SECTION("Non tile aligned, overlapped, equality 1") {
    write_labels(11, 20, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
    write_labels(15, 16, {0.4, 0.6});
  }

  SECTION("Non tile aligned, overlapped, equality 2") {
    write_labels(11, 20, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
    write_labels(15, 16, {0.5, 0.7});
  }

  SECTION("Tile aligned validate min") {
    write_labels(16, 20, {0.1, 0.2, 0.3, 0.4, 0.5});
    write_labels(21, 25, {0.4, 0.7, 0.8, 0.9, 1.0});
  }

  SECTION("Tile aligned validate min, equality") {
    write_labels(16, 20, {0.1, 0.2, 0.3, 0.4, 0.5});
    write_labels(21, 25, {0.5, 0.7, 0.8, 0.9, 1.0});
  }

  SECTION("Non tile aligned, contiguous, validate min") {
    write_labels(16, 21, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6});
    write_labels(22, 25, {0.5, 0.8, 0.9, 1.0});
  }

  SECTION("Non tile aligned, contiguous, validate min, equality") {
    write_labels(16, 21, {0.1, 0.2, 0.3, 0.4, 0.45, 0.5});
    write_labels(22, 25, {0.5, 0.8, 0.9, 1.0});
  }

  SECTION("Tile aligned, overlapped, validate min") {
    write_labels(6, 15, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
    write_labels(11, 20, {0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3});
  }

  SECTION("Tile aligned validate max") {
    write_labels(21, 25, {0.4, 0.7, 0.8, 0.9, 1.0});
    write_labels(16, 20, {0.1, 0.2, 0.3, 0.4, 0.5});
  }

  SECTION("Tile aligned validate max, equality") {
    write_labels(21, 25, {0.5, 0.7, 0.8, 0.9, 1.0});
    write_labels(16, 20, {0.1, 0.2, 0.3, 0.4, 0.5});
  }

  SECTION("Non tile aligned, contiguous, validate max") {
    write_labels(22, 25, {0.5, 0.8, 0.9, 1.0});
    write_labels(16, 21, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6});
  }

  SECTION("Non tile aligned, contiguous, validate max, equality") {
    write_labels(22, 25, {0.5, 0.8, 0.9, 1.0});
    write_labels(16, 21, {0.1, 0.2, 0.3, 0.4, 0.45, 0.5});
  }

  SECTION("Tile aligned, overlapped, validate max") {
    write_labels(16, 25, {0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3});
    write_labels(11, 20, {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
  }

  REQUIRE_THROWS_WITH(
      read_labels({0.8, 0.9}),
      ContainsSubstring("ReaderBase: Attribute out of order"));
}
