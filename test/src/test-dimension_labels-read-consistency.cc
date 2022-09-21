/**
 * @file test-dimension_labels-read-consistency.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Tests dimension labels only read fragments that exist in both labelled and
 * indexed array.
 */

#include <algorithm>

#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/array_schema/dimension_label_schema.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/label_order.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/storage_format/uri/generate_uri.h"

using namespace tiledb::sm;
using namespace tiledb::test;

/**
 * Example dimension label with fixed-sized labels.
 *
 * Dimension label summary:
 * * Order: Increasing
 * * Index dimension: type=uint64_t, domain=[1, 4]
 * * Label dimesnion: type=uint64_t, domain=[0, 400]
 *
 */
class ExampleFixedDimensionLabel : public TemporaryDirectoryFixture {
 public:
  ExampleFixedDimensionLabel()
      : uri_{URI(fullpath("fixed_label"))} {
    uint64_t index_tile_extent{4};
    uint64_t label_tile_extent{401};
    DimensionLabelSchema dim_label_schema{
        tiledb::sm::LabelOrder::INCREASING_LABELS,
        tiledb::sm::Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        tiledb::sm::Datatype::UINT64,
        label_domain,
        &label_tile_extent};
    create_dimension_label(uri_, *ctx->storage_manager(), dim_label_schema);
  }

  /**
   * Returns true if no valid fragments exist in the dimension label.
   */
  bool dimension_label_is_empty() {
    DimensionLabel dimension_label{uri_, ctx->storage_manager()};
    dimension_label.open(
        QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0);
    return dimension_label.indexed_array()->is_empty() &&
           dimension_label.labelled_array()->is_empty();
  }

  /**
   * Reads and returns all data in the indexed array.
   *
   * @returns A vector of the label values read from the indexed array.
   **/
  std::vector<uint64_t> read_indexed_array() {
    // Open dimension label.
    DimensionLabel dimension_label{uri_, ctx->storage_manager()};
    dimension_label.open(
        QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0);

    // Create label query buffer.
    std::vector<uint64_t> label_data(ncells_, 0);
    uint64_t label_data_size{label_data.size() * sizeof(uint64_t)};
    tiledb::sm::QueryBuffer label_data_buffer{
        label_data.data(), nullptr, &label_data_size, nullptr};

    // Create subarray.
    Subarray subarray(
        dimension_label.indexed_array().get(),
        (tiledb::sm::stats::Stats*)nullptr,
        ctx->storage_manager()->logger(),
        true,
        ctx->storage_manager());
    subarray.add_range(0, &index_domain[0], &index_domain[1], nullptr);

    // Create and submit query.
    Query query{ctx->storage_manager(), dimension_label.indexed_array()};
    REQUIRE_TILEDB_STATUS_OK(query.set_subarray(subarray));
    REQUIRE_TILEDB_STATUS_OK(query.set_layout(Layout::ROW_MAJOR));
    REQUIRE_TILEDB_STATUS_OK(query.set_data_buffer(
        dimension_label.label_dimension()->name(),
        label_data_buffer.buffer_,
        label_data_buffer.buffer_size_,
        true));
    REQUIRE_TILEDB_STATUS_OK(query.submit());
    REQUIRE(query.status() == QueryStatus::COMPLETED);

    // Return results
    return label_data;
  }

  /**
   * Returns the index and label data from the labelled array.
   *
   * @returns index_data, label_data
   **/
  tuple<std::vector<uint64_t>, std::vector<uint64_t>> read_labelled_array() {
    // Open dimension label.
    DimensionLabel dimension_label{uri_, ctx->storage_manager()};
    dimension_label.open(
        QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0);

    // Create index query buffer.
    std::vector<uint64_t> index_data(ncells_, 0);
    uint64_t index_data_size{index_data.size() * sizeof(uint64_t)};
    tiledb::sm::QueryBuffer index_data_buffer{
        index_data.data(), nullptr, &index_data_size, nullptr};

    // Create label query buffer.
    std::vector<uint64_t> label_data(ncells_, 0);
    uint64_t label_data_size{label_data.size() * sizeof(uint64_t)};
    tiledb::sm::QueryBuffer label_data_buffer{
        label_data.data(), nullptr, &label_data_size, nullptr};

    // Create and submit query.
    Query query{ctx->storage_manager(), dimension_label.labelled_array()};
    REQUIRE_TILEDB_STATUS_OK(query.set_layout(Layout::ROW_MAJOR));
    REQUIRE_TILEDB_STATUS_OK(query.set_data_buffer(
        dimension_label.label_dimension()->name(),
        label_data_buffer.buffer_,
        label_data_buffer.buffer_size_,
        true));
    REQUIRE_TILEDB_STATUS_OK(query.set_data_buffer(
        dimension_label.index_attribute()->name(),
        index_data_buffer.buffer_,
        index_data_buffer.buffer_size_,
        true));
    REQUIRE_TILEDB_STATUS_OK(query.submit());
    REQUIRE(query.status() == QueryStatus::COMPLETED);

    // Return results.
    return {index_data, label_data};
  }

  /**
   * Write the entire dimension label.
   *
   * @param label_data Label data for the entire label array.
   * @param enable_indexed_array_write If true, write to the indexed array.
   * @param enable_labelled_array_write If true, write to the lablled array.
   */
  void write_dimension_label(
      std::vector<uint64_t> label_data,
      bool enable_indexed_array_write = true,
      bool enable_labelled_array_write = true) {
    // Open dimension label for writing.
    DimensionLabel dimension_label{uri_, ctx->storage_manager()};
    dimension_label.open(
        QueryType::WRITE, EncryptionType::NO_ENCRYPTION, nullptr, 0);

    // Generate single fragment name for queries.
    auto timestamp = dimension_label.indexed_array()->timestamp_end_opened_at();
    auto timestamp2 =
        dimension_label.labelled_array()->timestamp_end_opened_at();
    REQUIRE(timestamp == timestamp2);
    auto fragment_name = tiledb::storage_format::generate_fragment_name(
        timestamp, constants::format_version);

    // Create label query buffer.
    uint64_t label_data_size{label_data.size() * sizeof(uint64_t)};
    tiledb::sm::QueryBuffer label_buffer{
        label_data.data(), nullptr, &label_data_size, nullptr};
    // Write indexed array.
    if (enable_indexed_array_write) {
      write_indexed_array(dimension_label, label_buffer, fragment_name);
    }

    // Read labelled array.
    if (enable_labelled_array_write) {
      write_labelled_array(dimension_label, label_buffer, fragment_name);
    }
  }

  /**
   * Write only to the indexed array.
   *
   * @param label_data Label data to write for the indexed array.
   */
  inline void write_indexed_array_only(std::vector<uint64_t> label_data) {
    write_dimension_label(label_data, true, false);
  }

  /**
   * Write the entire dimension label.
   *
   * @param label_data Label data for the entire label array.
   */
  inline void write_labelled_array_only(std::vector<uint64_t> label_data) {
    write_dimension_label(label_data, false, true);
  }

 protected:
  /** Number of cells in the index. */
  const uint64_t ncells_{4};

  /** Valid range for the index. */
  constexpr static uint64_t index_domain[2]{1, 4};

  /** Valid range for the label. */
  constexpr static uint64_t label_domain[2]{0, 400};

  /** URI of the dimension label. */
  URI uri_;

 private:
  /** Write data to the indexed array. */
  void write_indexed_array(
      const DimensionLabel& dimension_label,
      const tiledb::sm::QueryBuffer& label_data_buffer,
      const std::string& fragment_name) {
    Query query{
        ctx->storage_manager(), dimension_label.indexed_array(), fragment_name};
    REQUIRE_TILEDB_STATUS_OK(query.set_data_buffer(
        dimension_label.label_attribute()->name(),
        label_data_buffer.buffer_,
        label_data_buffer.buffer_size_,
        true));
    auto st = query.submit();
    INFO(st.to_string());
    REQUIRE_TILEDB_STATUS_OK(st);
    REQUIRE(query.status() == QueryStatus::COMPLETED);
  }

  /** Write data to the labelled array. */
  void write_labelled_array(
      const DimensionLabel& dimension_label,
      const tiledb::sm::QueryBuffer& label_data_buffer,
      const std::string& fragment_name) {
    Query query{
        ctx->storage_manager(),
        dimension_label.labelled_array(),
        fragment_name};

    // Create index query buffer.
    std::vector<uint64_t> index_data(ncells_, 0);
    for (uint64_t ii{0}; ii < ncells_; ++ii) {
      index_data[ii] = ii + index_domain[0];
    }
    uint64_t index_data_size{index_data.size() * sizeof(uint64_t)};
    tiledb::sm::QueryBuffer index_data_buffer{
        index_data.data(), nullptr, &index_data_size, nullptr};

    // Create the query.
    REQUIRE_TILEDB_STATUS_OK(query.set_layout(Layout::UNORDERED));
    REQUIRE_TILEDB_STATUS_OK(query.set_data_buffer(
        dimension_label.label_dimension()->name(),
        label_data_buffer.buffer_,
        label_data_buffer.buffer_size_,
        true));
    REQUIRE_TILEDB_STATUS_OK(query.set_data_buffer(
        dimension_label.index_attribute()->name(),
        index_data_buffer.buffer_,
        index_data_buffer.buffer_size_,
        true));
    REQUIRE_TILEDB_STATUS_OK(query.submit());
    REQUIRE(query.status() == QueryStatus::COMPLETED);
  }
};

TEST_CASE_METHOD(
    ExampleFixedDimensionLabel,
    "Read from dimension label with matching fragments",
    "[DimensionLabel][Query]") {
  // Write data to the dimension label.
  std::vector<uint64_t> input_label_data{10, 20, 30, 40};
  write_dimension_label(input_label_data);

  // Verify labelled array data is as expected.
  {
    auto&& [output_index_data, output_label_data] = read_labelled_array();

    // Check label data.
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(output_label_data[ii] == input_label_data[ii]);
    }

    // Check index data.
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(output_index_data[ii] == ii + 1);
    }
  }

  // Verify indexed array data is as expected.
  {
    auto output_label_data = read_indexed_array();
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(output_label_data[ii] == input_label_data[ii]);
    }
  }
}

TEST_CASE_METHOD(
    ExampleFixedDimensionLabel,
    "Read from dimension label with inconsistent fragments",
    "[DimensionLabel][Query]") {
  // Write good, matching fragment.
  std::vector<uint64_t> input_label_data{10, 20, 30, 40};
  write_dimension_label(input_label_data);

  // Write bad, non-matching fragments.
  std::vector<uint64_t> extra_data1{0, 100, 200, 300};
  write_labelled_array_only(extra_data1);
  std::vector<uint64_t> extra_data2{0, 1, 2, 3};
  write_indexed_array_only(extra_data2);

  // Verify labelled array only returns matched fragment data.
  {
    auto&& [output_index_data, output_label_data] = read_labelled_array();

    // Check label data.
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(output_label_data[ii] == input_label_data[ii]);
    }

    // Check index data.
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(output_index_data[ii] == ii + 1);
    }
  }

  // Verify indexed array data is as expected.
  {
    auto output_label_data = read_indexed_array();
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(output_label_data[ii] == input_label_data[ii]);
    }
  }
}

TEST_CASE_METHOD(
    ExampleFixedDimensionLabel,
    "Read from dimension label with disjoint fragments",
    "[DimensionLabel][Query]") {
  // Write bad, non-matching fragments.
  std::vector<uint64_t> extra_data1{0, 100, 200, 300};
  write_labelled_array_only(extra_data1);
  std::vector<uint64_t> extra_data2{0, 1, 2, 3};
  write_indexed_array_only(extra_data2);

  // Verify dimension label opens no fragments.
  REQUIRE(dimension_label_is_empty());
}
