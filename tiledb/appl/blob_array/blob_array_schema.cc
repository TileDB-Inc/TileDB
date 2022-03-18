/**
 * @file   blob_array_schema.cc
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
 * This file implements the BlobArraySchema class.
 */

#include "tiledb/appl/blob_array/blob_array_schema.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/filter/bit_width_reduction_filter.h"
#include "tiledb/sm/misc/time.h"

using namespace tiledb::common;

namespace tiledb {
namespace appl {
// namespace sm {

// using namespace tiledb::sm;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

BlobArraySchema::BlobArraySchema()
    : ArraySchema(ArrayType::DENSE) {
  allows_dups_ = false;
  array_uri_ = URI();
  uri_ = URI();
  name_ = "";
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;

  // Set domain
  domain_ = nullptr;
  domain_ = create_domain();

  // Set single attribute
  FilterPipeline attribute_filters;
  attribute_filters.add_filter(CompressionFilter(FilterType::FILTER_ZSTD, -1));
  tdb_shared_ptr<Attribute> attribute = create_attribute(attribute_filters);
  add_attribute(attribute);

  // Create dimension map
  dim_map_.clear();
  auto dim_num = domain_->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = dimension(d);
    dim_map_[dim->name()] = dim;
  }

  version_ = constants::format_version;
  auto timestamp = utils::time::timestamp_now_ms();
  timestamp_range_ = std::make_pair(timestamp, timestamp);

  // Set up default filter pipelines for coords, offsets, and validity values.
  cell_var_offsets_filters_.add_filter(CompressionFilter(
      constants::cell_var_offsets_compression,
      constants::cell_var_offsets_compression_level));
  cell_validity_filters_.add_filter(CompressionFilter(
      constants::cell_validity_compression,
      constants::cell_validity_compression_level));
}

BlobArraySchema::BlobArraySchema(const BlobArraySchema* blob_array_schema)
    : ArraySchema(*blob_array_schema) {
}

BlobArraySchema::~BlobArraySchema() {
  //  clear();
}

/* ****************************** */
/*               API              */
/* ****************************** */

void BlobArraySchema::set_schema_based_on_file_details(
    const uint64_t file_size, const bool file_compressed) {
  auto domain =
      create_domain(compute_tile_extent_based_on_file_size(file_size));
  set_domain(domain);

  // Set single attribute
  FilterPipeline attribute_filters;
  if (!file_compressed)
    attribute_filters.add_filter(
        CompressionFilter(FilterType::FILTER_ZSTD, -1));
  tdb_shared_ptr<Attribute> attribute = create_attribute(attribute_filters);

  // Safety check, if the attribute exists from the default constructor we have
  // to remove it else we leak This can be removed when we switch to shared_ptrs
  if (is_attr(constants::blob_array_attribute_name))
    drop_attribute(constants::blob_array_attribute_name);

  add_attribute(attribute);
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

shared_ptr<Domain> BlobArraySchema::create_domain(uint64_t tile_extent) {
  // Create domain
  auto domain = make_shared<Domain>(HERE());
  // Set dimension
  Dimension dimension(constants::blob_array_dimension_name, Datatype::UINT64);
  // Set domain
  std::array<uint64_t, 2> dim_domain = default_domain;
  dim_domain[1] = dim_domain[1] - tile_extent - 1;
  dimension.set_domain(&dim_domain);
  dimension.set_tile_extent(&tile_extent);
  // Set FilterPipeline
  // TODO: make default filter a constant
  FilterPipeline fp;
  fp.add_filter(BitWidthReductionFilter());
  dimension.set_filter_pipeline(&fp);
  domain->add_dimension(&dimension);
  return domain;
}

tdb_shared_ptr<Attribute> BlobArraySchema::create_attribute(
    const FilterPipeline& fp) {
  tdb_shared_ptr<Attribute> attribute = make_shared<
      // Attribute>(constants::blob_array_attribute_name, Datatype::UINT8,
      // false);
      Attribute>(constants::blob_array_attribute_name, Datatype::BLOB, false);
  attribute->set_filter_pipeline(&fp);
  // TBD: Is there currently only one attribute to be concerned with?
  attribute->set_cell_val_num(
      tiledb::sm::constants::var_num);  // tiledb_var_num());  //
                                        // TILEDB_VAR_NUM);
                                        // //1);
  return attribute;
}

uint64_t BlobArraySchema::compute_tile_extent_based_on_file_size(
    const uint64_t file_size) {
  if (file_size > 1024ULL * 1024ULL * 1024ULL * 10ULL) {  // 10GB
    return 1024ULL * 1024 * 100;                          // 100MB
  } else if (file_size > 1024UL * 1024 * 100) {           // 100MB
    return 1024ULL * 1024 * 1;                            // 1MB
  } else if (file_size > 1024UL * 1024 * 1) {             // 1MB
    return 1024ULL * 256;                                 // 1KB
  } else {
    return 1024ULL;  // 1KB
  }
}

}  // namespace appl
}  // namespace tiledb
