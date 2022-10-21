/**
 * @file dimension_label_query_create.h
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
 * Factory for creating dimension label query objects.
 */

#ifndef TILEDB_DIMENSION_LABEL_QUERY_CREATE_H
#define TILEDB_DIMENSION_LABEL_QUERY_CREATE_H

#include "tiledb/sm/query/dimension_label/dimension_label_data_query.h"

using namespace tiledb::common;

namespace tiledb::sm {

enum class LabelOrder : uint8_t;

class DimensionLabelQueryCreate {
 public:
  /**
   * Factory method for read data query.
   */
  static DimensionLabelDataQuery* make_write_query(
      const std::string& label_name,
      LabelOrder label_order,
      StorageManager* storage_manager,
      stats::Stats* stats,
      DimensionLabel* dimension_label,
      const Subarray& parent_subarray,
      const QueryBuffer& label_buffer,
      const QueryBuffer& index_buffer,
      const uint32_t dim_idx,
      optional<std::string> fragment_name);
};

}  // namespace tiledb::sm

#endif
