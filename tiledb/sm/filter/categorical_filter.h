/**
 * @file   categorical_filter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares class CategoricalFilter.
 */

#ifndef TILEDB_CATEGORICAL_FILTER_H
#define TILEDB_CATEGORICAL_FILTER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/filter.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * A filter.
 */
class CategoricalFilter : public Filter {
 public:
  /**
   * Constructor.
   */
  CategoricalFilter()
      : CategoricalFilter(std::vector<std::string>()) {
  }

  CategoricalFilter(std::vector<std::string> categories)
      : Filter(FilterType::FILTER_CATEGORICAL)
      , categories_(categories) {
  }

  /** Dumps the filter details in ASCII format in the selected output. */
  void dump(FILE* out) const override;

  /**
   * Encrypt the bytes of the input data into the output data buffer.
   */
  Status run_forward(
      const WriterTile& tile,
      WriterTile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Decrypt the bytes of the input data into the output data buffer.
   */
  Status run_reverse(
      const Tile& tile,
      Tile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

  /**
   * Sets an option on the filter.
   *
   * @param option The filter option to set.
   * @param value Value to be set for given option.
   * @return Status::Ok() on success. Throws on failure.
   */
  Status set_option_impl(FilterOption option, const void* value) override;

  /**
   * Gets an option from the filter.
   *
   * @param option The filter option to retrieve.
   * @param value Location to store found option value.
   * @return Status::Ok() on success. Throws on failure.
   */
  Status get_option_impl(FilterOption option, void* value) const override;

  /**
   * Serializes filter metadata.
   *
   * @param serializer Serializer with buffer to store metadata.
   */
  void serialize_impl(Serializer& serializer) const override;

 private:
  /** Returns a new clone of this filter. */
  CategoricalFilter* clone_impl() const override;

  /** The list of categories for this filter. */
  std::vector<std::string> categories_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CATEGORICAL_FILTER_H
