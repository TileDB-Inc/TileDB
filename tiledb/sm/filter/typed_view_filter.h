/**
 * @file   typed_view_filter.h
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
 * This file declares class TypedViewFilter.
 *
 * This filter is nearly a no-op, except that it changes the output type as
 * specified in the filter options. This is useful for filters that only work on
 * a specific type, such as the delta filters, which do not work on floating
 * point data; in that case, we would like to convert the data to an integer
 * type before running the filter.
 */

#ifndef TILEDB_TYPEDVIEW_FILTER_H
#define TILEDB_TYPEDVIEW_FILTER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/filter/filter.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * A filter that changes the output type. Input is passed unmodified to the
 * output.
 */
class TypedViewFilter : public Filter {
 public:
  /**
   * Constructor.
   */
  TypedViewFilter(std::optional<Datatype> output_datatype = std::nullopt);

  /** Dumps the filter details in ASCII format in the selected output. */
  void dump(FILE* out) const override;

  /** Returns the output data type. */
  Datatype output_datatype() const override;

  /** Sets an option on this filter. */
  Status set_option_impl(FilterOption option, const void* value) override;

  /** Gets an option from this filter. */
  Status get_option_impl(FilterOption option, void* value) const override;

  /**
   * Run forward.
   */
  Status run_forward(
      const WriterTile& tile,
      WriterTile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Run reverse.
   */
  Status run_reverse(
      const Tile& tile,
      Tile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

 private:
  /** Returns a new clone of this filter. */
  TypedViewFilter* clone_impl() const override;

  /** The output data type. */
  std::optional<Datatype> output_datatype_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TYPEDVIEW_FILTER_H
