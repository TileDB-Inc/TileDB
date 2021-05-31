/**
 * @file   noop_filter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file declares class ConversionFilter.
 */

#ifndef TILEDB_CONVERSION_FILTER_H
#define TILEDB_CONVERSION_FILTER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/enums/datatype.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * A filter that does nothing. Input is passed unmodified to the output.
 */
class ConversionFilter : public Filter {
 public:
  /**
   * Constructor.
   * @param query_datatype The Dataype to read or write
   * @param store_datatype The Datatype to store on disk
   */
  ConversionFilter(Datatype query_datatype, Datatype store_datatype);

  /** Return the datatype to read or write. */
  Datatype query_datatype() const;

  /** Return the datatype to store on disk or in memory. */
  Datatype store_datatype() const;

  /** Return calculated query size from original size. */
  uint64_t calc_query_size(uint64_t orig_size);

  /** Dumps the filter details in ASCII format in the selected output. */
  void dump(FILE* out) const override;

  /**
   * Run forward.
   */
  Status run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Run reverse.
   */
  Status run_reverse(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

  /** Check if the datatype is convertible or not. */
  static bool is_convertible(Datatype datatype);
 private:
  /** Returns a new clone of this filter. */
  ConversionFilter* clone_impl() const override;

  Datatype query_datatype_;
  Datatype store_datatype_;


  /** Run_forward method templated on the query and store datatypes. */
  template <typename TQurey, typename TStore>
  Status run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const;

  /** Run_forward method templated on the store datatypes. */
  template <typename TStore>
  Status run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const;


  /** Run_reverse method templated on the query and store datatypes. */
  template <typename TQuery, typename TStore>
  Status run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const;

  /** Run_reverse method templated on the query datatype. */
  template <typename TQurey>
  Status run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const;

};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_NOOP_FILTER_H
