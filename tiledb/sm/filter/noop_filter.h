/**
 * @file   noop_filter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file declares class NoopFilter.
 */

#ifndef TILEDB_NOOP_FILTER_H
#define TILEDB_NOOP_FILTER_H

#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/**
 * A filter that does nothing. Input is passed unmodified to the output.
 */
class NoopFilter : public Filter {
 public:
  /**
   * Constructor.
   */
  NoopFilter();

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
      FilterBuffer* output) const override;

 private:
  /** Returns a new clone of this filter. */
  NoopFilter* clone_impl() const override;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_NOOP_FILTER_H
