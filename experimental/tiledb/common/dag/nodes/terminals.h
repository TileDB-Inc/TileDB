/**
 * @file   consumer.h
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
 * This file declares the consumers classes for dag.
 */

#ifndef TILEDB_DAG_NODES_TERMINAL_H
#define TILEDB_DAG_NODES_TERMINAL_H

namespace tiledb::common {

/**
 * Simple consumer function object class.  Takes items and puts them on an
 * output iterator and increments the iterator.
 *
 * @tparam OutputIterator The type of the OutputIterator to use.
 * @tparam Block The datatype of objects being sent to the OutputIterator. Must
 * be convertible to the type expected by the OutputIterator.  (Note the
 * `value_type` of an OutputIterator defaults to `void`).
 */
template <class OutputIterator, class Block = size_t>
class terminal {
  OutputIterator iter_;

  /**
   * Constructor.  Saves the designated OutputIterator.
   */
 public:
  explicit terminal(OutputIterator iter)
      : iter_(iter) {
  }

  /**
   * Function operator.  Puts its argument onto the saved output iterator.
   *
   * @param item The data item to be put on the output iterator.
   */
  void operator()(const Block& item) {
    *iter_++ = item;
  }
};
}  // namespace tiledb::common
#endif  // TILEDB_DAG_NODES_TERMINAL_H
