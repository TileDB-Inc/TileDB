/**
 * @file   relevant_fragments.h
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
 * This file defines class RelevantFragments.
 */

#ifndef TILEDB_RELEVANT_FRAGMENTS_H
#define TILEDB_RELEVANT_FRAGMENTS_H

#include <atomic>

#include "tiledb/common/common.h"

#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * This class contains the list of relevant fragments. They have either been
 * already computed or they will return all fragments for the array if not.
 */
class RelevantFragments {
 public:
  /**
   * Simple iterator using an index.
   */
  struct Iterator {
    /** Constructs an interator starting at index i. */
    Iterator(const RelevantFragments& parent, size_t i)
        : parent_(parent)
        , i_(i) {
    }

    /** Returns the object at the current postion. */
    unsigned operator*() const {
      return parent_[i_];
    }

    /** Pre-increment operator. */
    Iterator& operator++() {
      i_++;
      return *this;
    }

    /** Post increment operator. */
    Iterator operator++(int) {
      Iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    /** Equality operator. */
    friend bool operator==(const Iterator& a, const Iterator& b) {
      return a.i_ == b.i_;
    };

    /** Non-equality operator. */
    friend bool operator!=(const Iterator& a, const Iterator& b) {
      return a.i_ != b.i_;
    };

   private:
    /** Reference to the parent. */
    const RelevantFragments& parent_;

    /** Current element index. */
    size_t i_;
  };

  /**
   * Constructs a relevant fragment object for an array with `frag_num`
   * fragments.
   */
  RelevantFragments(unsigned frag_num)
      : frag_num_(frag_num)
      , relevant_fragments_computed_(false) {
  }

  /** Returns the start iterator for the relevant fragments. */
  Iterator begin() const {
    return Iterator(*this, 0);
  }

  /** Returns the end iterator for the relevant fragments. */
  Iterator end() const {
    return Iterator(*this, size());
  }

  /** Returns the size of the relevant fragments. */
  inline size_t size() const {
    return relevant_fragments_computed_ ? computed_relevant_fragments_.size() :
                                          frag_num_;
  }

  /**
   * Returns the size of the computed relevant fragments or 0 if not computed.
   */
  inline size_t relevant_fragments_size() const {
    return computed_relevant_fragments_.size();
  }

  /** Returns the relevant fragment at index `i`. */
  inline unsigned operator[](size_t i) const {
    return relevant_fragments_computed_ ? computed_relevant_fragments_[i] : i;
  }

  /** Clears the computed relevant fragments. */
  inline void clear_computed_relevant_fragments() {
    relevant_fragments_computed_ = false;
    computed_relevant_fragments_.clear();
  }

  /** Reserves the computed relevant fragments vector. */
  inline void reserve_computed_relevant_fragments(size_t n) {
    relevant_fragments_computed_ = true;
    computed_relevant_fragments_.reserve(n);
  }

  /** Emplaces a computed relevant fragment to the end of the vector. */
  inline void emplace_computed_relevant_fragments_back(unsigned v) {
    computed_relevant_fragments_.emplace_back(v);
  }

 private:
  /** Number of fragments for the array. */
  unsigned frag_num_;

  /** Are the relevant fragments computed. */
  bool relevant_fragments_computed_;

  /** The vector of computed relevant fragments. */
  std::vector<unsigned> computed_relevant_fragments_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RELEVANT_FRAGMENTS_H
