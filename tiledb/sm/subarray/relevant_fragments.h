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
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /**
   * Size type for the number of dimensions of an array and for dimension
   * indices.
   *
   * Note: This should be the same as `Domain::dimension_size_type`. We're
   * not including `domain.h`, otherwise we'd use that definition here.
   */
  using dimension_size_type = unsigned int;

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

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructs a relevant fragments object for an array with `frag_num`
   * fragments.
   */
  RelevantFragments(size_t array_frag_num)
      : non_computed_fragment_num_(array_frag_num)
      , relevant_fragments_computed_(false) {
  }

  /**
   * Constructs a relevant fragments object from a fragment bytemaps
   * structure.
   */
  RelevantFragments(
      dimension_size_type dim_num,
      size_t array_frag_num,
      std::vector<std::vector<uint8_t>> fragment_bytemaps)
      : non_computed_fragment_num_(0)
      , relevant_fragments_computed_(true) {
    // Recalculate relevant fragments.
    computed_relevant_fragments_.reserve(array_frag_num);
    for (unsigned f = 0; f < array_frag_num; ++f) {
      bool relevant = true;
      for (uint32_t d = 0; d < dim_num; ++d) {
        if (fragment_bytemaps[d][f] == 0) {
          relevant = false;
          break;
        }
      }

      if (relevant) {
        computed_relevant_fragments_.emplace_back(f);
      }
    }
  }

  /**
   * Constructs a relevant fragments object from another filtering fragments
   * that are not in [min, max[.
   */
  RelevantFragments(
      const RelevantFragments& relevant_fragments, unsigned min, unsigned max)
      : non_computed_fragment_num_(0)
      , relevant_fragments_computed_(true) {
    computed_relevant_fragments_.reserve(relevant_fragments.size());
    for (auto f : relevant_fragments) {
      if (f >= min && f < max) {
        computed_relevant_fragments_.emplace_back(f);
      }
    }
  }

  /**
   * Constructs a relevant fragments object from a vector.
   */
  RelevantFragments(std::vector<unsigned>& relevant_fragments)
      : non_computed_fragment_num_(0)
      , relevant_fragments_computed_(true)
      , computed_relevant_fragments_(relevant_fragments) {
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

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
                                          non_computed_fragment_num_;
  }

  /**
   * Returns the size of the computed relevant fragments or 0 if not computed.
   */
  inline size_t relevant_fragments_size() const {
    return computed_relevant_fragments_.size();
  }

  /** Returns the relevant fragment at index `i`. */
  inline unsigned operator[](size_t i) const {
    return relevant_fragments_computed_ ? computed_relevant_fragments_[i] :
                                          static_cast<unsigned>(i);
  }

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** Number of fragments to use when relevant fragments are not computed. */
  size_t non_computed_fragment_num_;

  /** Are the relevant fragments computed. */
  bool relevant_fragments_computed_;

  /** The vector of computed relevant fragments. */
  std::vector<unsigned> computed_relevant_fragments_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RELEVANT_FRAGMENTS_H
