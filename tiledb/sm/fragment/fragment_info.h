/**
 * @file  fragment_info.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
 * This file defines struct FragmentInfo.
 */

#ifndef TILEDB_FRAGMENT_INFO_H
#define TILEDB_FRAGMENT_INFO_H

#include "tiledb/sm/misc/uri.h"

namespace tiledb {
namespace sm {

/** Stores basic information about a fragment. */
struct FragmentInfo {
  /** The fragment URI. */
  URI uri_;
  /** True if the fragment is sparse, and false if it is dense. */
  bool sparse_;
  /** The timestamp range of the fragment. */
  std::pair<uint64_t, uint64_t> timestamp_range_;
  /** The size of the entire fragment directory. */
  uint64_t fragment_size_;
  /** The fragment's non-empty domain. */
  std::vector<uint8_t> non_empty_domain_;
  /**
   * The fragment's expanded non-empty domain (in a way that
   * it coincides with tile boundaries. Applicable only to
   * dense fragments. For sparse fragments, the expanded
   * domain is the same as the non-empty domain.
   */
  std::vector<uint8_t> expanded_non_empty_domain_;

  /** Constructor. */
  FragmentInfo() {
    uri_ = URI("");
    sparse_ = false;
    timestamp_range_ = {0, 0};
    fragment_size_ = 0;
  }

  /** Constructor. */
  FragmentInfo(
      const URI& uri,
      bool sparse,
      const std::pair<uint64_t, uint64_t>& timestamp_range,
      uint64_t fragment_size,
      const std::vector<uint8_t>& non_empty_domain,
      const std::vector<uint8_t>& expanded_non_empty_domain)
      : uri_(uri)
      , sparse_(sparse)
      , timestamp_range_(timestamp_range)
      , fragment_size_(fragment_size)
      , non_empty_domain_(non_empty_domain)
      , expanded_non_empty_domain_(expanded_non_empty_domain) {
  }

  /** Copy constructor. */
  FragmentInfo(const FragmentInfo& info)
      : FragmentInfo() {
    auto clone = info.clone();
    swap(clone);
  }

  /** Move constructor. */
  FragmentInfo(FragmentInfo&& info)
      : FragmentInfo() {
    swap(info);
  }

  /** Copy assignment operator. */
  FragmentInfo& operator=(const FragmentInfo& info) {
    auto clone = info.clone();
    swap(clone);
    return *this;
  }

  /** Move assignment operator. */
  FragmentInfo& operator=(FragmentInfo&& info) {
    swap(info);
    return *this;
  }

  /**
   * Returns a deep copy of this FragmentInfo.
   * @return New FragmentInfo
   */
  FragmentInfo clone() const {
    FragmentInfo clone;
    clone.uri_ = uri_;
    clone.sparse_ = sparse_;
    clone.timestamp_range_ = timestamp_range_;
    clone.fragment_size_ = fragment_size_;
    clone.non_empty_domain_ = non_empty_domain_;
    clone.expanded_non_empty_domain_ = expanded_non_empty_domain_;
    return clone;
  }

  /** Swaps the contents (all field values) of this info with the given info. */
  void swap(FragmentInfo& info) {
    std::swap(uri_, info.uri_);
    std::swap(sparse_, info.sparse_);
    std::swap(timestamp_range_, info.timestamp_range_);
    std::swap(fragment_size_, info.fragment_size_);
    std::swap(non_empty_domain_, info.non_empty_domain_);
    std::swap(expanded_non_empty_domain_, info.expanded_non_empty_domain_);
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FRAGMENT_INFO_H
