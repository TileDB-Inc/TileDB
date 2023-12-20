/**
 * @file ls_scanner.h
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
 * This defines the LsScanner class and related types used for VFS.
 */

#ifndef TILEDB_LS_SCANNER_H
#define TILEDB_LS_SCANNER_H

#include "tiledb/common/exception/exception.h"
#include "tiledb/sm/filesystem/uri.h"

#include <cstdint>
#include <functional>
#include <stdexcept>

/** Inclusion predicate for objects collected by ls */
template <class F>
concept FilePredicate = true;

/**
 * DirectoryPredicate is currently unused, but is kept here for adding directory
 * pruning support in the future.
 */
template <class F>
concept DirectoryPredicate = true;

namespace tiledb::sm {
class LsScanException : public StatusException {
 public:
  explicit LsScanException(const std::string& message)
      : StatusException("LsScan", message) {
  }
};

using FileFilter = std::function<bool(const std::string_view&, uint64_t)>;

using DirectoryFilter = std::function<bool(const std::string_view&)>;
/** Static DirectoryFilter used as default argument. */
[[maybe_unused]] static bool accept_all_dirs(const std::string_view&) {
  return true;
}

/** Type defintion for objects returned from ls_recursive. */
using LsObjects = std::vector<std::pair<std::string, uint64_t>>;

/**
 * LsScanIterator iterates over the results of ls requests wrapped by classes
 * deriving from LsScanner. See S3Scanner as an example.
 *
 * The iterator is implemented as an input iterator, where the end() iterator
 * is default constructed, resulting in an LsScanIterator::ptr_ that is a
 * non-dereferencable iterator.
 *
 *
 * @tparam scanner_type The LsScanner type that created this iterator.
 * @tparam T The data type stored by this iterator.
 * TODO: Discuss using T for iterator type instead of underlying data type.
 */
template <class scanner_type, class T>
class LsScanIterator {
 public:
  using value_type = T;
  using difference_type = ptrdiff_t;
  using pointer = typename std::vector<T>::const_iterator;
  using reference = const T&;
  using iterator_category = std::input_iterator_tag;

  /**
   * Default constructor.
   *
   * This constructor is used to construct the end() iterator, where ptr_ is
   * default constructed as a non-de-referencable iterator.
   */
  LsScanIterator() noexcept = default;

  /**
   * Constructor.
   *
   * @param scanner The scanner that created this iterator.
   */
  explicit LsScanIterator(scanner_type* scanner) noexcept
      : scanner_(scanner)
      , ptr_(scanner->begin_) {
  }

  /**
   * Constructor.
   *
   * @param scanner The scanner that created this iterator.
   * @param ptr Pointer to set as the current object.
   */
  LsScanIterator(scanner_type* scanner, pointer ptr) noexcept
      : scanner_(scanner)
      , ptr_(ptr) {
  }

  /** Copy constructor. */
  LsScanIterator(const LsScanIterator& rhs) noexcept {
    ptr_ = rhs.ptr_;
    scanner_ = rhs.scanner_;
  }

  /** Copy assignment operator. */
  LsScanIterator& operator=(LsScanIterator rhs) noexcept {
    if (&rhs != this) {
      std::swap(ptr_, rhs.ptr_);
      std::swap(scanner_, rhs.scanner_);
    }
    return *this;
  }

  inline void ensure_dereferenceable() const {
    if (ptr_ == pointer()) {
      throw LsScanException("Failed to dereference invalid iterator.");
    }
  }

  /**
   * Dereference operator.
   *
   * @return The current object being visited.
   */
  constexpr reference operator*() const {
    ensure_dereferenceable();
    return *ptr_;
  }

  /**
   * Dereference operator.
   *
   * @return The current object being visited.
   */
  constexpr pointer operator->() const {
    ensure_dereferenceable();
    return ptr_;
  }

  /**
   * Prefix increment operator.
   * Calls the scanner's next() method to advance to the next object.
   *
   * @return Reference to this iterator after advancing to the next object.
   */
  LsScanIterator& operator++() {
    if (++ptr_ != pointer()) {
      scanner_->next(ptr_);
    }
    return *this;
  }

  /**
   * Postfix increment operator.
   * Calls next() method to advance to the next object via prefix operator.
   *
   * @return Copy of this iterator prior to advancing to the next object.
   */
  LsScanIterator operator++(int) {
    LsScanIterator tmp(*this);
    operator++();
    return tmp;
  }

  /** Inequality operator. */
  friend constexpr bool operator!=(
      const LsScanIterator& lhs, const LsScanIterator& rhs) noexcept {
    return !(lhs.ptr_ == rhs.ptr_);
  }

  /** Equality operator. */
  friend constexpr bool operator==(
      const LsScanIterator& lhs, const LsScanIterator& rhs) noexcept {
    return lhs.ptr_ == rhs.ptr_;
  }

 private:
  /** Pointer to the scanner that created this iterator. */
  scanner_type* scanner_;

  /** Pointer to the current object. */
  pointer ptr_;
};

/**
 * LsScanner is a base class for scanning a filesystem for objects that match
 * the given file and directory predicates. This should be used as a common
 * base class for future filesystem scanner implementations, similar to
 * S3Scanner.
 *
 * @tparam F The FilePredicate type used to filter object results.
 * @tparam D The DirectoryPredicate type used to prune prefix results.
 */
template <FilePredicate F, DirectoryPredicate D>
class LsScanner {
 public:
  /** Constructor. */
  LsScanner(
      const URI& prefix, F file_filter, D dir_filter, bool recursive = false)
      : prefix_(prefix)
      , file_filter_(file_filter)
      , dir_filter_(dir_filter)
      , is_recursive_(recursive) {
  }

 protected:
  /** URI prefix being scanned and filtered for results. */
  const URI prefix_;
  /** File predicate used to filter file or object results. */
  const F file_filter_;
  /** Directory predicate used to prune directory or prefix results. */
  const D dir_filter_;
  /** Whether or not to recursively scan the prefix. */
  const bool is_recursive_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_LS_SCANNER_H
