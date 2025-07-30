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

/**
 * Inclusion predicate for objects collected by ls.
 *
 * The predicate accepts:
 * - A string_view of the path to the object.
 * - The size of the object in bytes.
 *
 * The predicate returns true if the object should be included in the results,
 * and false otherwise.
 */
template <class F>
concept FilterPredicate = std::predicate<F, const std::string_view, uint64_t>;

namespace tiledb::sm {
class LsScanException : public StatusException {
 public:
  explicit LsScanException(const std::string& message)
      : StatusException("LsScan", message) {
  }
};

/**
 * Exception thrown when the user callback signals to stop traversal. This will
 * not result in an error and is only used to stop traversal early.
 *
 * @sa VFS::ls_recursive
 */
class LsStopTraversal : public LsScanException {
 public:
  explicit LsStopTraversal()
      : LsScanException("Callback signaled to stop traversal") {
  }
};

using ResultFilter = std::function<bool(const std::string_view&, uint64_t)>;

/**
 * Typedef for ls C API callback as std::function for passing to C++
 *
 * @param path The path of a visited object for the relative filesystem.
 * @param path_len The length of the path.
 * @param object_size The size of the object at the current path.
 * @param data Data passed to the callback used to store collected results.
 * @return 1 if the callback should continue to the next object, 0 to stop
 *      traversal, or -1 if an error occurred.
 */
using LsCallback = std::function<int32_t(const char*, size_t, uint64_t, void*)>;

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
 * @tparam Allocator The data type for the vector's allocator.
 */
template <class scanner_type, class T, class Allocator = std::allocator<T>>
class LsScanIterator {
 public:
  using value_type = T;
  using difference_type = ptrdiff_t;
  using pointer = typename std::vector<T, Allocator>::const_iterator;
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
 */
class LsScanner {
 public:
  /** Constructor. */
  LsScanner(
      const URI& prefix, ResultFilter&& result_filter, bool recursive = false)
      : prefix_(prefix)
      , result_filter_(std::move(result_filter))
      , is_recursive_(recursive) {
  }

 protected:
  /** URI prefix being scanned and filtered for results. */
  const URI prefix_;
  /** Predicate used to filter results. */
  const ResultFilter result_filter_;
  /** Whether or not to recursively scan the prefix. */
  const bool is_recursive_;
};

/**
 * Wrapper for the C API callback function to be passed to the C++ API.
 */
class CallbackWrapperCAPI {
 public:
  /** Default constructor is deleted */
  CallbackWrapperCAPI() = delete;

  /** Constructor */
  CallbackWrapperCAPI(LsCallback cb, void* data)
      : cb_(std::move(cb))
      , data_(data) {
    if (cb_ == nullptr) {
      throw LsScanException("ls_recursive callback function cannot be null");
    } else if (data_ == nullptr) {
      throw LsScanException("ls_recursive data cannot be null");
    }
  }

  /**
   * Operator to wrap the FilterPredicate used in the C++ API.
   * This will throw a LsStopTraversal exception if the user callback returns 0,
   * and will throw a LsScanException if the user callback returns -1.
   *
   * @param path The path of the object.
   * @param size The size of the object in bytes.
   * @return True if the object should be included, False otherwise.
   */
  bool operator()(std::string_view path, const uint64_t size) const {
    int ret = cb_(path.data(), path.size(), size, data_);
    if (ret == 0) {
      // Throw an exception to stop traversal, which will be caught by the C++
      // internal ls_recursive implementation to stop traversal.
      throw LsStopTraversal();
    } else if (ret == -1) {
      throw LsScanException("Error in user callback");
    }
    return ret;
  }

 private:
  /** CAPI callback as function object */
  LsCallback cb_;
  /** User data for callback */
  void* data_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_LS_SCANNER_H
