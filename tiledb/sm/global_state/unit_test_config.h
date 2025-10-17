/**
 * @file   unit_test_config.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file declares the UnitTestConfig class.
 */

#ifndef TILEDB_UNIT_TEST_CONFIG_H
#define TILEDB_UNIT_TEST_CONFIG_H

#include <cassert>
#include <optional>

#include "tiledb/common/assert.h"
#include "tiledb/common/macros.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * A global singleton for communication between unit tests and their classes
 * under test.
 */
class UnitTestConfig {
 public:
  /** Singleton instance. Thread-safe. */
  static UnitTestConfig& instance() {
    static UnitTestConfig self;
    return self;
  }

  template <typename T>
  class SetSentinel;

  /** Wraps an attribute type to determine if it has been intentionally set. */
  template <typename T>
  class Attribute {
    friend class SetSentinel<T>;

   public:
    /** Constructor. */
    Attribute() = default;

    DISABLE_COPY_AND_COPY_ASSIGN(Attribute);
    DISABLE_MOVE_AND_MOVE_ASSIGN(Attribute);

    /** Destructor. */
    ~Attribute() = default;

    /**
     * Returns true if the internal attribute has been set.
     *
     * @return bool
     */
    bool is_set() const {
      return attr_.has_value();
    }

    /**
     * Sets the internal attribute.
     *
     * @param attr the internal attribute value to set.
     * @returns Sentinel object that resets the attribute to its old value on
     * destruction.
     */
    SetSentinel<T> set(auto&& attr);

    /**
     * Unsets the internal attribute.
     */
    void reset() {
      attr_.reset();
    }

    /**
     * Returns value of the internal attribute.
     *
     * @return T the internal attribute value.
     */
    const T& get() const {
      passert(attr_.has_value());
      return attr_.value();
    }

   private:
    void assign(std::optional<T>&& value) {
      attr_ = std::move(value);
    }

    /** Attribute value. */
    std::optional<T> attr_;
  };

  template <typename T>
  class [[nodiscard]] SetSentinel {
    friend class Attribute<T>;

   public:
    ~SetSentinel() {
      attribute_.assign(std::move(old_value_));
    }

    DISABLE_COPY_AND_COPY_ASSIGN(SetSentinel);
    DISABLE_MOVE_AND_MOVE_ASSIGN(SetSentinel);

   private:
    SetSentinel(Attribute<T>& attribute, std::optional<T>&& old_value)
        : attribute_(attribute)
        , old_value_(std::move(old_value)) {
    }

    Attribute<T>& attribute_;
    std::optional<T> old_value_;
  };

  /** For every nth multipart upload request, return a non-OK status. */
  Attribute<unsigned int> s3_fail_every_nth_upload_request;

 private:
  /** Constructor. */
  UnitTestConfig() = default;

  /** Destructor. */
  ~UnitTestConfig() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(UnitTestConfig);
  DISABLE_MOVE_AND_MOVE_ASSIGN(UnitTestConfig);
};

template <typename T>
inline UnitTestConfig::SetSentinel<T> UnitTestConfig::Attribute<T>::set(
    auto&& attr) {
  auto old_value = attr_;
  attr_.emplace(std::forward<decltype(attr)>(attr));
  return SetSentinel(*this, std::move(old_value));
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_UNIT_TEST_CONFIG_H
