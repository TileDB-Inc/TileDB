/**
 * @file   unit_test_config.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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

#include "tiledb/sm/misc/macros.h"

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

  /** Wraps an attribute type to determine if it has been intentionally set. */
  template <typename T>
  class Attribute {
   public:
    /** Constructor. */
    Attribute()
        : set_(false) {
    }

    /** Destructor. */
    ~Attribute() = default;

    /**
     * Returns true if the internal attribute has been set.
     *
     * @return bool
     */
    bool is_set() const {
      return set_;
    }

    /**
     * Sets the internal attribute.
     *
     * @param T the internal attribute value to set.
     */
    void set(const T& attr) {
      attr_ = attr;
      set_ = true;
    }

    /**
     * Sets the internal attribute.
     *
     * @param T the internal attribute value to set.
     */
    void set(T&& attr) {
      attr_ = std::move(attr);
      set_ = true;
    }

    /**
     * Unsets the internal attribute.
     */
    void reset() {
      set_ = false;
    }

    /**
     * Returns value of the internal attribute.
     *
     * @return T the internal attribute value.
     */
    T get() const {
      assert(set_);
      return attr_;
    }

   private:
    DISABLE_COPY_AND_COPY_ASSIGN(Attribute);
    DISABLE_MOVE_AND_MOVE_ASSIGN(Attribute);

    /** True if 'attr_' has been set. */
    bool set_;

    /** Internal attribute. */
    T attr_;
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

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_UNIT_TEST_CONFIG_H
