/**
 * @file filter_create.h
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
 * This file declares class FilterCreate, which contains the factory for the C
 * API and the deserialize function.
 */

#ifndef TILEDB_FILTER_CREATE_H
#define TILEDB_FILTER_CREATE_H

#include "filter.h"

namespace tiledb::sm {

class EncryptionKey;

class FilterCreate {
 public:
  /**
   * Factory method to make a new Filter instance of the given type.
   *
   * @param type Filter type to make
   * @return New Filter instance or nullptr on error.
   */
  static tiledb::sm::Filter* make(tiledb::sm::FilterType type);

  /**
   * Deserializes a new Filter instance from the data in the given buffer.
   *
   * @param deserializer The deserializer to deserialize from.
   * @param encryption_key.
   * @param version Array schema version
   * @return Filter
   */
  static shared_ptr<Filter> deserialize(
      Deserializer& deserializer,
      const EncryptionKey& encryption_key,
      const format_version_t version);

  /**
   * Deserializes a new Filter instance from the data in the given buffer.
   *
   * @param buff The buffer to deserialize from.
   * @param version Array schema version
   * @return Filter
   */
  static shared_ptr<Filter> deserialize(
      Deserializer& deserializer, const format_version_t version);
};

}  // namespace tiledb::sm

#endif  // TILEDB_FILTER_CREATE_H
