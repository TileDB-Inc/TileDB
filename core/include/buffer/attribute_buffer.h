/**
 * @file   attribute_buffer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines class AttributeBuffer.
 */

#ifndef TILEDB_ATTRIBUTE_BUFFER_H
#define TILEDB_ATTRIBUTE_BUFFER_H

#include "attribute.h"
#include "buffer.h"
#include "status.h"

namespace tiledb {

class AttributeBuffer {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  AttributeBuffer();

  ~AttributeBuffer();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  bool overflow() const;

  Status set(void* buffer, uint64_t buffer_size);

  Status set(const Attribute* attr, void* buffer, uint64_t buffer_size);

  Status set(
      void* buffer,
      uint64_t buffer_size,
      void* buffer_var,
      uint64_t buffer_var_size);

  Status set(
      const Attribute* attr,
      void* buffer,
      uint64_t buffer_size,
      void* buffer_var,
      uint64_t buffer_var_size);


 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  const Attribute* attr_;

  Buffer* buf_;

  Buffer* buf_var_;
};

}  // namespace tiledb

#endif  // TILEDB_ATTRIBUTE_BUFFER_H
