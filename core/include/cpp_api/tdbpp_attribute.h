/**
 * @file   tiledb.h
 *
 * @author Ravi Gaddipati
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
 * This file declares the C++ API for TileDB.
 */

#ifndef TILEDB_GENOMICS_ATTRIBUTE_H
#define TILEDB_GENOMICS_ATTRIBUTE_H

#include "tdbpp_object.h"
#include "tiledb.h"

#include <functional>

namespace tdb {

  class Context;

  class Attribute {
  public:
    Attribute(Context &ctx) : _ctx(ctx) {}
    Attribute(Context &ctx, tiledb_attribute_t *attr) : _ctx(ctx) {
      if (attr) _init(attr);
    }
    ~Attribute();

  private:
    std::reference_wrapper <Context> _ctx;
    tiledb_datatype_t _type;
    Compressor _compressor;
    unsigned int _num;
    std::string _name;
    tiledb_attribute_t *_attr;

    void _init(tiledb_attribute_t *attr);

  };
}

#endif //TILEDB_GENOMICS_ATTRIBUTE_H
