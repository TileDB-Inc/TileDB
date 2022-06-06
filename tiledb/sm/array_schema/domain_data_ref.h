/**
 * @file tiledb/sm/array_schema/domain_data_ref.h
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
 * This file defines an abstract base class for reference to domain data.
 */

#ifndef TILEDB_ARRAYSCHEMA_DOMAIN_DATA_REF_H
#define TILEDB_ARRAYSCHEMA_DOMAIN_DATA_REF_H

#include "tiledb/common/types/untyped_datum.h"

namespace tiledb::type {

/**
 * An abstract base class for a reference to domain data.
 *
 * This class is transitional. In the new type system, it's a reference to a
 * product type of (in general) dynamic types, so it's three levels removed
 * from a primitive type. There's no practical way not have a transition type
 * here while the type infrastructure remains a work in progress.
 */
class DomainDataRef {
 protected:
  /**
   * Default constructor is only for subclasses.
   *
   * The default constructor must be the only constructor because this is an
   * abstract base class.
   */
  DomainDataRef() = default;

 public:
  /**
   * Returns a datum for the dimension at a given index.
   *
   * Each `DomainDataRef` must be created with reference to some specific
   * domain. This base class does not specify anything about how that reference
   * is obtained or held.
   *
   * Note: `unsigned int` is the _de facto_ type for the number of dimensions
   * in a domain.
   *
   * anonymous-param The index of the dimension within the domain.
   * @return
   */
  virtual UntypedDatumView dimension_datum_view(unsigned int) const = 0;
};

}  // namespace tiledb::type
#endif  // TILEDB_ARRAYSCHEMA_DOMAIN_DATA_REF_H
