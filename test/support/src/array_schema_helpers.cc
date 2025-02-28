/**
 * @file   array_schema_helpers.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file defines some array schema test suite helper functions.
 */

#include "test/support/src/array_schema_helpers.h"

using namespace tiledb;

namespace tiledb::test {

bool is_equivalent_filter(
    const tiledb::Filter& left, const tiledb::Filter& right) {
  // FIXME if it ever matters: check options
  return left.filter_type() == right.filter_type();
}

bool is_equivalent_filter_list(
    const tiledb::FilterList& left, const tiledb::FilterList& right) {
  if (left.nfilters() != right.nfilters()) {
    return false;
  }
  for (unsigned i = 0; i < left.nfilters(); i++) {
    if (!(is_equivalent_filter(left.filter(i), right.filter(i)))) {
      return false;
    }
  }
  return true;
}

bool is_equivalent_attribute(
    const tiledb::Attribute& left, const tiledb::Attribute& right) {
  if (!(left.name() == right.name() && left.type() == right.type() &&
        left.cell_val_num() == right.cell_val_num() &&
        left.nullable() == right.nullable() &&
        is_equivalent_filter_list(left.filter_list(), right.filter_list()))) {
    return false;
  }

  const void *leftfill, *rightfill;
  uint64_t leftfillsize, rightfillsize;
  uint8_t leftfillvalid, rightfillvalid;
  if (left.nullable()) {
    left.get_fill_value(&leftfill, &leftfillsize, &leftfillvalid);
    right.get_fill_value(&rightfill, &rightfillsize, &rightfillvalid);
  } else {
    left.get_fill_value(&leftfill, &leftfillsize);
    right.get_fill_value(&rightfill, &rightfillsize);
    leftfillvalid = rightfillvalid = 1;
  }
  return leftfillsize == rightfillsize && leftfillvalid == rightfillvalid &&
         (memcmp(leftfill, rightfill, leftfillsize) == 0);
}

bool is_equivalent_enumeration(
    const sm::Enumeration& left, const sm::Enumeration& right) {
  return left.name() == right.name() && left.type() == right.type() &&
         left.cell_val_num() == right.cell_val_num() &&
         left.ordered() == right.ordered() &&
         std::equal(
             left.data().begin(),
             left.data().end(),
             right.data().begin(),
             right.data().end());
}

}  // namespace tiledb::test
