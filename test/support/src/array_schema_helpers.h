/**
 * @file   array_schema_helpers.h
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
 * This file declares some array schema test suite helper functions.
 */

#ifndef TILEDB_TEST_ARRAY_SCHEMA_HELPERS_H
#define TILEDB_TEST_ARRAY_SCHEMA_HELPERS_H

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/cpp_api/tiledb"

#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

namespace tiledb::test {

/**
 * @return if two filters `left` and `right` represent the same transformations
 */
bool is_equivalent_filter(
    const tiledb::Filter& left, const tiledb::Filter& right);

/**
 * @return if two filter lists `left` and `right` have the same filters in the
 * same order
 */
bool is_equivalent_filter_list(
    const tiledb::FilterList& left, const tiledb::FilterList& right);

/**
 * @return if two attributes `left` and `right` are equivalent
 */
bool is_equivalent_attribute(
    const tiledb::Attribute& left, const tiledb::Attribute& right);

/**
 * @return if two enumerations `left` and `right` are equivalent,
 *         i.e. have the same name, datatype, variants, etc
 */
bool is_equivalent_enumeration(
    const sm::Enumeration& left, const sm::Enumeration& right);

/**
 * @return if two enumerations `left` and `right` are equivalent,
 *         i.e. have the same name, datatype, variants, etc
 */
bool is_equivalent_enumeration(
    const Enumeration& left, const Enumeration& right);

}  // namespace tiledb::test

#endif
