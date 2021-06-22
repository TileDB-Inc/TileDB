/**
 * @file   datatype_traits.h
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
 *
 */

#include "tiledb/sm/misc/datatype_traits.h"
#include "tiledb/sm/enums/datatype.h"

namespace tiledb {
namespace sm {

/*
 * At the present time, all valid datatypes have contiguous values.
 * The lowest is Datatype::INT32 = 0.
 * The largest is Datatype::TIME_AS = 39.
 * Datatype enum derives from uint8_t.
 */
static constexpr bool is_valid_datatype(Datatype d) {
  const auto x = static_cast<uint8_t>(d);
  return 0 <= x && x <= 39;
}

template struct datatype_traits_map<bool, dtm_is_valid>;
template struct new_datatype_traits_map<bool, dtm_is_valid>;

static_assert(new_datatype_traits_map<bool, dtm_is_valid>::map[0],"");

/*
 * One plus and one minus the limits of the datatype
 */
static_assert(
    !is_valid_datatype(Datatype(-1)), "-1 is not a valid Datatype index");
static_assert(
    !is_valid_datatype(Datatype(40)), "40 is not a valid Datatype index");

/*
 * Static checks that the new datatype traits faithfully capture the values of
 * the old ways they were present. Only some these values can be checked with
 * static_assert, those whose values can be declared constexpr. Notably, that
 * excludes those value declarate as std::string, which doesn't have constexpr
 * constructors until C++20.
 *
 * These checks only need to persist for a transition period during the
 * run-time function that use switch statements are being phased out.
 */

constexpr bool is_valid_datatype_static(Datatype d) {
  return datatype_traits_map<bool, dtm_is_valid>::map[static_cast<uint8_t>(d)];
}

constexpr bool does_datatype_match(Datatype d) {
  return d ==
         datatype_traits_map<Datatype, dtm_datatype>::map[static_cast<uint8_t>(d)];
}

constexpr bool does_sizeof_value_type_match(Datatype d) {
  return datatype_size(d) == datatype_traits_map<uint64_t, dtm_sizeof_value_type>::map
                                 [static_cast<uint8_t>(d)];
}

static_assert(is_valid_datatype(Datatype::CHAR), "");
static_assert(is_valid_datatype_static(Datatype::CHAR), "");
static_assert(does_datatype_match(Datatype::CHAR), "");
static_assert(does_sizeof_value_type_match(Datatype::CHAR), "");

static_assert(is_valid_datatype(Datatype::INT8), "");
static_assert(is_valid_datatype_static(Datatype::INT8), "");
static_assert(does_datatype_match(Datatype::INT8), "");
static_assert(does_sizeof_value_type_match(Datatype::INT8), "");

static_assert(is_valid_datatype(Datatype::INT16), "");
static_assert(is_valid_datatype_static(Datatype::INT16), "");
static_assert(does_datatype_match(Datatype::INT16), "");
static_assert(does_sizeof_value_type_match(Datatype::INT16), "");

static_assert(is_valid_datatype(Datatype::INT32), "");
static_assert(is_valid_datatype_static(Datatype::INT32), "");
static_assert(does_datatype_match(Datatype::INT32), "");
static_assert(does_sizeof_value_type_match(Datatype::INT32), "");

static_assert(is_valid_datatype(Datatype::INT64), "");
static_assert(is_valid_datatype_static(Datatype::INT64), "");
static_assert(does_datatype_match(Datatype::INT64), "");
static_assert(does_sizeof_value_type_match(Datatype::INT64), "");

static_assert(is_valid_datatype(Datatype::UINT8), "");
static_assert(is_valid_datatype_static(Datatype::UINT8), "");
static_assert(does_datatype_match(Datatype::UINT8), "");
static_assert(does_sizeof_value_type_match(Datatype::UINT8), "");

static_assert(is_valid_datatype(Datatype::UINT16), "");
static_assert(is_valid_datatype_static(Datatype::UINT16), "");
static_assert(does_datatype_match(Datatype::UINT16), "");
static_assert(does_sizeof_value_type_match(Datatype::UINT16), "");

static_assert(is_valid_datatype(Datatype::UINT32), "");
static_assert(is_valid_datatype_static(Datatype::UINT32), "");
static_assert(does_datatype_match(Datatype::UINT32), "");
static_assert(does_sizeof_value_type_match(Datatype::UINT32), "");

static_assert(is_valid_datatype(Datatype::UINT64), "");
static_assert(is_valid_datatype_static(Datatype::UINT64), "");
static_assert(does_datatype_match(Datatype::UINT64), "");
static_assert(does_sizeof_value_type_match(Datatype::UINT64), "");

static_assert(is_valid_datatype(Datatype::FLOAT32), "");
static_assert(is_valid_datatype_static(Datatype::FLOAT32), "");
static_assert(does_datatype_match(Datatype::FLOAT32), "");
static_assert(does_sizeof_value_type_match(Datatype::FLOAT32), "");

static_assert(is_valid_datatype(Datatype::FLOAT64), "");
static_assert(is_valid_datatype_static(Datatype::FLOAT64), "");
static_assert(does_datatype_match(Datatype::FLOAT64), "");
static_assert(does_sizeof_value_type_match(Datatype::FLOAT64), "");

static_assert(is_valid_datatype(Datatype::STRING_ASCII), "");
static_assert(is_valid_datatype_static(Datatype::STRING_ASCII), "");
static_assert(does_datatype_match(Datatype::STRING_ASCII), "");
static_assert(does_sizeof_value_type_match(Datatype::STRING_ASCII), "");

static_assert(is_valid_datatype(Datatype::STRING_UTF8), "");
static_assert(is_valid_datatype_static(Datatype::STRING_UTF8), "");
static_assert(does_datatype_match(Datatype::STRING_UTF8), "");
static_assert(does_sizeof_value_type_match(Datatype::STRING_UTF8), "");

static_assert(is_valid_datatype(Datatype::STRING_UTF16), "");
static_assert(is_valid_datatype_static(Datatype::STRING_UTF16), "");
static_assert(does_datatype_match(Datatype::STRING_UTF16), "");
static_assert(does_sizeof_value_type_match(Datatype::STRING_UTF16), "");

static_assert(is_valid_datatype(Datatype::STRING_UTF32), "");
static_assert(is_valid_datatype_static(Datatype::STRING_UTF32), "");
static_assert(does_datatype_match(Datatype::STRING_UTF32), "");
static_assert(does_sizeof_value_type_match(Datatype::STRING_UTF32), "");

static_assert(is_valid_datatype(Datatype::STRING_UCS2), "");
static_assert(is_valid_datatype_static(Datatype::STRING_UCS2), "");
static_assert(does_datatype_match(Datatype::STRING_UCS2), "");
static_assert(does_sizeof_value_type_match(Datatype::STRING_UCS2), "");

static_assert(is_valid_datatype(Datatype::STRING_UCS4), "");
static_assert(is_valid_datatype_static(Datatype::STRING_UCS4), "");
static_assert(does_datatype_match(Datatype::STRING_UCS4), "");
static_assert(does_sizeof_value_type_match(Datatype::STRING_UCS4), "");

static_assert(is_valid_datatype(Datatype::ANY), "");
static_assert(is_valid_datatype_static(Datatype::ANY), "");
static_assert(does_datatype_match(Datatype::ANY), "");
static_assert(does_sizeof_value_type_match(Datatype::ANY), "");

static_assert(is_valid_datatype(Datatype::DATETIME_YEAR), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_YEAR), "");
static_assert(does_datatype_match(Datatype::DATETIME_YEAR), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_YEAR), "");

static_assert(is_valid_datatype(Datatype::DATETIME_MONTH), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_MONTH), "");
static_assert(does_datatype_match(Datatype::DATETIME_MONTH), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_MONTH), "");

static_assert(is_valid_datatype(Datatype::DATETIME_WEEK), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_WEEK), "");
static_assert(does_datatype_match(Datatype::DATETIME_WEEK), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_WEEK), "");

static_assert(is_valid_datatype(Datatype::DATETIME_DAY), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_DAY), "");
static_assert(does_datatype_match(Datatype::DATETIME_DAY), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_DAY), "");

static_assert(is_valid_datatype(Datatype::DATETIME_HR), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_HR), "");
static_assert(does_datatype_match(Datatype::DATETIME_HR), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_HR), "");

static_assert(is_valid_datatype(Datatype::DATETIME_MIN), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_MIN), "");
static_assert(does_datatype_match(Datatype::DATETIME_MIN), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_MIN), "");

static_assert(is_valid_datatype(Datatype::DATETIME_SEC), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_SEC), "");
static_assert(does_datatype_match(Datatype::DATETIME_SEC), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_SEC), "");

static_assert(is_valid_datatype(Datatype::DATETIME_MS), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_MS), "");
static_assert(does_datatype_match(Datatype::DATETIME_MS), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_MS), "");

static_assert(is_valid_datatype(Datatype::DATETIME_US), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_US), "");
static_assert(does_datatype_match(Datatype::DATETIME_US), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_US), "");

static_assert(is_valid_datatype(Datatype::DATETIME_NS), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_NS), "");
static_assert(does_datatype_match(Datatype::DATETIME_NS), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_NS), "");

static_assert(is_valid_datatype(Datatype::DATETIME_PS), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_PS), "");
static_assert(does_datatype_match(Datatype::DATETIME_PS), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_PS), "");

static_assert(is_valid_datatype(Datatype::DATETIME_FS), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_FS), "");
static_assert(does_datatype_match(Datatype::DATETIME_FS), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_FS), "");

static_assert(is_valid_datatype(Datatype::DATETIME_AS), "");
static_assert(is_valid_datatype_static(Datatype::DATETIME_AS), "");
static_assert(does_datatype_match(Datatype::DATETIME_AS), "");
static_assert(does_sizeof_value_type_match(Datatype::DATETIME_AS), "");

static_assert(is_valid_datatype(Datatype::TIME_HR), "");
static_assert(is_valid_datatype_static(Datatype::TIME_HR), "");
static_assert(does_datatype_match(Datatype::TIME_HR), "");
static_assert(does_sizeof_value_type_match(Datatype::TIME_HR), "");

static_assert(is_valid_datatype(Datatype::TIME_MIN), "");
static_assert(is_valid_datatype_static(Datatype::TIME_MIN), "");
static_assert(does_datatype_match(Datatype::TIME_MIN), "");
static_assert(does_sizeof_value_type_match(Datatype::TIME_MIN), "");

static_assert(is_valid_datatype(Datatype::TIME_SEC), "");
static_assert(is_valid_datatype_static(Datatype::TIME_SEC), "");
static_assert(does_datatype_match(Datatype::TIME_SEC), "");
static_assert(does_sizeof_value_type_match(Datatype::TIME_SEC), "");

static_assert(is_valid_datatype(Datatype::TIME_MS), "");
static_assert(is_valid_datatype_static(Datatype::TIME_MS), "");
static_assert(does_datatype_match(Datatype::TIME_MS), "");
static_assert(does_sizeof_value_type_match(Datatype::TIME_MS), "");

static_assert(is_valid_datatype(Datatype::TIME_US), "");
static_assert(is_valid_datatype_static(Datatype::TIME_US), "");
static_assert(does_datatype_match(Datatype::TIME_US), "");
static_assert(does_sizeof_value_type_match(Datatype::TIME_US), "");

static_assert(is_valid_datatype(Datatype::TIME_NS), "");
static_assert(is_valid_datatype_static(Datatype::TIME_NS), "");
static_assert(does_datatype_match(Datatype::TIME_NS), "");
static_assert(does_sizeof_value_type_match(Datatype::TIME_NS), "");

static_assert(is_valid_datatype(Datatype::TIME_PS), "");
static_assert(is_valid_datatype_static(Datatype::TIME_PS), "");
static_assert(does_datatype_match(Datatype::TIME_PS), "");
static_assert(does_sizeof_value_type_match(Datatype::TIME_PS), "");

static_assert(is_valid_datatype(Datatype::TIME_FS), "");
static_assert(is_valid_datatype_static(Datatype::TIME_FS), "");
static_assert(does_datatype_match(Datatype::TIME_FS), "");
static_assert(does_sizeof_value_type_match(Datatype::TIME_FS), "");

static_assert(is_valid_datatype(Datatype::TIME_AS), "");
static_assert(is_valid_datatype_static(Datatype::TIME_AS), "");
static_assert(does_datatype_match(Datatype::TIME_AS), "");
static_assert(does_sizeof_value_type_match(Datatype::TIME_AS), "");

}  // namespace sm
}  // namespace tiledb
