/**
 * @file test/support/rapidcheck/show/array_schema_templates.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file provides forward declarations of `rc::detail::showValue`
 * overloads, which seemingly must be included prior to the rapidcheck
 * header files.
 */

#include <test/support/rapidcheck/array_schema_templates.h>
#include <test/support/rapidcheck/show.h>
#include <test/support/stdx/traits.h>

namespace rc {

template <stdx::is_fundamental T>
void showImpl(
    const tiledb::test::templates::Domain<T>& domain, std::ostream& os) {
  os << "[" << domain.lower_bound << ", " << domain.upper_bound << "]";
}

template <>
void show<tiledb::test::templates::Domain<int>>(
    const tiledb::test::templates::Domain<int>& domain, std::ostream& os) {
  showImpl(domain, os);
}

template <>
void show<tiledb::test::templates::Domain<uint64_t>>(
    const tiledb::test::templates::Domain<uint64_t>& domain, std::ostream& os) {
  showImpl(domain, os);
}

template <tiledb::sm::Datatype DT>
void showImpl(
    const tiledb::test::templates::Dimension<DT>& dimension, std::ostream& os) {
  os << "{\"domain\": ";
  showImpl(dimension.domain, os);
  os << ", \"extent\": " << dimension.extent << "}";
}

template <>
void show<Dimension<tiledb::sm::Datatype::UINT64>>(
    const templates::Dimension<tiledb::sm::Datatype::UINT64>& dimension,
    std::ostream& os) {
  showImpl(dimension, os);
}

}  // namespace rc
