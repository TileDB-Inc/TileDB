/**
 * @file test/support/rapidcheck/array_schema_templates.h
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
 * This file defines rapidcheck generators for the structures
 * defined in test/support/src/array_schema_templates.h.
 */

#ifndef TILEDB_RAPIDCHECK_ARRAY_SCHEMA_H
#define TILEDB_RAPIDCHECK_ARRAY_SCHEMA_H

#include <test/support/src/array_schema_templates.h>
#include <test/support/stdx/tuple.h>
#include <test/support/tdb_rapidcheck.h>

#include "tiledb/common/arithmetic.h"

namespace rc {

using namespace tiledb::test;
using namespace tiledb::test::templates;

template <DimensionType D>
Gen<templates::Domain<D>> make_domain(std::optional<D> bound = std::nullopt) {
  auto bounds = gen::mapcat(gen::arbitrary<D>(), [bound](D lb) {
    const D ub_limit =
        (bound.has_value() ?
             tiledb::common::checked_arithmetic<D>::add(lb, bound.value())
                 .value_or(std::numeric_limits<D>::max()) :
             std::numeric_limits<D>::max());
    if constexpr (std::is_same_v<D, int64_t> || std::is_same_v<D, uint64_t>) {
      return gen::pair(gen::just(lb), gen::inRange(lb, ub_limit));
    } else {
      // NB: `gen::inRange` is exclusive at the upper end but tiledb domain is
      // inclusive. So we have to use `int64_t` to avoid overflow.
      return gen::pair(
          gen::just(lb),
          gen::cast<D>(gen::inRange(int64_t(lb), int64_t(ub_limit) + 1)));
    }
  });

  return gen::map(bounds, [](std::pair<D, D> bounds) {
    return templates::Domain<D>(bounds.first, bounds.second);
  });
}

template <DimensionType D>
struct Arbitrary<templates::Domain<D>> {
  static Gen<templates::Domain<D>> arbitrary() {
    return make_domain<D>();
  }
};

template <DimensionType D>
Gen<D> make_extent(
    const templates::Domain<D>& domain, std::optional<D> bound = std::nullopt) {
  // upper bound on all possible extents to avoid unreasonably
  // huge tile sizes
  static constexpr D extent_limit = static_cast<D>(
      std::is_signed<D>::value ?
          std::min(
              static_cast<int64_t>(std::numeric_limits<D>::max()),
              static_cast<int64_t>(1024 * 16)) :
          std::min(
              static_cast<uint64_t>(std::numeric_limits<D>::max()),
              static_cast<uint64_t>(1024 * 16)));

  const D extent_bound =
      (bound.has_value() ? std::min(bound.value(), extent_limit) :
                           extent_limit);

  // NB: `gen::inRange` is exclusive at the upper end but tiledb domain is
  // inclusive. So we have to be careful to avoid overflow.

  D extent_lower_bound = 1;
  D extent_upper_bound;

  const auto bound_distance = tiledb::common::checked_arithmetic<D>::sub(
      domain.upper_bound, domain.lower_bound);
  if (bound_distance.has_value()) {
    extent_upper_bound =
        (bound_distance.value() < extent_bound ? bound_distance.value() + 1 :
                                                 extent_bound);
  } else {
    extent_upper_bound = extent_bound;
  }

  return gen::inRange(extent_lower_bound, extent_upper_bound + 1);
}

template <tiledb::sm::Datatype D>
Gen<templates::Dimension<D>> make_dimension(
    std::optional<typename templates::Dimension<D>::value_type> extent_bound =
        std::nullopt,
    std::optional<typename templates::Dimension<D>::value_type> domain_bound =
        std::nullopt) {
  using CoordType = templates::Dimension<D>::value_type;
  auto tup = gen::mapcat(
      make_domain<CoordType>(domain_bound),
      [extent_bound](Domain<CoordType> domain) {
        return gen::pair(gen::just(domain), make_extent(domain, extent_bound));
      });

  return gen::map(tup, [](std::pair<Domain<CoordType>, CoordType> tup) {
    return templates::Dimension<D>(tup.first, tup.second);
  });
}

template <tiledb::sm::Datatype D>
struct Arbitrary<templates::Dimension<D>> {
  static Gen<templates::Dimension<D>> arbitrary() {
    return make_dimension<D>();
  }
};

template <DimensionType D>
Gen<D> make_coordinate(const templates::Domain<D>& domain) {
  // `gen::inRange` does an exclusive upper bound,
  // whereas the domain upper bound is inclusive.
  // As a result some contortion is required to deal
  // with numeric_limits.
  if constexpr (std::is_same_v<D, StringDimensionCoordType>) {
    // NB: poor performance with small domains for sure
    return gen::suchThat(
        gen::map(
            gen::string<std::string>(),
            [](std::string s) {
              StringDimensionCoordType v(s.begin(), s.end());
              return v;
            }),
        [domain](const StringDimensionCoordType& s) {
          return domain.lower_bound <= s && s <= domain.upper_bound;
        });
  } else if constexpr (std::is_signed<D>::value) {
    if (int64_t(domain.upper_bound) < std::numeric_limits<int64_t>::max()) {
      return gen::cast<D>(gen::inRange(
          int64_t(domain.lower_bound), int64_t(domain.upper_bound + 1)));
    } else {
      return gen::inRange(domain.lower_bound, domain.upper_bound);
    }
  } else {
    if (uint64_t(domain.upper_bound) < std::numeric_limits<uint64_t>::max()) {
      return gen::cast<D>(gen::inRange(
          uint64_t(domain.lower_bound), uint64_t(domain.upper_bound + 1)));
    } else {
      return gen::inRange(domain.lower_bound, domain.upper_bound);
    }
  }
}

template <DimensionType D>
Gen<templates::Domain<D>> make_range(const templates::Domain<D>& domain) {
  return gen::apply(
      [](D p1, D p2) { return templates::Domain<D>(p1, p2); },
      make_coordinate<D>(domain),
      make_coordinate<D>(domain));
}

template <>
void show<Domain<int>>(const templates::Domain<int>& domain, std::ostream& os);

template <>
void show<Domain<uint64_t>>(
    const templates::Domain<uint64_t>& domain, std::ostream& os);

template <>
void show<Dimension<tiledb::sm::Datatype::UINT64>>(
    const templates::Dimension<tiledb::sm::Datatype::UINT64>& dimension,
    std::ostream& os);

}  // namespace rc

#endif
