/**
 * @file tiledb/sm/array_schema/test/array_schema_test_support.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This header defines support functions for defining array schemas and their
 * component objects.
 *
 * The array schema classes have constructors that operate only at a low level,
 * with arguments identical to (or at least very close to) the member variables.
 * As a result manual construction of array schemas is rather verbose when the
 * purpose is to define particular schemas rather than to support persistence to
 * storage or to implement the C API.
 *
 * The overall approach here is to wrap the schema classes to allow more compact
 * construction patterns. Using classes rather than factory functions means that
 * all the details of the underlying schema constructors is hidden during
 * construction.
 *
 * It is a top-level goal of this design that the constructors for an array
 * schema stand alone, that they not require construction of component parts
 * beforehand. These constructors avoid declaration junk, the separate variables
 * that might not be referenced later. Each of the schema components are also
 * constructible separately for testing situations where an entire array schema
 * is not required.
 *
 * These classes are "inside the API"; they don't call through the C API in
 * order to construct their objects. They should not be mixed with objects
 * obtained through the C API. Such tests might work when they're first written
 * but then would immediately become an obstacle to change. They would either
 * lock in an implementation requirement or need to be rewritten to make
 * progress.
 *
 * @section Implementation Notes
 *
 * At present there's only partial support for all the variation allowed in
 * schema. This is intentional. As we gain experience in using these, we'll have
 * a better idea how to incorporate variation without diminishing the goal of
 * compact, standalone schema definition.
 *
 * At present all the array schema classes are defined in this file. Wrappers
 * for components should migrate to their own headers over time.
 */

#ifndef TILEDB_ARRAY_SCHEMA_TEST_SUPPORT_H
#define TILEDB_ARRAY_SCHEMA_TEST_SUPPORT_H

#include <limits>
#include <utility>

#include "test/support/src/mem_helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/current_domain.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/data_order.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/filter/filter_pipeline.h"

using namespace tiledb::sm;

/*
 * Temporarily in this file. Will move elsewhere at some point, possibly with
 * a new namespace.
 */
namespace tiledb::integer_literals {
/*
 * With C++20 these should be changed to `constinit` and throw when out of
 * bounds.
 */
constexpr uint64_t operator""_n64(unsigned long long x) {
  return static_cast<uint64_t>(x);
}
constexpr uint32_t operator""_n32(unsigned long long x) {
  return static_cast<uint32_t>(x);
}
constexpr uint16_t operator""_n16(unsigned long long x) {
  return static_cast<uint16_t>(x);
}
constexpr uint8_t operator""_n8(unsigned long long x) {
  return static_cast<uint8_t>(x);
}

constexpr int64_t operator""_z64(unsigned long long x) {
  return static_cast<int64_t>(x);
}
constexpr int32_t operator""_z32(unsigned long long x) {
  return static_cast<int32_t>(x);
}
constexpr int16_t operator""_z16(unsigned long long x) {
  return static_cast<int16_t>(x);
}
constexpr int8_t operator""_z8(unsigned long long x) {
  return static_cast<int8_t>(x);
}
}  // namespace tiledb::integer_literals
using namespace tiledb::integer_literals;

template <Datatype d>
class DatatypeTestTraits {};

template <>
class DatatypeTestTraits<Datatype::UINT32> {
 public:
  static Range default_range;
  static /*constinit*/ ByteVecValue default_tile_extent;
};
Range DatatypeTestTraits<Datatype::UINT32>::default_range{
    Tag<uint32_t>{}, 0_n32, 99_n32};
ByteVecValue DatatypeTestTraits<Datatype::UINT32>::default_tile_extent{100_n32};

template <>
class DatatypeTestTraits<Datatype::UINT64> {
 public:
  static Range default_range;
  static /*constinit*/ ByteVecValue default_tile_extent;
};
Range DatatypeTestTraits<Datatype::UINT64>::default_range{
    Tag<uint64_t>{}, 0_n64, 99_n64};
ByteVecValue DatatypeTestTraits<Datatype::UINT64>::default_tile_extent{100_n64};

/*consteval*/ Range default_range(Datatype d) {
  switch (d) {
    case Datatype::UINT32:
      return DatatypeTestTraits<Datatype::UINT32>::default_range;
      break;
    case Datatype::UINT64:
      return DatatypeTestTraits<Datatype::UINT64>::default_range;
    default:
      throw std::runtime_error("datatype is invalid or not yet supported");
  }
}

/*consteval*/ ByteVecValue default_tile_extent(Datatype d) {
  switch (d) {
    case Datatype::UINT32:
      return DatatypeTestTraits<Datatype::UINT32>::default_tile_extent;
      break;
    case Datatype::UINT64:
      return DatatypeTestTraits<Datatype::UINT64>::default_tile_extent;
    default:
      throw std::runtime_error("datatype is invalid or not yet supported");
  }
}

/**
 * Filter pipelines are not yet supported. In order to have a standalone schema
 * constructor, though, we'll need a construction wrapper for `class
 * FilterPipeline` and all the individual filters.
 */
class TestFilterPipeline {};

/**
 * Dimension wrapper
 */
class TestDimension {
  shared_ptr<MemoryTracker> memory_tracker_;

  shared_ptr<Dimension> d_;

 public:
  /**
   * The simplest constructor makes a dimension with one element and with absent
   * or empty defaults for everything else about it.
   */
  TestDimension(const std::string& name, Datatype type)
      : memory_tracker_(tiledb::test::create_test_memory_tracker())
      , d_{make_shared<Dimension>(
            HERE(),
            name,
            type,
            1,                    // cell_val_num
            default_range(type),  // domain
            FilterPipeline{},
            default_tile_extent(type),  // fill value
            memory_tracker_)} {};

  /**
   * Accessor copies the underlying object.
   */
  [[nodiscard]] shared_ptr<Dimension> dimension() const {
    return d_;
  }
};

/**
 * Attribute wrapper
 */
class TestAttribute {
  /**
   * The underlying object is stored in the same type as the schema class uses.
   */
  shared_ptr<const Attribute> a_;

 public:
  /**
   * The simplest constructor makes a non-nullable attribute with one element
   * and with absent or empty defaults for everything else about it.
   */
  TestAttribute(const std::string& name, Datatype type)
      : a_{make_shared<Attribute>(HERE(), name, type)} {};

  /**
   * Accessor copies the underlying object.
   */
  [[nodiscard]] shared_ptr<const Attribute> attribute() const {
    return a_;
  }
};

/**
 * Array Schema wrapper
 */
class TestArraySchema {
  shared_ptr<MemoryTracker> memory_tracker_;

  ArraySchema schema_;

  /**
   * Convert an initializer list of `TestDimension` to a vector of `Dimension`
   * as the `Domain` constructor requires.
   */
  template <class... Args>
  static std::vector<shared_ptr<Dimension>> make_dimension_vector(
      std::initializer_list<TestDimension> dimensions) {
    std::vector<shared_ptr<Dimension>> v{};
    std::transform(
        dimensions.begin(),
        dimensions.end(),
        std::back_inserter(v),
        [](const TestDimension& td) { return td.dimension(); });
    return v;
  }

  /**
   * Factory for the `Domain` object required for the `ArraySchema` constructor.
   */
  static shared_ptr<Domain> make_domain(
      std::initializer_list<TestDimension> dimensions,
      Layout cell_order,
      Layout tile_order,
      shared_ptr<MemoryTracker> memory_tracker) {
    return make_shared<Domain>(
        HERE(),
        cell_order,
        make_dimension_vector(dimensions),
        tile_order,
        memory_tracker);
  }

  /**
   * Convert an initializer list of TestAttribute into a vector of Attribute as
   * the `ArraySchema` constructor requires.
   */
  static std::vector<shared_ptr<const Attribute>> make_attributes(
      std::initializer_list<TestAttribute> attributes) {
    std::vector<shared_ptr<const Attribute>> v{};
    std::transform(
        attributes.begin(),
        attributes.end(),
        std::back_inserter(v),
        [](const TestAttribute& ta) { return ta.attribute(); });
    return v;
  }

 public:
  /**
   * Default constructor is deleted.
   */
  TestArraySchema() = delete;

  /**
   * The simplest array constructor has defaults for everything but the list of
   * dimensions and attributes. Note that the domain is not specified
   * separately.
   */
  TestArraySchema(
      std::initializer_list<TestDimension> dimensions,
      std::initializer_list<TestAttribute> attributes,
      ArrayType array_type = ArrayType::DENSE,
      Layout cell_order = Layout::ROW_MAJOR,
      Layout tile_order = Layout::ROW_MAJOR)
      : schema_(
            URI(),
            constants::format_version,
            std::make_pair(0, std::numeric_limits<uint64_t>::max()),
            "",  // name
            array_type,
            false,  // allow duplicates
            make_domain(
                dimensions,
                cell_order,
                tile_order,
                tiledb::test::create_test_memory_tracker()),
            cell_order,
            tile_order,
            10000,  // capacity
            make_attributes(attributes),
            {},  // dimension labels
            {},  // the first enumeration thing
            {},  // the second enumeration thing
            FilterPipeline(),
            FilterPipeline(),
            FilterPipeline(),
            make_shared<CurrentDomain>(
                tiledb::test::create_test_memory_tracker(),
                constants::current_domain_version),
            tiledb::test::create_test_memory_tracker()) {
  }

  /**
   * Accessor for the underlying schema object.
   */
  const ArraySchema& schema() const {
    return schema_;
  };

  /**
   * Accessor for the domain object of the underlying schema.
   */
  const Domain& domain() const {
    return schema_.domain();
  }
};

#endif  // TILEDB_ARRAY_SCHEMA_TEST_SUPPORT_H
