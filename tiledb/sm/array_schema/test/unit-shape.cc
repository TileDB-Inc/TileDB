/**
 * @file unit-shape.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB Inc.
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
 * Tests the Shape API.
 */

#include <sstream>

#include "test/support/src/mem_helpers.h"
#include "test/support/tdb_catch.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/array_schema_evolution.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/array_schema/shape.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/storage_manager/context.h"

using namespace tiledb::sm;

struct ShapeFx {
  ShapeFx();
  ~ShapeFx();

  shared_ptr<const Shape> create_shape(
      const NDRange& ranges,
      shared_ptr<const ArraySchema> schema,
      shared_ptr<NDRectangle> ndrectangle = nullptr,
      bool empty = false);

  void check_storage_serialization(
      shared_ptr<ArraySchema> schema, const NDRange& ranges);

  storage_size_t calculate_serialized_size(shared_ptr<const Shape> shape);
  shared_ptr<WriterTile> serialize_to_tile(shared_ptr<const Shape> shape);

  shared_ptr<ArraySchema> create_schema();

  void create_array(shared_ptr<ArraySchema> schema);
  shared_ptr<Array> get_array(QueryType type);
  shared_ptr<ArrayDirectory> get_array_directory();
  shared_ptr<ArraySchema> get_array_schema_latest();
  void check_shapes_equal(
      shared_ptr<const Shape> s1, shared_ptr<const Shape> s2);

  void rm_array();

  shared_ptr<MemoryTracker> memory_tracker_;
  URI uri_;
  Config cfg_;
  Context ctx_;
  EncryptionKey enc_key_;
};

ShapeFx::ShapeFx()
    : memory_tracker_(tiledb::test::create_test_memory_tracker())
    , uri_("shape_array")
    , ctx_(cfg_) {
  rm_array();
  throw_if_not_ok(enc_key_.set_key(EncryptionType::NO_ENCRYPTION, nullptr, 0));
  memory_tracker_ = tiledb::test::create_test_memory_tracker();
}

ShapeFx::~ShapeFx() {
  rm_array();
}

void ShapeFx::rm_array() {
  bool is_dir;
  throw_if_not_ok(ctx_.resources().vfs().is_dir(uri_, &is_dir));
  if (is_dir) {
    throw_if_not_ok(ctx_.resources().vfs().remove_dir(uri_));
  }
}

shared_ptr<const Shape> ShapeFx::create_shape(
    const NDRange& ranges,
    shared_ptr<const ArraySchema> schema,
    shared_ptr<NDRectangle> ndrectangle,
    bool empty) {
  // create shape
  auto shape = make_shared<Shape>(memory_tracker_, constants::shape_version);
  if (empty) {
    return shape;
  }

  if (ndrectangle == nullptr) {
    // create ndrectangle
    ndrectangle = make_shared<NDRectangle>(
        memory_tracker_, schema->shared_domain(), ranges);
  }

  // set ndrectangle to shape
  shape->set_ndrectangle(ndrectangle);

  return shape;
}

void ShapeFx::check_storage_serialization(
    shared_ptr<ArraySchema> schema, const NDRange& ranges) {
  auto shape = create_shape(ranges, schema);

  auto tile = serialize_to_tile(shape);
  REQUIRE(tile->size() == calculate_serialized_size(shape));

  Deserializer deserializer(tile->data(), tile->size());
  auto deserialized = Shape::deserialize(
      deserializer, memory_tracker_, schema->shared_domain());

  check_shapes_equal(deserialized, shape);
}

void ShapeFx::check_shapes_equal(
    shared_ptr<const Shape> s1, shared_ptr<const Shape> s2) {
  REQUIRE(s1->empty() == s2->empty());
  REQUIRE(s1->type() == s2->type());
  REQUIRE(s1->version() == s2->version());
  REQUIRE(
      s1->ndrectangle()->get_ndranges() == s2->ndrectangle()->get_ndranges());
}

storage_size_t ShapeFx::calculate_serialized_size(
    shared_ptr<const Shape> shape) {
  storage_size_t num_bytes = 0;

  // uint32_t - version
  num_bytes += sizeof(uint32_t);

  // bool - empty shape flag
  num_bytes += sizeof(bool);

  if (shape->empty()) {
    return num_bytes;
  }

  // uint8_t - type
  num_bytes += sizeof(uint8_t);

  auto ndrectangle = shape->ndrectangle();
  for (auto& range : ndrectangle->get_ndranges()) {
    num_bytes += range.size();
  }

  return num_bytes;
}

shared_ptr<WriterTile> ShapeFx::serialize_to_tile(
    shared_ptr<const Shape> shape) {
  SizeComputationSerializer size_serializer;
  shape->serialize(size_serializer);

  auto tile{WriterTile::from_generic(size_serializer.size(), memory_tracker_)};
  Serializer serializer(tile->data(), tile->size());
  shape->serialize(serializer);

  return tile;
}

shared_ptr<ArraySchema> ShapeFx::create_schema() {
  auto schema =
      make_shared<ArraySchema>(HERE(), ArrayType::SPARSE, memory_tracker_);

  auto dom = make_shared<Domain>(HERE(), memory_tracker_);

  auto dim =
      make_shared<Dimension>(HERE(), "dim1", Datatype::INT32, memory_tracker_);
  int range[2] = {0, 1000};
  throw_if_not_ok(dim->set_domain(range));

  auto dim2 =
      make_shared<Dimension>(HERE(), "dim2", Datatype::INT32, memory_tracker_);
  throw_if_not_ok(dim2->set_domain(range));

  throw_if_not_ok(dom->add_dimension(dim));
  throw_if_not_ok(dom->add_dimension(dim2));

  throw_if_not_ok(schema->set_domain(dom));

  auto attr1 = make_shared<Attribute>(HERE(), "attr1", Datatype::INT32);
  throw_if_not_ok(schema->add_attribute(attr1));

  return schema;
}

void ShapeFx::create_array(shared_ptr<ArraySchema> schema) {
  throw_if_not_ok(ctx_.storage_manager()->array_create(uri_, schema, enc_key_));
}

shared_ptr<Array> ShapeFx::get_array(QueryType type) {
  auto array = make_shared<Array>(HERE(), uri_, ctx_.storage_manager());
  throw_if_not_ok(array->open(type, EncryptionType::NO_ENCRYPTION, nullptr, 0));
  return array;
}

shared_ptr<ArrayDirectory> ShapeFx::get_array_directory() {
  return make_shared<ArrayDirectory>(
      HERE(), ctx_.resources(), uri_, 0, UINT64_MAX, ArrayDirectoryMode::READ);
}

shared_ptr<ArraySchema> ShapeFx::get_array_schema_latest() {
  auto array_dir = get_array_directory();
  return array_dir->load_array_schema_latest(enc_key_, memory_tracker_);
}

TEST_CASE_METHOD(ShapeFx, "Create Empty Shape", "[shape][create][empty]") {
  auto schema = create_schema();
  auto shape =
      create_shape(schema->shared_domain()->domain(), schema, nullptr, true);
}

TEST_CASE_METHOD(ShapeFx, "Create Shape", "[shape][create]") {
  auto schema = create_schema();
  auto shape = create_shape(schema->shared_domain()->domain(), schema);
}

TEST_CASE_METHOD(
    ShapeFx, "Check disk serialization works", "[shape][serialization]") {
  auto schema = create_schema();
  auto shape = create_shape(schema->shared_domain()->domain(), schema);

  check_storage_serialization(schema, schema->shared_domain()->domain());
}

TEST_CASE_METHOD(
    ShapeFx,
    "Check array create throws if shape exceeds array schema domain bounds",
    "[shape][create][out-of-schema-domain]") {
  auto schema = create_schema();

  auto dom = make_shared<Domain>(HERE(), memory_tracker_);
  auto dim =
      make_shared<Dimension>(HERE(), "dim1", Datatype::INT32, memory_tracker_);
  int range[2] = {0, 1001};
  throw_if_not_ok(dim->set_domain(range));
  auto dim2 =
      make_shared<Dimension>(HERE(), "dim2", Datatype::INT32, memory_tracker_);
  throw_if_not_ok(dim2->set_domain(range));
  throw_if_not_ok(dom->add_dimension(dim));
  throw_if_not_ok(dom->add_dimension(dim2));

  auto shape = create_shape(dom->domain(), schema);

  // It's fine if an incorrect shape doesn't throw here, sanity checks on
  // schema are performed during array creation when we know schema domain
  // can't be changed anymore.
  schema->set_shape(shape);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "past the boundaries of the array schema domain");

  REQUIRE_THROWS_WITH(create_array(schema), matcher);
}

TEST_CASE_METHOD(
    ShapeFx,
    "Check array create throws if no all dims are set",
    "[shape][create][all-dims]") {
  auto schema = create_schema();

  auto dom = make_shared<Domain>(HERE(), memory_tracker_);
  auto dim =
      make_shared<Dimension>(HERE(), "dim1", Datatype::INT32, memory_tracker_);
  int range[2] = {0, 1000};
  throw_if_not_ok(dim->set_domain(range));
  throw_if_not_ok(dom->add_dimension(dim));

  auto shape = create_shape(dom->domain(), schema);

  // It's fine if an incorrect shape doesn't throw here, sanity checks on
  // schema are performed during array creation when we know schema domain
  // can't be changed anymore.
  schema->set_shape(shape);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "schema have a non-equal number of dimensions");

  REQUIRE_THROWS_WITH(create_array(schema), matcher);
}

TEST_CASE_METHOD(
    ShapeFx,
    "Check array create throws if shape has an empty range",
    "[shape][create][no-empty-ranges]") {
  auto schema = create_schema();

  auto dom = make_shared<Domain>(HERE(), memory_tracker_);
  auto dim =
      make_shared<Dimension>(HERE(), "dim1", Datatype::INT32, memory_tracker_);
  int range[2] = {0, 1000};
  throw_if_not_ok(dim->set_domain(range));
  throw_if_not_ok(dom->add_dimension(dim));

  auto ndrectangle =
      make_shared<NDRectangle>(memory_tracker_, schema->shared_domain());
  std::vector<int32_t> rdata = {1, 2};
  ndrectangle->set_range_for_name(
      Range(rdata.data(), 2 * sizeof(int32_t)), "dim1");

  auto shape = create_shape(dom->domain(), schema, ndrectangle);

  // It's fine if an incorrect shape doesn't throw here, sanity checks on
  // schema are performed during array creation when we know schema domain
  // can't be changed anymore.
  schema->set_shape(shape);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "no range specified for dimension idx");

  REQUIRE_THROWS_WITH(create_array(schema), matcher);
}

TEST_CASE_METHOD(
    ShapeFx,
    "Check NDRectangle verifies bounds for dim indices",
    "[shape][ndrectangle][index-bounds]") {
  auto schema = create_schema();

  auto ndrectangle =
      make_shared<NDRectangle>(memory_tracker_, schema->shared_domain());

  auto matcher = Catch::Matchers::ContainsSubstring("out of bounds");
  auto matcher2 = Catch::Matchers::ContainsSubstring("Invalid dimension name");

  std::vector<int32_t> rdata = {1, 2};
  auto r = Range(rdata.data(), 2 * sizeof(int32_t));
  REQUIRE_THROWS_WITH(ndrectangle->set_range(r, 2), matcher);
  REQUIRE_THROWS_WITH(ndrectangle->get_range(2), matcher);
  REQUIRE_THROWS_WITH(
      ndrectangle->set_range_for_name(r, "nonexistent"), matcher2);
  REQUIRE_THROWS_WITH(ndrectangle->get_range_for_name("nonexistent"), matcher2);
}

TEST_CASE_METHOD(
    ShapeFx,
    "Check writing/reading shape to/from array end to end",
    "[shape][create][end-to-end-shape]") {
  auto schema = create_schema();

  auto dom = make_shared<Domain>(HERE(), memory_tracker_);
  auto dim =
      make_shared<Dimension>(HERE(), "dim1", Datatype::INT32, memory_tracker_);
  int range[2] = {10, 50};
  throw_if_not_ok(dim->set_domain(range));
  auto dim2 =
      make_shared<Dimension>(HERE(), "dim2", Datatype::INT32, memory_tracker_);
  throw_if_not_ok(dim2->set_domain(range));
  throw_if_not_ok(dom->add_dimension(dim));
  throw_if_not_ok(dom->add_dimension(dim2));

  auto shape = create_shape(dom->domain(), schema);

  schema->set_shape(shape);

  create_array(schema);

  auto opened_array = get_array(QueryType::READ);

  check_shapes_equal(shape, opened_array->array_schema_latest().get_shape());
}

TEST_CASE_METHOD(
    ShapeFx,
    "Schema evolution, evolving to bigger shape works",
    "[shape][evolution][simple]") {
  auto schema = create_schema();

  std::vector<int32_t> rdata = {1, 50};
  auto r = Range(rdata.data(), 2 * sizeof(int32_t));
  auto shape = create_shape({r, r}, schema);
  schema->set_shape(shape);
  create_array(schema);
  auto opened_array = get_array(QueryType::READ);

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), memory_tracker_);
  auto orig_schema = opened_array->array_schema_latest_ptr();

  std::vector<int32_t> rdata_expanded = {1, 55};
  auto r_expanded = Range(rdata_expanded.data(), 2 * sizeof(int32_t));
  auto shape_expanded = create_shape({r_expanded, r_expanded}, orig_schema);

  // Check shape expansion doesn't throw
  ase->expand_shape(shape_expanded);
  auto evolved_schema = ase->evolve_schema(orig_schema);

  // Persist evolved schema and read it back, check it shows the correct
  // rectangle
  throw_if_not_ok(
      ctx_.storage_manager()->array_evolve_schema(uri_, ase.get(), enc_key_));

  // Read it back
  auto new_schema = get_array_schema_latest();

  REQUIRE(
      new_schema->get_shape()->ndrectangle()->get_ndranges() ==
      NDRange{{r_expanded, r_expanded}});
}

TEST_CASE_METHOD(
    ShapeFx,
    "Schema evolution, contracting shape throws",
    "[shape][evolution][contraction]") {
  auto schema = create_schema();

  std::vector<int32_t> rdata = {1, 50};
  auto r = Range(rdata.data(), 2 * sizeof(int32_t));
  auto shape = create_shape({r, r}, schema);
  schema->set_shape(shape);

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), memory_tracker_);

  std::vector<int32_t> rdata_contracted = {1, 45};
  auto r_contracted = Range(rdata_contracted.data(), 2 * sizeof(int32_t));
  auto shape_contracted = create_shape({r_contracted, r_contracted}, schema);

  // It's fine if a contracted shape doesn't throw here, sanity checks are
  // performed when evolution is applied and the schema is available.
  ase->expand_shape(shape_contracted);

  auto matcher = Catch::Matchers::ContainsSubstring("can only be expanded");
  REQUIRE_THROWS_WITH(ase->evolve_schema(schema), matcher);
}

TEST_CASE_METHOD(
    ShapeFx,
    "Schema evolution, empty shape evolution not allowed",
    "[shape][evolution][empty_new]") {
  auto schema = create_schema();

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), memory_tracker_);

  auto empty_shape = create_shape({}, schema, nullptr, true);

  auto matcher =
      Catch::Matchers::ContainsSubstring("specified an empty new shape");
  REQUIRE_THROWS_WITH(ase->expand_shape(empty_shape), matcher);
}

TEST_CASE_METHOD(
    ShapeFx,
    "Check you can evolve from an empty shape schema",
    "[shape][evolution][empty_existing]") {
  auto schema = create_schema();

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), memory_tracker_);

  std::vector<int32_t> rdata = {1, 50};
  auto r = Range(rdata.data(), 2 * sizeof(int32_t));
  auto shape = create_shape({r, r}, schema);

  ase->expand_shape(shape);

  CHECK_NOTHROW(ase->evolve_schema(schema));
}

TEST_CASE_METHOD(
    ShapeFx,
    "Check array evolution throws if new shape exceeds array schema domain "
    "bounds",
    "[shape][evolution][out-of-schema-domain]") {
  auto schema = create_schema();

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), memory_tracker_);

  std::vector<int32_t> rdata = {1, 1001};
  auto r = Range(rdata.data(), 2 * sizeof(int32_t));
  auto shape = create_shape({r, r}, schema);

  ase->expand_shape(shape);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "past the boundaries of the array schema domain");
  REQUIRE_THROWS_WITH(ase->evolve_schema(schema), matcher);
}

TEST_CASE_METHOD(
    ShapeFx,
    "Check array evolution throws if not all dims are set for the new shape",
    "[shape][evolution][all-dims]") {
  auto schema = create_schema();

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), memory_tracker_);

  std::vector<int32_t> rdata = {1, 50};
  auto r = Range(rdata.data(), 2 * sizeof(int32_t));
  auto shape = create_shape({r}, schema);

  ase->expand_shape(shape);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "schema have a non-equal number of dimensions");
  REQUIRE_THROWS_WITH(ase->evolve_schema(schema), matcher);
}

TEST_CASE_METHOD(
    ShapeFx,
    "Check array evolution throws if new shape has an empty range",
    "[shape][evolution][no-empty-ranges]") {
  auto schema = create_schema();

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), memory_tracker_);

  auto ndrectangle =
      make_shared<NDRectangle>(memory_tracker_, schema->shared_domain());
  std::vector<int32_t> rdata = {1, 2};
  ndrectangle->set_range_for_name(
      Range(rdata.data(), 2 * sizeof(int32_t)), "dim1");
  auto shape = create_shape({}, schema, ndrectangle);

  ase->expand_shape(shape);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "no range specified for dimension idx");
  REQUIRE_THROWS_WITH(ase->evolve_schema(schema), matcher);
}
