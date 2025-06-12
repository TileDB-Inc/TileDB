/**
 * @file unit_current_domain.cc
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
 * Tests the CurrentDomain.
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
#include "tiledb/sm/array_schema/current_domain.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/storage_manager/context.h"

using namespace tiledb::sm;

template <typename T>
class CurrentDomainFx {
 public:
  CurrentDomainFx();
  ~CurrentDomainFx();

  shared_ptr<CurrentDomain> create_current_domain(
      const NDRange& ranges,
      shared_ptr<const ArraySchema> schema,
      shared_ptr<NDRectangle> ndrectangle = nullptr,
      bool empty = false);

  void check_storage_serialization(
      shared_ptr<ArraySchema> schema, const NDRange& ranges);

  storage_size_t calculate_serialized_size(
      shared_ptr<CurrentDomain> current_domain);
  shared_ptr<WriterTile> serialize_to_tile(
      shared_ptr<CurrentDomain> current_domain);

  shared_ptr<ArraySchema> create_schema(bool dense = false);

  shared_ptr<ArraySchema> create_schema_var();

  void create_array(shared_ptr<ArraySchema> schema);
  shared_ptr<Array> get_array(QueryType type);
  shared_ptr<ArrayDirectory> get_array_directory();
  shared_ptr<ArraySchema> get_array_schema_latest();
  void check_current_domains_equal(
      shared_ptr<CurrentDomain> s1, shared_ptr<CurrentDomain> s2);

  void rm_array();

  shared_ptr<MemoryTracker> memory_tracker_;
  URI uri_;
  Config cfg_;
  Context ctx_;
  EncryptionKey enc_key_;
};

template <class T>
CurrentDomainFx<T>::CurrentDomainFx()
    : memory_tracker_(tiledb::test::create_test_memory_tracker())
    , uri_("current_domain_array")
    , ctx_(cfg_) {
  rm_array();
  throw_if_not_ok(enc_key_.set_key(EncryptionType::NO_ENCRYPTION, nullptr, 0));
  memory_tracker_ = tiledb::test::create_test_memory_tracker();
}

template <class T>
CurrentDomainFx<T>::~CurrentDomainFx() {
  rm_array();
}

template <class T>
void CurrentDomainFx<T>::rm_array() {
  bool is_dir;
  throw_if_not_ok(ctx_.resources().vfs().is_dir(uri_, &is_dir));
  if (is_dir) {
    throw_if_not_ok(ctx_.resources().vfs().remove_dir(uri_));
  }
}

template <class T>
shared_ptr<CurrentDomain> CurrentDomainFx<T>::create_current_domain(
    const NDRange& ranges,
    shared_ptr<const ArraySchema> schema,
    shared_ptr<NDRectangle> ndrectangle,
    bool empty) {
  // create current domain
  auto current_domain = make_shared<CurrentDomain>(
      memory_tracker_, constants::current_domain_version);
  if (empty) {
    return current_domain;
  }

  if (ndrectangle == nullptr) {
    // create ndrectangle
    ndrectangle = make_shared<NDRectangle>(
        memory_tracker_, schema->shared_domain(), ranges);
  }

  // set ndrectangle to current domain
  current_domain->set_ndrectangle(ndrectangle);

  return current_domain;
}

template <class T>
void CurrentDomainFx<T>::check_storage_serialization(
    shared_ptr<ArraySchema> schema, const NDRange& ranges) {
  auto current_domain = create_current_domain(ranges, schema);

  auto tile = serialize_to_tile(current_domain);
  REQUIRE(tile->size() == calculate_serialized_size(current_domain));

  Deserializer deserializer(tile->data(), tile->size());
  auto deserialized = CurrentDomain::deserialize(
      deserializer, memory_tracker_, schema->shared_domain());

  check_current_domains_equal(deserialized, current_domain);
}

template <class T>
void CurrentDomainFx<T>::check_current_domains_equal(
    shared_ptr<CurrentDomain> s1, shared_ptr<CurrentDomain> s2) {
  REQUIRE(s1->empty() == s2->empty());
  REQUIRE(s1->type() == s2->type());
  REQUIRE(s1->version() == s2->version());
  REQUIRE(
      s1->ndrectangle()->get_ndranges() == s2->ndrectangle()->get_ndranges());
}

template <class T>
storage_size_t CurrentDomainFx<T>::calculate_serialized_size(
    shared_ptr<CurrentDomain> current_domain) {
  storage_size_t num_bytes = 0;

  // uint32_t - version
  num_bytes += sizeof(uint32_t);

  // bool - empty current domain flag
  num_bytes += sizeof(bool);

  if (current_domain->empty()) {
    return num_bytes;
  }

  // uint8_t - type
  num_bytes += sizeof(uint8_t);

  auto ndrectangle = current_domain->ndrectangle();
  for (auto& range : ndrectangle->get_ndranges()) {
    if (range.var_size()) {
      // Range length
      num_bytes += sizeof(uint64_t);
      // Minimum value length
      num_bytes += sizeof(uint64_t);
    }
    num_bytes += range.size();
  }

  return num_bytes;
}

template <class T>
shared_ptr<WriterTile> CurrentDomainFx<T>::serialize_to_tile(
    shared_ptr<CurrentDomain> current_domain) {
  SizeComputationSerializer size_serializer;
  current_domain->serialize(size_serializer);

  auto tile{WriterTile::from_generic(size_serializer.size(), memory_tracker_)};
  Serializer serializer(tile->data(), tile->size());
  current_domain->serialize(serializer);

  return tile;
}

template <class T>
shared_ptr<ArraySchema> CurrentDomainFx<T>::create_schema(bool dense) {
  auto type = ArrayType::SPARSE;
  if (dense) {
    type = ArrayType::DENSE;
  }
  auto schema = make_shared<ArraySchema>(HERE(), type, memory_tracker_);

  auto dom = make_shared<Domain>(HERE(), memory_tracker_);

  auto dim =
      make_shared<Dimension>(HERE(), "dim1", Datatype::INT32, memory_tracker_);
  int range[2] = {0, 1000};
  dim->set_domain(range);

  auto dim2 =
      make_shared<Dimension>(HERE(), "dim2", Datatype::INT32, memory_tracker_);
  dim2->set_domain(range);

  dom->add_dimension(dim);
  dom->add_dimension(dim2);

  schema->set_domain(dom);

  auto attr1 = make_shared<Attribute>(HERE(), "attr1", Datatype::INT32);
  schema->add_attribute(attr1);

  return schema;
}

template <class T>
shared_ptr<ArraySchema> CurrentDomainFx<T>::create_schema_var() {
  auto schema =
      make_shared<ArraySchema>(HERE(), ArrayType::SPARSE, memory_tracker_);
  auto dom = make_shared<Domain>(HERE(), memory_tracker_);
  auto dim = make_shared<Dimension>(
      HERE(), "dim1", Datatype::STRING_ASCII, memory_tracker_);
  auto dim2 = make_shared<Dimension>(
      HERE(), "dim2", Datatype::STRING_ASCII, memory_tracker_);
  dom->add_dimension(dim);
  dom->add_dimension(dim2);
  schema->set_domain(dom);

  auto attr1 = make_shared<Attribute>(HERE(), "attr1", Datatype::INT32);

  schema->add_attribute(attr1);

  return schema;
}

template <class T>
void CurrentDomainFx<T>::create_array(shared_ptr<ArraySchema> schema) {
  Array::create(ctx_.resources(), uri_, schema, enc_key_);
}

template <class T>
shared_ptr<Array> CurrentDomainFx<T>::get_array(QueryType type) {
  auto array = make_shared<Array>(HERE(), ctx_.resources(), uri_);
  throw_if_not_ok(array->open(type, EncryptionType::NO_ENCRYPTION, nullptr, 0));
  return array;
}

template <class T>
shared_ptr<ArrayDirectory> CurrentDomainFx<T>::get_array_directory() {
  return make_shared<ArrayDirectory>(
      HERE(), ctx_.resources(), uri_, 0, UINT64_MAX, ArrayDirectoryMode::READ);
}

template <class T>
shared_ptr<ArraySchema> CurrentDomainFx<T>::get_array_schema_latest() {
  auto array_dir = get_array_directory();
  return array_dir->load_array_schema_latest(enc_key_, memory_tracker_);
}

TEST_CASE_METHOD(
    CurrentDomainFx<int32_t>,
    "Create Empty CurrentDomain",
    "[current_domain][create][empty]") {
  auto schema = create_schema();
  auto current_domain = create_current_domain(
      schema->shared_domain()->domain(), schema, nullptr, true);
}

using FixedVarTypes = std::tuple<int32_t, std::string>;
TEMPLATE_LIST_TEST_CASE_METHOD(
    CurrentDomainFx,
    "Create CurrentDomain",
    "[current_domain][create]",
    FixedVarTypes) {
  shared_ptr<ArraySchema> schema;
  std::vector<TestType> rdata;
  Range r;
  if constexpr (std::is_same_v<TestType, std::string>) {
    schema = this->create_schema_var();
    rdata = {"ABC", "ZYZ"};
    r = Range(rdata[0], rdata[1]);
  } else {
    schema = this->create_schema();
    rdata = {1, 1000};
    r = Range(rdata.data(), 2 * sizeof(int32_t));
  }
  auto current_domain = this->create_current_domain({r, r}, schema);
}

TEMPLATE_LIST_TEST_CASE_METHOD(
    CurrentDomainFx,
    "Check disk serialization works",
    "[current_domain][serialization]",
    FixedVarTypes) {
  shared_ptr<ArraySchema> schema;
  std::vector<TestType> rdata;
  Range r;
  if constexpr (std::is_same_v<TestType, std::string>) {
    schema = this->create_schema_var();
    rdata = {"ABC", "ZYZ"};
    r = Range(rdata[0], rdata[1]);
  } else {
    schema = this->create_schema();
    rdata = {1, 1000};
    r = Range(rdata.data(), 2 * sizeof(int32_t));
  }

  this->check_storage_serialization(schema, {r, r});
}

TEST_CASE_METHOD(
    CurrentDomainFx<int32_t>,
    "Check array create throws if current domain exceeds array schema domain "
    "bounds",
    "[current_domain][create][out-of-schema-domain]") {
  auto schema = create_schema();

  auto dom = make_shared<Domain>(HERE(), memory_tracker_);
  auto dim =
      make_shared<Dimension>(HERE(), "dim1", Datatype::INT32, memory_tracker_);
  int range[2] = {0, 1001};
  dim->set_domain(range);
  auto dim2 =
      make_shared<Dimension>(HERE(), "dim2", Datatype::INT32, memory_tracker_);
  dim2->set_domain(range);
  dom->add_dimension(dim);
  dom->add_dimension(dim2);

  auto current_domain = create_current_domain(dom->domain(), schema);

  // It's fine if an incorrect current domain doesn't throw here, sanity checks
  // on schema are performed during array creation when we know schema domain
  // can't be changed anymore.
  schema->set_current_domain(current_domain);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "past the boundaries of the array schema domain");

  REQUIRE_THROWS_WITH(create_array(schema), matcher);
}

TEMPLATE_LIST_TEST_CASE_METHOD(
    CurrentDomainFx,
    "Check array create throws if not all dims are set",
    "[current_domain][create][all-dims]",
    FixedVarTypes) {
  shared_ptr<ArraySchema> schema;
  std::vector<TestType> rdata;
  Range r;
  if constexpr (std::is_same_v<TestType, std::string>) {
    schema = this->create_schema_var();
    rdata = {"ABC", "ZYZ"};
    r = Range(rdata[0], rdata[1]);
  } else {
    schema = this->create_schema();
    rdata = {1, 1000};
    r = Range(rdata.data(), 2 * sizeof(int32_t));
  }

  auto current_domain = this->create_current_domain({r}, schema);

  // It's fine if an incorrect current domain doesn't throw here, sanity checks
  // on schema are performed during array creation when we know schema domain
  // can't be changed anymore.
  schema->set_current_domain(current_domain);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "schema have a non-equal number of dimensions");

  REQUIRE_THROWS_WITH(this->create_array(schema), matcher);
}

TEMPLATE_LIST_TEST_CASE_METHOD(
    CurrentDomainFx,
    "Check array create throws if current domain has an empty range",
    "[current_domain][create][no-empty-ranges]",
    FixedVarTypes) {
  shared_ptr<ArraySchema> schema;
  std::vector<TestType> rdata;
  Range r;
  if constexpr (std::is_same_v<TestType, std::string>) {
    schema = this->create_schema_var();
    rdata = {"ABC", "ZYZ"};
    r = Range(rdata[0], rdata[1]);
  } else {
    schema = this->create_schema();
    rdata = {1, 1000};
    r = Range(rdata.data(), 2 * sizeof(int32_t));
  }

  auto ndrectangle =
      make_shared<NDRectangle>(this->memory_tracker_, schema->shared_domain());
  ndrectangle->set_range_for_name(r, "dim1");

  auto current_domain =
      this->create_current_domain({r, r}, schema, ndrectangle);

  // It's fine if an incorrect current domain doesn't throw here, sanity checks
  // on schema are performed during array creation when we know schema domain
  // can't be changed anymore.
  schema->set_current_domain(current_domain);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "no range specified for dimension idx");

  REQUIRE_THROWS_WITH(this->create_array(schema), matcher);
}

TEST_CASE_METHOD(
    CurrentDomainFx<int32_t>,
    "Check NDRectangle verifies bounds for dim indices",
    "[current_domain][ndrectangle][index-bounds]") {
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

TEMPLATE_LIST_TEST_CASE_METHOD(
    CurrentDomainFx,
    "Check writing/reading current domain to/from array end to end",
    "[current_domain][create][end-to-end-current_domain]",
    FixedVarTypes) {
  shared_ptr<ArraySchema> schema;
  std::vector<TestType> rdata;
  Range r;
  if constexpr (std::is_same_v<TestType, std::string>) {
    schema = this->create_schema_var();
    rdata = {"ABC", "ZYZ"};
    r = Range(rdata[0], rdata[1]);
  } else {
    schema = this->create_schema();
    rdata = {10, 59};
    r = Range(rdata.data(), 2 * sizeof(int32_t));
  }

  auto current_domain = this->create_current_domain({r, r}, schema);

  schema->set_current_domain(current_domain);

  this->create_array(schema);

  auto opened_array = this->get_array(QueryType::READ);

  this->check_current_domains_equal(
      current_domain, opened_array->array_schema_latest().get_current_domain());
}

TEMPLATE_LIST_TEST_CASE_METHOD(
    CurrentDomainFx,
    "Schema evolution, evolving to bigger current domain works",
    "[current_domain][evolution][simple]",
    FixedVarTypes) {
  shared_ptr<ArraySchema> schema;
  std::vector<TestType> rdata;
  Range r;
  std::vector<TestType> rdata_expanded;
  Range r_expanded;
  if constexpr (std::is_same_v<TestType, std::string>) {
    schema = this->create_schema_var();
    rdata = {"ABC", "ZYZ"};
    r = Range(rdata[0], rdata[1]);
    rdata_expanded = {"ABB", "ZZZ"};
    r_expanded = Range(rdata_expanded[0], rdata_expanded[1]);
  } else {
    schema = this->create_schema();
    rdata = {1, 50};
    r = Range(rdata.data(), 2 * sizeof(int32_t));
    rdata_expanded = {1, 55};
    r_expanded = Range(rdata_expanded.data(), 2 * sizeof(int32_t));
  }

  auto current_domain = this->create_current_domain({r, r}, schema);
  schema->set_current_domain(current_domain);
  this->create_array(schema);
  auto opened_array = this->get_array(QueryType::READ);

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), this->memory_tracker_);
  auto orig_schema = opened_array->array_schema_latest_ptr();

  auto current_domain_expanded =
      this->create_current_domain({r_expanded, r_expanded}, orig_schema);

  // Check current domain expansion doesn't throw
  ase->expand_current_domain(current_domain_expanded);
  auto evolved_schema = ase->evolve_schema(orig_schema);

  // Persist evolved schema and read it back, check it shows the correct
  // rectangle
  Array::evolve_array_schema(
      this->ctx_.resources(), this->uri_, ase.get(), this->enc_key_);

  // Read it back
  auto new_schema = this->get_array_schema_latest();

  REQUIRE(
      new_schema->get_current_domain()->ndrectangle()->get_ndranges() ==
      NDRange{{r_expanded, r_expanded}});
}

TEMPLATE_LIST_TEST_CASE_METHOD(
    CurrentDomainFx,
    "Schema evolution, contracting current domain throws",
    "[current_domain][evolution][contraction]",
    FixedVarTypes) {
  shared_ptr<ArraySchema> schema;
  std::vector<TestType> rdata;
  Range r;
  std::vector<TestType> rdata_contracted;
  Range r_contracted;
  if constexpr (std::is_same_v<TestType, std::string>) {
    schema = this->create_schema_var();
    rdata = {"ABC", "ZYZ"};
    r = Range(rdata[0], rdata[1]);
    rdata_contracted = {"ABD", "ZZZ"};
    r_contracted = Range(rdata_contracted[0], rdata_contracted[1]);
  } else {
    schema = this->create_schema();
    rdata = {1, 50};
    r = Range(rdata.data(), 2 * sizeof(int32_t));
    rdata_contracted = {1, 45};
    r_contracted = Range(rdata_contracted.data(), 2 * sizeof(int32_t));
  }

  auto current_domain = this->create_current_domain({r, r}, schema);
  schema->set_current_domain(current_domain);

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), this->memory_tracker_);

  auto current_domain_contracted =
      this->create_current_domain({r_contracted, r_contracted}, schema);

  // It's fine if a contracted current domain doesn't throw here, sanity checks
  // are performed when evolution is applied and the schema is available.
  ase->expand_current_domain(current_domain_contracted);

  auto matcher = Catch::Matchers::ContainsSubstring("can only be expanded");
  REQUIRE_THROWS_WITH(ase->evolve_schema(schema), matcher);
}

TEST_CASE_METHOD(
    CurrentDomainFx<int32_t>,
    "Schema evolution, empty current domain evolution not allowed",
    "[current_domain][evolution][empty_new]") {
  auto schema = create_schema();

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), memory_tracker_);

  auto empty_current_domain = create_current_domain({}, schema, nullptr, true);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "the new current domain specified is empty");
  REQUIRE_THROWS_WITH(
      ase->expand_current_domain(empty_current_domain), matcher);
}

TEMPLATE_LIST_TEST_CASE_METHOD(
    CurrentDomainFx,
    "Check you can evolve from an empty current domain schema",
    "[current_domain][evolution][empty_existing]",
    FixedVarTypes) {
  shared_ptr<ArraySchema> schema;
  std::vector<TestType> rdata;
  Range r;
  if constexpr (std::is_same_v<TestType, std::string>) {
    schema = this->create_schema_var();
    rdata = {"ABC", "ZYZ"};
    r = Range(rdata[0], rdata[1]);
  } else {
    schema = this->create_schema();
    rdata = {1, 50};
    r = Range(rdata.data(), 2 * sizeof(int32_t));
  }

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), this->memory_tracker_);

  auto current_domain = this->create_current_domain({r, r}, schema);

  ase->expand_current_domain(current_domain);

  CHECK_NOTHROW(ase->evolve_schema(schema));
}

TEST_CASE_METHOD(
    CurrentDomainFx<int32_t>,
    "Check array evolution throws if new current domain exceeds array schema "
    "domain bounds",
    "[current_domain][evolution][out-of-schema-domain]") {
  auto schema = create_schema();

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), memory_tracker_);

  std::vector<int32_t> rdata = {1, 1001};
  auto r = Range(rdata.data(), 2 * sizeof(int32_t));
  auto current_domain = create_current_domain({r, r}, schema);

  ase->expand_current_domain(current_domain);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "past the boundaries of the array schema domain");
  REQUIRE_THROWS_WITH(ase->evolve_schema(schema), matcher);
}

TEMPLATE_LIST_TEST_CASE_METHOD(
    CurrentDomainFx,
    "Check array evolution throws if not all dims are set for the new "
    "current domain",
    "[current_domain][evolution][all-dims]",
    FixedVarTypes) {
  shared_ptr<ArraySchema> schema;
  std::vector<TestType> rdata;
  Range r;
  if constexpr (std::is_same_v<TestType, std::string>) {
    schema = this->create_schema_var();
    rdata = {"ABC", "ZYZ"};
    r = Range(rdata[0], rdata[1]);
  } else {
    schema = this->create_schema();
    rdata = {1, 50};
    r = Range(rdata.data(), 2 * sizeof(int32_t));
  }

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), this->memory_tracker_);

  auto current_domain = this->create_current_domain({r}, schema);

  ase->expand_current_domain(current_domain);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "schema have a non-equal number of dimensions");
  REQUIRE_THROWS_WITH(ase->evolve_schema(schema), matcher);
}

TEMPLATE_LIST_TEST_CASE_METHOD(
    CurrentDomainFx,
    "Check array evolution throws if new current domain has an empty range",
    "[current_domain][evolution][no-empty-ranges]",
    FixedVarTypes) {
  shared_ptr<ArraySchema> schema;
  std::vector<TestType> rdata;
  Range r;
  if constexpr (std::is_same_v<TestType, std::string>) {
    schema = this->create_schema_var();
    rdata = {"ABC", "ZYZ"};
    r = Range(rdata[0], rdata[1]);
  } else {
    schema = this->create_schema();
    rdata = {1, 50};
    r = Range(rdata.data(), 2 * sizeof(int32_t));
  }

  auto ase = make_shared<ArraySchemaEvolution>(HERE(), this->memory_tracker_);

  auto ndrectangle =
      make_shared<NDRectangle>(this->memory_tracker_, schema->shared_domain());
  ndrectangle->set_range_for_name(r, "dim1");
  auto current_domain = this->create_current_domain({}, schema, ndrectangle);

  ase->expand_current_domain(current_domain);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "no range specified for dimension idx");
  REQUIRE_THROWS_WITH(ase->evolve_schema(schema), matcher);
}
