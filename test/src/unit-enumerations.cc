/**
 * @file unit-enumerations.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc.
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
 * Tests the C++ API for enumeration related functions.
 */

#include <sstream>

#include "test/support/tdb_catch.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/array_schema_evolution.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/buffer_list.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/storage_manager/context.h"

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/array_schema_evolution.h"
#include "tiledb/sm/serialization/query.h"
#endif

using namespace tiledb::sm;

#define ENUM_NAME_PTR "an_enumeration"
const static std::string default_enmr_name = ENUM_NAME_PTR;

struct EnumerationFx {
  EnumerationFx();
  ~EnumerationFx();

  template <typename T>
  shared_ptr<const Enumeration> create_enumeration(
      const std::vector<T>& values,
      bool ordered = false,
      Datatype type = static_cast<Datatype>(255),
      std::string enmr_name = default_enmr_name);

  template <typename T>
  void check_enumeration(
      shared_ptr<const Enumeration> enmr,
      const std::string& name,
      const std::vector<T>& values,
      Datatype data_type,
      uint32_t cell_val_num,
      bool ordered);

  template <typename T>
  void check_storage_serialization(const std::vector<T>& values);

  template <typename T>
  void check_storage_deserialization(const std::vector<T>& values);

  storage_size_t calculate_serialized_size(shared_ptr<const Enumeration> enmr);
  WriterTile serialize_to_tile(shared_ptr<const Enumeration> enmr);

  template <typename T>
  std::vector<T> as_vector(shared_ptr<const Enumeration> enmr);

  shared_ptr<ArraySchema> create_schema();

  void create_array();
  shared_ptr<Array> get_array(QueryType type);
  shared_ptr<ArrayDirectory> get_array_directory();
  shared_ptr<ArraySchema> get_array_schema_latest(bool load_enmrs = false);

  // Serialization helpers
  ArraySchema ser_des_array_schema(
      shared_ptr<const ArraySchema> schema,
      bool client_side,
      SerializationType stype);

  shared_ptr<ArraySchemaEvolution> ser_des_array_schema_evolution(
      ArraySchemaEvolution* ase, bool client_side, SerializationType stype);

  void ser_des_query(
      Query* q_in, Query* q_out, bool client_side, SerializationType stype);

  template <typename T>
  bool vec_cmp(std::vector<T> v1, std::vector<T> v2);

  void flatten_buffer_list(BufferList& blist, Buffer& buf);

  void rm_array();

  URI uri_;
  Config cfg_;
  Context ctx_;
  EncryptionKey enc_key_;
};

template <typename T>
QueryCondition create_qc(
    const char* field_name, T condition_value, const QueryConditionOp& op);

/* ********************************* */
/*        Testing Enumeration        */
/* ********************************* */

TEST_CASE_METHOD(
    EnumerationFx,
    "Basic Fixed Size Enumeration Creation",
    "[enumeration][basic][fixed]") {
  std::vector<uint32_t> values = {1, 2, 3, 4, 5};
  auto enmr = create_enumeration(values);
  check_enumeration(
      enmr, default_enmr_name, values, Datatype::UINT32, 1, false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Basic Variable Size Enumeration Creation",
    "[enumeration][basic][fixed]") {
  std::vector<std::string> values = {"foo", "bar", "baz", "bingo", "bango"};
  auto enmr = create_enumeration(values);
  check_enumeration(
      enmr,
      default_enmr_name,
      values,
      Datatype::STRING_ASCII,
      constants::var_num,
      false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Basic Variable Size With Empty Value Enumeration Creation",
    "[enumeration][basic][fixed]") {
  std::vector<std::string> values = {"foo", "bar", "", "bingo", "bango"};
  auto enmr = create_enumeration(values);
  check_enumeration(
      enmr,
      default_enmr_name,
      values,
      Datatype::STRING_ASCII,
      constants::var_num,
      false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation with Ordered",
    "[enumeration][basic][var-size][ordered]") {
  std::vector<std::string> values = {"foo", "bar", "baz", "bingo", "bango"};
  auto enmr = create_enumeration(values, true);
  check_enumeration(
      enmr,
      default_enmr_name,
      values,
      Datatype::STRING_ASCII,
      constants::var_num,
      true);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation with Datatype",
    "[enumeration][basic][var-size][custom-datatype]") {
  std::vector<std::string> values = {"foo", "bar", "baz", "bingo", "bango"};
  auto enmr = create_enumeration(values, false, Datatype::STRING_UTF8);
  check_enumeration(
      enmr,
      default_enmr_name,
      values,
      Datatype::STRING_UTF8,
      constants::var_num,
      false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation with Multi-Cell Val Num",
    "[enumeration][basic][fixed][multi-cell-val-num]") {
  std::vector<int> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  auto enmr = Enumeration::create(
      default_enmr_name,
      Datatype::INT32,
      2,
      false,
      values.data(),
      values.size() * sizeof(int),
      nullptr,
      0);
  check_enumeration(enmr, default_enmr_name, values, Datatype::INT32, 2, false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Invalid empty name - std::string",
    "[enumeration][error][invalid-name]") {
  std::vector<int> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  REQUIRE_THROWS(Enumeration::create(
      std::string(),
      Datatype::INT32,
      2,
      false,
      values.data(),
      values.size() * sizeof(int),
      nullptr,
      0));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Invalid empty name - char*",
    "[enumeration][error][invalid-name]") {
  std::vector<int> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  REQUIRE_THROWS(Enumeration::create(
      "",
      Datatype::INT32,
      2,
      false,
      values.data(),
      values.size() * sizeof(int),
      nullptr,
      0));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Invalid name with slash",
    "[enumeration][error][invalid-name]") {
  std::vector<int> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  REQUIRE_THROWS(Enumeration::create(
      "an/enumeration",
      Datatype::INT32,
      2,
      false,
      values.data(),
      values.size() * sizeof(int),
      nullptr,
      0));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Invalid cell val num",
    "[enumeration][error][invalid-cell-val-num]") {
  std::vector<int> values = {1, 2, 3};
  REQUIRE_THROWS(Enumeration::create(
      default_enmr_name,
      Datatype::INT32,
      0,
      false,
      values.data(),
      values.size() * sizeof(int),
      nullptr,
      0));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - No data pointer",
    "[enumeration][error][data-nullptr]") {
  std::vector<int> values = {1, 2, 3};
  REQUIRE_THROWS(Enumeration::create(
      default_enmr_name,
      Datatype::INT32,
      1,
      false,
      nullptr,
      values.size() * sizeof(int),
      nullptr,
      0));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Zero data size",
    "[enumeration][error][data-zero-size]") {
  std::vector<int> values = {1, 2, 3};
  REQUIRE_THROWS(Enumeration::create(
      default_enmr_name,
      Datatype::INT32,
      1,
      false,
      values.data(),
      0,
      nullptr,
      0));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - No offsets pointer",
    "[enumeration][error][offsets-nullptr]") {
  auto data = "foobarbazbam";
  std::vector<uint64_t> offsets = {0, 3, 6, 9};
  REQUIRE_THROWS(Enumeration::create(
      default_enmr_name,
      Datatype::STRING_ASCII,
      constants::var_num,
      false,
      data,
      strlen(data),
      nullptr,
      offsets.size() * sizeof(uint64_t)));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - No offsets size",
    "[enumeration][error][offsets-zero-size]") {
  auto data = "foobarbazbam";
  std::vector<uint64_t> offsets = {0, 3, 6, 9};
  REQUIRE_THROWS(Enumeration::create(
      default_enmr_name,
      Datatype::STRING_ASCII,
      constants::var_num,
      false,
      data,
      strlen(data),
      offsets.data(),
      0));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Offsets not required, pointer provided",
    "[enumeration][error][offsets-not-required]") {
  std::vector<int> values = {0, 1, 2, 3, 4};
  std::vector<uint64_t> offsets = {0, 3, 6, 9};
  REQUIRE_THROWS(Enumeration::create(
      default_enmr_name,
      Datatype::INT32,
      1,
      false,
      values.data(),
      values.size() * sizeof(int),
      offsets.data(),
      0));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Offsets not required, size provided",
    "[enumeration][error][offsets-not-required]") {
  std::vector<int> values = {0, 1, 2, 3, 4};
  REQUIRE_THROWS(Enumeration::create(
      default_enmr_name,
      Datatype::INT32,
      1,
      false,
      values.data(),
      values.size() * sizeof(int),
      nullptr,
      100));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Invalid offsests size provided",
    "[enumeration][error][offsets-invalid-size]") {
  auto data = "foobarbazbam";
  std::vector<uint64_t> offsets = {0, 3, 6, 9};
  // Passing 3 for the offsets size is incorrect because the offsets size has
  // to be a multiple of `sizeof(uint64_t)`
  REQUIRE_THROWS(Enumeration::create(
      default_enmr_name,
      Datatype::STRING_ASCII,
      constants::var_num,
      false,
      data,
      strlen(data),
      offsets.data(),
      3));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Offsets to data beyond provided data size",
    "[enumeration][error][invalid-offset-data]") {
  auto data = "foobarbazbam";
  std::vector<uint64_t> offsets = {0, 3, 6, 100};
  // The last offset is larger than data_size
  REQUIRE_THROWS(Enumeration::create(
      default_enmr_name,
      Datatype::STRING_ASCII,
      constants::var_num,
      false,
      data,
      strlen(data),
      offsets.data(),
      offsets.size() * sizeof(uint64_t)));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Invalid data size",
    "[enumeration][error][invalid-data-size]") {
  std::vector<int> values = {1, 2, 3, 4, 5};
  // Passing 3 for the data size is invalid as its not a multiple of
  // sizeof(int)
  REQUIRE_THROWS(Enumeration::create(
      default_enmr_name,
      Datatype::INT32,
      1,
      false,
      values.data(),
      3,
      nullptr,
      0));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Repeated fixed sized values",
    "[enumeration][error][repeated-values]") {
  std::vector<int> values = {1, 2, 3, 3, 4, 5};
  REQUIRE_THROWS(create_enumeration(values));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Repeated variable sized values",
    "[enumeration][error][repeated-values]") {
  std::vector<std::string> values = {"foo", "bar", "bang", "bar"};
  REQUIRE_THROWS(create_enumeration(values));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Creation Error - Repeated empty variable sized values",
    "[enumeration][error][repeated-values]") {
  std::vector<std::string> values = {"foo", "", "bang", ""};
  REQUIRE_THROWS(create_enumeration(values));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Serialization - Fixed Size",
    "[enumeration][serialization][fixed-size]") {
  std::vector<int> values = {1, 2, 3, 4, 5};
  check_storage_serialization(values);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Serialization - Variable Size",
    "[enumeration][serialization][var-size]") {
  std::vector<std::string> values = {"foo", "bar", "baz", "bam", "cap"};
  check_storage_serialization(values);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Deserialization - Fixed Size",
    "[enumeration][deserialization][fixed-size]") {
  std::vector<int> values = {1, 2, 3, 4, 5};
  check_storage_deserialization(values);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Deserialization - Variable Size",
    "[enumeration][deserialization][var-size]") {
  std::vector<std::string> values = {"foo", "bar", "baz", "bam", "cap"};
  check_storage_deserialization(values);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration Deserialization Error - Invalid version",
    "[enumeration][deserialization][error][invalid-version]") {
  std::vector<std::string> values = {"foo", "bar", "baz", "bam", "cap"};
  auto enmr = create_enumeration(values);
  auto tile = serialize_to_tile(enmr);

  REQUIRE(tile.size() > 4);
  auto data = tile.data();
  memset(data, 1, 4);

  Deserializer deserializer(tile.data(), tile.size());
  REQUIRE_THROWS(Enumeration::deserialize(deserializer));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration index_of - Fixed Size",
    "[enumeration][index-of][fixed-size]") {
  std::vector<int> values = {1, 2, 3, 4, 5};
  auto enmr = create_enumeration(values);

  for (uint64_t i = 0; i < values.size(); i++) {
    int tmp = values[i];
    UntypedDatumView udv(&tmp, sizeof(int));
    REQUIRE(enmr->index_of(udv) == i);
  }
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration index_of - Fixed Size Missing",
    "[enumeration][index-of][fixed-size]") {
  std::vector<int> values = {1, 2, 3, 4, 5};
  auto enmr = create_enumeration(values);

  int zero = 0;
  UntypedDatumView udv_zero(&zero, sizeof(int));
  REQUIRE(enmr->index_of(udv_zero) == constants::enumeration_missing_value);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration index_of - Variable Size",
    "[enumeration][index-of][var-size]") {
  std::vector<std::string> values = {"foo", "bar", "baz", "bang", "ohai"};
  auto enmr = create_enumeration(values);

  for (uint64_t i = 0; i < values.size(); i++) {
    UntypedDatumView udv(values[i].data(), values[i].size());
    REQUIRE(enmr->index_of(udv) == i);
  }
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Enumeration index_of - Variable Size Missing",
    "[enumeration][index-of][var-size]") {
  std::vector<std::string> values = {"foo", "bar", "baz", "bang", "ohai"};
  auto enmr = create_enumeration(values);

  UntypedDatumView udv_empty("", 0);
  REQUIRE(enmr->index_of(udv_empty) == constants::enumeration_missing_value);
}

/* ********************************* */
/*        Testing Attribute          */
/* ********************************* */

TEST_CASE_METHOD(
    EnumerationFx,
    "Attribute set_enumeration_name - Error - Empty Name",
    "[enumeration][attribute][error]") {
  auto attr = make_shared<Attribute>(HERE(), "foo", Datatype::INT8);
  REQUIRE_THROWS(attr->set_enumeration_name(""));
}

/* ********************************* */
/*          Testing Array            */
/* ********************************* */

TEST_CASE_METHOD(
    EnumerationFx,
    "Array - Get Enumeration",
    "[enumeration][array][get-enumeration]") {
  create_array();
  auto array = get_array(QueryType::READ);
  auto enmr = array->get_enumeration("test_enmr");
  REQUIRE(enmr != nullptr);

  std::vector<std::string> values = {"ant", "bat", "cat", "dog", "emu"};
  check_enumeration(
      enmr,
      "test_enmr",
      values,
      Datatype::STRING_ASCII,
      constants::var_num,
      false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Array - Get Enumeration Repeated",
    "[enumeration][array][get-enumeration]") {
  create_array();
  auto array = get_array(QueryType::READ);
  auto enmr1 = array->get_enumeration("test_enmr");
  auto enmr2 = array->get_enumeration("test_enmr");
  REQUIRE(enmr1 == enmr2);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Array - Get Enumeration Error - REMOTE NOT YET SUPPORTED",
    "[enumeration][array][error][get-remote]") {
  std::string uri_str = "tiledb://namespace/array_name";
  auto array = make_shared<Array>(HERE(), URI(uri_str), ctx_.storage_manager());
  REQUIRE_THROWS(array->get_enumeration("something_here"));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Array - Get Enumeration Error - Not Open",
    "[enumeration][array][error][not-open]") {
  auto array = make_shared<Array>(HERE(), uri_, ctx_.storage_manager());
  REQUIRE_THROWS(array->get_enumeration("foo"));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Array - Get Non-existent Enumeration",
    "[enumeration][array][unknown-enumeration]") {
  create_array();
  auto array = get_array(QueryType::READ);
  REQUIRE_THROWS(array->get_enumeration("foo"));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Array - Load All Enumerations - Latest Only",
    "[enumeration][array][load-all-enumerations][latest-only]") {
  create_array();
  auto array = get_array(QueryType::READ);
  auto schema = array->array_schema_latest_ptr();
  REQUIRE(schema->is_enumeration_loaded("test_enmr") == false);
  CHECK_NOTHROW(array->load_all_enumerations());
  REQUIRE(schema->is_enumeration_loaded("test_enmr") == true);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Array - Load All Enumerations - Repeated",
    "[enumeration][array][load-all-enumerations][repeated]") {
  create_array();
  auto array = get_array(QueryType::READ);
  auto schema = array->array_schema_latest_ptr();

  REQUIRE(schema->is_enumeration_loaded("test_enmr") == false);

  CHECK_NOTHROW(array->load_all_enumerations());
  REQUIRE(schema->is_enumeration_loaded("test_enmr") == true);

  CHECK_NOTHROW(array->load_all_enumerations());
  REQUIRE(schema->is_enumeration_loaded("test_enmr") == true);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Array - Load All Enumerations",
    "[enumeration][array][load-all-enumerations]") {
  create_array();
  auto array = get_array(QueryType::READ);
  auto schema = array->array_schema_latest_ptr();
  REQUIRE(schema->is_enumeration_loaded("test_enmr") == false);
  CHECK_NOTHROW(array->load_all_enumerations(false));
  REQUIRE(schema->is_enumeration_loaded("test_enmr") == true);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Array - Load All Enumerations Error - REMOTE NOT YET SUPPORTED",
    "[enumeration][array][error][get-remote]") {
  std::string uri_str = "tiledb://namespace/array_name";
  auto array = make_shared<Array>(HERE(), URI(uri_str), ctx_.storage_manager());
  REQUIRE_THROWS(array->load_all_enumerations());
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Array - Load All Enumerations Error - Not Open",
    "[enumeration][array][error][not-open]") {
  auto array = make_shared<Array>(HERE(), uri_, ctx_.storage_manager());
  REQUIRE_THROWS(array->load_all_enumerations());
}

/* ********************************* */
/*     Testing ArrayDirectory        */
/* ********************************* */

TEST_CASE_METHOD(
    EnumerationFx,
    "ArrayDirectory - Load Enumeration",
    "[enumeration][array-directory][load-enumeration]") {
  create_array();

  auto schema = get_array_schema_latest();
  auto ad = get_array_directory();
  auto enmr_name = schema->attribute("attr1")->get_enumeration_name();
  REQUIRE(enmr_name.has_value());

  auto enmr = ad->load_enumeration(schema, enmr_name.value(), enc_key_);
  std::vector<std::string> values = {"ant", "bat", "cat", "dog", "emu"};
  check_enumeration(
      enmr,
      "test_enmr",
      values,
      Datatype::STRING_ASCII,
      constants::var_num,
      false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArrayDirectory - Load Enumeration Error",
    "[enumeration][array-directory][error]") {
  create_array();

  auto schema = get_array_schema_latest();
  auto ad = get_array_directory();

  // Check that this function throws an exception when attempting to load
  // an unknown enumeration
  REQUIRE_THROWS(ad->load_enumeration(schema, "not_an_enumeration", enc_key_));
}

/* ********************************* */
/*       Testing ArraySchema         */
/* ********************************* */

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Add Enumeration - Enumeration nullptr Error",
    "[enumeration][array-schema][error]") {
  auto schema = make_shared<ArraySchema>(HERE());
  REQUIRE_THROWS(schema->add_enumeration(nullptr));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Add Basic Enumeration",
    "[enumeration][array-schema][basic]") {
  auto schema = make_shared<ArraySchema>(HERE());

  std::vector<int> values = {1, 2, 3, 4, 5};
  auto enmr = create_enumeration(values);
  schema->add_enumeration(enmr);

  auto attr = make_shared<Attribute>(HERE(), "foo", Datatype::INT8);
  attr->set_enumeration_name(enmr->name());
  CHECK_NOTHROW(schema->add_attribute(attr));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Get Enumeration",
    "[enumeration][array-schema][get]") {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::DENSE);

  std::vector<int> values = {1, 2, 3, 4, 5};
  auto enmr1 = create_enumeration(values);
  schema->add_enumeration(enmr1);

  auto enmr2 = schema->get_enumeration(default_enmr_name);
  check_enumeration(enmr2, enmr1->name(), values, Datatype::INT32, 1, false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Get Missing Enumeration Error",
    "[enumeration][array-schema][error]") {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::SPARSE);
  REQUIRE_THROWS(schema->get_enumeration("not_an_enumeration"));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Add Enumeration with Existing Enumeration of same Name",
    "[enumeration][array-schema][eror]") {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::SPARSE);
  std::vector<int> values = {1, 2, 3, 4, 5};
  auto enmr = create_enumeration(values);

  schema->add_enumeration(enmr);
  REQUIRE_THROWS(schema->add_enumeration(enmr));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Add Attribute with Missing Enumeration Error",
    "[enumeration][array-schema][eror]") {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::SPARSE);
  auto attr = make_shared<Attribute>(HERE(), "an_attr", Datatype::INT32);
  attr->set_enumeration_name("not_an_enumeration");
  REQUIRE(!schema->add_attribute(attr).ok());
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Get All Enumeration Names Empty",
    "[enumeration][array-schema][get-all][empty]") {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::DENSE);
  auto enmr_names = schema->get_enumeration_names();
  REQUIRE(enmr_names.size() == 0);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Get All Enumeration Names",
    "[enumeration][array-schema][get-all]") {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::DENSE);

  std::vector<float> values = {1.0f, 1.1f, 1.2f, 1.3f, 1.4f};
  auto enmr1 = create_enumeration(values);
  schema->add_enumeration(enmr1);

  auto enmr_names = schema->get_enumeration_names();
  REQUIRE(enmr_names.size() == 1);

  auto enmr2 = schema->get_enumeration(enmr_names[0]);
  REQUIRE(enmr2 == enmr1);
  check_enumeration(
      enmr2, default_enmr_name, values, Datatype::FLOAT32, 1, false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Attribute with Invalid Datatype",
    "[enumeration][array-schema][error][bad-attr-datatype]") {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::DENSE);

  std::vector<int> values = {1, 2, 3, 4, 5};
  auto enmr = create_enumeration(values);
  schema->add_enumeration(enmr);

  auto attr = make_shared<Attribute>(HERE(), "ohai", Datatype::FLOAT32);
  attr->set_enumeration_name(default_enmr_name);
  REQUIRE(schema->add_attribute(attr).ok() == false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Attribute with Invalid Cell Val Num",
    "[enumeration][array-schema][error][bad-attr-cell-val-num]") {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::DENSE);

  std::vector<int> values = {1, 2, 3, 4, 5};
  auto enmr = create_enumeration(values);
  schema->add_enumeration(enmr);

  auto attr = make_shared<Attribute>(
      HERE(), "ohai", Datatype::INT32, 2, DataOrder::UNORDERED_DATA);
  attr->set_enumeration_name(default_enmr_name);
  REQUIRE(schema->add_attribute(attr).ok() == false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Store nullptr Enumeration Error",
    "[enumeration][array-schema][error][store-nullptr-enumeration]") {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::DENSE);
  REQUIRE_THROWS(schema->store_enumeration(nullptr));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Store Enumeration Error",
    "[enumeration][array-schema][error][store-unknown-enumeration]") {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::DENSE);
  std::vector<int> values = {1, 2, 3, 4, 5};
  auto enmr =
      create_enumeration(values, false, Datatype::INT32, "unknown_enmr");
  REQUIRE_THROWS(schema->store_enumeration(enmr));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Store Enumeration Error - Already Loaded",
    "[enumeration][array-schema][error][store-loaded-enumeration]") {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::DENSE);

  std::vector<uint32_t> values = {0, 1, 2, 100000000};
  auto enmr = create_enumeration(values);
  schema->add_enumeration(enmr);

  // This is an error because there's already an enumeration stored in the
  // ArraySchema::enumeration_map_. When deserializing schemas from disk the
  // entries in ArraySchema::enumeration_map_ are (std::string, nullptr).
  REQUIRE_THROWS(schema->store_enumeration(enmr));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Attribute Get Enumeration Name From Attribute",
    "[enumeration][array-schema][has-enumeration]") {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::SPARSE);

  std::vector<std::string> values = {"a", "spot", "of", "tea", "perhaps?"};
  auto enmr = create_enumeration(values);
  schema->add_enumeration(enmr);

  REQUIRE(schema->get_enumeration(enmr->name()) == enmr);
  REQUIRE(schema->get_enumeration_path_name(enmr->name()) == enmr->path_name());

  auto attr = make_shared<Attribute>(HERE(), "ohai", Datatype::INT64);
  attr->set_enumeration_name(default_enmr_name);
  throw_if_not_ok(schema->add_attribute(attr));

  REQUIRE(schema->attribute("ohai")->get_enumeration_name().has_value());
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Schema Copy Constructor",
    "[enumeration][array-schema][copy-ctor]") {
  auto schema = create_schema();

  // Check that the schema is valid and that we can copy it using the
  // copy constructor.
  CHECK_NOTHROW(schema->check());
  CHECK_NOTHROW(make_shared<ArraySchema>(HERE(), *(schema.get())));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Mismatched path name error",
    "[enumeration][array-schema][error]") {
  create_array();
  auto schema = get_array_schema_latest();

  // Creating a new Enumeration will give it a different UUID path name.
  std::vector<int> values = {1, 2, 3, 4, 5};
  auto enmr = create_enumeration(values, false, Datatype::INT32, "test_enmr");

  // Storing an enumeration with a known name but different path is an error
  REQUIRE_THROWS(schema->store_enumeration(enmr));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Drop Enumeration - Empty Name",
    "[enumeration][array-schema][error]") {
  create_array();
  auto schema = get_array_schema_latest();
  REQUIRE_THROWS(schema->drop_enumeration(""));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchema - Drop Enumeration - Unknown Enumeration Name",
    "[enumeration][array-schema][error]") {
  create_array();
  auto schema = get_array_schema_latest();
  REQUIRE_THROWS(schema->drop_enumeration("not_an_enumeration"));
}

/* ********************************* */
/*   Testing ArraySchemaEvolution    */
/* ********************************* */

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Simple No Enumeration",
    "[enumeration][array-schema-evolution][simple]") {
  create_array();
  auto orig_schema = get_array_schema_latest(true);
  auto ase = make_shared<ArraySchemaEvolution>(HERE());
  auto attr3 = make_shared<Attribute>(HERE(), "attr3", Datatype::UINT32);
  ase->add_attribute(attr3.get());
  CHECK_NOTHROW(ase->evolve_schema(orig_schema));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Simple With Enumeration",
    "[enumeration][array-schema-evolution][simple]") {
  create_array();
  auto orig_schema = get_array_schema_latest();
  auto ase = make_shared<ArraySchemaEvolution>(HERE());

  std::vector<uint64_t> values{0, 1, 2, 3, 4, 1000};
  auto enmr = create_enumeration(values);
  ase->add_enumeration(enmr);

  auto attr3 = make_shared<Attribute>(HERE(), "attr3", Datatype::UINT32);
  attr3->set_enumeration_name(default_enmr_name);
  ase->add_attribute(attr3.get());

  ase->drop_attribute("attr2");

  CHECK_NOTHROW(ase->evolve_schema(orig_schema));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Drop Attribute After Add",
    "[enumeration][array-schema-evolution][drop-add]") {
  create_array();
  auto orig_schema = get_array_schema_latest();
  auto ase = make_shared<ArraySchemaEvolution>(HERE());

  std::vector<uint64_t> values{0, 1, 2, 3, 4, 1000};
  auto enmr = create_enumeration(values);
  ase->add_enumeration(enmr);

  auto attr3 = make_shared<Attribute>(HERE(), "attr3", Datatype::UINT32);
  attr3->set_enumeration_name(default_enmr_name);
  ase->add_attribute(attr3.get());

  CHECK_NOTHROW(ase->drop_attribute("attr3"));
  CHECK_NOTHROW(ase->evolve_schema(orig_schema));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Enumeration Attribute Names to Add",
    "[enumeration][array-schema-evolution][simple]") {
  create_array();
  auto orig_schema = get_array_schema_latest();

  auto ase = make_shared<ArraySchemaEvolution>(HERE());

  std::vector<uint64_t> values{0, 1, 2, 3, 4, 1000};
  auto enmr = create_enumeration(values);
  ase->add_enumeration(enmr);

  auto enmr_names = ase->enumeration_names_to_add();
  REQUIRE(enmr_names.size() == 1);
  REQUIRE(enmr_names[0] == enmr->name());

  CHECK_NOTHROW(ase->evolve_schema(orig_schema));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Enumeration to Add",
    "[enumeration][array-schema-evolution][enmr-to-add]") {
  create_array();
  auto orig_schema = get_array_schema_latest();
  auto ase = make_shared<ArraySchemaEvolution>(HERE());

  std::vector<uint64_t> values{0, 1, 2, 3, 4, 1000};
  auto enmr1 = create_enumeration(values);
  ase->add_enumeration(enmr1);

  auto enmr2 = ase->enumeration_to_add(enmr1->name());
  REQUIRE(enmr2 == enmr1);

  CHECK_NOTHROW(ase->evolve_schema(orig_schema));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Drop Enumeration",
    "[enumeration][array-schema-evolution][enmr-to-add]") {
  create_array();
  auto orig_schema = get_array_schema_latest();
  auto ase1 = make_shared<ArraySchemaEvolution>(HERE());

  std::vector<uint64_t> values{0, 1, 2, 3, 4, 1000};
  auto enmr1 = create_enumeration(values, false, Datatype::UINT64, "enmr");
  ase1->add_enumeration(enmr1);

  auto new_schema = ase1->evolve_schema(orig_schema);

  auto ase2 = make_shared<ArraySchemaEvolution>(HERE());
  ase2->drop_enumeration("enmr");

  CHECK_NOTHROW(ase2->evolve_schema(new_schema));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Drop Enumeration",
    "[enumeration][array-schema-evolution][enmr-to-drop]") {
  auto ase = make_shared<ArraySchemaEvolution>(HERE());
  CHECK_NOTHROW(ase->drop_enumeration("test_enmr"));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Drop Enumeration Repeated",
    "[enumeration][array-schema-evolution][enmr-to-drop-repeated]") {
  auto ase = make_shared<ArraySchemaEvolution>(HERE());
  CHECK_NOTHROW(ase->drop_enumeration("test_enmr"));
  CHECK_NOTHROW(ase->drop_enumeration("test_enmr"));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Drop Enumeration After Add",
    "[enumeration][array-schema-evolution][enmr-add-drop]") {
  auto ase = make_shared<ArraySchemaEvolution>(HERE());

  std::vector<uint64_t> values{0, 1, 2, 3, 4, 1000};
  auto enmr = create_enumeration(values, false, Datatype::UINT64, "enmr");
  ase->add_enumeration(enmr);
  REQUIRE(ase->enumeration_names_to_add().size() == 1);
  CHECK_NOTHROW(ase->drop_enumeration("enmr"));
  REQUIRE(ase->enumeration_names_to_add().size() == 0);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Enumeration to Add - nullptr",
    "[enumeration][array-schema-evolution][enmr-nullptr]") {
  create_array();
  auto ase = make_shared<ArraySchemaEvolution>(HERE());
  REQUIRE_THROWS(ase->add_enumeration(nullptr));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Enumeration to Add - Already Added",
    "[enumeration][array-schema-evolution][enmr-already-added]") {
  create_array();
  auto ase = make_shared<ArraySchemaEvolution>(HERE());

  std::vector<uint64_t> values{0, 1, 2, 3, 4, 1000};
  auto enmr1 = create_enumeration(values, false, Datatype::UINT64, "enmr");
  auto enmr2 = create_enumeration(values, false, Datatype::UINT64, "enmr");
  ase->add_enumeration(enmr1);
  REQUIRE_THROWS(ase->add_enumeration(enmr2));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Enumeration to Add - Missing Name",
    "[enumeration][array-schema-evolution][missing-name]") {
  create_array();
  auto ase = make_shared<ArraySchemaEvolution>(HERE());
  REQUIRE(ase->enumeration_to_add("foo") == nullptr);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Drop enumeration while still in use.",
    "[enumeration][array-schema-evolution][enmr-still-in-use]") {
  create_array();
  auto orig_schema = get_array_schema_latest();
  auto ase = make_shared<ArraySchemaEvolution>(HERE());
  ase->drop_enumeration("test_enmr");

  REQUIRE_THROWS(ase->evolve_schema(orig_schema));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Enumeration not Loaded",
    "[enumeration][array-schema-evolution][enmr-not-loaded]") {
  create_array();

  auto attr3 = make_shared<Attribute>(HERE(), "attr3", Datatype::UINT32);
  attr3->set_enumeration_name("test_enmr");

  auto ase = make_shared<ArraySchemaEvolution>(HERE());
  ase->add_attribute(attr3.get());

  auto orig_schema = get_array_schema_latest();
  REQUIRE_THROWS(ase->evolve_schema(orig_schema));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Enumeration to large for signed attribute type",
    "[enumeration][array-schema-evolution][enmr-to-large]") {
  create_array();

  std::vector<int> values;
  values.resize(129);
  std::iota(values.begin(), values.end(), 0);
  auto enmr = create_enumeration(values, false, Datatype::INT32, "big_enmr");

  auto attr3 = make_shared<Attribute>(HERE(), "attr3", Datatype::INT8);
  attr3->set_enumeration_name("big_enmr");

  auto ase = make_shared<ArraySchemaEvolution>(HERE());
  ase->add_enumeration(enmr);
  ase->add_attribute(attr3.get());

  auto orig_schema = get_array_schema_latest();
  REQUIRE_THROWS(ase->evolve_schema(orig_schema));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "ArraySchemaEvolution - Enumeration to large for unsigned attribute type",
    "[enumeration][array-schema-evolution][enmr-to-large]") {
  create_array();

  std::vector<int> values;
  values.resize(257);
  std::iota(values.begin(), values.end(), 0);
  auto enmr = create_enumeration(values, false, Datatype::INT32, "big_enmr");

  auto attr3 = make_shared<Attribute>(HERE(), "attr3", Datatype::UINT8);
  attr3->set_enumeration_name("big_enmr");

  auto ase = make_shared<ArraySchemaEvolution>(HERE());
  ase->add_enumeration(enmr);
  ase->add_attribute(attr3.get());

  auto orig_schema = get_array_schema_latest();
  REQUIRE_THROWS(ase->evolve_schema(orig_schema));
}

/* ********************************* */
/*     Testing QueryCondition        */
/* ********************************* */

TEST_CASE_METHOD(
    EnumerationFx,
    "QueryCondition - Rewrite Enumeration Value",
    "[enumeration][query-condition][rewrite-enumeration-value]") {
  create_array();
  auto array = get_array(QueryType::READ);
  auto schema = array->array_schema_latest_ptr();

  // This is normally invoked by the query class when not being tested. It's
  // required here so that the enumeration's data is loaded.
  array->get_enumeration("test_enmr");

  // Assert that the enumerations were loaded
  auto enmr_names = schema->get_loaded_enumeration_names();
  REQUIRE(enmr_names.size() == 1);
  REQUIRE(enmr_names[0] == "test_enmr");

  // Create two copies of the same query condition for assertions
  auto qc1 = create_qc("attr1", std::string("cat"), QueryConditionOp::EQ);
  auto qc2 = qc1;

  qc2.rewrite_enumeration_conditions(*(schema.get()));

  // Assert that the rewritten tree matches in the right places while also
  // different to verify the assertion of having been rewritten.
  auto& tree1 = qc1.ast();
  auto& tree2 = qc2.ast();

  REQUIRE(tree1->is_expr() == false);
  REQUIRE(tree1->get_field_name() == "attr1");

  REQUIRE(tree2->is_expr() == tree1->is_expr());
  REQUIRE(tree2->get_field_name() == tree1->get_field_name());

  auto udv1 = tree1->get_condition_value_view();
  auto udv2 = tree2->get_condition_value_view();
  REQUIRE(udv2.size() != udv1.size());
  REQUIRE(udv2.content() != udv1.content());
  REQUIRE(udv2.value_as<int>() == 2);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "QueryCondition - Skip enumeration rewrite",
    "[enumeration][query-condition][skip-rewrite]") {
  create_array();
  auto schema = get_array_schema_latest();

  // Almost exactly the same test as before, except this time we call
  // `set_use_enumeration(false)` before rewriting and assert that the
  // resulting rewritten query tree matches exactly since no enumeration
  // rewriting has taken place.
  auto qc1 = create_qc("attr1", (int)2, QueryConditionOp::EQ);
  qc1.set_use_enumeration(false);
  auto qc2 = qc1;

  qc2.rewrite_enumeration_conditions(*(schema.get()));

  auto& tree1 = qc1.ast();
  auto& tree2 = qc2.ast();

  // Check that both trees match exactly
  REQUIRE(tree1->is_expr() == false);
  REQUIRE(tree1->get_field_name() == "attr1");

  REQUIRE(tree2->is_expr() == tree1->is_expr());
  REQUIRE(tree2->get_field_name() == tree1->get_field_name());

  auto udv1 = tree1->get_condition_value_view();
  auto udv2 = tree1->get_condition_value_view();
  REQUIRE(udv2.size() == udv1.size());
  REQUIRE(udv2.content() == udv1.content());
  REQUIRE(udv2.value_as<int>() == 2);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "QueryCondition - Rewrite - No failure on unknown attribute",
    "[enumeration][query-condition]") {
  create_array();
  auto schema = get_array_schema_latest();

  auto qc1 = create_qc("not_an_attr", (int)2, QueryConditionOp::EQ);
  qc1.rewrite_enumeration_conditions(*(schema.get()));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "QueryCondition - Rewrite - Enumeration Not Loaded",
    "[enumeration][query-condition][error]") {
  create_array();
  auto schema = get_array_schema_latest();

  auto qc1 = create_qc("attr1", (int)2, QueryConditionOp::EQ);
  REQUIRE_THROWS(qc1.rewrite_enumeration_conditions(*(schema.get())));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "QueryCondition - Rewrite - Inequality on Unordered Enumeration",
    "[enumeration][query-condition][error]") {
  // If an enumeration isn't marked as ordered, then it should throw an error
  // when attempting to use an inequality operator on the attribute.
  create_array();
  auto array = get_array(QueryType::READ);
  auto schema = get_array_schema_latest();

  // This is normally invoked by the query class when not being tested.
  array->get_enumeration("test_enmr");

  auto qc1 = create_qc("attr1", (int)2, QueryConditionOp::LT);
  REQUIRE_THROWS(qc1.rewrite_enumeration_conditions(*(schema.get())));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "QueryCondition - Rewrite Empty QC - Coverage",
    "[enumeration][query-condition][coverage]") {
  // Check that qc.rewrite_enumeration_conditions doesn't throw on an empty QC
  create_array();
  auto schema = get_array_schema_latest();

  QueryCondition qc;
  CHECK_NOTHROW(qc.rewrite_enumeration_conditions(*(schema.get())));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "QueryCondition - use_enumeration - Check Accessor for Coverage",
    "[enumeration][query-condition][coverage]") {
  auto qc1 = create_qc("attr1", (int)2, QueryConditionOp::LT);
  auto& node = qc1.ast();

  REQUIRE(node->use_enumeration() == true);
  qc1.set_use_enumeration(false);
  REQUIRE(node->use_enumeration() == false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "QueryCondition - set_use_enumeration - Affects Children",
    "[enumeration][query-condition][set_use_enumeration]") {
  // Check that set_use_enumeration is applied to the entire tree if applied
  // to an expression node.
  auto qc1 = create_qc("attr1", (int)2, QueryConditionOp::EQ);
  auto qc2 = create_qc("attr2", 3.0f, QueryConditionOp::LT);
  QueryCondition qc3;
  throw_if_not_ok(qc1.combine(qc2, QueryConditionCombinationOp::AND, &qc3));

  auto& tree1 = qc3.ast();
  for (auto& child : tree1->get_children()) {
    REQUIRE(child->use_enumeration() == true);
  }

  qc3.set_use_enumeration(false);

  auto& tree2 = qc3.ast();
  for (auto& child : tree2->get_children()) {
    REQUIRE(child->use_enumeration() == false);
  }
}

TEST_CASE_METHOD(
    EnumerationFx,
    "QueryCondition - use_enumeration - Error on ASTNodeExpr",
    "[enumeration][query-condition][error]") {
  // Check that an ASTNodeExpr throws correctly when calling
  // `use_enumeration()`.
  auto qc1 = create_qc("attr1", (int)2, QueryConditionOp::EQ);
  auto qc2 = create_qc("attr2", 3.0f, QueryConditionOp::LT);
  QueryCondition qc3;
  throw_if_not_ok(qc1.combine(qc2, QueryConditionCombinationOp::AND, &qc3));
  auto& node = qc3.ast();

  REQUIRE_THROWS(node->use_enumeration());
}

/* ********************************* */
/* Testing Cap'N Proto Serialization */
/* ********************************* */

#ifdef TILEDB_SERIALIZATION

TEST_CASE_METHOD(
    EnumerationFx,
    "Cap'N Proto - Basic New ArraySchema Serialization",
    "[enumeration][capnp][basic][initialized-in-ram") {
  auto client_side = GENERATE(true, false);
  auto ser_type = GENERATE(SerializationType::CAPNP, SerializationType::JSON);

  auto schema1 = create_schema();
  auto schema2 = ser_des_array_schema(schema1, client_side, ser_type);

  auto all_names1 = schema1->get_enumeration_names();
  auto all_names2 = schema2.get_enumeration_names();
  REQUIRE(vec_cmp(all_names1, all_names2));

  auto loaded_names1 = schema1->get_loaded_enumeration_names();
  auto loaded_names2 = schema2.get_loaded_enumeration_names();
  REQUIRE(vec_cmp(loaded_names1, loaded_names2));

  // This is a new schema in RAM, so the loaded names should be the same
  // as all names.
  REQUIRE(vec_cmp(all_names1, loaded_names1));
  REQUIRE(vec_cmp(all_names2, loaded_names2));
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Cap'N Proto - Basic Exisitng ArraySchema Serialization",
    "[enumeration][capnp][basic][deserialized-from-disk]") {
  create_array();

  auto client_side = GENERATE(true, false);
  auto ser_type = GENERATE(SerializationType::CAPNP, SerializationType::JSON);

  auto schema1 = get_array_schema_latest();
  auto schema2 = ser_des_array_schema(schema1, client_side, ser_type);

  auto all_names1 = schema1->get_enumeration_names();
  auto all_names2 = schema2.get_enumeration_names();
  REQUIRE(vec_cmp(all_names1, all_names2));

  // This schema was deserialized from disk without any enumerations loaded
  // so both of these should be empty.
  auto loaded_names1 = schema1->get_loaded_enumeration_names();
  auto loaded_names2 = schema2.get_loaded_enumeration_names();

  REQUIRE(loaded_names1.empty());
  REQUIRE(loaded_names2.empty());
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Cap'N Proto - Basic ArraySchemaEvolution Serialization",
    "[enumeration][capnp][basic][array-schema-evolution]") {
  auto client_side = GENERATE(true, false);
  auto ser_type = GENERATE(SerializationType::CAPNP, SerializationType::JSON);

  std::vector<int> values1 = {1, 2, 3, 4, 5};
  auto enmr1 = create_enumeration(values1, false, Datatype::INT32, "enmr1");

  std::vector<double> values2 = {1.0, 2.0, 3.0, 4.0, 5.0};
  auto enmr2 = create_enumeration(values2, true, Datatype::FLOAT64, "enmr2");

  auto attr = make_shared<Attribute>(HERE(), "ohai", Datatype::INT64);
  attr->set_enumeration_name("enmr2");

  ArraySchemaEvolution ase1;
  ase1.add_attribute(attr.get());
  ase1.add_enumeration(enmr1);
  ase1.add_enumeration(enmr2);
  ase1.drop_attribute("some_attr");
  ase1.drop_enumeration("an_enumeration");

  auto ase2 = ser_des_array_schema_evolution(&ase1, client_side, ser_type);

  auto enmrs_to_add1 = ase1.enumeration_names_to_add();
  auto enmrs_to_add2 = ase2->enumeration_names_to_add();
  REQUIRE(vec_cmp(enmrs_to_add1, enmrs_to_add2));

  for (auto& name : enmrs_to_add1) {
    REQUIRE(ase1.enumeration_to_add(name) != nullptr);
    REQUIRE(ase2->enumeration_to_add(name) != nullptr);
    REQUIRE(ase1.enumeration_to_add(name) != ase2->enumeration_to_add(name));
  }
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Cap'N Proto - Basic Backwards Compatible Query Serialization",
    "[enumeration][capnp][basic][query-condition-old]") {
  auto client_side = GENERATE(true, false);

  // Query does not support serialization to JSON
  auto ser_type = SerializationType::CAPNP;

  create_array();
  auto array = get_array(QueryType::READ);

  // This is normally invoked by the query class when not being tested.
  array->get_enumeration("test_enmr");

  auto qc1 = create_qc("attr1", (int)2, QueryConditionOp::EQ);
  qc1.set_use_enumeration(false);

  Query q1(ctx_.storage_manager(), array);
  throw_if_not_ok(q1.set_condition(qc1));

  Query q2(ctx_.storage_manager(), array);
  ser_des_query(&q1, &q2, client_side, ser_type);

  auto qc2 = q2.condition();
  REQUIRE(qc2.has_value());

  auto& node = qc2.value().ast();
  REQUIRE(node->use_enumeration() == false);
}

TEST_CASE_METHOD(
    EnumerationFx,
    "Cap'N Proto - Basic New Query Serialization",
    "[enumeration][capnp][basic][query-condition-new]") {
  auto client_side = GENERATE(true, false);

  // Query does not support serialization to JSON
  auto ser_type = SerializationType::CAPNP;

  create_array();
  auto array = get_array(QueryType::READ);

  // This is normally invoked by the query class when not being tested.
  array->get_enumeration("test_enmr");

  auto qc1 = create_qc("attr1", (int)2, QueryConditionOp::EQ);
  qc1.set_use_enumeration(false);

  auto qc2 = create_qc("attr2", std::string("foo"), QueryConditionOp::NE);
  QueryCondition qc3;

  throw_if_not_ok(qc1.combine(qc2, QueryConditionCombinationOp::OR, &qc3));

  Query q1(ctx_.storage_manager(), array);
  throw_if_not_ok(q1.set_condition(qc3));

  Query q2(ctx_.storage_manager(), array);
  ser_des_query(&q1, &q2, client_side, ser_type);

  auto qc4 = q2.condition();
  REQUIRE(qc4.has_value());

  auto& node1 = qc4.value().ast()->get_children()[0];
  auto& node2 = qc4.value().ast()->get_children()[1];
  REQUIRE(node1->use_enumeration() == false);
  REQUIRE(node2->use_enumeration() == true);
}

#endif  // ifdef TILEDB_SERIALIZATIONs

/* ********************************* */
/*      VERIFY SUPPORT CODE          */
/* ********************************* */

TEST_CASE_METHOD(
    EnumerationFx,
    "Check EnumerationFx::vec_cmp",
    "[enumeration][test-code][verify]") {
  std::vector<int> v1 = {1, 2, 3, 4, 5};
  std::vector<int> v2 = {5, 3, 4, 2, 1};

  REQUIRE(vec_cmp(v1, v2));

  std::vector<int> v3 = {};
  REQUIRE(!vec_cmp(v1, v3));

  std::vector<int> v4 = {1, 2};
  REQUIRE(!vec_cmp(v1, v4));

  std::vector<int> v5 = {3, 4, 5, 6, 7};
  REQUIRE(!vec_cmp(v1, v5));
}

/* ********************************* */
/*       TEST SUPPORT CODE           */
/* ********************************* */

struct TypeParams {
  TypeParams(Datatype type, uint32_t cell_val_num)
      : type_(type)
      , cell_val_num_(cell_val_num) {
  }

  template <typename T>
  static TypeParams get(const std::vector<std::basic_string<T>>&) {
    return TypeParams(Datatype::STRING_ASCII, constants::var_num);
  }

  static TypeParams get(const std::vector<int>&) {
    return TypeParams(Datatype::INT32, 1);
  }

  static TypeParams get(const std::vector<uint32_t>&) {
    return TypeParams(Datatype::UINT32, 1);
  }

  static TypeParams get(const std::vector<uint64_t>&) {
    return TypeParams(Datatype::UINT64, 1);
  }

  static TypeParams get(const std::vector<float>&) {
    return TypeParams(Datatype::FLOAT32, 1);
  }

  static TypeParams get(const std::vector<double>&) {
    return TypeParams(Datatype::FLOAT64, 1);
  }

  Datatype type_;
  uint32_t cell_val_num_;
};

EnumerationFx::EnumerationFx()
    : uri_("enumeration_test_array")
    , ctx_(cfg_) {
  rm_array();
  throw_if_not_ok(enc_key_.set_key(EncryptionType::NO_ENCRYPTION, nullptr, 0));
}

EnumerationFx::~EnumerationFx() {
  rm_array();
}

template <typename T>
shared_ptr<const Enumeration> EnumerationFx::create_enumeration(
    const std::vector<T>& values,
    bool ordered,
    Datatype type,
    std::string name) {
  TypeParams tp = TypeParams::get(values);

  if (type != static_cast<Datatype>(255)) {
    tp.type_ = type;
  }

  if constexpr (std::is_pod_v<T>) {
    return Enumeration::create(
        name,
        tp.type_,
        tp.cell_val_num_,
        ordered,
        values.data(),
        values.size() * sizeof(T),
        nullptr,
        0);
  } else {
    uint64_t total_size = 0;
    for (auto v : values) {
      total_size += v.size();
    }

    std::vector<uint8_t> data(total_size, 0);
    std::vector<uint64_t> offsets;
    offsets.reserve(values.size());
    uint64_t curr_offset = 0;

    for (auto v : values) {
      std::memcpy(data.data() + curr_offset, v.data(), v.size());
      offsets.push_back(curr_offset);
      curr_offset += v.size();
    }

    return Enumeration::create(
        name,
        tp.type_,
        tp.cell_val_num_,
        ordered,
        data.data(),
        total_size,
        offsets.data(),
        offsets.size() * sizeof(uint64_t));
  }
}

template <typename T>
void EnumerationFx::check_enumeration(
    shared_ptr<const Enumeration> enmr,
    const std::string& name,
    const std::vector<T>& values,
    Datatype data_type,
    uint32_t cell_val_num,
    bool ordered) {
  REQUIRE(enmr->name() == name);
  REQUIRE(!enmr->path_name().empty());
  REQUIRE(enmr->type() == data_type);
  REQUIRE(enmr->cell_val_num() == cell_val_num);
  REQUIRE(enmr->ordered() == ordered);

  std::vector<T> data = as_vector<T>(enmr);
  REQUIRE(data == values);
}

template <typename T>
void EnumerationFx::check_storage_serialization(const std::vector<T>& values) {
  auto enmr = create_enumeration(values);
  auto tile = serialize_to_tile(enmr);
  REQUIRE(tile.size() == calculate_serialized_size(enmr));
}

template <typename T>
void EnumerationFx::check_storage_deserialization(
    const std::vector<T>& values) {
  auto enmr = create_enumeration(values);
  auto tile = serialize_to_tile(enmr);

  Deserializer deserializer(tile.data(), tile.size());
  auto deserialized = Enumeration::deserialize(deserializer);

  REQUIRE(deserialized->name() == enmr->name());
  REQUIRE(deserialized->path_name().empty() == false);
  REQUIRE(deserialized->type() == enmr->type());
  REQUIRE(deserialized->cell_val_num() == enmr->cell_val_num());
  REQUIRE(deserialized->ordered() == enmr->ordered());
  REQUIRE(deserialized->cell_size() == enmr->cell_size());
  REQUIRE(deserialized->var_size() == enmr->var_size());

  auto orig_dspan = enmr->data();
  auto des_dspan = deserialized->data();
  REQUIRE(des_dspan.size() == orig_dspan.size());
  REQUIRE(memcmp(des_dspan.data(), orig_dspan.data(), orig_dspan.size()) == 0);

  if (enmr->var_size()) {
    auto orig_ospan = enmr->offsets();
    auto des_ospan = deserialized->offsets();
    REQUIRE(orig_ospan.size() == des_ospan.size());
    REQUIRE(
        memcmp(des_ospan.data(), orig_ospan.data(), orig_ospan.size()) == 0);
  }
}

storage_size_t EnumerationFx::calculate_serialized_size(
    shared_ptr<const Enumeration> enmr) {
  // Size is the sum of the following sizes:
  storage_size_t num_bytes = 0;

  // uint32_t - version
  num_bytes += sizeof(uint32_t);

  // uint32_t - name length
  num_bytes += sizeof(uint32_t);

  // name.size() bytes
  num_bytes += enmr->name().size();

  // uint32_t - path_name length;
  num_bytes += sizeof(uint32_t);

  // path_name.size() bytes
  num_bytes += enmr->path_name().size();

  // uint8_t - data type
  num_bytes += sizeof(uint8_t);

  // uint32_t - cell_val_num
  num_bytes += sizeof(uint32_t);

  // bool - ordered
  num_bytes += sizeof(bool);

  // uint64_t - data.size()
  // data.size() bytes
  auto dspan = enmr->data();
  num_bytes += sizeof(uint64_t);
  num_bytes += dspan.size();

  // if var_sized:
  if (enmr->var_size()) {
    auto ospan = enmr->offsets();
    num_bytes += sizeof(uint64_t);
    num_bytes += ospan.size();
  }

  return num_bytes;
}

WriterTile EnumerationFx::serialize_to_tile(
    shared_ptr<const Enumeration> enmr) {
  SizeComputationSerializer size_serializer;
  enmr->serialize(size_serializer);

  WriterTile tile{WriterTile::from_generic(size_serializer.size())};
  Serializer serializer(tile.data(), tile.size());
  enmr->serialize(serializer);

  return tile;
}

template <typename T>
std::vector<T> EnumerationFx::as_vector(shared_ptr<const Enumeration> enmr) {
  std::vector<T> ret;

  if constexpr (std::is_pod_v<T>) {
    auto dspan = enmr->data();

    const T* elems = reinterpret_cast<const T*>(dspan.data());
    size_t count = dspan.size() / sizeof(T);

    ret.reserve(count);
    for (size_t i = 0; i < count; i++) {
      ret.push_back(elems[i]);
    }
  } else {
    auto dspan = enmr->data();
    auto ospan = enmr->offsets();

    auto str_data = reinterpret_cast<const char*>(dspan.data());
    auto elems = reinterpret_cast<const uint64_t*>(ospan.data());
    size_t count = ospan.size() / sizeof(uint64_t);

    ret.reserve(count);
    for (size_t i = 0; i < count; i++) {
      uint64_t len;
      if (i + 1 < count) {
        len = elems[i + 1] - elems[i];
      } else {
        len = dspan.size() - elems[i];
      }
      ret.emplace_back(str_data + elems[i], len);
    }
  }

  return ret;
}

shared_ptr<ArraySchema> EnumerationFx::create_schema() {
  // Create a schema to serialize
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::SPARSE);

  auto dim = make_shared<Dimension>(HERE(), "dim1", Datatype::INT32);
  int range[2] = {0, 1000};
  throw_if_not_ok(dim->set_domain(range));

  auto dom = make_shared<Domain>(HERE());
  throw_if_not_ok(dom->add_dimension(dim));
  throw_if_not_ok(schema->set_domain(dom));

  std::vector<std::string> values = {"ant", "bat", "cat", "dog", "emu"};
  auto enmr =
      create_enumeration(values, false, Datatype::STRING_ASCII, "test_enmr");
  schema->add_enumeration(enmr);

  auto attr1 = make_shared<Attribute>(HERE(), "attr1", Datatype::INT32);
  attr1->set_enumeration_name("test_enmr");
  throw_if_not_ok(schema->add_attribute(attr1));

  auto attr2 = make_shared<Attribute>(HERE(), "attr2", Datatype::STRING_ASCII);
  throw_if_not_ok(schema->add_attribute(attr2));

  return schema;
}

void EnumerationFx::create_array() {
  auto schema = create_schema();
  throw_if_not_ok(ctx_.storage_manager()->array_create(uri_, schema, enc_key_));
}

shared_ptr<Array> EnumerationFx::get_array(QueryType type) {
  auto array = make_shared<Array>(HERE(), uri_, ctx_.storage_manager());
  throw_if_not_ok(array->open(type, EncryptionType::NO_ENCRYPTION, nullptr, 0));
  return array;
}

shared_ptr<ArrayDirectory> EnumerationFx::get_array_directory() {
  return make_shared<ArrayDirectory>(
      HERE(), ctx_.resources(), uri_, 0, UINT64_MAX, ArrayDirectoryMode::READ);
}

shared_ptr<ArraySchema> EnumerationFx::get_array_schema_latest(
    bool load_enmrs) {
  auto array_dir = get_array_directory();
  auto schema = array_dir->load_array_schema_latest(enc_key_);
  if (load_enmrs) {
    array_dir->load_all_enumerations(schema, enc_key_);
  }
  return schema;
}

#ifdef TILEDB_SERIALIZATION

ArraySchema EnumerationFx::ser_des_array_schema(
    shared_ptr<const ArraySchema> schema,
    bool client_side,
    SerializationType stype) {
  Buffer buf;
  throw_if_not_ok(serialization::array_schema_serialize(
      *(schema.get()), stype, &buf, client_side));
  return serialization::array_schema_deserialize(stype, buf);
}

shared_ptr<ArraySchemaEvolution> EnumerationFx::ser_des_array_schema_evolution(
    ArraySchemaEvolution* ase, bool client_side, SerializationType stype) {
  Buffer buf;
  throw_if_not_ok(serialization::array_schema_evolution_serialize(
      ase, stype, &buf, client_side));

  ArraySchemaEvolution* ret;
  throw_if_not_ok(
      serialization::array_schema_evolution_deserialize(&ret, stype, buf));

  return shared_ptr<ArraySchemaEvolution>(ret);
}

void EnumerationFx::ser_des_query(
    Query* q_in, Query* q_out, bool client_side, SerializationType stype) {
  Buffer buf;
  BufferList blist;

  throw_if_not_ok(
      serialization::query_serialize(q_in, stype, client_side, &blist));

  flatten_buffer_list(blist, buf);

  throw_if_not_ok(serialization::query_deserialize(
      buf,
      stype,
      client_side,
      nullptr,
      q_out,
      &(ctx_.resources().compute_tp())));
}

#else  // No TILEDB_SERIALIZATION

ArraySchema EnumerationFx::ser_des_array_schema(
    shared_ptr<const ArraySchema>, bool, SerializationType) {
  throw std::logic_error("Serialization not enabled.");
}

shared_ptr<ArraySchemaEvolution> EnumerationFx::ser_des_array_schema_evolution(
    ArraySchemaEvolution*, bool, SerializationType) {
  throw std::logic_error("Serialization not enabled.");
}

void EnumerationFx::ser_des_query(Query*, Query*, bool, SerializationType) {
  throw std::logic_error("Serialization not enabled.");
}

#endif  // TILEDB_SERIALIZATION

template <typename T>
bool EnumerationFx::vec_cmp(std::vector<T> v1, std::vector<T> v2) {
  std::sort(v1.begin(), v1.end());
  std::sort(v2.begin(), v2.end());

  if (v1.size() != v2.size()) {
    return false;
  }

  for (size_t i = 0; i < v1.size(); i++) {
    if (v1[i] != v2[i]) {
      return false;
    }
  }

  return true;
}

void EnumerationFx::flatten_buffer_list(BufferList& blist, Buffer& buf) {
  const auto nbytes = blist.total_size();
  throw_if_not_ok(buf.realloc(nbytes));

  blist.reset_offset();
  throw_if_not_ok(blist.read(buf.data(), nbytes));
  buf.set_size(nbytes);
}

void EnumerationFx::rm_array() {
  bool is_dir;
  throw_if_not_ok(ctx_.resources().vfs().is_dir(uri_, &is_dir));
  if (is_dir) {
    throw_if_not_ok(ctx_.resources().vfs().remove_dir(uri_));
  }
}

template <class T>
QueryCondition create_qc(
    const char* field_name, T condition_value, const QueryConditionOp& op) {
  QueryCondition ret;

  if constexpr (std::is_pod_v<T>) {
    throw_if_not_ok(ret.init(field_name, &condition_value, sizeof(T), op));
  } else {
    throw_if_not_ok(ret.init(
        field_name, condition_value.data(), condition_value.size(), op));
  }

  return ret;
}
