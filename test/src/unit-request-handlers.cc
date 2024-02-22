/**
 * @file unit-request-handlers.cc
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
 * Tests for the C API request handlers.
 */

#ifdef TILEDB_SERIALIZATION

#include "test/support/tdb_catch.h"
#include "tiledb/api/c_api/buffer/buffer_api_internal.h"
#include "tiledb/api/c_api/string/string_api_internal.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/query_plan.h"
#include "tiledb/sm/storage_manager/context.h"

using namespace tiledb::sm;

struct RequestHandlerFx {
  RequestHandlerFx(const std::string array_uri);
  virtual ~RequestHandlerFx();

  virtual shared_ptr<ArraySchema> create_schema() = 0;

  void create_array();
  void delete_array();

  shared_ptr<Array> get_array(QueryType type);

  URI uri_;
  Config cfg_;
  Context ctx_;
  EncryptionKey enc_key_;
};

struct HandleLoadArraySchemaRequestFx : RequestHandlerFx {
  HandleLoadArraySchemaRequestFx()
      : RequestHandlerFx("load_array_schema_handler") {
  }

  virtual shared_ptr<ArraySchema> create_schema() override;
  ArraySchema call_handler(
      serialization::LoadArraySchemaRequest req, SerializationType stype);

  shared_ptr<const Enumeration> create_string_enumeration(
      std::string name, std::vector<std::string>& values);
};

struct HandleQueryPlanRequestFx : RequestHandlerFx {
  HandleQueryPlanRequestFx()
      : RequestHandlerFx("query_plan_handler") {
  }

  virtual shared_ptr<ArraySchema> create_schema() override;
  QueryPlan call_handler(SerializationType stype, Query& query);
};

struct HandleConsolidationPlanRequestFx : RequestHandlerFx {
  HandleConsolidationPlanRequestFx()
      : RequestHandlerFx("consolidation_plan_handler") {
  }

  virtual shared_ptr<ArraySchema> create_schema() override {
    auto schema = make_shared<ArraySchema>(HERE(), ArrayType::SPARSE);
    auto dim = make_shared<Dimension>(HERE(), "dim1", Datatype::INT32);
    int range[2] = {0, 1000};
    throw_if_not_ok(dim->set_domain(range));
    auto dom = make_shared<Domain>(HERE());
    throw_if_not_ok(dom->add_dimension(dim));
    throw_if_not_ok(schema->set_domain(dom));
    return schema;
  };
};

/* ********************************* */
/*   Testing Array Schema Loading    */
/* ********************************* */

TEST_CASE_METHOD(
    HandleLoadArraySchemaRequestFx,
    "tiledb_handle_load_array_schema_request - default request",
    "[request_handler][load_array_schema][default]") {
  auto stype = GENERATE(SerializationType::JSON, SerializationType::CAPNP);

  create_array();
  auto schema =
      call_handler(serialization::LoadArraySchemaRequest(false), stype);
  REQUIRE(schema.has_enumeration("enmr"));
  REQUIRE(schema.get_loaded_enumeration_names().size() == 0);
}

TEST_CASE_METHOD(
    HandleLoadArraySchemaRequestFx,
    "tiledb_handle_load_array_schema_request - load enumerations",
    "[request_handler][load_array_schema][with-enumerations]") {
  auto stype = GENERATE(SerializationType::JSON, SerializationType::CAPNP);

  create_array();
  auto schema =
      call_handler(serialization::LoadArraySchemaRequest(true), stype);
  REQUIRE(schema.has_enumeration("enmr"));
  REQUIRE(schema.get_loaded_enumeration_names().size() == 1);
  REQUIRE(schema.get_loaded_enumeration_names()[0] == "enmr");
  REQUIRE(schema.get_enumeration("enmr") != nullptr);
}

TEST_CASE_METHOD(
    HandleLoadArraySchemaRequestFx,
    "Error Checks: tiledb_handle_load_array_schema_request",
    "[request_handler][load_array_schema][errors]") {
  create_array();

  auto ctx = tiledb::Context();
  auto array = tiledb::Array(ctx, uri_.to_string(), TILEDB_READ);
  auto stype = TILEDB_CAPNP;
  auto req_buf = tiledb_buffer_handle_t::make_handle();
  auto resp_buf = tiledb_buffer_handle_t::make_handle();

  auto rval = tiledb_handle_load_array_schema_request(
      nullptr,
      array.ptr().get(),
      static_cast<tiledb_serialization_type_t>(stype),
      req_buf,
      resp_buf);
  REQUIRE(rval != TILEDB_OK);

  rval = tiledb_handle_load_array_schema_request(
      ctx.ptr().get(),
      nullptr,
      static_cast<tiledb_serialization_type_t>(stype),
      req_buf,
      resp_buf);
  REQUIRE(rval != TILEDB_OK);

  rval = tiledb_handle_load_array_schema_request(
      ctx.ptr().get(),
      array.ptr().get(),
      static_cast<tiledb_serialization_type_t>(stype),
      nullptr,
      resp_buf);
  REQUIRE(rval != TILEDB_OK);

  rval = tiledb_handle_load_array_schema_request(
      ctx.ptr().get(),
      array.ptr().get(),
      static_cast<tiledb_serialization_type_t>(stype),
      req_buf,
      nullptr);
  REQUIRE(rval != TILEDB_OK);
}

/* ******************************************** */
/*       Testing Query Plan serialization       */
/* ******************************************** */

TEST_CASE_METHOD(
    HandleQueryPlanRequestFx,
    "tiledb_handle_query_plan_request - check json",
    "[request_handler][query_plan][full]") {
  auto stype = GENERATE(SerializationType::JSON, SerializationType::CAPNP);

  // Create and open array
  create_array();
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(NULL, &ctx) == TILEDB_OK);
  tiledb_array_t* array = nullptr;
  REQUIRE(tiledb_array_alloc(ctx, uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);
  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR) == TILEDB_OK);
  int32_t dom[] = {1, 2, 1, 2};
  REQUIRE(tiledb_query_set_subarray(ctx, query, &dom) == TILEDB_OK);

  uint64_t size = 1;
  std::vector<int32_t> a1(2);
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "attr1", a1.data(), &size) ==
      TILEDB_OK);
  std::vector<int32_t> a2(2);
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "attr2", a2.data(), &size) ==
      TILEDB_OK);
  std::vector<int64_t> a3(2);
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "attr3", a3.data(), &size) ==
      TILEDB_OK);

  // Use C API to get the query plan
  tiledb_string_handle_t* query_plan = nullptr;
  REQUIRE(tiledb_query_get_plan(ctx, query, &query_plan) == TILEDB_OK);

  // Call handler to get query plan via serialized req/deserialized response
  auto query_plan_ser_deser = call_handler(stype, *query->query_);

  // Compare the two query plans
  REQUIRE(query_plan->view() == query_plan_ser_deser.dump_json());

  // Clean up
  REQUIRE(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);
}

TEST_CASE_METHOD(
    HandleQueryPlanRequestFx,
    "tiledb_handle_query_plan_request - error checks",
    "[request_handler][query_plan][errors]") {
  create_array();

  auto ctx = tiledb::Context();
  auto array = tiledb::Array(ctx, uri_.to_string(), TILEDB_READ);
  auto stype = TILEDB_CAPNP;
  auto req_buf = tiledb_buffer_handle_t::make_handle();
  auto resp_buf = tiledb_buffer_handle_t::make_handle();

  auto rval = tiledb_handle_query_plan_request(
      nullptr,
      array.ptr().get(),
      static_cast<tiledb_serialization_type_t>(stype),
      req_buf,
      resp_buf);
  REQUIRE(rval != TILEDB_OK);

  rval = tiledb_handle_query_plan_request(
      ctx.ptr().get(),
      nullptr,
      static_cast<tiledb_serialization_type_t>(stype),
      req_buf,
      resp_buf);
  REQUIRE(rval != TILEDB_OK);

  rval = tiledb_handle_query_plan_request(
      ctx.ptr().get(),
      array.ptr().get(),
      static_cast<tiledb_serialization_type_t>(stype),
      nullptr,
      resp_buf);
  REQUIRE(rval != TILEDB_OK);

  rval = tiledb_handle_query_plan_request(
      ctx.ptr().get(),
      array.ptr().get(),
      static_cast<tiledb_serialization_type_t>(stype),
      req_buf,
      nullptr);
  REQUIRE(rval != TILEDB_OK);
}

TEST_CASE_METHOD(
    HandleConsolidationPlanRequestFx,
    "tiledb_handle_consolidation_plan_request - error checks",
    "[request_handler][consolidation-plan][errors]") {
  create_array();

  auto ctx = tiledb::Context();
  auto array = tiledb::Array(ctx, uri_.to_string(), TILEDB_READ);
  auto stype = TILEDB_CAPNP;
  auto req_buf = tiledb_buffer_handle_t::make_handle();
  auto resp_buf = tiledb_buffer_handle_t::make_handle();

  auto rval = tiledb_handle_consolidation_plan_request(
      nullptr,
      array.ptr().get(),
      static_cast<tiledb_serialization_type_t>(stype),
      req_buf,
      resp_buf);
  REQUIRE(rval != TILEDB_OK);

  rval = tiledb_handle_consolidation_plan_request(
      ctx.ptr().get(),
      nullptr,
      static_cast<tiledb_serialization_type_t>(stype),
      req_buf,
      resp_buf);
  REQUIRE(rval != TILEDB_OK);

  rval = tiledb_handle_consolidation_plan_request(
      ctx.ptr().get(),
      array.ptr().get(),
      static_cast<tiledb_serialization_type_t>(stype),
      nullptr,
      resp_buf);
  REQUIRE(rval != TILEDB_OK);

  rval = tiledb_handle_consolidation_plan_request(
      ctx.ptr().get(),
      array.ptr().get(),
      static_cast<tiledb_serialization_type_t>(stype),
      req_buf,
      nullptr);
  REQUIRE(rval != TILEDB_OK);
}

/* ********************************* */
/*       Testing Support Code        */
/* ********************************* */

RequestHandlerFx::RequestHandlerFx(const std::string uri)
    : uri_(uri)
    , ctx_(cfg_) {
  delete_array();
  throw_if_not_ok(enc_key_.set_key(EncryptionType::NO_ENCRYPTION, nullptr, 0));
}

RequestHandlerFx::~RequestHandlerFx() {
  delete_array();
}

void RequestHandlerFx::create_array() {
  auto schema = create_schema();
  throw_if_not_ok(ctx_.storage_manager()->array_create(uri_, schema, enc_key_));
}

void RequestHandlerFx::delete_array() {
  bool is_dir;
  throw_if_not_ok(ctx_.resources().vfs().is_dir(uri_, &is_dir));
  if (is_dir) {
    throw_if_not_ok(ctx_.resources().vfs().remove_dir(uri_));
  }
}

shared_ptr<Array> RequestHandlerFx::get_array(QueryType type) {
  auto array = make_shared<Array>(HERE(), uri_, ctx_.storage_manager());
  throw_if_not_ok(array->open(type, EncryptionType::NO_ENCRYPTION, nullptr, 0));
  return array;
}

shared_ptr<const Enumeration>
HandleLoadArraySchemaRequestFx::create_string_enumeration(
    std::string name, std::vector<std::string>& values) {
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
      Datatype::STRING_ASCII,
      constants::var_num,
      false,
      data.data(),
      total_size,
      offsets.data(),
      offsets.size() * sizeof(uint64_t));
}

shared_ptr<ArraySchema> HandleLoadArraySchemaRequestFx::create_schema() {
  // Create a schema to serialize
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::SPARSE);
  auto dim = make_shared<Dimension>(HERE(), "dim1", Datatype::INT32);
  int range[2] = {0, 1000};
  throw_if_not_ok(dim->set_domain(range));

  auto dom = make_shared<Domain>(HERE());
  throw_if_not_ok(dom->add_dimension(dim));
  throw_if_not_ok(schema->set_domain(dom));

  std::vector<std::string> values = {"pig", "cow", "chicken", "dog", "cat"};
  auto enmr = create_string_enumeration("enmr", values);
  schema->add_enumeration(enmr);

  auto attr = make_shared<Attribute>(HERE(), "attr", Datatype::INT32);
  attr->set_enumeration_name("enmr");
  throw_if_not_ok(schema->add_attribute(attr));

  return schema;
}

ArraySchema HandleLoadArraySchemaRequestFx::call_handler(
    serialization::LoadArraySchemaRequest req, SerializationType stype) {
  // If this looks weird, its because we're using the public C++ API to create
  // these objets instead of the internal APIs elsewhere in this test suite.
  // This is because the handlers are C API so accept API handles, not internal
  // objects.
  auto ctx = tiledb::Context();
  auto array = tiledb::Array(ctx, uri_.to_string(), TILEDB_READ);
  auto req_buf = tiledb_buffer_handle_t::make_handle();
  auto resp_buf = tiledb_buffer_handle_t::make_handle();

  serialization::serialize_load_array_schema_request(
      cfg_, req, stype, req_buf->buffer());
  auto rval = tiledb_handle_load_array_schema_request(
      ctx.ptr().get(),
      array.ptr().get(),
      static_cast<tiledb_serialization_type_t>(stype),
      req_buf,
      resp_buf);
  REQUIRE(rval == TILEDB_OK);

  return serialization::deserialize_load_array_schema_response(
      stype, resp_buf->buffer());
}

shared_ptr<ArraySchema> HandleQueryPlanRequestFx::create_schema() {
  auto schema = make_shared<ArraySchema>(HERE(), ArrayType::DENSE);
  schema->set_capacity(10000);
  throw_if_not_ok(schema->set_cell_order(Layout::ROW_MAJOR));
  throw_if_not_ok(schema->set_tile_order(Layout::ROW_MAJOR));
  uint32_t dim_domain[] = {1, 10, 1, 10};

  auto dim1 = make_shared<Dimension>(HERE(), "dim1", Datatype::INT32);
  throw_if_not_ok(dim1->set_domain(&dim_domain[0]));
  auto dim2 = make_shared<Dimension>(HERE(), "dim2", Datatype::INT32);
  throw_if_not_ok(dim2->set_domain(&dim_domain[2]));

  auto dom = make_shared<Domain>(HERE());
  throw_if_not_ok(dom->add_dimension(dim1));
  throw_if_not_ok(dom->add_dimension(dim2));
  throw_if_not_ok(schema->set_domain(dom));

  auto attr1 = make_shared<Attribute>(HERE(), "attr1", Datatype::INT32);
  throw_if_not_ok(schema->add_attribute(attr1));
  auto attr2 = make_shared<Attribute>(HERE(), "attr2", Datatype::INT32);
  throw_if_not_ok(schema->add_attribute(attr2));
  auto attr3 = make_shared<Attribute>(HERE(), "attr3", Datatype::INT64);
  throw_if_not_ok(schema->add_attribute(attr3));

  return schema;
}

QueryPlan HandleQueryPlanRequestFx::call_handler(
    SerializationType stype, Query& query) {
  auto ctx = tiledb::Context();
  auto array = tiledb::Array(ctx, uri_.to_string(), TILEDB_READ);
  auto req_buf = tiledb_buffer_handle_t::make_handle();
  auto resp_buf = tiledb_buffer_handle_t::make_handle();

  serialization::serialize_query_plan_request(
      cfg_, query, stype, req_buf->buffer());
  auto rval = tiledb_handle_query_plan_request(
      ctx.ptr().get(),
      array.ptr().get(),
      static_cast<tiledb_serialization_type_t>(stype),
      req_buf,
      resp_buf);
  REQUIRE(rval == TILEDB_OK);

  auto query_plan = serialization::deserialize_query_plan_response(
      query, stype, resp_buf->buffer());

  tiledb_buffer_handle_t::break_handle(req_buf);
  tiledb_buffer_handle_t::break_handle(resp_buf);

  return query_plan;
}
#endif  // TILEDB_SERIALIZATION
