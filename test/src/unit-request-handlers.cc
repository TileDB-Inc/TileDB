/**
 * @file unit-request-handlers.cc
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
 * Tests for the C API request handlers.
 */

#ifdef TILEDB_SERIALIZATION

#include "test/support/src/helpers.h"
#include "test/support/src/mem_helpers.h"
#include "test/support/tdb_catch.h"
#include "tiledb/api/c_api/buffer/buffer_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/string/string_api_internal.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
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

  shared_ptr<MemoryTracker> memory_tracker_;
  URI uri_;
  Config cfg_;
  Context ctx_;
  EncryptionKey enc_key_;
  shared_ptr<ArraySchema> schema_;
};

struct HandleLoadArraySchemaRequestFx : RequestHandlerFx {
  HandleLoadArraySchemaRequestFx()
      : RequestHandlerFx("load_array_schema_handler") {
  }

  virtual shared_ptr<ArraySchema> create_schema() override;

  std::tuple<
      shared_ptr<ArraySchema>,
      std::unordered_map<std::string, shared_ptr<ArraySchema>>>
  call_handler(
      serialization::LoadArraySchemaRequest req, SerializationType stype);

  shared_ptr<const Enumeration> create_string_enumeration(
      std::string name, std::vector<std::string>& values);

  shared_ptr<ArraySchema> schema_add_attribute(const std::string& attr_name);
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
    auto schema =
        make_shared<ArraySchema>(HERE(), ArrayType::SPARSE, memory_tracker_);
    auto dim = make_shared<Dimension>(
        HERE(), "dim1", Datatype::INT32, memory_tracker_);
    int range[2] = {0, 1000};
    throw_if_not_ok(dim->set_domain(range));
    auto dom = make_shared<Domain>(HERE(), memory_tracker_);
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
    "tiledb_handle_load_array_schema_request - no enumerations",
    "[request_handler][load_array_schema][default]") {
  auto stype = GENERATE(SerializationType::JSON, SerializationType::CAPNP);

  create_array();
  auto schema_response =
      call_handler(serialization::LoadArraySchemaRequest(cfg_), stype);
  auto schema = std::get<0>(schema_response);
  REQUIRE(schema->has_enumeration("enmr"));
  REQUIRE(schema->get_loaded_enumeration_names().size() == 0);
  tiledb::test::schema_equiv(*schema, *schema_);

  // We did not evolve the schema so there should only be one.
  auto all_schemas = std::get<1>(schema_response);
  REQUIRE(all_schemas.size() == 1);
  tiledb::test::schema_equiv(
      *all_schemas.find(schema->name())->second, *schema_);
}

TEST_CASE_METHOD(
    HandleLoadArraySchemaRequestFx,
    "tiledb_handle_load_array_schema_request - load enumerations",
    "[request_handler][load_array_schema][with-enumerations]") {
  auto stype = GENERATE(SerializationType::JSON, SerializationType::CAPNP);

  create_array();
  REQUIRE(cfg_.set("rest.load_enumerations_on_array_open", "true").ok());
  auto schema_response =
      call_handler(serialization::LoadArraySchemaRequest(cfg_), stype);
  auto schema = std::get<0>(schema_response);
  REQUIRE(schema->has_enumeration("enmr"));
  REQUIRE(schema->get_loaded_enumeration_names().size() == 1);
  REQUIRE(schema->get_loaded_enumeration_names()[0] == "enmr");
  REQUIRE(schema->get_enumeration("enmr") != nullptr);
  tiledb::test::schema_equiv(*schema, *schema_);

  // We did not evolve the schema so there should only be one.
  auto all_schemas = std::get<1>(schema_response);
  REQUIRE(all_schemas.size() == 1);
  tiledb::test::schema_equiv(
      *all_schemas.find(schema->name())->second, *schema_);
}

TEST_CASE_METHOD(
    HandleLoadArraySchemaRequestFx,
    "tiledb_handle_load_array_schema_request - multiple schemas",
    "[request_handler][load_array_schema][schema-evolution]") {
  auto stype = GENERATE(SerializationType::JSON, SerializationType::CAPNP);
  std::string load_enums = GENERATE("true", "false");

  create_array();

  std::vector<shared_ptr<ArraySchema>> all_schemas{schema_};
  all_schemas.push_back(schema_add_attribute("b"));
  all_schemas.push_back(schema_add_attribute("c"));
  all_schemas.push_back(schema_add_attribute("d"));

  REQUIRE(cfg_.set("rest.load_enumerations_on_array_open", load_enums).ok());
  auto schema_response =
      call_handler(serialization::LoadArraySchemaRequest(cfg_), stype);
  auto schema = std::get<0>(schema_response);
  if (load_enums == "true") {
    REQUIRE(schema->has_enumeration("enmr"));
    REQUIRE(schema->get_loaded_enumeration_names().size() == 1);
    REQUIRE(schema->get_loaded_enumeration_names()[0] == "enmr");
    REQUIRE(schema->get_enumeration("enmr") != nullptr);
  }
  // The latest schema should be equal to the last applied evolution.
  tiledb::test::schema_equiv(*schema, *all_schemas.back());

  // Validate schemas returned from the request in the order they were created.
  auto r_all_schemas = std::get<1>(schema_response);
  std::map<std::string, shared_ptr<ArraySchema>> resp(
      r_all_schemas.begin(), r_all_schemas.end());
  for (int i = 0; const auto& s : resp) {
    tiledb::test::schema_equiv(*s.second, *all_schemas[i++]);
  }
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

  // Create subarray
  int32_t dom[] = {1, 2, 1, 2};
  tiledb_subarray_t* sub;
  REQUIRE(tiledb_subarray_alloc(ctx, array, &sub) == TILEDB_OK);
  REQUIRE(tiledb_subarray_set_subarray(ctx, sub, dom) == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);
  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR) == TILEDB_OK);
  REQUIRE(tiledb_query_set_subarray_t(ctx, query, sub) == TILEDB_OK);

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
  tiledb_subarray_free(&sub);
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
    : memory_tracker_(tiledb::test::create_test_memory_tracker())
    , uri_(uri)
    , ctx_(cfg_)
    , schema_(make_shared<ArraySchema>(
          ArrayType::DENSE, ctx_.resources().ephemeral_memory_tracker())) {
  delete_array();
  throw_if_not_ok(enc_key_.set_key(EncryptionType::NO_ENCRYPTION, nullptr, 0));
}

RequestHandlerFx::~RequestHandlerFx() {
  delete_array();
}

void RequestHandlerFx::create_array() {
  auto schema = create_schema();
  Array::create(ctx_.resources(), uri_, schema, enc_key_);
}

void RequestHandlerFx::delete_array() {
  bool is_dir;
  throw_if_not_ok(ctx_.resources().vfs().is_dir(uri_, &is_dir));
  if (is_dir) {
    throw_if_not_ok(ctx_.resources().vfs().remove_dir(uri_));
  }
}

shared_ptr<Array> RequestHandlerFx::get_array(QueryType type) {
  auto array = make_shared<Array>(HERE(), ctx_.resources(), uri_);
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
      offsets.size() * sizeof(uint64_t),
      tiledb::test::create_test_memory_tracker());
}

shared_ptr<ArraySchema> HandleLoadArraySchemaRequestFx::schema_add_attribute(
    const std::string& attr_name) {
  tiledb::Context ctx;
  tiledb::ArraySchemaEvolution ase(ctx);
  auto attr = tiledb::Attribute::create<int32_t>(ctx, attr_name);
  ase.add_attribute(attr);
  // Evolve and update the original schema member variable.
  schema_ = ase.ptr()->array_schema_evolution_->evolve_schema(schema_);
  // Apply the schema evolution.
  Array::evolve_array_schema(
      this->ctx_.resources(),
      this->uri_,
      ase.ptr()->array_schema_evolution_,
      this->enc_key_);

  // Return the new evolved schema for validation.
  return schema_;
}

shared_ptr<ArraySchema> HandleLoadArraySchemaRequestFx::create_schema() {
  // Create a schema to serialize
  schema_ =
      make_shared<ArraySchema>(HERE(), ArrayType::SPARSE, memory_tracker_);
  auto dim =
      make_shared<Dimension>(HERE(), "dim1", Datatype::INT32, memory_tracker_);
  int range[2] = {0, 1000};
  throw_if_not_ok(dim->set_domain(range));

  auto dom = make_shared<Domain>(HERE(), memory_tracker_);
  throw_if_not_ok(dom->add_dimension(dim));
  throw_if_not_ok(schema_->set_domain(dom));

  std::vector<std::string> values = {"pig", "cow", "chicken", "dog", "cat"};
  auto enmr = create_string_enumeration("enmr", values);
  schema_->add_enumeration(enmr);

  auto attr = make_shared<Attribute>(HERE(), "attr", Datatype::INT32);
  attr->set_enumeration_name("enmr");
  throw_if_not_ok(schema_->add_attribute(attr));

  return schema_;
}

std::tuple<
    shared_ptr<ArraySchema>,
    std::unordered_map<std::string, shared_ptr<ArraySchema>>>
HandleLoadArraySchemaRequestFx::call_handler(
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
      uri_, stype, resp_buf->buffer(), memory_tracker_);
}

shared_ptr<ArraySchema> HandleQueryPlanRequestFx::create_schema() {
  auto schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker_);
  schema->set_capacity(10000);
  throw_if_not_ok(schema->set_cell_order(Layout::ROW_MAJOR));
  throw_if_not_ok(schema->set_tile_order(Layout::ROW_MAJOR));
  uint32_t dim_domain[] = {1, 10, 1, 10};

  auto dim1 = make_shared<Dimension>(
      HERE(), "dim1", Datatype::INT32, tiledb::test::get_test_memory_tracker());
  throw_if_not_ok(dim1->set_domain(&dim_domain[0]));
  auto dim2 = make_shared<Dimension>(
      HERE(), "dim2", Datatype::INT32, tiledb::test::get_test_memory_tracker());
  throw_if_not_ok(dim2->set_domain(&dim_domain[2]));

  auto dom = make_shared<Domain>(HERE(), memory_tracker_);
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
