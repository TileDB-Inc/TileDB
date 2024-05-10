#include <climits>

#include <test/support/tdb_catch.h>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

using namespace tiledb;

const std::string ARRAY_URI = "test_hilbert_order";

struct ExtentCheckFailureFx {
  ExtentCheckFailureFx() {
    auto rc = tiledb_ctx_alloc(nullptr, &ctx_);
    CHECK(rc == TILEDB_OK);

    Context ctx;
    auto obj = Object::object(ctx, ARRAY_URI);
    if (obj.type() != Object::Type::Invalid) {
      Object::remove(ctx, ARRAY_URI);
    }
  }

  tiledb_ctx_t* ctx_;

  void create_array();
};

void ExtentCheckFailureFx::create_array() {
  int8_t range[] = {-70, 121};
  tiledb_dimension_t* d1;
  int rc =
      tiledb_dimension_alloc(ctx_, "d1", TILEDB_INT8, &range[0], nullptr, &d1);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, ARRAY_URI.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    ExtentCheckFailureFx,
    "Don't overflow signed integers in extent check",
    "[signed-overflow][bug][sc47034]") {
  this->create_array();

  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, ARRAY_URI.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  tiledb_array_schema_t* schema;
  rc = tiledb_array_get_schema(ctx_, array, &schema);
  CHECK(rc == TILEDB_OK);

  tiledb_domain_t* dom;
  rc = tiledb_array_schema_get_domain(ctx_, schema, &dom);
  CHECK(rc == TILEDB_OK);

  tiledb_dimension_t* dim;
  rc = tiledb_domain_get_dimension_from_index(ctx_, dom, 0, &dim);
  CHECK(rc == TILEDB_OK);

  const void* value = nullptr;
  rc = tiledb_dimension_get_tile_extent(ctx_, dim, &value);
  CHECK(rc == TILEDB_OK);

  const auto extent_ptr = (int8_t*)value;
  CHECK(*extent_ptr > 0);
}
