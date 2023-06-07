#include <tiledb/tiledb_experimental.h>
#include <fstream>
#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// This example assumes you have a local deployment of TileDB REST server
// serving at localhost:8181 where test_gow_rest2 is a registered array.
// The example creates/deletes this array directly from S3, but as long as the
// array was registered on the REST server, the test should work fine
std::string array_namespace("tiledb://demo/");
std::string s3_array("s3://tiledb-shaun/arrays/test_gow_rest2");

uint64_t tile_extent = 32;
uint64_t capacity = tile_extent;
uint64_t dim_limit = 1572864;  // 12mb
uint64_t ncells = 393216;      // 3MB

// Needs to be tile aligned
uint64_t chunk_size = 131072;  // 1MB

std::vector<uint64_t> a1;
std::vector<uint64_t> a2;
std::vector<uint8_t> a2_nullable;

std::string a3 = "abcd";
std::vector<char> a3_data;
std::vector<uint64_t> a3_offsets;

// Replace with TILEDB_SPARSE to test a sparse array
tiledb_array_type_t array_type = TILEDB_DENSE;
std::vector<uint64_t> coords;

void create_array(Context& ctx) {
  ArraySchema schema(ctx, array_type);
  Domain domain(ctx);
  domain.add_dimension(
      Dimension::create<uint64_t>(ctx, "d1", {0, dim_limit}, tile_extent));
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<uint64_t>(ctx, "a1"));
  schema.add_attribute(
      Attribute::create<uint64_t>(ctx, "a2").set_nullable(true));
  schema.add_attribute(Attribute::create<std::vector<char>>(ctx, "a3"));
  if (array_type == TILEDB_SPARSE) {
    schema.set_capacity(capacity);
  }
  Array::create(array_namespace + s3_array, schema);
}

// Each global order write of size chunk_size will create an intermediate S3
// chunk which can be observed (when pausing execution before
// submit_and_finalize) in the fragment path under e.g.
// frag_uuid/__global_order_write_chunks/a1.tdb_0
void global_write(Context& ctx) {
  Array array(ctx, array_namespace + s3_array, TILEDB_WRITE);

  for (uint64_t i = 0; i < ncells; i++) {
    a1.push_back(i);
    a2.push_back(2 * i);
    a2_nullable.push_back(a2.back() % 5 == 0 ? 0 : 1);
    a3_offsets.push_back(i % chunk_size * a3.size());
    a3_data.insert(a3_data.end(), a3.begin(), a3.end());
    if (array_type == TILEDB_SPARSE) {
      coords.push_back(i);
    }
  }

  uint64_t last_space_tile =
      (ncells / tile_extent +
       static_cast<uint64_t>(ncells % tile_extent != 0)) *
          tile_extent -
      1;

  Query query(ctx, array);
  query.set_layout(TILEDB_GLOBAL_ORDER);

  if (array_type == TILEDB_DENSE) {
    Subarray subarray(ctx, array);
    subarray.add_range(0, (uint64_t)0, last_space_tile);
    query.set_subarray(subarray);
  }

  uint64_t begin = 0;
  while (begin < ncells - chunk_size) {
    query.set_data_buffer("a1", a1.data() + begin, chunk_size);
    if (array_type == TILEDB_SPARSE) {
      query.set_data_buffer("d1", coords.data() + begin, chunk_size);
    }
    query.set_data_buffer("a2", a2.data() + begin, chunk_size);
    query.set_validity_buffer("a2", a2_nullable.data() + begin, chunk_size);
    query.set_data_buffer(
        "a3", a3_data.data() + begin * a3.size(), chunk_size * a3.size());
    query.set_offsets_buffer("a3", a3_offsets.data() + begin, chunk_size);
    query.submit();

    begin += chunk_size;
  }

  query.set_data_buffer("a1", a1.data() + begin, last_space_tile - begin + 1);
  if (array_type == TILEDB_SPARSE) {
    query.set_data_buffer(
        "d1", coords.data() + begin, last_space_tile - begin + 1);
  }
  query.set_data_buffer("a2", a2.data() + begin, last_space_tile - begin + 1);
  query.set_validity_buffer(
      "a2", a2_nullable.data() + begin, last_space_tile - begin + 1);
  query.set_data_buffer(
      "a3",
      a3_data.data() + begin * a3.size(),
      (last_space_tile - begin + 1) * a3.size());
  query.set_offsets_buffer(
      "a3", a3_offsets.data() + begin, last_space_tile - begin + 1);
  query.submit_and_finalize();
  if (query.query_status() != Query::Status::COMPLETE) {
    throw std::runtime_error("Query incomplete");
  }
}

void read_and_validate(Context& ctx) {
  Array array(ctx, array_namespace + s3_array, TILEDB_READ);

  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR);
  if (array_type == TILEDB_DENSE) {
    Subarray subarray(ctx, array);
    subarray.add_range(0, (uint64_t)0, ncells - 1);
    query.set_subarray(subarray);
  } else {
    query.set_data_buffer("d1", coords);
  }
  std::vector<uint64_t> a1_result(ncells);
  std::vector<uint64_t> a2_result(ncells);
  std::vector<uint8_t> a2_result_nullable(ncells);
  std::vector<char> a3_result_data(a3.size() * ncells);
  std::vector<uint64_t> a3_result_offsets(ncells);
  query.set_data_buffer("a1", a1_result.data(), a1_result.size());
  query.set_data_buffer("a2", a2_result.data(), a2_result.size());
  query.set_validity_buffer(
      "a2", a2_result_nullable.data(), a2_result_nullable.size());
  query.set_data_buffer(
      "a3", a3_result_data.data(), a3_result_data.size() * a3.size());
  query.set_offsets_buffer(
      "a3", a3_result_offsets.data(), a3_result_offsets.size());
  query.submit();

  if (query.query_status() != Query::Status::COMPLETE) {
    throw std::runtime_error("Query incomplete during read");
  }

  for (uint64_t i = 0; i < ncells; ++i) {
    if (a1[i] != a1_result[i]) {
      throw std::runtime_error("Incorrect data read on a1");
    }
    if (a2[i] != a2_result[i]) {
      throw std::runtime_error("Incorrect data read on a2");
    }
    if (a2_nullable[i] != a2_result_nullable[i]) {
      throw std::runtime_error("Incorrect data read on nullable vector a2");
    }
    for (uint64_t j = 0; j < a3.size(); ++j) {
      if (a3_data[i * a3.size() + j] != a3_result_data[i * a3.size() + j]) {
        throw std::runtime_error("Incorrect data read on a3");
      }
    }
  }
}

int main() {
  Config cfg;
  cfg["rest.username"] = "demo";
  cfg["rest.password"] = "Demodemodemo!";
  cfg["rest.server_address"] = "http://127.0.0.1:8181";

  Context ctx(cfg);

  try {
    create_array(ctx);
  } catch (...) {
    Array::delete_array(ctx, array_namespace + s3_array);
    std::cout << "Removed existing array" << std::endl;
    create_array(ctx);
  }
  global_write(ctx);
  read_and_validate(ctx);

  return 0;
}
