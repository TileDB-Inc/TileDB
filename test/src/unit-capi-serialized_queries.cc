/**
 * @file   unit-capi-serialized_queries.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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
 * Tests for query serialization/deserialization.
 */

#include "catch.hpp"

#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/serialization/query.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

using namespace tiledb;

namespace {

#ifdef _WIN32
static const char PATH_SEPARATOR = '\\';
static std::string current_dir() {
  return sm::Win::current_dir();
}
#else
static const char PATH_SEPARATOR = '/';
static std::string current_dir() {
  return sm::Posix::current_dir();
}
#endif

struct SerializationFx {
  const std::string tmpdir = "serialization_test_dir";
  const std::string array_name = "testarray";
  const std::string array_uri =
      current_dir() + PATH_SEPARATOR + tmpdir + "/" + array_name;

  Context ctx;
  VFS vfs;

  SerializationFx()
      : vfs(ctx) {
    if (vfs.is_dir(tmpdir))
      vfs.remove_dir(tmpdir);
    vfs.create_dir(tmpdir);
    if (!vfs.is_dir(tmpdir))
      std::cerr << "'created' but not finding dir '" << tmpdir << "'"
                << std::endl;
  }

  ~SerializationFx() {
    if (vfs.is_dir(tmpdir))
      vfs.remove_dir(tmpdir);
  }

  void create_array(tiledb_array_type_t type) {
    ArraySchema schema(ctx, type);
    Domain domain(ctx);
    domain.add_dimension(Dimension::create<int32_t>(ctx, "d1", {1, 10}, 2))
        .add_dimension(Dimension::create<int32_t>(ctx, "d2", {1, 10}, 2));
    schema.set_domain(domain);

    schema.add_attribute(Attribute::create<uint32_t>(ctx, "a1"));
    schema.add_attribute(
        Attribute::create<std::array<uint32_t, 2>>(ctx, "a2").set_nullable(
            true));
    schema.add_attribute(Attribute::create<std::vector<char>>(ctx, "a3"));

    Array::create(array_uri, schema);
  }

  void write_dense_array() {
    std::vector<int32_t> subarray = {1, 10, 1, 10};
    std::vector<uint32_t> a1;
    std::vector<uint32_t> a2;
    std::vector<uint8_t> a2_nullable;
    std::vector<char> a3_data;
    std::vector<uint64_t> a3_offsets;

    const unsigned ncells =
        (subarray[1] - subarray[0] + 1) * (subarray[3] - subarray[2] + 1);
    for (unsigned i = 0; i < ncells; i++) {
      a1.push_back(i);
      a2.push_back(i);
      a2.push_back(2 * i);
      a2_nullable.push_back(a2.back() % 5 == 0 ? 0 : 1);

      std::string a3 = "a";
      for (unsigned j = 0; j < i; j++)
        a3.push_back('a');
      a3_offsets.push_back(a3_data.size());
      a3_data.insert(a3_data.end(), a3.begin(), a3.end());
    }

    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array);
    query.set_subarray(subarray);
    query.set_buffer("a1", a1);
    query.set_buffer_nullable("a2", a2, a2_nullable);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy and submit.
    std::vector<uint8_t> serialized;
    serialize_query(ctx, query, &serialized, true);
    Array array2(ctx, array_uri, TILEDB_WRITE);
    Query query2(ctx, array2);
    deserialize_query(ctx, serialized, &query2, false);
    query2.submit();
    serialize_query(ctx, query2, &serialized, false);
    deserialize_query(ctx, serialized, &query, true);
  }

  void write_dense_array_ranges() {
    std::vector<int32_t> subarray = {1, 10, 1, 10};
    std::vector<uint32_t> a1;
    std::vector<uint32_t> a2;
    std::vector<uint8_t> a2_nullable;
    std::vector<char> a3_data;
    std::vector<uint64_t> a3_offsets;

    const unsigned ncells =
        (subarray[1] - subarray[0] + 1) * (subarray[3] - subarray[2] + 1);
    for (unsigned i = 0; i < ncells; i++) {
      a1.push_back(i);
      a2.push_back(i);
      a2.push_back(2 * i);
      a2_nullable.push_back(a2.back() % 5 == 0 ? 0 : 1);

      std::string a3 = "a";
      for (unsigned j = 0; j < i; j++)
        a3.push_back('a');
      a3_offsets.push_back(a3_data.size());
      a3_data.insert(a3_data.end(), a3.begin(), a3.end());
    }

    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array);
    query.add_range(0, subarray[0], subarray[1]);
    query.add_range(1, subarray[2], subarray[3]);
    query.set_buffer("a1", a1);
    query.set_buffer_nullable("a2", a2, a2_nullable);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy and submit.
    std::vector<uint8_t> serialized;
    serialize_query(ctx, query, &serialized, true);
    Array array2(ctx, array_uri, TILEDB_WRITE);
    Query query2(ctx, array2);
    deserialize_query(ctx, serialized, &query2, false);
    query2.submit();
    serialize_query(ctx, query2, &serialized, false);
    deserialize_query(ctx, serialized, &query, true);
  }

  void write_sparse_array() {
    std::vector<int32_t> coords = {1, 1, 2, 2, 3, 3, 4, 4, 5,  5,
                                   6, 6, 7, 7, 8, 8, 9, 9, 10, 10};
    std::vector<uint32_t> a1;
    std::vector<uint32_t> a2;
    std::vector<uint8_t> a2_nullable;
    std::vector<char> a3_data;
    std::vector<uint64_t> a3_offsets;

    const unsigned ncells = 10;
    for (unsigned i = 0; i < ncells; i++) {
      a1.push_back(i);
      a2.push_back(i);
      a2.push_back(2 * i);
      a2_nullable.push_back(a2.back() % 5 == 0 ? 0 : 1);

      std::string a3 = "a";
      for (unsigned j = 0; j < i; j++)
        a3.push_back('a');
      a3_offsets.push_back(a3_data.size());
      a3_data.insert(a3_data.end(), a3.begin(), a3.end());
    }

    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array);
    query.set_layout(TILEDB_UNORDERED);
    query.set_coordinates(coords);
    query.set_buffer("a1", a1);
    query.set_buffer_nullable("a2", a2, a2_nullable);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy and submit.
    std::vector<uint8_t> serialized;
    serialize_query(ctx, query, &serialized, true);
    Array array2(ctx, array_uri, TILEDB_WRITE);
    Query query2(ctx, array2);
    deserialize_query(ctx, serialized, &query2, false);
    query2.submit();
    serialize_query(ctx, query2, &serialized, false);
    deserialize_query(ctx, serialized, &query, true);
  }

  void write_sparse_array_split_coords() {
    std::vector<int32_t> d1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    std::vector<int32_t> d2 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    std::vector<uint32_t> a1;
    std::vector<uint32_t> a2;
    std::vector<uint8_t> a2_nullable;
    std::vector<char> a3_data;
    std::vector<uint64_t> a3_offsets;

    const unsigned ncells = 10;
    for (unsigned i = 0; i < ncells; i++) {
      a1.push_back(i);
      a2.push_back(i);
      a2.push_back(2 * i);
      a2_nullable.push_back(a2.back() % 5 == 0 ? 0 : 1);

      std::string a3 = "a";
      for (unsigned j = 0; j < i; j++)
        a3.push_back('a');
      a3_offsets.push_back(a3_data.size());
      a3_data.insert(a3_data.end(), a3.begin(), a3.end());
    }

    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array);
    query.set_layout(TILEDB_UNORDERED);
    query.set_buffer("d1", d1);
    query.set_buffer("d2", d2);
    query.set_buffer("a1", a1);
    query.set_buffer_nullable("a2", a2, a2_nullable);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy and submit.
    std::vector<uint8_t> serialized;
    serialize_query(ctx, query, &serialized, true);
    Array array2(ctx, array_uri, TILEDB_WRITE);
    Query query2(ctx, array2);
    deserialize_query(ctx, serialized, &query2, false);
    query2.submit();
    serialize_query(ctx, query2, &serialized, false);
    deserialize_query(ctx, serialized, &query, true);
  }

  /**
   * Helper function that serializes a query from the "client" or "server"
   * perspective. The flow being mimicked here is (for read queries):
   *
   * - Client sets up read query object including buffers.
   * - Client submits query to a remote array.
   * - Internal code (not C API) serializes that query and send it via curl.
   * - Server receives and deserializes the query using the C API.
   * - Server submits query.
   * - Server serializes (using C API) the query and sends it back.
   * - Client receives response and deserializes the query (not C API). This
   *   copies the query results into the original user buffers.
   * - Client's blocking call to tiledb_query_submit() now returns.
   */
  static void serialize_query(
      const Context& ctx,
      Query& query,
      std::vector<uint8_t>* serialized,
      bool clientside) {
    // Serialize
    tiledb_buffer_list_t* buff_list;
    ctx.handle_error(tiledb_serialize_query(
        ctx.ptr().get(),
        query.ptr().get(),
        TILEDB_CAPNP,
        clientside ? 1 : 0,
        &buff_list));

    // Flatten
    tiledb_buffer_t* c_buff;
    ctx.handle_error(
        tiledb_buffer_list_flatten(ctx.ptr().get(), buff_list, &c_buff));

    // Wrap in a safe pointer
    auto deleter = [](tiledb_buffer_t* b) { tiledb_buffer_free(&b); };
    std::unique_ptr<tiledb_buffer_t, decltype(deleter)> buff_ptr(
        c_buff, deleter);

    // Copy into user vector
    void* data;
    uint64_t num_bytes;
    ctx.handle_error(
        tiledb_buffer_get_data(ctx.ptr().get(), c_buff, &data, &num_bytes));
    serialized->clear();
    serialized->insert(
        serialized->end(),
        static_cast<const uint8_t*>(data),
        static_cast<const uint8_t*>(data) + num_bytes);

    // Free buffer list
    tiledb_buffer_list_free(&buff_list);
  }

  /**
   * Helper function that deserializes a query from the "client" or "server"
   * perspective.
   */
  static void deserialize_query(
      const Context& ctx,
      std::vector<uint8_t>& serialized,
      Query* query,
      bool clientside) {
    tiledb_buffer_t* c_buff;
    ctx.handle_error(tiledb_buffer_alloc(ctx.ptr().get(), &c_buff));

    // Wrap in a safe pointer
    auto deleter = [](tiledb_buffer_t* b) { tiledb_buffer_free(&b); };
    std::unique_ptr<tiledb_buffer_t, decltype(deleter)> buff_ptr(
        c_buff, deleter);

    ctx.handle_error(tiledb_buffer_set_data(
        ctx.ptr().get(),
        c_buff,
        reinterpret_cast<void*>(&serialized[0]),
        static_cast<uint64_t>(serialized.size())));

    // Deserialize
    ctx.handle_error(tiledb_deserialize_query(
        ctx.ptr().get(),
        c_buff,
        TILEDB_CAPNP,
        clientside ? 1 : 0,
        query->ptr().get()));
  }

  /**
   * Helper function that allocates buffers on a query object that has been
   * deserialized on the "server" side.
   */
  static std::vector<void*> allocate_query_buffers(
      const Context& ctx, const Array& array, Query* query) {
    (void)array;
    std::vector<void*> to_free;
    void* unused1;
    uint64_t* unused2;
    uint8_t* unused3;
    uint64_t *a1_size, *a2_size, *a2_validity_size, *a3_size, *a3_offset_size,
        *coords_size;
    ctx.handle_error(tiledb_query_get_buffer(
        ctx.ptr().get(), query->ptr().get(), "a1", &unused1, &a1_size));
    ctx.handle_error(tiledb_query_get_buffer_nullable(
        ctx.ptr().get(),
        query->ptr().get(),
        "a2",
        &unused1,
        &a2_size,
        &unused3,
        &a2_validity_size));
    ctx.handle_error(tiledb_query_get_buffer_var(
        ctx.ptr().get(),
        query->ptr().get(),
        "a3",
        &unused2,
        &a3_offset_size,
        &unused1,
        &a3_size));
    ctx.handle_error(tiledb_query_get_buffer(
        ctx.ptr().get(),
        query->ptr().get(),
        TILEDB_COORDS,
        &unused1,
        &coords_size));

    if (a1_size != nullptr) {
      void* buff = std::malloc(*a1_size);
      ctx.handle_error(tiledb_query_set_buffer(
          ctx.ptr().get(), query->ptr().get(), "a1", buff, a1_size));
      to_free.push_back(buff);
    }

    if (a2_size != nullptr) {
      void* buff = std::malloc(*a2_size);
      uint8_t* validity = (uint8_t*)std::malloc(*a2_validity_size);
      ctx.handle_error(tiledb_query_set_buffer_nullable(
          ctx.ptr().get(),
          query->ptr().get(),
          "a2",
          buff,
          a2_size,
          validity,
          a2_validity_size));
      to_free.push_back(buff);
    }

    if (a3_size != nullptr) {
      void* buff = std::malloc(*a3_size);
      uint64_t* offsets = (uint64_t*)std::malloc(*a3_offset_size);
      ctx.handle_error(tiledb_query_set_buffer_var(
          ctx.ptr().get(),
          query->ptr().get(),
          "a3",
          offsets,
          a3_offset_size,
          buff,
          a3_size));
      to_free.push_back(buff);
      to_free.push_back(offsets);
    }

    if (coords_size != nullptr) {
      void* buff = std::malloc(*coords_size);
      ctx.handle_error(tiledb_query_set_buffer(
          ctx.ptr().get(),
          query->ptr().get(),
          TILEDB_COORDS,
          buff,
          coords_size));
      to_free.push_back(buff);
    }

    return to_free;
  }
};

}  // namespace

TEST_CASE_METHOD(
    SerializationFx,
    "Query serialization, dense",
    "[query][dense][serialization]") {
  create_array(TILEDB_DENSE);
  write_dense_array();

  SECTION("- Read all") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(500);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    query.set_subarray(subarray);
    query.set_buffer("a1", a1);
    query.set_buffer_nullable("a2", a2, a2_nullable);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy (client side).
    std::vector<uint8_t> serialized;
    serialize_query(ctx, query, &serialized, true);

    // Deserialize into a new query and allocate buffers (server side).
    Array array2(ctx, array_uri, TILEDB_READ);
    Query query2(ctx, array2);
    deserialize_query(ctx, serialized, &query2, false);
    auto to_free = allocate_query_buffers(ctx, array2, &query2);

    // Submit and serialize results (server side).
    query2.submit();
    serialize_query(ctx, query2, &serialized, false);

    // Deserialize into original query (client side).
    deserialize_query(ctx, serialized, &query, true);
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 100);
    REQUIRE(std::get<1>(result_el["a2"]) == 200);
    REQUIRE(std::get<2>(result_el["a2"]) == 100);
    REQUIRE(std::get<0>(result_el["a3"]) == 100);
    REQUIRE(std::get<1>(result_el["a3"]) == 5050);

    for (void* b : to_free)
      std::free(b);
  }

  SECTION("- Read subarray") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(500);
    std::vector<uint8_t> a2_nullable(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {3, 4, 3, 4};

    query.set_subarray(subarray);
    query.set_buffer("a1", a1);
    query.set_buffer_nullable("a2", a2, a2_nullable);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy (client side).
    std::vector<uint8_t> serialized;
    serialize_query(ctx, query, &serialized, true);

    // Deserialize into a new query and allocate buffers (server side).
    Array array2(ctx, array_uri, TILEDB_READ);
    Query query2(ctx, array2);
    deserialize_query(ctx, serialized, &query2, false);
    auto to_free = allocate_query_buffers(ctx, array2, &query2);

    // Submit and serialize results (server side).
    query2.submit();
    serialize_query(ctx, query2, &serialized, false);

    // Deserialize into original query (client side).
    deserialize_query(ctx, serialized, &query, true);
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 4);
    REQUIRE(std::get<1>(result_el["a2"]) == 8);
    REQUIRE(std::get<2>(result_el["a2"]) == 4);
    REQUIRE(std::get<0>(result_el["a3"]) == 4);
    REQUIRE(std::get<1>(result_el["a3"]) == 114);

    for (void* b : to_free)
      std::free(b);
  }

  SECTION("- Incomplete read") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(4);
    std::vector<uint32_t> a2(4);
    std::vector<uint8_t> a2_nullable(4);
    std::vector<char> a3_data(60);
    std::vector<uint64_t> a3_offsets(4);
    std::vector<int32_t> subarray = {3, 4, 3, 4};
    query.set_subarray(subarray);

    auto set_buffers = [&](Query& q) {
      q.set_buffer("a1", a1);
      q.set_buffer_nullable("a2", a2, a2_nullable);
      q.set_buffer("a3", a3_offsets, a3_data);
    };

    auto serialize_and_submit = [&](Query& q) {
      // Serialize into a copy (client side).
      std::vector<uint8_t> serialized;
      serialize_query(ctx, q, &serialized, true);

      // Deserialize into a new query and allocate buffers (server side).
      Array array2(ctx, array_uri, TILEDB_READ);
      Query query2(ctx, array2);
      deserialize_query(ctx, serialized, &query2, false);
      auto to_free = allocate_query_buffers(ctx, array2, &query2);

      // Submit and serialize results (server side).
      query2.submit();
      serialize_query(ctx, query2, &serialized, false);

      // Deserialize into original query (client side).
      deserialize_query(ctx, serialized, &q, true);

      for (void* b : to_free)
        std::free(b);
    };

    // Submit initial query.
    set_buffers(query);
    serialize_and_submit(query);
    REQUIRE(query.query_status() == Query::Status::INCOMPLETE);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 2);
    REQUIRE(std::get<1>(result_el["a2"]) == 4);
    REQUIRE(std::get<2>(result_el["a2"]) == 2);
    REQUIRE(std::get<0>(result_el["a3"]) == 2);
    REQUIRE(std::get<1>(result_el["a3"]) == 47);

    // Reset buffers, serialize and resubmit
    set_buffers(query);
    serialize_and_submit(query);

    REQUIRE(query.query_status() == Query::Status::INCOMPLETE);
    result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 1);
    REQUIRE(std::get<1>(result_el["a2"]) == 2);
    REQUIRE(std::get<2>(result_el["a2"]) == 1);
    REQUIRE(std::get<0>(result_el["a3"]) == 1);
    REQUIRE(std::get<1>(result_el["a3"]) == 33);

    // Reset buffers, serialize and resubmit
    set_buffers(query);
    serialize_and_submit(query);

    REQUIRE(query.query_status() == Query::Status::COMPLETE);
    result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 1);
    REQUIRE(std::get<1>(result_el["a2"]) == 2);
    REQUIRE(std::get<2>(result_el["a2"]) == 1);
    REQUIRE(std::get<0>(result_el["a3"]) == 1);
    REQUIRE(std::get<1>(result_el["a3"]) == 34);
  }
}

TEST_CASE_METHOD(
    SerializationFx,
    "Query serialization, sparse",
    "[query][sparse][serialization]") {
  create_array(TILEDB_SPARSE);
  write_sparse_array();

  SECTION("- Read all") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<int32_t> coords(1000);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    query.set_subarray(subarray);
    query.set_coordinates(coords);
    query.set_buffer("a1", a1);
    query.set_buffer_nullable("a2", a2, a2_nullable);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy and submit.
    std::vector<uint8_t> serialized;
    serialize_query(ctx, query, &serialized, true);
    Array array2(ctx, array_uri, TILEDB_READ);
    Query query2(ctx, array2);
    deserialize_query(ctx, serialized, &query2, false);
    auto to_free = allocate_query_buffers(ctx, array2, &query2);
    query2.submit();
    serialize_query(ctx, query2, &serialized, false);

    // Deserialize into original query
    deserialize_query(ctx, serialized, &query, true);
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 10);
    REQUIRE(std::get<1>(result_el["a2"]) == 20);
    REQUIRE(std::get<2>(result_el["a2"]) == 10);
    REQUIRE(std::get<0>(result_el["a3"]) == 10);
    REQUIRE(std::get<1>(result_el["a3"]) == 55);

    for (void* b : to_free)
      std::free(b);
  }
}

TEST_CASE_METHOD(
    SerializationFx,
    "Query serialization, split coords, sparse",
    "[query][sparse][serialization][split-coords]") {
  create_array(TILEDB_SPARSE);
  write_sparse_array_split_coords();

  SECTION("- Read all") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<int32_t> coords(1000);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    query.set_subarray(subarray);
    query.set_coordinates(coords);
    query.set_buffer("a1", a1);
    query.set_buffer_nullable("a2", a2, a2_nullable);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy and submit.
    std::vector<uint8_t> serialized;
    serialize_query(ctx, query, &serialized, true);
    Array array2(ctx, array_uri, TILEDB_READ);
    Query query2(ctx, array2);
    deserialize_query(ctx, serialized, &query2, false);
    auto to_free = allocate_query_buffers(ctx, array2, &query2);
    query2.submit();
    serialize_query(ctx, query2, &serialized, false);

    // Deserialize into original query
    deserialize_query(ctx, serialized, &query, true);
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el[TILEDB_COORDS]) == 20);
    REQUIRE(std::get<1>(result_el["a1"]) == 10);
    REQUIRE(std::get<1>(result_el["a2"]) == 20);
    REQUIRE(std::get<2>(result_el["a2"]) == 10);
    REQUIRE(std::get<0>(result_el["a3"]) == 10);
    REQUIRE(std::get<1>(result_el["a3"]) == 55);

    for (void* b : to_free)
      std::free(b);
  }
}

TEST_CASE_METHOD(
    SerializationFx,
    "Query serialization, dense ranges",
    "[query][dense][serialization]") {
  create_array(TILEDB_DENSE);
  write_dense_array_ranges();

  SECTION("- Read all") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    query.add_range(0, subarray[0], subarray[1]);
    query.add_range(1, subarray[2], subarray[3]);
    query.set_buffer("a1", a1);
    query.set_buffer_nullable("a2", a2, a2_nullable);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy (client side).
    std::vector<uint8_t> serialized;
    serialize_query(ctx, query, &serialized, true);

    // Deserialize into a new query and allocate buffers (server side).
    Array array2(ctx, array_uri, TILEDB_READ);
    Query query2(ctx, array2);
    deserialize_query(ctx, serialized, &query2, false);
    auto to_free = allocate_query_buffers(ctx, array2, &query2);

    // Submit and serialize results (server side).
    query2.submit();
    serialize_query(ctx, query2, &serialized, false);

    // Deserialize into original query (client side).
    deserialize_query(ctx, serialized, &query, true);
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 100);
    REQUIRE(std::get<1>(result_el["a2"]) == 200);
    REQUIRE(std::get<2>(result_el["a2"]) == 100);
    REQUIRE(std::get<0>(result_el["a3"]) == 100);
    REQUIRE(std::get<1>(result_el["a3"]) == 5050);

    for (void* b : to_free)
      std::free(b);
  }

  SECTION("- Read subarray") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {3, 4, 3, 4};

    query.add_range(0, subarray[0], subarray[1]);
    query.add_range(1, subarray[2], subarray[3]);
    query.set_buffer("a1", a1);
    query.set_buffer_nullable("a2", a2, a2_nullable);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy (client side).
    std::vector<uint8_t> serialized;
    serialize_query(ctx, query, &serialized, true);

    // Deserialize into a new query and allocate buffers (server side).
    Array array2(ctx, array_uri, TILEDB_READ);
    Query query2(ctx, array2);
    deserialize_query(ctx, serialized, &query2, false);
    auto to_free = allocate_query_buffers(ctx, array2, &query2);

    // Submit and serialize results (server side).
    query2.submit();
    serialize_query(ctx, query2, &serialized, false);

    // Deserialize into original query (client side).
    deserialize_query(ctx, serialized, &query, true);
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 4);
    REQUIRE(std::get<1>(result_el["a2"]) == 8);
    REQUIRE(std::get<2>(result_el["a2"]) == 4);
    REQUIRE(std::get<0>(result_el["a3"]) == 4);
    REQUIRE(std::get<1>(result_el["a3"]) == 114);

    for (void* b : to_free)
      std::free(b);
  }

  SECTION("- Incomplete read") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(4);
    std::vector<uint32_t> a2(4);
    std::vector<uint8_t> a2_nullable(4);
    std::vector<char> a3_data(60);
    std::vector<uint64_t> a3_offsets(4);
    std::vector<int32_t> subarray = {3, 4, 3, 4};
    query.add_range(0, subarray[0], subarray[1]);
    query.add_range(1, subarray[2], subarray[3]);

    auto set_buffers = [&](Query& q) {
      q.set_buffer("a1", a1);
      q.set_buffer_nullable("a2", a2, a2_nullable);
      q.set_buffer("a3", a3_offsets, a3_data);
    };

    auto serialize_and_submit = [&](Query& q) {
      // Serialize into a copy (client side).
      std::vector<uint8_t> serialized;
      serialize_query(ctx, q, &serialized, true);

      // Deserialize into a new query and allocate buffers (server side).
      Array array2(ctx, array_uri, TILEDB_READ);
      Query query2(ctx, array2);
      deserialize_query(ctx, serialized, &query2, false);
      auto to_free = allocate_query_buffers(ctx, array2, &query2);

      // Submit and serialize results (server side).
      query2.submit();
      serialize_query(ctx, query2, &serialized, false);

      // Deserialize into original query (client side).
      deserialize_query(ctx, serialized, &q, true);

      for (void* b : to_free)
        std::free(b);
    };

    // Submit initial query.
    set_buffers(query);
    serialize_and_submit(query);
    REQUIRE(query.query_status() == Query::Status::INCOMPLETE);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 2);
    REQUIRE(std::get<1>(result_el["a2"]) == 4);
    REQUIRE(std::get<2>(result_el["a2"]) == 2);
    REQUIRE(std::get<0>(result_el["a3"]) == 2);
    REQUIRE(std::get<1>(result_el["a3"]) == 47);

    // Reset buffers, serialize and resubmit
    set_buffers(query);
    serialize_and_submit(query);

    REQUIRE(query.query_status() == Query::Status::INCOMPLETE);
    result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 1);
    REQUIRE(std::get<1>(result_el["a2"]) == 2);
    REQUIRE(std::get<2>(result_el["a2"]) == 1);
    REQUIRE(std::get<0>(result_el["a3"]) == 1);
    REQUIRE(std::get<1>(result_el["a3"]) == 33);

    // Reset buffers, serialize and resubmit
    set_buffers(query);
    serialize_and_submit(query);

    REQUIRE(query.query_status() == Query::Status::COMPLETE);
    result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 1);
    REQUIRE(std::get<1>(result_el["a2"]) == 2);
    REQUIRE(std::get<2>(result_el["a2"]) == 1);
    REQUIRE(std::get<0>(result_el["a3"]) == 1);
    REQUIRE(std::get<1>(result_el["a3"]) == 34);
  }
}
