/**
 * @file   fragment_info.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * When run, this program will create a simple 2D dense array, write some data
 * with one query (creating a fragment) and collect information on the fragment.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

Context ctx;

std::string uri("/Users/ypatia/Documents/core-1234");
// std::string
// uri("s3://tiledb-seth/customers/az/core-383/100k_fragment_array");

int main() {
  tiledb::Object obj = tiledb::Object::object(ctx, uri);
  // if (obj.type() != tiledb::Object::Type::Array) {
  //   ArraySchema schema(ctx, TILEDB_SPARSE);
  //   Domain domain(ctx);
  //   // Create domain
  //   domain.add_dimension(
  //       Dimension::create(ctx, "d0", TILEDB_STRING_ASCII, nullptr, nullptr));
  //   domain.add_dimension(Dimension::create<uint64_t>(
  //       ctx,
  //       "d1",
  //       {{0, std::numeric_limits<uint32_t>::max() - 1}},
  //       std::numeric_limits<uint32_t>::max()));
  //   domain.add_dimension(
  //       Dimension::create(ctx, "d2", TILEDB_STRING_ASCII, nullptr, nullptr));
  //   schema.set_domain(domain);

  //   schema.add_attribute(Attribute(ctx, "a0", TILEDB_UINT8));
  //   schema.add_attribute(Attribute(ctx, "a1", TILEDB_UINT8));
  //   schema.add_attribute(Attribute(ctx, "a2", TILEDB_UINT8));
  //   schema.add_attribute(Attribute(ctx, "a3", TILEDB_UINT8));

  //   Array::create(uri, schema);

  //   // Create a million fragments by writing one cell in each
  //   std::vector<char> d0_data = {'a'};
  //   std::vector<uint64_t> d0_offsets = {0};
  //   std::vector<uint64_t> d1_data = {0};
  //   std::vector<char> d2_data = {'a'};
  //   std::vector<uint64_t> d2_offsets = {0};
  //   std::vector<uint8_t> a0_data = {0};
  //   std::vector<uint8_t> a1_data = {0};
  //   std::vector<uint8_t> a2_data = {0};
  //   std::vector<uint8_t> a3_data = {0};

  //   tiledb::Array arr(ctx, uri, TILEDB_WRITE);
  //   for (uint64_t i = 0; i < 100000; i++) {
  //     tiledb::Query query(ctx, arr);
  //     query.set_data_buffer("d0", d0_data);
  //     query.set_offsets_buffer("d0", d0_offsets);
  //     query.set_data_buffer("d1", d1_data);
  //     query.set_data_buffer("d2", d2_data);
  //     query.set_offsets_buffer("d2", d2_offsets);

  //     // attributes
  //     query.set_data_buffer("a0", a0_data);
  //     query.set_data_buffer("a1", a1_data);
  //     query.set_data_buffer("a2", a2_data);
  //     query.set_data_buffer("a3", a3_data);

  //     query.submit();
  //   }

  //   tiledb::Config cfg;

  //   cfg.set("sm.consolidation.mode", "fragment_meta");
  //   cfg.set("sm.vacuum.mode", "fragment_meta");
  //   std::cout << "Consolidating fragment_meta" << std::endl;
  //   tiledb::Array::consolidate(ctx, uri, &cfg);
  //   std::cout << "Vacuuming fragment_meta" << std::endl;
  //   tiledb::Array::vacuum(ctx, uri, &cfg);

  //   cfg.set("sm.consolidation.mode", "commits");
  //   cfg.set("sm.vacuum.mode", "commits");
  //   std::cout << "Consolidating commits" << std::endl;
  //   tiledb::Array::consolidate(ctx, uri, &cfg);
  //   std::cout << "Vacuuming commits" << std::endl;
  //   tiledb::Array::vacuum(ctx, uri, &cfg);
  // }

  //  tiledb_serialize_array(*ctx.ptr(),
  // std::vector<bool> modes = {true, false};
  // std::vector<bool> capnp_modes = {true, false};
  std::vector<bool> modes = {true};
  // std::vector<bool> capnp_modes = false;
  // for (auto capnp_mode : capnp_modes) {
  for (auto mode : modes) {
    tiledb::Config cfg = tiledb::Config();

    std::string mode_str = "true";
    if (mode) {
      mode_str = "true";
    }

    // std::string capnp_copy_str = "false";
    // if (capnp_mode) {
    //   capnp_copy_str = "true";
    // }

    // std::cout << "use_refactored_array_open: " << mode << std::endl;

    // cfg.set("rest.use_refactored_array_open", mode_str);
    // cfg.set("rest.use_refactored_array_open_and_query_submit", mode_str);

    //      std::cout << "capnp buffer copy: " << capnp_mode << std::endl;
    //      cfg.set("capnp_copy", capnp_copy_str);
    // std::cout << "old_fragment_serialization: " << capnp_mode << std::endl;
    // cfg.set("old_fragment_serialization", capnp_copy_str);

    ctx = tiledb::Context(cfg);
    std::cout << "Opening array for serialization" << std::endl;
    auto start_time_array_open = std::chrono::high_resolution_clock::now();
    tiledb::Array array = Array(ctx, uri, TILEDB_READ);
    auto end_time_array_open = std::chrono::high_resolution_clock::now();
    std::cout << "array_open duration: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     end_time_array_open - start_time_array_open)
                     .count()
              << "ms" << std::endl;

    // std::cout << "Serializing" << std::endl;
    // tiledb_buffer_t* buff;
    // int rc = tiledb_serialize_array(
    //     ctx.ptr().get(), array.ptr().get(), TILEDB_CAPNP, 0, &buff);
    // REQUIRE(rc == TILEDB_OK);

    // std::cout << "Deserializing" << std::endl;
    // // Load array from the rest server
    // tiledb_array_t* new_array = nullptr;
    // auto start_time_array_deserializing =
    //     std::chrono::high_resolution_clock::now();
    // rc = tiledb_deserialize_array(
    //     ctx.ptr().get(), buff, TILEDB_CAPNP, 1, uri.c_str(), &new_array);
    // REQUIRE(rc == TILEDB_OK);
    // auto end_time_array_deserializing =
    //     std::chrono::high_resolution_clock::now();
    // std::cout << "array_deserializing duration: "
    //           << std::chrono::duration_cast<std::chrono::milliseconds>(
    //                   end_time_array_deserializing -
    //                   start_time_array_deserializing)
    //                   .count()
    //           << "ms" << std::endl;

    // Clean up.
    // tiledb_buffer_free(&buff);

    std::cout << std::endl;
  }
}
