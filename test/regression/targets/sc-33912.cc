
#include <test/support/tdb_catch.h>
#include <tiledb/tiledb>

#include <iostream>

using namespace tiledb;
// Both tests below use the same SparseUnorderedWithDups reader, set one
// dimension with no filters, and at least one other dimension with a filter.
// Both tests also submit incomplete reads and validate the data read back.

// This test has a schema that is closer to what VCF was using, but does not use
// zipped coordinates. (dimension types are different)
TEST_CASE("Coordinate filters VCF", "[sc33912][vcf]") {
  std::string array_name =
      std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays/sc33912";
  Config cfg;
  cfg["sm.io_concurrency_level"] = "1";
  cfg["sm.compute_concurrency_level"] = "1";
  Context ctx(cfg);

  VFS vfs(ctx);
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Create array.
  {
    // Empty FilterList
    FilterList no_filters(ctx);

    Domain domain(ctx);
    auto d1 =
        Dimension::create(ctx, "d1", TILEDB_STRING_ASCII, nullptr, nullptr);
    d1.set_filter_list(no_filters);
    domain.add_dimension(d1);
    CHECK(d1.filter_list().nfilters() == 0);

    auto d2 = Dimension::create<uint32_t>(
        ctx,
        "d2",
        {{0, std::numeric_limits<uint32_t>::max() - 1}},
        std::numeric_limits<uint32_t>::max());
    FilterList d2_filters(ctx);
    d2_filters.add_filter({ctx, TILEDB_FILTER_GZIP});
    d2.set_filter_list(d2_filters);
    domain.add_dimension(d2);
    CHECK(d2.filter_list().nfilters() == 1);
    CHECK(d2.filter_list().filter(0).filter_type() == TILEDB_FILTER_GZIP);

    auto d3 =
        Dimension::create(ctx, "d3", TILEDB_STRING_ASCII, nullptr, nullptr);
    FilterList d3_filters(ctx);
    d3_filters.add_filter({ctx, TILEDB_FILTER_BZIP2});
    d3.set_filter_list(d3_filters);
    domain.add_dimension(d3);
    CHECK(d3.filter_list().nfilters() == 1);
    CHECK(d3.filter_list().filter(0).filter_type() == TILEDB_FILTER_BZIP2);

    ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
    auto a1 = Attribute::create<int16_t>(ctx, "a1");
    int16_t fill_value = -1;
    a1.set_fill_value(&fill_value, sizeof(fill_value));
    schema.add_attribute(a1);
    schema.set_allows_dups(true);

    Array::create(array_name, schema);
  }

  std::string d1_coords =
      "sample0sample1sample2sample3sample4sample5sample6sample7sample8sample9";
  std::vector<uint64_t> d1_offsets = {0, 7, 14, 21, 28, 35, 42, 49, 56, 63};

  std::vector<uint32_t> d2_coords = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90};

  std::string d3_coords =
      "region0region1region2region3region4region5region6region7region8region9";
  std::vector<uint64_t> d3_offsets = {0, 7, 14, 21, 28, 35, 42, 49, 56, 63};

  // Write some data to the array.
  std::vector<int16_t> a1_write = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  {
    Array array(ctx, array_name, TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);

    query.set_data_buffer("d1", d1_coords).set_offsets_buffer("d1", d1_offsets);
    query.set_data_buffer("d2", d2_coords);
    query.set_data_buffer("d3", d3_coords).set_offsets_buffer("d3", d3_offsets);
    query.set_data_buffer("a1", a1_write);
    query.submit();
    CHECK(query.query_status() == Query::Status::COMPLETE);
  }

  // Read array.
  {
    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);

    // Validate that the default coordinate filters are used for the dimension
    // with no filters.
    auto coords_filters = array.schema().coords_filter_list();
    auto d1_filters = array.schema().domain().dimension(0).filter_list();
    // These checks fail since Luc's commit is not on this branch yet.
    // TODO: uncomment these checks when Luc's commit is on this branch.
    //    CHECK(d1_filters.nfilters() == coords_filters.nfilters());
    //    for (size_t i = 0; i < coords_filters.nfilters(); i++) {
    //      CHECK(
    //          d1_filters.filter(i).filter_type() ==
    //          coords_filters.filter(i).filter_type());
    //    }

    auto d2 = array.schema().domain().dimension(1);
    CHECK(d2.filter_list().nfilters() == 1);
    CHECK(d2.filter_list().filter(0).filter_type() == TILEDB_FILTER_GZIP);

    auto d3 = array.schema().domain().dimension(2);
    CHECK(d3.filter_list().nfilters() == 1);
    CHECK(d3.filter_list().filter(0).filter_type() == TILEDB_FILTER_BZIP2);

    size_t batch_size = 2;
    std::vector<int16_t> a1_read(2);
    query.set_layout(TILEDB_UNORDERED);
    query.set_data_buffer("d1", d1_coords).set_offsets_buffer("d1", d1_offsets);
    query.set_data_buffer("d2", d2_coords);
    query.set_data_buffer("d3", d3_coords).set_offsets_buffer("d3", d3_offsets);

    query.set_data_buffer("a1", a1_read);

    size_t pos = 0;
    do {
      query.submit();
      CHECK(query.result_buffer_elements()["a1"].second == batch_size);
      for (size_t i = 0; i < batch_size; i++) {
        CHECK(a1_write[pos++] == a1_read[i]);
      }
    } while (query.query_status() == Query::Status::INCOMPLETE);

    CHECK(query.query_status() == Query::Status::COMPLETE);
  }

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

// This test uses zipped coordinates but seems to be failing for another reason.
// I'm getting segfaults when I run this test on this branch and current dev.
// The segfault is in SparseUnorderedWithDups::copy_fixed_data_tile during read:
// 1079    src_buff = array_schema_.attribute(name)->fill_value().data();
// Where `name` == "__coords"
TEST_CASE("Zipped coordinates", "[sc33912][zipped]") {
  std::string array_name =
      std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays/sc33912";
  Config cfg;
  cfg["sm.io_concurrency_level"] = "1";
  cfg["sm.compute_concurrency_level"] = "1";
  Context ctx(cfg);

  VFS vfs(ctx);
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Create array.
  {
    FilterList no_filters(ctx);

    Domain domain(ctx);
    auto d1 = Dimension::create<int16_t>(ctx, "d1", {{1, 4}}, 4);
    d1.set_filter_list(no_filters);
    domain.add_dimension(d1);
    CHECK(d1.filter_list().nfilters() == 0);

    auto d2 = Dimension::create<int16_t>(ctx, "d2", {{1, 4}}, 4);
    FilterList d2_filters(ctx);
    d2_filters.add_filter({ctx, TILEDB_FILTER_GZIP});
    d2.set_filter_list(d2_filters);
    domain.add_dimension(d2);
    CHECK(d2.filter_list().nfilters() == 1);
    CHECK(d2.filter_list().filter(0).filter_type() == TILEDB_FILTER_GZIP);

    ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
    auto a1 = Attribute::create<int16_t>(ctx, "a1");
    int16_t fill_value = -9;
    a1.set_fill_value(&fill_value, sizeof(fill_value));
    schema.add_attribute(a1);
    schema.set_allows_dups(true);

    Array::create(array_name, schema);
  }

  bool zipped_coords = GENERATE(false, true);
  DYNAMIC_SECTION(
      "Zipped coordinates: " << (zipped_coords ? "true" : "false")) {
    std::vector<int16_t> d1_coords = {1, 1, 2, 2, 3, 3, 4, 4};
    std::vector<int16_t> d2_coords = {1, 3, 2, 4, 1, 4, 2, 3};
    // clang-format off
    std::vector<int16_t> d1_d2_coords =
        {1, 1, 1, 3,
         2, 2, 2, 4,
         3, 1, 3, 4,
         4, 2, 4, 3,};
    // clang-format on

    // Write some data to the array.
    std::vector<int16_t> a1_write = {1, 2, 3, 4, 5, 6, 7, 8};
    {
      Array array(ctx, array_name, TILEDB_WRITE);
      Query query(ctx, array, TILEDB_WRITE);

      if (zipped_coords) {
        query.set_coordinates(d1_d2_coords);
        //        query.set_data_buffer("__coords", d1_d2_coords);
      } else {
        query.set_data_buffer("d1", d1_coords);
        query.set_data_buffer("d2", d2_coords);
      }
      query.set_data_buffer("a1", a1_write);
      query.submit();
      CHECK(query.query_status() == Query::Status::COMPLETE);
    }

    // Read array.
    {
      Array array(ctx, array_name, TILEDB_READ);
      Query query(ctx, array, TILEDB_READ);

      auto coords_filters = array.schema().coords_filter_list();
      auto d1_filters = array.schema().domain().dimension(0).filter_list();
      // TODO: uncomment these checks when Luc's commit is on this branch.
      //      CHECK(d1_filters.nfilters() == coords_filters.nfilters());
      //      for (size_t i = 0; i < coords_filters.nfilters(); i++) {
      //        CHECK(
      //            d1_filters.filter(i).filter_type() ==
      //            coords_filters.filter(i).filter_type());
      //      }

      auto d2 = array.schema().domain().dimension(1);
      CHECK(d2.filter_list().nfilters() == 1);
      CHECK(d2.filter_list().filter(0).filter_type() == TILEDB_FILTER_GZIP);

      size_t batch_size = 2;
      std::vector<int16_t> a1_read(batch_size);
      query.set_layout(TILEDB_UNORDERED);
      if (zipped_coords) {
        query.set_coordinates(d1_d2_coords);
        //        query.set_data_buffer("__coords", d1_d2_coords);
      } else {
        query.set_data_buffer("d1", d1_coords);
        query.set_data_buffer("d2", d2_coords);
      }
      query.set_data_buffer("a1", a1_read);

      size_t pos = 0;
      do {
        // Segfault is here, but only for the zipped coords case.
        // The other test passes using separate coord buffers.
        query.submit();
        CHECK(query.result_buffer_elements()["a1"].second == batch_size);
        for (size_t i = 0; i < batch_size; i++) {
          CHECK(a1_write[pos++] == a1_read[i]);
        }
      } while (query.query_status() == Query::Status::INCOMPLETE);

      CHECK(query.query_status() == Query::Status::COMPLETE);
    }
  }

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}
