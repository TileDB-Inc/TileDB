#include <limits>
#include <string>

#include <tiledb/tiledb>

#include <test/support/tdb_catch.h>

std::string array_name = "cpp_unit_array_24079";

TEST_CASE(
    "C++ API: DoubleDelta filter typecheck should account for output type of "
    "FloatScaleFilter",
    "[cppapi][filter][float-scaling]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  tiledb::Domain domain(ctx);
  float domain_lo = static_cast<float>(std::numeric_limits<int64_t>::min());
  float domain_hi = static_cast<float>(std::numeric_limits<int64_t>::max() - 1);

  // Create and initialize dimension.
  auto d1 = tiledb::Dimension::create<float>(
      ctx, "d1", {{domain_lo, domain_hi}}, 2048);

  tiledb::Filter float_scale(ctx, TILEDB_FILTER_SCALE_FLOAT);
  double scale = 1.0f;
  double offset = 0.0f;
  uint64_t byte_width = 8;

  float_scale.set_option(TILEDB_SCALE_FLOAT_BYTEWIDTH, &byte_width);
  float_scale.set_option(TILEDB_SCALE_FLOAT_FACTOR, &scale);
  float_scale.set_option(TILEDB_SCALE_FLOAT_OFFSET, &offset);

  tiledb::Filter dd(ctx, TILEDB_FILTER_DOUBLE_DELTA);

  tiledb::FilterList filters(ctx);
  filters.add_filter(float_scale);
  filters.add_filter(dd);

  d1.set_filter_list(filters);
  domain.add_dimension(d1);

  auto a1 = tiledb::Attribute::create<float>(ctx, "a1");
  a1.set_filter_list(filters);

  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);
  schema.set_capacity(100000);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  CHECK_NOTHROW(tiledb::Array::create(array_name, schema));
  std::vector<float> d1_data = {
      1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
  std::vector<float> a1_data = {
      1.0f, 2.1f, 3.2f, 4.3f, 5.4f, 6.5f, 7.6f, 8.7f, 9.8f, 10.9f};

  // Write to array.
  {
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    query.set_data_buffer("d1", d1_data);
    query.set_data_buffer("a1", a1_data);
    query.submit();
    CHECK(tiledb::Query::Status::COMPLETE == query.query_status());
  }

  // Read from array.
  {
    std::vector<float> d1_read(10);
    std::vector<float> a1_read(10);
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    tiledb::Subarray subarray(ctx, array);
    subarray.add_range(0, domain_lo, domain_hi);
    query.set_subarray(subarray);
    query.set_data_buffer("a1", a1_read);
    query.set_data_buffer("d1", d1_read);
    query.submit();
    CHECK(tiledb::Query::Status::COMPLETE == query.query_status());
    CHECK(
        std::vector<float>{
            1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 7.0f, 8.0f, 9.0f, 10.0f, 11.0f} ==
        a1_read);
    CHECK(d1_data == d1_read);
  }

  // Cleanup.
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}
