#include <limits>
#include <string>

#include <tiledb/tiledb>

#include <catch2/catch_test_macros.hpp>

TEST_CASE(
    "C++ API: Consolidation slowness in create_buffer with large number of "
    "attributes"
    "[cppapi][consolidation][sc36372]") {
  std::string array_name = "cpp_unit_array_36372";

  tiledb::Config cfg;
  cfg["sm.consolidation.step_min_frags"] = 2;
  cfg["sm.consolidation.step_max_frags"] = 4;
  tiledb::Context ctx(cfg);
  tiledb::VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  tiledb::Domain domain(ctx);
  auto domain_lo = std::numeric_limits<unsigned int>::min();
  auto domain_hi = std::numeric_limits<unsigned int>::max() - 1;

  // Create and initialize dimension.
  auto d0 = tiledb::Dimension::create<unsigned int>(
      ctx, "d0", {{domain_lo, domain_hi}}, 2);
  auto d1 = tiledb::Dimension::create<unsigned int>(
      ctx, "d1", {{domain_lo, domain_hi}}, 4);
  auto d2 = tiledb::Dimension::create<unsigned int>(
      ctx, "d2", {{domain_lo, domain_hi}}, 50);
  auto d3 = tiledb::Dimension::create<unsigned int>(
      ctx, "d3", {{domain_lo, domain_hi}}, 200);
  auto d4 = tiledb::Dimension::create<unsigned int>(
      ctx, "d4", {{domain_lo, domain_hi}}, 2);
  auto d5 = tiledb::Dimension::create<unsigned int>(
      ctx, "d5", {{domain_lo, domain_hi}}, 2);

  domain.add_dimensions(d0, d1, d2, d3, d4, d5);

  auto a0 = tiledb::Attribute::create<double>(ctx, "a0").set_cell_val_num(
      TILEDB_VAR_NUM);
  auto a1 = tiledb::Attribute::create<double>(ctx, "a1").set_cell_val_num(
      TILEDB_VAR_NUM);
  auto a2 = tiledb::Attribute::create<double>(ctx, "a2").set_cell_val_num(
      TILEDB_VAR_NUM);
  auto a3 = tiledb::Attribute::create<double>(ctx, "a3").set_cell_val_num(
      TILEDB_VAR_NUM);
  auto a4 = tiledb::Attribute::create<double>(ctx, "a4").set_cell_val_num(
      TILEDB_VAR_NUM);
  auto a5 = tiledb::Attribute::create<double>(ctx, "a5").set_cell_val_num(
      TILEDB_VAR_NUM);
  auto a6 = tiledb::Attribute::create<double>(ctx, "a6").set_cell_val_num(
      TILEDB_VAR_NUM);
  auto a7 = tiledb::Attribute::create<double>(ctx, "a7").set_cell_val_num(
      TILEDB_VAR_NUM);
  auto a8 = tiledb::Attribute::create<double>(ctx, "a8").set_cell_val_num(
      TILEDB_VAR_NUM);
  auto a9 = tiledb::Attribute::create<double>(ctx, "a9").set_cell_val_num(
      TILEDB_VAR_NUM);
  auto a10 = tiledb::Attribute::create<unsigned int>(ctx, "a10")
                 .set_cell_val_num(TILEDB_VAR_NUM);
  auto a11 = tiledb::Attribute::create<unsigned int>(ctx, "a11")
                 .set_cell_val_num(TILEDB_VAR_NUM);
  auto a12 = tiledb::Attribute::create<unsigned int>(ctx, "a12")
                 .set_cell_val_num(TILEDB_VAR_NUM);
  auto a13 = tiledb::Attribute::create<float>(ctx, "a13")
                 .set_cell_val_num(TILEDB_VAR_NUM);
  auto a14 = tiledb::Attribute::create<unsigned int>(ctx, "a14")
                 .set_cell_val_num(TILEDB_VAR_NUM);

  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attributes(
      a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14);
  schema.set_capacity(10000000);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  CHECK_NOTHROW(tiledb::Array::create(array_name, schema));

  // Perform Write
  std::vector<unsigned int> d0_data(1, 0);
  std::vector<unsigned int> d1_data(1, 0);
  std::vector<unsigned int> d2_data(1, 0);
  std::vector<unsigned int> d3_data(1, 0);
  std::vector<unsigned int> d4_data(1, 0);
  std::vector<unsigned int> d5_data(1, 0);
  std::vector<double> a0_data(1, 0);
  std::vector<uint64_t> a0_offsets(1, 0);
  std::vector<double> a1_data(1, 0);
  std::vector<uint64_t> a1_offsets(1, 0);
  std::vector<double> a2_data(1, 0);
  std::vector<uint64_t> a2_offsets(1, 0);
  std::vector<double> a3_data(1, 0);
  std::vector<uint64_t> a3_offsets(1, 0);
  std::vector<double> a4_data(1, 0);
  std::vector<uint64_t> a4_offsets(1, 0);
  std::vector<double> a5_data(1, 0);
  std::vector<uint64_t> a5_offsets(1, 0);
  std::vector<double> a6_data(1, 0);
  std::vector<uint64_t> a6_offsets(1, 0);
  std::vector<double> a7_data(1, 0);
  std::vector<uint64_t> a7_offsets(1, 0);
  std::vector<double> a8_data(1, 0);
  std::vector<uint64_t> a8_offsets(1, 0);
  std::vector<double> a9_data(1, 0);
  std::vector<uint64_t> a9_offsets(1, 0);
  std::vector<unsigned int> a10_data(1, 0);
  std::vector<uint64_t> a10_offsets(1, 0);
  std::vector<unsigned int> a11_data(1, 0);
  std::vector<uint64_t> a11_offsets(1, 0);
  std::vector<unsigned int> a12_data(1, 0);
  std::vector<uint64_t> a12_offsets(1, 0);
  std::vector<float> a13_data(1, 0);
  std::vector<uint64_t> a13_offsets(1, 0);
  std::vector<unsigned int> a14_data(1, 0);
  std::vector<uint64_t> a14_offsets(1, 0);

  uint8_t fragments_to_create = 196;
  tiledb::Array array(ctx, array_name, TILEDB_WRITE);
  for (uint8_t i = 0; i < fragments_to_create; i++) {
    d0_data[0] = i;
    d1_data[0] = i;
    d2_data[0] = i;
    d3_data[0] = i;
    d4_data[0] = i;
    d5_data[0] = i;
    a0_data[0] = i;
    a1_data[0] = i;
    a2_data[0] = i;
    a3_data[0] = i;
    a4_data[0] = i;
    a5_data[0] = i;
    a6_data[0] = i;
    a7_data[0] = i;
    a8_data[0] = i;
    a9_data[0] = i;
    a10_data[0] = i;
    a11_data[0] = i;
    a12_data[0] = i;
    a13_data[0] = i;
    a14_data[0] = i;

    tiledb::Query query(ctx, array);
    query.set_data_buffer("d0", d0_data);
    query.set_data_buffer("d1", d1_data);
    query.set_data_buffer("d2", d2_data);
    query.set_data_buffer("d3", d3_data);
    query.set_data_buffer("d4", d4_data);
    query.set_data_buffer("d5", d5_data);

    query.set_data_buffer("a0", a0_data).set_offsets_buffer("a0", a0_offsets);
    query.set_data_buffer("a1", a1_data).set_offsets_buffer("a1", a1_offsets);
    query.set_data_buffer("a2", a2_data).set_offsets_buffer("a2", a2_offsets);
    query.set_data_buffer("a3", a3_data).set_offsets_buffer("a3", a3_offsets);
    query.set_data_buffer("a4", a4_data).set_offsets_buffer("a4", a4_offsets);
    query.set_data_buffer("a5", a5_data).set_offsets_buffer("a5", a5_offsets);
    query.set_data_buffer("a6", a6_data).set_offsets_buffer("a6", a6_offsets);
    query.set_data_buffer("a7", a7_data).set_offsets_buffer("a7", a7_offsets);
    query.set_data_buffer("a8", a8_data).set_offsets_buffer("a8", a8_offsets);
    query.set_data_buffer("a9", a9_data).set_offsets_buffer("a9", a9_offsets);
    query.set_data_buffer("a10", a10_data)
        .set_offsets_buffer("a10", a10_offsets);
    query.set_data_buffer("a11", a11_data)
        .set_offsets_buffer("a11", a11_offsets);
    query.set_data_buffer("a12", a12_data)
        .set_offsets_buffer("a12", a12_offsets);
    query.set_data_buffer("a13", a13_data)
        .set_offsets_buffer("a13", a13_offsets);
    query.set_data_buffer("a14", a14_data)
        .set_offsets_buffer("a14", a14_offsets);

    query.submit();
  }

  // Consolidate
  tiledb::Stats::enable();
  tiledb::Array::consolidate(ctx, array_name);
  tiledb::Stats::dump();

  // Vacuum
  tiledb::Array::vacuum(ctx, array_name);

  // Cleanup.
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}
