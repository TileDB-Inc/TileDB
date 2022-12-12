#include <string>
#include <limits>

#include <tiledb/tiledb>

#include <test/support/tdb_catch.h>

std::string array_name = "cpp_unit_array_24079";

TEST_CASE("C++ API: DoubleDelta filter typecheck should account for output type of FloatScaleFilter", "[cppapi][filter][float-scaling]") {
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
        ctx, "soma_joinid", {{domain_lo, domain_hi}}, 2048);
    
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

    auto a = tiledb::Attribute::create<float>(ctx, "A");

    tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.add_attribute(a);
    schema.set_capacity(100000);
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_ROW_MAJOR);
    CHECK_NOTHROW(tiledb::Array::create(array_name, schema));

    // Cleanup.
    if (vfs.is_dir(array_name)) {
        vfs.remove_dir(array_name);
    }

}