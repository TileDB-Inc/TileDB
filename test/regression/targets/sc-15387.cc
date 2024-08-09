#include <tiledb/tiledb>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <chrono>
#include <thread>

using namespace tiledb;

namespace {
void create_array(Context& ctx, std::string uri, bool use_two_dims) {
  auto dim0 =
      Dimension::create(ctx, "__dim_0", TILEDB_STRING_ASCII, nullptr, nullptr);
  tiledb::FilterList dim0_filters{ctx};
  dim0_filters.add_filter({ctx, TILEDB_FILTER_RLE});
  dim0.set_filter_list(dim0_filters);

  auto dim1 =
      Dimension::create(ctx, "__dim_1", TILEDB_STRING_ASCII, nullptr, nullptr);

  tiledb::Filter dim1_filter(ctx, TILEDB_FILTER_ZSTD);
  int level{22};
  dim1_filter.set_option(TILEDB_COMPRESSION_LEVEL, &level);
  tiledb::FilterList dim1_filter_list{ctx};
  dim1_filter_list.add_filter(dim1_filter);
  dim1.set_filter_list(dim1_filter_list);

  tiledb::FilterList attr_filter_list{ctx};
  attr_filter_list.add_filter({ctx, TILEDB_FILTER_ZSTD});

  Domain domain{ctx};
  domain.add_dimension(dim0);
  if (use_two_dims) {
    domain.add_dimension(dim1);
  }

  Attribute attr{ctx, "value", TILEDB_FLOAT32, attr_filter_list};

  tiledb::FilterList offsets_filters{ctx};
  offsets_filters.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA})
      .add_filter({ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION})
      .add_filter({ctx, TILEDB_FILTER_ZSTD});

  ArraySchema schema{ctx, TILEDB_SPARSE};
  schema.set_allows_dups(true);
  schema.set_capacity(100000);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_COL_MAJOR);
  schema.set_domain(domain);
  schema.add_attribute(attr);

  Array::create(uri, schema);
}

void write_array(Context& ctx, std::string uri, bool use_two_dims) {
  {
    std::vector<char> d0_data{'a', 'b', 'c'};
    std::vector<uint64_t> d0_offsets{0, 1, 2};
    std::vector<char> d1_data{'s', 't', 'u'};
    std::vector<uint64_t> d1_offsets{0, 1, 2};
    std::vector<float> data{4, 5, 6};

    Array array{ctx, uri, TILEDB_WRITE};
    Query query{ctx, array, TILEDB_WRITE};

    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("__dim_0", d0_data)
        .set_offsets_buffer("__dim_0", d0_offsets)
        .set_data_buffer("value", data);

    if (use_two_dims) {
      query.set_data_buffer("__dim_1", d1_data)
          .set_offsets_buffer("__dim_1", d1_offsets);
    }

    query.submit();
    query.finalize();

    REQUIRE(query.query_status() == Query::Status::COMPLETE);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  {
    std::vector<char> d0_data{'d', 'e', 'f'};
    std::vector<uint64_t> d0_offsets{0, 1, 2};
    std::vector<char> d1_data{'v', 'w', 'x'};
    std::vector<uint64_t> d1_offsets{0, 1, 2};
    std::vector<float> data{4, 5, 6};

    Array array{ctx, uri, TILEDB_WRITE};
    Query query{ctx, array, TILEDB_WRITE};

    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("__dim_0", d0_data)
        .set_offsets_buffer("__dim_0", d0_offsets)
        .set_data_buffer("value", data);

    if (use_two_dims) {
      query.set_data_buffer("__dim_1", d1_data)
          .set_offsets_buffer("__dim_1", d1_offsets);
    }

    query.submit();
    query.finalize();

    REQUIRE(query.query_status() == Query::Status::COMPLETE);
  }
}

};  // anonymous namespace

TEST_CASE("SC-15387") {
  Context ctx;

  std::string uri{"foo1"};

  auto object = tiledb::Object::object(ctx, uri);
  if (object.type() == tiledb::Object::Type::Array)
    tiledb::Object::remove(ctx, uri);

  auto use_two_dims = GENERATE(false, true);

  create_array(ctx, uri, use_two_dims);

  write_array(ctx, uri, use_two_dims);

  // use_two_dims
  // - false: segfaults if pre-increment used in comparators.h
  // - true: expected to segfault before fix
  tiledb::Array::consolidate(ctx, uri);

  // tiledb::Object::remove(ctx, uri);
};
