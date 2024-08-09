#include <chrono>
#include <climits>
#include <thread>

#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

#include <catch2/catch_test_macros.hpp>

using namespace tiledb;

static void create_array(const std::string& array_uri);
static void write_array(const std::string& array_uri);
static void read_array(const std::string& array_uri);

TEST_CASE(
    "Properly sort data in Hilbert order when all cells are in the same bucket",
    "[hilbert][bug][sc44758]") {
  std::string array_uri = "test_hilbert_order";

  // Test setup
  create_array(array_uri);
  write_array(array_uri);
  read_array(array_uri);
}

void create_array(const std::string& array_uri) {
  Context ctx;

  auto obj = Object::object(ctx, array_uri);
  if (obj.type() != Object::Type::Invalid) {
    Object::remove(ctx, array_uri);
  }

  auto X = Dimension::create<float>(
      ctx,
      "x",
      {{std::numeric_limits<float>::lowest(),
        std::numeric_limits<float>::max()}});
  auto Y = Dimension::create<float>(
      ctx,
      "y",
      {{std::numeric_limits<float>::lowest(),
        std::numeric_limits<float>::max()}});

  Domain dom(ctx);
  dom.add_dimension(X).add_dimension(Y);

  auto attr = Attribute::create<int32_t>(ctx, "a");

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_cell_order(TILEDB_HILBERT).set_domain(dom).add_attribute(attr);

  Array::create(array_uri, schema);
}

void write_fragment(
    Context& ctx,
    Array& array,
    std::vector<float> x,
    std::vector<float> y,
    std::vector<int32_t> a) {
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("x", x)
      .set_data_buffer("y", y)
      .set_data_buffer("a", a);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
}

void write_array(const std::string& array_uri) {
  Context ctx;
  Array array(ctx, array_uri, TILEDB_WRITE);
  write_fragment(
      ctx,
      array,
      {50913.5209f, 46300.4576f, 53750.3951f, 47779.8514f, 45815.4787f,
       45738.904f,  47445.7143f, 51352.0412f, 49088.727f,  52722.6237f,
       48501.783f,  53915.4312f, 50512.0801f, 45781.8652f, 53743.3637f,
       51288.4185f, 54457.4034f, 52333.0674f, 50988.1421f, 49246.9677f,
       53489.8377f, 49678.9367f, 50262.7812f, 45269.6639f, 54301.9674f},
      {6119.8819f,  2227.1279f,  4709.1357f,  -6009.2908f, -3196.8194f,
       3999.3447f,  -956.7883f,  -9022.1859f, 7735.0127f,  2641.4245f,
       -3325.7246f, -4835.4291f, 1449.9719f,  -5958.2026f, 7479.1415f,
       -4966.7886f, 8656.5012f,  -690.8002f,  1651.4824f,  -9181.8585f,
       -1045.1637f, -8038.3517f, -7083.2645f, -7555.8585f, -3279.0184f},
      {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
       13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24});
  write_fragment(
      ctx,
      array,
      {48932.8545f, 53999.9728f, 52448.9716f, 53026.5806f, 53609.8738f,
       49870.8329f, 53261.7657f, 54868.0211f, 50919.4791f, 51548.2142f,
       46907.8445f, 45835.1908f, 53411.073f,  52597.0232f, 47379.0257f,
       50703.926f,  47457.7695f, 54561.2923f, 49672.1336f, 48719.4054f,
       51188.1191f, 52083.7624f, 51569.5062f, 52931.5174f, 51622.6334f},
      {-2084.0598f, 780.3959f,   -5696.0102f, 7110.3894f,  2958.4756f,
       -8536.3301f, -2389.5892f, 5234.3587f,  321.5067f,   7850.7334f,
       -265.8565f,  9017.0814f,  -737.5592f,  1569.3621f,  4444.4227f,
       -4509.9735f, -7676.8195f, -3205.2129f, -370.9372f,  5879.6844f,
       4343.399f,   -5246.6839f, 9784.3999f,  -7532.3645f, -7613.6955f},
      {25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
       38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49});
  write_fragment(
      ctx,
      array,
      {51088.8635f, 50685.8091f, 54907.3208f, 53226.0392f, 49276.2669f,
       48473.3678f, 46088.6933f, 49581.7425f, 45380.7934f, 47440.2517f,
       48541.5523f, 46043.6958f, 45821.4628f, 54135.571f,  46101.602f,
       46876.8079f, 47082.2505f, 46077.7971f, 48246.9454f, 50715.8986f,
       46061.9485f, 54009.0435f, 46262.2024f, 46478.2223f, 51952.6307f},
      {9111.9753f,  -8600.7575f, -9750.4502f, -1009.7165f, -2659.2155f,
       8411.8389f,  1178.1284f,  -4547.992f,  2341.4306f,  7600.4032f,
       -4077.5538f, 5656.9615f,  35.4158f,    -9610.731f,  -8035.895f,
       2742.678f,   6426.1031f,  9734.5399f,  -3222.952f,  -4063.2662f,
       -6085.3865f, 2549.7113f,  1882.7361f,  7581.7167f,  -5296.0846f},
      {50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62,
       63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74});
  array.close();
}

void read_array(const std::string& array_uri) {
  Context ctx;
  Array array(ctx, array_uri, TILEDB_READ);

  std::vector<float> x(10000000);
  std::vector<float> y(10000000);
  std::vector<int32_t> a(10000000);
  Query query(ctx, array, TILEDB_READ);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("x", x)
      .set_data_buffer("y", y)
      .set_data_buffer("a", a);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
}
