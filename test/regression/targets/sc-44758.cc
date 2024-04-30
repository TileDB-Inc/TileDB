#include <chrono>
#include <climits>
#include <thread>

#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

#include <test/support/tdb_catch.h>

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
      {50913.5209, 46300.4576, 53750.3951, 47779.8514, 45815.4787,
       45738.904,  47445.7143, 51352.0412, 49088.727,  52722.6237,
       48501.783,  53915.4312, 50512.0801, 45781.8652, 53743.3637,
       51288.4185, 54457.4034, 52333.0674, 50988.1421, 49246.9677,
       53489.8377, 49678.9367, 50262.7812, 45269.6639, 54301.9674},
      {6119.8819,  2227.1279,  4709.1357,  -6009.2908, -3196.8194,
       3999.3447,  -956.7883,  -9022.1859, 7735.0127,  2641.4245,
       -3325.7246, -4835.4291, 1449.9719,  -5958.2026, 7479.1415,
       -4966.7886, 8656.5012,  -690.8002,  1651.4824,  -9181.8585,
       -1045.1637, -8038.3517, -7083.2645, -7555.8585, -3279.0184},
      {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
       13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24});
  write_fragment(
      ctx,
      array,
      {48932.8545, 53999.9728, 52448.9716, 53026.5806, 53609.8738,
       49870.8329, 53261.7657, 54868.0211, 50919.4791, 51548.2142,
       46907.8445, 45835.1908, 53411.073,  52597.0232, 47379.0257,
       50703.926,  47457.7695, 54561.2923, 49672.1336, 48719.4054,
       51188.1191, 52083.7624, 51569.5062, 52931.5174, 51622.6334},
      {-2084.0598, 780.3959,   -5696.0102, 7110.3894,  2958.4756,
       -8536.3301, -2389.5892, 5234.3587,  321.5067,   7850.7334,
       -265.8565,  9017.0814,  -737.5592,  1569.3621,  4444.4227,
       -4509.9735, -7676.8195, -3205.2129, -370.9372,  5879.6844,
       4343.399,   -5246.6839, 9784.3999,  -7532.3645, -7613.6955},
      {25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
       38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49});
  write_fragment(
      ctx,
      array,
      {51088.8635, 50685.8091, 54907.3208, 53226.0392, 49276.2669,
       48473.3678, 46088.6933, 49581.7425, 45380.7934, 47440.2517,
       48541.5523, 46043.6958, 45821.4628, 54135.571,  46101.602,
       46876.8079, 47082.2505, 46077.7971, 48246.9454, 50715.8986,
       46061.9485, 54009.0435, 46262.2024, 46478.2223, 51952.6307},
      {9111.9753,  -8600.7575, -9750.4502, -1009.7165, -2659.2155,
       8411.8389,  1178.1284,  -4547.992,  2341.4306,  7600.4032,
       -4077.5538, 5656.9615,  35.4158,    -9610.731,  -8035.895,
       2742.678,   6426.1031,  9734.5399,  -3222.952,  -4063.2662,
       -6085.3865, 2549.7113,  1882.7361,  7581.7167,  -5296.0846},
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
