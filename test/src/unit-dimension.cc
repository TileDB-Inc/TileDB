/**
 * @file unit-dimension.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB, Inc.
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
 * Tests the `Dimension` class.
 */

#include "test/support/src/helpers-dimension.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/hilbert.h"

#include <test/support/tdb_catch.h>
#include <iostream>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::test;

TEST_CASE(
    "Dimension: Test map_to_uint64, integers",
    "[dimension][map_to_uint64][int]") {
  // Create dimensions
  auto memory_tracker = get_test_memory_tracker();
  Dimension d1("d1", Datatype::INT32, memory_tracker);
  int32_t dom1[] = {0, 100};
  d1.set_domain(dom1);
  Dimension d2("d2", Datatype::INT32, memory_tracker);
  int32_t dom2[] = {0, 200};
  d2.set_domain(dom2);

  // Create 2D hilbert curve (auxiliary here)
  Hilbert h(2);
  auto bits = h.bits();
  uint64_t coords[2];
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  // Map (1, 1)
  int32_t dv = 1;
  auto v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 21474836);
  coords[0] = v;
  dv = 1;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 10737418);
  coords[1] = v;
  auto hv = h.coords_to_hilbert(coords);
  CHECK(hv == 972175364522868);

  // Map (1, 3)
  dv = 1;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 21474836);
  coords[0] = v;
  dv = 3;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 32212254);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 673795214387276);

  // Map (4, 2)
  dv = 4;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 85899345);
  coords[0] = v;
  dv = 2;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 21474836);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 15960414315352633);

  // Map (5, 4)
  dv = 5;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 107374182);
  coords[0] = v;
  dv = 4;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 42949672);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 14307296941447292);

  // Map (2, 1)
  dv = 2;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 42949672);
  coords[0] = v;
  dv = 1;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 10737418);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 1282377960629798);

  // Map (2, 2)
  dv = 2;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 42949672);
  coords[0] = v;
  dv = 2;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 21474836);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 2093929125029754);

  // Map (3, 7)
  dv = 3;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 64424509);
  coords[0] = v;
  dv = 7;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 75161927);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 8953131325824998);

  // Map (7, 7)
  dv = 7;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 150323855);
  coords[0] = v;
  dv = 7;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 75161927);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 34410827116042986);
}

TEST_CASE(
    "Dimension: Test map_to_uint64, int32, negative",
    "[dimension][map_to_uint64][int32][negative]") {
  // Create dimensions
  auto memory_tracker = get_test_memory_tracker();
  Dimension d1("d1", Datatype::INT32, memory_tracker);
  int32_t dom1[] = {-50, 50};
  d1.set_domain(dom1);
  Dimension d2("d2", Datatype::INT32, memory_tracker);
  int32_t dom2[] = {-100, 100};
  d2.set_domain(dom2);

  // Create 2D hilbert curve (auxiliary here)
  Hilbert h(2);
  auto bits = h.bits();
  uint64_t coords[2];
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  // Map (-49, -99)
  int32_t dv = -49;
  auto v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 21474836);
  coords[0] = v;
  dv = -99;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 10737418);
  coords[1] = v;
  auto hv = h.coords_to_hilbert(coords);
  CHECK(hv == 972175364522868);

  // Map (-49, -97)
  dv = -49;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 21474836);
  coords[0] = v;
  dv = -97;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 32212254);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 673795214387276);

  // Map (-46, -98)
  dv = -46;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 85899345);
  coords[0] = v;
  dv = -98;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 21474836);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 15960414315352633);

  // Map (-45, -96)
  dv = -45;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 107374182);
  coords[0] = v;
  dv = -96;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 42949672);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 14307296941447292);

  // Map (-48, -99)
  dv = -48;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 42949672);
  coords[0] = v;
  dv = -99;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 10737418);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 1282377960629798);

  // Map (-48, -98)
  dv = -48;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 42949672);
  coords[0] = v;
  dv = -98;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 21474836);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 2093929125029754);

  // Map (-47, -93)
  dv = -47;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 64424509);
  coords[0] = v;
  dv = -93;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 75161927);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 8953131325824998);

  // Map (-43, -93)
  dv = -43;
  v = d1.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 150323855);
  coords[0] = v;
  dv = -93;
  v = d2.map_to_uint64(&dv, sizeof(int32_t), bits, max_bucket_val);
  CHECK(v == 75161927);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 34410827116042986);
}

TEST_CASE(
    "Dimension: Test map_to_uint64, float32",
    "[dimension][map_to_uint64][float32]") {
  // Create dimensions
  auto memory_tracker = get_test_memory_tracker();
  Dimension d1("d1", Datatype::FLOAT32, memory_tracker);
  float dom1[] = {0.0f, 1.0f};
  d1.set_domain(dom1);
  Dimension d2("d2", Datatype::FLOAT32, memory_tracker);
  float dom2[] = {0.0f, 2.0f};
  d2.set_domain(dom2);

  // Create 2D hilbert curve (auxiliary here)
  Hilbert h(2);
  auto bits = h.bits();
  uint64_t coords[2];
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  // Map (0.1f, 0.3f)
  float dv = 0.1f;
  auto v = d1.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 214748367);
  coords[0] = v;
  dv = 0.3f;
  v = d2.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 322122559);
  coords[1] = v;
  auto hv = h.coords_to_hilbert(coords);
  CHECK(hv == 141289400074368426);

  // Map (0.1f, 0.1f)
  dv = 0.1f;
  v = d1.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 214748367);
  coords[0] = v;
  dv = 0.1f;
  v = d2.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 107374183);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 31040194354799722);

  // Map (0.5f, 0.4f)
  dv = 0.5f;
  v = d1.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 1073741823);
  coords[0] = v;
  dv = 0.4f;
  v = d2.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 429496735);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 474732384249878186);

  // Map (0.4f, 0.2f)
  dv = 0.4f;
  v = d1.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 858993471);
  coords[0] = v;
  dv = 0.2f;
  v = d2.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 214748367);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 429519776226080170);

  // Map (0.2f, 0.1f)
  dv = 0.2f;
  v = d1.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 429496735);
  coords[0] = v;
  dv = 0.1f;
  v = d2.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 107374183);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 276927224145762282);

  // Map (0.2f, 0.2f)
  dv = 0.2f;
  v = d1.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 429496735);
  coords[0] = v;
  dv = 0.2f;
  v = d2.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 214748367);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 230584300921369344);

  // Map (0.3f, 0.7f)
  dv = 0.3f;
  v = d1.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 644245119);
  coords[0] = v;
  dv = 0.7f;
  v = d2.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 751619263);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 607500946658220714);

  // Map (0.7f, 0.7f)
  dv = 0.7f;
  v = d1.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 1503238527);
  coords[0] = v;
  dv = 0.7f;
  v = d2.map_to_uint64(&dv, sizeof(float), bits, max_bucket_val);
  CHECK(v == 751619263);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 4004185071769213610);

  // (0.1f, 0.1f) ->   31040194354799722
  // (0.1f, 0.3f) ->  141289400074368426
  // (0.2f, 0.2f) ->  230584300921369344
  // (0.2f, 0.1f) ->  276927224145762282
  // (0.4f, 0.2f) ->  429519776226080170
  // (0.5f, 0.4f) ->  474732384249878186
  // (0.3f, 0.7f) ->  607500946658220714
  // (0.7f, 0.7f) -> 4004185071769213610
}

TEST_CASE(
    "Dimension: Test map_to_uint64, string",
    "[dimension][map_to_uint64][string]") {
  // Create dimensions
  auto memory_tracker = get_test_memory_tracker();
  Dimension d1("d1", Datatype::STRING_ASCII, memory_tracker);
  Dimension d2("d2", Datatype::STRING_ASCII, memory_tracker);

  // Create 2D hilbert curve (auxiliary here)
  Hilbert h(2);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;
  uint64_t coords[2];

  // Map (1a, cat)
  std::string dv = "1a";
  auto v = d1.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 414220288);
  coords[0] = v;
  dv = "cat";
  v = d2.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 833665536);
  coords[1] = v;
  auto hv = h.coords_to_hilbert(coords);
  CHECK(hv == 919167533801450154);

  // Map (dog, stop)
  dv = "dog";
  v = d1.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 842511232);
  coords[0] = v;
  dv = "stop";
  v = d2.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 968505272);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 785843883856635242);

  // Map (camel, stock)
  dv = "camel";
  v = d1.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 833664690);
  coords[0] = v;
  dv = "stock";
  v = d2.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 968505265);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 785914162406170797);

  // Map (33, t1)
  dv = "33";
  v = d1.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 429490176);
  coords[0] = v;
  dv = "t1";
  v = d2.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 974684160);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 877430626372812800);

  // Map (blue, ac3)
  dv = "blue";
  v = d1.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 825637554);
  coords[0] = v;
  dv = "ace";
  v = d2.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 816951936);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 721526731798250756);

  // Map (az, yellow)
  dv = "az";
  v = d1.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 817692672);
  coords[0] = v;
  dv = "yellow";
  v = d2.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 1018345014);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 788282729955763606);

  // Map (star, red)
  dv = "star";
  v = d1.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 968503481);
  coords[0] = v;
  dv = "red";
  v = d2.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 959623680);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 757250025264009195);

  // Map (urn, grey)
  dv = "urn";
  v = d1.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 985216768);
  coords[0] = v;
  dv = "grey";
  v = d2.map_to_uint64(dv.data(), dv.size(), bits, max_bucket_val);
  CHECK(v == 867775164);
  coords[1] = v;
  hv = h.coords_to_hilbert(coords);
  CHECK(hv == 741275904800572752);

  // (blue, ace)    ->     721526731798250756
  // (urn, grey)    ->     741275904800572752
  // (star, red)    ->     757250025264009195
  // (dog, stop)    ->     785843883856635242
  // (camel, stock) ->     785914162406170797
  // (az, yellow)   ->     788282729955763606
  // (33, t1)       ->     877430626372812800
  // (1a, cat)      ->     919167533801450154
}

TEST_CASE(
    "Dimension: Test map_from_uint64, int32",
    "[dimension][map_from_uint64][int32]") {
  // Create dimensions
  auto memory_tracker = get_test_memory_tracker();
  Dimension d1("d1", Datatype::INT32, memory_tracker);
  int32_t dom1[] = {0, 100};
  d1.set_domain(dom1);

  // Set number of buckets
  Hilbert h(2);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  auto val = d1.map_from_uint64(64424509, bits, max_bucket_val);
  auto val_int32 = *(const int32_t*)(val.data());
  CHECK(val_int32 == 3);
  val = d1.map_from_uint64(42949672, bits, max_bucket_val);
  val_int32 = *(const int32_t*)(val.data());
  CHECK(val_int32 == 2);
}

TEST_CASE(
    "Dimension: Test map_from_uint64, int32, negative",
    "[dimension][map_from_uint64][int32][negative]") {
  // Create dimensions
  auto memory_tracker = get_test_memory_tracker();
  Dimension d1("d1", Datatype::INT32, memory_tracker);
  int32_t dom1[] = {-50, 50};
  d1.set_domain(dom1);

  // Set number of buckets
  Hilbert h(2);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  auto val = d1.map_from_uint64(64424509, bits, max_bucket_val);
  auto val_int32 = *(const int32_t*)(val.data());
  CHECK(val_int32 == -47);
  val = d1.map_from_uint64(42949672, bits, max_bucket_val);
  val_int32 = *(const int32_t*)(val.data());
  CHECK(val_int32 == -48);
}

TEST_CASE(
    "Dimension: Test map_from_uint64, float32",
    "[dimension][map_from_uint64][float32]") {
  // Create dimensions
  auto memory_tracker = get_test_memory_tracker();
  Dimension d1("d1", Datatype::FLOAT32, memory_tracker);
  float dom1[] = {0.0f, 1.0f};
  d1.set_domain(dom1);

  // Set number of buckets
  Hilbert h(2);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  auto val = d1.map_from_uint64(1503238527, bits, max_bucket_val);
  auto val_int32 = *(const float*)(val.data());
  CHECK(round(100 * val_int32) == 70);
  val = d1.map_from_uint64(429496735, bits, max_bucket_val);
  val_int32 = *(const float*)(val.data());
  CHECK(round(100 * val_int32) == 20);
}

TEST_CASE(
    "Dimension: Test map_from_uint64, string",
    "[dimension][map_from_uint64][string]") {
  // Create dimensions
  auto memory_tracker = get_test_memory_tracker();
  Dimension d1("d1", Datatype::STRING_ASCII, memory_tracker);

  // Set number of buckets
  Hilbert h(2);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  std::string v_str = std::string("star\0\0\0\0", 8);
  auto v = d1.map_to_uint64(v_str.data(), v_str.size(), bits, max_bucket_val);
  auto val = d1.map_from_uint64(v, bits, max_bucket_val);
  auto val_str = std::string((const char*)(val.data()), 8);
  CHECK(val_str == v_str);

  v_str = std::string("blue\0\0\0\0", 8);
  v = d1.map_to_uint64(v_str.data(), v_str.size(), bits, max_bucket_val);
  val = d1.map_from_uint64(v, bits, max_bucket_val);
  val_str = std::string((const char*)(val.data()), 4);
  CHECK(val_str == std::string("blud", 4));

  v_str = std::string("yellow\0\0", 8);
  v = d1.map_to_uint64(v_str.data(), v_str.size(), bits, max_bucket_val);
  val = d1.map_from_uint64(v, bits, max_bucket_val);
  val_str = std::string((const char*)(val.data()), 4);
  CHECK(val_str == std::string("yell", 4));
}

template <class T>
double basic_verify_overlap_ratio(
    T range1_low, T range1_high, T range2_low, T range2_high) {
  auto r1 = TypedRange<T>(range1_low, range1_high);
  auto r2 = TypedRange<T>(range2_low, range2_high);
  Dimension d("foo", RangeTraits<T>::datatype, get_test_memory_tracker());
  auto ratio = d.overlap_ratio(r1, r2);
  CHECK(0.0 <= ratio);
  CHECK(ratio <= 1.0);
  return ratio;
}

/**
 * The denominator of the ratio is computed as range2_high - range2_low. For a
 * k-bit signed integer, the largest this value can take is 2^k-1, which is
 * larger than the maximum signed value of 2^(k-1)-1.
 */
TEST_CASE(
    "Range<int32_t>.overlap_ratio: denominator overflow",
    "[dimension][overlap_ratio][signed][integral]") {
  typedef int32_t T;
  auto min = std::numeric_limits<T>::min();
  auto max = std::numeric_limits<T>::max();
  basic_verify_overlap_ratio<T>(min / 3, 1, min + 1, max);
}

/**
 * Denominator overflow for an unsigned integral type.
 */
TEST_CASE(
    "Range<uint32_t>.overlap_ratio: denominator overflow",
    "[dimension][overlap_ratio][unsigned][integral]") {
  typedef uint32_t T;
  auto min = std::numeric_limits<T>::min();
  auto max = std::numeric_limits<T>::max();
  basic_verify_overlap_ratio<T>(0, 1, min + 1, max);
}

/**
 * Denominator overflow for double.
 */
TEST_CASE(
    "Range<double>.overlap_ratio: denominator overflow",
    "[dimension][overlap_ratio][signed][floating]") {
  typedef double T;
  auto min = std::numeric_limits<T>::min();
  auto max = std::numeric_limits<T>::max();
  basic_verify_overlap_ratio<T>(0, 1, min + 1, max);
}

TEST_CASE(
    "Range<uint32_t>.overlap_ratio: max base range",
    "[dimension][overlap_ratio][unsigned][integral]") {
  typedef uint32_t T;
  auto min = std::numeric_limits<T>::min();
  auto max = std::numeric_limits<T>::max();
  basic_verify_overlap_ratio<T>(min, min + 1, min, max);
}

TEST_CASE(
    "Range<uint32_t>.overlap_ratio: max both ranges",
    "[dimension][overlap_ratio][unsigned][integral]") {
  typedef uint32_t T;
  auto min = std::numeric_limits<T>::min();
  auto max = std::numeric_limits<T>::max();
  basic_verify_overlap_ratio<T>(min, max, min, max);
}

TEST_CASE(
    "Range<int32_t>.overlap_ratio: over-by-1 legit-max base range",
    "[dimension][overlap_ratio][signed][integral]") {
  typedef int32_t T;
  auto max = std::numeric_limits<T>::max();
  basic_verify_overlap_ratio<T>(0, 1, -1, max);
}

TEST_CASE(
    "Range<int32_t>.overlap_ratio: legit-max base range",
    "[dimension][overlap_ratio][signed][integral]") {
  typedef int32_t T;
  auto max = std::numeric_limits<T>::max();
  basic_verify_overlap_ratio<T>(0, 1, -2, max - 2);
}
