/**
 * @file tiledb/sm/array_schema/test/unit_dimension_label.cc
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
 * This file tests the DimensionLabel object
 */

#include <tdb_catch.h>
#include "tiledb/sm/array_schema/dimension_label.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/data_order.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/storage_format/serialization/serializers.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::type;

TEST_CASE(
    "Roundtrip dimension label serialization",
    "[dimension_label][serialize][deserialize]") {
  const uint32_t version{14};
  DimensionLabel::dimension_size_type dim_id{0};
  std::string name{"label0"};
  std::string label_attr_name{"label"};
  URI uri{"label/l0", false};
  DataOrder label_order{DataOrder::INCREASING_DATA};
  bool is_external{true};
  bool is_relative{true};
  DimensionLabel label{
      dim_id,
      name,
      URI("label/l0", false),
      label_attr_name,
      label_order,
      Datatype::FLOAT64,
      1,

      nullptr,
      is_external,
      is_relative};
  SizeComputationSerializer size_computation_serializer;
  label.serialize(size_computation_serializer, version);

  std::vector<uint8_t> data(size_computation_serializer.size());
  Serializer serializer(data.data(), data.size());
  label.serialize(serializer, version);

  Deserializer deserializer(data.data(), data.size());
  auto label2 = DimensionLabel::deserialize(deserializer, version);
  CHECK(dim_id == label2->dimension_index());
  CHECK(name == label2->name());
  CHECK(label2->label_type() == Datatype::FLOAT64);
  CHECK(label2->label_cell_val_num() == 1);
  CHECK(label2->is_external() == is_external);
  CHECK(label2->uri().to_string() == uri.to_string());
}
