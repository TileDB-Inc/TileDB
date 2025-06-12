/**
 * @file tiledb/sm/serialization/test/unit_capnp_utils.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file contains unit tests for the array schema
 */

#include <capnp/message.h>

#include <test/support/tdb_catch.h>
#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/dimension_label.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/data_order.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/serialization/array_schema.h"

using namespace tiledb::common;
using namespace tiledb::sm;

/**
 * Check the two array schemas have equivalent values.
 *
 * Note: For dimensions, attributes, and dimension labels this only checks the
 * name and number of components, no the propoperties set on those components.
 *
 * Note: Does not check filters.
 */
void check_array_schema_is_equivalent(
    ArraySchema* schema1, ArraySchema* schema2) {
  CHECK(schema1->array_uri().to_string() == schema2->array_uri().to_string());
  CHECK(schema1->capacity() == schema2->capacity());
  CHECK(schema1->cell_order() == schema2->cell_order());
  CHECK(schema1->tile_order() == schema2->tile_order());
  CHECK(schema1->dense() == schema2->dense());
  CHECK(schema1->allows_dups() == schema2->allows_dups());
  CHECK(schema1->timestamp_range() == schema2->timestamp_range());

  // Check attribute number and names.
  CHECK(schema1->attribute_num() == schema2->attribute_num());
  auto nattr = schema1->attribute_num();
  for (uint32_t index{0}; index < nattr; ++index) {
    CHECK(
        schema1->attribute(index)->name() == schema2->attribute(index)->name());
  }

  // Check dimension number and names.
  CHECK(schema1->dim_num() == schema2->dim_num());
  auto ndim = schema1->dim_num();
  for (uint32_t index{0}; index < ndim; ++index) {
    CHECK(
        schema1->domain().dimension_ptr(index)->name() ==
        schema2->domain().dimension_ptr(index)->name());
  }

  // Check dimension label number and names.
  CHECK(schema1->dim_label_num() == schema2->dim_label_num());
  auto nlabel = schema1->dim_label_num();
  for (uint32_t index{0}; index < nlabel; ++index) {
    CHECK(
        schema1->dimension_label(index).name() ==
        schema2->dimension_label(index).name());
  }
}

TEST_CASE(
    "Serialize and deserialize dimension label",
    "[DimensionLabel][serialization]") {
  shared_ptr<DimensionLabel> dim_label{nullptr};

  SECTION("Internal dimension label") {
    // Create dimension label array schema.
    auto schema = make_shared<ArraySchema>(
        HERE(), ArrayType::DENSE, tiledb::test::create_test_memory_tracker());
    std::vector<shared_ptr<Dimension>> dims{make_shared<Dimension>(
        HERE(),
        "index",
        Datatype::UINT32,
        tiledb::test::get_test_memory_tracker())};

    uint32_t domain1[2]{1, 64};
    dims[0]->set_domain(&domain1[0]);

    schema->set_domain(make_shared<Domain>(
        HERE(),
        Layout::ROW_MAJOR,
        dims,
        Layout::ROW_MAJOR,
        tiledb::test::get_test_memory_tracker()));
    schema->add_attribute(
        make_shared<Attribute>(HERE(), "label", Datatype::FLOAT64));

    schema->check_without_config();

    // Create dimension label.
    dim_label = make_shared<DimensionLabel>(
        HERE(),
        3,
        "label1",
        URI("__labels/l1"),
        "label",
        DataOrder::INCREASING_DATA,
        Datatype::FLOAT64,
        1,
        schema,
        false,
        true);
  }
  SECTION("Loaded dimension label") {
    // After writing to disk, the dimension label schema is not loaded back into
    // memory.
    // Create dimension label.
    dim_label = make_shared<DimensionLabel>(
        HERE(),
        3,
        "label1",
        URI("__labels/l1"),
        "label",
        DataOrder::INCREASING_DATA,
        Datatype::FLOAT64,
        1,
        nullptr,
        false,
        true);
  }

  // Serialize
  ::capnp::MallocMessageBuilder message;
  tiledb::sm::serialization::capnp::DimensionLabel::Builder builder =
      message.initRoot<tiledb::sm::serialization::capnp::DimensionLabel>();
  tiledb::sm::serialization::dimension_label_to_capnp(
      *dim_label.get(), &builder, true);
  auto dim_label_clone = tiledb::sm::serialization::dimension_label_from_capnp(
      builder, tiledb::test::get_test_memory_tracker());

  // Check dimension label properties and components.
  CHECK(dim_label->has_schema() == dim_label_clone->has_schema());
  if (dim_label->has_schema() && dim_label_clone->has_schema()) {
    check_array_schema_is_equivalent(
        dim_label->schema().get(), dim_label_clone->schema().get());
  }
  CHECK(dim_label->dimension_index() == dim_label_clone->dimension_index());
  CHECK(dim_label->is_external() == dim_label_clone->is_external());
  CHECK(dim_label->is_var() == dim_label_clone->is_var());
  CHECK(dim_label->label_attr_name() == dim_label->label_attr_name());
  CHECK(dim_label->label_cell_val_num() == dim_label->label_cell_val_num());
  CHECK(dim_label->label_order() == dim_label->label_order());
  CHECK(dim_label->label_type() == dim_label->label_type());
  CHECK(dim_label->name() == dim_label->name());
  CHECK(dim_label->uri().to_string() == dim_label->uri().to_string());
  CHECK(dim_label->uri_is_relative() == dim_label->uri_is_relative());
}
