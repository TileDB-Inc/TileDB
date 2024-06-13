/**
 * @file   count_aggregator.h
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
 * This file defines class CountAggregator.
 */

#ifndef TILEDB_COUNT_AGGREGATOR_H
#define TILEDB_COUNT_AGGREGATOR_H

#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_with_count.h"
#include "tiledb/sm/query/readers/aggregators/iaggregator.h"
#include "tiledb/sm/query/readers/aggregators/no_op.h"
#include "tiledb/sm/query/readers/aggregators/validity_policies.h"

namespace tiledb::sm {

class QueryBuffer;

template <class ValidityPolicy>
class CountAggregatorBase : public OutputBufferValidator, public IAggregator {
 public:
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  CountAggregatorBase(FieldInfo field_info = FieldInfo());

  DISABLE_COPY_AND_COPY_ASSIGN(CountAggregatorBase);
  DISABLE_MOVE_AND_MOVE_ASSIGN(CountAggregatorBase);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns if the aggregation is var sized or not. */
  bool aggregation_var_sized() override {
    return false;
  };

  /** Returns if the aggregation is nullable or not. */
  bool aggregation_nullable() override {
    return false;
  }

  /** Returns if the aggregate needs to be recomputed on overflow. */
  bool need_recompute_on_overflow() override {
    return true;
  }

  /**
   * Validate the result buffer.
   *
   * @param output_field_name Name for the output buffer.
   * @param buffers Query buffers.
   */
  void validate_output_buffer(
      std::string output_field_name,
      std::unordered_map<std::string, QueryBuffer>& buffers) override;

  /**
   * Aggregate data using the aggregator.
   *
   * @param input_data Input data for aggregation.
   */
  void aggregate_data(AggregateBuffer& input_data) override;

  /**
   * Aggregate a tile with fragment metadata.
   *
   * @param tile_metadata Tile metadata for aggregation.
   */
  void aggregate_tile_with_frag_md(TileMetadata& tile_metadata) override;

  /**
   * Copy final data to the user buffer.
   *
   * @param output_field_name Name for the output buffer.
   * @param buffers Query buffers.
   */
  void copy_to_user_buffer(
      std::string output_field_name,
      std::unordered_map<std::string, QueryBuffer>& buffers) override;

  /** Returns the TileDB datatype of the output field for the aggregate. */
  Datatype output_datatype() override {
    return Datatype::UINT64;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** AggregateWithCount to do summation of AggregateBuffer data. */
  AggregateWithCount<uint8_t, uint64_t, NoOp, ValidityPolicy>
      aggregate_with_count_;

  /** Cell count. */
  std::atomic<uint64_t> count_;
};

class CountAggregator : public CountAggregatorBase<NonNull> {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  CountAggregator()
      : CountAggregatorBase() {
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the field name for the aggregator. */
  std::string field_name() override {
    return constants::count_of_rows;
  }

  /** Returns name of the aggregate. */
  std::string aggregate_name() override {
    return constants::aggregate_count_str;
  }

  /** Returns if the aggregation is for validity only data. */
  bool aggregation_validity_only() override {
    return false;
  }
};

class NullCountAggregator : public CountAggregatorBase<Null>,
                            public InputFieldValidator {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  NullCountAggregator(FieldInfo field_info);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the field name for the aggregator. */
  std::string field_name() override {
    return field_info_.name_;
  }

  /** Returns name of the aggregate. */
  std::string aggregate_name() override {
    return constants::aggregate_null_count_str;
  }

  /** Returns if the aggregation is for validity only data. */
  bool aggregation_validity_only() override {
    return true;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Field information. */
  const FieldInfo field_info_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_COUNT_AGGREGATOR_H
