/**
 * @file   operation.h
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
 * This file defines class Operation and its derivates.
 */

#ifndef TILEDB_OPERATION_H
#define TILEDB_OPERATION_H

#include "tiledb/common/common.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/query/readers/aggregators/count_aggregator.h"
#include "tiledb/sm/query/readers/aggregators/min_max_aggregator.h"
#include "tiledb/sm/query/readers/aggregators/sum_aggregator.h"
#include "tiledb/sm/query/readers/aggregators/sum_type.h"
#include "tiledb/type/apply_with_type.h"

namespace tiledb::sm {

class Operation : public InputFieldValidator {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  virtual ~Operation(){};

  /* ********************************* */
  /*                API                */
  /* ********************************* */
  /**
   * @return a shared pointer to the internal aggregator object
   */
  [[nodiscard]] virtual shared_ptr<tiledb::sm::IAggregator> aggregator() const {
    return aggregator_;
  }

  /**
   * Create the right operation based on the aggregator name
   *
   * @param name The name of the aggregator e.g. SUM, MIN
   * @param fi A FieldInfo object describing the input field or nullopt for
   * nullary operations
   * @return The appropriate operation for the name
   */
  static shared_ptr<Operation> make_operation(
      const std::string& name, const std::optional<tiledb::sm::FieldInfo>& fi);

  /* ********************************* */
  /*         PROTECTED ATTRIBUTES      */
  /* ********************************* */
 protected:
  shared_ptr<tiledb::sm::IAggregator> aggregator_ = nullptr;
};

class MinOperation : public Operation {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  MinOperation() = delete;

  /**
   * Construct the operation where the internal aggregator object
   * is instantiated to the correct type given the input field type.
   * @param fi The FieldInfo for the input field
   */
  explicit MinOperation(const tiledb::sm::FieldInfo& fi) {
    auto g = [&](auto T) {
      if constexpr (tiledb::type::TileDBFundamental<decltype(T)>) {
        if constexpr (tiledb::type::TileDBNumeric<decltype(T)>) {
          ensure_field_numeric(fi);
        }

        // This is a min/max on strings, should be refactored out once we
        // change (STRING_ASCII,CHAR) mapping in apply_with_type
        if constexpr (std::is_same_v<char, decltype(T)>) {
          aggregator_ =
              tdb::make_shared<tiledb::sm::MinAggregator<std::string>>(
                  HERE(), fi);
        } else {
          aggregator_ =
              tdb::make_shared<tiledb::sm::MinAggregator<decltype(T)>>(
                  HERE(), fi);
        }
      } else {
        throw std::logic_error(
            "MIN aggregates can only be requested on numeric and string "
            "types");
      }
    };
    type::apply_with_type(g, fi.type_);
  }
};

class MaxOperation : public Operation {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  MaxOperation() = delete;

  /**
   * Construct the operation where the internal aggregator object
   * is instantiated to the correct type given the input field type.
   * @param fi The FieldInfo for the input field
   */
  explicit MaxOperation(const tiledb::sm::FieldInfo& fi) {
    auto g = [&](auto T) {
      if constexpr (tiledb::type::TileDBFundamental<decltype(T)>) {
        if constexpr (tiledb::type::TileDBNumeric<decltype(T)>) {
          ensure_field_numeric(fi);
        }

        // This is a min/max on strings, should be refactored out once we
        // change (STRING_ASCII,CHAR) mapping in apply_with_type
        if constexpr (std::is_same_v<char, decltype(T)>) {
          aggregator_ =
              tdb::make_shared<tiledb::sm::MaxAggregator<std::string>>(
                  HERE(), fi);
        } else {
          aggregator_ =
              tdb::make_shared<tiledb::sm::MaxAggregator<decltype(T)>>(
                  HERE(), fi);
        }
      } else {
        throw std::logic_error(
            "MAX aggregates can only be requested on numeric and string "
            "types");
      }
    };
    type::apply_with_type(g, fi.type_);
  }
};

class SumOperation : public Operation {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  SumOperation() = delete;

  /**
   * Construct the operation where the internal aggregator object
   * is instantiated to the correct type given the input field type.
   * @param fi The FieldInfo for the input field
   */
  explicit SumOperation(const tiledb::sm::FieldInfo& fi) {
    auto g = [&](auto T) {
      if constexpr (tiledb::type::TileDBNumeric<decltype(T)>) {
        ensure_field_numeric(fi);
        aggregator_ = tdb::make_shared<tiledb::sm::SumAggregator<decltype(T)>>(
            HERE(), fi);
      } else {
        throw std::logic_error(
            "SUM aggregates can only be requested on numeric types");
      }
    };
    type::apply_with_type(g, fi.type_);
  }
};

class CountOperation : public Operation {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  /** Default constructor */
  CountOperation() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */
  // For count operations we have a constant handle, create the aggregator when
  // requested so that we get a different object for each query.
  [[nodiscard]] shared_ptr<tiledb::sm::IAggregator> aggregator()
      const override {
    return common::make_shared<tiledb::sm::CountAggregator>(HERE());
  }
};

class MeanOperation : public Operation {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  MeanOperation() = delete;

  /**
   * Construct the operation where the internal aggregator object
   * is instantiated to the correct type given the input field type.
   * @param fi The FieldInfo for the input field
   */
  explicit MeanOperation(const tiledb::sm::FieldInfo& fi) {
    auto g = [&](auto T) {
      if constexpr (tiledb::type::TileDBNumeric<decltype(T)>) {
        ensure_field_numeric(fi);
        aggregator_ = tdb::make_shared<tiledb::sm::MeanAggregator<decltype(T)>>(
            HERE(), fi);
      } else {
        throw std::logic_error(
            "MEAN aggregates can only be requested on numeric types");
      }
    };
    type::apply_with_type(g, fi.type_);
  }
};

class NullCountOperation : public Operation {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  NullCountOperation() = delete;

  /**
   * Construct the NULL_COUNT operation
   * @param fi The FieldInfo for the input field
   */
  explicit NullCountOperation(const tiledb::sm::FieldInfo& fi) {
    aggregator_ = tdb::make_shared<tiledb::sm::NullCountAggregator>(HERE(), fi);
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_OPERATION_H
