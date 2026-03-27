/**
 * @file   tiledb_struct_def.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file contains the TileDB C API struct object definitions.
 */

#ifndef TILEDB_C_API_STRUCT_DEF_H
#define TILEDB_C_API_STRUCT_DEF_H

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/sm/consolidation_plan/consolidation_plan.h"

struct tiledb_consolidation_plan_handle_t : public tiledb::api::CAPIHandle {
  /** Type name */
  static constexpr std::string_view object_type_name{"consolidation_plan"};

 private:
  shared_ptr<tiledb::sm::ConsolidationPlan> consolidation_plan_ = nullptr;

 public:
  tiledb_consolidation_plan_handle_t(
      shared_ptr<tiledb::sm::ConsolidationPlan> consolidation_plan)
      : consolidation_plan_(consolidation_plan) {
  }

  tiledb_consolidation_plan_handle_t(std::in_place_t, auto&&... args)
      : consolidation_plan_(make_shared<tiledb::sm::ConsolidationPlan>(
            HERE(), std::forward<decltype(args)>(args)...)) {
  }

  const tiledb::sm::ConsolidationPlan& consolidation_plan() const {
    return *consolidation_plan_;
  }
};

#endif
