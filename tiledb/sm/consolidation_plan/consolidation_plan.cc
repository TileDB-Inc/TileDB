/**
 * @file   consolidation_plan.cc
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
 * This file implements the ConsolidationPlan class.
 */

#include "tiledb/sm/consolidation_plan/consolidation_plan.h"
#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"

using namespace tiledb::sm;
using namespace tiledb::common;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ConsolidationPlan::ConsolidationPlan(shared_ptr<Array> array, uint64_t) {
  generate(array);
}

ConsolidationPlan::~ConsolidationPlan() = default;

/* ********************************* */
/*                API                */
/* ********************************* */

void ConsolidationPlan::dump(FILE* out) const {
  if (out == nullptr) {
    out = stdout;
  }

  std::stringstream ss;
  ss << "Not implemented\n";
  fprintf(out, "%s", ss.str().c_str());
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

void ConsolidationPlan::generate(shared_ptr<Array>) {
}
