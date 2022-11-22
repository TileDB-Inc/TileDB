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

ConsolidationPlan::ConsolidationPlan(
    shared_ptr<Array> array, uint64_t fragment_size)
    : desired_fragment_size_(fragment_size) {
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

void ConsolidationPlan::generate(shared_ptr<Array> array) {
  // Start with the plan being a single fragment per node.
  std::list<PlanNode> plan;
  for (uint64_t idx = 0; idx < array->fragment_metadata().size(); idx++) {
    plan.emplace_back(array, idx);
  }

  // Process until we don't find any overlapping fragments.
  bool overlap_found = true;
  while (overlap_found) {
    overlap_found = false;

    // Go through all nodes.
    auto current = plan.begin();
    while (current != plan.end()) {
      // Compare to other nodes.
      auto other = current;
      other++;
      while (other != plan.end()) {
        // If there is overlap, combine the nodes.
        if (current->overlap(*other)) {
          overlap_found = true;
          current->combine(*other);
          auto to_delete = other;
          other++;
          plan.erase(to_delete);
        } else {
          other++;
        }
      }

      current++;
    }
  }

  // Move the combined nodes to the beginning of the list.
  auto start = std::partition(
      plan.begin(), plan.end(), [&](const auto& el) { return el.combined(); });

  // Move single nodes that we can split to the beginning of the list.
  start = std::partition(start, plan.end(), [&](const auto& el) {
    return el.size() > 2 * desired_fragment_size_;
  });

  // Try to combine smaller fragments.
  // TODO.

  plan.erase(start, plan.end());

  // Fill in the data for the plan.
  num_nodes_ = plan.size();
  fragment_uris_.reserve(num_nodes_);

  for (auto& node : plan) {
    fragment_uris_.emplace_back(node.uris());
  }
}