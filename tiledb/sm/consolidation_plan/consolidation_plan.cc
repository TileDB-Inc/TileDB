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
#include "tiledb/sm/rest/rest_client.h"

using namespace tiledb::sm;
using namespace tiledb::common;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ConsolidationPlan::ConsolidationPlan(
    shared_ptr<Array> array, uint64_t fragment_size)
    : desired_fragment_size_(fragment_size) {
  if (array->is_remote()) {
    auto rest_client = array->rest_client();
    if (!rest_client) {
      throw std::runtime_error(
          "Failed to create a consolidation plan; Remote array"
          "with no REST client.");
    }
    // reach out to the REST client to populate class members
    fragment_uris_per_node_ = rest_client->post_consolidation_plan_from_rest(
        array->array_uri(), array->config(), fragment_size);
    num_nodes_ = fragment_uris_per_node_.size();
  } else {
    generate(array);
  }
}

ConsolidationPlan::ConsolidationPlan(
    uint64_t fragment_size,
    std::vector<std::vector<std::string>> fragment_uris_per_node)
    : num_nodes_{fragment_uris_per_node.size()}
    , fragment_uris_per_node_{fragment_uris_per_node}
    , desired_fragment_size_{fragment_size} {
}

ConsolidationPlan::~ConsolidationPlan() = default;

/* ********************************* */
/*                API                */
/* ********************************* */

std::string ConsolidationPlan::dump() const {
  std::string ret = "{\n  \"nodes\": [\n";
  for (uint64_t n = 0; n < fragment_uris_per_node_.size(); n++) {
    auto node = fragment_uris_per_node_[n];
    ret += "    {\n      \"uris\" : [\n";
    for (uint64_t u = 0; u < node.size(); u++) {
      auto uri = node[u];
      ret += "        {\n";
      ret += "           \"uri\" : \"" + uri + "\"\n";
      if (u != node.size() - 1) {
        ret += "        },\n";
      } else {
        ret += "        }\n";
      }
    }

    if (n != fragment_uris_per_node_.size() - 1) {
      ret += "      ]\n    },\n";
    } else {
      ret += "      ]\n    }\n";
    }
  }

  ret += "  ]\n}\n";
  return ret;
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

void ConsolidationPlan::generate(shared_ptr<Array> array) {
  // Start with the plan being a single fragment per node.
  std::list<PlanNode> plan;
  for (unsigned f = 0;
       f < static_cast<unsigned>(array->fragment_metadata().size());
       f++) {
    plan.emplace_back(array, f);
  }

  // First we combine all fragments that have overlap so they get disantangled.
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

  // Second, we try to combine smaller fragments. The result should not
  // intersect any other fragments. Process until we don't find any small nodes
  // to combine.
  bool combination_found = true;
  uint64_t small_size = desired_fragment_size_ / 2;
  while (combination_found) {
    combination_found = false;

    // Go through all small nodes.
    auto current = plan.begin();
    while (current != plan.end()) {
      if (current->size() <= small_size) {
        // Compare to all other small nodes.
        auto other = current;
        other++;
        while (other != plan.end()) {
          if (other->size() <= small_size) {
            // Get the combined NED.
            auto combined_ned = current->get_combined_ned(*other);

            // See if there is any overlap with any other nodes.
            bool overlap_found = false;
            auto to_check_overlap = plan.begin();
            while (to_check_overlap != plan.end()) {
              if (to_check_overlap != current && to_check_overlap != other) {
                if (to_check_overlap->overlap(combined_ned)) {
                  overlap_found = true;
                  break;
                }
              }

              to_check_overlap++;
            }

            // If there is no overlap with any other fragments, combine the
            // nodes.
            if (!overlap_found) {
              combination_found = true;
              current->combine(*other);
              auto to_delete = other;
              other++;
              plan.erase(to_delete);
            } else {
              other++;
            }
          } else {
            other++;
          }
        }
      }

      current++;
    }
  }

  // Move the combined nodes to the beginning of the list.
  auto start = std::partition(
      plan.begin(), plan.end(), [&](const auto& el) { return el.combined(); });

  // Move single nodes that we can split right after the combined nodes.
  start = std::partition(start, plan.end(), [&](const auto& el) {
    return el.size() >= 1.5 * desired_fragment_size_;
  });

  // Erase the nodes that are not included in the plan.
  plan.erase(start, plan.end());

  // Fill in the data for the plan.
  num_nodes_ = plan.size();
  fragment_uris_per_node_.reserve(num_nodes_);

  for (auto& node : plan) {
    fragment_uris_per_node_.emplace_back(node.uris());
  }
}
