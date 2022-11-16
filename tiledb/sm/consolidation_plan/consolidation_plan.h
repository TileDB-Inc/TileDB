/**
 * @file  consolidation_plan.h
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
 * This file defines class ConsolidationPlan.
 */

#ifndef TILEDB_CONSOLIDATION_PLAN_H
#define TILEDB_CONSOLIDATION_PLAN_H

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"

#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;

class ConsolidationPlanStatusException : public StatusException {
 public:
  explicit ConsolidationPlanStatusException(const std::string& message)
      : StatusException("ConsolidationPlan", message) {
  }
};

/** Stores ba consolidation plan. */
class ConsolidationPlan {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ConsolidationPlan(StorageManager* storage_manager, shared_ptr<Array> array);

  /** Destructor. */
  ~ConsolidationPlan();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  inline uint64_t get_num_nodes() const {
    return num_nodes_;
  }

  inline uint64_t get_num_fragments(uint64_t node_idx) const {
    if (node_idx == std::numeric_limits<uint64_t>::max() ||
        node_idx + 1 > num_nodes_) {
      throw ConsolidationPlanStatusException(
          "Trying to access a node that doesn't exists");
    }

    return num_fragments_[node_idx];
  }

  inline const char* get_fragment_uri(
      uint64_t node_idx, uint64_t fragment_idx) const {
    if (node_idx == std::numeric_limits<uint64_t>::max() ||
        node_idx + 1 > num_nodes_) {
      throw ConsolidationPlanStatusException(
          "Trying to access a node that doesn't exists");
    }

    if (fragment_idx == std::numeric_limits<uint64_t>::max() ||
        fragment_idx + 1 > num_fragments_[node_idx]) {
      throw ConsolidationPlanStatusException(
          "Trying to access a fragment that doesn't exists");
    }

    return fragment_uris_[node_idx][fragment_idx].c_str();
  }

  void dump(FILE* out) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The number of nodes in the consolidation plan. */
  uint64_t num_nodes_;

  /** The number of nodes in the consolidation plan. */
  std::vector<uint64_t> num_fragments_;

  /** Fragment uris, per node. */
  std::vector<std::vector<std::string>> fragment_uris_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CONSOLIDATION_PLAN_H
