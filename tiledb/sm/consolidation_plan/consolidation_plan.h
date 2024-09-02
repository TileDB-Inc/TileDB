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
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
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

/** Stores a consolidation plan. */
class ConsolidationPlan {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ConsolidationPlan(shared_ptr<Array> array, uint64_t fragment_size);
  /** Constructor. */
  ConsolidationPlan(
      uint64_t fragment_size,
      std::vector<std::vector<std::string>> fragment_uris_per_node);

  /** Destructor. */
  ~ConsolidationPlan();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** @return the number of nodes for the plan. */
  inline uint64_t get_num_nodes() const {
    return num_nodes_;
  }

  /**
   * Get the number of fragments for a node.
   *
   * @param node_idx Index of the node.
   * @return Number of fragment for the node.
   */
  inline uint64_t get_num_fragments(uint64_t node_idx) const {
    if (node_idx == std::numeric_limits<uint64_t>::max() ||
        node_idx + 1 > num_nodes_) {
      throw ConsolidationPlanStatusException(
          "Trying to access a node that doesn't exist.");
    }

    return fragment_uris_per_node_[node_idx].size();
  }

  /**
   * Get the fragment URI for a node/fragment index.
   *
   * @param node_idx Index of the node.
   * @param fragment_idx Index of the fragment.
   * @return The null terminated string for the fragment URI.
   */
  inline const char* get_fragment_uri(
      uint64_t node_idx, uint64_t fragment_idx) const {
    if (node_idx == std::numeric_limits<uint64_t>::max() ||
        node_idx + 1 > num_nodes_) {
      throw ConsolidationPlanStatusException(
          "Trying to access a node that doesn't exist.");
    }

    if (fragment_idx == std::numeric_limits<uint64_t>::max() ||
        fragment_idx + 1 > fragment_uris_per_node_[node_idx].size()) {
      throw ConsolidationPlanStatusException(
          "Trying to access a fragment that doesn't exist.");
    }

    return fragment_uris_per_node_[node_idx][fragment_idx].c_str();
  }

  /** @return the consolidation plan in JSON format. */
  std::string dump() const;

 private:
  /* ********************************* */
  /*        PRIVATE DATATYPES          */
  /* ********************************* */

  /** Plan node of a consolidation plan. */
  class PlanNode {
   public:
    /* ****************************** */
    /*   CONSTRUCTORS & DESTRUCTORS   */
    /* ****************************** */

    PlanNode() = delete;

    /** Constructs a plan object using a single fragment index. */
    PlanNode(shared_ptr<Array> array, unsigned frag_idx)
        : array_(array)
        , fragment_indexes_({frag_idx})
        , combined_non_empty_domain_(
              array->fragment_metadata()[frag_idx]->non_empty_domain())
        , fragment_size_(
              array->fragment_metadata()[frag_idx]->fragment_size()) {
    }

    /* ********************************* */
    /*               API                 */
    /* ********************************* */

    /** @return Combined NEDs. */
    NDRange get_combined_ned(PlanNode& other) {
      auto combined_ned = combined_non_empty_domain_;
      array_->array_schema_latest().domain().expand_ndrange(
          other.combined_non_empty_domain_, &combined_ned);
      return combined_ned;
    }

    /** Combined two plan nodes. */
    void combine(PlanNode& other) {
      array_->array_schema_latest().domain().expand_ndrange(
          other.combined_non_empty_domain_, &combined_non_empty_domain_);
      fragment_indexes_.insert(
          fragment_indexes_.end(),
          other.fragment_indexes_.begin(),
          other.fragment_indexes_.end());
      fragment_size_ += other.fragment_size_;
    }

    /** @return `true` if the two plan nodes have overlapping domains. */
    bool overlap(PlanNode& other) const {
      return array_->array_schema_latest().domain().overlap(
          combined_non_empty_domain_, other.combined_non_empty_domain_);
    }

    /** @return `true` node has overlapping domains with the other NDRange. */
    bool overlap(NDRange& other) const {
      return array_->array_schema_latest().domain().overlap(
          combined_non_empty_domain_, other);
    }

    /** @return `true` if a fragment is node is combined, `false` if not. */
    bool combined() const {
      return fragment_indexes_.size() > 1;
    }

    /** @return Approximate fragment size. */
    uint64_t size() const {
      return fragment_size_;
    }

    /** @return Uris for this node. */
    std::vector<std::string> uris() {
      std::vector<std::string> ret;
      ret.reserve(fragment_indexes_.size());
      for (auto& idx : fragment_indexes_) {
        ret.emplace_back(array_->fragment_metadata()[idx]
                             ->fragment_uri()
                             .last_path_part()
                             .c_str());
      }

      return ret;
    }

   private:
    /* ********************************* */
    /*         PRIVATE ATTRIBUTES        */
    /* ********************************* */

    /** Array. */
    shared_ptr<Array> array_;

    /** Fragment indexes included in this plan object. */
    std::vector<unsigned> fragment_indexes_;

    /** Combined non empty domain for this plan object. */
    NDRange combined_non_empty_domain_;

    /** Approximate fragment size. */
    storage_size_t fragment_size_;
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The number of nodes in the consolidation plan. */
  uint64_t num_nodes_;

  /** Fragment uris, per node. */
  std::vector<std::vector<std::string>> fragment_uris_per_node_;

  /** Desired fragment size, in bytes. */
  storage_size_t desired_fragment_size_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Generate the consolidation plan.
   *
   * @param array Opened array to generate the plan for.
   */
  void generate(shared_ptr<Array> array);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CONSOLIDATION_PLAN_H
