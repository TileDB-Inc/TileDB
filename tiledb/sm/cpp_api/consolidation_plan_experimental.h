/**
 * @file   consolidation_plan_experimental.h
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
 * This file declares the C++ API for the TileDB consolidation plan.
 */

#ifndef TILEDB_CPP_API_CONSOLIDATION_PLAN_EXPERIMENTAL_H
#define TILEDB_CPP_API_CONSOLIDATION_PLAN_EXPERIMENTAL_H

#include "context.h"
#include "tiledb.h"

namespace tiledb {
class ConsolidationPlan {
 public:
  /**
   * Constructor. This creates the consolidation plan for an array with the
   * given desired maximum fragment size.
   *
   * @param ctx TileDB context.
   * @param array The array.
   * @param fragment_size The desired fragment size.
   */
  ConsolidationPlan(
      const Context& ctx, const Array& array, uint64_t fragment_size)
      : ctx_(ctx) {
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    tiledb_array_t* c_array = array.ptr().get();
    tiledb_consolidation_plan_t* consolidation_plan;
    ctx.handle_error(tiledb_consolidation_plan_create_with_mbr(
        c_ctx, c_array, fragment_size, &consolidation_plan));
    consolidation_plan_ = std::shared_ptr<tiledb_consolidation_plan_t>(
        consolidation_plan, deleter_);
  }

  ConsolidationPlan(const ConsolidationPlan&) = default;
  ConsolidationPlan(ConsolidationPlan&&) = default;
  ConsolidationPlan& operator=(const ConsolidationPlan&) = default;
  ConsolidationPlan& operator=(ConsolidationPlan&&) = default;

  /** Destructor. */
  ~ConsolidationPlan() {
  }

  /**
   * Returns the number of nodes in the consolidation plan.
   */
  uint64_t num_nodes() const {
    uint64_t num;
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_consolidation_plan_get_num_nodes(
        c_ctx, consolidation_plan_.get(), &num));
    return num;
  }

  /**
   * Returns the number of fragments for a node in the consolidation plan.
   *
   * @param node_idx Node index to retreive the data for.
   */
  uint64_t num_fragments(uint64_t node_idx) const {
    uint64_t num;
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_consolidation_plan_get_num_fragments(
        c_ctx, consolidation_plan_.get(), node_idx, &num));
    return num;
  }

  /**
   * Returns the fragment uri for a node/fragment in the consolidation plan.
   *
   * @param node_idx Node index to retreive the data for.
   * @param fragment_idx Fragment index to retreive the data for.
   */
  std::string fragment_uri(uint64_t node_idx, uint64_t fragment_idx) const {
    const char* uri_c;
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    ctx.handle_error(tiledb_consolidation_plan_get_fragment_uri(
        c_ctx, consolidation_plan_.get(), node_idx, fragment_idx, &uri_c));
    return std::string(uri_c);
  }

  /**
   * Dumps the consolidation plan in a JSON representation to an output.
   *
   * @return the JSON string for the plan.
   */
  std::string dump() const {
    auto& ctx = ctx_.get();

    char* str = nullptr;
    ctx.handle_error(tiledb_consolidation_plan_dump_json_str(
        ctx.ptr().get(), consolidation_plan_.get(), &str));

    std::string ret(str);
    tiledb_consolidation_plan_free_json_str(&str);

    return ret;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Pointer to the TileDB C consolidation plan object. */
  std::shared_ptr<tiledb_consolidation_plan_t> consolidation_plan_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_CONSOLIDATION_PLAN_EXPERIMENTAL_H
