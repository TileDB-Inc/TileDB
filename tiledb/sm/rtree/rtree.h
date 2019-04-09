/**
 * @file   rtree.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file defines class RTree.
 */

#ifndef TILEDB_RTREE_H
#define TILEDB_RTREE_H

#include <memory>
#include <vector>
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/tile_overlap.h"
#include "tiledb/sm/subarray/subarray.h"

namespace tiledb {
namespace sm {

/**
 * A simple RTree implementation. It supports storing only n-dimensional
 * MBRs (not points). Also it only offers bottom-up bulk-loading
 * (without incremental updates), and range and point queries.
 */
class RTree {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  RTree();

  /**
   * Constructor. This admits a list of sorted MBRs that will
   * constitute the leaf level of the tree. The constructor will
   * construct bottom up the tree based on these ``mbrs``.
   * The input MBRs will be copied into the leaf level.
   */
  RTree(
      Datatype type,
      unsigned dim_num,
      unsigned fanout,
      const std::vector<void*>& mbrs);

  /** Destructor. */
  ~RTree();

  /** Copy constructor. This performs a deep copy. */
  RTree(const RTree& rtree);

  /** Move constructor. */
  RTree(RTree&& rtree) noexcept;

  /** Copy-assign operator. This performs a deep copy. */
  RTree& operator=(const RTree& rtree);

  /** Move-assign operator. */
  RTree& operator=(RTree&& rtree) noexcept;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the number of dimensions. */
  unsigned dim_num() const;

  /** Returns the fanout. */
  unsigned fanout() const;

  /**
   * Returns the tile overlap of the input range with the MBRs stored
   * in the RTree.
   */
  template <class T>
  TileOverlap get_tile_overlap(const std::vector<const T*>& range) const;

  /** Returns the tree height. */
  unsigned height() const;

  /**
   * Returns the overlap between a range and an RTree MBR, as the ratio
   * of the volume of the overlap over the volume of the MBR.
   */
  template <class T>
  double range_overlap(const std::vector<const T*>& range, const T* mbr) const;

  /**
   * Returns the number of leaves that are stored in a (full) subtree
   * rooted at the input level. Note that the root is at level 0.
   */
  uint64_t subtree_leaf_num(uint64_t level) const;

  /** Returns the datatype of the R-Tree. */
  Datatype type() const;

  /** Serializes the contents of the object to the input buffer. */
  Status serialize(Buffer* buff) const;

  /** Deserializes the contents of the object from the input buffer. */
  Status deserialize(ConstBuffer* cbuff);

 private:
  /* ********************************* */
  /*      PRIVATE TYPE DEFINITIONS     */
  /* ********************************* */

  /**
   * The RTree is composed of nodes and is constructed bottom up
   * such that each node has exactly `fanout_` children. The
   * construction algorithm attempts to create a full tree and,
   * therefore, all nodes have `fanout_` children except for the
   * "rightmost" nodes which may contain less.
   *
   * The nodes at the same height of the tree form a level of
   * the tree. Also each node simply contains up to `fanout_`
   * MBRs.
   *
   * A `Level` object serializes the contents of the nodes at
   * the same tree level, that is it serializes all the MBRs
   * of all those nodes. Since we know the `fanout_` and the
   * number of levels, as well as the fact that the tree is from
   * left-to-right complete, we can easily infer which MBRs
   * in the `Level` object correspond to which node of that level.
   *
   * Note that the `Level` objects are stored in a simple vector
   * `levels_`, where the first level is the root. This is how
   * we can infer which tree level each `Level` object corresponds to.
   */
  struct Level {
    /** Number of MBRs in the level (across all nodes in the level). */
    uint64_t mbr_num_ = 0;
    /** The serialized MBRs of the level, in the form
     * ``(low_1, high_1), ..., (low_d, high_d)`` where ``d`` is
     * the number of dimensions.
     */
    std::vector<uint8_t> mbrs_;
  };

  /** Defines an R-Tree level entry, which corresponds to a node
   * at a particular level. It stores the level the entry belongs
   * to, as well as the starting index of the first MBR in the
   * corresponding R-Tree node.
   */
  struct Entry {
    /** The level of the node the entry corresponds to. */
    uint64_t level_;
    /** The index of the first MBR of the corresponding node. */
    uint64_t mbr_idx_;
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The number of dimensions. */
  unsigned dim_num_;

  /** The fanout of the tree. */
  unsigned fanout_;

  /** The data type. */
  Datatype type_;

  /**
   * The tree levels. The first level is the root. Note that the root
   * always consists of a single MBR.
   */
  std::vector<Level> levels_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Builds the RTree bottom-up on the input MBRs. */
  Status build_tree(const std::vector<void*>& mbrs);

  /** Builds the RTree bottom-up on the input MBRs. */
  template <class T>
  Status build_tree(const std::vector<void*>& mbrs);

  /** Builds the tree leaf level using the input mbrs. */
  Level build_leaf_level(const std::vector<void*>& mbrs);

  /** Builds a single tree level on top of the input level. */
  template <class T>
  Level build_level(const Level& level);

  /** Returns a deep copy of this RTree. */
  RTree clone() const;

  /**
   * Swaps the contents (all field values) of this RTree with the
   * given ``rtree``.
   */
  void swap(RTree& rtree);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RTREE_H
