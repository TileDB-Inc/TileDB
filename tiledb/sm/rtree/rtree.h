/**
 * @file   rtree.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include <vector>

#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/misc/tile_overlap.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;

enum class Datatype : uint8_t;
enum class Layout : uint8_t;

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

  /** Constructor. */
  RTree(const Domain* domain, unsigned fanout);

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

  /** Builds the RTree bottom-up on the current leaf level. */
  Status build_tree();

  /** Frees the memory associated with the rtree. */
  uint64_t free_memory();

  /** The number of dimensions of the R-tree. */
  unsigned dim_num() const;

  /** Returns the domain. */
  const Domain* domain() const;

  /** Returns the fanout. */
  unsigned fanout() const;

  /**
   * Returns the tile overlap of the input range with the MBRs stored
   * in the RTree.
   */
  TileOverlap get_tile_overlap(
      const NDRange& range, std::vector<bool>& is_default) const;

  /**
   * Compute tile bitmap for the curent range.
   */
  void compute_tile_bitmap(
      const Range& range, unsigned d, std::vector<uint8_t>* tile_bitmap) const;

  /** Returns the tree height. */
  unsigned height() const;

  /** Returns the leaf MBR with the input index. */
  const NDRange& leaf(uint64_t leaf_idx) const;

  /** Returns the leaves of the tree. */
  const std::vector<NDRange>& leaves() const;

  /**
   * Returns the number of leaves that are stored in a (full) subtree
   * rooted at the input level. Note that the root is at level 0.
   */
  uint64_t subtree_leaf_num(uint64_t level) const;

  /**
   * Serializes the contents of the object to the input buffer.
   * Note that `domain_` is not serialized in the buffer.
   */
  Status serialize(Buffer* buff) const;

  /**
   * Sets the RTree domain.
   */
  Status set_domain(const Domain* domain);

  /**
   * Sets an MBR as a leaf in the tree. The function will error out
   * if the number of levels in the tree is different from exactly
   * 1 (the leaf level), and if `leaf_id` is out of bounds / invalid.
   */
  Status set_leaf(uint64_t leaf_id, const NDRange& mbr);

  /**
   * Sets the input MBRs as leaves. This will destroy the existing
   * RTree.
   */
  Status set_leaves(const std::vector<NDRange>& mbrs);

  /**
   * Resizes the leaf level. It destroys the upper levels
   * of the tree if they exist.
   * It errors if `num` is smaller than the current number
   * of leaves.
   */
  Status set_leaf_num(uint64_t num);

  /**
   * Deserializes the contents of the object from the input buffer based
   * on the format version.
   * It also sets the input domain, as that is not serialized.
   */
  Status deserialize(
      ConstBuffer* cbuff, const Domain* domain, uint32_t version);

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
  typedef std::vector<NDRange> Level;

  /**
   * Defines an R-Tree level entry, which corresponds to a node
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

  /** The domain. */
  const Domain* domain_;

  /** The fanout of the tree. */
  unsigned fanout_;

  /**
   * The tree levels. The first level is the root. Note that the root
   * always consists of a single MBR.
   */
  std::vector<Level> levels_;

  /**
   * Stores the size of the buffer used to deserialize the data, used for
   * memory tracking pusposes on reads.
   */
  uint64_t deserialized_buffer_size_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Builds a single tree level on top of the input level. */
  Level build_level(const Level& level);

  /** Returns a deep copy of this RTree. */
  RTree clone() const;

  /**
   * Deserializes the contents of the object from the input buffer based
   * on the format version.
   * It also sets the input domain, as that is not serialized.
   *
   * Applicable to versions 1-4
   */
  Status deserialize_v1_v4(ConstBuffer* cbuff, const Domain* domain);

  /**
   * Deserializes the contents of the object from the input buffer based
   * on the format version.
   * It also sets the input domain, as that is not serialized.
   *
   * Applicable to versions >= 5
   */
  Status deserialize_v5(ConstBuffer* cbuff, const Domain* domain);

  /**
   * Swaps the contents (all field values) of this RTree with the
   * given ``rtree``.
   */
  void swap(RTree& rtree);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RTREE_H
