/**
 * @file   rtree.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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

#include <deque>
#include <vector>

#include "tiledb/common/common.h"
#include "tiledb/common/pmr.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/misc/tile_overlap.h"
#include "tiledb/storage_format/serialization/serializers.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class MemoryTracker;
class Subarray;

enum class Datatype : uint8_t;
enum class Layout : uint8_t;

template <typename T>
concept BoundingRectanglePredicate =
    requires(const T& rectangle, const NDRange& mbr) {
      { rectangle.overlap_ratio(mbr) } -> std::same_as<double>;
    };

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
  RTree() = delete;

  /** Constructor. */
  RTree(
      const Domain* domain,
      unsigned fanout,
      shared_ptr<MemoryTracker> memory_tracker);

  /** Destructor. */
  ~RTree();

  DISABLE_COPY_AND_COPY_ASSIGN(RTree);
  DISABLE_MOVE_AND_MOVE_ASSIGN(RTree);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Builds the RTree bottom-up on the current leaf level. */
  void build_tree();

  /** Frees the memory associated with the rtree. */
  uint64_t free_memory();

  /** The number of dimensions of the R-tree. */
  unsigned dim_num() const;

  /** Returns the domain. */
  inline const Domain* domain() const {
    return domain_;
  }

  /** Returns the fanout. */
  unsigned fanout() const;

  /**
   * Computes the tiles which qualify according to the visitor selectivity
   * analysis.
   *
   * @param visitor
   * @return the tile overlap containing qualifying tiles according to
   * `visitor.visit(mbr)`
   */
  template <BoundingRectanglePredicate T>
  TileOverlap get_tile_overlap(const T& predicate) const;

  TileOverlap get_tile_overlap(const NDRange& rectangle) const;

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
  const tdb::pmr::vector<NDRange>& leaves() const;

  /**
   * Returns the number of leaves that are stored in a (full) subtree
   * rooted at the input level. Note that the root is at level 0.
   */
  uint64_t subtree_leaf_num(uint64_t level) const;

  /**
   * Serializes the contents of the object to the input buffer.
   * Note that `domain_` is not serialized in the buffer.
   */
  void serialize(Serializer& serializer) const;

  /**
   * Sets the RTree domain.
   */
  inline void set_domain(const Domain* domain) {
    domain_ = domain;
  }

  /**
   * Sets an MBR as a leaf in the tree. The function will error out
   * if the number of levels in the tree is different from exactly
   * 1 (the leaf level), and if `leaf_id` is out of bounds / invalid.
   */
  void set_leaf(uint64_t leaf_id, const NDRange& mbr);

  /**
   * Sets the input MBRs as leaves. This will destroy the existing
   * RTree.
   */
  void set_leaves(const tdb::pmr::vector<NDRange>& mbrs);

  /**
   * Resizes the leaf level. It destroys the upper levels
   * of the tree if they exist.
   * It errors if `num` is smaller than the current number
   * of leaves.
   */
  void set_leaf_num(uint64_t num);

  /**
   * Deserializes the contents of the object from the input buffer based
   * on the format version.
   * It also sets the input domain, as that is not serialized.
   */
  void deserialize(
      Deserializer& deserializer, const Domain* domain, uint32_t version);

  /**
   * Resets the RTree with the input domain and fanout.
   *
   * @param domain The domain to use for the RTree.
   * @param fanout The fanout of the RTree.
   */
  void reset(const Domain* domain, unsigned fanout);

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
  typedef tdb::pmr::vector<NDRange> Level;

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

  /** Memory tracker for the RTree. */
  shared_ptr<MemoryTracker> memory_tracker_;

  /**
   * The domain for which this R-tree provides an index.
   *
   * This member variable can be changed to `const Domain&` after this class is
   * C.41-compliant and its default constructor removed.
   */
  const Domain* domain_;

  /** The fanout of the tree. */
  unsigned fanout_;

  /**
   * The tree levels. The first level is the root. Note that the root
   * always consists of a single MBR.
   */
  tdb::pmr::vector<Level> levels_;

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

  /**
   * Deserializes the contents of the object from the input buffer based
   * on the format version.
   * It also sets the input domain, as that is not serialized.
   *
   * Applicable to versions 1-4
   */
  void deserialize_v1_v4(Deserializer& deserializer, const Domain* domain);

  /**
   * Deserializes the contents of the object from the input buffer based
   * on the format version.
   * It also sets the input domain, as that is not serialized.
   *
   * Applicable to versions >= 5
   */
  void deserialize_v5(Deserializer& deserializer, const Domain* domain);
};

template <BoundingRectanglePredicate T>
TileOverlap RTree::get_tile_overlap(const T& predicate) const {
  TileOverlap overlap;

  // Empty tree
  if (domain_ == nullptr || levels_.empty())
    return overlap;

  // This will keep track of the traversal
  std::deque<Entry> traversal;
  traversal.push_front({0, 0});
  auto leaf_num = levels_.back().size();
  auto height = this->height();

  while (!traversal.empty()) {
    // Get next entry
    auto entry = traversal.front();
    traversal.pop_front();
    const auto& mbr = levels_[entry.level_][entry.mbr_idx_];

    // Get overlap ratio
    const auto ratio = predicate.overlap_ratio(mbr);

    // If there is overlap
    if (ratio != 0.0) {
      // If there is full overlap
      if (ratio == 1.0) {
        auto subtree_leaf_num = this->subtree_leaf_num(entry.level_);
        assert(subtree_leaf_num > 0);
        uint64_t start = entry.mbr_idx_ * subtree_leaf_num;
        uint64_t end = start + std::min(subtree_leaf_num, leaf_num - start) - 1;
        auto tile_range = std::pair<uint64_t, uint64_t>(start, end);
        overlap.tile_ranges_.emplace_back(tile_range);
      } else {  // Partial overlap
        // If this is the leaf level, insert into results
        if (entry.level_ == height - 1) {
          auto mbr_idx_ratio =
              std::pair<uint64_t, double>(entry.mbr_idx_, ratio);
          overlap.tiles_.emplace_back(mbr_idx_ratio);
        } else {  // Insert all "children" to traversal
          auto next_mbr_num = (uint64_t)levels_[entry.level_ + 1].size();
          auto start = entry.mbr_idx_ * fanout_;
          auto end = std::min(start + fanout_ - 1, next_mbr_num - 1);
          for (uint64_t i = start; i <= end; ++i)
            traversal.push_front({entry.level_ + 1, end - (i - start)});
        }
      }
    }
  }

  return overlap;
}

/**
 * Contextualizes an `NDRange` with a `Domain` for use in `get_tile_overlap`
 */
struct NDRangeInDomain {
  const Domain& domain_;
  const NDRange& rectangle_;
  const std::vector<bool> is_default_;

  NDRangeInDomain(const Domain& domain, const NDRange& rectangle)
      : domain_(domain)
      , rectangle_(rectangle)
      , is_default_(rectangle.size(), false) {
  }

  double overlap_ratio(const NDRange& rtree_rectangle) const {
    return domain_.overlap_ratio(rectangle_, is_default_, rtree_rectangle);
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RTREE_H
