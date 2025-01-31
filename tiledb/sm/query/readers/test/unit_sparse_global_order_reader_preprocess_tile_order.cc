/**
 * @file unit_sparse_global_order_reader_preprocess_tile_order.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * Tests the `SparseGlobalOrderReader` preprocess tile merge functionality.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/query/readers/sparse_global_order_reader.h"
#include "tiledb/sm/query/readers/sparse_index_reader_base.h"

struct VerifySetCursorFromReadState;

// forward declarations of `showValue` overloads
// (these must be declared prior to including `rapidcheck/Show.hpp` for some
// reason)
namespace rc {
namespace detail {

void showValue(const tiledb::sm::ResultTileId& rt, std::ostream& os);
void showValue(const tiledb::sm::FragIdx& f, std::ostream& os);
void showValue(const VerifySetCursorFromReadState& instance, std::ostream& os);

}  // namespace detail
}  // namespace rc

#include <test/support/assert_helpers.h>
#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

using namespace tiledb::sm;
using tiledb::test::AsserterCatch;
using tiledb::test::AsserterRapidcheck;

struct NotAsync {
  void wait_for(uint64_t) {
    // no-op
  }

  static NotAsync INSTANCE;
};

NotAsync NotAsync::INSTANCE = NotAsync();

/*
static bool has_not_started_yet(
    const FragIdx& fragment_read_state, uint64_t tile) {
  if (fragment_read_state.tile_idx_ == tile) {
    return fragment_read_state.cell_idx_ == 0;
  } else {
    return fragment_read_state.tile_idx_ < tile;
  }
}

static bool has_not_started_yet(
    const FragIdx& fragment_read_state, const ResultTileId& tile) {
  return has_not_started_yet(fragment_read_state, tile.tile_idx_);
}
*/

struct VerifySetCursorFromReadState {
  std::map<unsigned, FragIdx> read_state_;
  std::vector<ResultTileId> qualified_tiles_;

  VerifySetCursorFromReadState(
      std::map<unsigned, FragIdx>&& read_state,
      std::vector<ResultTileId>&& qualified_tiles)
      : read_state_(std::move(read_state))
      , qualified_tiles_(qualified_tiles) {
  }

  VerifySetCursorFromReadState(
      std::vector<FragIdx>&& read_state,
      std::vector<ResultTileId>&& qualified_tiles)
      : qualified_tiles_(std::move(qualified_tiles)) {
    for (unsigned f = 0; f < read_state.size(); f++) {
      if (read_state[f].tile_idx_ != 0 || read_state[f].cell_idx_ != 0) {
        read_state_[f].swap(read_state[f]);
      }
    }
  }

  RelevantFragments relevant_fragments() const {
    std::set<unsigned> distinct_fragments;
    for (const auto& rt : qualified_tiles_) {
      distinct_fragments.insert(rt.fragment_idx_);
    }

    std::vector<unsigned> relevant_fragments(
        distinct_fragments.begin(), distinct_fragments.end());
    return RelevantFragments(relevant_fragments);
  }

  /** validate input */
  template <typename Asserter>
  void validate() {
    if (qualified_tiles_.empty()) {
      ASSERTER(read_state_.empty());
    }

    // each fragment must have its tiles sorted
    std::map<unsigned, uint64_t> last_tile;
    for (const auto& rt : qualified_tiles_) {
      if (last_tile.find(rt.fragment_idx_) != last_tile.end()) {
        RC_PRE(last_tile[rt.fragment_idx_] < rt.tile_idx_);
      }
      last_tile[rt.fragment_idx_] = rt.tile_idx_;
    }

    for (const auto& fragment : read_state_) {
      const unsigned f = fragment.first;
      const auto& frag_idx = fragment.second;
      if (last_tile.find(f) != last_tile.end()) {
        // read state does not have to be a tile in the list,
        // but if it isn't then its cell had better be zero
        bool found = frag_idx.cell_idx_ == 0;
        for (size_t rti = 0; !found && rti < qualified_tiles_.size(); rti++) {
          const auto& rt = qualified_tiles_[rti];
          if (rt.fragment_idx_ == f && rt.tile_idx_ == frag_idx.tile_idx_) {
            found = true;
          }
        }
        RC_PRE(found);
      } else {
        // no read state
        RC_PRE(frag_idx.tile_idx_ == 0);
        RC_PRE(frag_idx.cell_idx_ == 0);
      }
    }
  }

  template <typename Asserter>
  uint64_t verify() {
    validate<Asserter>();

    std::vector<FragIdx> read_state;
    if (!qualified_tiles_.empty()) {
      read_state.resize(
          1 + std::max_element(
                  qualified_tiles_.begin(),
                  qualified_tiles_.end(),
                  [](const auto& a, const auto& b) {
                    return a.fragment_idx_ < b.fragment_idx_;
                  })
                  ->fragment_idx_);
    }
    for (const auto& fragment : read_state_) {
      read_state[fragment.first] = std::move(
          FragIdx(fragment.second.tile_idx_, fragment.second.cell_idx_));
    }

    const uint64_t cursor = PreprocessTileOrder::compute_cursor_from_read_state(
        relevant_fragments(), read_state, qualified_tiles_, NotAsync::INSTANCE);
    ASSERTER(cursor <= qualified_tiles_.size());

    for (uint64_t rti = qualified_tiles_.size(); rti > cursor; rti--) {
      const auto& rt = qualified_tiles_[rti - 1];
      const auto& rstate = read_state[rt.fragment_idx_];
      ASSERTER(rstate.tile_idx_ <= rt.tile_idx_);
      if (rstate.tile_idx_ == rt.tile_idx_) {
        ASSERTER(rstate.cell_idx_ == 0);
      }
    }
    return cursor;
  }
};

// rapidcheck generators
namespace rc {

namespace detail {

void showValue(const ResultTileId& rt, std::ostream& os) {
  os << "ResultTileId { .fragment_idx_ = " << rt.fragment_idx_
     << ", .tile_idx_ = " << rt.tile_idx_ << "}" << std::endl;
}

void showValue(const FragIdx& f, std::ostream& os) {
  os << "FragIdx { .tile_idx_ = " << f.tile_idx_
     << ", .cell_idx_ = " << f.cell_idx_ << "}" << std::endl;
}

void showValue(const VerifySetCursorFromReadState& instance, std::ostream& os) {
  os << ".read_state_ = ";
  showValue(instance.read_state_, os);
  os << ".qualified_tiles_ = ";
  showValue(instance.qualified_tiles_, os);
}

}  // namespace detail

template <>
struct Arbitrary<ResultTileId> {
  static Gen<ResultTileId> arbitrary() {
    return gen::apply(
        [](unsigned f, uint64_t t) { return ResultTileId(f, t); },
        gen::inRange<unsigned>(0, 1024),
        gen::inRange<uint64_t>(0, 1024 * 1024));
  }
};

Gen<std::vector<ResultTileId>> make_qualified_tiles() {
  auto gen_rts =
      gen::container<std::vector<ResultTileId>>(gen::arbitrary<ResultTileId>());

  return gen::map(gen_rts, [](std::vector<ResultTileId> rts) {
    std::vector<ResultTileId> rts_out(rts.size());
    std::map<uint64_t, std::vector<uint64_t>> fragments;
    for (size_t rt = 0; rt < rts.size(); rt++) {
      fragments[rts[rt].fragment_idx_].push_back(rt);
    }
    for (auto& f : fragments) {
      const auto idx_unsorted = f.second;
      auto idx_sorted = idx_unsorted;
      std::sort(idx_sorted.begin(), idx_sorted.end(), [&](size_t a, size_t b) {
        return rts[a].tile_idx_ < rts[b].tile_idx_;
      });

      for (size_t fi = 0; fi < idx_sorted.size(); fi++) {
        rts_out[idx_unsorted[fi]] = rts[idx_sorted[fi]];
      }
    }
    return rts_out;
  });
}

template <>
struct Arbitrary<VerifySetCursorFromReadState> {
  static Gen<VerifySetCursorFromReadState> arbitrary() {
    auto gen_tiles = make_qualified_tiles();

    auto gen_read_states =
        gen::mapcat(gen_tiles, [](std::vector<ResultTileId> tiles) {
          if (tiles.empty()) {
            return gen::pair(
                gen::just(tiles),
                gen::construct<std::vector<std::pair<unsigned, FragIdx>>>());
          }
          return gen::pair(
              gen::just(tiles),
              gen::container<std::vector<std::pair<unsigned, FragIdx>>>(
                  gen::apply(
                      [](ResultTileId qualified_tile, Maybe<uint64_t> cell_idx)
                          -> std::pair<unsigned, FragIdx> {
                        if (cell_idx) {
                          return std::make_pair(
                              qualified_tile.fragment_idx_,
                              FragIdx(qualified_tile.tile_idx_, *cell_idx));
                        } else {
                          return std::make_pair(
                              qualified_tile.fragment_idx_,
                              FragIdx(qualified_tile.tile_idx_ + 1, 0));
                        }
                      },
                      gen::elementOf(tiles),
                      gen::maybe(
                          gen::inRange<uint64_t>(0, 1024 * 1024 * 128)))));
        });

    return gen::apply(
        [](std::pair<
            std::vector<ResultTileId>,
            std::vector<std::pair<unsigned, FragIdx>>> arg) {
          std::vector<ResultTileId> tiles = std::move(arg.first);
          std::map<unsigned, FragIdx> read_state;

          for (auto& state : arg.second) {
            read_state[state.first].swap(state.second);
          }

          return VerifySetCursorFromReadState(
              std::move(read_state), std::move(tiles));
        },
        gen_read_states);
  }
};

}  // namespace rc

TEST_CASE(
    "SparseGlobalOrderReader: PreprocessTileMerge: correct cursor "
    "computation",
    "[sparse-global-order][preprocess-tile-merge]") {
  using RT = ResultTileId;

  SECTION("Example") {
    std::vector<FragIdx> read_state;
    read_state.resize(10);

    // partially done fragment
    read_state[4] = std::move(FragIdx(7, 32));
    // done, no more tiles in this fragment
    read_state[6] = std::move(FragIdx(15, 0));
    // other fragments not started

    std::vector<ResultTileId> tiles = {
        RT(6, 8),
        RT(6, 9),
        RT(6, 10),
        RT(6, 11),
        RT(6, 12),
        RT(6, 13),
        RT(6, 14),
        RT(4, 4),
        RT(4, 5),
        RT(4, 6),
        RT(4, 7),
        RT(8, 32),
        RT(4, 8),
        RT(8, 33),
        RT(8, 34),
        RT(8, 35),
        RT(8, 36)};

    const auto cursor =
        VerifySetCursorFromReadState(std::move(read_state), std::move(tiles))
            .verify<AsserterCatch>();
    CHECK(cursor == 11);
  }

  SECTION("Shrink", "Some examples found by rapidcheck") {
    SECTION("Example 1") {
      std::vector<FragIdx> read_state;
      read_state.emplace_back(0, 1);

      auto tiles = std::vector<ResultTileId>{ResultTileId(0, 0)};

      const auto cursor =
          VerifySetCursorFromReadState(std::move(read_state), std::move(tiles))
              .verify<AsserterCatch>();
      CHECK(cursor == 1);
    }
    SECTION("Example 2") {
      std::vector<FragIdx> read_state;
      read_state.emplace_back(0, 1);
      read_state.emplace_back(0, 0);

      auto tiles = std::vector<ResultTileId>{
          ResultTileId(0, 0), ResultTileId(1, 0), ResultTileId(0, 1)};

      const auto cursor =
          VerifySetCursorFromReadState(std::move(read_state), std::move(tiles))
              .verify<AsserterCatch>();
      CHECK(cursor == 1);
    }
    SECTION("Example 3") {
      std::vector<FragIdx> read_state;
      read_state.emplace_back(0, 0);
      read_state.emplace_back(0, 1);

      auto tiles = std::vector<ResultTileId>{
          ResultTileId(0, 0),
          ResultTileId(1, 0),
          ResultTileId(0, 1),
          ResultTileId(0, 2),
          ResultTileId(0, 3)};

      const auto cursor =
          VerifySetCursorFromReadState(std::move(read_state), std::move(tiles))
              .verify<AsserterCatch>();
      CHECK(cursor == 2);
    }
    SECTION("Example 4") {
      std::vector<FragIdx> read_state;
      read_state.emplace_back(0, 0);
      read_state.emplace_back(0, 0);
      read_state.emplace_back(0, 0);

      std::vector<RT> tiles = {
          RT(2, 0),
          RT(0, 0),
          RT(0, 1),
          RT(0, 2),
          RT(0, 3),
          RT(0, 4),
          RT(0, 5),
          RT(2, 1)};

      const auto cursor =
          VerifySetCursorFromReadState(std::move(read_state), std::move(tiles))
              .verify<AsserterCatch>();
      CHECK(cursor == 0);
    }
    SECTION(
        "Example 5", "Read state cell_idx=0 can be used as the bound tile") {
      std::vector<FragIdx> read_state;
      read_state.emplace_back(3, 0);
      read_state.emplace_back(0, 0);
      read_state.emplace_back(0, 0);

      std::vector<RT> tiles = {
          RT(2, 0),
          RT(0, 0),
          RT(0, 1),
          RT(0, 2),
          RT(0, 3),
          RT(0, 4),
          RT(0, 5),
          RT(2, 1)};

      const auto cursor =
          VerifySetCursorFromReadState(std::move(read_state), std::move(tiles))
              .verify<AsserterCatch>();
      CHECK(cursor == 4);
    }
    SECTION("Example 6") {
      std::vector<FragIdx> read_state;
      read_state.emplace_back(0, 0);
      read_state.emplace_back(0, 1);

      std::vector<RT> tiles = {RT(0, 0), RT(0, 1), RT(1, 0), RT(0, 2)};

      const auto cursor =
          VerifySetCursorFromReadState(std::move(read_state), std::move(tiles))
              .verify<AsserterCatch>();
      CHECK(cursor == 3);
    }
  }

  SECTION("Rapidcheck") {
    rc::prop(
        "verify_set_cursor_from_read_state",
        [](VerifySetCursorFromReadState input) {
          input.verify<AsserterRapidcheck>();
        });
  }
}
