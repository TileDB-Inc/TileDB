/**
 * @file attribute_order_validator.h
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
 * Class for validating fragment order for ordered attributes.
 */

#ifndef TILEDB_ATTRIBUTE_ORDER_VALIDATOR_H
#define TILEDB_ATTRIBUTE_ORDER_VALIDATOR_H

#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/query/readers/result_tile.h"

#include <vector>

using namespace tiledb::common;

class ArraySchema;

namespace tiledb::sm {

class AttributeOrderValidatorStatusException : public StatusException {
 public:
  explicit AttributeOrderValidatorStatusException(const std::string& message)
      : StatusException("ReaderBase", message) {
  }
};

/**
 * Utilitary function that returns if a value is contained in a non empty
 * domain.
 *
 * @tparam Index type
 * @param v Value to check.
 * @param domain Pointer to the domain values.
 * @return `true` if the value is in the given domain and `false` if it is not.
 */
template <typename IndexType>
bool in_domain(IndexType v, const IndexType* domain) {
  return v >= domain[0] && v <= domain[1];
};

class AttributeOrderValidator {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  AttributeOrderValidator() = delete;

  /**
   * Construct a new Order Validation Data object.
   *
   * @param attribute_name Name of the attribute to validate.
   * @param num_frags Number of fragments.
   */
  AttributeOrderValidator(
      const std::string& attribute_name,
      uint64_t num_frags,
      shared_ptr<MemoryTracker> memory_tracker)
      : memory_tracker_(memory_tracker)
      , attribute_name_(attribute_name)
      , result_tiles_to_load_(num_frags)
      , per_fragment_validation_data_(num_frags) {
  }

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns 'true' if tiles need to be loaded. */
  inline bool need_to_load_tiles() {
    for (auto& rt_map : result_tiles_to_load_) {
      if (!rt_map.empty()) {
        return true;
      }
    }
    return false;
  }

  /** Returns a vector of pointers to tiles to load. */
  std::vector<ResultTile*> tiles_to_load() {
    std::vector<ResultTile*> ret;
    uint64_t size = 0;
    for (auto& rt_map : result_tiles_to_load_) {
      size += rt_map.size();
    }

    ret.reserve(size);
    for (auto& rt_map : result_tiles_to_load_) {
      for (auto& rt : rt_map) {
        ret.emplace_back(&rt.second);
      }
    }
    std::sort(ret.begin(), ret.end(), result_tile_cmp);

    return ret;
  }

  /**
   * Find fragments to check against, for each bounds, or mark the bound as
   * already validated if we don't need further validation.
   *
   * For fragments with adjacent non-empty domains, we only need to check the
   * interface once. This method marks all fragment bounds as validated except
   * the bound on the most recent fragment.
   *
   * This also marks as validate fragment bounds that are covered or a min/max
   * for the entire array's non-empty domain, as no check is needed in these
   * cases.
   *
   * @tparam Index type.
   * @param array_min_idx Minimum index value for the array.
   * @param array_max_idx Maximum index value for the array.
   * @param f Fragment index.
   * @param non_empty_domains Vector of pointers to the non-empty domains for
   *     each fragments.
   */
  template <typename IndexType>
  void find_fragments_to_check(
      IndexType array_min_idx,
      IndexType array_max_idx,
      uint64_t f,
      const std::vector<const void*>& non_empty_domains) {
    // Set useful variables for this fragment.
    auto& val_data = per_fragment_validation_data_[f];
    const IndexType* non_empty_domain =
        static_cast<const IndexType*>(non_empty_domains[f]);
    auto min = non_empty_domain[0];
    auto max = non_empty_domain[1];

    // If the fragment minimum is also the array minimum, then it necessarily
    // satisfies the required ordering.
    val_data.min_validated_ = (min == array_min_idx);

    // If the fragment maximum is also the array maximum, then it necessarily
    // satisfies the required ordering.
    val_data.max_validated_ = (max == array_max_idx);

    // If both bounds are validated, then no fragments need to be checked and we
    // can return.
    if (val_data.min_validated_ && val_data.max_validated_) {
      return;
    }

    // Check if fragment is covered or already being checked.
    for (uint64_t f2 = non_empty_domains.size() - 1; f2 > f; --f2) {
      // Get the non-empty domain for this fragment.
      const IndexType* non_empty_domain2 =
          static_cast<const IndexType*>(non_empty_domains[f2]);

      // Check if the lower bound can be validated.
      if (!val_data.min_validated_) {
        // See if the min is covered by this fragment.
        val_data.min_validated_ |= in_domain(min, non_empty_domain2);

        // If the min is next to the max of a more recent fragment, it will
        // be validated when processing that fragment.
        val_data.min_validated_ |= min - 1 == non_empty_domain2[1];
      }

      // Check if the upper bound can be validated.
      if (!val_data.max_validated_) {
        // See if the max is covered.
        val_data.max_validated_ |= in_domain(max, non_empty_domain2);

        // If the max is next to the min of a more recent fragment, it will
        // be validated when processing that fragment.
        val_data.max_validated_ |= max + 1 == non_empty_domain2[0];
      }

      // If both bounds are validated, then no fragments need to be checked and
      // we can return.
      if (val_data.min_validated_ && val_data.max_validated_) {
        return;
      }
    }

    // Get fragment to check against for both the lower and upper boundaries
    // of this fragment.
    bool finished_lower_search = val_data.min_validated_;
    bool finished_upper_search = val_data.max_validated_;
    for (int64_t f2 = f - 1; f2 >= 0; --f2) {
      // Get the non-empty domain for this fragment.
      const IndexType* non_empty_domain2 =
          static_cast<const IndexType*>(non_empty_domains[f2]);

      // If not validated and fragment to check is not yet found, check if
      // this fragment is over-lapping or directly preceeding the min.
      if (!finished_lower_search && (in_domain(min, non_empty_domain2) ||
                                     min - 1 == non_empty_domain2[1])) {
        val_data.min_frag_to_compare_to_ = f2;
        finished_lower_search = true;
      }

      // If not validated and fragment to check is not yet found, check if
      // this fragment is over-lapping or directly following the max.
      if (!finished_upper_search && (in_domain(max, non_empty_domain2) ||
                                     max + 1 == non_empty_domain2[0])) {
        val_data.max_frag_to_compare_to_ = f2;
        finished_upper_search = true;
      }

      // If both fragments are either validated or the matching fragment was
      // found, then we are done searching.
      if (finished_lower_search && finished_upper_search) {
        return;
      }
    }

    // If the search/validation failed, then there is a discontinuity in this
    // array.
    throw AttributeOrderValidatorStatusException(
        "Discontinuity found in array domain");
  }

  /**
   * Performs validation that can be done without loading tile.
   *
   * If a validation check fails, this will throw an error. This requires
   * `find_fragments_to_check` to be ran prior to execution.
   *
   * @tparam Index type.
   * @tparam Attribute type.
   * @param index_dim The index dimension.
   * @param f Fragment index.
   * @param non_empty_domains Vector of pointers to the non-empty domain for
   *     each fragment.
   */
  template <typename IndexType, typename AttributeType>
  void validate_without_loading_tiles(
      const Dimension* index_dim,
      bool increasing_data,
      uint64_t f,
      const std::vector<const void*>& non_empty_domains,
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
      const std::vector<uint64_t>& frag_first_array_tile_idx) {
    // For easy reference.
    auto& val_data = per_fragment_validation_data_[f];
    const IndexType* non_empty_domain =
        static_cast<const IndexType*>(non_empty_domains[f]);
    const IndexType* dim_dom = index_dim->domain().typed_data<IndexType>();
    auto tile_extent{index_dim->tile_extent().rvalue_as<IndexType>()};

    if (!val_data.min_validated_) {
      // Get the fragment number to compare against.
      auto f2 = val_data.min_frag_to_compare_to_.value();

      // Get the min index. See if the min is tile aligned.
      auto min = non_empty_domain[0];
      bool min_tile_aligned = min == index_dim->round_to_tile<IndexType>(
                                         min, dim_dom[0], tile_extent);

      // Get the tile index of the tile in f2 immediately proceeding f (with
      // respect to tile index in f2). If f is tile aligned we need to subtract
      // an additional value.
      uint64_t f2_tile_idx = frag_first_array_tile_idx[f] -
                             frag_first_array_tile_idx[f2] - min_tile_aligned;

      // If we are tile aligned or non-overlapping (the min is right next to
      // the other fragment's max), we can validate. Otherwise, we'll need to
      // load the tile.
      const IndexType* non_empty_domain2 =
          static_cast<const IndexType*>(non_empty_domains[f2]);
      if (min_tile_aligned || min - 1 == non_empty_domain2[1]) {
        val_data.min_validated_ = true;

        // Check the order.
        if (increasing_data) {
          // Increasing data: The first value on f is the minimum on f. Check
          // that it is greater than the last (maximum) value in the proceeding
          // tile on f2.
          auto value = fragment_metadata[f]
                           ->loaded_metadata()
                           ->get_tile_min_as<AttributeType>(attribute_name_, 0);

          auto value_previous = fragment_metadata[f2]
                                    ->loaded_metadata()
                                    ->get_tile_max_as<AttributeType>(
                                        attribute_name_, f2_tile_idx);

          if (value_previous >= value) {
            throw AttributeOrderValidatorStatusException(
                "Attribute out of order");
          }
        } else {
          // Decreasing data: The first value on f is the maximum of f. Check
          // that is is less than the last (minimum) value in the proceeding
          // tile on f2.
          auto value = fragment_metadata[f]
                           ->loaded_metadata()
                           ->get_tile_max_as<AttributeType>(attribute_name_, 0);

          auto value_previous = fragment_metadata[f2]
                                    ->loaded_metadata()
                                    ->get_tile_min_as<AttributeType>(
                                        attribute_name_, f2_tile_idx);

          if (value_previous <= value) {
            throw AttributeOrderValidatorStatusException(
                "Attribute out of order");
          }
        }
      } else {
        // Add the tile to the list of tiles to load.
        add_tile_to_load(f, true, f2, f2_tile_idx, fragment_metadata[f2]);
      }
    }

    if (!val_data.max_validated_) {
      // Get the fragment number to compare against.
      auto f2 = val_data.max_frag_to_compare_to_.value();

      // Get the max index and max tile index. See if the max is tile aligned.
      auto max = non_empty_domain[1];
      auto max_tile_idx = fragment_metadata[f]->tile_num() - 1;
      bool max_tile_aligned = max + 1 == index_dim->round_to_tile<IndexType>(
                                             max + 1, dim_dom[0], tile_extent);

      // Get the tile index of the tile in f2 immediately following f (with
      // respect to tile index in f2). If f is tile aligned we need to add an
      // additional value.
      uint64_t f2_tile_idx = max_tile_idx + frag_first_array_tile_idx[f] -
                             frag_first_array_tile_idx[f2] + max_tile_aligned;

      // If we are tile aligned or non-overlapping (the max is right next to
      // the other fragment's min), we can validate. Otherwise, we'll need to
      // load the tile.
      const IndexType* non_empty_domain2 =
          static_cast<const IndexType*>(non_empty_domains[f2]);
      if (max_tile_aligned || max + 1 == non_empty_domain2[0]) {
        val_data.max_validated_ = true;

        // Check the order.
        if (increasing_data) {
          // Increasing data: The last value on f is the maximum on f. Check
          // that is less than the first (minimum) value on the following
          // tile in f2.
          auto value = fragment_metadata[f]
                           ->loaded_metadata()
                           ->get_tile_max_as<AttributeType>(
                               attribute_name_, max_tile_idx);
          auto value_next = fragment_metadata[f2]
                                ->loaded_metadata()
                                ->get_tile_min_as<AttributeType>(
                                    attribute_name_, f2_tile_idx);
          if (value_next <= value) {
            throw AttributeOrderValidatorStatusException(
                "Attribute out of order");
          }

        } else {
          // Decreasinging data: The last value on f is the minimum on f. Check
          // that is greater than the first (maximum) value on the following
          // tile in f2.
          auto value = fragment_metadata[f]
                           ->loaded_metadata()
                           ->get_tile_min_as<AttributeType>(
                               attribute_name_, max_tile_idx);
          auto value_next = fragment_metadata[f2]
                                ->loaded_metadata()
                                ->get_tile_max_as<AttributeType>(
                                    attribute_name_, f2_tile_idx);
          if (value_next >= value) {
            throw AttributeOrderValidatorStatusException(
                "Attribute out of order");
          }
        }

      } else {
        // Add the tile to the list of tiles to load.
        add_tile_to_load(f, false, f2, f2_tile_idx, fragment_metadata[f2]);
      }
    }
  }

  /**
   * Performs validation that requires tile data.
   *
   * For best performance, only execute this method after first validating
   * without loading tiles.  This requires `find_fragments_to_check` to be
   * ran prior to execution.  If a validation check fails, this will throw an
   * error.
   *
   * @tparam Index type.
   * @tparam Attribute type.
   * @param index_dim The index dimension.
   * @param f Fragment index.
   * @param non_empty_domains Vector of pointers to the non-empty domain for
   *     each fragment.
   */
  template <typename IndexType, typename AttributeType>
  void validate_with_loaded_tiles(
      const Dimension* index_dim,
      bool increasing_data,
      uint64_t f,
      const std::vector<const void*>& non_empty_domains,
      const std::vector<shared_ptr<FragmentMetadata>> fragment_metadata,
      const std::vector<uint64_t>& frag_first_array_tile_idx) {
    auto& val_data = per_fragment_validation_data_[f];
    const IndexType* non_empty_domain =
        static_cast<const IndexType*>(non_empty_domains[f]);
    const IndexType* dim_dom = index_dim->domain().typed_data<IndexType>();
    auto tile_extent = index_dim->tile_extent().rvalue_as<IndexType>();

    if (!val_data.min_validated_) {
      // Get the min of the current fragment.
      auto value = fragment_metadata[f]
                       ->loaded_metadata()
                       ->get_tile_min_as<AttributeType>(attribute_name_, 0);

      // Get the previous value from the loaded tile.
      auto rt = min_tile_to_compare_against(f);
      const auto cell_idx =
          non_empty_domain[0] -
          index_dim->tile_coord_low(
              rt->tile_idx() + frag_first_array_tile_idx[rt->frag_idx()],
              dim_dom[0],
              tile_extent) -
          1;
      AttributeType value_previous =
          rt->attribute_value<AttributeType>(attribute_name_, cell_idx);

      // Validate the order.
      if (increasing_data) {
        if (value_previous >= value) {
          throw AttributeOrderValidatorStatusException(
              "Attribute out of order");
        }
      } else {
        if (value_previous <= value) {
          throw AttributeOrderValidatorStatusException(
              "Attribute out of order");
        }
      }
    }

    if (!val_data.max_validated_) {
      // Get the min of the current fragment.
      auto max_tile_idx = fragment_metadata[f]->tile_num() - 1;
      auto value =
          fragment_metadata[f]
              ->loaded_metadata()
              ->get_tile_max_as<AttributeType>(attribute_name_, max_tile_idx);

      // Get the previous value from the loaded tile.
      auto rt = max_tile_to_compare_against(f);
      const auto cell_idx =
          non_empty_domain[1] -
          index_dim->tile_coord_low(
              rt->tile_idx() + frag_first_array_tile_idx[rt->frag_idx()],
              dim_dom[0],
              tile_extent) +
          1;
      AttributeType value_next =
          rt->attribute_value<AttributeType>(attribute_name_, cell_idx);

      // Validate the order.
      if (increasing_data) {
        if (value >= value_next) {
          throw AttributeOrderValidatorStatusException(
              "Attribute out of order");
        }
      } else {
        if (value <= value_next) {
          throw AttributeOrderValidatorStatusException(
              "Attribute out of order");
        }
      }
    }
  }

 private:
  /* ********************************* */
  /*       PRIVATE DECLARATIONS        */
  /* ********************************* */

  class AttributeOrderValidationData {
   public:
    /** Has the min has been validated already. */
    bool min_validated_{false};

    /** Has the max has been validated already. */
    bool max_validated_{false};

    /*
     * Which fragment index to validate the min bound against.
     *
     * If it was possible to validate the value without looking at another
     * fragment, this will be nullopt.
     */
    optional<uint64_t> min_frag_to_compare_to_{nullopt};

    /*
     * Which fragment index to validate the max bound against.
     *
     * If it was possible to validate the value without looking at another
     * fragment, this will be nullopt.
     */
    optional<uint64_t> max_frag_to_compare_to_{nullopt};

    /**
     * Which tile we should compare data against for the min. The value is an
     * index into `result_tiles_to_load_`.
     */
    optional<uint64_t> min_tile_to_compare_to_{nullopt};

    /**
     * Which tile we should compare data against for the max. The value is an
     * index into `result_tiles_to_load_`.
     */
    optional<uint64_t> max_tile_to_compare_to_{nullopt};
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The memory tracker to use. */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** Name of the attribute to validate. */
  std::string attribute_name_;

  /** Map of result tiles to load, per fragments. */
  std::vector<std::unordered_map<uint64_t, ResultTile>> result_tiles_to_load_;

  /** Attribute order validation data, per fragment. */
  std::vector<AttributeOrderValidationData> per_fragment_validation_data_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /**
   * Add a tile to compare against when running the order validation against
   * tile data.
   *
   * @param f Current fragment index.
   * @param is_lower_bound Is this for the lower bound or upper bound.
   * @param f_to_compare Fragment index of the tile to compare against.
   * @param t_to_compare Tile index of the tile to compare against.
   * @param schema Array schema.
   */
  inline void add_tile_to_load(
      unsigned f,
      bool is_lower_bound,
      uint64_t f_to_compare,
      uint64_t t_to_compare,
      const shared_ptr<FragmentMetadata> fragment_metadata) {
    auto& val_data = per_fragment_validation_data_[f];
    auto it = result_tiles_to_load_[f].find(t_to_compare);
    if (it == result_tiles_to_load_[f].end()) {
      result_tiles_to_load_[f].emplace(
          std::piecewise_construct,
          std::forward_as_tuple(t_to_compare),
          std::forward_as_tuple(
              f_to_compare,
              t_to_compare,
              *fragment_metadata.get(),
              memory_tracker_));
    }

    if (is_lower_bound) {
      val_data.min_tile_to_compare_to_ = t_to_compare;
    } else {
      val_data.max_tile_to_compare_to_ = t_to_compare;
    }
  }

  /**
   * Return the tile, for the fragment min, to compare against.
   *
   * @param f Fragment index.
   * @return Tile to compare against.
   */
  inline ResultTile* min_tile_to_compare_against(unsigned f) {
    auto idx = per_fragment_validation_data_[f].min_tile_to_compare_to_.value();
    auto iter = result_tiles_to_load_[f].find(idx);
    if (iter == result_tiles_to_load_[f].end()) {
      throw std::runtime_error("Invalid minimum tile index.");
    }
    return &(iter->second);
  }

  /**
   * Return the tile, for the fragment max, to compare against.
   *
   * @param f Fragment index.
   * @return Tile to compare against.
   */
  inline ResultTile* max_tile_to_compare_against(unsigned f) {
    auto idx = per_fragment_validation_data_[f].max_tile_to_compare_to_.value();
    auto iter = result_tiles_to_load_[f].find(idx);
    if (iter == result_tiles_to_load_[f].end()) {
      throw std::runtime_error("Invalid maximum tile index.");
    }
    return &(iter->second);
  }
};

}  // namespace tiledb::sm

#endif
