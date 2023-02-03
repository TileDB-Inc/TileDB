/**
 * @file tiledb/sm/query/domain_buffer.h
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
 * This file defines a data-view class for Domain buffers
 */
#ifndef TILEDB_QUERY_DOMAIN_BUFFER_H
#define TILEDB_QUERY_DOMAIN_BUFFER_H

#include <new>

#include "../query_buffer.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/array_schema/domain_data_ref.h"
#include "tiledb/sm/array_schema/domain_typed_data_view.h"

namespace tiledb::sm {

namespace detail {
class DomainBuffersTypes {
 public:
  /**
   * Buffer type for an individual dimension.
   *
   * The pointer declaration here is central to the "view" aspect of this class.
   * The lifespan of the `QueryBuffer` objects is determined externally to this
   * class.
   */
  using per_dimension_type = const QueryBuffer*;

  /**
   * The storage type for the list of buffers.
   *
   * TODO: Convert this to use `DynamicArray`
   */
  using storage_type = std::vector<per_dimension_type>;

  /**
   * The type of the sizes and indices of the storage type
   */
  using size_type = storage_type::size_type;
};
}  // namespace detail

/**
 * A reference to a domain-typed datum. Roughly equivalent to a reference to
 * a DomainTypedDataView.
 */
class DomainBufferDataRef : public detail::DomainBuffersTypes,
                            public type::DomainDataRef {
  /**
   * Friends with DomainBuffersView for its factory.
   */
  friend class DomainBuffersView;

  const Domain& domain_;

  /**
   * The list of buffers, one for each dimension for some domain.
   */
  const storage_type& qb_;

  /**
   * The index into the buffers that this object refers to.
   */
  size_type k_;

 public:
  explicit DomainBufferDataRef(
      const Domain& domain, const storage_type& qb, size_type k)
      : domain_(domain)
      , qb_(qb)
      , k_(k) {
  }

  UntypedDatumView dimension_datum_view(unsigned int i) const override {
    return {qb_[i]->dimension_datum_at(*domain_.dimension_ptr(i), k_).datum()};
  }

  DomainBufferDataRef(const Domain& domain) = delete;
};

/**
 * A container to store data for a single coordinate value across all
 * dimensions.
 */
class SingleCoord {
 public:
  /**
   * Default constructor is prohibited.
   */
  SingleCoord() = delete;

  /**
   * Constructor
   *
   * @param schema Array schema.
   * @param coord Data ref to the coordinate.
   */
  SingleCoord(const ArraySchema& schema, const DomainBufferDataRef& coord)
      : coords_(schema.dim_num())
      , qb_(schema.dim_num())
      , sizes_(schema.dim_num() + 1)
      , single_offset_(1) {
    sizes_[schema.dim_num()] = sizeof(uint64_t);
    for (unsigned d = 0; d < coords_.size(); ++d) {
      bool var_size = schema.dimension_ptr(d)->var_size();
      auto dv = coord.dimension_datum_view(d);
      sizes_[d] = dv.size();
      coords_[d].resize(sizes_[d]);
      memcpy(coords_[d].data(), dv.content(), sizes_[d]);

      if (var_size) {
        qb_[d].set_offsets_buffer(
            &single_offset_[0], &sizes_[schema.dim_num()]);
        qb_[d].set_data_var_buffer(coords_[d].data(), &sizes_[d]);
      } else {
        qb_[d].set_data_buffer(coords_[d].data(), &sizes_[d]);
      }
    }
  }

  /**
   * Construct a SingleCoord object from deserialized data.
   *
   * @param schema The array schema
   * @param coords Deserialized coordinates vector
   * @param sizes Deserialized sizes vector
   * @param single_offset Deserialized offset vector
   */
  SingleCoord(
      const ArraySchema& schema,
      std::vector<std::vector<uint8_t>> coords,
      std::vector<uint64_t> sizes,
      std::vector<uint64_t> single_offset)
      : coords_(schema.dim_num())
      , qb_(schema.dim_num())
      , sizes_(schema.dim_num() + 1)
      , single_offset_(1) {
    sizes_[schema.dim_num()] = sizeof(uint64_t);
    single_offset_ = single_offset;
    for (unsigned d = 0; d < schema.dim_num(); ++d) {
      bool var_size = schema.dimension_ptr(d)->var_size();
      sizes_[d] = sizes[d];
      coords_[d].resize(sizes[d]);
      memcpy(coords_[d].data(), coords[d].data(), sizes[d]);

      if (var_size) {
        qb_[d].set_offsets_buffer(
            single_offset_.data(), sizes_.data() + schema.dim_num());
        qb_[d].set_data_var_buffer(coords_[d].data(), sizes_.data() + d);
      } else {
        qb_[d].set_data_buffer(coords_[d].data(), sizes_.data() + d);
      }
    }
  }

  /**
   * Get the QueryBuffer object for a specific dimension.
   *
   * @param d Dimension index.
   */
  inline QueryBuffer* get_qb(const unsigned d) {
    return &qb_[d];
  }

  inline const std::vector<std::vector<uint8_t>>& get_coords() const {
    return coords_;
  }
  inline const std::vector<uint64_t>& get_sizes() const {
    return sizes_;
  }
  inline const std::vector<uint64_t>& get_single_offset() const {
    return single_offset_;
  }

 private:
  /**
   * The coordinate data, per dimension.
   */
  std::vector<std::vector<uint8_t>> coords_;

  /**
   * Query buffer pointing to the data, per dimension.
   */
  std::vector<QueryBuffer> qb_;

  /**
   * Size of the data, per dimension.
   */
  std::vector<uint64_t> sizes_;

  /**
   * Used as the offsets buffer for a var sized attribute.
   */
  std::vector<uint64_t> single_offset_;
};

/**
 * A non-owning sequence of QueryBuffer pointers, one per dimension of the
 * domain of an open array.
 *
 * This class at present is hardly optimal. It began as a thin rewrite of
 * legacy code and still retains its flavor. It remains a relatively thin
 * wrapper around its storage type.
 */
class DomainBuffersView : public detail::DomainBuffersTypes {
  /**
   * The list of buffers, one for each dimension for some domain.
   */
  storage_type qb_;

 public:
  /**
   * Default constructor is prohibited. An object in a view class is senseless
   * if there's nothing to view.
   */
  DomainBuffersView() = delete;

  /**
   * Constructor
   *
   * TODO: Change argument from `ArraySchema` to `Domain`. The current type is
   * the result of code refactoring.
   *
   * @param schema the schema of an open array
   * @param buffers a buffer map for each dimension of the domain
   */
  DomainBuffersView(
      const ArraySchema& schema,
      const std::unordered_map<std::string, QueryBuffer>& buffers)
      : qb_(schema.dim_num()) {
    auto n_dimensions{schema.dim_num()};
    for (decltype(n_dimensions) i = 0; i < n_dimensions; ++i) {
      const auto& name{schema.dimension_ptr(i)->name()};
      qb_[i] = &buffers.at(name);
    }
  }

  /**
   * Constructor
   *
   * @param domain the domain of an open array
   * @param buffers a buffer map for each dimension of the domain
   */
  DomainBuffersView(
      const Domain& domain,
      const std::unordered_map<std::string, QueryBuffer>& buffers)
      : qb_(domain.dim_num()) {
    auto n_dimensions{domain.dim_num()};
    for (decltype(n_dimensions) i = 0; i < n_dimensions; ++i) {
      const auto& name{domain.dimension_ptr(i)->name()};
      qb_[i] = &buffers.at(name);
    }
  }

  /**
   * Constructor
   *
   * TODO: Change argument from `ArraySchema` to `Domain`. The current type is
   * the result of code refactoring.
   *
   * @param schema the schema of an open array
   * @param coord a single coordinate value
   */
  DomainBuffersView(const ArraySchema& schema, SingleCoord& coord)
      : qb_(schema.dim_num()) {
    auto n_dimensions{schema.dim_num()};
    for (decltype(n_dimensions) i = 0; i < n_dimensions; ++i) {
      qb_[i] = coord.get_qb(i);
    }
  }

  /**
   * Constructor
   *
   * @param domain the domain of an open array
   * @param buffers a buffer vector for each dimension of the domain
   */
  DomainBuffersView(const Domain& domain, std::vector<QueryBuffer>& qb_vector)
      : qb_(domain.dim_num()) {
    auto n_dimensions{domain.dim_num()};
    for (decltype(n_dimensions) i = 0; i < n_dimensions; ++i) {
      qb_[i] = &qb_vector[i];
    }
  }

  /**
   * Accessor to wrapped container.
   */
  [[nodiscard]] inline const storage_type& buffers() const {
    return qb_;
  }

  /**
   * Accessor to an individual element of the container.
   *
   * @param k Dimension index within the domain
   */
  [[nodiscard]] inline per_dimension_type operator[](size_t k) const {
    return qb_[k];
  }

  /**
   * Accessor to an individual element of the container.
   *
   * @param k Dimension index within the domain
   */
  [[nodiscard]] inline per_dimension_type at(size_t k) const {
    return qb_.at(k);
  }

  /**
   * Initializer (Initializer) policy class for DynamicArray for values drawn
   * from a list of QueryBuffer (QB) pointers.
   */
  class InitializerQB {
   public:
    /**
     * Constructs a dimension value drawn from a QueryBuffer that's associated
     * with a domain.
     *
     * Argument `qb` is initialized with member variable `qb_`.
     *
     * @param item Location in which to place a new value as UntypedDatumView
     * @param i Index of item in container; same as dimension index
     * @param domain Domain associated with the value
     * @param qb Container of pointers to query buffers, one per dimension
     * @param k Dimension index with the domain
     */
    inline static void initialize(
        UntypedDatumView* item,
        unsigned int i,
        const Domain& domain,
        const storage_type& qb,
        size_t k) {
      // Construct datum in place with placement-new
      new (item) UntypedDatumView{
          qb[i]->dimension_datum_at(*domain.dimension_ptr(i), k).datum()};
    }
  };

  /**
   * Factory method for DomainTypedDataView. Extracts data at the index from
   * the QueryBuffer for each dimension.
   *
   * @param k Dimension index within the domain
   * @return Domain value at index `k` drawn from the QueryBuffer map given at
   * construction
   */
  [[nodiscard]] DomainTypedDataView domain_data_at(
      const Domain& domain, size_t k) const {
    return {domain, tdb::Tag<InitializerQB>{}, qb_, k};
  }

  /**
   * Factory method for DomainTypedDataRef. Creates a reference to data drawn
   * from the QueryBuffer for each dimension, each at the given index.
   *
   * @param k Dimension index within the domain
   * @return Domain value at index `k` drawn from the QueryBuffer map given at
   * construction
   */
  [[nodiscard]] DomainBufferDataRef domain_ref_at(
      const Domain& domain, size_t k) const {
    return DomainBufferDataRef{domain, qb_, k};
  }
};

}  // namespace tiledb::sm
#endif  // TILEDB_QUERY_DOMAIN_BUFFER_H
