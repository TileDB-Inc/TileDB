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
#include "query_buffer.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain_typed_data_view.h"

namespace tiledb::sm {

/**
 * A non-owning sequence of QueryBuffer pointers, one per dimension of the
 * domain of an open array.
 *
 * This class at present is hardly optimal. It began as a thin rewrite of
 * legacy code and still retains its flavor. It remains a relatively thin
 * wrapper around its storage type.
 */
class DomainBuffersView {
  using per_dimension_type = const QueryBuffer*;
  using storage_type = std::vector<per_dimension_type>;

  storage_type qb_;

 public:
  /// Default constructor is prohibited.
  DomainBuffersView() = delete;

  /**
   * Constructor
   *
   * @param schema the schema of an open array
   * @param buffers a buffer map for each element of the domain and codomain of
   * the array
   */
  DomainBuffersView(
      const ArraySchema& schema,
      const std::unordered_map<std::string, QueryBuffer>& buffers)
      : qb_(schema.dim_num()) {
    auto n_dimensions{schema.dim_num()};
    for (decltype(n_dimensions) i = 0; i < n_dimensions; ++i) {
      const auto& name = schema.dimension(i)->name();
      qb_[i] = &buffers.at(name);
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
  [[nodiscard]] inline per_dimension_type buffer_at(size_t k) const {
    return qb_.at(k);
  }

  class InitializerQB {
   public:
    inline static void initialize(
        UntypedDatumView* item,
        unsigned int i,
        const Domain& domain,
        const storage_type& qb,
        size_t k) {
      // Construct datum in place with placement-new
      new (item) UntypedDatumView{
          qb[i]->dimension_datum_at(*domain.dimension(i), k).datum()};
    }
  };

  /**
   * Factory method for DomainTypedDataView. Extracts data at the index from
   * the QueryBuffer for each dimension.
   *
   * @param k Dimension index within the domain
   */
  [[nodiscard]] DomainTypedDataView domain_data_at(
      const Domain& domain, size_t k) const {
    return DomainTypedDataView{domain, Tag<InitializerQB>{}, qb_, k};
  }
};

}  // namespace tiledb::sm
#endif  // TILEDB_QUERY_DOMAIN_BUFFER_H
