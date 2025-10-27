/**
 * @file test/support/rapidcheck/show.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2025 TileDB, Inc.
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
 * This file provides forward declarations of `rc::detail::showValue`
 * overloads, which seemingly must be included prior to the rapidcheck
 * header files.
 */

#include <test/support/rapidcheck/array_templates.h>
#include <test/support/rapidcheck/show.h>

#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/query/ast/query_ast.h"

namespace rc::detail {

template <DimensionType D>
void showDomain(const templates::Domain<D>& domain, std::ostream& os) {
  os << "[" << domain.lower_bound << ", " << domain.upper_bound << "]";
}

void showValue(const templates::Domain<int>& domain, std::ostream& os) {
  showDomain(domain, os);
}
void showValue(const templates::Domain<int64_t>& domain, std::ostream& os) {
  showDomain(domain, os);
}
void showValue(const templates::Domain<uint64_t>& domain, std::ostream& os) {
  showDomain(domain, os);
}

template <stdx::is_fundamental T>
void showQueryBuffers(const templates::query_buffers<T>& qb, std::ostream& os) {
  show(qb.values_, os);
}

void showValue(const tiledb::sm::ASTNode& node, std::ostream& os) {
  const tiledb::sm::ASTNodeVal* valnode =
      static_cast<const tiledb::sm::ASTNodeVal*>(&node);
  const tiledb::sm::ASTNodeExpr* exprnode =
      dynamic_cast<const tiledb::sm::ASTNodeExpr*>(&node);

  if (valnode) {
    const auto fname = valnode->get_field_name();
    const auto op = valnode->get_op();
    const auto bytes = valnode->get_data();

    std::stringstream value;
    for (size_t b = 0; b < bytes.size(); b++) {
      if (b != 0) {
        value << " ";
      }
      value << "0x" << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(bytes.data()[b]);
    }

    os << fname << " " << query_condition_op_str(op) << " " << value.str();
  } else if (exprnode) {
    const auto op = exprnode->get_combination_op();
    const auto& children = exprnode->get_children();
    for (unsigned i = 0; i < children.size(); i++) {
      if (i != 0) {
        os << " " << query_condition_combination_op_str(op) << " ";
      }
      os << "(";
      showValue(*children[i].get(), os);
      os << ")";
    }
  } else {
    os << "???" << std::endl;
  }
}

}  // namespace rc::detail

namespace rc {

void showValue(const templates::query_buffers<uint64_t>& qb, std::ostream& os) {
  detail::showQueryBuffers(qb, os);
}

}  // namespace rc
