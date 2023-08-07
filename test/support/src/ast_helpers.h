/**
 * @file   ast_helpers.h
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
 * This file declares some test suite helper functions specific to ast.
 */

#ifndef TILEDB_AST_HELPERS_H
#define TILEDB_AST_HELPERS_H

#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/ast/query_ast.h"

namespace tiledb::test {

/**
 * Returns the byte string given a pointer and a size.
 */
std::string ptr_to_hex_str(const void* data, size_t size);

/**
 * Returns the byte string representation of a ByteVecValue.
 */
std::string bbv_to_hex_str(const tiledb::sm::ByteVecValue& b);

/**
 * Returns the string representation of a Query AST node.
 */
std::string ast_node_to_str(const tiledb::sm::ASTNode* node);

template <class T>
inline std::string ast_node_to_str(const tdb_unique_ptr<T>& node) {
  return ast_node_to_str(static_cast<tiledb::sm::ASTNode*>(node.get()));
}

/**
 * Returns whether two ASTs are syntactically equal.
 */
bool ast_equal(const tiledb::sm::ASTNode* lhs, const tiledb::sm::ASTNode* rhs);

template <class T1, class T2>
inline bool ast_equal(
    const tdb_unique_ptr<T1>& lhs, const tdb_unique_ptr<T2>& rhs) {
  return ast_equal(
      static_cast<tiledb::sm::ASTNode*>(lhs.get()),
      static_cast<tiledb::sm::ASTNode*>(rhs.get()));
}

}  // namespace tiledb::test
#endif  //  TILEDB_AST_HELPERS_H
