/**
 * @file   ast_helpers.cc
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
 * This file defines some test suite helper functions, specific to ast.
 */

#include <sstream>
#include <string>

#include "ast_helpers.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/ast/query_ast.h"

namespace tiledb::test {

std::string ptr_to_hex_str(const void* data, size_t size) {
  const char* c_data = reinterpret_cast<const char*>(data);
  std::stringstream ss;
  for (size_t i = 0; i < size; ++i) {
    ss << std::setfill('0') << std::setw(2) << std::hex
       << (0xff & (unsigned int)c_data[i]);
    if (i != size - 1) {
      ss << " ";
    }
  }
  return ss.str();
}

std::string bbv_to_hex_str(const tiledb::sm::ByteVecValue& b) {
  std::stringstream ss;
  for (size_t i = 0; i < b.size(); i++) {
    if (b.data()[i] < 16) {
      ss << "0";
    }
    ss << std::hex << +b.data()[i];
    if (i != b.size() - 1) {
      ss << " ";
    }
  }
  return ss.str();
}

std::string ast_node_to_str(const tdb_unique_ptr<tiledb::sm::ASTNode>& node) {
  if (node == nullptr)
    return "";
  std::string result_str;
  if (!node->is_expr()) {
    result_str = node->get_field_name() + " " +
                 query_condition_op_str(node->get_op()) + " ";
    if (node->get_condition_value_view().content()) {
      result_str += ptr_to_hex_str(
          node->get_condition_value_view().content(),
          node->get_condition_value_view().size());
    } else {
      result_str += "null";
    }
    return result_str;
  } else {
    result_str = "(";
    const auto& nodes = node->get_children();
    for (size_t i = 0; i < nodes.size(); i++) {
      result_str += ast_node_to_str(nodes[i]);
      if (i != nodes.size() - 1) {
        result_str += " ";
        result_str +=
            query_condition_combination_op_str(node->get_combination_op());
        result_str += " ";
      }
    }
    result_str += ")";
    return result_str;
  }
}
}  // namespace tiledb::test
