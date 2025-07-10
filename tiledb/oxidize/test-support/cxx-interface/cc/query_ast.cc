#include "tiledb/oxidize/test-support/cxx-interface/cc/query_ast.h"

using namespace tiledb::sm;

namespace tiledb::test::oxidize {

std::shared_ptr<ASTNode> new_ast_value_node(
    const std::string& field,
    QueryConditionOp op,
    rust::slice<const uint8_t> value) {
  return std::make_shared<ASTNodeVal>(field, value.data(), value.length(), op);
}

std::shared_ptr<ASTNode> new_ast_value_node_null(
    const std::string& field, QueryConditionOp op) {
  return std::make_shared<ASTNodeVal>(field, nullptr, 0, op);
}

std::shared_ptr<ASTNode> new_ast_value_node_var(
    const std::string& field,
    QueryConditionOp op,
    rust::slice<const uint8_t> value,
    rust::slice<const uint64_t> offsets) {
  return std::make_shared<ASTNodeVal>(
      field,
      value.data(),
      value.length() * sizeof(uint8_t),
      offsets.data(),
      offsets.length() * sizeof(uint64_t),
      op);
}

std::shared_ptr<ASTNode> new_ast_combination(
    std::shared_ptr<ASTNode> left,
    std::shared_ptr<ASTNode> right,
    QueryConditionCombinationOp op) {
  return left->combine(*right.get(), op);
}

std::shared_ptr<ASTNode> new_ast_negate(std::shared_ptr<ASTNode> arg) {
  return arg->get_negated_tree();
}

}  // namespace tiledb::test::oxidize
