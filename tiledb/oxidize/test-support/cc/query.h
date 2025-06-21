#ifndef TILEDB_OXIDIZE_TEST_SUPPORT_QUERY_H
#define TILEDB_OXIDIZE_TEST_SUPPORT_QUERY_H

#include "tiledb/oxidize/rust.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/query/ast/query_ast.h"

namespace tiledb::test::oxidize {

using namespace tiledb::sm;

std::shared_ptr<ASTNode> new_ast_value_node(
    const std::string& field,
    QueryConditionOp op,
    rust::slice<const uint8_t> value);

std::shared_ptr<ASTNode> new_ast_value_node_null(
    const std::string& field, QueryConditionOp op);

std::shared_ptr<ASTNode> new_ast_value_node_var(
    const std::string& field,
    QueryConditionOp op,
    rust::slice<const uint8_t> value,
    rust::slice<const uint64_t> offsets);

std::shared_ptr<ASTNode> new_ast_combination(
    std::shared_ptr<ASTNode> left,
    std::shared_ptr<ASTNode> right,
    QueryConditionCombinationOp op);

std::shared_ptr<ASTNode> new_ast_negate(std::shared_ptr<ASTNode> arg);

}  // namespace tiledb::test::oxidize

#endif
