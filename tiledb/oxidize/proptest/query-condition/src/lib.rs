use tiledb_common::query::condition::QueryConditionExpr;
use tiledb_oxidize::sm::query::ast::ASTNode;

pub fn ast_from_query_condition(
    query_condition: &QueryConditionExpr,
) -> anyhow::Result<cxx::SharedPtr<ASTNode>> {
    todo!()
}
