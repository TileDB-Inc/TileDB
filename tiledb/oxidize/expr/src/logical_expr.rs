//! Provides definitions for interacting with DataFusion logical expressions.

use datafusion::common::DataFusionError;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::logical_expr::Expr;

/// Returns a list of the names of the columns used in this expression.
pub fn columns(expr: &Expr) -> impl Iterator<Item = &str> {
    expr.column_refs().into_iter().map(|c| c.name.as_ref())
}

/// Returns true if `expr` contains aggregate functions and false otherwise.
pub fn has_aggregate_functions(expr: &Expr) -> bool {
    let rec = expr.visit(&mut AggregateFunctionChecker::default());
    let rec = {
        // SAFETY: AggregateFunctionChecker does not return any errors
        rec.unwrap()
    };
    matches!(rec, TreeNodeRecursion::Stop)
}

#[derive(Default)]
struct AggregateFunctionChecker {}

impl TreeNodeVisitor<'_> for AggregateFunctionChecker {
    type Node = Expr;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion, DataFusionError> {
        if matches!(node, Expr::AggregateFunction(_)) {
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    }
}
