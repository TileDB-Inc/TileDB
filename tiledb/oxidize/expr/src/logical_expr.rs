//! Provides definitions for interacting with DataFusion logical expressions.

use std::fmt::{Display, Formatter, Result as FmtResult};

use arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{Column, DFSchema, DataFusionError, ScalarValue};
use datafusion::logical_expr::{Expr, ExprSchemable};
use tiledb_arrow::schema::WhichSchema;
use tiledb_cxx_interface::sm::array_schema::ArraySchema;

#[derive(Debug, thiserror::Error)]
pub enum TypeError {
    #[error("Schema error: {0}")]
    ArraySchema(#[from] tiledb_arrow::schema::Error),
    #[error("Expression error: {0}")]
    Expr(#[from] DataFusionError),
}

/// Wraps a DataFusion [Expr] for passing across the FFI boundary.
pub struct LogicalExpr(pub Expr);

impl LogicalExpr {
    pub fn columns(&self) -> Vec<String> {
        self.0
            .column_refs()
            .into_iter()
            .map(|c| c.name.clone())
            .collect()
    }

    pub fn output_type(&self, schema: &ArraySchema) -> Result<ArrowDataType, TypeError> {
        let cols = self.0.column_refs();
        let arrow_schema = tiledb_arrow::schema::project_arrow(schema, WhichSchema::View, |f| {
            let Ok(field_name) = f.name() else {
                // NB: if the field name is not UTF-8 then it cannot possibly match the column name
                return false;
            };
            cols.contains(&Column::new_unqualified(field_name))
        })?;
        let dfschema = {
            // SAFETY: the only error we can get from the above is if the arrow schema
            // has duplicate names, which will not happen since it was constructed from
            // an ArraySchema which does not allow duplicate names
            DFSchema::try_from(arrow_schema.0).unwrap()
        };

        Ok(self.0.get_type(&dfschema)?)
    }

    pub fn has_aggregate_functions(&self) -> bool {
        let rec = self.0.visit(&mut AggregateFunctionChecker::default());
        let rec = {
            // SAFETY: AggregateFunctionChecker does not return any errors
            rec.unwrap()
        };
        matches!(rec, TreeNodeRecursion::Stop)
    }

    pub fn is_predicate(&self, schema: &ArraySchema) -> Result<bool, TypeError> {
        Ok(matches!(self.output_type(schema)?, ArrowDataType::Boolean))
    }
}

impl Display for LogicalExpr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        self.0.human_display().fmt(f)
    }
}

/// Returns a conjunction of the logical exprs `e1 AND e2 AND ... AND eN`.
pub fn make_conjunction(exprs: &[Box<LogicalExpr>]) -> Box<LogicalExpr> {
    Box::new(LogicalExpr(
        datafusion::logical_expr::utils::conjunction(exprs.iter().map(|e| e.0.clone()))
            .unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(true)), None)),
    ))
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
