//! Provides definitions for interacting with DataFusion logical expressions.

use std::fmt::{Display, Formatter, Result as FmtResult};

use datafusion::logical_expr::Expr;

/// Wraps a DataFusion [Expr] for passing across the FFI boundary.
pub struct LogicalExpr(pub Expr);

impl Display for LogicalExpr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        self.0.human_display().fmt(f)
    }
}
