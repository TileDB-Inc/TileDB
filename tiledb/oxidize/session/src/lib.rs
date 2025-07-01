#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        include!("tiledb/sm/array_schema/array_schema.h");

        type ArraySchema = tiledb_cxx_interface::sm::array_schema::ArraySchema;
    }

    #[namespace = "tiledb::oxidize::datafusion::logical_expr"]
    extern "Rust" {
        type ExternLogicalExpr;
    }

    #[namespace = "tiledb::oxidize::datafusion::session"]
    extern "Rust" {
        type Session;

        fn new_session() -> Box<Session>;

        #[cxx_name = "parse_expr"]
        fn parse_expr_ffi(
            &self,
            expr: &str,
            array_schema: &ArraySchema,
        ) -> Result<Box<ExternLogicalExpr>>;
    }
}

#[repr(transparent)]
struct ExternLogicalExpr(pub LogicalExpr);

unsafe impl cxx::ExternType for ExternLogicalExpr {
    type Id = cxx::type_id!("tiledb::oxidize::datafusion::logical_expr::LogicalExpr");
    type Kind = cxx::kind::Opaque;
}

fn new_session() -> Box<Session> {
    Box::new(Session::new())
}

use datafusion::common::DFSchema;
use datafusion::common::tree_node::TreeNode;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::Expr;
use tiledb_cxx_interface::sm::array_schema::ArraySchema;
use tiledb_expr::LogicalExpr;

#[derive(Debug, thiserror::Error)]
pub enum ParseExprError {
    #[error("Schema error: {0}")]
    Schema(#[from] tiledb_arrow::schema::Error),
    #[error("Parse error: {0}")]
    Parse(#[source] datafusion::common::DataFusionError),
    #[error("Type coercion error: {0}")]
    TypeCoercion(#[source] datafusion::common::DataFusionError),
}

/// Wraps a DataFusion [SessionContext] for passing across the FFI boundary.
pub struct Session(pub SessionContext);

impl Session {
    pub fn new() -> Self {
        Self(SessionContext::from(
            SessionStateBuilder::new_with_default_features().build(),
        ))
    }

    fn parse_expr_ffi(
        &self,
        expr: &str,
        array_schema: &ArraySchema,
    ) -> Result<Box<ExternLogicalExpr>, ParseExprError> {
        let e = self.parse_expr(expr, array_schema)?;
        Ok(Box::new(ExternLogicalExpr(LogicalExpr(e))))
    }

    fn parse_expr(&self, expr: &str, array_schema: &ArraySchema) -> Result<Expr, ParseExprError> {
        let arrow_schema = tiledb_arrow::schema::to_arrow(array_schema)?;
        let df_schema = {
            // SAFETY: this only errors if the names are not unique,
            // which they will be because `ArraySchema` requires it
            DFSchema::try_from(arrow_schema).unwrap()
        };

        let parsed = self
            .0
            .parse_sql_expr(expr, &df_schema)
            .map_err(ParseExprError::Parse)?;

        let mut coercion_rewriter =
            datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter::new(&df_schema);
        //.map_err(ParseExprError::TypeCoercion)?;

        parsed
            .rewrite(&mut coercion_rewriter)
            .map(|t| t.data)
            .map_err(ParseExprError::TypeCoercion)
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}
