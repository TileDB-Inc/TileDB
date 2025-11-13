#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        include!("tiledb/sm/array_schema/array_schema.h");
        include!("tiledb/sm/query/readers/result_tile.h");

        type ArraySchema = tiledb_cxx_interface::sm::array_schema::ArraySchema;
        type ResultTile = tiledb_cxx_interface::sm::query::readers::ResultTile;
    }

    #[namespace = "tiledb::oxidize"]
    extern "Rust" {
        type QueryPredicates;

        #[cxx_name = "new_query_predicates"]
        fn new_query_predicates_ffi(schema: &ArraySchema) -> Result<Box<QueryPredicates>>;

        fn add_predicate(&mut self, expr: &str) -> Result<()>;

        fn evaluate_into_bitmap(&self, tile: &ResultTile, bitmap: &mut [u8]) -> Result<()>;
    }
}

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::ExprSchemable;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use tiledb_cxx_interface::sm::array_schema::ArraySchema;
use tiledb_cxx_interface::sm::query::readers::ResultTile;

#[derive(Debug, thiserror::Error)]
pub enum ParseExprError {
    #[error("Schema error: {0}")]
    Schema(#[from] tiledb_arrow::schema::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum AddPredicateError {
    #[error("Parse error: {0}")]
    Parse(#[source] datafusion::common::DataFusionError),
    #[error("Expression is not a predicate: found return type {0}")]
    NotAPredicate(DataType),
    #[error("Expression contains aggregate functions which are not supported in predicates")]
    ContainsAggregateFunctions,
    #[error("Type coercion error: {0}")]
    TypeCoercion(#[source] datafusion::common::DataFusionError),
    #[error("Output type error: {0}")]
    OutputType(#[source] datafusion::common::DataFusionError),
    #[error("Expression compile error: {0}")]
    Compile(#[source] datafusion::common::DataFusionError),
}

#[derive(Debug, thiserror::Error)]
pub enum EvaluatePredicateError {
    #[error("Result tile error: {0}")]
    ResultTile(#[from] tiledb_arrow::record_batch::Error),
    #[error("Evaluation error: {0}")]
    Evaluate(#[source] datafusion::common::DataFusionError),
}

pub struct QueryPredicates {
    dfsession: SessionContext,
    dfschema: DFSchema,
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl QueryPredicates {
    pub fn new(schema: &ArraySchema) -> Result<Self, tiledb_arrow::schema::Error> {
        let (arrow_schema, _) =
            tiledb_arrow::schema::to_arrow(schema, tiledb_arrow::schema::WhichSchema::View)?;
        let dfschema = {
            // SAFETY: this only errors if the names are not unique,
            // which they will be because `ArraySchema` requires it
            DFSchema::try_from(arrow_schema).unwrap()
        };

        Ok(QueryPredicates {
            dfsession: SessionContext::from(
                SessionStateBuilder::new_with_default_features().build(),
            ),
            dfschema,
            predicate: None,
        })
    }

    pub fn add_predicate(&mut self, expr: &str) -> Result<(), AddPredicateError> {
        let parsed_expr = self
            .dfsession
            .parse_sql_expr(expr, &self.dfschema)
            .map_err(AddPredicateError::Parse)?;

        let mut coercion_rewriter =
            datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter::new(
                &self.dfschema,
            );
        let logical_expr = parsed_expr
            .rewrite(&mut coercion_rewriter)
            .map(|t| t.data)
            .map_err(AddPredicateError::TypeCoercion)?;

        let output_type = logical_expr
            .get_type(&self.dfschema)
            .map_err(AddPredicateError::OutputType)?;
        if output_type != DataType::Boolean {
            return Err(AddPredicateError::NotAPredicate(output_type));
        } else if tiledb_expr::logical_expr::has_aggregate_functions(&logical_expr) {
            return Err(AddPredicateError::ContainsAggregateFunctions);
        }
        let physical_expr = datafusion::physical_expr::create_physical_expr(
            &logical_expr,
            &self.dfschema,
            &ExecutionProps::new(),
        )
        .map_err(AddPredicateError::Compile)?;

        self.predicate = Some(datafusion::physical_expr::conjunction(
            self.predicate
                .take()
                .into_iter()
                .chain(std::iter::once(physical_expr)),
        ));
        Ok(())
    }

    pub fn evaluate(&self, tile: &ResultTile) -> Result<ColumnarValue, EvaluatePredicateError> {
        let rb = unsafe {
            // SAFETY: "This function is safe to call as long as the returned
            // RecordBatch is not used after the ResultTile is destructed."
            // The RecordBatch only lives in this stack frame, so we will follow this contract.
            tiledb_arrow::record_batch::to_record_batch(self.dfschema.inner(), tile)?
        };
        if let Some(p) = self.predicate.as_ref() {
            Ok(p.evaluate(&rb).map_err(EvaluatePredicateError::Evaluate)?)
        } else {
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
        }
    }

    fn evaluate_into_bitmap(
        &self,
        tile: &ResultTile,
        bitmap: &mut [u8],
    ) -> Result<(), EvaluatePredicateError> {
        // TODO: consider not evaluating on cells where the bitmap is already set

        let result = self.evaluate(tile)?;
        match result {
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::Boolean(Some(true)) => {
                    // all cells pass predicates, no need to update bitmap
                    Ok(())
                }
                ScalarValue::Boolean(Some(false)) => {
                    // no cells pass predicates, clear bitmap
                    bitmap.fill(0);
                    Ok(())
                }
                ScalarValue::Boolean(None) => {
                    // no cells pass predicates, clear bitmap
                    bitmap.fill(0);
                    Ok(())
                }
                _ => {
                    // should not be reachable due to return type check
                    unreachable!()
                }
            },
            ColumnarValue::Array(a) => {
                if *a.data_type() == DataType::Boolean {
                    let bools = arrow::array::as_boolean_array(&a);
                    for (i, b) in bools.iter().enumerate() {
                        if !matches!(b, Some(true)) {
                            bitmap[i] = 0;
                        }
                    }
                    Ok(())
                } else {
                    // should not be reachable due to return type check
                    unreachable!()
                }
            }
        }
    }
}

fn new_query_predicates_ffi(
    schema: &ArraySchema,
) -> Result<Box<QueryPredicates>, tiledb_arrow::schema::Error> {
    Ok(Box::new(QueryPredicates::new(schema)?))
}
