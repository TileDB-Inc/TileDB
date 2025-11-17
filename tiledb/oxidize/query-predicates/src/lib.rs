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

        fn compile(&mut self) -> Result<()>;

        unsafe fn field_names<'a>(&'a self) -> Vec<&'a str>;

        #[cxx_name = "add_predicate"]
        fn add_text_predicate(&mut self, expr: &str) -> Result<()>;

        fn evaluate_into_bitmap_u8(&self, tile: &ResultTile, bitmap: &mut [u8]) -> Result<()>;
        fn evaluate_into_bitmap_u64(&self, tile: &ResultTile, bitmap: &mut [u64]) -> Result<()>;
    }
}

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{Expr, ExprSchemable};
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use itertools::Itertools;
use num_traits::Zero;
use tiledb_arrow::schema::WhichSchema;
use tiledb_cxx_interface::sm::array_schema::ArraySchema;
use tiledb_cxx_interface::sm::query::readers::ResultTile;

#[derive(Debug, thiserror::Error)]
pub enum ParseExprError {
    #[error("Schema error: {0}")]
    Schema(#[from] tiledb_arrow::schema::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum AddPredicateError {
    #[error("Query is in progress")]
    InvalidState,
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
}

#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("Query is in progress")]
    InvalidState,
    #[error("Expression compile error: {0}")]
    PhysicalExpr(#[source] datafusion::common::DataFusionError),
}

#[derive(Debug, thiserror::Error)]
pub enum EvaluatePredicateError {
    #[error("Query has not been started")]
    InvalidState,
    #[error("Data error: {0}")]
    ResultTile(#[from] tiledb_arrow::record_batch::Error),
    #[error("Evaluation error: {0}")]
    Evaluate(#[source] datafusion::common::DataFusionError),
}

/// Holds state to parse, analyze and evaluate predicates of a TileDB query.
pub enum QueryPredicates {
    /// Predicates are being added to the query.
    Build(Builder),
    /// The query is being evaluated.
    Evaluate(Evaluator),
}

impl QueryPredicates {
    pub fn new(schema: &ArraySchema) -> Result<Self, tiledb_arrow::schema::Error> {
        Ok(Self::Build(Builder::new(schema, WhichSchema::View)?))
    }

    /// Parses a text predicate into a logical expression and adds it to the list of predicates to
    /// evaluate.
    ///
    /// This is only valid from the `Build` state.
    pub fn add_text_predicate(&mut self, expr: &str) -> Result<(), AddPredicateError> {
        match self {
            Self::Build(builder) => builder.add_text_predicate(expr),
            Self::Evaluate(_) => Err(AddPredicateError::InvalidState),
        }
    }

    /// Transitions state from `Build` to `Evaluate`.
    pub fn compile(&mut self) -> Result<(), CompileError> {
        match self {
            Self::Build(builder) => {
                *self = Self::Evaluate(builder.compile()?);
                Ok(())
            }
            Self::Evaluate(_) => Err(CompileError::InvalidState),
        }
    }

    /// Returns a list of unique field names which are used in the predicates.
    pub fn field_names(&self) -> Vec<&str> {
        match self {
            Self::Build(builder) => builder.field_names(),
            Self::Evaluate(evaluator) => evaluator.field_names(),
        }
    }

    pub fn evaluate(&self, tile: &ResultTile) -> Result<ColumnarValue, EvaluatePredicateError> {
        match self {
            Self::Build(_) => Err(EvaluatePredicateError::InvalidState),
            Self::Evaluate(evaluator) => evaluator.evaluate(tile),
        }
    }

    pub fn evaluate_into_bitmap<T>(
        &self,
        tile: &ResultTile,
        bitmap: &mut [T],
    ) -> Result<(), EvaluatePredicateError>
    where
        T: Copy + Zero,
    {
        match self {
            Self::Build(_) => Err(EvaluatePredicateError::InvalidState),
            Self::Evaluate(evaluator) => evaluator.evaluate_into_bitmap(tile, bitmap),
        }
    }

    fn evaluate_into_bitmap_u8(
        &self,
        tile: &ResultTile,
        bitmap: &mut [u8],
    ) -> Result<(), EvaluatePredicateError> {
        self.evaluate_into_bitmap::<u8>(tile, bitmap)
    }

    fn evaluate_into_bitmap_u64(
        &self,
        tile: &ResultTile,
        bitmap: &mut [u64],
    ) -> Result<(), EvaluatePredicateError> {
        self.evaluate_into_bitmap::<u64>(tile, bitmap)
    }
}

/// Structure which accumulates predicates.
pub struct Builder {
    /// DataFusion evaluation context.
    dfsession: SessionContext,
    /// Array schema mapped onto DataFusion data types.
    dfschema: DFSchema,
    /// Logical syntax tree representations of the predicates.
    logical_exprs: Vec<Expr>,
}

impl Builder {
    pub fn new(
        schema: &ArraySchema,
        which: WhichSchema,
    ) -> Result<Self, tiledb_arrow::schema::Error> {
        let (arrow_schema, _) = tiledb_arrow::schema::to_arrow(schema, which)?;
        let dfschema = {
            // SAFETY: this only errors if the names are not unique,
            // which they will be because `ArraySchema` requires it
            DFSchema::try_from(arrow_schema).unwrap()
        };

        Ok(Builder {
            dfsession: SessionContext::from(
                SessionStateBuilder::new_with_default_features().build(),
            ),
            dfschema,
            logical_exprs: vec![],
        })
    }

    /// Returns a list of all of the field names used in all of the predicates.
    pub fn field_names(&self) -> Vec<&str> {
        self.logical_exprs
            .iter()
            .flat_map(tiledb_expr::logical_expr::columns)
            .unique()
            .collect()
    }

    /// Parses a predicate into a logical expression and adds it to the list of predicates to
    /// evaluate.
    pub fn add_text_predicate(&mut self, expr: &str) -> Result<(), AddPredicateError> {
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

        self.add_predicate(logical_expr)
    }

    /// Adds a predicate to the list of predicates to evaluate.
    pub fn add_predicate(&mut self, logical_expr: Expr) -> Result<(), AddPredicateError> {
        let output_type = logical_expr
            .get_type(&self.dfschema)
            .map_err(AddPredicateError::OutputType)?;
        if output_type != DataType::Boolean && output_type != DataType::Null {
            // NB: see non-pub DataFusion API `Filter::is_allowed_filter_type`
            return Err(AddPredicateError::NotAPredicate(output_type));
        } else if tiledb_expr::logical_expr::has_aggregate_functions(&logical_expr) {
            return Err(AddPredicateError::ContainsAggregateFunctions);
        }
        self.logical_exprs.push(logical_expr);

        Ok(())
    }

    /// Returns an `Evaluator` which can evaluate the conjunction of all of the predicates.
    pub fn compile(&self) -> Result<Evaluator, CompileError> {
        let evaluation_schema = {
            let projection_fields = self
                .field_names()
                .iter()
                .map(|fname| self.dfschema.as_arrow().field_with_name(fname))
                .process_results(|fs| fs.cloned().collect::<Vec<_>>());

            let projection_fields = {
                // SAFETY: all field names have already been validated as part of the schema
                projection_fields.unwrap()
            };

            // SAFETY: this only errors if the names are not unique,
            // which they will be because `self.field_names()` produces unique field names
            DFSchema::try_from(ArrowSchema::new(projection_fields)).unwrap()
        };
        let predicate = {
            let execution_props = ExecutionProps::new();
            self.logical_exprs
                .iter()
                .map(|e| {
                    datafusion::physical_expr::create_physical_expr(
                        e,
                        &evaluation_schema,
                        &execution_props,
                    )
                    .map_err(CompileError::PhysicalExpr)
                })
                .process_results(|es| datafusion::physical_expr::conjunction(es))?
        };
        Ok(Evaluator {
            dfschema: evaluation_schema,
            predicate,
        })
    }
}

pub struct Evaluator {
    /// Array schema mapped onto DataFusion data types; this is a projection of the full schema
    /// consisting only of the fields which are used to evaluate `self.predicate`.
    /// The tiles corresponding to fields in this schema will be converted to [RecordBatch]
    /// columns, so to avoid extra conversions (which may allocate memory) we do not
    /// want to keep all of the fields here.
    dfschema: DFSchema,
    /// Expression evaluator which evaluates all predicates as a conjunction.
    predicate: Arc<dyn PhysicalExpr>,
}

impl Evaluator {
    /// Returns a list of all of the field names used in all of the predicates.
    pub fn field_names(&self) -> Vec<&str> {
        self.dfschema
            .fields()
            .iter()
            .map(|f| f.name().as_ref())
            .collect::<Vec<_>>()
    }

    pub fn evaluate(&self, tile: &ResultTile) -> Result<ColumnarValue, EvaluatePredicateError> {
        let rb = unsafe {
            // SAFETY: "This function is safe to call as long as the returned
            // RecordBatch is not used after the ResultTile is destructed."
            // The RecordBatch only lives in this stack frame, so we will follow this contract.
            tiledb_arrow::record_batch::to_record_batch(self.dfschema.inner(), tile)?
        };
        self.predicate
            .evaluate(&rb)
            .map_err(EvaluatePredicateError::Evaluate)
    }

    pub fn evaluate_into_bitmap<T>(
        &self,
        tile: &ResultTile,
        bitmap: &mut [T],
    ) -> Result<(), EvaluatePredicateError>
    where
        T: Copy + Zero,
    {
        // TODO: consider not evaluating on cells where the bitmap is already set.
        // This might happen if there is a historical query condition or if there
        // is timestamp duplication.

        let result = self.evaluate(tile)?;
        match result {
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::Boolean(Some(true)) => {
                    // all cells pass predicates, no need to update bitmap
                    Ok(())
                }
                ScalarValue::Boolean(Some(false)) => {
                    // no cells pass predicates, clear bitmap
                    bitmap.fill(T::zero());
                    Ok(())
                }
                ScalarValue::Null | ScalarValue::Boolean(None) => {
                    // no cells pass predicates, clear bitmap
                    bitmap.fill(T::zero());
                    Ok(())
                }
                _ => {
                    // should not be reachable due to return type check in `Builder::add_predicate`
                    unreachable!()
                }
            },
            ColumnarValue::Array(a) => {
                if *a.data_type() == DataType::Boolean {
                    let bools = arrow::array::as_boolean_array(&a);
                    for (i, b) in bools.iter().enumerate() {
                        if !matches!(b, Some(true)) {
                            bitmap[i] = T::zero();
                        }
                    }
                    Ok(())
                } else if *a.data_type() == DataType::Null {
                    // no cells pass predicates, clear bitmap
                    bitmap.fill(T::zero());
                    Ok(())
                } else {
                    // should not be reachable due to return type check in `Builder::add_predicate`
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
