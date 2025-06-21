//! Provides definitions for compiling DataFusion logical expressions
//! into DataFusion physical expressions which can be evaluated;
//! and definitions for evaluating those physical expressions.

use std::ops::Deref;
use std::sync::Arc;

use datafusion::common::arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::arrow::{array as aa, compute, datatypes as adt};
use datafusion::common::{DFSchema, DataFusionError, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr as DatafusionPhysicalExpr;
use tiledb_arrow::record_batch::ArrowRecordBatch;
use tiledb_arrow::schema::ArrowSchema;
use tiledb_cxx_interface::sm::enums::Datatype;

use crate::LogicalExpr;

/// An error using a [PhysicalExpr].
#[derive(Debug, thiserror::Error)]
pub enum PhysicalExprError {
    #[error("Compiling expression: {0}")]
    Create(#[source] DataFusionError),
    #[error("Evaluate expression: {0}")]
    Evaluate(#[source] DataFusionError),
}

/// An error using the output of physical expression evaluation.
#[derive(Debug, thiserror::Error)]
pub enum PhysicalExprOutputError {
    #[error("Target type is unavailable: {0}")]
    TypeUnavailable(#[source] tiledb_arrow::schema::FieldError),
    #[error("Cast expression result: {0}")]
    Cast(#[source] DataFusionError),
    #[error("Cannot read array as static datatype '{0}': found '{1}'")]
    InvalidStaticType(&'static str, ArrowDataType),
}

/// Wraps a DataFusion [PhysicalExpr] for passing across the FFI boundary.
pub struct PhysicalExpr(Arc<dyn DatafusionPhysicalExpr>);

impl PhysicalExpr {
    pub fn evaluate(
        &self,
        records: &ArrowRecordBatch,
    ) -> Result<Box<PhysicalExprOutput>, PhysicalExprError> {
        Ok(Box::new(PhysicalExprOutput(
            self.0
                .evaluate(&records.arrow)
                .map_err(PhysicalExprError::Evaluate)?,
        )))
    }
}

/// Returns a [PhysicalExpr] which evaluates a [LogicalExpr] for the given `schema`.
pub fn create_physical_expr(
    schema: &ArrowSchema,
    expr: Box<LogicalExpr>,
) -> Result<Box<PhysicalExpr>, PhysicalExprError> {
    let dfschema = DFSchema::from_field_specific_qualified_schema(
        vec![None; schema.fields.len()],
        schema.deref(),
    )
    .map_err(PhysicalExprError::Create)?;
    let dfexpr =
        datafusion::physical_expr::create_physical_expr(&expr.0, &dfschema, &ExecutionProps::new())
            .map_err(PhysicalExprError::Create)?;
    Ok(Box::new(PhysicalExpr(dfexpr)))
}

/// Wraps the output of physical expression evaluation for passing across the FFI boundary.
pub struct PhysicalExprOutput(ColumnarValue);

impl PhysicalExprOutput {
    pub fn is_scalar(&self) -> bool {
        matches!(self.0, ColumnarValue::Scalar(_))
    }

    pub fn is_array(&self) -> bool {
        matches!(self.0, ColumnarValue::Array(_))
    }

    /// Cast `self` to a target datatype.
    pub fn cast_to(
        &self,
        datatype: Datatype,
    ) -> Result<Box<PhysicalExprOutput>, PhysicalExprOutputError> {
        let arrow_type = tiledb_arrow::schema::arrow_datatype(datatype)
            .map_err(PhysicalExprOutputError::TypeUnavailable)?;
        let columnar_value = match &self.0 {
            ColumnarValue::Scalar(s) => ColumnarValue::Scalar(
                s.cast_to(&arrow_type)
                    .map_err(PhysicalExprOutputError::Cast)?,
            ),
            ColumnarValue::Array(a) => {
                ColumnarValue::Array(compute::kernels::cast::cast(a, &arrow_type).map_err(|e| {
                    PhysicalExprOutputError::Cast(DataFusionError::ArrowError(e, None))
                })?)
            }
        };
        Ok(Box::new(PhysicalExprOutput(columnar_value)))
    }

    /// Returns the result as a `&[u8]` if it is of that type,
    /// and returns `Err` otherwise.
    pub fn values_u8(&self) -> Result<&[u8], PhysicalExprOutputError> {
        match &self.0 {
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::UInt8(maybe_byte) => Ok(maybe_byte.as_slice()),
                _ => Err(PhysicalExprOutputError::InvalidStaticType(
                    "u8",
                    s.data_type().clone(),
                )),
            },
            ColumnarValue::Array(a) => {
                if *a.data_type() == adt::DataType::UInt8 {
                    // SAFETY: type check right above this
                    let primitive_array = a.as_any().downcast_ref::<aa::UInt8Array>().unwrap();
                    Ok(primitive_array.values().as_ref())
                } else {
                    Err(PhysicalExprOutputError::InvalidStaticType(
                        "u8",
                        a.data_type().clone(),
                    ))
                }
            }
        }
    }

    /// Returns the result as a `&[u64]` if it is of that type,
    /// and returns `Err` otherwise.
    pub fn values_u64(&self) -> Result<&[u64], PhysicalExprOutputError> {
        match &self.0 {
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::UInt64(maybe_value) => Ok(maybe_value.as_slice()),
                _ => Err(PhysicalExprOutputError::InvalidStaticType(
                    "u64",
                    s.data_type().clone(),
                )),
            },
            ColumnarValue::Array(a) => {
                if *a.data_type() == adt::DataType::UInt64 {
                    // SAFETY: type check right above this
                    let primitive_array = a.as_any().downcast_ref::<aa::UInt64Array>().unwrap();
                    Ok(primitive_array.values().as_ref())
                } else {
                    Err(PhysicalExprOutputError::InvalidStaticType(
                        "u64",
                        a.data_type().clone(),
                    ))
                }
            }
        }
    }
}
