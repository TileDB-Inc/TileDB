use std::sync::Arc;

use datafusion::common::arrow::{array as aa, compute, datatypes as adt};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr as DatafusionPhysicalExpr;
use oxidize::sm::enums::Datatype;

use crate::schema;
use crate::{ArrowRecordBatch, DataFusionSchema, LogicalExpr};

#[derive(Debug, thiserror::Error)]
pub enum PhysicalExprError {
    #[error("Compiling expression: {0}")]
    Create(#[source] DataFusionError),
    #[error("Evaluate expression: {0}")]
    Evaluate(#[source] DataFusionError),
}

#[derive(Debug, thiserror::Error)]
pub enum PhysicalExprOutputError {
    #[error("Target type is unavailable: {0}")]
    TypeUnavailable(#[source] schema::FieldError),
    #[error("Cast expression result: {0}")]
    Cast(#[source] DataFusionError),
}

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

pub fn create_physical_expr(
    schema: &DataFusionSchema,
    expr: Box<LogicalExpr>,
) -> Result<Box<PhysicalExpr>, PhysicalExprError> {
    let dfexpr =
        datafusion::physical_expr::create_physical_expr(&expr.0, &schema.0, &ExecutionProps::new())
            .map_err(PhysicalExprError::Create)?;
    Ok(Box::new(PhysicalExpr(dfexpr)))
}

pub struct PhysicalExprOutput(ColumnarValue);

impl PhysicalExprOutput {
    pub fn is_scalar(&self) -> bool {
        matches!(self.0, ColumnarValue::Scalar(_))
    }

    pub fn is_array(&self) -> bool {
        matches!(self.0, ColumnarValue::Array(_))
    }

    pub fn raw_values(&self) -> *const u8 {
        match &self.0 {
            ColumnarValue::Scalar(s) => todo!(),
            ColumnarValue::Array(a) => todo!(),
        }
    }

    pub fn raw_size(&self) -> usize {
        match &self.0 {
            ColumnarValue::Scalar(s) => todo!(),
            ColumnarValue::Array(a) => todo!(),
        }
    }

    /// Cast `self` to a target datatype.
    pub fn cast_to(
        &self,
        datatype: Datatype,
    ) -> Result<Box<PhysicalExprOutput>, PhysicalExprOutputError> {
        let arrow_type =
            schema::arrow_datatype(datatype).map_err(PhysicalExprOutputError::TypeUnavailable)?;
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

    pub fn values_u8(&self) -> Result<&[u8], PhysicalExprOutputError> {
        match &self.0 {
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::UInt8(maybe_byte) => Ok(maybe_byte.as_slice()),
                _ => todo!(),
            },
            ColumnarValue::Array(a) => {
                if *a.data_type() == adt::DataType::UInt8 {
                    // SAFETY: type check right above this
                    let primitive_array = a.as_any().downcast_ref::<aa::UInt8Array>().unwrap();
                    Ok(primitive_array.values().as_ref())
                } else {
                    todo!()
                }
            }
        }
    }

    pub fn values_u64(&self) -> Result<&[u64], PhysicalExprOutputError> {
        match &self.0 {
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::UInt64(maybe_value) => Ok(maybe_value.as_slice()),
                _ => todo!(),
            },
            ColumnarValue::Array(a) => {
                if *a.data_type() == adt::DataType::UInt64 {
                    // SAFETY: type check right above this
                    let primitive_array = a.as_any().downcast_ref::<aa::UInt64Array>().unwrap();
                    Ok(primitive_array.values().as_ref())
                } else {
                    todo!()
                }
            }
        }
    }
}
