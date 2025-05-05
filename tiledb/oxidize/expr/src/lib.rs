#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        include!("tiledb/sm/array_schema/array_schema.h");
        include!("tiledb/sm/query/ast/query_ast.h");
        include!("tiledb/sm/query/readers/result_tile.h");

        type ArraySchema = oxidize::sm::array_schema::ArraySchema;
        type ASTNode = oxidize::sm::query::ast::ASTNode;
        type ResultTile = oxidize::sm::query::readers::ResultTile;
    }

    #[namespace = "tiledb::rust"]
    extern "Rust" {
        type Expr;
        fn to_string(&self) -> String;

        fn to_datafusion(schema: &ArraySchema, query_condition: &ASTNode) -> Result<Box<Expr>>;
    }

    #[namespace = "tiledb::rust"]
    extern "Rust" {
        type ArrowRecordBatch;

        fn to_arrow_record_batch(
            schema: &ArraySchema,
            tile: &ResultTile,
        ) -> Result<Box<ArrowRecordBatch>>;
    }
}

mod record_batch;

pub use record_batch::{ArrowRecordBatch, to_arrow_record_batch};

use anyhow::anyhow;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr as DatafusionExpr, Operator};
use oxidize::sm::array_schema::{ArraySchema, Field};
use oxidize::sm::enums::{Datatype, QueryConditionCombinationOp, QueryConditionOp};
use oxidize::sm::query::ast::ASTNode;

pub struct Expr {
    datafusion_expr: DatafusionExpr,
}

impl Expr {
    pub fn to_string(&self) -> String {
        self.datafusion_expr.human_display().to_string()
    }
}

fn leaf_ast_to_binary_expr(
    schema: &ArraySchema,
    query_condition: &ASTNode,
    operator: Operator,
) -> anyhow::Result<DatafusionExpr> {
    let Some(field) = schema.field_cxx(query_condition.get_field_name()) else {
        todo!()
    };
    let column = DatafusionExpr::Column(Column::from_name(field.name()?));
    let value = {
        let value_type = field.datatype();
        let bytes = query_condition.get_data();
        if bytes.size() == 0 {
            return Ok(DatafusionExpr::Literal(match value_type {
                Datatype::INT8 => ScalarValue::Int8(None),
                Datatype::INT16 => ScalarValue::Int16(None),
                Datatype::INT32 => ScalarValue::Int32(None),
                Datatype::INT64 => ScalarValue::Int64(None),
                Datatype::UINT8 => ScalarValue::UInt8(None),
                Datatype::UINT16 => ScalarValue::UInt16(None),
                Datatype::UINT32 => ScalarValue::UInt32(None),
                Datatype::UINT64 => ScalarValue::UInt64(None),
                Datatype::FLOAT32 => ScalarValue::Float32(None),
                Datatype::FLOAT64 => ScalarValue::Float64(None),
                _ => todo!(),
            }));
        } else if bytes.size() != value_type.value_size() {
            todo!()
        }

        macro_rules! as_byte_array {
            ($ptr:expr, $arraylen:literal) => {{
                let raw = $ptr.data();
                let slice = unsafe { std::slice::from_raw_parts(raw, $arraylen) };
                // SAFETY: this macro is invoked after a type and size check
                type ArrayType = [u8; $arraylen];
                ArrayType::try_from(slice).unwrap()
            }};
        }

        let scalar: ScalarValue = match value_type {
            Datatype::INT8 => i8::from_le_bytes(as_byte_array!(bytes, 1)).into(),
            Datatype::INT16 => i16::from_le_bytes(as_byte_array!(bytes, 2)).into(),
            Datatype::INT32 => i32::from_le_bytes(as_byte_array!(bytes, 4)).into(),
            Datatype::INT64 => i64::from_le_bytes(as_byte_array!(bytes, 8)).into(),
            Datatype::UINT8 => u8::from_le_bytes(as_byte_array!(bytes, 1)).into(),
            Datatype::UINT16 => u16::from_le_bytes(as_byte_array!(bytes, 2)).into(),
            Datatype::UINT32 => u32::from_le_bytes(as_byte_array!(bytes, 4)).into(),
            Datatype::UINT64 => u64::from_le_bytes(as_byte_array!(bytes, 8)).into(),
            Datatype::FLOAT32 => f32::from_le_bytes(as_byte_array!(bytes, 4)).into(),
            Datatype::FLOAT64 => f64::from_le_bytes(as_byte_array!(bytes, 8)).into(),
            _ => todo!(),
        };
        DatafusionExpr::Literal(scalar)
    };
    Ok(DatafusionExpr::BinaryExpr(BinaryExpr {
        left: Box::new(column),
        op: operator,
        right: Box::new(value),
    }))
}

fn combination_ast_to_binary_expr(
    schema: &ArraySchema,
    query_condition: &ASTNode,
    operator: Operator,
) -> anyhow::Result<DatafusionExpr> {
    match query_condition.num_children() {
        0 => {
            return Err(anyhow!(
                "Expected two arguments for binary expression, found none"
            ));
        }
        1 => {
            return Err(anyhow!(
                "Expected two arguments for binary expression, found one"
            ));
        }
        2 => (),
        n => {
            return Err(anyhow!(
                "Expected two arguments for binary expression, found {n}"
            ));
        }
    };
    let left = {
        let ptr_left = query_condition.get_child(0);
        if ptr_left.is_null() {
            return Err(anyhow!(
                "Unexpected NULL pointer for binary expression left argument"
            ));
        }
        let ast_left = unsafe { &*ptr_left };
        to_datafusion_impl(schema, ast_left)?
    };
    let right = {
        let ptr_right = query_condition.get_child(1);
        if ptr_right.is_null() {
            return Err(anyhow!(
                "Unexpected NULL pointer for binary expression right argument"
            ));
        }
        let ast_right = unsafe { &*ptr_right };
        to_datafusion_impl(schema, ast_right)?
    };
    Ok(DatafusionExpr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op: operator,
        right: Box::new(right),
    }))
}

fn to_datafusion_impl(
    schema: &ArraySchema,
    query_condition: &ASTNode,
) -> anyhow::Result<DatafusionExpr> {
    if query_condition.is_expr() {
        match *query_condition.get_combination_op() {
            QueryConditionCombinationOp::AND => {
                combination_ast_to_binary_expr(schema, query_condition, Operator::And)
            }
            QueryConditionCombinationOp::OR => {
                combination_ast_to_binary_expr(schema, query_condition, Operator::Or)
            }
            QueryConditionCombinationOp::NOT => todo!(),
            _ => todo!(),
        }
    } else {
        match *query_condition.get_op() {
            QueryConditionOp::LT => leaf_ast_to_binary_expr(schema, query_condition, Operator::Lt),
            QueryConditionOp::LE => {
                leaf_ast_to_binary_expr(schema, query_condition, Operator::LtEq)
            }
            QueryConditionOp::GT => leaf_ast_to_binary_expr(schema, query_condition, Operator::Gt),
            QueryConditionOp::GE => {
                leaf_ast_to_binary_expr(schema, query_condition, Operator::GtEq)
            }
            QueryConditionOp::EQ => leaf_ast_to_binary_expr(schema, query_condition, Operator::Eq),
            QueryConditionOp::NE => {
                leaf_ast_to_binary_expr(schema, query_condition, Operator::NotEq)
            }
            QueryConditionOp::ALWAYS_TRUE => {
                Ok(DatafusionExpr::Literal(ScalarValue::Boolean(Some(true))))
            }
            QueryConditionOp::ALWAYS_FALSE => {
                Ok(DatafusionExpr::Literal(ScalarValue::Boolean(Some(false))))
            }
            _ => todo!(),
        }
    }
}

pub fn to_datafusion(schema: &ArraySchema, query_condition: &ASTNode) -> anyhow::Result<Box<Expr>> {
    Ok(Box::new(Expr {
        datafusion_expr: to_datafusion_impl(schema, query_condition)?,
    }))
}
