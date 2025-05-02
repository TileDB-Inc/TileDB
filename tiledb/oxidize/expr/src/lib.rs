#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        type Datatype = array_schema::Datatype;
        type Attribute = array_schema::Attribute;
        type Dimension = array_schema::Dimension;
        type Domain = array_schema::Domain;
        type ArraySchema = array_schema::ArraySchema;
    }

    #[namespace = "tiledb::sm"]
    enum QueryConditionOp {
        LT,
        LE,
        GT,
        GE,
        EQ,
        NE,
        IN,
        NOT_IN,
        ALWAYS_TRUE = 253,
        ALWAYS_FALSE = 254,
    }

    #[namespace = "tiledb::sm"]
    enum QueryConditionCombinationOp {
        AND,
        OR,
        NOT,
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/misc/types.h");

        type ByteVecValue;
        fn size(&self) -> usize;
        fn data(&self) -> *const u8;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/enums/query_condition_op.h");
        type QueryConditionOp;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/enums/query_condition_combination_op.h");
        type QueryConditionCombinationOp;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/query/ast/query_ast.h");

        type ASTNode;
        fn is_expr(&self) -> bool;
        fn get_field_name(&self) -> &CxxString;
        fn get_op(&self) -> &QueryConditionOp;
        fn get_combination_op(&self) -> &QueryConditionCombinationOp;
        fn get_data(&self) -> &ByteVecValue;
        fn num_children(&self) -> u64;
        fn get_child(&self, i: u64) -> *const ASTNode;
    }

    #[namespace = "tiledb::rust"]
    extern "Rust" {
        type Expr;

        fn to_datafusion(schema: &ArraySchema, query_condition: &ASTNode) -> Result<Box<Expr>>;
    }
}

use anyhow::anyhow;
use array_schema::{ArraySchema, Datatype};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{BinaryExpr, Expr as DatafusionExpr, Operator};

struct Expr {
    datafusion_expr: DatafusionExpr,
}

fn leaf_ast_to_binary_expr(
    schema: &ArraySchema,
    query_condition: &crate::ffi::ASTNode,
    operator: Operator,
) -> anyhow::Result<DatafusionExpr> {
    let Some(field) = schema.field_cxx(query_condition.get_field_name()) else {
        todo!()
    };
    let column = DatafusionExpr::Column(Column::from_name(field.name()?));
    let value = {
        let value_type = field.datatype();
        let bytes = query_condition.get_data();
        if bytes.size() != value_type.value_size() {
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
    query_condition: &crate::ffi::ASTNode,
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
    query_condition: &crate::ffi::ASTNode,
) -> anyhow::Result<DatafusionExpr> {
    if query_condition.is_expr() {
        match *query_condition.get_combination_op() {
            ffi::QueryConditionCombinationOp::AND => {
                combination_ast_to_binary_expr(schema, query_condition, Operator::And)
            }
            ffi::QueryConditionCombinationOp::OR => {
                combination_ast_to_binary_expr(schema, query_condition, Operator::Or)
            }
            ffi::QueryConditionCombinationOp::NOT => todo!(),
            _ => todo!(),
        }
    } else {
        match *query_condition.get_op() {
            ffi::QueryConditionOp::LT => {
                leaf_ast_to_binary_expr(schema, query_condition, Operator::Lt)
            }
            _ => todo!(),
        }
    }
}

fn to_datafusion(
    schema: &ArraySchema,
    query_condition: &crate::ffi::ASTNode,
) -> anyhow::Result<Box<Expr>> {
    Ok(Box::new(Expr {
        datafusion_expr: to_datafusion_impl(schema, query_condition)?,
    }))
}
