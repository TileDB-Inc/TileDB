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
mod schema;

pub use record_batch::{ArrowRecordBatch, to_arrow_record_batch};

use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr as DatafusionExpr, Operator};
use itertools::Itertools;
use oxidize::sm::array_schema::{ArraySchema, CellValNum, Field};
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

macro_rules! from_le_bytes {
    ($primitive:ty, $arraylen:literal) => {{
        |bytes| {
            if bytes.len() == 0 {
                ScalarValue::from(None::<$primitive>)
            } else {
                type ArrayType = [u8; $arraylen];
                // SAFETY: this macro is invoked after a type and size check
                let array = ArrayType::try_from(bytes).unwrap();
                let primitive = <$primitive>::from_le_bytes(array);
                ScalarValue::from(Some(primitive))
            }
        }
    }};
}

fn leaf_ast_to_binary_expr(
    schema: &ArraySchema,
    ast: &ASTNode,
    op: Operator,
) -> anyhow::Result<DatafusionExpr> {
    let Some(field) = schema.field_cxx(ast.get_field_name()) else {
        todo!()
    };

    fn apply<F>(
        field: Field,
        ast: &ASTNode,
        operator: Operator,
        value_to_scalar: F,
    ) -> anyhow::Result<DatafusionExpr>
    where
        F: Fn(&[u8]) -> ScalarValue,
    {
        let column = DatafusionExpr::Column(Column::from_name(field.name()?));
        let value_type = field.datatype();
        let bytes = ast.get_data();
        if bytes.size() % value_type.value_size() != 0 {
            todo!()
        }
        let right = match field.cell_val_num() {
            CellValNum::Single => value_to_scalar(ast.get_data().as_slice()),
            CellValNum::Fixed(_) => todo!(),
            CellValNum::Var => {
                let values = ast
                    .get_data()
                    .as_slice()
                    .chunks(value_type.value_size())
                    .map(value_to_scalar)
                    .collect::<Vec<ScalarValue>>();
                ScalarValue::LargeList(ScalarValue::new_large_list(&values, &values[0].data_type()))
            }
        };
        Ok(DatafusionExpr::BinaryExpr(BinaryExpr {
            left: Box::new(column),
            op: operator,
            right: Box::new(DatafusionExpr::Literal(right)),
        }))
    }

    let value_type = field.datatype();

    match value_type {
        Datatype::INT8 => apply(field, ast, op, from_le_bytes!(i8, 1)),
        Datatype::INT16 => apply(field, ast, op, from_le_bytes!(i16, 2)),
        Datatype::INT32 => apply(field, ast, op, from_le_bytes!(i32, 4)),
        Datatype::INT64
        | Datatype::DATETIME_YEAR
        | Datatype::DATETIME_MONTH
        | Datatype::DATETIME_WEEK
        | Datatype::DATETIME_DAY
        | Datatype::DATETIME_HR
        | Datatype::DATETIME_MIN
        | Datatype::DATETIME_SEC
        | Datatype::DATETIME_MS
        | Datatype::DATETIME_US
        | Datatype::DATETIME_NS
        | Datatype::DATETIME_PS
        | Datatype::DATETIME_FS
        | Datatype::DATETIME_AS
        | Datatype::TIME_HR
        | Datatype::TIME_MIN
        | Datatype::TIME_SEC
        | Datatype::TIME_MS
        | Datatype::TIME_US
        | Datatype::TIME_NS
        | Datatype::TIME_PS
        | Datatype::TIME_FS
        | Datatype::TIME_AS => apply(field, ast, op, from_le_bytes!(i64, 8)),
        Datatype::UINT8
        | Datatype::STRING_ASCII
        | Datatype::STRING_UTF8
        | Datatype::ANY
        | Datatype::BLOB
        | Datatype::BOOL
        | Datatype::GEOM_WKB
        | Datatype::GEOM_WKT => apply(field, ast, op, from_le_bytes!(u8, 1)),
        Datatype::UINT16 | Datatype::STRING_UTF16 | Datatype::STRING_UCS2 => {
            apply(field, ast, op, from_le_bytes!(u16, 2))
        }
        Datatype::UINT32 | Datatype::STRING_UTF32 | Datatype::STRING_UCS4 => {
            apply(field, ast, op, from_le_bytes!(u32, 4))
        }
        Datatype::UINT64 => apply(field, ast, op, from_le_bytes!(u64, 8)),
        Datatype::FLOAT32 => apply(field, ast, op, from_le_bytes!(f32, 4)),
        Datatype::FLOAT64 => apply(field, ast, op, from_le_bytes!(f64, 8)),
        Datatype::CHAR => apply(field, ast, op, from_le_bytes!(std::ffi::c_char, 1)),
        _ => todo!(),
    }
}

fn leaf_ast_to_in_list(
    schema: &ArraySchema,
    ast: &ASTNode,
    negated: bool,
) -> anyhow::Result<DatafusionExpr> {
    let Some(field) = schema.field_cxx(ast.get_field_name()) else {
        todo!()
    };

    fn apply<F>(
        field: Field,
        ast: &ASTNode,
        negated: bool,
        value_to_scalar: F,
    ) -> anyhow::Result<DatafusionExpr>
    where
        F: Fn(&[u8]) -> ScalarValue,
    {
        let column = DatafusionExpr::Column(Column::from_name(field.name()?));
        let value_type = field.datatype();
        let bytes = ast.get_data();
        if bytes.size() % value_type.value_size() != 0 {
            todo!()
        }

        let in_list = bytes
            .as_slice()
            .chunks(value_type.value_size())
            .map(value_to_scalar)
            .map(DatafusionExpr::Literal)
            .collect::<Vec<_>>();
        Ok(DatafusionExpr::InList(InList {
            expr: Box::new(column),
            list: in_list,
            negated,
        }))
    }

    let value_type = field.datatype();
    match value_type {
        Datatype::INT8 => apply(field, ast, negated, from_le_bytes!(i8, 1)),
        Datatype::INT16 => apply(field, ast, negated, from_le_bytes!(i16, 2)),
        Datatype::INT32 => apply(field, ast, negated, from_le_bytes!(i32, 4)),
        Datatype::INT64 => apply(field, ast, negated, from_le_bytes!(i64, 8)),
        Datatype::UINT8 => apply(field, ast, negated, from_le_bytes!(u8, 1)),
        Datatype::UINT16 => apply(field, ast, negated, from_le_bytes!(u16, 2)),
        Datatype::UINT32 => apply(field, ast, negated, from_le_bytes!(u32, 4)),
        Datatype::UINT64 => apply(field, ast, negated, from_le_bytes!(u64, 8)),
        Datatype::FLOAT32 => apply(field, ast, negated, from_le_bytes!(f32, 4)),
        Datatype::FLOAT64 => apply(field, ast, negated, from_le_bytes!(f64, 8)),
        _ => todo!(),
    }
}

fn combination_ast_to_binary_expr(
    schema: &ArraySchema,
    query_condition: &ASTNode,
    operator: Operator,
) -> anyhow::Result<DatafusionExpr> {
    let mut level = query_condition
        .children()
        .map(|ast| to_datafusion_impl(schema, ast))
        .collect::<Result<Vec<_>, _>>()?;

    while level.len() != 1 {
        level = level
            .into_iter()
            .chunks(2)
            .into_iter()
            .map(|mut pair| {
                let Some(left) = pair.next() else {
                    unreachable!()
                };
                if let Some(right) = pair.next() {
                    assert!(pair.next().is_none());
                    DatafusionExpr::BinaryExpr(BinaryExpr {
                        left: Box::new(left),
                        op: operator,
                        right: Box::new(right),
                    })
                } else {
                    left
                }
            })
            .collect::<Vec<_>>();
    }
    assert_eq!(1, level.len());

    Ok(level.into_iter().next().unwrap())
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
            QueryConditionOp::IN => leaf_ast_to_in_list(schema, query_condition, false),
            QueryConditionOp::NOT_IN => leaf_ast_to_in_list(schema, query_condition, true),
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
