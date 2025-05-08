use std::sync::Arc;

use datafusion::common::arrow::array::{ArrayData, GenericListArray};
use datafusion::common::arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use itertools::Itertools;
use oxidize::sm::array_schema::{ArraySchema, CellValNum, Field};
use oxidize::sm::enums::{Datatype, QueryConditionCombinationOp, QueryConditionOp};
use oxidize::sm::query::ast::ASTNode;

pub struct LogicalExpr(pub Expr);

impl LogicalExpr {
    pub fn to_string(&self) -> String {
        self.0.human_display().to_string()
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
) -> anyhow::Result<Expr> {
    let Some(field) = schema.field_cxx(ast.get_field_name()) else {
        todo!()
    };

    fn apply<F>(
        field: Field,
        ast: &ASTNode,
        operator: Operator,
        value_to_scalar: F,
    ) -> anyhow::Result<Expr>
    where
        F: Fn(&[u8]) -> ScalarValue,
    {
        let column = Expr::Column(Column::from_name(field.name()?));
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

                // NB: we cannot just use `ScalarValue::new_large_list` because
                // datafusion does not handle non-nullable fields of lists well.
                // We have to manually rewrite the array to have a non-nullable
                // field.

                let list = {
                    let l = Arc::try_unwrap(ScalarValue::new_large_list(
                        &values,
                        &values[0].data_type(),
                    ));

                    // SAFETY: the only reference is created within this scope
                    l.unwrap()
                };

                let adata = ArrayData::from(list);
                assert_eq!(1, adata.child_data().len());
                assert_eq!(0, adata.child_data()[0].null_count());

                let non_nullable_child = {
                    // SAFETY: TODO
                    let maybe = ArrayData::builder(adata.child_data()[0].data_type().clone())
                        .len(adata.child_data()[0].len())
                        .add_buffers(adata.child_data()[0].buffers().to_vec())
                        .build();
                    maybe.unwrap()
                };

                let non_null_field_list_type =
                    ArrowDataType::new_large_list(non_nullable_child.data_type().clone(), false);

                let non_null_field_adata = {
                    let maybe = ArrayData::try_new(
                        non_null_field_list_type,
                        adata.len(),
                        adata.nulls().map(|n| n.buffer().clone()),
                        adata.offset(),
                        adata.buffers().to_vec(),
                        vec![non_nullable_child],
                    );

                    // SAFETY: TODO
                    maybe.unwrap()
                };

                ScalarValue::LargeList(Arc::new(GenericListArray::<i64>::from(
                    non_null_field_adata,
                )))
            }
        };
        Ok(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(column),
            op: operator,
            right: Box::new(Expr::Literal(right)),
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

fn leaf_ast_to_in_list(schema: &ArraySchema, ast: &ASTNode, negated: bool) -> anyhow::Result<Expr> {
    let Some(field) = schema.field_cxx(ast.get_field_name()) else {
        todo!()
    };

    fn apply<F>(
        field: Field,
        ast: &ASTNode,
        negated: bool,
        value_to_scalar: F,
    ) -> anyhow::Result<Expr>
    where
        F: Fn(&[u8]) -> ScalarValue,
    {
        let column = Expr::Column(Column::from_name(field.name()?));
        let value_type = field.datatype();
        let bytes = ast.get_data();
        if bytes.size() % value_type.value_size() != 0 {
            todo!()
        }

        let in_list = bytes
            .as_slice()
            .chunks(value_type.value_size())
            .map(value_to_scalar)
            .map(Expr::Literal)
            .collect::<Vec<_>>();
        Ok(Expr::InList(InList {
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
) -> anyhow::Result<Expr> {
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
                    Expr::BinaryExpr(BinaryExpr {
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

fn to_datafusion_impl(schema: &ArraySchema, query_condition: &ASTNode) -> anyhow::Result<Expr> {
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
            QueryConditionOp::ALWAYS_TRUE => Ok(Expr::Literal(ScalarValue::Boolean(Some(true)))),
            QueryConditionOp::ALWAYS_FALSE => Ok(Expr::Literal(ScalarValue::Boolean(Some(false)))),
            _ => todo!(),
        }
    }
}

pub fn to_datafusion(
    schema: &ArraySchema,
    query_condition: &ASTNode,
) -> anyhow::Result<Box<LogicalExpr>> {
    Ok(Box::new(LogicalExpr(to_datafusion_impl(
        schema,
        query_condition,
    )?)))
}
