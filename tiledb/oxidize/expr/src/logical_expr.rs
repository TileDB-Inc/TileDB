use std::str::Utf8Error;
use std::sync::Arc;

use datafusion::common::arrow::array::{
    self as aa, Array as ArrowArray, FixedSizeListArray, GenericListArray,
};
use datafusion::common::arrow::buffer::OffsetBuffer;
use datafusion::common::arrow::datatypes::Field as ArrowField;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use itertools::Itertools;
use num_traits::FromBytes;
use oxidize::sm::array_schema::{ArraySchema, CellValNum, Field};
use oxidize::sm::enums::{Datatype, QueryConditionCombinationOp, QueryConditionOp};
use oxidize::sm::misc::ByteVecValue;
use oxidize::sm::query::ast::ASTNode;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unknown dimension or attribute: {0}")]
    UnknownField(String),
    #[error("")]
    FieldNameNotUtf8(Utf8Error),
}

pub struct LogicalExpr(pub Expr);

impl LogicalExpr {
    pub fn to_string(&self) -> String {
        self.0.human_display().to_string()
    }
}

fn values_iter<T>(
    datatype: Datatype,
    cell_val_num: CellValNum,
    bytes: &ByteVecValue,
) -> Result<impl Iterator<Item = T>, Error>
where
    T: FromBytes,
    <T as FromBytes>::Bytes: for<'a> TryFrom<&'a [u8]>,
{
    if bytes.size() % datatype.value_size() != 0 {
        todo!()
    }

    type B<T> = <T as FromBytes>::Bytes;

    Ok(bytes.as_slice().chunks(datatype.value_size()).map(|slice| {
        let array = match B::<T>::try_from(slice) {
            Ok(array) => array,
            Err(_) => {
                // SAFETY: we assume that `B` is a fixed-length array type whose length is equal to
                // `value_type.value_size()`. We know that `slice.len()` is equal to that length.
                //
                // NB: we do this instead of `unwrap()` to avoid a `Debug` trait bound
                // which we expect not to use.
                unreachable!()
            }
        };

        T::from_ne_bytes(&array)
    }))
}

fn leaf_ast_to_binary_expr(
    schema: &ArraySchema,
    ast: &ASTNode,
    op: Operator,
) -> Result<Expr, Error> {
    let Some(field) = schema.field_cxx(ast.get_field_name()) else {
        return Err(Error::UnknownField(
            ast.get_field_name().to_string_lossy().into_owned(),
        ));
    };

    fn apply<T, A>(field: Field, ast: &ASTNode, operator: Operator) -> Result<Expr, Error>
    where
        T: FromBytes,
        <T as FromBytes>::Bytes: for<'a> TryFrom<&'a [u8]>,
        ScalarValue: From<Option<T>>,
        A: ArrowArray + From<Vec<T>> + 'static,
    {
        let column = Expr::Column(Column::from_name(
            field.name().map_err(Error::FieldNameNotUtf8)?,
        ));

        let mut values = values_iter::<T>(field.datatype(), field.cell_val_num(), ast.get_data())?;

        let right = match field.cell_val_num() {
            CellValNum::Single => ScalarValue::from(values.next()),
            CellValNum::Fixed(nz) => {
                let values = values.collect::<Vec<T>>();
                if values.len() != nz.get() as usize {
                    todo!()
                }
                let Ok(fixed_size) = i32::try_from(nz.get()) else {
                    todo!()
                };
                let arrow_values = A::from(values);
                let element_field =
                    ArrowField::new_list_field(arrow_values.data_type().clone(), false);
                ScalarValue::FixedSizeList(
                    FixedSizeListArray::new(
                        element_field.into(),
                        fixed_size,
                        Arc::new(arrow_values) as Arc<dyn ArrowArray>,
                        None,
                    )
                    .into(),
                )
            }
            CellValNum::Var => {
                let values = values.collect::<Vec<T>>();
                let arrow_values = A::from(values);
                let element_field =
                    ArrowField::new_list_field(arrow_values.data_type().clone(), false);
                ScalarValue::LargeList(
                    GenericListArray::<i64>::new(
                        element_field.into(),
                        OffsetBuffer::<i64>::from_lengths(std::iter::once(arrow_values.len())),
                        Arc::new(arrow_values) as Arc<dyn ArrowArray>,
                        None,
                    )
                    .into(),
                )
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
        Datatype::INT8 => apply::<i8, aa::Int8Array>(field, ast, op),
        Datatype::INT16 => apply::<i16, aa::Int16Array>(field, ast, op),
        Datatype::INT32 => apply::<i32, aa::Int32Array>(field, ast, op),
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
        | Datatype::TIME_AS => apply::<i64, aa::Int64Array>(field, ast, op),
        Datatype::UINT8
        | Datatype::STRING_ASCII
        | Datatype::STRING_UTF8
        | Datatype::ANY
        | Datatype::BLOB
        | Datatype::BOOL
        | Datatype::GEOM_WKB
        | Datatype::GEOM_WKT => apply::<u8, aa::UInt8Array>(field, ast, op),
        Datatype::UINT16 | Datatype::STRING_UTF16 | Datatype::STRING_UCS2 => {
            apply::<u16, aa::UInt16Array>(field, ast, op)
        }
        Datatype::UINT32 | Datatype::STRING_UTF32 | Datatype::STRING_UCS4 => {
            apply::<u32, aa::UInt32Array>(field, ast, op)
        }
        Datatype::UINT64 => apply::<u64, aa::UInt64Array>(field, ast, op),
        Datatype::FLOAT32 => apply::<f32, aa::Float32Array>(field, ast, op),
        Datatype::FLOAT64 => apply::<f64, aa::Float64Array>(field, ast, op),
        Datatype::CHAR => {
            // CHAR requires signed/unsigned check due to platform differences
            if std::ffi::c_char::MIN == 0 {
                apply::<u8, aa::UInt8Array>(field, ast, op)
            } else {
                apply::<i8, aa::Int8Array>(field, ast, op)
            }
        }
        _ => todo!(),
    }
}

fn leaf_ast_to_in_list(schema: &ArraySchema, ast: &ASTNode, negated: bool) -> anyhow::Result<Expr> {
    let Some(field) = schema.field_cxx(ast.get_field_name()) else {
        todo!()
    };

    fn apply<T>(field: Field, ast: &ASTNode, negated: bool) -> anyhow::Result<Expr>
    where
        T: FromBytes,
        <T as FromBytes>::Bytes: for<'a> TryFrom<&'a [u8]>,
        ScalarValue: From<T>,
    {
        let column = Expr::Column(Column::from_name(field.name()?));

        let in_list = values_iter::<T>(field.datatype(), field.cell_val_num(), ast.get_data())?
            .map(ScalarValue::from)
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
        Datatype::INT8 => apply::<i8>(field, ast, negated),
        Datatype::INT16 => apply::<i16>(field, ast, negated),
        Datatype::INT32 => apply::<i32>(field, ast, negated),
        Datatype::INT64 => apply::<i64>(field, ast, negated),
        Datatype::UINT8 => apply::<u8>(field, ast, negated),
        Datatype::UINT16 => apply::<u16>(field, ast, negated),
        Datatype::UINT32 => apply::<u32>(field, ast, negated),
        Datatype::UINT64 => apply::<u64>(field, ast, negated),
        Datatype::FLOAT32 => apply::<f32>(field, ast, negated),
        Datatype::FLOAT64 => apply::<f64>(field, ast, negated),
        _ => todo!(),
    }
}

fn leaf_ast_to_null_test(schema: &ArraySchema, ast: &ASTNode) -> anyhow::Result<Expr> {
    let Some(field) = schema.field_cxx(ast.get_field_name()) else {
        todo!()
    };
    let column = Expr::Column(Column::from_name(field.name()?));
    if *ast.get_op() == QueryConditionOp::EQ {
        Ok(Expr::IsNull(Box::new(column)))
    } else {
        Ok(Expr::IsNotNull(Box::new(column)))
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
            QueryConditionCombinationOp::NOT => {
                let children = query_condition.children().collect::<Vec<_>>();
                if children.len() != 1 {
                    todo!()
                }
                let negate_arg = to_datafusion_impl(schema, &children[0])?;
                Ok(!negate_arg)
            }
            _ => todo!(),
        }
    } else if query_condition.is_null_test() {
        leaf_ast_to_null_test(schema, query_condition)
    } else {
        match *query_condition.get_op() {
            QueryConditionOp::LT => Ok(leaf_ast_to_binary_expr(
                schema,
                query_condition,
                Operator::Lt,
            )?),
            QueryConditionOp::LE => Ok(leaf_ast_to_binary_expr(
                schema,
                query_condition,
                Operator::LtEq,
            )?),
            QueryConditionOp::GT => Ok(leaf_ast_to_binary_expr(
                schema,
                query_condition,
                Operator::Gt,
            )?),
            QueryConditionOp::GE => Ok(leaf_ast_to_binary_expr(
                schema,
                query_condition,
                Operator::GtEq,
            )?),
            QueryConditionOp::EQ => Ok(leaf_ast_to_binary_expr(
                schema,
                query_condition,
                Operator::Eq,
            )?),
            QueryConditionOp::NE => Ok(leaf_ast_to_binary_expr(
                schema,
                query_condition,
                Operator::NotEq,
            )?),
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
