use std::str::Utf8Error;
use std::sync::Arc;

use datafusion::common::arrow::array::{
    self as aa, Array as ArrowArray, ArrayData, FixedSizeListArray, GenericListArray,
};
use datafusion::common::arrow::buffer::OffsetBuffer;
use datafusion::common::arrow::datatypes::Field as ArrowField;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datatype::apply_physical_type;
use itertools::Itertools;
use num_traits::FromBytes;
use oxidize::sm::array_schema::{ArraySchema, CellValNum, Field};
use oxidize::sm::enums::{Datatype, QueryConditionCombinationOp, QueryConditionOp};
use oxidize::sm::misc::ByteVecValue;
use oxidize::sm::query::ast::ASTNode;

use crate::offsets::Error as OffsetsError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Logical expression internal error: {0}")]
    Internal(#[from] InternalError),
    #[error("Logical expression error: {0}")]
    User(#[from] UserError),
}

#[derive(Debug, thiserror::Error)]
pub enum InternalError {
    #[error("Internal error: invalid discriminant for field datatype: {0}")]
    InvalidDatatype(u64),
    #[error("Internal error: invalid discriminant for query condition operator: {0}")]
    InvalidOp(u64),
    #[error("Internal error: invalid discriminant for query condition combination operator: {0}")]
    InvalidCombinationOp(u64),
    #[error("Invalid number of arguments to NOT expression: expected 1, found {0}")]
    NotTree(usize),
    #[error("Error in field '{0}': {1}")]
    SchemaField(String, crate::schema::FieldError),
}

#[derive(Debug, thiserror::Error)]
pub enum UserError {
    #[error("Unknown dimension or attribute: {0}")]
    UnknownField(String),
    #[error("Field name is not UTF-8: {0}")]
    FieldNameNotUtf8(Utf8Error),
    #[error("Value cannot be coerced to datatype '{0}': invalid value size '{1}'")]
    DatatypeMismatch(Datatype, usize),
    #[error("Cell val num mismatch: expected {0}, found {1}")]
    CellValNumMismatch(CellValNum, usize),
    #[error("Cell val num out of range: {0}")]
    CellValNumOutOfRange(CellValNum),
    #[error("Variable-length data offsets: ")]
    InListVarOffsets(#[from] OffsetsError),
}

pub struct LogicalExpr(pub Expr);

impl LogicalExpr {
    pub fn to_string(&self) -> String {
        self.0.human_display().to_string()
    }
}

fn values_iter<T>(
    datatype: Datatype,
    bytes: &ByteVecValue,
) -> Result<impl Iterator<Item = T>, Error>
where
    T: FromBytes,
    <T as FromBytes>::Bytes: for<'a> TryFrom<&'a [u8]>,
{
    if bytes.size() % datatype.value_size() != 0 {
        return Err(UserError::DatatypeMismatch(datatype, bytes.size()).into());
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
        return Err(
            UserError::UnknownField(ast.get_field_name().to_string_lossy().into_owned()).into(),
        );
    };

    fn apply<T>(field: &Field, ast: &ASTNode, operator: Operator) -> Result<Expr, Error>
    where
        T: FromBytes,
        <T as FromBytes>::Bytes: for<'a> TryFrom<&'a [u8]>,
        ScalarValue: From<T> + From<Option<T>>,
    {
        let column = Expr::Column(Column::from_name(
            field.name().map_err(UserError::FieldNameNotUtf8)?,
        ));

        let mut values = values_iter::<T>(field.datatype(), ast.get_data())?
            .map(ScalarValue::from)
            .peekable();

        let expect_datatype = crate::schema::field_arrow_datatype(field).map_err(|e| {
            InternalError::SchemaField(field.name_cxx().to_string_lossy().into_owned(), e)
        })?;

        let right = match field.cell_val_num() {
            CellValNum::Single => {
                let Some(r) = values.next() else {
                    return Err(UserError::CellValNumMismatch(field.cell_val_num(), 0).into());
                };
                if values.peek().is_some() {
                    return Err(UserError::CellValNumMismatch(
                        field.cell_val_num(),
                        1 + values.count(),
                    )
                    .into());
                } else {
                    r
                }
            }
            CellValNum::Fixed(nz) => {
                let values = if values.peek().is_none() {
                    aa::make_array(ArrayData::new_empty(&expect_datatype))
                } else {
                    // SAFETY: `values` produces a static type, so all will match.
                    // `values` is also non-empty per `peek`.
                    ScalarValue::iter_to_array(values).unwrap()
                };
                if values.len() != nz.get() as usize {
                    return Err(
                        UserError::CellValNumMismatch(field.cell_val_num(), values.len()).into(),
                    );
                }
                let Ok(fixed_size) = i32::try_from(nz.get()) else {
                    return Err(UserError::CellValNumOutOfRange(field.cell_val_num()).into());
                };
                let element_field = ArrowField::new_list_field(values.data_type().clone(), false);
                ScalarValue::FixedSizeList(
                    FixedSizeListArray::new(element_field.into(), fixed_size, values, None).into(),
                )
            }
            CellValNum::Var => {
                let values = if values.peek().is_none() {
                    aa::make_array(ArrayData::new_empty(&expect_datatype))
                } else {
                    // SAFETY: `values` produces a static type, so all will match.
                    // `values` is also non-empty per `peek`.
                    ScalarValue::iter_to_array(values).unwrap()
                };
                let element_field = ArrowField::new_list_field(values.data_type().clone(), false);
                ScalarValue::LargeList(
                    GenericListArray::<i64>::new(
                        element_field.into(),
                        OffsetBuffer::<i64>::from_lengths(std::iter::once(values.len())),
                        values,
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
    apply_physical_type!(
        value_type,
        NativeType,
        apply::<NativeType>(&field, ast, op),
        |invalid: Datatype| Err(InternalError::InvalidDatatype(invalid.repr.into()).into())
    )
}

fn leaf_ast_to_in_list(schema: &ArraySchema, ast: &ASTNode, negated: bool) -> Result<Expr, Error> {
    let Some(field) = schema.field_cxx(ast.get_field_name()) else {
        return Err(
            UserError::UnknownField(ast.get_field_name().to_string_lossy().into_owned()).into(),
        );
    };

    fn apply<T>(field: &Field, ast: &ASTNode, negated: bool) -> Result<Expr, Error>
    where
        T: FromBytes,
        <T as FromBytes>::Bytes: for<'a> TryFrom<&'a [u8]>,
        ScalarValue: From<T>,
    {
        let column = Expr::Column(Column::from_name(
            field.name().map_err(UserError::FieldNameNotUtf8)?,
        ));

        let scalars = values_iter::<T>(field.datatype(), ast.get_data())?.map(ScalarValue::from);

        let in_list = match field.cell_val_num() {
            CellValNum::Single => scalars.map(Expr::Literal).collect::<Vec<_>>(),
            CellValNum::Fixed(_) => {
                // SAFETY: well unknown to be honest, has anyone ever tried query conditions on
                // these?
                unreachable!()
            }
            CellValNum::Var => {
                let array_values = {
                    // SAFETY: `values_iter` produces all the same native type
                    ScalarValue::iter_to_array(scalars).unwrap()
                };
                assert!(!array_values.is_nullable());

                let array_offsets = crate::offsets::try_from_bytes_and_num_values(
                    field.datatype().value_size(),
                    ast.get_offsets().as_slice(),
                    ast.get_data().len(),
                )
                .map_err(UserError::from)?;

                let element_field = Arc::new(ArrowField::new_list_field(
                    array_values.data_type().clone(),
                    false,
                ));

                array_offsets
                    .windows(2)
                    .map(|w| {
                        let elts = array_values.slice(w[0] as usize, (w[1] - w[0]) as usize);
                        Arc::new(GenericListArray::new(
                            Arc::clone(&element_field),
                            OffsetBuffer::from_lengths(std::iter::once(elts.len())),
                            elts,
                            None,
                        ))
                    })
                    .map(ScalarValue::LargeList)
                    .map(Expr::Literal)
                    .collect::<Vec<_>>()
            }
        };

        Ok(Expr::InList(InList {
            expr: Box::new(column),
            list: in_list,
            negated,
        }))
    }

    let value_type = field.datatype();
    apply_physical_type!(
        value_type,
        NativeType,
        apply::<NativeType>(&field, ast, negated),
        |invalid: Datatype| Err(InternalError::InvalidDatatype(invalid.repr.into()).into())
    )
}

fn leaf_ast_to_null_test(schema: &ArraySchema, ast: &ASTNode) -> Result<Expr, Error> {
    let Some(field) = schema.field_cxx(ast.get_field_name()) else {
        return Err(
            UserError::UnknownField(ast.get_field_name().to_string_lossy().into_owned()).into(),
        );
    };
    let column = Expr::Column(Column::from_name(
        field.name().map_err(UserError::FieldNameNotUtf8)?,
    ));
    match *ast.get_op() {
        QueryConditionOp::EQ => Ok(Expr::IsNull(Box::new(column))),
        QueryConditionOp::NE => Ok(Expr::IsNotNull(Box::new(column))),
        QueryConditionOp::ALWAYS_TRUE => Ok(Expr::Literal(ScalarValue::Boolean(Some(true)))),
        QueryConditionOp::ALWAYS_FALSE => Ok(Expr::Literal(ScalarValue::Boolean(Some(false)))),
        QueryConditionOp::LT
        | QueryConditionOp::LE
        | QueryConditionOp::GT
        | QueryConditionOp::GE => {
            // TODO: are these invalid?
            Ok(Expr::Literal(ScalarValue::Boolean(Some(false))))
        }
        invalid => return Err(InternalError::InvalidOp(invalid.repr.into()).into()),
    }
}

fn combination_ast_to_binary_expr(
    schema: &ArraySchema,
    query_condition: &ASTNode,
    operator: Operator,
) -> Result<Expr, Error> {
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

fn to_datafusion_impl(schema: &ArraySchema, query_condition: &ASTNode) -> Result<Expr, Error> {
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
                    return Err(InternalError::NotTree(children.len()).into());
                }
                let negate_arg = to_datafusion_impl(schema, &children[0])?;
                Ok(!negate_arg)
            }
            invalid => return Err(InternalError::InvalidCombinationOp(invalid.repr.into()).into()),
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
            invalid => return Err(InternalError::InvalidOp(invalid.repr.into()).into()),
        }
    }
}

pub fn to_datafusion(
    schema: &ArraySchema,
    query_condition: &ASTNode,
) -> Result<Box<LogicalExpr>, Error> {
    Ok(Box::new(LogicalExpr(to_datafusion_impl(
        schema,
        query_condition,
    )?)))
}
