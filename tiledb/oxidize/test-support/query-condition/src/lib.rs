//! Provides methods for constructing internal query condition objects
//! from the objects which represent them in `tiledb_common`.
//!
//! This enables property-based testing against arbitrary query conditions
//! using the strategies we have already written in `tiledb_common`.

mod enums;

use tiledb_common::query::condition::*;
use tiledb_cxx_interface::sm::query::ast::ASTNode;
use tiledb_test_support_cxx_interface::query as tiledb_test_support;

/// Constructs an internal query condition syntax tree from a [QueryConditionExpr].
pub fn ast_from_query_condition(
    query_condition: &QueryConditionExpr,
) -> anyhow::Result<cxx::SharedPtr<ASTNode>> {
    match query_condition {
        QueryConditionExpr::Cond(predicate) => ast_from_predicate(predicate),
        QueryConditionExpr::Comb { lhs, rhs, op } => {
            let lhs = ast_from_query_condition(lhs)?;
            let rhs = ast_from_query_condition(rhs)?;
            let op = enums::convert_combination_op(*op);
            Ok(tiledb_test_support::new_ast_combination(lhs, rhs, op))
        }
        QueryConditionExpr::Negate(qc) => {
            let arg = ast_from_query_condition(qc)?;
            Ok(tiledb_test_support::new_ast_negate(arg))
        }
    }
}

/// Constructs an internal query condition syntax tree from an atomic query condition [Predicate].
fn ast_from_predicate(predicate: &Predicate) -> anyhow::Result<cxx::SharedPtr<ASTNode>> {
    match predicate {
        Predicate::Equality(p) => {
            cxx::let_cxx_string! { field = p.field() };
            let op = enums::convert_equality_op(p.operation());
            let value_bytes = p.value().to_bytes();
            Ok(tiledb_test_support::new_ast_value_node(
                &field,
                op,
                &value_bytes,
            ))
        }
        Predicate::SetMembership(p) => {
            cxx::let_cxx_string! { field = p.field() };
            let op = enums::convert_set_membership_op(p.operation());
            let (value_bytes, value_offsets) = if let Some((values_ptr, values_size)) =
                p.members().as_ptr_and_size()
            {
                let value_bytes = if values_size == 0 {
                    unsafe {
                        std::slice::from_raw_parts(std::ptr::NonNull::<u8>::dangling().as_ptr(), 0)
                    }
                } else {
                    unsafe {
                        std::slice::from_raw_parts(values_ptr as *const u8, values_size as usize)
                    }
                };
                let value_offsets =
                    std::iter::repeat_n(p.members().elem_size() as u64, p.members().len())
                        .scan(0u64, |state, s| {
                            let offset = *state;
                            *state += s;
                            Some(offset)
                        })
                        .collect::<Vec<u64>>();

                (value_bytes.to_vec(), value_offsets)
            } else {
                let SetMembers::String(strs) = p.members() else {
                    // SAFETY: only way that `as_ptr_and_size()` is `None`
                    unreachable!()
                };
                let value_bytes = strs.iter().flat_map(|s| s.bytes()).collect::<Vec<u8>>();
                let value_offsets = strs
                    .iter()
                    .scan(0u64, |state, s| {
                        let offset = *state;
                        *state += s.len() as u64;
                        Some(offset)
                    })
                    .collect::<Vec<u64>>();
                (value_bytes, value_offsets)
            };
            Ok(tiledb_test_support::new_ast_value_node_var(
                &field,
                op,
                &value_bytes,
                &value_offsets,
            ))
        }
        Predicate::Nullness(p) => {
            cxx::let_cxx_string! { field = p.field() };
            let op = enums::convert_nullness_op(p.operation());
            Ok(tiledb_test_support::new_ast_value_node_null(&field, op))
        }
    }
}
