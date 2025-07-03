pub use tiledb_arrow;
pub use tiledb_expr;

#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        include!("tiledb/sm/array_schema/array_schema.h");
        include!("tiledb/sm/query/ast/query_ast.h");
        include!("tiledb/sm/query/readers/result_tile.h");

        type ArraySchema = tiledb_cxx_interface::sm::array_schema::ArraySchema;
        type ASTNode = tiledb_cxx_interface::sm::query::ast::ASTNode;
        type ResultTile = tiledb_cxx_interface::sm::query::readers::ResultTile;
    }

    #[namespace = "tiledb::test::query_condition_datafusion"]
    unsafe extern "C++" {
        include!("tiledb/oxidize/staticlibs/unit-query-condition/cc/oxidize.h");

        #[cxx_name = "example_schema"]
        fn example_schema_query_condition_datafusion() -> SharedPtr<ArraySchema>;

        #[cxx_name = "instance_ffi"]
        fn instance_query_condition_datafusion(
            array_schema: &ArraySchema,
            tile: &ResultTile,
            ast: &ASTNode,
        ) -> Result<UniquePtr<CxxVector<u8>>>;
    }

    #[namespace = "tiledb::test"]
    extern "Rust" {
        fn examples_query_condition_datafusion() -> Result<bool>;
        fn proptest_query_condition_datafusion() -> Result<bool>;
    }
}

use std::rc::Rc;
use std::sync::Arc;

use anyhow::anyhow;
use arrow::array::{
    Array as _, FixedSizeListArray, GenericListArray, PrimitiveArray, create_array,
};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use proptest::prelude::*;
use proptest::test_runner::{TestCaseError, TestRunner};
use tiledb_common::query::condition::QueryConditionExpr;
use tiledb_common::query::condition::strategy::Parameters as QueryConditionParameters;
use tiledb_pod::array::schema::SchemaData;
use tiledb_pod::array::schema::strategy::Requirements as SchemaRequirements;
use tiledb_test_cells::strategy::{CellsParameters, CellsStrategySchema, SchemaWithDomain};
use tiledb_test_cells::{Cells, FieldData};

fn instance_query_condition_datafusion(
    schema: &SchemaData,
    cells: &Cells,
    condition: &[QueryConditionExpr],
) -> anyhow::Result<()> {
    let cxx_schema = tiledb_test_array_schema::schema_from_pod(schema)?;
    let cxx_tile = tiledb_test_result_tile::result_tile_from_cells(&cxx_schema, cells)?;

    for qc in condition.iter() {
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(qc)?;
        let _ = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
    }

    Ok(())
}

fn examples_query_condition_datafusion() -> anyhow::Result<bool> {
    tiledb_test_ffi::panic_to_exception(examples_query_condition_datafusion_impl)
}

fn examples_query_condition_datafusion_impl() -> anyhow::Result<bool> {
    let cxx_schema = ffi::example_schema_query_condition_datafusion();

    let cells = {
        let d_array = create_array!(UInt64, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_array = create_array!(
            UInt64,
            [
                1, 22, 333, 4444, 55555, 666666, 7777777, 88888888, 999999999, 1010101010
            ]
        );
        let v_array = {
            let utf8_array = arrow::array::LargeStringArray::from_iter(vec![
                Some("one"),
                None,
                Some("onetwo"),
                None,
                Some("onetwothree"),
                None,
                Some("onetwothreefour"),
                None,
                Some("onetwothreefourfive"),
                Some("onetwothreefourfivesix"),
            ]);
            let (offsets, values, nulls) = utf8_array.into_parts();
            let list_array = GenericListArray::try_new(
                ArrowField::new_list_field(ArrowDataType::UInt8, false).into(),
                offsets,
                Arc::new(PrimitiveArray::<arrow::datatypes::UInt8Type>::new(
                    ScalarBuffer::<u8>::from(values),
                    None,
                )),
                nulls,
            )
            .unwrap();
            Arc::new(list_array)
        };
        let f_array = {
            let f_value = create_array!(
                UInt16,
                [
                    1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6, 7, 7,
                    7, 7, 8, 8, 8, 8, 9, 9, 9, 9, 10, 10, 10, 10
                ]
            );
            let f_validity = vec![
                true, true, false, true, true, false, true, true, false, true,
            ]
            .into_iter()
            .collect::<NullBuffer>();
            let f_array = FixedSizeListArray::new(
                ArrowField::new_list_field(f_value.data_type().clone(), false).into(),
                4,
                f_value,
                Some(f_validity),
            );
            Arc::new(f_array)
        };
        let ea_array = create_array!(
            Int32,
            [
                Some(0),
                Some(2),
                Some(4),
                None,
                None,
                Some(1),
                Some(2),
                Some(1),
                None,
                None
            ]
        );
        let ev_array = create_array!(
            Int16,
            [
                Some(4),
                None,
                Some(3),
                Some(4),
                Some(6),
                Some(2),
                None,
                None,
                Some(0),
                Some(4)
            ]
        );

        RecordBatch::try_new(
            ArrowSchema::new(vec![
                ArrowField::new("d", d_array.data_type().clone(), d_array.is_nullable()),
                ArrowField::new("a", a_array.data_type().clone(), a_array.is_nullable()),
                ArrowField::new("v", v_array.data_type().clone(), v_array.is_nullable()),
                ArrowField::new("f", f_array.data_type().clone(), f_array.is_nullable()),
                ArrowField::new("ea", ea_array.data_type().clone(), ea_array.is_nullable()),
                ArrowField::new("ev", ev_array.data_type().clone(), ev_array.is_nullable()),
            ])
            .into(),
            vec![d_array, a_array, v_array, f_array, ea_array, ev_array],
        )
        .unwrap()
    };

    let cxx_tile = tiledb_test_result_tile::PackagedResultTile::new(&cxx_schema, cells)?;

    // a < 100000
    {
        let ast = QueryConditionExpr::field("a").lt(100000u64);
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(&ast)?;
        let result = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
        assert_eq!(result.as_slice(), vec![1, 1, 1, 1, 1, 0, 0, 0, 0, 0]);
    }
    // d != 6
    {
        let ast = QueryConditionExpr::field("d").ne(6u64);
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(&ast)?;
        let result = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
        assert_eq!(result.as_slice(), vec![1, 1, 1, 1, 1, 0, 1, 1, 1, 1]);
    }
    // 4 <= d <= 8
    {
        let lhs = QueryConditionExpr::field("d").ge(4u64);
        let cxx_lhs = tiledb_test_query_condition::ast_from_query_condition(&lhs)?;
        let result_lhs =
            ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_lhs)?;
        assert_eq!(result_lhs.as_slice(), vec![0, 0, 0, 1, 1, 1, 1, 1, 1, 1]);

        let rhs = QueryConditionExpr::field("d").le(8u64);
        let cxx_rhs = tiledb_test_query_condition::ast_from_query_condition(&rhs)?;
        let result_rhs =
            ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_rhs)?;
        assert_eq!(result_rhs.as_slice(), vec![1, 1, 1, 1, 1, 1, 1, 1, 0, 0]);

        let ast = lhs.clone() & rhs.clone();
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(&ast)?;
        let result = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
        assert_eq!(result.as_slice(), vec![0, 0, 0, 1, 1, 1, 1, 1, 0, 0]);
    }
    // v = 'onetwothree'
    {
        let ast = QueryConditionExpr::field("v").eq("onetwothree");
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(&ast)?;
        let result = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
        assert_eq!(result.as_slice(), vec![0, 0, 0, 0, 1, 0, 0, 0, 0, 0]);
    }
    // v IS NOT NULL
    {
        let ast = QueryConditionExpr::field("v").not_null();
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(&ast)?;
        let result = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
        assert_eq!(result.as_slice(), vec![1, 0, 1, 0, 1, 0, 1, 0, 1, 1]);
    }
    // v IS NULL
    {
        let ast = QueryConditionExpr::field("v").is_null();
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(&ast)?;
        let result = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
        assert_eq!(result.as_slice(), vec![0, 1, 0, 1, 0, 1, 0, 1, 0, 0]);
    }
    // f IS NOT NULL
    {
        let ast = QueryConditionExpr::field("f").not_null();
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(&ast)?;
        let result = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
        assert_eq!(result.as_slice(), vec![1, 1, 0, 1, 1, 0, 1, 1, 0, 1]);
    }
    // d < 4 OR a > 1000000
    {
        let lhs = QueryConditionExpr::field("d").lt(4u64);
        let cxx_lhs = tiledb_test_query_condition::ast_from_query_condition(&lhs)?;
        let lhs_result =
            ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_lhs)?;
        assert_eq!(lhs_result.as_slice(), vec![1, 1, 1, 0, 0, 0, 0, 0, 0, 0]);

        let rhs = QueryConditionExpr::field("a").gt(1000000u64);
        let cxx_rhs = tiledb_test_query_condition::ast_from_query_condition(&rhs)?;
        let rhs_result =
            ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_rhs)?;
        assert_eq!(rhs_result.as_slice(), vec![0, 0, 0, 0, 0, 0, 1, 1, 1, 1]);

        let ast = lhs.clone() | rhs.clone();
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(&ast)?;
        let result = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
        assert_eq!(result.as_slice(), vec![1, 1, 1, 0, 0, 0, 1, 1, 1, 1]);
    }
    // ea = -2
    {
        let ast = QueryConditionExpr::field("ea").eq(-2i64);
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(&ast)?;
        let result = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
        assert_eq!(result.as_slice(), vec![0, 0, 0, 0, 0, 1, 0, 1, 0, 0]);
    }
    // ev != 'grault'
    {
        let ast = QueryConditionExpr::field("ev").ne("grault");
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(&ast)?;
        let result = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
        assert_eq!(result.as_slice(), vec![0, 0, 1, 0, 1, 1, 0, 0, 1, 0]);
    }

    Ok(true)
}

fn cells_ensure_utf8(schema: &SchemaData, cells: Cells) -> Cells {
    let mut new_fields = cells.fields().clone();
    for (fname, fdata) in new_fields.iter_mut() {
        let Some(field) = schema.field(fname.clone()) else {
            continue;
        };

        use tiledb_common::array::CellValNum;
        use tiledb_common::datatype::Datatype;

        if matches!(field.cell_val_num(), Some(CellValNum::Var))
            && matches!(
                field.datatype(),
                Datatype::StringAscii | Datatype::StringUtf8
            )
        {
            let FieldData::VecUInt8(strs) = fdata else {
                continue;
            };
            strs.iter_mut().for_each(|s| {
                *s = String::from_utf8_lossy(s).into_owned().into_bytes();
            });
        }
    }
    Cells::new(new_fields)
}

/// Returns a [Strategy] which produces inputs to `instance_query_condition_datafusion`.
fn strat_query_condition_datafusion()
-> impl Strategy<Value = (Rc<SchemaData>, Rc<Cells>, Vec<QueryConditionExpr>)> {
    let schema_params = SchemaRequirements {
        // NB: enumerations are not working properly with `Cells`.
        // The best thing to do would be to remove `Cells` and just use `RecordBatch`,
        // so we're not going to worry about it since we do have some test examples
        // with enumerations.
        attribute_enumeration_likelihood: 0.0,
        ..Default::default()
    };
    any_with::<SchemaData>(schema_params.into())
        .prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            let schema_move_into_strat = Rc::clone(&schema);
            let strat_cells = any_with::<Cells>(CellsParameters {
                schema: Some(CellsStrategySchema::WriteSchema(Rc::clone(&schema))),
                ..Default::default()
            })
            .prop_map(move |cells| cells_ensure_utf8(&schema_move_into_strat, cells));
            (Just(schema), strat_cells)
        })
        .prop_flat_map(|(schema, cells)| {
            let cells = Rc::new(cells);
            let strat_qc = any_with::<QueryConditionExpr>(QueryConditionParameters {
                domain: Some(Rc::new(SchemaWithDomain::new(Rc::clone(&schema), &cells))),
                ..Default::default()
            });
            (Just(schema), Just(cells), strat_qc.prop_map(|qc| vec![qc]))
        })
}

/// Evaluates `instance_query_condition_datafusion` against values drawn randomly
/// from `strat_query_condition_datafusion`.
///
/// Returns `Ok` if all test cases were successful and `Err` otherwise,
/// logging the "minimum" failing example to standard output.
fn proptest_query_condition_datafusion() -> anyhow::Result<bool> {
    let mut runner = TestRunner::new(proptest::test_runner::Config {
        cases: 2048,
        ..Default::default()
    });
    match runner.run(
        &strat_query_condition_datafusion(),
        |(schema, cells, condition): (Rc<SchemaData>, Rc<Cells>, Vec<QueryConditionExpr>)| {
            instance_query_condition_datafusion(&schema, &cells, &condition)
                .map_err(|e| TestCaseError::Fail(e.to_string().into()))
        },
    ) {
        Ok(_) => Ok(true),
        Err(e) => Err(anyhow!(e.to_string())),
    }
}
