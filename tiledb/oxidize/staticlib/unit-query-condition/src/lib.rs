pub use tiledb_arrow;
pub use tiledb_expr;

#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        include!("tiledb/sm/array_schema/array_schema.h");
        include!("tiledb/sm/query/ast/query_ast.h");
        include!("tiledb/sm/query/readers/result_tile.h");

        type ArraySchema = tiledb_oxidize::sm::array_schema::ArraySchema;
        type ASTNode = tiledb_oxidize::sm::query::ast::ASTNode;
        type ResultTile = tiledb_oxidize::sm::query::readers::ResultTile;
    }

    #[namespace = "tiledb::test::query_condition_datafusion"]
    unsafe extern "C++" {
        include!("tiledb/oxidize/staticlib/unit-query-condition/cc/oxidize.h");

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
        fn examples_query_condition_datafusion() -> Result<()>;
        fn proptest_query_condition_datafusion() -> Result<()>;
    }
}

use std::collections::HashMap;
use std::rc::Rc;

use anyhow::anyhow;
use proptest::prelude::*;
use proptest::test_runner::{TestCaseError, TestRunner};
use tiledb_common::query::condition::QueryConditionExpr;
use tiledb_common::query::condition::strategy::Parameters as QueryConditionParameters;
use tiledb_pod::array::schema::SchemaData;
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

fn examples_query_condition_datafusion() -> anyhow::Result<()> {
    let cxx_schema = ffi::example_schema_query_condition_datafusion();

    let cells = Cells::new(HashMap::from([
        (
            "d".to_owned(),
            FieldData::UInt64(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        ),
        (
            "a".to_owned(),
            FieldData::UInt64(vec![
                1, 22, 333, 4444, 55555, 666666, 7777777, 88888888, 999999999, 1010101010,
            ]),
        ),
        (
            "v".to_owned(),
            FieldData::VecUInt8(
                vec![
                    "one",
                    "onetwo",
                    "onetwothree",
                    "onetwothreefour",
                    "onetwothreefourfive",
                    "onetwothreefourfivesix",
                    "onetwothreefourfivesixseven",
                    "onetwothreefourfivesixseveneight",
                    "onetwothreefourfivesixseveneightnine",
                    "onetwothreefourfivesixseveneightnineten",
                ]
                .into_iter()
                .map(|s| s.bytes().collect::<Vec<u8>>())
                .collect::<Vec<Vec<u8>>>(),
            ),
        ),
        (
            "f".to_owned(),
            FieldData::VecUInt16(vec![
                vec![1, 1, 1, 1],
                vec![2, 2, 2, 2],
                vec![3, 3, 3, 3],
                vec![4, 4, 4, 4],
                vec![5, 5, 5, 5],
                vec![6, 6, 6, 6],
                vec![7, 7, 7, 7],
                vec![8, 8, 8, 8],
                vec![9, 9, 9, 9],
                vec![10, 10, 10, 10],
            ]),
        ),
    ]));

    let cxx_tile = tiledb_test_result_tile::result_tile_from_cells(&cxx_schema, &cells)?;

    {
        let ast = QueryConditionExpr::field("a").lt(100000u64);
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(&ast)?;
        let result = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
        assert_eq!(result.as_slice(), vec![1, 1, 1, 1, 1, 0, 0, 0, 0, 0]);
    }
    {
        let ast = QueryConditionExpr::field("d").ne(6u64);
        let cxx_ast = tiledb_test_query_condition::ast_from_query_condition(&ast)?;
        let result = ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
        assert_eq!(result.as_slice(), vec![1, 1, 1, 1, 1, 0, 1, 1, 1, 1]);
    }
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

    Ok(())
}

fn strat_query_condition_datafusion()
-> impl Strategy<Value = (Rc<SchemaData>, Rc<Cells>, Vec<QueryConditionExpr>)> {
    any::<SchemaData>()
        .prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            let strat_cells = any_with::<Cells>(CellsParameters {
                schema: Some(CellsStrategySchema::WriteSchema(Rc::clone(&schema))),
                ..Default::default()
            });
            (Just(schema), strat_cells)
        })
        .prop_flat_map(|(schema, cells)| {
            let cells = Rc::new(cells);
            let strat_params = any_with::<QueryConditionExpr>(QueryConditionParameters {
                domain: Some(Rc::new(SchemaWithDomain::new(Rc::clone(&schema), &cells))),
                ..Default::default()
            });
            (
                Just(schema),
                Just(cells),
                strat_params.prop_map(|qc| vec![qc]),
            )
        })
}

fn proptest_query_condition_datafusion() -> anyhow::Result<()> {
    let mut runner = TestRunner::new(Default::default());
    match runner.run(
        &strat_query_condition_datafusion(),
        |(schema, cells, condition): (Rc<SchemaData>, Rc<Cells>, Vec<QueryConditionExpr>)| {
            instance_query_condition_datafusion(&schema, &cells, &condition)
                .map_err(|e| TestCaseError::Fail(e.to_string().into()))
        },
    ) {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!(e.to_string())),
    }
}
