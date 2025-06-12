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

    #[namespace = "tiledb::test"]
    unsafe extern "C++" {
        include!("tiledb/oxidize/bundle/unit-query-condition/cc/oxidize.h");

        fn instance_query_condition_datafusion(
            array_schema: &ArraySchema,
            tile: &ResultTile,
            ast: &ASTNode,
        ) -> Result<()>;
    }

    #[namespace = "tiledb::test"]
    extern "Rust" {
        fn proptest_query_condition_datafusion();
    }
}

use std::rc::Rc;

use proptest::prelude::*;
use tiledb_common::query::condition::QueryConditionExpr;
use tiledb_common::query::condition::strategy::Parameters as QueryConditionParameters;
use tiledb_pod::array::schema::SchemaData;
use tiledb_test_cells::Cells;
use tiledb_test_cells::strategy::{
    CellsAsQueryConditionSchema, CellsParameters, CellsStrategySchema,
};

fn instance_query_condition_datafusion(
    schema: &SchemaData,
    cells: &Cells,
    condition: &[QueryConditionExpr],
) -> anyhow::Result<()> {
    let cxx_schema = tiledb_proptest_array_schema::schema_from_pod(schema)?;
    let cxx_tile = tiledb_proptest_result_tile::result_tile_from_cells(&cxx_schema, cells)?;

    for qc in condition.iter() {
        let cxx_ast = tiledb_proptest_query_condition::ast_from_query_condition(qc)?;
        ffi::instance_query_condition_datafusion(&cxx_schema, &cxx_tile, &cxx_ast)?;
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
                domain: Some(Rc::new(CellsAsQueryConditionSchema::new(Rc::clone(&cells)))),
                ..Default::default()
            });
            (
                Just(schema),
                Just(cells),
                proptest::collection::vec(strat_params, 1..=32),
            )
        })
}

proptest! {
    fn proptest_query_condition_datafusion(
        (schema, condition, cells) in strat_query_condition_datafusion()
    ) {
        instance_query_condition_datafusion(&schema, &condition, &cells)
            .expect("Error in instance_query_condition_datafusion");
    }
}
