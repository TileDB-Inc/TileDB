#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        include!("tiledb/sm/array_schema/array_schema.h");
        include!("tiledb/sm/query/ast/query_ast.h");
        include!("tiledb/sm/query/readers/result_tile.h");

        type ArraySchema = oxidize::sm::array_schema::ArraySchema;
        type ASTNode = oxidize::sm::query::ast::ASTNode;
        type Datatype = oxidize::sm::enums::Datatype;
        type ResultTile = oxidize::sm::query::readers::ResultTile;
    }

    #[namespace = "tiledb::oxidize::datafusion::schema"]
    extern "Rust" {
        type DataFusionSchema;

        #[cxx_name = "create"]
        fn array_schema_to_dfschema(
            schema: &ArraySchema,
            select: &CxxVector<CxxString>,
        ) -> Result<Box<DataFusionSchema>>;
    }

    #[namespace = "tiledb::oxidize::datafusion::logical_expr"]
    extern "Rust" {
        type LogicalExpr;
        fn to_string(&self) -> String;

        #[cxx_name = "create"]
        fn query_condition_to_logical_expr(
            schema: &ArraySchema,
            query_condition: &ASTNode,
        ) -> Result<Box<LogicalExpr>>;
    }

    #[namespace = "tiledb::oxidize::arrow"]
    extern "Rust" {
        type ArrowRecordBatch;

        #[cxx_name = "to_record_batch"]
        fn result_tile_to_record_batch(
            schema: &DataFusionSchema,
            tile: &ResultTile,
        ) -> Result<Box<ArrowRecordBatch>>;
    }

    #[namespace = "tiledb::oxidize::datafusion::physical_expr"]
    extern "Rust" {
        type PhysicalExpr;
        fn evaluate(&self, records: &ArrowRecordBatch) -> Result<Box<PhysicalExprOutput>>;

        // TODO: we can avoid the double box using the trait object trick,
        // see the pdavis 65154 branch
        #[cxx_name = "create"]
        fn create_physical_expr(
            schema: &DataFusionSchema,
            expr: Box<LogicalExpr>,
        ) -> Result<Box<PhysicalExpr>>;
    }

    #[namespace = "tiledb::oxidize::datafusion::physical_expr"]
    extern "Rust" {
        type PhysicalExprOutput;

        fn is_scalar(&self) -> bool;
        fn is_array(&self) -> bool;

        fn cast_to(&self, datatype: Datatype) -> Result<Box<PhysicalExprOutput>>;

        fn values_u8(&self) -> Result<&[u8]>;
        fn values_u64(&self) -> Result<&[u64]>;
    }
}

mod logical_expr;
mod physical_expr;
mod record_batch;
mod schema;

pub use logical_expr::{LogicalExpr, to_datafusion as query_condition_to_logical_expr};
pub use physical_expr::{PhysicalExpr, PhysicalExprOutput, create_physical_expr};
pub use record_batch::{ArrowRecordBatch, to_record_batch as result_tile_to_record_batch};
pub use schema::{DataFusionSchema, to_datafusion_cxx as array_schema_to_dfschema};
