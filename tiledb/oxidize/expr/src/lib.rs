#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        include!("tiledb/sm/array_schema/array_schema.h");
        include!("tiledb/sm/query/ast/query_ast.h");

        type ArraySchema = tiledb_cxx_interface::sm::array_schema::ArraySchema;
        type ASTNode = tiledb_cxx_interface::sm::query::ast::ASTNode;
        type Datatype = tiledb_cxx_interface::sm::enums::Datatype;
    }

    extern "C++" {
        include!("tiledb/oxidize/arrow.h");

        #[namespace = "tiledb::oxidize::arrow::record_batch"]
        type ArrowRecordBatch = tiledb_arrow::record_batch::ArrowRecordBatch;

        #[namespace = "tiledb::oxidize::arrow::schema"]
        type ArrowSchema = tiledb_arrow::schema::ArrowSchema;
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

    #[namespace = "tiledb::oxidize::datafusion::physical_expr"]
    extern "Rust" {
        type PhysicalExpr;
        fn evaluate(&self, records: &ArrowRecordBatch) -> Result<Box<PhysicalExprOutput>>;

        // TODO: we can avoid the double box using the trait object trick,
        // see the pdavis 65154 branch
        #[cxx_name = "create"]
        fn create_physical_expr(
            schema: &ArrowSchema,
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

pub use logical_expr::{LogicalExpr, to_datafusion as query_condition_to_logical_expr};
pub use physical_expr::{PhysicalExpr, PhysicalExprOutput, create_physical_expr};
