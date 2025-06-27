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
        fn is_predicate(&self, schema: &ArraySchema) -> Result<bool>;
        fn has_aggregate_functions(&self) -> bool;
        fn to_string(&self) -> String;

        fn columns(&self) -> Vec<String>;

        #[cxx_name = "create"]
        fn query_condition_to_logical_expr(
            schema: &ArraySchema,
            query_condition: &ASTNode,
        ) -> Result<Box<LogicalExpr>>;

        /// Returns a conjunction of the logical exprs `e1 AND e2 AND ... AND eN`.
        fn make_conjunction(exprs: &[Box<LogicalExpr>]) -> Box<LogicalExpr>;
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
mod query_condition;

pub use logical_expr::{LogicalExpr, make_conjunction};
pub use physical_expr::{PhysicalExpr, PhysicalExprOutput, create_physical_expr};
pub use query_condition::to_datafusion as query_condition_to_logical_expr;

unsafe impl cxx::ExternType for LogicalExpr {
    type Id = cxx::type_id!("tiledb::oxidize::datafusion::logical_expr::LogicalExpr");
    type Kind = cxx::kind::Opaque;
}
