#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/enums/query_condition_op.h");
        type QueryConditionOp;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/query/ast/query_ast.h");

        type ASTNode;
        fn is_expr(&self) -> bool;
        fn get_field_name(&self) -> &CxxString;
        fn get_value_size(&self) -> u64;
        fn get_op(&self) -> &QueryConditionOp;
    }

    #[namespace = "tiledb::rust"]
    extern "Rust" {
        type Expr;

        fn to_datafusion(query_condition: &ASTNode) -> Result<Box<Expr>>;
    }
}

struct Expr {
    datafusion_expr: datafusion::logical_expr::Expr,
}

fn to_datafusion(query_condition: &crate::ffi::ASTNode) -> anyhow::Result<Box<Expr>> {
    todo!()
}
