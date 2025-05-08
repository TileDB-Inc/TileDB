#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        type ByteVecValue = crate::sm::misc::ByteVecValue;
        type QueryConditionOp = crate::sm::enums::QueryConditionOp;
        type QueryConditionCombinationOp = crate::sm::enums::QueryConditionCombinationOp;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/query/ast/query_ast.h");

        type ASTNode;
        fn is_expr(&self) -> bool;
        fn get_field_name(&self) -> &CxxString;
        fn get_op(&self) -> &QueryConditionOp;
        fn get_combination_op(&self) -> &QueryConditionCombinationOp;
        fn get_data(&self) -> &ByteVecValue;
        fn num_children(&self) -> u64;
        fn get_child(&self, i: u64) -> *const ASTNode;
    }
}

pub use ffi::ASTNode;

use crate::sm::enums::QueryConditionOp;

impl ASTNode {
    pub fn children(&self) -> impl Iterator<Item = &ASTNode> {
        (0..self.num_children()).map(|i| {
            // SAFETY: `i` should be valid due to range
            unsafe { &*self.get_child(i) }
        })
    }

    pub fn is_null_test(&self) -> bool {
        self.get_data().size() == 0
            && !matches!(
                *self.get_op(),
                QueryConditionOp::IN | QueryConditionOp::NOT_IN
            )
    }
}
