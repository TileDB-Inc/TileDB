#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        include!("tiledb/common/memory_tracker.h");
        include!("tiledb/oxidize/oxidize/cc/array_schema.h");
        include!("tiledb/sm/array_schema/attribute.h");
        include!("tiledb/sm/array_schema/dimension.h");
        include!("tiledb/sm/array_schema/domain.h");
        include!("tiledb/sm/array_schema/array_schema.h");
        include!("tiledb/sm/enums/array_type.h");
        include!("tiledb/sm/enums/datatype.h");
        include!("tiledb/sm/enums/query_condition_combination_op.h");
        include!("tiledb/sm/enums/query_condition_op.h");
        include!("tiledb/sm/query/ast/query_ast.h");
        include!("tiledb/sm/query/readers/result_tile.h");

        type ArrayType = tiledb_oxidize::sm::enums::ArrayType;
        type Datatype = tiledb_oxidize::sm::enums::Datatype;
        type MemoryTracker = tiledb_oxidize::common::memory_tracker::MemoryTracker;

        type Attribute = tiledb_oxidize::sm::array_schema::Attribute;
        type ConstAttribute = tiledb_oxidize::sm::array_schema::ConstAttribute;
        type Dimension = tiledb_oxidize::sm::array_schema::Dimension;
        type Domain = tiledb_oxidize::sm::array_schema::Domain;
        type ArraySchema = tiledb_oxidize::sm::array_schema::ArraySchema;

        type ASTNode = tiledb_oxidize::sm::query::ast::ASTNode;
        type QueryConditionOp = tiledb_oxidize::sm::enums::QueryConditionOp;
        type QueryConditionCombinationOp = tiledb_oxidize::sm::enums::QueryConditionCombinationOp;

        type ResultTile = tiledb_oxidize::sm::query::readers::ResultTile;
    }

    #[namespace = "tiledb::test"]
    unsafe extern "C++" {
        include!("test/support/src/mem_helpers.h");

        fn get_test_memory_tracker() -> SharedPtr<MemoryTracker>;
    }

    #[namespace = "tiledb::test::oxidize"]
    unsafe extern "C++" {
        include!("tiledb/oxidize/test-support/cc/array_schema.h");

        fn new_attribute(
            name: &CxxString,
            datatype: Datatype,
            nullable: bool,
        ) -> UniquePtr<Attribute>;

        fn new_dimension(
            name: &CxxString,
            datatype: Datatype,
            memory_tracker: SharedPtr<MemoryTracker>,
        ) -> UniquePtr<Dimension>;

        fn new_domain(memory_tracker: SharedPtr<MemoryTracker>) -> UniquePtr<Domain>;

        fn new_array_schema(
            array_type: ArrayType,
            memory_tracker: SharedPtr<MemoryTracker>,
        ) -> UniquePtr<ArraySchema>;

        fn attribute_to_shared(attr: UniquePtr<Attribute>) -> SharedPtr<Attribute>;
        fn dimension_to_shared(dimension: UniquePtr<Dimension>) -> SharedPtr<Dimension>;
        fn domain_to_shared(domain: UniquePtr<Domain>) -> SharedPtr<Domain>;
        fn array_schema_to_shared(array_schema: UniquePtr<ArraySchema>) -> SharedPtr<ArraySchema>;

        fn as_const_attribute(attr: SharedPtr<Attribute>) -> SharedPtr<ConstAttribute>;
    }

    #[namespace = "tiledb::test::oxidize"]
    unsafe extern "C++" {
        include!("tiledb/oxidize/test-support/cc/query.h");

        fn new_ast_value_node(
            field: &CxxString,
            op: QueryConditionOp,
            value: &[u8],
        ) -> SharedPtr<ASTNode>;

        fn new_ast_value_node_null(field: &CxxString, op: QueryConditionOp) -> SharedPtr<ASTNode>;

        fn new_ast_value_node_var(
            field: &CxxString,
            op: QueryConditionOp,
            value: &[u8],
            offsets: &[u64],
        ) -> SharedPtr<ASTNode>;

        fn new_ast_combination(
            left: SharedPtr<ASTNode>,
            right: SharedPtr<ASTNode>,
            op: QueryConditionCombinationOp,
        ) -> SharedPtr<ASTNode>;

        fn new_ast_negate(arg: SharedPtr<ASTNode>) -> SharedPtr<ASTNode>;
    }

    #[namespace = "tiledb::test::oxidize"]
    unsafe extern "C++" {
        include!("tiledb/oxidize/test-support/cc/result_tile.h");

        fn new_result_tile(
            cell_num: u64,
            array_schema: &ArraySchema,
            memory_tracker: SharedPtr<MemoryTracker>,
        ) -> SharedPtr<ResultTile>;

        fn init_coord_tile(
            tile: SharedPtr<ResultTile>,
            schema: &ArraySchema,
            field: &CxxString,
            data: &[u8],
            offsets: &[u64],
            dim_idx: u32,
        );

        unsafe fn init_attr_tile(
            tile: SharedPtr<ResultTile>,
            schema: &ArraySchema,
            field: &CxxString,
            data: &[u8],
            offsets: &[u64],
            validity: *const u8,
        );
    }
}

pub use ffi::get_test_memory_tracker;

pub mod array_schema {
    pub use crate::ffi::{
        array_schema_to_shared, as_const_attribute, attribute_to_shared, dimension_to_shared,
        domain_to_shared, new_array_schema, new_attribute, new_dimension, new_domain,
    };
}

pub mod query {
    pub use crate::ffi::{
        new_ast_combination, new_ast_negate, new_ast_value_node, new_ast_value_node_null,
        new_ast_value_node_var,
    };
}

pub mod result_tile {
    pub use crate::ffi::{init_attr_tile, init_coord_tile, new_result_tile};
}
