#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        include!("tiledb/oxidize/oxidize/cc/array_schema.h");
        include!("tiledb/sm/enums/datatype.h");
        include!("tiledb/sm/enums/array_type.h");
        include!("tiledb/common/memory_tracker.h");
        include!("tiledb/sm/array_schema/attribute.h");
        include!("tiledb/sm/array_schema/dimension.h");
        include!("tiledb/sm/array_schema/domain.h");
        include!("tiledb/sm/array_schema/array_schema.h");

        type ArrayType = tiledb_oxidize::sm::enums::ArrayType;
        type Datatype = tiledb_oxidize::sm::enums::Datatype;
        type MemoryTracker = tiledb_oxidize::common::memory_tracker::MemoryTracker;

        type Attribute = tiledb_oxidize::sm::array_schema::Attribute;
        type ConstAttribute = tiledb_oxidize::sm::array_schema::ConstAttribute;
        type Dimension = tiledb_oxidize::sm::array_schema::Dimension;
        type Domain = tiledb_oxidize::sm::array_schema::Domain;
        type ArraySchema = tiledb_oxidize::sm::array_schema::ArraySchema;
    }

    #[namespace = "tiledb::test"]
    unsafe extern "C++" {
        include!("test/support/src/mem_helpers.h");

        fn get_test_memory_tracker() -> SharedPtr<MemoryTracker>;
    }

    #[namespace = "tiledb::oxidize"]
    unsafe extern "C++" {
        include!("tiledb/oxidize/test-support/cc/oxidize.h");

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
}

pub use ffi::{
    array_schema_to_shared, as_const_attribute, attribute_to_shared, dimension_to_shared,
    domain_to_shared, get_test_memory_tracker, new_array_schema, new_attribute, new_dimension,
    new_domain,
};
