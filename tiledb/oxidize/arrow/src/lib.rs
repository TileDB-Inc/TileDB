#[cxx::bridge]
pub mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        include!("tiledb/sm/array_schema/array_schema.h");
        include!("tiledb/sm/array_schema/enumeration.h");
        include!("tiledb/sm/query/readers/result_tile.h");

        type ArraySchema = tiledb_cxx_interface::sm::array_schema::ArraySchema;
        type Enumeration = tiledb_cxx_interface::sm::array_schema::Enumeration;
        type ResultTile = tiledb_cxx_interface::sm::query::readers::ResultTile;
    }

    #[namespace = "tiledb::oxidize::arrow::schema"]
    extern "Rust" {
        type ArrowArraySchema;

        #[cxx_name = "create"]
        fn array_schema_create_arrow_schema(schema: &ArraySchema) -> Result<Box<ArrowArraySchema>>;

        #[cxx_name = "project"]
        fn array_schema_project_arrow_schema(
            schema: &ArraySchema,
            select: &Vec<String>,
        ) -> Result<Box<ArrowArraySchema>>;
    }

    #[namespace = "tiledb::oxidize::arrow::array"]
    extern "Rust" {
        type ArrowArray;

        #[cxx_name = "from_enumeration"]
        unsafe fn array_from_enumeration_ffi(enumeration: &Enumeration) -> Result<Box<ArrowArray>>;
    }

    #[namespace = "tiledb::oxidize::arrow::record_batch"]
    extern "Rust" {
        type ArrowRecordBatch;

        #[cxx_name = "create"]
        unsafe fn result_tile_to_record_batch(
            schema: &ArrowArraySchema,
            tile: &ResultTile,
        ) -> Result<Box<ArrowRecordBatch>>;
    }
}

pub mod enumeration;
pub mod offsets;
pub mod record_batch;
pub mod schema;

use std::sync::Arc;

use enumeration::array_from_enumeration_ffi;
use record_batch::{ArrowRecordBatch, to_record_batch as result_tile_to_record_batch};
use schema::{
    ArrowArraySchema, cxx::project_arrow as array_schema_project_arrow_schema,
    cxx::to_arrow as array_schema_create_arrow_schema,
};

/// Wraps a [dyn ArrowArray] for passing across the FFI boundary.
pub struct ArrowArray(pub Arc<dyn arrow::array::Array>);

unsafe impl cxx::ExternType for ArrowRecordBatch {
    type Id = cxx::type_id!("tiledb::oxidize::arrow::record_batch::ArrowRecordBatch");
    type Kind = cxx::kind::Opaque;
}

unsafe impl cxx::ExternType for ArrowArraySchema {
    type Id = cxx::type_id!("tiledb::oxidize::arrow::schema::ArrowArraySchema");
    type Kind = cxx::kind::Opaque;
}
