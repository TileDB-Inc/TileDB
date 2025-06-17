#[cxx::bridge]
pub mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        include!("tiledb/sm/array_schema/array_schema.h");
        include!("tiledb/sm/query/readers/result_tile.h");

        type ArraySchema = tiledb_cxx_interface::sm::array_schema::ArraySchema;
        type ResultTile = tiledb_cxx_interface::sm::query::readers::ResultTile;
    }

    #[namespace = "tiledb::oxidize::arrow::schema"]
    extern "Rust" {
        type ArrowSchema;

        #[cxx_name = "create"]
        fn array_schema_to_arrow_schema(
            schema: &ArraySchema,
            select: &CxxVector<CxxString>,
        ) -> Result<Box<ArrowSchema>>;
    }

    #[namespace = "tiledb::oxidize::arrow::record_batch"]
    extern "Rust" {
        type ArrowRecordBatch;

        #[cxx_name = "create"]
        fn result_tile_to_record_batch(
            schema: &ArrowSchema,
            tile: &ResultTile,
        ) -> Result<Box<ArrowRecordBatch>>;
    }
}

pub mod offsets;
pub mod record_batch;
pub mod schema;

use record_batch::{ArrowRecordBatch, to_record_batch as result_tile_to_record_batch};
use schema::{ArrowSchema, cxx::to_arrow as array_schema_to_arrow_schema};

unsafe impl cxx::ExternType for ArrowRecordBatch {
    type Id = cxx::type_id!("tiledb::oxidize::arrow::record_batch::ArrowRecordBatch");
    type Kind = cxx::kind::Opaque;
}

unsafe impl cxx::ExternType for ArrowSchema {
    type Id = cxx::type_id!("tiledb::oxidize::arrow::schema::ArrowSchema");
    type Kind = cxx::kind::Opaque;
}
