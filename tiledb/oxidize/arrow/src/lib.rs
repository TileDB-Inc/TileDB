#[cxx::bridge]
pub mod ffi {
    /// Indicates how an [ArraySchema] should be translated into an Arrow [Schema].
    ///
    /// See `schema` module documentation.
    #[namespace = "tiledb::oxidize::arrow::schema"]
    enum WhichSchema {
        Storage,
        View,
    }
}

pub mod enumeration;
pub mod offsets;
pub mod record_batch;
pub mod schema;
