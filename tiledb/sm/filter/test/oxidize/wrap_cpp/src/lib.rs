#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/filter/filter_pipeline.h");
        type FilterPipeline;
    }

    #[namespace = "tiledb::sm::test"]
    unsafe extern "C++" {
        include!("tiledb/sm/filter/test/oxidize.h");
        fn build_pipeline_65154() -> UniquePtr<FilterPipeline>;

        fn filter_pipeline_roundtrip(pipeline: &FilterPipeline, data: &[u8]) -> Result<()>;
    }
}

pub use ffi::*;

#[cfg(test)]
mod tests;
