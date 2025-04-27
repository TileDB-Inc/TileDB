use proptest::prelude::*;

#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::oxidized::test"]
    extern "Rust" {
        fn run_filter_pipeline_rs() -> bool;
        fn run_proptest_65154() -> bool;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/filter/filter_pipeline.h");
        include!("tiledb/oxidized/cpp/filter/filter_pipeline.h");
        type FilterPipeline;
    }

    #[namespace = "tiledb::sm::test"]
    unsafe extern "C++" {
        fn build_pipeline_65154() -> UniquePtr<FilterPipeline>;
        fn filter_pipeline_roundtrip(
            pipeline: &FilterPipeline,
            data: &[u8],
        ) -> Result<()>;
    }
}

pub fn run_filter_pipeline_rs() -> bool {
    eprintln!("Yay Rust!");
    true
}

pub fn run_proptest_65154() -> bool {
    proptest!(|(data in proptest::collection::vec(any::<u32>(), 0..=1024))| {
        run_test(&data).expect("Error testing property.")
    });

    true
}

fn run_test(data: &[u32]) -> anyhow::Result<()> {
    let pipeline = ffi::build_pipeline_65154();
    let as_bytes = unsafe { std::mem::transmute::<&[u32], &[u8]>(data) };
    Ok(ffi::filter_pipeline_roundtrip(&pipeline, as_bytes)?)
}
