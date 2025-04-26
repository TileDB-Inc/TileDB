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
        type FilterPipeline;
    }

    #[namespace = "tiledb::sm::test"]
    unsafe extern "C++" {
        include!("tiledb/sm/filter/test/unit_filter_pipeline_rs.h");

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
    proptest!(|(data in proptest::collection::vec(any::<i32>(), 0..=1024*1024))| {
        run_test(&data).expect("Error testing property.")
    });

    true
}

fn run_test(data: &[i32]) -> anyhow::Result<()> {
    let pipeline = ffi::build_pipeline_65154();
    let as_bytes = unsafe { std::mem::transmute::<&[i32], &[u8]>(data) };
    Ok(ffi::filter_pipeline_roundtrip(&pipeline, as_bytes)?)
}
