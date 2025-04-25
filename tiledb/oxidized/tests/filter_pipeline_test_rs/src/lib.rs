#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::oxidized"]
    extern "Rust" {
        fn run_filter_pipeline_rs() -> bool;
    }
}

pub fn run_filter_pipeline_rs() -> bool {
    eprintln!("Yay Rust!");
    true
}
