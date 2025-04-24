fn main() {
    let _bridge = cxx_build::bridge("src/lib.rs");
    println!("cargo:rerun-if-changed=src/lib.rs");

    let search_path = std::env::var("CARGO_TARGET_DIR").unwrap();
    println!("cargo::rustc-link-search={search_path}");
    println!("cargo::rustc-link-lib=dylib=filter_pipeline_objlib");

    println!("cargo::rustc-link-search={search_path}/debug");
    println!("cargo::rustc-link-lib=dylib=filter_pipeline_objlib");
}
