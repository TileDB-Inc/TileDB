fn main() {
    let cxx_root = std::path::PathBuf::from(std::env::var("CXX_ROOT").unwrap());

    let mut cxx_rust = cxx_root.clone();
    cxx_rust.push("rust");

    let mut wrap_cpp = cxx_root.clone();
    wrap_cpp.push("wrap_cpp");
    wrap_cpp.push("src");

    let include_dir = std::env::var("TILEDB_ROOT").unwrap();

    let _bridge = cxx_build::bridge("src/lib.rs")
        .include(cxx_rust)
        .include(wrap_cpp)
        .include(include_dir)
        .std("c++20")
        .compile("wrap_cpp");
    println!("cargo:rerun-if-changed=src/lib.rs");

    let search_path = std::env::var("CARGO_TARGET_DIR").unwrap();
    println!("cargo::rustc-link-search={search_path}");
    println!("cargo::rustc-link-search={search_path}/debug");
    println!("cargo::rustc-link-lib=dylib=filter_pipeline_objlib");
    println!("cargo::rustc-link-lib=wrap_rust");

    let parts = [
        "cargo::rustc-link-arg=",
        "-Wl,",
        "-rpath,@loader_path,",
        "-rpath,$ORIGIN,",
        "-rpath,",
        &search_path,
    ];

    println!("{}", parts.join(""));
}
