fn main() {
    /*
    if false {
        // for tests we have to link the test objects
        if let Ok(target_dir) = std::env::var("CARGO_TARGET_DIR") {
            println!("cargo::warning=TARGET_DIR={}", target_dir);
            println!("cargo::rustc-link-search=native={}", target_dir);
        } else {
            // need to build the thing and add it to search path - extra credit
            todo!()
        }
        let crate_name = std::env::var("CARGO_PKG_NAME").expect("CARGO_PKG_NAME is not set");
        println!("cargo::warning=dylib={}", crate_name);
        println!(
            "cargo::rustc-link-lib=static:+whole-archive={}_sys",
            crate_name
        );
    }
    */

    // does not work for `cargo test`, this is for integration test targets
    //println!("cargo::rustc-link-arg-tests=-l");
    //println!("cargo::rustc-link-arg-tests=dylib={}_sys", crate_name);

    let _bridge = cxx_build::bridge("src/lib.rs");
    println!("cargo:rerun-if-changed=src/lib.rs");
}
