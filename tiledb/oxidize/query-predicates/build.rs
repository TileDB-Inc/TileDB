fn main() {
    let _bridge = cxx_build::bridge("src/lib.rs");
    println!("cargo:rerun-if-changed=src/lib.rs");
}
