fn main() {
    let bridge_sources = vec![
        "src/sm/array_schema/mod.rs",
        "src/sm/enums/mod.rs",
        "src/sm/misc/mod.rs",
        "src/sm/query/ast/mod.rs",
        "src/sm/query/readers/mod.rs",
        "src/sm/tile/mod.rs",
    ];
    let _bridges = cxx_build::bridges(bridge_sources.clone());

    for bridge in bridge_sources {
        println!("cargo:rerun-if-changed={bridge}");
    }

    let dir = std::env::var("CARGO_TARGET_DIR").unwrap();
    let mut pb = std::path::PathBuf::from(dir);
    pb.push("build_rs.cc");
    std::fs::write(pb, &[]).unwrap();
}
