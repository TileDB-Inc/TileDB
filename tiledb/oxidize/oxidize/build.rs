fn main() {
    let bridge_sources = vec![
        "src/sm/array_schema/mod.rs",
        "src/sm/enums/mod.rs",
        "src/sm/query/ast/mod.rs",
    ];
    let _bridges = cxx_build::bridges(bridge_sources.clone());

    for bridge in bridge_sources {
        println!("cargo:rerun-if-changed={bridge}");
    }
}
