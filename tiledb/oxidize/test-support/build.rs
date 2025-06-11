fn main() {
    let bridge_sources = vec!["src/lib.rs"];
    let _bridges = cxx_build::bridges(bridge_sources.clone());

    for bridge in bridge_sources {
        println!("cargo:rerun-if-changed={bridge}");
    }
}
