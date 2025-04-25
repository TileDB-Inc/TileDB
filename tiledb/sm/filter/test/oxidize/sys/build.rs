use std::path::PathBuf;

fn main() {
    if false {
        for (k, v) in std::env::vars() {
            println!("cargo::warning={k}: {v}");
        }
    }

    if false {
        if let Ok(search_paths) = std::env::var("CRATE_SEARCH_PATH") {
            for search in search_paths.split(";") {
                println!("cargo::warning=SEARCH: {}", search);
                println!("cargo::rustc-link-search=all={}", search);
            }
        }

        if let Ok(archives) = std::env::var("CRATE_SHARED_OBJECTS") {
            for archive in archives.split(";") {
                println!("cargo::warning=ARCHIVE: {}", archive);
                println!("cargo::rustc-link-lib=dylib={}", archive);
            }
        }
    }

    if let Ok(target_dir) = std::env::var("CARGO_TARGET_DIR") {
        println!("cargo::warning=TARGET_DIR={}", target_dir);
        println!("cargo::rustc-link-search=native={}", target_dir);
    }
    let crate_name = std::env::var("CARGO_PKG_NAME").expect("CARGO_PKG_NAME is not set");
    println!("cargo::warning=ARCHIVE={}", crate_name);
    println!("cargo::rustc-link-lib=dylib={}", crate_name);

    /*
    if let Ok(objects) = std::env::var("CARGO_FFI_LINK_OBJECTS") {
        //println!("cargo::warning={objects}");
        for obj in objects.split(":") {
            //println!("cargo::warning={obj}");
            println!("cargo::rustc-link-arg={}", obj);
        }
    }
    */

    /*
    println!("cargo::warning=HELLO WORLD ENV VARS");
    for (key, value) in std::env::vars() {
        if key.starts_with("CARGO") {
            println!("cargo::warning={key}: {value}")
        }
    }
    */
}
