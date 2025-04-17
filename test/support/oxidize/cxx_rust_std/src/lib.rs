//! Exports capabilities from the Rust standard library
//! for use in C++ code.

#[cxx::bridge]
mod ffi {
    #[namespace = "rust::std"]
    extern "Rust" {
        fn u64_checked_sub(a: u64, b: u64, out: &mut i64) -> bool;
        fn i64_checked_sub(a: i64, b: i64, out: &mut i64) -> bool;
    }
}

fn u64_checked_sub(a: u64, b: u64, out: &mut i64) -> bool {
    let a = a as i128;
    let b = b as i128;
    if let Some(Ok(result)) = a.checked_sub(b).map(|out| i64::try_from(out)) {
        *out = result;
        true
    } else {
        false
    }
}

fn i64_checked_sub(a: i64, b: i64, out: &mut i64) -> bool {
    let a = a as i128;
    let b = b as i128;
    if let Some(Ok(result)) = a.checked_sub(b).map(|out| i64::try_from(out)) {
        *out = result;
        true
    } else {
        false
    }
}
