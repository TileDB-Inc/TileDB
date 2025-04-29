//! Exports capabilities from the Rust standard library
//! for use in C++ code.

#[cxx::bridge]
mod ffi {
    #[namespace = "rust::arithmetic"]
    extern "Rust" {
        fn u32_checked_add(a: u32, b: u32, out: &mut u32) -> bool;
        fn i32_checked_add(a: i32, b: i32, out: &mut i32) -> bool;
        fn u64_checked_add(a: u64, b: u64, out: &mut u64) -> bool;
        fn i64_checked_add(a: i64, b: i64, out: &mut i64) -> bool;

        fn u32_checked_sub(a: u32, b: u32, out: &mut u32) -> bool;
        fn i32_checked_sub(a: i32, b: i32, out: &mut i32) -> bool;
        fn u64_checked_sub(a: u64, b: u64, out: &mut u64) -> bool;
        fn i64_checked_sub(a: i64, b: i64, out: &mut i64) -> bool;

        fn u64_checked_sub_signed(a: u64, b: u64, out: &mut i64) -> bool;
    }
}

use num_traits::{CheckedAdd, CheckedSub};

fn do_checked_add<T>(a: T, b: T, out: &mut T) -> bool
where
    T: CheckedAdd,
{
    if let Some(result) = a.checked_add(&b) {
        *out = result;
        true
    } else {
        false
    }
}

fn do_checked_sub<T>(a: T, b: T, out: &mut T) -> bool
where
    T: CheckedSub,
{
    if let Some(result) = a.checked_sub(&b) {
        *out = result;
        true
    } else {
        false
    }
}

fn u32_checked_add(a: u32, b: u32, out: &mut u32) -> bool {
    do_checked_add::<u32>(a, b, out)
}

fn i32_checked_add(a: i32, b: i32, out: &mut i32) -> bool {
    do_checked_add::<i32>(a, b, out)
}

fn u64_checked_add(a: u64, b: u64, out: &mut u64) -> bool {
    do_checked_add::<u64>(a, b, out)
}

fn i64_checked_add(a: i64, b: i64, out: &mut i64) -> bool {
    do_checked_add::<i64>(a, b, out)
}

fn u32_checked_sub(a: u32, b: u32, out: &mut u32) -> bool {
    do_checked_sub::<u32>(a, b, out)
}

fn i32_checked_sub(a: i32, b: i32, out: &mut i32) -> bool {
    do_checked_sub::<i32>(a, b, out)
}

fn u64_checked_sub(a: u64, b: u64, out: &mut u64) -> bool {
    do_checked_sub::<u64>(a, b, out)
}

fn i64_checked_sub(a: i64, b: i64, out: &mut i64) -> bool {
    do_checked_sub::<i64>(a, b, out)
}

fn u64_checked_sub_signed(a: u64, b: u64, out: &mut i64) -> bool {
    let a = a as i128;
    let b = b as i128;
    if let Some(Ok(result)) = a.checked_sub(b).map(|out| i64::try_from(out)) {
        *out = result;
        true
    } else {
        false
    }
}
