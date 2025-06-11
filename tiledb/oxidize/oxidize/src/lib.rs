//! This module provides bridges between C++ and Rust,
//! and utility `impl`s on Rust type defined in those bridges.
//!
//! Each `mod.rs` contains a `#[cxx::bridge]`.
//! The module structure reflects the structure of the parent
//! `tiledb` directory.

pub mod common;
pub mod sm;

/// Returns a safe slice over the raw data.
pub(self) fn raw_as_slice<'a, T>(ptr: *const T, len: usize) -> &'a [T] {
    if ptr.is_null() {
        assert_eq!(0, len);
        // SAFETY: as long as the above assert holds
        unsafe { std::slice::from_raw_parts(std::ptr::NonNull::<T>::dangling().as_ptr(), 0) }
    } else {
        // SAFETY: depends on safety of the underlying C++ structure
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}
