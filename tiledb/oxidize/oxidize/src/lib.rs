//! This module provides bridges between C++ and Rust,
//! and utility `impl`s on Rust type defined in those bridges.
//!
//! Each `mod.rs` contains a `#[cxx::bridge]`.
//! The module structure reflects the structure of the parent
//! `tiledb` directory.

pub mod sm;
