#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/misc/types.h");

        type ByteVecValue;
        fn size(&self) -> usize;
        fn data(&self) -> *const u8;
    }
}

pub use ffi::ByteVecValue;

impl ByteVecValue {
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: depends on safety of the underlying C++ structure
        unsafe { std::slice::from_raw_parts(self.data(), self.size()) }
    }
}
