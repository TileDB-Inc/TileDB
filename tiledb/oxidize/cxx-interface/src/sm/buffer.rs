#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/buffer/buffer.h");
        type Buffer;

        fn size(&self) -> u64;
        fn offset(&self) -> u64;

        #[cxx_name = "bytes"]
        fn as_ptr(&self) -> *const u8;
    }
}

pub use ffi::Buffer;

impl Buffer {
    pub fn as_slice(&self) -> &[u8] {
        let ptr = self.as_ptr();
        let ptr = if ptr.is_null() {
            assert_eq!(0, self.size());
            std::ptr::NonNull::<u8>::dangling().as_ptr()
        } else {
            ptr
        };
        unsafe { std::slice::from_raw_parts(ptr, self.size() as usize) }
    }
}
