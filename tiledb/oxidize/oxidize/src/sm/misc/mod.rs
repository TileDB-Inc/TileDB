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
    pub fn as_slice<'a>(&'a self) -> &'a [u8] {
        crate::raw_as_slice::<'a, u8>(self.data(), self.size())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.size()
    }
}
