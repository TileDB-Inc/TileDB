#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/tile/tile.h");

        type Tile;

        fn size(&self) -> u64;
        fn cell_size(&self) -> u64;

        #[cxx_name = "data_u8"]
        fn data(&self) -> *mut u8;
    }
}

pub use ffi::Tile;

impl Tile {
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: depends on safety of the underlying C++ structure
        unsafe { std::slice::from_raw_parts(self.data(), self.size() as usize) }
    }
}
