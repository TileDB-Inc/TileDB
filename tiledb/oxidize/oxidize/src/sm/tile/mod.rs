#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/tile/tile.h");

        type Tile;

        fn size(&self) -> u64;
        fn cell_size(&self) -> u64;

        #[cxx_name = "data_u8"]
        fn data(&self) -> *const u8;
    }
}

pub use ffi::Tile;

impl Tile {
    pub fn as_slice<'a>(&self) -> &'a [u8] {
        unsafe {
            // SAFETY: `self.data()` is NULL if and only if `self.size() == 0`
            crate::raw_as_slice::<'a>(self.data(), self.size() as usize)
        }
    }
}
