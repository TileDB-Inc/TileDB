#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        type Tile = crate::sm::tile::Tile;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/query/readers/result_tile.h");

        type TileTuple;
        fn has_var_tile(&self) -> bool;
        fn has_validity_tile(&self) -> bool;
        fn fixed_tile(&self) -> &Tile;

        /// Returns a reference to the variable data [Tile].
        ///
        /// # Safety
        ///
        /// Caller must ensure that `self.has_var_tile()` returns `true`.
        #[cxx_name = "var_tile"]
        #[allow(clippy::missing_safety_doc)] // false positive
        unsafe fn var_tile_unchecked(&self) -> &Tile;

        /// Returns a reference to the validity [Tile].
        ///
        /// # Safety
        ///
        /// Caller must ensure that `self.has_validity_tile()` returns `true`.
        #[cxx_name = "validity_tile"]
        #[allow(clippy::missing_safety_doc)] // false positive
        unsafe fn validity_tile_unchecked(&self) -> &Tile;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/query/readers/result_tile.h");

        type ResultTile;
        fn cell_num(&self) -> u64;
        fn tile_tuple(&self, s: &CxxString) -> *const TileTuple;
    }

    impl SharedPtr<ResultTile> {}
}

pub use ffi::{ResultTile, TileTuple};

use crate::sm::tile::Tile;

impl TileTuple {
    pub fn validity_tile(&self) -> Option<&Tile> {
        if self.has_validity_tile() {
            Some(unsafe {
                // SAFETY: we checked
                self.validity_tile_unchecked()
            })
        } else {
            None
        }
    }

    pub fn var_tile(&self) -> Option<&Tile> {
        if self.has_var_tile() {
            Some(unsafe {
                // SAFETY: we checked
                self.var_tile_unchecked()
            })
        } else {
            None
        }
    }
}
