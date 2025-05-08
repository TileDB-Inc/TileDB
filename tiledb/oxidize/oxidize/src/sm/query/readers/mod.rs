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

        #[cxx_name = "var_tile"]
        unsafe fn var_tile_unchecked(&self) -> &Tile;

        #[cxx_name = "validity_tile"]
        unsafe fn validity_tile_unchecked(&self) -> &Tile;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/query/readers/result_tile.h");

        type ResultTile;
        fn cell_num(&self) -> u64;
        fn tile_tuple(&self, s: &CxxString) -> *const TileTuple;
    }
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
