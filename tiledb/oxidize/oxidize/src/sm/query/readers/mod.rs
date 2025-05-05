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
        fn var_tile(&self) -> &Tile;
        fn validity_tile(&self) -> &Tile;
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
    pub fn validity(&self) -> Option<&Tile> {
        if self.has_validity_tile() {
            Some(self.validity_tile())
        } else {
            None
        }
    }

    pub fn offsets(&self) -> Option<&Tile> {
        if self.has_var_tile() {
            Some(self.var_tile())
        } else {
            None
        }
    }
}
