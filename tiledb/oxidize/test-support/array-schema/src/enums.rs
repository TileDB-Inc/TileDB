//! Provides methods for converting enum variants from `tiledb-rs`
//! to their corresponding internal enum variants.

use tiledb_common::array::{
    ArrayType as RsArrayType, CellOrder, CellValNum as RsCellValNum, TileOrder,
};
use tiledb_common::datatype::Datatype as RsDatatype;
use tiledb_cxx_interface::sm::array_schema::CellValNum as OxCellValNum;
use tiledb_cxx_interface::sm::enums::{ArrayType as OxArrayType, Datatype as OxDatatype, Layout};

/// Returns the internal [ArrayType] variant corresponding to a `tiledb-rs` [ArrayType].
pub fn convert_array_type(array_type: RsArrayType) -> OxArrayType {
    match array_type {
        RsArrayType::Dense => OxArrayType::DENSE,
        RsArrayType::Sparse => OxArrayType::SPARSE,
    }
}

/// Returns the internal [Layout] variant corresponding to a `tiledb-rs` [TileOrder].
pub fn convert_tile_order(tile_order: TileOrder) -> Layout {
    match tile_order {
        TileOrder::RowMajor => Layout::ROW_MAJOR,
        TileOrder::ColumnMajor => Layout::COL_MAJOR,
    }
}

/// Returns the internal [Layout] variant corresponding to a `tiledb-rs` [CellOrder].
pub fn convert_cell_order(cell_order: CellOrder) -> Layout {
    match cell_order {
        CellOrder::RowMajor => Layout::ROW_MAJOR,
        CellOrder::ColumnMajor => Layout::COL_MAJOR,
        CellOrder::Unordered => Layout::UNORDERED,
        CellOrder::Global => Layout::GLOBAL_ORDER,
        CellOrder::Hilbert => Layout::HILBERT,
    }
}

/// Returns the internal [Datatype] variant corresponding to a `tiledb-rs` [Datatype].
pub fn convert_datatype(datatype: RsDatatype) -> OxDatatype {
    type Rs = RsDatatype;
    type Ox = OxDatatype;
    match datatype {
        Rs::UInt8 => Ox::UINT8,
        Rs::UInt16 => Ox::UINT16,
        Rs::UInt32 => Ox::UINT32,
        Rs::UInt64 => Ox::UINT64,
        Rs::Int8 => Ox::INT8,
        Rs::Int16 => Ox::INT16,
        Rs::Int32 => Ox::INT32,
        Rs::Int64 => Ox::INT64,
        Rs::Float32 => Ox::FLOAT32,
        Rs::Float64 => Ox::FLOAT64,
        Rs::Char => Ox::CHAR,
        Rs::StringAscii => Ox::STRING_ASCII,
        Rs::StringUtf8 => Ox::STRING_UTF8,
        Rs::StringUtf16 => Ox::STRING_UTF16,
        Rs::StringUtf32 => Ox::STRING_UTF32,
        Rs::StringUcs2 => Ox::STRING_UCS2,
        Rs::StringUcs4 => Ox::STRING_UCS4,
        Rs::Any => Ox::ANY,
        Rs::DateTimeYear => Ox::DATETIME_YEAR,
        Rs::DateTimeMonth => Ox::DATETIME_MONTH,
        Rs::DateTimeWeek => Ox::DATETIME_WEEK,
        Rs::DateTimeDay => Ox::DATETIME_DAY,
        Rs::DateTimeHour => Ox::DATETIME_HR,
        Rs::DateTimeMinute => Ox::DATETIME_MIN,
        Rs::DateTimeSecond => Ox::DATETIME_SEC,
        Rs::DateTimeMillisecond => Ox::DATETIME_MS,
        Rs::DateTimeMicrosecond => Ox::DATETIME_US,
        Rs::DateTimeNanosecond => Ox::DATETIME_NS,
        Rs::DateTimePicosecond => Ox::DATETIME_PS,
        Rs::DateTimeFemtosecond => Ox::DATETIME_FS,
        Rs::DateTimeAttosecond => Ox::DATETIME_AS,
        Rs::TimeHour => Ox::TIME_HR,
        Rs::TimeMinute => Ox::TIME_MIN,
        Rs::TimeSecond => Ox::TIME_SEC,
        Rs::TimeMillisecond => Ox::TIME_MS,
        Rs::TimeMicrosecond => Ox::TIME_US,
        Rs::TimeNanosecond => Ox::TIME_NS,
        Rs::TimePicosecond => Ox::TIME_PS,
        Rs::TimeFemtosecond => Ox::TIME_FS,
        Rs::TimeAttosecond => Ox::TIME_AS,
        Rs::Blob => Ox::BLOB,
        Rs::Boolean => Ox::BOOL,
        Rs::GeometryWkb => Ox::GEOM_WKB,
        Rs::GeometryWkt => Ox::GEOM_WKT,
    }
}

/// Returns the internal [CellValNum] variant corresponding to a `tiledb-rs` [CellValNum].
pub fn convert_cell_val_num(cvn: RsCellValNum) -> OxCellValNum {
    match cvn {
        RsCellValNum::Fixed(nz) if nz.get() == 1 => OxCellValNum::Single,
        RsCellValNum::Fixed(nz) => OxCellValNum::Fixed(nz),
        RsCellValNum::Var => OxCellValNum::Var,
    }
}
