#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    enum Datatype {
        INT32,
        INT64,
        FLOAT32,
        FLOAT64,
        CHAR,
        INT8,
        UINT8,
        INT16,
        UINT16,
        UINT32,
        UINT64,
        STRING_ASCII,
        STRING_UTF8,
        STRING_UTF16,
        STRING_UTF32,
        STRING_UCS2,
        STRING_UCS4,
        ANY,
        DATETIME_YEAR,
        DATETIME_MONTH,
        DATETIME_WEEK,
        DATETIME_DAY,
        DATETIME_HR,
        DATETIME_MIN,
        DATETIME_SEC,
        DATETIME_MS,
        DATETIME_US,
        DATETIME_NS,
        DATETIME_PS,
        DATETIME_FS,
        DATETIME_AS,
        TIME_HR,
        TIME_MIN,
        TIME_SEC,
        TIME_MS,
        TIME_US,
        TIME_NS,
        TIME_PS,
        TIME_FS,
        TIME_AS,
        BLOB,
        BOOL,
        GEOM_WKB,
        GEOM_WKT,
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/enums/datatype.h");
        type Datatype;

        fn datatype_size(datatype: Datatype) -> u64;
        fn datatype_is_valid(datatype: Datatype) -> bool;
    }

    #[namespace = "tiledb::sm"]
    enum QueryConditionOp {
        LT,
        LE,
        GT,
        GE,
        EQ,
        NE,
        IN,
        NOT_IN,
        ALWAYS_TRUE = 253,
        ALWAYS_FALSE = 254,
    }

    #[namespace = "tiledb::sm"]
    enum QueryConditionCombinationOp {
        AND,
        OR,
        NOT,
    }
}

pub use ffi::{Datatype, QueryConditionCombinationOp, QueryConditionOp};

impl Datatype {
    pub fn is_valid(&self) -> bool {
        ffi::datatype_is_valid(*self)
    }

    pub fn value_size(&self) -> usize {
        ffi::datatype_size(*self) as usize
    }
}
