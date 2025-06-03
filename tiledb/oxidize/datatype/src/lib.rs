#[macro_export]
macro_rules! apply_physical_type {
    ($datatype:expr, $typename:ident, $action:expr, $invalid:expr) => {{
        use tiledb_oxidize::sm::enums::Datatype;

        match $datatype {
            Datatype::INT8 => {
                type $typename = i8;
                $action
            }
            Datatype::INT16 => {
                type $typename = i16;
                $action
            }
            Datatype::INT32 => {
                type $typename = i32;
                $action
            }
            Datatype::INT64
            | Datatype::DATETIME_YEAR
            | Datatype::DATETIME_MONTH
            | Datatype::DATETIME_WEEK
            | Datatype::DATETIME_DAY
            | Datatype::DATETIME_HR
            | Datatype::DATETIME_MIN
            | Datatype::DATETIME_SEC
            | Datatype::DATETIME_MS
            | Datatype::DATETIME_US
            | Datatype::DATETIME_NS
            | Datatype::DATETIME_PS
            | Datatype::DATETIME_FS
            | Datatype::DATETIME_AS
            | Datatype::TIME_HR
            | Datatype::TIME_MIN
            | Datatype::TIME_SEC
            | Datatype::TIME_MS
            | Datatype::TIME_US
            | Datatype::TIME_NS
            | Datatype::TIME_PS
            | Datatype::TIME_FS
            | Datatype::TIME_AS => {
                type $typename = i64;
                $action
            }
            Datatype::UINT8
            | Datatype::STRING_ASCII
            | Datatype::STRING_UTF8
            | Datatype::ANY
            | Datatype::BLOB
            | Datatype::BOOL
            | Datatype::GEOM_WKB
            | Datatype::GEOM_WKT => {
                type $typename = u8;
                $action
            }
            Datatype::UINT16 | Datatype::STRING_UTF16 | Datatype::STRING_UCS2 => {
                type $typename = u16;
                $action
            }
            Datatype::UINT32 | Datatype::STRING_UTF32 | Datatype::STRING_UCS4 => {
                type $typename = u32;
                $action
            }
            Datatype::UINT64 => {
                type $typename = u64;
                $action
            }
            Datatype::FLOAT32 => {
                type $typename = f32;
                $action
            }
            Datatype::FLOAT64 => {
                type $typename = f64;
                $action
            }
            Datatype::CHAR => {
                type $typename = std::ffi::c_char;
                $action
            }
            invalid => $invalid(invalid),
        }
    }};
}
