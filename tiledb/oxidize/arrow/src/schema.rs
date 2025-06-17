use std::ops::Deref;
use std::str::Utf8Error;
use std::sync::Arc;

use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, Schema,
};
use tiledb_cxx_interface::sm::array_schema::{ArraySchema, CellValNum, Field};
use tiledb_cxx_interface::sm::enums::Datatype;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Field name is not UTF-8")]
    NameNotUtf8(Vec<u8>, Utf8Error),
    #[error("Error in field '{0}': {1}")]
    FieldError(String, FieldError),
}

#[derive(Debug, thiserror::Error)]
pub enum FieldError {
    #[error("Invalid cell val num: {0}")]
    InvalidCellValNum(CellValNum),
    #[error("Internal error: invalid discriminant for data type: {0}")]
    InternalInvalidDatatype(u8),
}

pub struct ArrowSchema(pub Arc<Schema>);

impl Deref for ArrowSchema {
    type Target = Arc<Schema>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub mod cxx {
    use super::*;

    pub fn to_arrow(
        array_schema: &ArraySchema,
        select: &::cxx::Vector<::cxx::String>,
    ) -> Result<Box<ArrowSchema>, Error> {
        Ok(Box::new(ArrowSchema(Arc::new(super::to_arrow(
            array_schema,
            |field: &Field| select.iter().any(|s| s == field.name_cxx()),
        )?))))
    }
}

/// Returns a [Schema] which represents the physical field types of `array_schema`.
pub fn to_arrow<F>(array_schema: &ArraySchema, select: F) -> Result<Schema, Error>
where
    F: Fn(&Field) -> bool,
{
    let fields = array_schema.fields().filter(select).map(|f| {
        let field_name = f
            .name()
            .map_err(|e| Error::NameNotUtf8(f.name_cxx().as_bytes().to_vec(), e))?;
        let arrow_type =
            field_arrow_datatype(&f).map_err(|e| Error::FieldError(field_name.to_owned(), e))?;

        // NB: fields can always be null due to schema evolution
        Ok(ArrowField::new(field_name, arrow_type, true))
    });

    Ok(Schema {
        fields: fields.collect::<Result<ArrowFields, _>>()?,
        metadata: Default::default(),
    })
}

/// Returns an [ArrowDataType] which represents the physical data type of `field`.
pub fn field_arrow_datatype(field: &Field) -> Result<ArrowDataType, FieldError> {
    match field.cell_val_num() {
        CellValNum::Single => Ok(arrow_datatype(field.datatype())?),
        CellValNum::Fixed(nz) => {
            if let Ok(fixed_length) = i32::try_from(nz.get()) {
                let value_type = arrow_datatype(field.datatype())?;
                Ok(ArrowDataType::FixedSizeList(
                    Arc::new(ArrowField::new_list_field(value_type, false)),
                    fixed_length,
                ))
            } else {
                // cell val num greater than i32::MAX
                Err(FieldError::InvalidCellValNum(field.cell_val_num()))
            }
        }
        CellValNum::Var => {
            let value_type = arrow_datatype(field.datatype())?;
            Ok(ArrowDataType::LargeList(Arc::new(
                ArrowField::new_list_field(value_type, false),
            )))
        }
    }
}

/// Returns an [ArrowDataType] which represents the physical type of a single value of `datatype`.
pub fn arrow_datatype(datatype: Datatype) -> Result<ArrowDataType, FieldError> {
    Ok(match datatype {
        Datatype::INT8 => ArrowDataType::Int8,
        Datatype::INT16 => ArrowDataType::Int16,
        Datatype::INT32 => ArrowDataType::Int32,
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
        | Datatype::TIME_AS => ArrowDataType::Int64,
        Datatype::UINT8
        | Datatype::STRING_ASCII
        | Datatype::STRING_UTF8
        | Datatype::ANY
        | Datatype::BLOB
        | Datatype::BOOL
        | Datatype::GEOM_WKB
        | Datatype::GEOM_WKT => ArrowDataType::UInt8,
        Datatype::UINT16 | Datatype::STRING_UTF16 | Datatype::STRING_UCS2 => ArrowDataType::UInt16,
        Datatype::UINT32 | Datatype::STRING_UTF32 | Datatype::STRING_UCS4 => ArrowDataType::UInt32,
        Datatype::UINT64 => ArrowDataType::UInt64,
        Datatype::FLOAT32 => ArrowDataType::Float32,
        Datatype::FLOAT64 => ArrowDataType::Float64,
        Datatype::CHAR => {
            if std::any::TypeId::of::<std::ffi::c_char>() == std::any::TypeId::of::<i8>() {
                ArrowDataType::Int8
            } else {
                ArrowDataType::UInt8
            }
        }
        other => {
            assert!(!other.is_valid());
            return Err(FieldError::InternalInvalidDatatype(other.repr));
        }
    })
}
