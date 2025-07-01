//! Provides definitions for mapping an [ArraySchema] or the contents of an
//! [ArraySchema] onto representative Arrow [Schema]ta or [Field]s.
use std::ops::Deref;
use std::str::Utf8Error;
use std::sync::Arc;

use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, Schema,
};
use tiledb_cxx_interface::sm::array_schema::{ArraySchema, CellValNum, Field};
use tiledb_cxx_interface::sm::enums::Datatype;

/// An error converting [ArraySchema] to [Schema].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Field name is not UTF-8")]
    NameNotUtf8(Vec<u8>, Utf8Error),
    #[error("Error in field '{0}': {1}")]
    FieldError(String, FieldError),
}

/// An error converting an [ArraySchema] [Field] to [ArrowField].
#[derive(Debug, thiserror::Error)]
pub enum FieldError {
    #[error("Invalid cell val num: {0}")]
    InvalidCellValNum(CellValNum),
    #[error("Internal error: invalid discriminant for data type: {0}")]
    InternalInvalidDatatype(u8),
    #[error("Internal error: enumeration not found: {0}")]
    InternalEnumerationNotFound(String),
}

/// Wraps a [Schema] for passing across the FFI boundary.
pub struct ArrowSchema(pub Arc<Schema>);

impl Deref for ArrowSchema {
    type Target = Arc<Schema>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub mod cxx {
    use super::*;

    pub fn to_arrow(array_schema: &ArraySchema) -> Result<Box<ArrowSchema>, Error> {
        Ok(Box::new(ArrowSchema(Arc::new(super::project_arrow(
            array_schema,
            |_: &Field| true,
        )?))))
    }

    /// Returns a [Schema] which represents the physical field types of
    /// the fields from `array_schema` which are contained in `select`.
    // NB: we use `Vec` for facilitating the FFI boundary
    #[allow(clippy::ptr_arg)]
    pub fn project_arrow(
        array_schema: &ArraySchema,
        select: &Vec<String>,
    ) -> Result<Box<ArrowSchema>, Error> {
        Ok(Box::new(ArrowSchema(Arc::new(super::project_arrow(
            array_schema,
            |field: &Field| select.iter().any(|s| s.as_str() == field.name_cxx()),
        )?))))
    }
}

pub fn to_arrow(array_schema: &ArraySchema) -> Result<Schema, Error> {
    project_arrow(array_schema, |_: &Field| true)
}

/// Returns a [Schema] which represents the physical field types of the selected fields from `array_schema`.
pub fn project_arrow<F>(array_schema: &ArraySchema, select: F) -> Result<Schema, Error>
where
    F: Fn(&Field) -> bool,
{
    let fields = array_schema.fields().filter(select).map(|f| {
        let field_name = f
            .name()
            .map_err(|e| Error::NameNotUtf8(f.name_cxx().as_bytes().to_vec(), e))?;
        let arrow_type = field_arrow_datatype(array_schema, &f)
            .map_err(|e| Error::FieldError(field_name.to_owned(), e))?;

        // NB: fields can always be null due to schema evolution
        Ok(ArrowField::new(field_name, arrow_type, true))
    });

    Ok(Schema {
        fields: fields.collect::<Result<ArrowFields, _>>()?,
        metadata: Default::default(),
    })
}

/// Returns an [ArrowDataType] which represents the physical data type of `field`.
pub fn field_arrow_datatype(
    array_schema: &ArraySchema,
    field: &Field,
) -> Result<ArrowDataType, FieldError> {
    if let Some(e_name) = field.enumeration_name_cxx() {
        if !array_schema.has_enumeration(e_name) {
            return Err(FieldError::InternalEnumerationNotFound(
                e_name.to_string_lossy().into_owned(),
            ));
        }

        let enumeration = array_schema.enumeration_cxx(e_name);

        let key_type = arrow_datatype(field.datatype(), field.cell_val_num())?;
        let value_type = if let Some(enumeration) = enumeration.as_ref() {
            arrow_datatype(enumeration.datatype(), enumeration.cell_val_num())?
        } else {
            // NB: we don't necessarily want to return an error here
            // because the enumeration might not actually be used
            // in a predicate. We can return some representation
            // which we will check later if it is actually used,
            // and return an error then.
            ArrowDataType::Null
        };
        Ok(ArrowDataType::Dictionary(
            Box::new(key_type),
            Box::new(value_type),
        ))
    } else {
        arrow_datatype(field.datatype(), field.cell_val_num())
    }
}

pub fn arrow_datatype(
    datatype: Datatype,
    cell_val_num: CellValNum,
) -> Result<ArrowDataType, FieldError> {
    match cell_val_num {
        CellValNum::Single => Ok(arrow_primitive_datatype(datatype)?),
        CellValNum::Fixed(nz) => {
            if let Ok(fixed_length) = i32::try_from(nz.get()) {
                let value_type = arrow_primitive_datatype(datatype)?;
                Ok(ArrowDataType::FixedSizeList(
                    Arc::new(ArrowField::new_list_field(value_type, false)),
                    fixed_length,
                ))
            } else {
                // cell val num greater than i32::MAX
                Err(FieldError::InvalidCellValNum(cell_val_num))
            }
        }
        CellValNum::Var => {
            if matches!(datatype, Datatype::STRING_ASCII | Datatype::STRING_UTF8) {
                Ok(ArrowDataType::LargeUtf8)
            } else {
                let value_type = arrow_primitive_datatype(datatype)?;
                Ok(ArrowDataType::LargeList(Arc::new(
                    ArrowField::new_list_field(value_type, false),
                )))
            }
        }
    }
}

/// Returns an [ArrowDataType] which represents the physical type of a single value of `datatype`.
pub fn arrow_primitive_datatype(datatype: Datatype) -> Result<ArrowDataType, FieldError> {
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
