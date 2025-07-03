//! Provides definitions for mapping an [ArraySchema] or the contents of an
//! [ArraySchema] onto representative Arrow [Schema]ta or [Field]s.
//!
//! For the most part, the mapping of fields from [ArraySchema] to Arrow [Schema]
//! is not complicated. In some cases there is not an exact datatype match.
//! We resolve this by using the _physical_ Arrow data type when possible.
//!
//! The greater complexity comes from enumerations. Arrow does have a Dictionary
//! type which seems appropriate; however, the Arrow `DictionaryArray` requires
//! that its keys are valid indices into the dictionary values. But TileDB
//! enumerations require the opposite: invalid key values are specifically
//! allowed, with the expectation that a user will attach additional variants
//! later.
//!
//! To address this we will separately consider schemata for the "array storage"
//! and the "array view".  The "array storage" schema contains the attribute key type
//! and the "array view" contains the enumeration value type. Like with SQL
//! views we will apply expression rewrites in order to translate between them.
//!
//! As a guideline, the "array storage" schema should be used internally and
//! the "array view" schema should be used for user endpoint APIs.
use std::collections::HashMap;
use std::str::Utf8Error;
use std::sync::Arc;

use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, Schema,
};
use itertools::Itertools;
use tiledb_cxx_interface::sm::array_schema::{ArraySchema, CellValNum, Field};
use tiledb_cxx_interface::sm::enums::Datatype;

pub use super::ffi::WhichSchema;

/// An error converting [ArraySchema] to [Schema].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Field name is not UTF-8")]
    NameNotUtf8(Vec<u8>, Utf8Error),
    #[error("Error in field '{0}': {1}")]
    FieldError(String, FieldError),
    #[error("Error in enumeration '{0}': {1}")]
    EnumerationError(String, crate::enumeration::Error),
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
    #[error("Enumeration name is not UTF-8")]
    EnumerationNameNotUtf8(Vec<u8>, Utf8Error),
}

pub type Enumerations = HashMap<String, Option<Arc<dyn arrow::array::Array>>>;

/// Wraps a [Schema] for passing across the FFI boundary.
pub struct ArrowArraySchema {
    pub schema: Arc<Schema>,
    pub enumerations: Arc<Enumerations>,
}

pub mod cxx {
    use super::*;

    pub fn to_arrow(
        array_schema: &ArraySchema,
        which: WhichSchema,
    ) -> Result<Box<ArrowArraySchema>, Error> {
        let (schema, enumerations) = super::project_arrow(array_schema, which, |_: &Field| true)?;
        Ok(Box::new(ArrowArraySchema {
            schema: Arc::new(schema),
            enumerations: Arc::new(enumerations),
        }))
    }

    /// Returns a [Schema] which represents the physical field types of
    /// the fields from `array_schema` which are contained in `select`.
    // NB: we use `Vec` for facilitating the FFI boundary
    #[allow(clippy::ptr_arg)]
    pub fn project_arrow(
        array_schema: &ArraySchema,
        which: WhichSchema,
        select: &Vec<String>,
    ) -> Result<Box<ArrowArraySchema>, Error> {
        let (schema, enumerations) = super::project_arrow(array_schema, which, |field: &Field| {
            select.iter().any(|s| s.as_str() == field.name_cxx())
        })?;
        Ok(Box::new(ArrowArraySchema {
            schema: Arc::new(schema),
            enumerations: Arc::new(enumerations),
        }))
    }
}

pub fn to_arrow(
    array_schema: &ArraySchema,
    which: WhichSchema,
) -> Result<(Schema, Enumerations), Error> {
    project_arrow(array_schema, which, |_: &Field| true)
}

/// Returns a [Schema] which represents the physical field types of the selected fields from `array_schema`.
pub fn project_arrow<F>(
    array_schema: &ArraySchema,
    which: WhichSchema,
    select: F,
) -> Result<(Schema, Enumerations), Error>
where
    F: Fn(&Field) -> bool,
{
    let fields = array_schema
        .fields()
        .filter(select)
        .map(|f| {
            let field_name = f
                .name()
                .map_err(|e| Error::NameNotUtf8(f.name_cxx().as_bytes().to_vec(), e))?;
            let arrow_type = field_arrow_datatype(array_schema, which, &f)
                .map_err(|e| Error::FieldError(field_name.to_owned(), e))?;

            // NB: fields can always be null due to schema evolution
            let arrow = ArrowField::new(field_name, arrow_type, true);

            if let Some(ename) = f.enumeration_name() {
                let ename = ename
                    .map_err(|e| {
                        let ename_cxx = {
                            // SAFETY: it's `Some` to get into the block, it still will be
                            f.enumeration_name_cxx().unwrap()
                        };
                        FieldError::EnumerationNameNotUtf8(ename_cxx.as_bytes().to_vec(), e)
                    })
                    .map_err(|e| Error::FieldError(field_name.to_owned(), e))?;
                Ok(arrow.with_metadata(HashMap::from([(
                    "enumeration".to_owned(),
                    ename.to_owned(),
                )])))
            } else {
                Ok(arrow)
            }
        })
        .collect::<Result<ArrowFields, _>>()?;

    let enumerations = fields
        .iter()
        .filter_map(|f| f.metadata().get("enumeration"))
        .unique()
        .map(|e| {
            let enumeration = array_schema.enumeration(e);
            if enumeration.is_null() {
                Ok((e.to_owned(), None))
            } else {
                let a = unsafe {
                    // SAFETY: TODO comment
                    crate::enumeration::array_from_enumeration(&enumeration)
                }
                .map_err(|err| Error::EnumerationError(e.to_owned(), err))?;
                Ok((e.to_owned(), Some(a)))
            }
        })
        .collect::<Result<Enumerations, _>>()?;

    Ok((
        Schema {
            fields,
            metadata: Default::default(),
        },
        enumerations,
    ))
}

/// Returns an [ArrowDataType] which represents the physical data type of `field`.
pub fn field_arrow_datatype(
    array_schema: &ArraySchema,
    which: WhichSchema,
    field: &Field,
) -> Result<ArrowDataType, FieldError> {
    match which {
        WhichSchema::Storage => arrow_datatype(field.datatype(), field.cell_val_num()),
        WhichSchema::View => {
            let Some(e_name) = field.enumeration_name_cxx() else {
                return arrow_datatype(field.datatype(), field.cell_val_num());
            };
            if !array_schema.has_enumeration(e_name) {
                return Err(FieldError::InternalEnumerationNotFound(
                    e_name.to_string_lossy().into_owned(),
                ));
            }

            let enumeration = array_schema.enumeration_cxx(e_name);

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
            Ok(value_type)
        }
        invalid => unreachable!(
            "Request for invalid schema type with discriminant {}",
            invalid.repr
        ),
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
