use std::sync::Arc;

use arrow::array::Array as ArrowArray;
use arrow::datatypes::Field as ArrowField;

use tiledb_cxx_interface::sm::array_schema::Enumeration;

use crate::{record_batch, schema};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Enumeration data type error: {0}")]
    DataType(#[from] crate::schema::FieldError),
    #[error("Enumeration variants error: {0}")]
    Variants(#[from] crate::record_batch::FieldError),
}

/// Returns an [ArrowArray] whose elements are the variants of an [Enumeration].
///
/// # Safety
///
/// When possible this function avoids copying data. This means that the
/// returned [ArrowArray] may reference data which lives inside the [Enumeration].
/// It is not safe to use the value returned from this function after
/// the [Enumeration] is destructed. The caller must take care to abide this
/// requirement. Otherwise this function is safe to use.
pub unsafe fn array_from_enumeration_ffi(
    enumeration: &Enumeration,
) -> Result<Box<super::ArrowArray>, Error> {
    let a = unsafe { array_from_enumeration(enumeration) }?;
    Ok(Box::new(super::ArrowArray(a)))
}

/// Returns an [ArrowArray] whose elements are the variants of an [Enumeration].
///
/// # Safety
///
/// When possible this function avoids copying data. This means that the
/// returned [ArrowArray] may reference data which lives inside the [Enumeration].
/// It is not safe to use the value returned from this function after
/// the [Enumeration] is destructed. The caller must take care to abide this
/// requirement. Otherwise this function is safe to use.
pub unsafe fn array_from_enumeration(
    enumeration: &Enumeration,
) -> Result<Arc<dyn ArrowArray + 'static>, Error> {
    let field = {
        let adt = schema::arrow_datatype(enumeration.datatype(), enumeration.cell_val_num())?;
        ArrowField::new("unused", adt, false)
    };

    if let Some(offsets) = enumeration.offsets() {
        let (_, offsets, _) = unsafe {
            // SAFETY: just a transmutes u64 to u8 which always succeeds
            // with no possible alignment issues
            offsets.align_to::<u8>()
        };
        Ok(unsafe {
            record_batch::to_arrow_array(&field, offsets, Some(enumeration.data()), None)
        }?)
    } else {
        Ok(unsafe { record_batch::to_arrow_array(&field, enumeration.data(), None, None) }?)
    }
}
