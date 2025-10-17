//! Provides definitions for viewing a [ResultTile] as an Arrow [RecordBatch].
//!
//! The functions in this module are `unsafe` because the FFI boundary
//! prevents us from expressing a lifetime relationship between the
//! returned [RecordBatch] and the argument [ResultTile].
use std::sync::Arc;

use arrow::array::{
    self as aa, Array as ArrowArray, FixedSizeListArray, GenericListArray, LargeStringArray,
    PrimitiveArray,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{self as adt, ArrowPrimitiveType, Field};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use tiledb_cxx_interface::sm::query::readers::{ResultTile, TileTuple};

use super::*;
use crate::offsets::Error as OffsetsError;

/// An error creating a [RecordBatch] to represent a [ResultTile].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Cannot process field '{0}': {1}")]
    FieldError(String, #[source] FieldError),
}

/// An error creating an [ArrowArray] for a specific field of a tile.
#[derive(Debug, thiserror::Error)]
pub enum FieldError {
    #[error("Internal error: invalid data type: {0}")]
    ArrowDataType(#[from] crate::schema::FieldError),
    #[error("Unexpected validity tile for non-nullable field")]
    UnexpectedValidityTile,
    #[error("Unexpected offsets tile for fixed-length field")]
    UnexpectedVarTile,
    #[error("Expected offsets tile for field with variable cell val num")]
    ExpectedVarTile,
    #[error("Internal error: unaligned tile data values")]
    InternalUnalignedValues,
    #[error("Internal error: invalid variable-length data offsets: {0}")]
    InternalOffsets(#[from] OffsetsError),
    #[error("Error reading tile: {0}")]
    InvalidTileData(#[source] arrow::error::ArrowError),
    #[error("Attributes with enumerations are not supported in text predicates")]
    EnumerationNotSupported,
}

/// Wraps a [RecordBatch] for passing across the FFI boundary.
pub struct ArrowRecordBatch {
    pub arrow: RecordBatch,
}

/// Returns a [RecordBatch] which contains the same contents as a [ResultTile].
///
/// # Safety
///
/// When possible this function avoids copying data. This means that the
/// returned [RecordBatch] may reference data which lives inside one or more
/// of the [ResultTile]'s attribute [Tile]s. This function is safe to call as
/// long as the returned [RecordBatch] is not used after the [ResultTile]
/// is destructed.
pub unsafe fn to_record_batch(
    schema: &ArrowArraySchema,
    tile: &ResultTile,
) -> Result<Box<ArrowRecordBatch>, Error> {
    let columns = schema
        .schema
        .fields()
        .iter()
        .map(|f| {
            // FIXME: see is_special_attribute case
            let ptr_tile = {
                cxx::let_cxx_string!(fname = f.name());
                tile.tile_tuple(&fname)
            };
            if ptr_tile.is_null() {
                return Ok(aa::new_null_array(f.data_type(), tile.cell_num() as usize));
            }

            let tile = unsafe {
                // SAFETY: diverging `is_null` above
                &*ptr_tile
            };
            unsafe {
                // SAFETY: the caller is responsible that each attribute tile in `tile`
                // out-lives the `Arc<dyn ArrowArray>` created here. See function docs.
                tile_to_arrow_array(f, tile)
            }
            .map_err(|e| Error::FieldError(f.name().to_owned(), e))
        })
        .collect::<Result<Vec<Arc<dyn ArrowArray>>, _>>()?;

    // SAFETY: should be clear from iteration
    assert_eq!(schema.schema.fields().len(), columns.len());

    // SAFETY: `tile_to_arrow_array` must do this, major internal error if not
    // which is not recoverable
    assert!(
        schema
            .schema
            .fields()
            .iter()
            .zip(columns.iter())
            .all(|(f, c)| f.data_type() == c.data_type())
    );

    // SAFETY: dependent on the correctness of `tile_to_arrow_array` AND the integrity of
    // the underlying `ResultTile`.
    // Neither of these conditions is a recoverable error from the user perspective -
    // hence assert.
    assert!(
        columns.iter().all(|c| c.len() as u64 == tile.cell_num()),
        "Columns do not all have same number of cells: {:?} {:?}",
        schema.schema.fields(),
        columns.iter().map(|c| c.len()).collect::<Vec<_>>()
    );

    // SAFETY: the four asserts above rule out each of the possible error conditions
    let arrow = if columns.is_empty() {
        RecordBatch::try_new_with_options(
            Arc::clone(&schema.schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(tile.cell_num() as usize)),
        )
    } else {
        RecordBatch::try_new(Arc::clone(&schema.schema), columns)
    };

    let arrow = arrow.expect("Logic error: preconditions for constructing RecordBatch not met");

    Ok(Box::new(ArrowRecordBatch { arrow }))
}

/// Returns an [ArrowArray] which contains the same contents as the provided
/// [TileTuple].
///
/// # Safety
///
/// When possible this function avoids copying data. This means that the
/// returned [ArrowArray] may reference data which lives inside the [Tile]s.
/// This function is safe to call as long as the returned [ArrowArray] is not
/// used after those [Tile]s are destructed.
unsafe fn tile_to_arrow_array(
    f: &Field,
    tile: &TileTuple,
) -> Result<Arc<dyn ArrowArray>, FieldError> {
    unsafe {
        // SAFETY: the caller is responsible that each of the tiles tile out-live
        // the `Arc<dyn ArrowArray>` created here. See function docs.
        to_arrow_array(
            f,
            tile.fixed_tile().as_slice(),
            tile.var_tile().map(|t| t.as_slice()),
            tile.validity_tile().map(|t| t.as_slice()),
        )
    }
}

/// Returns an [ArrowArray] which contains the same contents as the provided
/// triple of `&[u8]`s.
///
/// If `var.is_some()`, then `fixed` contains the offsets and `var` contains
/// the values. Otherwise, `fixed` contains the values.
///
/// The `validity` `&[u8]` contains one value per cell.
///
/// # Safety
///
/// When possible this function avoids copying data. This means that the
/// returned [ArrowArray] may reference data which lives inside the `&[u8]`s.
/// This function is safe to call as long as the returned [ArrowArray] is not
/// used after the data which the `&[u8]` borrows are destructed.
pub unsafe fn to_arrow_array(
    f: &Field,
    fixed: &[u8],
    var: Option<&[u8]>,
    validity: Option<&[u8]>,
) -> Result<Arc<dyn ArrowArray>, FieldError> {
    let null_buffer = if let Some(validity) = validity {
        if !f.is_nullable() {
            return Err(FieldError::UnexpectedValidityTile);
        }
        Some(validity.iter().map(|v| *v != 0).collect::<NullBuffer>())
    } else if f.is_nullable() {
        // NB: this is allowed even for nullable fields, it means that none of
        // the cells is `NULL`.  Note that due to schema evolution the arrow
        // field is always declared nullable.
        None
    } else {
        None
    };

    macro_rules! match_arm_primitive {
        ($primitivetype:ty) => {{
            if var.is_some() {
                return Err(FieldError::UnexpectedVarTile);
            }
            unsafe {
                // SAFETY: the caller is responsible that the `fixed` tile out-lives
                // the `PrimitiveArray` created here. See function docs.
                to_primitive_array::<$primitivetype>(fixed, null_buffer)
            }
        }};
    }

    use adt::*;
    match f.data_type() {
        DataType::Int8 => match_arm_primitive!(Int8Type),
        DataType::Int16 => match_arm_primitive!(Int16Type),
        DataType::Int32 => match_arm_primitive!(Int32Type),
        DataType::Int64 => match_arm_primitive!(Int64Type),
        DataType::UInt8 => match_arm_primitive!(UInt8Type),
        DataType::UInt16 => match_arm_primitive!(UInt16Type),
        DataType::UInt32 => match_arm_primitive!(UInt32Type),
        DataType::UInt64 => match_arm_primitive!(UInt64Type),
        DataType::Float32 => match_arm_primitive!(Float32Type),
        DataType::Float64 => match_arm_primitive!(Float64Type),
        DataType::FixedSizeList(value_field, fixed_size) => {
            let values = unsafe {
                // SAFETY: the caller is responsible that the `fixed` tile out-lives
                // the `PrimitiveArray` created here. See function docs.
                to_arrow_array(value_field, fixed, None, None)?
            };
            Ok(Arc::new(FixedSizeListArray::new(
                Arc::clone(value_field),
                *fixed_size,
                values,
                null_buffer,
            )))
        }
        DataType::LargeUtf8 => {
            let Some(var_tile) = var else {
                return Err(FieldError::ExpectedVarTile);
            };
            let offsets = crate::offsets::try_from_bytes(1, fixed)?;
            let values = unsafe {
                // SAFETY: the caller is responsible that `fixed` out-lives
                // the `Buffer` created here. See function docs.
                to_buffer::<UInt8Type>(var_tile)
            }?;

            Ok(Arc::new(
                LargeStringArray::try_new(offsets, values.into(), null_buffer)
                    .map_err(FieldError::InvalidTileData)?,
            ))
        }
        DataType::LargeList(value_field) => {
            let Some(var_tile) = var else {
                return Err(FieldError::ExpectedVarTile);
            };
            let offsets = to_offsets_buffer(value_field, fixed)?;
            let values = unsafe {
                // SAFETY: the caller is responsible that `var_tile` out-lives
                // the `PrimitiveArray` created here. See function docs.
                to_arrow_array(value_field, var_tile, None, None)?
            };
            Ok(Arc::new(GenericListArray::new(
                Arc::clone(value_field),
                offsets,
                values,
                null_buffer,
            )))
        }
        DataType::Null => {
            // NB: see `arrow/src/schema.rs`.
            // This represents the value type of an attribute with an enumeration
            // which we will implement later in CORE-285.
            Err(FieldError::EnumerationNotSupported)
        }
        _ => {
            // SAFETY: ensured by limited range of return values of `crate::schema::arrow_datatype`
            unreachable!(
                "Tile has unexpected data type for arrow array: {:?}",
                f.data_type()
            )
        }
    }
}

/// Returns a [PrimitiveArray] which contains the same contents as a [Tile]
/// with the provided `validity`.
///
/// # Safety
///
/// The returned [PrimitiveArray] shares a buffer with the argument [Tile].
/// This function is safe to call as long as the returned [PrimitiveArray]
/// is not used after the argument [Tile] is destructed.
unsafe fn to_primitive_array<T>(
    bytes: &[u8],
    validity: Option<NullBuffer>,
) -> Result<Arc<dyn ArrowArray>, FieldError>
where
    T: ArrowPrimitiveType,
{
    let values = unsafe {
        // SAFETY: TODO add comment
        to_buffer::<T>(bytes)
    }?;
    Ok(Arc::new(PrimitiveArray::<T>::new(values, validity)) as Arc<dyn ArrowArray>)
}

/// Returns a [Buffer] which refers to the data contained inside the `&[u8]`.
///
/// # Safety
///
/// This function is safe to call as long as the returned [Buffer]
/// is not used after the argument [Tile] is destructed.
unsafe fn to_buffer<T>(bytes: &[u8]) -> Result<ScalarBuffer<T::Native>, FieldError>
where
    T: ArrowPrimitiveType,
{
    let (prefix, values, suffix) = {
        // SAFETY: transmuting u8 to primitive types is safe
        unsafe { bytes.align_to::<T::Native>() }
    };
    if !(prefix.is_empty() && suffix.is_empty()) {
        return Err(FieldError::InternalUnalignedValues);
    }

    Ok(ScalarBuffer::<T::Native>::from(
        if let Some(ptr) = std::ptr::NonNull::new(values.as_ptr() as *mut u8) {
            // SAFETY:
            //
            // `Buffer::from_custom_allocation` creates a buffer which refers to an existing
            // memory region whose ownership is tracked by some `Arc<dyn Allocation>`.
            // `Allocation` is basically any type, whose `drop` implementation is responsible
            // for freeing the memory.
            //
            // The tile memory which we reference lives on the `extern "C++"` side of the
            // FFI boundary, as such we cannot use `Arc` to track its lifetime.
            //
            // As such:
            // 1) we will use an object with trivial `drop` to set up the memory aliasing
            // 2) there is an implicit lifetime requirement that the Tile must out-live
            //    this Buffer, else we shall suffer use after free
            // 3) the caller is responsible for upholding that guarantee
            unsafe { Buffer::from_custom_allocation(ptr, bytes.len(), Arc::new(())) }
        } else {
            Buffer::from_vec(Vec::<T::Native>::new())
        },
    ))
}

/// Returns an [OffsetBuffer] which represents the contents of the `[u8]`.
fn to_offsets_buffer(value_field: &Field, bytes: &[u8]) -> Result<OffsetBuffer<i64>, OffsetsError> {
    let Some(value_size) = value_field.data_type().primitive_width() else {
        // SAFETY: all list types have primitive element
        // FIXME: this is true for schema fields, not generally true,
        // so either do proper error handling or refactor
        unreachable!(
            "Unexpected list field element type: {}",
            value_field.data_type()
        )
    };
    crate::offsets::try_from_bytes(value_size, bytes)
}
