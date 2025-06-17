use std::sync::Arc;

use arrow::array::{
    self as aa, Array as ArrowArray, FixedSizeListArray, GenericListArray, PrimitiveArray,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{self as adt, ArrowPrimitiveType, Field};
use arrow::record_batch::RecordBatch;
use tiledb_cxx_interface::sm::query::readers::{ResultTile, TileTuple};
use tiledb_cxx_interface::sm::tile::Tile;

use super::*;
use crate::offsets::Error as OffsetsError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Cannot process field '{0}': {1}")]
    FieldError(String, #[source] FieldError),
}

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
}

pub struct ArrowRecordBatch {
    pub arrow: RecordBatch,
}

pub fn to_record_batch(
    schema: &ArrowSchema,
    tile: &ResultTile,
) -> Result<Box<ArrowRecordBatch>, Error> {
    let columns = schema
        .0
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
            tile_to_arrow_array(f, tile).map_err(|e| Error::FieldError(f.name().to_owned(), e))
        })
        .collect::<Result<Vec<Arc<dyn ArrowArray>>, _>>()?;

    let arrow = RecordBatch::try_new(Arc::clone(&schema.0), columns);

    // SAFETY: construction should ensure all the preconditions are met
    // "the vec of columns to not be empty" admittedly this is an assumption but we should have at
    // least one tile tuple
    // "the schema and column data types to have equal lengths and match" comes from the `collect`
    // "each array in columns to have the same length" this is definitely the sketchiest
    // since it assumes that the written tile data satisifes this condition, AND that our
    // to-arrow implementation preserves it. If either of these is not true then it is not a
    // recoverable error from the user perspective
    let arrow = arrow.unwrap();

    Ok(Box::new(ArrowRecordBatch { arrow }))
}

fn tile_to_arrow_array(f: &Field, tile: &TileTuple) -> Result<Arc<dyn ArrowArray>, FieldError> {
    to_arrow_array(f, tile.fixed_tile(), tile.var_tile(), tile.validity_tile())
}

fn to_arrow_array(
    f: &Field,
    fixed: &Tile,
    var: Option<&Tile>,
    validity: Option<&Tile>,
) -> Result<Arc<dyn ArrowArray>, FieldError> {
    let null_buffer = if let Some(validity) = validity {
        if !f.is_nullable() {
            return Err(FieldError::UnexpectedValidityTile);
        }
        Some(
            validity
                .as_slice()
                .iter()
                .map(|v| *v != 0)
                .collect::<NullBuffer>(),
        )
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
            to_primitive_array::<$primitivetype>(fixed, null_buffer)
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
            let values = to_arrow_array(value_field, fixed, None, None)?;
            Ok(Arc::new(FixedSizeListArray::new(
                Arc::clone(value_field),
                *fixed_size,
                values,
                null_buffer,
            )))
        }
        DataType::LargeList(value_field) => {
            let Some(var_tile) = var else {
                return Err(FieldError::ExpectedVarTile);
            };
            let offsets = to_offsets_buffer(value_field, fixed)?;
            let values = to_arrow_array(value_field, var_tile, None, None)?;
            Ok(Arc::new(GenericListArray::new(
                Arc::clone(value_field),
                offsets,
                values,
                null_buffer,
            )))
        }
        _ => {
            // SAFETY: ensured by limited range of return values of `crate::schema::arrow_datatype`
            unreachable!()
        }
    }
}

fn to_primitive_array<T>(
    tile: &Tile,
    validity: Option<NullBuffer>,
) -> Result<Arc<dyn ArrowArray>, FieldError>
where
    T: ArrowPrimitiveType,
{
    let (prefix, values, suffix) = {
        // SAFETY: transmuting u8 to primitive types is safe
        unsafe { tile.as_slice().align_to::<T::Native>() }
    };
    if !(prefix.is_empty() && suffix.is_empty()) {
        return Err(FieldError::InternalUnalignedValues);
    }
    let tile_buffer = if let Some(ptr) = std::ptr::NonNull::new(values.as_ptr() as *mut u8) {
        // SAFETY: dependent upon this Array out-living the tile
        unsafe { Buffer::from_custom_allocation(ptr, tile.size() as usize, Arc::new(())) }
    } else {
        Buffer::from_vec(Vec::<T::Native>::new())
    };

    Ok(Arc::new(PrimitiveArray::<T>::new(
        ScalarBuffer::from(tile_buffer),
        validity,
    )) as Arc<dyn ArrowArray>)
}

fn to_offsets_buffer(value_field: &Field, tile: &Tile) -> Result<OffsetBuffer<i64>, OffsetsError> {
    let Some(value_size) = value_field.data_type().primitive_width() else {
        // SAFETY: all list types have primitive element
        // FIXME: this is true for schema fields, not generally true,
        // so either do proper error handling or refactor
        unreachable!(
            "Unexpected list field element type: {}",
            value_field.data_type()
        )
    };
    crate::offsets::try_from_bytes(value_size, tile.as_slice())
}
