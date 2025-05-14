use std::sync::Arc;

use datafusion::common::arrow::array::{
    Array as ArrowArray, FixedSizeListArray, GenericListArray, PrimitiveArray,
};
use datafusion::common::arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::common::arrow::datatypes::{self as adt, ArrowPrimitiveType, Field};
use datafusion::common::arrow::record_batch::RecordBatch;
use oxidize::sm::query::readers::{ResultTile, TileTuple};
use oxidize::sm::tile::Tile;

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Tile data is unavailable for field '{0}'")]
    NoData(String),
    #[error("Cannot process field '{0}': {1}")]
    FieldError(String, #[source] FieldError),
}

#[derive(Debug, thiserror::Error)]
pub enum FieldError {
    #[error("Internal error: invalid data type: {0}")]
    ArrowDataType(#[from] crate::schema::FieldError),
    #[error("Unexpected validity tile for non-nullable field")]
    UnexpectedValidityTile,
    #[error("Expected validity tile for nullable field")]
    ExpectedValidityTile,
    #[error("Unexpected offsets tile for fixed-length field")]
    UnexpectedVarTile,
    #[error("Expected offsets tile for field with variable cell val num")]
    ExpectedVarTile,
    #[error("Internal error: variable-length data offsets index a non-integral cell")]
    InternalNonIntegralOffsets,
    #[error("Internal error: unaligned variable-length data offsets")]
    InternalUnalignedOffsets,
    #[error("Internal error: unaligned tile data values")]
    InternalUnalignedValues,
}

pub struct ArrowRecordBatch {
    pub arrow: RecordBatch,
}

pub fn to_record_batch(
    schema: &DataFusionSchema,
    tile: &ResultTile,
) -> Result<Box<ArrowRecordBatch>, Error> {
    let columns = schema
        .0
        .fields()
        .iter()
        .map(|f| {
            let ptr_tile = {
                cxx::let_cxx_string!(fname = f.name());
                tile.tile_tuple(&fname)
            };
            if ptr_tile.is_null() {
                return Err(Error::NoData(f.name().to_owned()));
            }

            let tile = unsafe {
                // SAFETY: diverging `is_null` above
                &*ptr_tile
            };
            Ok(tile_to_arrow_array(f, tile)
                .map_err(|e| Error::FieldError(f.name().to_owned(), e))?)
        })
        .collect::<Result<Vec<Arc<dyn ArrowArray>>, _>>()?;

    let arrow = RecordBatch::try_new(Arc::clone(schema.0.inner()), columns);

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
        return Err(FieldError::ExpectedValidityTile);
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
        _ => todo!(),
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

fn to_offsets_buffer(value_field: &Field, tile: &Tile) -> Result<OffsetBuffer<i64>, FieldError> {
    let (prefix, offsets, suffix) = unsafe {
        // SAFETY: transmuting u8 to i64 is safe
        tile.as_slice().align_to::<i64>()
    };
    if !prefix.is_empty() || !suffix.is_empty() {
        return Err(FieldError::InternalUnalignedOffsets);
    }

    // TileDB storage format (as of this writing) uses byte offsets.
    // Arrow uses element offsets.
    // Those must be converted.
    //
    // There's also a reasonable question about the number of offsets.
    // In Arrow there are `n + 1` offsets for `n` cells, such that each
    //   cell `i` is delineated by offsets `i, i + 1`.
    // In TileDB write and read queries there are just `n` offsets,
    //   and the last cell is delineated by the end of the var data.
    //
    // However, it appears that the actual tile contents follow the
    // `n + 1` offsets format.  This is undoubtedly a good thing
    // for our use here (and subjectively so otherwise).
    // uses the fixed data length to implicitly indicate the length of the last
    // element, so as to have `n` offsets for `n` cells.
    //
    // But because we have to change from bytes to elements
    // we nonetheless have to dynamically allocate the offsets.

    let Some(value_size) = value_field.data_type().primitive_width().map(|s| s as i64) else {
        todo!()
    };
    let try_element_offset = |o: i64| {
        if o % value_size == 0 {
            Ok(o / value_size)
        } else {
            Err(FieldError::InternalNonIntegralOffsets)
        }
    };

    let sbuf = ScalarBuffer::<i64>::from(unsafe {
        // SAFETY: slice has a trusted upper length
        Buffer::try_from_trusted_len_iter(offsets.iter().map(|o| try_element_offset(*o)))?
    });
    if sbuf[0] < 0 {
        todo!()
    } else if !sbuf.windows(2).all(|w| w[0] <= w[1]) {
        todo!()
    }

    Ok(OffsetBuffer::<i64>::new(sbuf))
}
