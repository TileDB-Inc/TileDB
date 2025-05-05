use std::str::Utf8Error;
use std::sync::Arc;

use datafusion::common::arrow::array::{
    Array as ArrowArray, FixedSizeListArray, GenericListArray, PrimitiveArray,
};
use datafusion::common::arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::common::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use datafusion::common::arrow::record_batch::RecordBatch;
use itertools::Itertools;
use oxidize::sm::array_schema::{ArraySchema, CellValNum, Field};
use oxidize::sm::query::readers::{ResultTile, TileTuple};
use oxidize::sm::tile::Tile;

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Field name is not UTF-8")]
    NameNotUtf8(Vec<u8>, Utf8Error),
    #[error("Cannot process field '{0}': {1}")]
    FieldError(String, #[source] FieldError),
}

#[derive(Debug, thiserror::Error)]
pub enum FieldError {
    #[error("Unexpected validity tile for non-nullable field")]
    UnexpectedValidityTile,
    #[error("Expected validity tile for nullable field")]
    ExpectedValidityTile,
    #[error("Unexpected offsets tile for field with cell val num {0}")]
    UnexpectedVarTile(CellValNum),
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
    arrow: RecordBatch,
}

pub fn to_arrow_record_batch(
    schema: &ArraySchema,
    tile: &ResultTile,
) -> Result<Box<ArrowRecordBatch>, Error> {
    let (fields, columns) = schema
        .fields()
        .map(|f| {
            let ptr_tile = tile.tile_tuple(f.name_cxx());
            if ptr_tile.is_null() {
                Ok::<Option<_>, Error>(None)
            } else {
                let field_name = f
                    .name()
                    .map_err(|e| Error::NameNotUtf8(f.name_cxx().as_bytes().to_vec(), e))?;
                let tile = unsafe {
                    // SAFETY: `is_null` above
                    &*ptr_tile
                };
                let array = to_arrow_array(&f, tile)
                    .map_err(|e| Error::FieldError(field_name.to_owned(), e))?;
                Ok(Some((
                    ArrowField::new(
                        field_name.to_owned(),
                        array.data_type().clone(),
                        f.nullable(),
                    ),
                    array,
                )))
            }
        })
        .process_results(|oks| {
            oks.flatten()
                .collect::<(Vec<ArrowField>, Vec<Arc<dyn ArrowArray>>)>()
        })?;

    let arrow = RecordBatch::try_new(
        Arc::new(ArrowSchema {
            fields: fields.into(),
            metadata: Default::default(),
        }),
        columns,
    );

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

fn to_arrow_array(f: &Field, tile: &TileTuple) -> Result<Arc<dyn ArrowArray>, FieldError> {
    let null_buffer = if let Some(validity) = tile.validity() {
        if !f.nullable() {
            return Err(FieldError::UnexpectedValidityTile);
        }
        Some(
            validity
                .as_slice()
                .iter()
                .map(|v| *v != 0)
                .collect::<NullBuffer>(),
        )
    } else if f.nullable() {
        return Err(FieldError::ExpectedValidityTile);
    } else {
        None
    };

    match f.cell_val_num() {
        CellValNum::Single => {
            if tile.has_var_tile() {
                return Err(FieldError::UnexpectedVarTile(f.cell_val_num()));
            }
            to_primitive_array(f, tile.fixed_tile(), null_buffer)
        }
        CellValNum::Fixed(nz) => {
            if tile.has_var_tile() {
                return Err(FieldError::UnexpectedVarTile(f.cell_val_num()));
            }
            let Ok(fixed_size) = i32::try_from(nz.get()) else {
                todo!()
            };

            let values = to_primitive_array(f, tile.fixed_tile(), None)?;

            Ok(Arc::new(FixedSizeListArray::new(
                Arc::new(ArrowField::new_list_field(
                    values.data_type().clone(),
                    false,
                )),
                fixed_size,
                values,
                null_buffer,
            )))
        }
        CellValNum::Var => {
            let Some(offsets_tile) = tile.offsets() else {
                return Err(FieldError::ExpectedVarTile);
            };
            let offsets_arrow = {
                let (prefix, offsets, suffix) = unsafe {
                    // SAFETY: transmuting u8 to i64 is safe
                    offsets_tile.as_slice().align_to::<i64>()
                };
                if !prefix.is_empty() || !suffix.is_empty() {
                    return Err(FieldError::InternalUnalignedOffsets);
                }

                // TileDB storage format (as of this writing) uses byte offsets and
                // uses the fixed data length to implicitly indicate the length of the last
                // element, so as to have `n` offsets for `n` cells.
                //
                // Arrow uses element offsets and has `n + 1` offsets for `n` cells.
                //
                // Because of this it is necessary to dynamically allocate the offsets,
                // which is very sad.

                let value_size = f.datatype().value_size() as i64;
                let try_element_offset = |o: i64| {
                    if o % value_size == 0 {
                        Ok(o / value_size)
                    } else {
                        Err(FieldError::InternalNonIntegralOffsets)
                    }
                };

                let sbuf = ScalarBuffer::<i64>::from(unsafe {
                    // SAFETY: slice has a trusted upper length
                    Buffer::try_from_trusted_len_iter(
                        offsets
                            .iter()
                            .map(|o| try_element_offset(*o))
                            .chain(std::iter::once(try_element_offset(
                                tile.fixed_tile().size() as i64,
                            ))),
                    )?
                });
                if sbuf[0] < 0 {
                    todo!()
                } else if !sbuf.windows(2).all(|w| w[0] <= w[1]) {
                    todo!()
                }

                OffsetBuffer::<i64>::new(sbuf)
            };

            let values = to_primitive_array(f, tile.fixed_tile(), None)?;
            Ok(Arc::new(GenericListArray::new(
                Arc::new(ArrowField::new_list_field(
                    values.data_type().clone(),
                    false,
                )),
                offsets_arrow,
                values,
                null_buffer,
            )))
        }
    }
}

fn to_primitive_array(
    field: &Field,
    fixed_tile: &Tile,
    validity: Option<NullBuffer>,
) -> Result<Arc<dyn ArrowArray>, FieldError> {
    macro_rules! to_primitive_array {
        ($arrowtype:ty) => {{
            type Native = <$arrowtype as ArrowPrimitiveType>::Native;
            let (prefix, values, suffix) = unsafe { fixed_tile.as_slice().align_to::<Native>() };
            if !(prefix.is_empty() && suffix.is_empty()) {
                // SAFETY: transmuting u8 to primitive types is safe
                return Err(FieldError::InternalUnalignedValues);
            }
            Ok(Arc::new(PrimitiveArray::<$arrowtype>::new(
                ScalarBuffer::from(Buffer::from_slice_ref::<Native, _>(values)),
                validity,
            )) as Arc<dyn ArrowArray>)
        }};
    }

    use datafusion::common::arrow::array::types::*;
    match field.datatype() {
        Datatype::INT8 => to_primitive_array!(Int8Type),
        Datatype::INT16 => to_primitive_array!(Int16Type),
        Datatype::INT32 => to_primitive_array!(Int32Type),
        Datatype::INT64 => to_primitive_array!(Int64Type),
        Datatype::UINT8 => to_primitive_array!(UInt8Type),
        Datatype::UINT16 => to_primitive_array!(UInt16Type),
        Datatype::UINT32 => to_primitive_array!(UInt32Type),
        Datatype::UINT64 => to_primitive_array!(UInt64Type),
        Datatype::FLOAT32 => to_primitive_array!(Float32Type),
        Datatype::FLOAT64 => to_primitive_array!(Float64Type),
        _ => todo!(),
    }
}
