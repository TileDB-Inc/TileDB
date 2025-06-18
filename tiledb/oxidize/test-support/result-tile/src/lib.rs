//! Provides methods for constructing internal [ResultTile] objects
//! from `tiledb_test_cells::Cells`.
//!
//! This enables property-based testing against arbitrary tiles
//! using the strategies we have already written in `tiledb_test_cells`.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use arrow::array::{Array as ArrowArray, GenericListArray, PrimitiveArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use tiledb_cxx_interface::sm::array_schema::{ArraySchema, CellValNum};
use tiledb_cxx_interface::sm::query::readers::ResultTile;
use tiledb_test_cells::{Cells, FieldData, typed_field_data_go};

/// Packages a `ResultTile` with the buffers which contain the tile data.
pub struct PackagedResultTile {
    /// Buffers underlying the [ResultTile].
    #[allow(dead_code)]
    buffers: RecordBatch,
    /// Buffers for offsets which in the `RecordBatch` are element units and in
    /// the `ResultTile` are byte units.
    #[allow(dead_code)]
    offsets: HashMap<Vec<u8>, Vec<u64>>,
    /// Buffers for validity which is not bit-packed and thus not compatible with arrow.
    /// Since `Cells` does not have nullable fields this can be shared by all fields.
    #[allow(dead_code)]
    validity: HashMap<Vec<u8>, Vec<u8>>,
    // NB: tile data borrows the record batch columns, this is for sure "unsafe"
    // but should be fine since the contents are behind an `Arc`
    result_tile: cxx::SharedPtr<ResultTile>,
}

impl PackagedResultTile {
    pub fn new(schema: &ArraySchema, batch: RecordBatch) -> anyhow::Result<PackagedResultTile> {
        // FIXME: check that all nested fields are not nullable
        result_tile_from_record_batch(schema, batch)
    }
}

impl Deref for PackagedResultTile {
    type Target = ResultTile;
    fn deref(&self) -> &Self::Target {
        &self.result_tile
    }
}

/// Constructs a [ResultTile] which contains the same logical contents of the requested `cells`.
pub fn result_tile_from_cells(
    schema: &ArraySchema,
    cells: &Cells,
) -> anyhow::Result<PackagedResultTile> {
    let buffers = cells_to_record_batch(cells);
    PackagedResultTile::new(schema, buffers)
}

/// Constructs a [ResultTile] from an Arrow [RecordBatch].
fn result_tile_from_record_batch(
    schema: &ArraySchema,
    batch: RecordBatch,
) -> anyhow::Result<PackagedResultTile> {
    let result_tile = tiledb_test_support::result_tile::new_result_tile(
        batch.num_rows() as u64,
        schema,
        tiledb_test_support::get_test_memory_tracker(),
    );

    let mut offsets = HashMap::new();
    let mut validity = HashMap::new();

    for field in batch.schema().fields.iter() {
        let column = {
            // SAFETY: guaranteed by `RecordBatch` contract
            batch.column_by_name(field.name()).unwrap()
        };
        cxx::let_cxx_string! { field_name = field.name() };

        let column_data = column.to_data();
        assert_eq!(0, column_data.offset());
        assert_eq!(column.len(), column_data.len());

        let (data, value_offsets) = if schema.var_size(&field_name) {
            assert_eq!(1, column_data.buffers().len());
            assert_eq!(1, column_data.child_data().len());
            assert_eq!(1, column_data.child_data()[0].buffers().len());

            let value_width = column_data.child_data()[0]
                .data_type()
                .primitive_width()
                .unwrap();

            (
                column_data.child_data()[0].buffer::<u8>(0),
                Some((column_data.buffer::<u64>(0), value_width as u64)),
            )
        } else if let CellValNum::Fixed(nz) = schema.cell_val_num(&field_name) {
            // list type, whether FixedSizeListArray or ListArray is source data dependendent
            assert_eq!(1, column_data.child_data().len());
            assert_eq!(1, column_data.child_data()[0].buffers().len());

            if column_data.buffers().len() == 1 {
                // this came from a source such as `tiledb_test_cells::Cells` which does not
                // know the cell val num. The source array is also a large list,
                // here we will check that all the offsets match and then flatten
                for offset in column_data.buffer::<u64>(0).windows(2) {
                    assert_eq!(offset[1] - offset[0], nz.get() as u64);
                }
            } else {
                // source data was a FixedSizeList array
                assert_eq!(0, column_data.buffers().len());
            }
            (column_data.child_data()[0].buffer::<u8>(0), None)
        } else {
            assert_eq!(1, column_data.buffers().len());
            assert_eq!(0, column_data.child_data().len());
            (column_data.buffer::<u8>(0), None)
        };

        let field_key = field_name.as_bytes().to_vec();

        let tile_offsets = if let Some((value_offsets, value_width)) = value_offsets {
            offsets.insert(
                field_key.clone(),
                value_offsets
                    .iter()
                    .map(|o| *o * value_width)
                    .collect::<Vec<u64>>(),
            );
            offsets.get(&field_key).map(|v| v.as_slice()).unwrap()
        } else {
            unsafe { std::slice::from_raw_parts(std::ptr::NonNull::<u64>::dangling().as_ptr(), 0) }
        };

        if let Some(nulls) = column_data.nulls() {
            validity.insert(
                field_key.clone(),
                nulls
                    .iter()
                    .map(|b| if b { 1u8 } else { 0u8 })
                    .collect::<Vec<_>>(),
            );
        } else if schema.is_nullable(&field_name) {
            validity.insert(field_key.clone(), vec![1u8; batch.num_rows()]);
        }

        if schema.is_dim(&field_name) {
            assert!(validity.contains_key(&field_key));
            let dim_num = schema.domain().get_dimension_index(&field_name);
            tiledb_test_support::result_tile::init_coord_tile(
                cxx::SharedPtr::clone(&result_tile),
                schema,
                &field_name,
                data,
                tile_offsets,
                dim_num,
            );
        } else {
            let column_validity = validity
                .get(&field_key)
                .map(|v| v.as_slice().as_ptr())
                .unwrap_or(std::ptr::null::<u8>());
            unsafe {
                tiledb_test_support::result_tile::init_attr_tile(
                    cxx::SharedPtr::clone(&result_tile),
                    schema,
                    &field_name,
                    data,
                    tile_offsets,
                    column_validity,
                )
            }
        }
    }

    Ok(PackagedResultTile {
        buffers: batch,
        offsets,
        validity,
        result_tile,
    })
}

fn cells_to_record_batch(cells: &Cells) -> RecordBatch {
    let (fields, columns) = cells
        .fields()
        .iter()
        .map(|(fname, fdata)| {
            let arrow_array = field_data_to_array(fdata);
            (
                ArrowField::new(fname.to_owned(), arrow_array.data_type().clone(), false),
                arrow_array,
            )
        })
        .collect::<(Vec<ArrowField>, Vec<Arc<dyn ArrowArray>>)>();

    // SAFETY: test code who cares
    RecordBatch::try_new(
        Arc::new(ArrowSchema {
            fields: fields.into(),
            metadata: Default::default(),
        }),
        columns,
    )
    .unwrap()
}

fn field_data_to_array(field: &FieldData) -> Arc<dyn ArrowArray> {
    typed_field_data_go!(
        field,
        _DT,
        cells,
        Arc::new(cells.iter().copied().collect::<PrimitiveArray<_>>()) as Arc<dyn ArrowArray>,
        {
            let values = cells
                .iter()
                .flatten()
                .copied()
                .collect::<PrimitiveArray<_>>();
            let offsets = OffsetBuffer::<i64>::from_lengths(cells.iter().map(|s| s.len()));
            let cells = GenericListArray::new(
                Arc::new(ArrowField::new_list_field(
                    values.data_type().clone(),
                    false,
                )),
                offsets,
                Arc::new(values),
                None,
            );
            Arc::new(cells)
        }
    )
}
