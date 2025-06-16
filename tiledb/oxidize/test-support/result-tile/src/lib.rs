use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use arrow::array::{Array as ArrowArray, GenericListArray, PrimitiveArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use tiledb_oxidize::sm::array_schema::{ArraySchema, CellValNum};
use tiledb_oxidize::sm::query::readers::ResultTile;
use tiledb_test_cells::{Cells, FieldData, typed_field_data_go};

/// Packages a `ResultTile` with the buffers which contain the tile data.
pub struct PackagedResultTile {
    /// Buffers underlying the [ResultTile].
    #[allow(dead_code)]
    buffers: RecordBatch,
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

pub fn result_tile_from_cells(
    schema: &ArraySchema,
    cells: &Cells,
) -> anyhow::Result<PackagedResultTile> {
    let buffers = cells_to_record_batch(cells);
    PackagedResultTile::new(schema, buffers)
}

fn result_tile_from_record_batch(
    schema: &ArraySchema,
    batch: RecordBatch,
) -> anyhow::Result<PackagedResultTile> {
    let result_tile = tiledb_test_support::result_tile::new_result_tile(
        batch.num_rows() as u64,
        schema,
        tiledb_test_support::get_test_memory_tracker(),
    );

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

        let (data, offsets) = if schema.var_size(&field_name) {
            assert_eq!(1, column_data.buffers().len());
            let offsets = {
                let offsets_buffer = column_data.buffers()[0].as_slice();
                let (pre, offsets, post) = unsafe { offsets_buffer.align_to::<u64>() };
                assert!(pre.is_empty());
                assert!(post.is_empty());
                offsets
            };

            assert_eq!(1, column_data.child_data().len());
            assert_eq!(1, column_data.child_data()[0].buffers().len());

            (column_data.child_data()[0].buffer::<u8>(0), offsets)
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
            (column_data.child_data()[0].buffer::<u8>(0), unsafe {
                std::slice::from_raw_parts::<u64>(std::ptr::NonNull::<u64>::dangling().as_ptr(), 0)
            })
        } else {
            assert_eq!(1, column_data.buffers().len());
            assert_eq!(0, column_data.child_data().len());
            (column_data.buffer::<u8>(0), unsafe {
                std::slice::from_raw_parts::<u64>(std::ptr::NonNull::<u64>::dangling().as_ptr(), 0)
            })
        };

        let validity_key = field_name.as_bytes().to_vec();

        if let Some(nulls) = column_data.nulls() {
            validity.insert(
                validity_key.clone(),
                nulls
                    .iter()
                    .map(|b| if b { 1u8 } else { 0u8 })
                    .collect::<Vec<_>>(),
            );
        }

        if schema.is_dim(&field_name) {
            assert!(validity.get(&validity_key).is_none());
            let dim_num = schema.domain().get_dimension_index(&field_name);
            tiledb_test_support::result_tile::init_coord_tile(
                cxx::SharedPtr::clone(&result_tile),
                schema,
                &field_name,
                data,
                offsets,
                dim_num,
            );
        } else {
            let column_validity =
                validity
                    .get(&validity_key)
                    .map(|v| v.as_ref())
                    .unwrap_or(unsafe {
                        std::slice::from_raw_parts::<u8>(
                            std::ptr::NonNull::<u8>::dangling().as_ptr(),
                            0,
                        )
                    });
            tiledb_test_support::result_tile::init_attr_tile(
                cxx::SharedPtr::clone(&result_tile),
                schema,
                &field_name,
                data,
                offsets,
                &column_validity,
            );
        }
    }

    Ok(PackagedResultTile {
        buffers: batch,
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
