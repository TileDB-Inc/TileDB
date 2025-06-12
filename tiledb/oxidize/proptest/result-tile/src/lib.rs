use std::ops::Deref;
use std::sync::Arc;

use arrow::array::{Array as ArrowArray, GenericListArray, PrimitiveArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use tiledb_oxidize::sm::array_schema::ArraySchema;
use tiledb_oxidize::sm::query::readers::ResultTile;
use tiledb_test_cells::{Cells, FieldData, typed_field_data_go};

/// Packages a `ResultTile` with the buffers which contain the tile data.
pub struct PackagedResultTile {
    /// Buffers underlying the [ResultTile].
    #[allow(dead_code)]
    buffers: RecordBatch,
    // NB: tile data borrows the record batch columns, this is for sure "unsafe"
    // but should be fine since the contents are behind an `Arc`
    result_tile: cxx::SharedPtr<ResultTile>,
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
    let result_tile = tiledb_test_support::result_tile::new_result_tile(
        tiledb_test_support::get_test_memory_tracker(),
    );

    let buffers = cells_to_record_batch(cells);

    for field in buffers.schema().fields.iter() {
        let column = {
            // SAFETY: guaranteed by `RecordBatch` contract
            buffers.column_by_name(field.name()).unwrap()
        };
        cxx::let_cxx_string! { field_name = field.name() };

        let column_data = column.to_data();
        assert_eq!(0, column_data.offset());
        assert_eq!(column.len(), column_data.len());

        assert_eq!(1, column_data.buffers().len());

        let (data, offsets) = if schema.var_size(&field_name) {
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
        } else {
            assert_eq!(0, column_data.child_data().len());
            (column_data.buffer::<u8>(0), unsafe {
                std::slice::from_raw_parts::<u64>(std::ptr::NonNull::<u64>::dangling().as_ptr(), 0)
            })
        };
        tiledb_test_support::result_tile::init_attr_tile(
            cxx::SharedPtr::clone(&result_tile),
            schema,
            &field_name,
            data,
            offsets,
        );
    }

    Ok(PackagedResultTile {
        buffers,
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
