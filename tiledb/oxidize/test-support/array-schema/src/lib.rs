pub mod enums;

use itertools::Itertools;
use tiledb_common::dimension_constraints_go;
use tiledb_cxx_interface::sm::array_schema::{ArraySchema, Attribute, Dimension, Domain};
use tiledb_pod::array::schema::{AttributeData, DimensionData, DomainData, SchemaData};

pub fn schema_from_pod(pod: &SchemaData) -> anyhow::Result<cxx::SharedPtr<ArraySchema>> {
    let domain = domain_from_pod(&pod.domain)?;

    let mut schema = tiledb_test_support::array_schema::new_array_schema(
        enums::convert_array_type(pod.array_type),
        tiledb_test_support::get_test_memory_tracker(),
    );

    // NB: a lot of this looks kind of weird and that's because `Pin<&mut T>` is kind
    // of weird. `Pin` does not impl `Copy` so we must call `as_mut()` to copy the
    // pointer for each method receiver.
    {
        let Some(mut schema) = schema.as_mut() else {
            unreachable!()
        };

        schema.as_mut().set_domain(domain)?;

        pod.attributes
            .iter()
            .map(attribute_from_pod)
            .process_results(|attrs| {
                attrs.for_each(|attr| {
                    schema.as_mut().add_attribute(
                        tiledb_test_support::array_schema::as_const_attribute(attr),
                        false,
                    )
                })
            })?;

        if let Some(tile_order) = pod.tile_order {
            schema
                .as_mut()
                .set_tile_order(enums::convert_tile_order(tile_order));
        }
        if let Some(cell_order) = pod.cell_order {
            schema
                .as_mut()
                .set_cell_order(enums::convert_cell_order(cell_order));
        }
        if let Some(capacity) = pod.capacity {
            schema.as_mut().set_capacity(capacity);
        }
        if let Some(allow_dups) = pod.allow_duplicates {
            schema.as_mut().set_allows_dups(allow_dups);
        }

        // enumerations
        // coords filters
        // offsets filters
        // validity filters
    }

    Ok(tiledb_test_support::array_schema::array_schema_to_shared(
        schema,
    ))
}

pub fn domain_from_pod(pod: &DomainData) -> anyhow::Result<cxx::SharedPtr<Domain>> {
    let mut d = tiledb_test_support::array_schema::new_domain(
        tiledb_test_support::get_test_memory_tracker(),
    );

    {
        let Some(mut d) = d.as_mut() else {
            unreachable!();
        };
        pod.dimension
            .iter()
            .map(dimension_from_pod)
            .process_results(move |dims| dims.for_each(move |dim| d.as_mut().add_dimension(dim)))?;
    }

    Ok(tiledb_test_support::array_schema::domain_to_shared(d))
}

pub fn dimension_from_pod(pod: &DimensionData) -> anyhow::Result<cxx::SharedPtr<Dimension>> {
    cxx::let_cxx_string!(name = &pod.name);

    let mut dimension = tiledb_test_support::array_schema::new_dimension(
        &name,
        enums::convert_datatype(pod.datatype),
        tiledb_test_support::get_test_memory_tracker(),
    );

    {
        dimension_constraints_go!(
            pod.constraints,
            DT,
            [lower_bound, upper_bound],
            extent,
            {
                dimension
                    .pin_mut()
                    .set_domain::<DT>(lower_bound, upper_bound)?;
                if let Some(extent) = extent {
                    dimension.pin_mut().set_tile_extent::<DT>(extent)?;
                }
            },
            {
                // do nothing
            }
        );

        // filters
    }

    Ok(tiledb_test_support::array_schema::dimension_to_shared(
        dimension,
    ))
}

pub fn attribute_from_pod(pod: &AttributeData) -> anyhow::Result<cxx::SharedPtr<Attribute>> {
    cxx::let_cxx_string!(name = &pod.name);

    let mut attribute = tiledb_test_support::array_schema::new_attribute(
        &name,
        enums::convert_datatype(pod.datatype),
        pod.nullability.unwrap_or(false),
    );

    {
        let Some(attribute) = attribute.as_mut() else {
            unreachable!()
        };

        if let Some(cvn) = pod.cell_val_num {
            attribute.set_cell_val_num(u32::from(enums::convert_cell_val_num(cvn)));
        }

        // fill value
        // filters
        // enumeration
    }

    Ok(tiledb_test_support::array_schema::attribute_to_shared(
        attribute,
    ))
}
