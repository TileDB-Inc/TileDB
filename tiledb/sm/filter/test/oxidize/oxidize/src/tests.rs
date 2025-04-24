use proptest::prelude::*;

use super::*;

fn instance_pipeline_65154(data: &[i32]) -> anyhow::Result<()> {
    let pipeline = build_pipeline_65154();
    let as_bytes = unsafe { std::mem::transmute::<&[i32], &[u8]>(data) };
    Ok(filter_pipeline_roundtrip(&pipeline, as_bytes)?)
}

proptest! {
    #[test]
    fn proptest_pipeline_65154(
        data in proptest::collection::vec(any::<i32>(), 0..=1024*1024)
    ) {
        instance_pipeline_65154(&data)
            .expect("Error in instance_pipeline_65154");
    }
}
