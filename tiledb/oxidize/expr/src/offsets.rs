use datafusion::common::arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Offsets index a non-integral cell")]
    NonIntegralOffsets,
    #[error("Offsets are not aligned to i64")]
    UnalignedOffsets,
    #[error("Invalid negative offset: {0}")]
    NegativeOffset(i64),
    #[error("Non-descending offsets for variable length data found at cell {0}: {1}, {2}")]
    DescendingOffsets(usize, i64, i64),
}

pub fn try_from_bytes(value_size: usize, bytes: &[u8]) -> Result<OffsetBuffer<i64>, Error> {
    let (prefix, offsets, suffix) = unsafe {
        // SAFETY: transmuting u8 to i64 is safe
        bytes.align_to::<i64>()
    };
    if !prefix.is_empty() || !suffix.is_empty() {
        return Err(Error::UnalignedOffsets);
    }

    try_new(value_size, offsets.iter().copied())
}

pub fn try_from_bytes_and_num_values(
    value_size: usize,
    bytes: &[u8],
    num_values: usize,
) -> Result<OffsetBuffer<i64>, Error> {
    let (prefix, offsets, suffix) = unsafe {
        // SAFETY: transmuting u8 to i64 is safe
        bytes.align_to::<i64>()
    };
    if !prefix.is_empty() || !suffix.is_empty() {
        return Err(Error::UnalignedOffsets);
    }

    try_new(
        value_size,
        offsets
            .iter()
            .copied()
            .chain(std::iter::once(num_values as i64)),
    )
}

pub fn try_new(
    value_size: usize,
    raw_offsets: impl Iterator<Item = i64>,
) -> Result<OffsetBuffer<i64>, Error> {
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

    let value_size = value_size as i64; // arrow offsets are signed for some reason

    let try_element_offset = |o: i64| {
        if o % value_size == 0 {
            Ok(o / value_size)
        } else {
            Err(Error::NonIntegralOffsets)
        }
    };

    let sbuf = ScalarBuffer::<i64>::from(unsafe {
        // SAFETY: slice has a trusted upper length
        Buffer::try_from_trusted_len_iter(raw_offsets.map(|o| try_element_offset(o)))?
    });
    if sbuf[0] < 0 {
        return Err(Error::NegativeOffset(sbuf[0]));
    } else if !sbuf.windows(2).all(|w| w[0] <= w[1]) {
        for (i, w) in sbuf.windows(2).enumerate() {
            if w[0] > w[1] {
                return Err(Error::DescendingOffsets(i, w[0], w[1]));
            }
        }
        // SAFETY: `all` was false, therefore `any` is true
        unreachable!()
    }

    Ok(OffsetBuffer::<i64>::new(sbuf))
}
