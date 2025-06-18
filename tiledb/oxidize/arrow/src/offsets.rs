//! Provides definitions for converting TileDB offsets into Arrow [OffsetBuffer]s.
//!
//! For `N` cells, an Arrow [OffsetBuffer] contains `N + 1` offsets, whose units
//! are elements of the accompanying values.
//!
//! TileDB offsets do not abide the same semantics.
//! In some situations `N` cells have `N + 1` offsets, like Arrow; but in other
//! situations `N` cells have `N` offsets, with the length of the accompanying
//! values buffer implicity used as the `N + 1`th offset.
//! And in all situations, TileDB offset units are bytes, not elements.
//!
//! As a result the functions in this module always must allocate new memory
//! for the [OffsetBuffer].

use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};

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

/// Constructs an [OffsetBuffer] from a raw byte slice.
///
/// This function reinterprets the raw bytes as `i64` offsets.
/// It returns an error if the reinterpreted bytes do not constitute
/// valid offsets for elements of size `value_size` or are otherwise
/// invalid for an [OffsetBuffer].
///
/// If the input `bytes` constitutes `N` offsets then the output [OffsetBuffer]
/// also contains `N` offsets.
pub fn try_from_bytes(value_size: usize, bytes: &[u8]) -> Result<OffsetBuffer<i64>, Error> {
    let (prefix, offsets, suffix) = unsafe {
        // SAFETY: transmuting u8 to i64 is safe and we will check error below
        bytes.align_to::<i64>()
    };
    if !prefix.is_empty() || !suffix.is_empty() {
        return Err(Error::UnalignedOffsets);
    }

    try_new(value_size, offsets.iter().copied())
}

/// Constructs an [OffsetBuffer] from a raw byte slice and the total number of values.
///
/// This function reinterprets the raw bytes as `i64` offsets.
/// It returns an error if the reinterpreted bytes do not constitute
/// valid offsets for elements of size `value_size` or are otherwise invalid
/// for an [OffsetBuffer].
///
/// This function can be used for offsets where the number of offsets is equal
/// to the number of cells, and the length of the final cell is determined
/// by the total number of values. An [OffsetBuffer] instead expresses that
/// final offset in its buffer, such that the length of a cell `i` is
/// always determined by `offsets[i + 1] - offsets[i]`.
///
/// As such, if the input buffer constitutes `N` offsets, the output
/// [OffsetBuffer] will contain `N + 1` offsets.
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

/// Constructs an [OffsetBuffer] from a stream of offsets whose unit
/// is bytes. The resulting [OffsetBuffer] unit is elements whose
/// size is given by `value_size`.
///
/// If the input [Iterator] produces `N` elements then the returned
/// [OffsetBuffer] contains `N` offsets.
pub fn try_new(
    value_size: usize,
    raw_offsets: impl Iterator<Item = i64>,
) -> Result<OffsetBuffer<i64>, Error> {
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
        Buffer::try_from_trusted_len_iter(raw_offsets.map(try_element_offset))?
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
