mod append;

/// Provides extra adaptors and methods for an [Iterator].
pub trait IteratorExt: Iterator {
    /// Appends a single additional value to the iterator sequence.
    ///
    /// The returned [Iterator] yields all of the values from `self`,
    /// and then yields `value`.
    ///
    /// This is a specialization of [Iterator::chain] for chaining
    /// a single value, which implements [ExactSizeIterator].
    fn append(self, value: Self::Item) -> append::Append<Self>
    where
        Self: Sized,
    {
        append::Append::new(self, value)
    }
}

impl<I> IteratorExt for I where I: Iterator {}
