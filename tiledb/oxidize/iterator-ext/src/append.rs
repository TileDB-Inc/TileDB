use std::iter::FusedIterator;

/// An iterator that extends any iterator with a single element.
///
/// This `struct` is created by [IteratorExt::append].
pub struct Append<I>
where
    I: Iterator,
{
    a: Option<I>,
    b: Option<I::Item>,
}

impl<I> Append<I>
where
    I: Iterator,
{
    pub fn new(a: I, b: I::Item) -> Self {
        Self {
            a: Some(a),
            b: Some(b),
        }
    }
}

impl<I> Iterator for Append<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(a) = self.a.as_mut().and_then(|a| a.next()) {
            return Some(a);
        }
        if self.a.is_some() {
            self.a = None;
        }
        self.b.take()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Append {
                a: Some(a),
                b: Some(_),
            } => {
                let (lb, ub) = a.size_hint();
                (lb + 1, ub.map(|ub| ub + 1))
            }
            Append {
                a: Some(_),
                b: None,
            } => {
                unreachable!("Invalid internal state for `Append`")
            }
            Append {
                a: None,
                b: Some(_),
            } => (1, Some(1)),
            Append { a: None, b: None } => (0, Some(0)),
        }
    }
}

impl<I> ExactSizeIterator for Append<I> where I: ExactSizeIterator {}

impl<I> FusedIterator for Append<I> where I: Iterator {}

#[cfg(test)]
mod tests {
    use crate::IteratorExt;

    #[test]
    fn empty() {
        let empty: Vec<u64> = Vec::new();
        let mut append = empty.into_iter().append(100);

        assert_eq!(1, append.len());
        assert_eq!(Some(100), append.next());

        assert_eq!(0, append.len());
        assert!(append.next().is_none());

        assert_eq!(0, append.len());
        assert!(append.next().is_none());
    }

    #[test]
    fn nonempty() {
        let mut append = (0..3).append(4);

        assert_eq!(4, append.len());
        assert_eq!(Some(0), append.next());

        assert_eq!(3, append.len());
        assert_eq!(Some(1), append.next());

        assert_eq!(2, append.len());
        assert_eq!(Some(2), append.next());

        assert_eq!(1, append.len());
        assert_eq!(Some(4), append.next());

        assert_eq!(0, append.len());
        assert!(append.next().is_none());

        assert_eq!(0, append.len());
        assert!(append.next().is_none());
    }
}
