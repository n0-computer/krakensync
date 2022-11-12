use std::{iter::FusedIterator, marker::PhantomData};

/// Transform an iterator like filter_map, but additionally perform limit calculations.
///
/// Limit is called on each item and can either skip items `Ok(None)`, emit items `Ok(Some(item))`, or
/// terminate the iteration with an error value `Err(cause)` that will be the last element of the resulting
/// iterator.
///
/// The function is a `FnMut` so it can decrement some limits and terminate once they are exceeded
pub fn limit<I, R, F>(iter: I, f: F) -> Limit<I, F, R>
where
    I: Iterator,
    F: FnMut(I::Item) -> std::result::Result<Option<R>, R>,
{
    Limit {
        iter: Some(iter),
        f,
        _r: PhantomData,
    }
}

pub struct Limit<I, F, R> {
    iter: Option<I>,
    f: F,
    _r: PhantomData<R>,
}

impl<I, F, R> Iterator for Limit<I, F, R>
where
    I: Iterator,
    F: FnMut(I::Item) -> std::result::Result<Option<R>, R>,
{
    type Item = R;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.iter.as_mut()?.next()?;
            match (self.f)(item) {
                Ok(Some(r)) => {
                    // limit not reached, return the result
                    break Some(r);
                }
                Ok(None) => {
                    // limit not reached, skip the item
                    continue;
                }
                Err(r) => {
                    // limit reached, stop iterating and return limit marker
                    self.iter = None;
                    break Some(r);
                }
            }
        }
    }
}

impl<I, F, R> FusedIterator for Limit<I, F, R>
where
    I: Iterator,
    F: FnMut(I::Item) -> std::result::Result<Option<R>, R>,
{
}
