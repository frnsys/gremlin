//! A special sequential type representing open-ended, potentially non-contiguous time series.
//!
//! This is different than the [`Array`] type which represents fixed-length contiguous series.
//!
//! For non-contiguous series the fill and extend behaviors have to be defined.
//!
//! Here "fill" means how intermediate values are interpolated (between known values)
//! and "extend" means how values before and after the first and last values are determined.
//! For the "extend" case the lack of an additional value means interpolation can't happen.

use std::{
    collections::BTreeMap,
    ops::{Index, IndexMut},
};

use serde::{ser::SerializeStruct, Serializer};

use super::Numeric;

// serialization type
// key type

pub enum Lerp {
    Linear,
    Forward,
}

pub trait Interval {
    type Key: Ord + Copy;

    fn serialize<N: Numeric, S: Serializer>(
        serializer: S,
        key: Self::Key,
        value: N,
    ) -> Result<S::Ok, S::Error>;
}

pub struct Annual;
impl Interval for Annual {
    type Key = u16;

    fn serialize<N: Numeric, S: Serializer>(
        serializer: S,
        key: Self::Key,
        value: N,
    ) -> Result<S::Ok, S::Error> {
        let mut state =
            serializer.serialize_struct("Record", 2)?;
        state.serialize_field("year", &key)?;
        state.serialize_field("value", &value)?;
        state.end()
    }
}

#[derive(Debug, Default, Clone)]
pub struct TimeSeries<N: Numeric, I: Interval> {
    data: BTreeMap<I::Key, N>,
    lerp: Option<Vec<N>>,
}
impl<N: Numeric, I: Interval> TimeSeries<N, I> {
    pub fn new(mut series: Vec<(I::Key, N)>) -> Self {
        series.sort_by_key(|(key, _)| *key);
        todo!()
        // self.data.sort()
    }

    // pub fn min_key(&self) -> Option<&N> {
    //     self.data.keys().min()
    // }

    /// Iterate over *known* values only.
    /// As such this is not guaranteed to be contiguous!
    pub fn values_known(&self) -> impl Iterator<Item = &N> {
        self.data.values()
    }

    /// Iterate over contiguous values,
    /// with unknown values interpolated.
    // pub fn values_lerped(&self) -> impl Iterator<Item = &N> {
    //     // TODO
    //     let vals = [N::default()];
    //     vals.iter()
    // }

    pub fn iter_known(
        &self,
    ) -> impl Iterator<Item = (&I::Key, &N)> {
        self.data.iter()
    }

    // pub fn iter_lerped(&self) -> impl Iterator<Item = &N> {
    //     // TODO
    //     [N::default()].iter()
    // }

    /// The number of known values.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Get the minimum known element.
    pub fn min(&self) -> N {
        self.values_known()
            .map(|val| (*val).into())
            .fold(f32::INFINITY, |a, b| a.min(b))
            .into()
    }

    /// Get the maximum known element.
    pub fn max(&self) -> N {
        self.values_known()
            .map(|val| (*val).into())
            .fold(-f32::INFINITY, |a, b| a.max(b))
            .into()
    }

    /// Get the mean of known values.
    pub fn mean(&self) -> N {
        self.values_known().copied().sum::<N>()
            / self.len() as f32
    }
}
