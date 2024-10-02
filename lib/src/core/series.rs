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
    marker::PhantomData,
    ops::{Add, Deref},
    path::Path,
};

use chrono::{Datelike, Duration, NaiveDateTime, Timelike};
use generic_array::{functional::FunctionalSequence, ArrayLength, GenericArray};
use itertools::{traits::HomogeneousTuple, Itertools};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use typenum::{Add1, B1, U};

use super::{Array, Numeric};

pub enum Lerp {
    Linear,
    Forward,
}

/// An interval defines the time unit that each
/// record in a [`TimeSeries`] represents.
///
/// This implementation runs up against some
/// limitations of current Rust, but should feel
/// more ergonomic to the end-user.
///
/// You have to define a `Key` type, which should
/// be a tuple of `u16`s, e.g. for a time unit
/// delimited by years & hours, this would be `(u16, u16)`.
/// Tuples are used because they're more convenient for
/// unpacking while iterating.
///
/// You also have to define `KeySize` which should match
/// the length of the `Key` tuple, using `typenum`'s `U`
/// type, e.g. for the previous example you'd use `U<2>`.
///
/// Finally, the [`Interval::key_columns`] method should return the
/// names of each tuple element. For the previous example
/// you'd use something like `["year", "hour"].into()`.
/// This is used to name columns when writing to CSVs
/// using [`TimeSeries::to_csv`].
pub trait Interval {
    type KeySize: ArrayLength + Add<B1>;
    type Key: Into<GenericArray<u16, Self::KeySize>> + Copy + HomogeneousTuple<Item = u16>;
    fn key_columns() -> GenericArray<&'static str, Self::KeySize>;
    fn as_datetime(key: &Self::Key) -> NaiveDateTime;
}

type Key<I> = GenericArray<u16, <I as Interval>::KeySize>;

/// An annual interval for a time series.
pub struct Annual;
impl Interval for Annual {
    type KeySize = U<1>;
    type Key = (u16,);

    fn key_columns() -> GenericArray<&'static str, Self::KeySize> {
        ["year"].into()
    }

    fn as_datetime(key: &Self::Key) -> NaiveDateTime {
        let dt = NaiveDateTime::default();
        dt.with_year(key.0 as i32).expect("Is a valid year")
    }
}

/// An hourly interval for a time series.
pub struct Hourly;
impl Interval for Hourly {
    type KeySize = U<1>;
    type Key = (u16,);

    fn key_columns() -> GenericArray<&'static str, Self::KeySize> {
        ["hour"].into()
    }

    fn as_datetime(key: &Self::Key) -> NaiveDateTime {
        let epoch = NaiveDateTime::default();
        let duration = Duration::hours(key.0 as i64);
        epoch + duration
    }
}

/// This time series is basically just a `Vec`
/// and provides no guarantees about data contiguity;
/// for contiguous data see [`FullTimeSeries`] instead.
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
#[serde(bound(serialize = "I::Key: Serialize, T: Serialize"))]
#[serde(bound(deserialize = "I::Key: DeserializeOwned, T: DeserializeOwned"))]
pub struct TimeSeries<T, I: Interval> {
    data: Vec<(I::Key, T)>,
    marker: PhantomData<I>,
}

impl<T, I: Interval> Deref for TimeSeries<T, I> {
    type Target = Vec<(I::Key, T)>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl<T, I: Interval> Default for TimeSeries<T, I> {
    fn default() -> Self {
        TimeSeries {
            data: vec![],
            marker: PhantomData,
        }
    }
}

impl<T, I: Interval, IT> From<IT> for TimeSeries<T, I>
where
    IT: Iterator<Item = (I::Key, T)>,
{
    fn from(value: IT) -> Self {
        TimeSeries {
            data: value.collect(),
            marker: PhantomData,
        }
    }
}
impl<T, I: Interval> TimeSeries<T, I> {
    pub fn iter(&self) -> impl Iterator<Item = &(I::Key, T)> {
        self.data.iter()
    }

    /// Write this time series to a CSV.
    /// The interval key columns will be used to name
    /// the columns, see [`Interval`].
    ///
    /// The value column will always be named "value".
    pub fn to_csv<P: AsRef<Path>>(&self, path: P) -> csv::Result<()>
    where
        T: ToString,
    {
        let mut w = csv::Writer::from_path(path)?;
        let headers = [I::key_columns().as_slice(), &["value"]].concat();
        w.write_record(headers)?;
        for (keys, value) in &self.data {
            let keys: Key<I> = (*keys).into();
            let mut row: Vec<String> = keys.map(|k| k.to_string()).to_vec();
            row.push(value.to_string());
            w.write_record(row)?;
        }
        w.flush()?;
        Ok(())
    }
}

impl<N: Clone, I: Interval, const U: usize> TimeSeries<Array<N, { U }>, I> {
    /// This takes a time series whose elements are [`Array<N>`]s
    /// and flattens it into a time series of `N`s.
    ///
    /// For example, say you have a `TimeSeries<ByDayHour<f32>, Annual>`,
    /// so each record represents a year and each year has 24 values, one
    /// for each hour of the day. So for `n` years you'll have `n` records,
    /// which are indexed by `(u16,)`, representing the year.
    ///
    /// You can flatten it into a time series where instead
    /// each record represents a year-hour, so instead of one record
    /// per year you now have 24, and thus `n*24` records in total.
    /// These would be indexed by `(u16, u16)`, representing the `(year, hour)`.
    pub fn flatten<J: Interval<KeySize = Add1<I::KeySize>>>(&self) -> TimeSeries<N, J>
    where
        J::KeySize: ArrayLength,
    {
        self.data
            .iter()
            .flat_map(|(key, arr)| {
                arr.iter().enumerate().map(|(i, val)| {
                    let key: Key<I> = (*key).into();
                    let key = [key.as_slice(), &[i as u16]].concat();
                    let key: J::Key = key.into_iter().collect_tuple().unwrap();
                    (key, val.clone())
                })
            })
            .collect()
    }
}

impl<T: Clone, I: Interval> TimeSeries<T, I> {
    pub fn as_ref_vec(&self) -> Vec<&T> {
        self.data.iter().map(|(_, val)| val).collect()
    }

    pub fn as_vec(&self) -> Vec<T> {
        self.data.iter().map(|(_, val)| val.clone()).collect()
    }
}
impl<T: Clone, I: Interval> Clone for TimeSeries<T, I> {
    fn clone(&self) -> Self {
        TimeSeries {
            data: self.data.clone(),
            marker: PhantomData,
        }
    }
}
impl<T: std::fmt::Debug, I: Interval> std::fmt::Debug for TimeSeries<T, I>
where
    I::Key: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TimeSeries<{}, {}> [{:?}]",
            std::any::type_name::<T>(),
            std::any::type_name::<I>(),
            self.data
        )
    }
}
impl<T: PartialEq, I: Interval> PartialEq for TimeSeries<T, I>
where
    I::Key: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl<T, I: Interval, K: Into<I::Key>> FromIterator<(K, T)> for TimeSeries<T, I> {
    fn from_iter<IT: IntoIterator<Item = (K, T)>>(iter: IT) -> Self {
        Self {
            data: iter.into_iter().map(|(k, v)| (k.into(), v)).collect(),
            marker: PhantomData,
        }
    }
}

pub type AnnualSeries<N> = TimeSeries<N, Annual>;
impl<N: Clone> AnnualSeries<N> {
    /// Get all values before the provided year (non-inclusive).
    pub fn before(&self, year: u16) -> AnnualSeries<N> {
        self.data
            .iter()
            .filter(|(y, _)| y.0 < year)
            .cloned()
            .collect()
    }

    /// Get all values after the provided year (non-inclusive).
    pub fn after(&self, year: u16) -> AnnualSeries<N> {
        self.data
            .iter()
            .filter(|(y, _)| y.0 > year)
            .cloned()
            .collect()
    }
}

/// A full time series is a time series that is
/// guaranteed to be continuous/contiguous, i.e.
/// missing values will be interpolated.
#[derive(Debug, Default, Clone)]
pub struct FullTimeSeries<N: Numeric, I: Interval> {
    data: BTreeMap<I::Key, N>,
    lerp: Option<Vec<N>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hourly_interval() {
        Hourly::as_datetime(&(128u16,));
    }
}
