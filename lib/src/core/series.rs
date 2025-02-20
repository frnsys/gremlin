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
    ops::{Deref, DerefMut},
    path::Path,
};

use chrono::{Datelike, Duration, NaiveDateTime};
use serde::{
    de::{self, DeserializeOwned},
    ser::SerializeMap,
    Deserialize, Deserializer, Serialize, Serializer,
};
use serde_json::{Map, Value};

use crate::file::{write_flat_csv, CsvError};

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
pub trait Interval: std::fmt::Debug + Clone + Default + Ord + Serialize + DeserializeOwned {
    fn as_datetime(row: &Self) -> NaiveDateTime;
}

#[derive(Debug)]
pub struct SeriesRow<I: Interval, T> {
    pub when: I,
    pub value: T,
}
impl<I: Interval, T: Default> Default for SeriesRow<I, T> {
    fn default() -> Self {
        Self {
            when: I::default(),
            value: T::default(),
        }
    }
}
impl<I: Interval, T: Clone> Clone for SeriesRow<I, T> {
    fn clone(&self) -> Self {
        Self {
            when: self.when.clone(),
            value: self.value.clone(),
        }
    }
}
impl<I: Interval, T: PartialEq> PartialEq for SeriesRow<I, T> {
    fn eq(&self, other: &Self) -> bool {
        other.when == other.when && other.value == other.value
    }
}

/// An annual interval for a time series.
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Annual {
    pub year: u16,
}
impl Interval for Annual {
    fn as_datetime(row: &Self) -> NaiveDateTime {
        let dt = NaiveDateTime::default();
        dt.with_year(row.year as i32).expect("Is a valid year")
    }
}

/// An hourly interval for a time series.
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Hourly {
    pub hour: u16,
}
impl Interval for Hourly {
    fn as_datetime(row: &Self) -> NaiveDateTime {
        let epoch = NaiveDateTime::default();
        let duration = Duration::hours(row.hour as i64);
        epoch + duration
    }
}

// Note: 1-indexed month.
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Monthly {
    pub year: u16,
    pub month: u16,
}
impl Interval for Monthly {
    fn as_datetime(row: &Self) -> NaiveDateTime {
        let dt = NaiveDateTime::default();
        dt.with_year(row.year as i32)
            .expect("Is a valid year")
            .with_month(row.month as u32)
            .expect("Is a valid month")
    }
}

/// This time series is basically just a `Vec`
/// and provides no guarantees about data contiguity;
/// for contiguous data see [`FullTimeSeries`] instead.
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
#[serde(bound(serialize = "I: Serialize, T: Serialize"))]
#[serde(bound(deserialize = "I: DeserializeOwned, T: DeserializeOwned"))]
pub struct TimeSeries<T, I: Interval> {
    rows: Vec<SeriesRow<I, T>>,
}
impl<T, I: Interval> TimeSeries<T, I> {
    pub fn new(rows: Vec<SeriesRow<I, T>>) -> Self {
        Self { rows }
    }
}

impl<T, I: Interval> Deref for TimeSeries<T, I> {
    type Target = Vec<SeriesRow<I, T>>;
    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}
impl<T, I: Interval> DerefMut for TimeSeries<T, I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rows
    }
}

impl<T, I: Interval> Default for TimeSeries<T, I> {
    fn default() -> Self {
        TimeSeries { rows: vec![] }
    }
}

impl<T: Default + Serialize, I: Interval> TimeSeries<T, I> {
    pub fn to_csv<P: AsRef<Path> + std::fmt::Debug>(&self, path: P) -> Result<(), CsvError> {
        write_flat_csv(path, &self.rows)
    }

    /// Sort the series by key.
    pub fn sort(&mut self) {
        self.rows.sort_by_key(|row| row.when.clone())
    }

    /// Get the minimum key of the series.
    pub fn min_key(&self) -> Option<&I> {
        self.rows
            .iter()
            .min_by_key(|row| &row.when)
            .map(|row| &row.when)
    }

    /// Get the maximum key of the series.
    pub fn max_key(&self) -> Option<&I> {
        self.rows
            .iter()
            .max_by_key(|row| &row.when)
            .map(|row| &row.when)
    }
}

impl<N: Clone, I: Interval, const U: usize> TimeSeries<Array<N, { U }>, I> {
    // This takes a time series whose elements are [`Array<N>`]s
    // and flattens it into a time series of `N`s.
    //
    // For example, say you have a `TimeSeries<ByDayHour<f32>, Annual>`,
    // so each record represents a year and each year has 24 values, one
    // for each hour of the day. So for `n` years you'll have `n` records,
    // which are indexed by `(u16,)`, representing the year.
    //
    // You can flatten it into a time series where instead
    // each record represents a year-hour, so instead of one record
    // per year you now have 24, and thus `n*24` records in total.
    // These would be indexed by `(u16, u16)`, representing the `(year, hour)`.
    pub fn flatten(&self) -> impl Iterator<Item = (usize, I, N)> + '_ {
        self.rows.iter().flat_map(|row| {
            row.value
                .iter()
                .enumerate()
                .map(|(i, value)| (i, row.when.clone(), value.clone()))
        })
    }
}

impl<T, I: Interval> IntoIterator for TimeSeries<T, I> {
    type Item = SeriesRow<I, T>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl<T, I: Interval> TimeSeries<T, I> {
    pub fn map<U>(&self, f: impl Fn(&T) -> U) -> TimeSeries<U, I> {
        self.rows
            .iter()
            .map(|row| SeriesRow {
                when: row.when.clone(),
                value: f(&row.value),
            })
            .collect()
    }
}

impl<T: Clone, I: Interval> TimeSeries<T, I> {
    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.rows.iter().map(|row| &row.value)
    }

    pub fn as_ref_vec(&self) -> Vec<&T> {
        self.rows.iter().map(|row| &row.value).collect()
    }

    pub fn as_vec(&self) -> Vec<T> {
        self.rows.iter().map(|row| row.value.clone()).collect()
    }
}
impl<T: Clone, I: Interval + Clone> Clone for TimeSeries<T, I> {
    fn clone(&self) -> Self {
        TimeSeries {
            rows: self.rows.clone(),
        }
    }
}
impl<T: std::fmt::Debug, I: Interval> std::fmt::Debug for TimeSeries<T, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TimeSeries<{}, {}> [{:?}]",
            std::any::type_name::<T>(),
            std::any::type_name::<I>(),
            self.rows
        )
    }
}
impl<T: PartialEq, I: Interval> PartialEq for TimeSeries<T, I> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl<T, I: Interval> FromIterator<SeriesRow<I, T>> for TimeSeries<T, I> {
    fn from_iter<IT: IntoIterator<Item = SeriesRow<I, T>>>(iter: IT) -> Self {
        Self {
            rows: iter.into_iter().collect(),
        }
    }
}

pub type MonthlySeries<N> = TimeSeries<N, Monthly>;

pub type AnnualSeries<N> = TimeSeries<N, Annual>;
impl<N: Clone> AnnualSeries<N> {
    /// Get all values before the provided year (non-inclusive).
    pub fn before(&self, year: u16) -> AnnualSeries<N> {
        self.rows
            .iter()
            .filter(|row| row.when.year < year)
            .cloned()
            .collect()
    }

    /// Get all values after the provided year (non-inclusive).
    pub fn after(&self, year: u16) -> AnnualSeries<N> {
        self.rows
            .iter()
            .filter(|row| row.when.year > year)
            .cloned()
            .collect()
    }
}
impl<T> FromIterator<(u16, T)> for AnnualSeries<T> {
    fn from_iter<IT: IntoIterator<Item = (u16, T)>>(iter: IT) -> Self {
        Self {
            rows: iter
                .into_iter()
                .map(|(year, value)| SeriesRow {
                    when: Annual { year },
                    value,
                })
                .collect(),
        }
    }
}

// TODO finish this?
/// A full time series is a time series that is
/// guaranteed to be continuous/contiguous, i.e.
/// missing values will be interpolated.
#[derive(Debug, Default, Clone)]
pub struct FullTimeSeries<N: Numeric, I: Interval> {
    #[allow(dead_code)]
    data: BTreeMap<I, N>,

    #[allow(dead_code)]
    lerp: Option<Vec<N>>,
}

// Custom serialization implementation which effectively
// flattens a `SeriesRow` in CSVs.
// We could tag the `when` and `value` fields with
// `#[serde(flatten)]` except that this fails
// if `value` is a primitive, e.g. `f32`.
impl<I, T> Serialize for SeriesRow<I, T>
where
    I: Interval + Serialize,
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let when_value = serde_json::to_value(&self.when).map_err(serde::ser::Error::custom)?;
        let value_value = serde_json::to_value(&self.value).map_err(serde::ser::Error::custom)?;

        let mut map = serializer.serialize_map(None)?;

        // Serialize `when` fields
        if let Value::Object(when_map) = when_value {
            for (k, v) in when_map {
                serialize_json_value::<S>(&mut map, &k, v)?;
            }
        } else {
            return Err(serde::ser::Error::custom(
                "Expected `when` to serialize to an object",
            ));
        }

        // Serialize `value` fields or primitives
        match value_value {
            Value::Object(value_map) => {
                for (k, v) in value_map {
                    serialize_json_value::<S>(&mut map, &k, v)?;
                }
            }
            primitive => {
                serialize_json_value::<S>(&mut map, "value", primitive)?;
            }
        }

        map.end()
    }
}

// Helper function to serialize `serde_json::Value` while preserving native types
fn serialize_json_value<S>(
    map: &mut S::SerializeMap,
    key: &str,
    value: Value,
) -> Result<(), S::Error>
where
    S: Serializer,
{
    match value {
        Value::String(s) => map.serialize_entry(key, &s),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                map.serialize_entry(key, &i)
            } else if let Some(u) = n.as_u64() {
                map.serialize_entry(key, &u)
            } else if let Some(f) = n.as_f64() {
                map.serialize_entry(key, &f)
            } else {
                Err(serde::ser::Error::custom("Invalid number format"))
            }
        }
        Value::Bool(b) => map.serialize_entry(key, &b),
        Value::Null => map.serialize_entry(key, &""),
        Value::Array(arr) => {
            let joined = arr
                .into_iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            map.serialize_entry(key, &joined)
        }
        Value::Object(_) => map.serialize_entry(key, &"[object]"), // Customize if needed
    }
}

// Custom Deserialize implementation
impl<'de, I, T> Deserialize<'de> for SeriesRow<I, T>
where
    I: Interval + DeserializeOwned,
    T: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map: Map<String, Value> = Map::deserialize(deserializer)?;

        // Attempt to split the fields between `when` and `value`
        let when_candidate = serde_json::from_value(Value::Object(map.clone()))
            .map_err(|_| de::Error::custom("Failed to deserialize `when`"))?;

        // Try to deserialize as a struct first
        let value_candidate: Result<T, _> = serde_json::from_value(Value::Object(map.clone()));

        let value = if let Ok(v) = value_candidate {
            v
        } else if let Some(primitive_value) = map.get("value") {
            serde_json::from_value(primitive_value.clone())
                .map_err(|_| de::Error::custom("Failed to deserialize primitive `value`"))?
        } else {
            return Err(de::Error::custom(
                "Missing `value` field for primitive types",
            ));
        };

        Ok(SeriesRow {
            when: when_candidate,
            value,
        })
    }
}
