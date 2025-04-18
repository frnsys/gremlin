//! Helpers for working with numbers and collections of numbers.

mod array;
mod numeric;
mod series;
mod units;
mod var;

use std::iter::Sum;

use ahash::HashMap;
pub use array::Array;
use enum_map::{Enum, EnumArray, EnumMap};
use extend::ext;
pub use generic_array::GenericArray;
pub use numeric::Numeric;
pub use series::*;
pub use typenum::U;
pub use units::*;
pub use var::*;

use crate::data::{IterExt, Row};

/// A fixed array of 24 elements.
pub type ByDayHour<N> = Array<N, 24>;
impl<N: Numeric> From<HashMap<u8, N>> for ByDayHour<N> {
    fn from(value: HashMap<u8, N>) -> Self {
        let mut val = ByDayHour::default();
        for (k, v) in value {
            val[k as usize] = v;
        }
        val
    }
}
impl<T: Copy> Row for ByDayHour<T>
where
    f32: From<T>,
{
    fn values(&self) -> Vec<f32> {
        let mut values: Vec<f32> = self.iter().map(|val| f32::from(*val)).collect();
        let mean = values.iter().mean();
        values.insert(0, mean);
        values
    }

    fn columns() -> Vec<String> {
        vec![
            "mean".into(),
            "00h".into(),
            "01h".into(),
            "02h".into(),
            "03h".into(),
            "04h".into(),
            "05h".into(),
            "06h".into(),
            "07h".into(),
            "08h".into(),
            "09h".into(),
            "10h".into(),
            "11h".into(),
            "12h".into(),
            "13h".into(),
            "14h".into(),
            "15h".into(),
            "16h".into(),
            "17h".into(),
            "18h".into(),
            "19h".into(),
            "20h".into(),
            "21h".into(),
            "22h".into(),
            "23h".into(),
        ]
    }

    fn facet(&self) -> String {
        String::new()
    }
}

/// An array with one element per hour of the year.
/// We assume no leap years, so a year is always 8760 hours.
pub type ByYearHour<N> = Array<N, 8760>;
impl<N: Numeric> ByYearHour<N> {
    /// Repeat a day across the year.
    pub fn from_day(value: [N; 24]) -> Self {
        let values: [N; 8760] = std::iter::repeat_n(value, 365)
            .flatten()
            .collect::<Vec<_>>()
            .try_into()
            .expect("Will have the correct amount of entries");
        Array::new(values)
    }

    /// Calculate the sum of values for each hour of the day.
    /// This will give a `ByDayHour` where the first entry is
    /// the sum of all values for 0h, the second is the same for
    /// 1h, etc.
    pub fn sum_day_hours(&self) -> ByDayHour<N> {
        let mut sums = [N::default(); 24];
        for day in self.chunks(24) {
            for (value, sum) in day.iter().zip(&mut sums) {
                *sum += *value;
            }
        }
        ByDayHour::new(sums)
    }

    /// Calculate the mean of values for each hour of the day.
    /// This will give a `ByDayHour` where the first entry is
    /// the mean of all values for 0h, the second is the same for
    /// 1h, etc.
    pub fn mean_day_hours(&self) -> ByDayHour<N> {
        let sums = self.sum_day_hours();
        sums / 365.
    }

    /// Iterate over values, but also get:
    ///
    /// - the hour of the year (0 to 8760; first in the tuple)
    /// - the hour of the day (0 to 24; second in the tuple)
    pub fn enumerate(&self) -> impl Iterator<Item = (usize, usize, &N)> {
        self.iter().enumerate().map(|(year_hour, val)| {
            let day_hour = year_hour % 24;
            (year_hour, day_hour, val)
        })
    }
}
impl<N: Numeric + for<'a> Sum<&'a N>> ByYearHour<N> {
    /// Merge day-hours into days by summing the values
    /// for each hour, resulting in 365 data points
    /// (one for each day of the year).
    pub fn sum_days(&self) -> Array<N, 365> {
        self.chunks(24).map(|day| day.iter().sum()).collect()
    }
}

// Just implement for `f32` so we don't have to specify `N`
// when using this method.
impl ByYearHour<f32> {
    /// Iterate over each hour of each day throughout a year.
    /// The first iterated value is the hour of the year (0 to 8760)
    /// and the second is the hour of the day (0 to 24).
    pub fn iter_hours() -> impl Iterator<Item = (usize, usize)> {
        let range = 0..24;
        let n = range.len();
        range.cycle().take(365 * n).enumerate()
    }
}

#[ext(name=EnumMapExt)]
pub impl<K: Enum + EnumArray<V>, V: Clone> EnumMap<K, V> {
    /// Create an enum map by repeating a single value.
    fn splat(v: V) -> Self {
        EnumMap::from_fn(|_| v.clone())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_mean_day_hours() {
        // TODO
    }
}
