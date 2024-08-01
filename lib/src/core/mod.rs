use derive_more::{Deref, DerefMut};

mod array;
mod numeric;
mod series;
mod units;
mod var;

pub use array::Array;
use enum_map::{Enum, EnumArray, EnumMap};
use extend::ext;
pub use numeric::Numeric;
pub use units::*;

pub type ByDayHour<N> = Array<N, 24>;

/// An array with one element per hour of the year.
/// We assume no leap years, so a year is always 8760 hours.
#[derive(Deref, DerefMut)]
pub struct ByYearHour<N: Numeric>(Array<N, 8760>); // HourlyYearSeries
impl<N: Numeric> ByYearHour<N> {
    /// Repeat a day across the year.
    pub fn from_day(value: [N; 24]) -> Self {
        let values: [N; 8760] = std::iter::repeat(value)
            .take(365)
            .flatten()
            .collect::<Vec<_>>()
            .try_into()
            .expect("Will have the correct amount of entries");
        Self(Array::new(values))
    }

    /// Calculate the sum of values for each hour of the day.
    /// This will give a `ByDayHour` where the first entry is
    /// the sum of all values for 0h, the second is the same for
    /// 1h, etc.
    pub fn sum_day_hours(&self) -> ByDayHour<N> {
        let mut sums = [N::default(); 24];
        for day in self.0.chunks(24) {
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
}

#[ext(name=EnumMapExt)]
pub impl<K: Enum + EnumArray<V>, V: Copy> EnumMap<K, V> {
    fn splat(v: V) -> Self {
        EnumMap::from_fn(|_| v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mean_day_hours() {
        // TODO
    }
}
