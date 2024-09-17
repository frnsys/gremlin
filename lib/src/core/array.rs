use std::{
    iter::Sum,
    ops::{Add, AddAssign, Div, DivAssign, Index, IndexMut, Mul, MulAssign, Sub, SubAssign},
    path::Path,
    str::FromStr,
};

use derive_more::Deref;
use serde::{
    de::{self, DeserializeOwned},
    Deserialize, Deserializer, Serialize, Serializer,
};

use super::numeric::Numeric;

/// Fixed-length numeric sequence, i.e. an array,
/// but with some additional functionality where
/// they can be treated a bit more like n-d vectors.
///
/// You probably won't interact with this type directly
/// but rather through one of its pre-defined aliases
/// that represent fixed time intervals, e.g. [`ByDayHour`](super::ByDayHour).
///
/// Note that these arrays can be deserialized from a single value,
/// which splats the value. Similarly, when serializing, if the array is all
/// the same value, then it's serialized as a single value.
#[derive(Debug, Clone, PartialEq, Deref, Deserialize)]
#[serde(transparent)]
#[serde(bound(
    serialize = "N: Serialize",
    deserialize = "N: DeserializeOwned + Clone"
))]
pub struct Array<N, const U: usize>(
    #[serde(deserialize_with = "deserialize_splat_array")] Box<[N; U]>,
);
impl<N, const U: usize> Array<N, U> {
    pub fn new(vals: [N; U]) -> Self {
        Self(Box::new(vals))
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &N> {
        self.0.iter()
    }

    pub fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = &mut N> {
        self.0.iter_mut()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into<M>(self) -> Array<M, U>
    where
        M: From<N>,
    {
        let arr = self.0.map(|v| M::from(v));
        Array::new(arr)
    }

    /// Write this array to a CSV.
    pub fn to_csv<P: AsRef<Path>>(&self, column_name: &str, path: P) -> csv::Result<()>
    where
        N: ToString,
    {
        let mut w = csv::Writer::from_path(path)?;
        let headers = [column_name];
        w.write_record(headers)?;
        for value in self.iter() {
            w.write_record([value.to_string()])?;
        }
        w.flush()?;
        Ok(())
    }
}

impl<N: Numeric, const U: usize> AsRef<Self> for Array<N, U> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<N: Numeric, const U: usize> Array<N, U> {
    /// Get the minimum element.
    pub fn min(&self) -> N {
        self.iter()
            .map(|val| (*val).into())
            .fold(f32::INFINITY, |a, b| a.min(b))
            .into()
    }

    /// Get the maximum element.
    pub fn max(&self) -> N {
        self.iter()
            .map(|val| (*val).into())
            .fold(-f32::INFINITY, |a, b| a.max(b))
            .into()
    }

    /// Get the sum.
    pub fn sum(&self) -> N {
        self.iter().copied().sum::<N>()
    }

    /// Get the mean.
    pub fn mean(&self) -> N {
        self.iter().copied().sum::<N>() / self.len() as f32
    }

    /// Element-wise minimums.
    pub fn mins(&self, other: &Self) -> Self {
        let mut vals = [N::default(); U];
        for i in 0..U {
            vals[i] = self[i].min(&other[i]);
        }
        Self::new(vals)
    }

    /// Element-wise minimums.
    pub fn maxs(&self, other: &Self) -> Self {
        let mut vals = [N::default(); U];
        for i in 0..U {
            vals[i] = self[i].max(&other[i]);
        }
        Self::new(vals)
    }

    /// Limit all elements to a maximum value.
    pub fn clamp(&self, val: N) -> Self {
        let mut vals = [N::default(); U];
        for i in 0..U {
            vals[i] = self[i].max(&val);
        }
        Self::new(vals)
    }

    /// Distribute the provided value according to
    /// the provided distribution (which should sum to 1.0).
    pub fn distribute_value(val: N, dist: &Array<f32, U>) -> Array<N, U> {
        Self::new(dist.map(|p| val * p))
    }
}
impl<N: Default, const U: usize> Default for Array<N, U> {
    fn default() -> Self {
        let arr: [N; U] = std::array::from_fn(|_| N::default());
        Self::new(arr)
    }
}

impl<N: Clone, const U: usize> Array<N, U> {
    /// Create a new array from a single value.
    pub fn splat(val: N) -> Self {
        let arr: [N; U] = std::array::from_fn(|_| val.clone());
        Self::new(arr)
    }
}

impl<N, const U: usize> Index<usize> for Array<N, U> {
    type Output = N;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}
impl<N, const U: usize> IndexMut<usize> for Array<N, U> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

impl<N: Numeric, const U: usize> Array<N, U> {
    pub fn add_assign<A: AsRef<Array<N, U>>>(&mut self, rhs: A) {
        for (x, o) in self.0.iter_mut().zip(rhs.as_ref().0.iter()) {
            *x += *o;
        }
    }

    pub fn add<A: AsRef<Array<N, U>>>(mut self, rhs: A) -> Self {
        self += rhs.as_ref();
        self
    }

    pub fn sub_assign<A: AsRef<Array<N, U>>>(&mut self, rhs: A) {
        for (x, o) in self.0.iter_mut().zip(rhs.as_ref().0.iter()) {
            *x -= *o;
        }
    }

    pub fn sub<A: AsRef<Array<N, U>>>(mut self, rhs: A) -> Self {
        self -= rhs.as_ref();
        self
    }
}

macro_rules! impl_arithmetic {
    ($($target:ty => $rhs:ty),*) => {
        $(
            impl<N: Numeric, const U: usize> Add<$rhs> for $target {
                type Output = Array<N, U>;
                fn add(self, rhs: $rhs) -> Self::Output {
                    Array::add(self, rhs)
                }
            }

            impl<N: Numeric, const U: usize> AddAssign<$rhs> for $target {
                fn add_assign(&mut self, rhs: $rhs) {
                    Array::add_assign(self, rhs)
                }
            }

            impl<N: Numeric, const U: usize> Sub<$rhs> for $target {
                type Output = Array<N, U>;
                fn sub(self, rhs: $rhs) -> Self::Output {
                    Array::sub(self, rhs)
                }
            }

            impl<N: Numeric, const U: usize> SubAssign<$rhs> for $target {
                fn sub_assign(&mut self, rhs: $rhs) {
                    Array::sub_assign(self, rhs)
                }
            }
        )*
    }
}
impl_arithmetic! {
    Array<N, U> => Array<N, U>,
    Array<N, U> => &Array<N, U>
}

macro_rules! impl_unit_arithmetic {
    ($($target:ty),*) => {
        $(
            impl<N: Numeric, const U: usize> Add<N> for $target {
                type Output = Array<N, U>;
                fn add(self, rhs: N) -> Self::Output {
                    let result = self.0.map(|v| v + rhs);
                    Self::Output::new(result)
                }
            }

            impl<N: Numeric, const U: usize> Sub<N> for $target {
                type Output = Array<N, U>;
                fn sub(self, rhs: N) -> Self::Output {
                    let result = self.0.map(|v| v - rhs);
                    Self::Output::new(result)
                }
            }

            impl<N: Numeric, const U: usize> Mul<f32> for $target {
                type Output = Array<N, U>;
                fn mul(self, rhs: f32) -> Self::Output {
                    let result = self.0.map(|v| v * rhs);
                    Self::Output::new(result)
                }
            }
            impl<N: Numeric, const U: usize> Div<f32> for $target {
                type Output = Array<N, U>;
                fn div(self, rhs: f32) -> Self::Output {
                    let result = self.0.map(|v| v / rhs);
                    Self::Output::new(result)
                }
            }
        )*
    }
}
impl_unit_arithmetic! {
    Array<N, U>,
    &Array<N, U>
}
macro_rules! impl_mut_unit_arithmetic {
    ($($target:ty),*) => {
        $(
            impl<N: Numeric, const U: usize> SubAssign<N> for $target {
                fn sub_assign(&mut self, rhs: N) {
                    for val in self.0.iter_mut() {
                        *val -= rhs;
                    }
                }
            }

            impl<N: Numeric, const U: usize> AddAssign<N> for $target {
                fn add_assign(&mut self, rhs: N) {
                    for val in self.0.iter_mut() {
                        *val += rhs;
                    }
                }
            }

            impl<N: Numeric, const U: usize> MulAssign<f32> for $target {
                fn mul_assign(&mut self, rhs: f32) {
                    for val in self.0.iter_mut() {
                        *val *= rhs;
                    }
                }
            }

            impl<N: Numeric, const U: usize> DivAssign<f32> for $target {
                fn div_assign(&mut self, rhs: f32) {
                    for val in self.0.iter_mut() {
                        *val /= rhs;
                    }
                }
            }
        )*
    }
}
impl_mut_unit_arithmetic! {
    Array<N, U>,
    &mut Array<N, U>
}

impl<N: Numeric, const U: usize> Sum for Array<N, U> {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        let mut ret = Self::default();
        for other in iter {
            ret += other;
        }
        ret
    }
}

impl<N, const U: usize> FromIterator<N> for Array<N, U> {
    fn from_iter<I: IntoIterator<Item = N>>(iter: I) -> Self {
        let v: Vec<_> = iter.into_iter().collect();
        Array::from(v)
    }
}
impl<N, const U: usize> From<Vec<N>> for Array<N, U> {
    fn from(value: Vec<N>) -> Self {
        let Ok(arr): Result<[N; U], _> = value.try_into() else {
            panic!("Vec should be of length {U}")
        };
        Self::new(arr)
    }
}

// Can't work because of lack of specialization?
// impl<N: Numeric, const U: usize> Into<Array<N, U>>
//     for Array<f32, U>
// where
//     N: From<f32>,
// {
//     fn into(self) -> Array<N, U> {
//         let arr = self.0.map(|v| N::from(v));
//         Array(arr)
//     }
// }

impl<N: Numeric, const U: usize> FromStr for Array<N, U> {
    type Err = <f32 as FromStr>::Err;
    /// Deserialize from a comma-delimited string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let vals = s
            .split(',')
            .map(|v| v.parse::<f32>())
            .collect::<Result<Vec<f32>, _>>()?;

        // Can't seem to do `Vec::try_into` as
        // it's not implemented for arrays of generic size?
        if vals.len() != U {
            panic!("Expected {U} values but got {}", vals.len());
        }
        let mut arr = [N::default(); U];
        for i in 0..U {
            arr[i] = vals[i].into();
        }
        Ok(Self::new(arr))
    }
}
impl<N: Numeric, const U: usize> std::fmt::Display for Array<N, U> {
    /// Serialize as a comma-delimited string.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let as_str = self
            .iter()
            .map(|val| val.to_string())
            .collect::<Vec<_>>()
            .join(",");
        write!(f, "{}", as_str)
    }
}

/// Deserialize an array from either a single value, which is splatted,
/// or the full array.
fn deserialize_splat_array<'de, D, N, const U: usize>(
    deserializer: D,
) -> Result<Box<[N; U]>, D::Error>
where
    D: Deserializer<'de>,
    N: Clone + DeserializeOwned,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ArrayInner<N> {
        Splat(N),
        Array(Vec<N>),
    }

    let inner: ArrayInner<N> = Deserialize::deserialize(deserializer)?;

    Ok(match inner {
        ArrayInner::Splat(val) => {
            let arr: [N; U] = std::array::from_fn(|_| val.clone());
            Box::new(arr)
        }
        ArrayInner::Array(arr) => {
            let n = arr.len();
            let arr: [N; U] = arr
                .try_into()
                .map_err(|_| de::Error::invalid_length(n, &format!("length of {U}").as_str()))?;
            Box::new(arr)
        }
    })
}

impl<N, const U: usize> Serialize for Array<N, U>
where
    N: Serialize + PartialEq + Clone,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let first = &self.0[0];
        if self.0.iter().all(|x| x == first) {
            // Serialize as a single value if all elements are the same
            first.serialize(serializer)
        } else {
            // Serialize as an array otherwise
            self.0.as_ref().serialize(serializer)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_min_max_mean() {
        let arr = Array::new([1., 2., 3.]);
        assert_eq!(arr.min(), 1.);
        assert_eq!(arr.max(), 3.);
        assert_eq!(arr.mean(), 2.);
    }

    #[test]
    fn test_array_f32_ops() {
        let arr = Array::new([1., 2., 3.]);
        assert_eq!(&arr - 1., Array::new([0., 1., 2.]));
        assert_eq!(&arr + 1., Array::new([2., 3., 4.]));
        assert_eq!(&arr * 2., Array::new([2., 4., 6.]));
        assert_eq!(&arr / 2., Array::new([0.5, 1.0, 1.5]));
    }

    #[test]
    fn test_array_array_ops() {
        let a = Array::new([1., 2., 3.]);
        let b = Array::new([4., 5., 6.]);
        assert_eq!(a.clone() + &b, Array::new([5., 7., 9.]));
        assert_eq!(a.clone() - &b, Array::new([-3., -3., -3.]));
    }

    #[test]
    fn test_array_deserialize() {
        let raw = "[1, 2, 3, 4]";
        let arr: Array<f32, 4> = serde_yaml::from_str(raw).unwrap();
        assert_eq!(arr, Array::new([1., 2., 3., 4.]));

        let raw = "1";
        let arr: Array<f32, 4> = serde_yaml::from_str(raw).unwrap();
        assert_eq!(arr, Array::splat(1.));
    }

    #[test]
    fn test_array_serialize() {
        let arr = Array::new([1., 2., 3., 4.]);
        let raw = serde_json::to_string(&arr).unwrap();
        assert_eq!(raw, "[1.0,2.0,3.0,4.0]");

        let arr: Array<f32, 4> = Array::splat(1.);
        let raw = serde_json::to_string(&arr).unwrap();
        assert_eq!(raw, "1.0");
    }
}
