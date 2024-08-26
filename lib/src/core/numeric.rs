//! Defines a trait which represents a type that can
//! effectively be handled as if it were a `f32`.
//! Primarily used so that sequence types can take SI units
//! just as easily as they would a `f32`.

use std::{
    fmt::{Debug, Display},
    iter::Sum,
    ops::{
        Add,
        AddAssign,
        Div,
        DivAssign,
        Mul,
        MulAssign,
        Sub,
        SubAssign,
    },
    str::FromStr,
};

use serde::{de::DeserializeOwned, Serialize};

/// Any type that can effectively be treated
/// as a `f32` should implement this trait,
/// while respecting the rules of units,
/// which means that a few operations are *NOT*
/// included:
///
/// - `Mul` and `MulAssign`
///     because these operations are *NOT* equivalent when using units,
///     e.g. `10m * 2m != 20m` but rather `20m^2`.
/// - `DivAssign` because self-division does *NOT* preserve units,
///     e.g. `10m/2m != 5m` but rather just `5`.
///     However `Div` is ok but requires that it return a unitless `f32`,
///     i.e. only `Div<Output = f32`.
/// - `Add<f32>` and `Sub<f32>` (and `AddAssign<f32>` and `SubAssign<f32>`)
///     because we want to be strict about ensuring only the same types can be
///     added/subtracted (to enforce units).
pub trait Numeric:
    Sum
    + Add<Output = Self>
    + AddAssign
    + Sub<Output = Self>
    + SubAssign
    + Mul<f32, Output = Self>
    + MulAssign<f32>
    + Div<f32, Output = Self>
    + DivAssign<f32>
    + Div<Output = f32>
    + PartialEq
    + PartialOrd
    + From<f32>
    + Into<f32>
    + Clone
    + Copy
    + Default
    + Display
    + Debug
    + FromStr
    + Serialize
    + DeserializeOwned
{
    fn min(&self, other: &Self) -> Self {
        if self <= other {
            *self
        } else {
            *other
        }
    }

    fn max(&self, other: &Self) -> Self {
        if self >= other {
            *self
        } else {
            *other
        }
    }
}

impl Numeric for f32 {}
