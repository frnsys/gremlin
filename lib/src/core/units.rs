//! Defines a bunch of newtypes to distinguish
//! between quantities of different units, so we can
//! leverage the type system to make sure we're not
//! mixing up units or forgetting to do conversions.
//!
//! Aside from the compile-time unit checking these
//! are otherwise meant to function transparently as `f32`s,
//! with some additional features such as
//! handling unit conversions, e.g. dividing a unit A by unit B
//! yields a ratio unit A/B.

#![allow(non_camel_case_types)]

use std::{
    fmt::{Debug, Display, Formatter},
    marker::PhantomData,
};

use super::numeric::Numeric;
use serde::{Deserialize, Serialize};

/// Represents a base unit, e.g. "watts".
///
/// _Note_: `Deref` should *not* be implemented because we want
/// to be explicit and know when we're ignoring the associated
/// units. Instead you use `.value()` to access the raw value.
pub trait Unit: Debug + From<f32> {
    /// The abbreviation for this unit, e.g. "W".
    fn abbrev() -> String;

    /// The abbreviation for this unit, e.g. "W".
    fn units(&self) -> String {
        Self::abbrev()
    }

    /// Return the raw value of this quantity.
    fn value(&self) -> f32;

    /// Return a mutable reference to the value of this quantity.
    fn value_mut(&mut self) -> &mut f32;
}

/// Implement all the traits necessary for to implement [`Numeric`].
#[doc(hidden)]
#[macro_export]
macro_rules! impl_numeric {
    ($type:ty$(, $($bounds:tt)*)?) => {
        impl<$($($bounds)*)?> Clone for $type {
            fn clone(&self) -> Self {
                *self
            }
        }
        impl<$($($bounds)*)?> Copy for $type {}

        impl<$($($bounds)*)?> PartialEq for $type {
            fn eq(&self, other: &Self) -> bool {
                self.value() == other.value()
            }
        }
        impl<$($($bounds)*)?> PartialOrd for $type {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl<$($($bounds)*)?> Eq for $type {}
        impl<$($($bounds)*)?> Ord for $type {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.value().total_cmp(&other.value())
            }
        }

        impl<$($($bounds)*)?> std::ops::Add for $type {
            type Output = $type;
            fn add(self, rhs: $type) -> Self::Output {
                Self::new(self.value() + rhs.value())
            }
        }
        impl<$($($bounds)*)?> std::ops::AddAssign for $type {
            fn add_assign(&mut self, rhs: $type) {
                *self.value_mut() += rhs.value();
            }
        }
        impl<$($($bounds)*)?> std::ops::Sub for $type {
            type Output = $type;
            fn sub(self, rhs: $type) -> Self::Output {
                Self::new(self.value() - rhs.value())
            }
        }
        impl<$($($bounds)*)?> std::ops::SubAssign for $type {
            fn sub_assign(&mut self, rhs: $type) {
                *self.value_mut() -= rhs.value();
            }
        }
        impl<$($($bounds)*)?> std::ops::Mul<f32> for $type {
            type Output = $type;
            fn mul(self, rhs: f32) -> Self::Output {
                Self::new(self.value() * rhs)
            }
        }
        impl<$($($bounds)*)?> std::ops::MulAssign<f32> for $type {
            fn mul_assign(&mut self, rhs: f32) {
                *self.value_mut() *= rhs;
            }
        }
        impl<$($($bounds)*)?> std::ops::Div<f32> for $type {
            type Output = $type;
            fn div(self, rhs: f32) -> Self::Output {
                Self::new(self.value() / rhs)
            }
        }
        impl<$($($bounds)*)?> std::ops::DivAssign<f32> for $type {
            fn div_assign(&mut self, rhs: f32) {
                *self.value_mut() /= rhs;
            }
        }
        impl<$($($bounds)*)?> std::ops::Div for $type {
            type Output = f32;
            fn div(self, rhs: $type) -> Self::Output {
                self.value() / rhs.value()
            }
        }
        impl<$($($bounds)*)?> std::iter::Sum for $type {
            fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
                iter.fold(Self::default(), |a, b| a + b)
            }
        }

        impl<$($($bounds)*)?> std::ops::Mul<Percent> for $type {
            type Output = $type;
            fn mul(self, rhs: Percent) -> Self::Output {
                Self::new(self.value() * rhs.value())
            }
        }
        impl<$($($bounds)*)?> std::ops::Mul<&Percent> for $type {
            type Output = $type;
            fn mul(self, rhs: &Percent) -> Self::Output {
                Self::new(self.value() * rhs.value())
            }
        }

        impl<$($($bounds)*)?> Default for $type{
            fn default() -> Self {
                Self::new(0.)
            }
        }

        impl<$($($bounds)*)?> From<&$type> for f32 {
            fn from(value: &$type) -> f32 {
                value.value()
            }
        }
        impl<$($($bounds)*)?> From<f32> for $type {
            fn from(value: f32) -> $type {
                Self::new(value)
            }
        }
        impl<$($($bounds)*)?> From<$type> for f32 {
            fn from(value: $type) -> f32 {
                value.value()
            }
        }

        impl<$($($bounds)*)?> std::str::FromStr for $type {
            type Err = std::num::ParseFloatError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse().map(|val| Self::new(val))
            }
        }


        impl<$($($bounds)*)?> serde::Serialize for $type {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_f32(self.value())
            }
        }

        impl<'de, $($($bounds)*)?> serde::Deserialize<'de> for $type {
            fn deserialize<De>(deserializer: De) -> Result<Self, De::Error>
            where
                De: serde::Deserializer<'de>,
            {
                let value: f32 = f32::deserialize(deserializer)?;
                Ok(Self::new(value))
            }
        }

        impl<$($($bounds)*)?> Numeric for $type {}
    }
}

/// A ratio of two units, e.g. meters per second (`m/s`).
/// We assume the denominator is always 1.
#[derive(Debug)]
pub struct Ratio<N: Unit, D: Unit> {
    value: f32,
    marker: PhantomData<(N, D)>,
}
impl_numeric!(Ratio<N, D>, N: Unit, D: Unit);
impl<N: Unit, D: Unit> Display for Ratio<N, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.value, Self::abbrev())
    }
}
impl<N: Unit, D: Unit> Ratio<N, D> {
    fn new(value: f32) -> Self {
        Self {
            value,
            marker: PhantomData,
        }
    }
}
impl<N: Unit, D: Unit> Unit for Ratio<N, D> {
    fn abbrev() -> String {
        format!("{}/{}", N::abbrev(), D::abbrev())
    }

    fn value(&self) -> f32 {
        self.value
    }

    fn value_mut(&mut self) -> &mut f32 {
        &mut self.value
    }
}

/// A product of two units, e.g. watt-hours (`Wh`).
#[derive(Debug)]
pub struct Product<N: Unit, M: Unit> {
    value: f32,
    marker: PhantomData<(N, M)>,
}
impl_numeric!(Product<N, M>, N: Unit, M: Unit);
impl<N: Unit, M: Unit> Display for Product<N, M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.value, Self::abbrev())
    }
}
impl<N: Unit, M: Unit> Product<N, M> {
    fn new(value: f32) -> Self {
        Self {
            value,
            marker: PhantomData,
        }
    }
}
impl<N: Unit, M: Unit> Unit for Product<N, M> {
    fn abbrev() -> String {
        format!("{}{}", N::abbrev(), M::abbrev())
    }

    fn value(&self) -> f32 {
        self.value
    }

    fn value_mut(&mut self) -> &mut f32 {
        &mut self.value
    }
}

/// Implement [`Div`](std::ops::Div) so that we can do `A / B -> A/B`,
/// i.e. create ratios by dividing base units,
/// and other ops that result in conflicting implementations when using
/// generics.
///
/// This is hacky and less than ideal but so far seems to be the only
/// way to do this. Ideally we just have a blanket implementation
/// of:
///
/// ```
/// impl<M: LocalUnit, $($($bounds)*)?> std::ops::Div<M> for $type {
///     type Output = Ratio<$type, M>;
///     fn div(self, rhs: M) -> Self::Output {
///         (self.value() / rhs.value()).into()
///     }
/// }
/// ```
///
/// However we also define `std::ops::Div<Self>` and because
/// `Self: LocalUnit` this is considered a conflicting implementation.
/// The `std::ops::Div<Self>` implementation is important because it's a
/// special case; a unit divided by another quantity of the same unit yields
/// a unitless result, whereas a unit divided by a quantity of a *different* unit
/// yields a ratio unit.
///
/// Again, with negative traits, negative trait bounds, or specialization
/// this might become possible. Ultimately we want to do something like:
///
/// ```
/// impl<M: LocalUnit, $($($bounds)*)?> std::ops::Div<M> for $type where M != $type {
/// ```
///
/// The cleanest way that requires the least of the downstream user is to have
/// a proc macro to, when using `define_units!`, have a separate
/// concrete implementation for each pair of units, e.g. if we're defining
/// units `A, B, C` then we have `impl std::ops::Div<B> for A`,
/// `impl std::ops::Div<C> for A`, etc, and for the identity case,
/// i.e. `impl std::ops::Div<A> for A`, which is the conflicting implementation,
/// we skip it. The [`non_identity_pairs!`] proc macro handles this looping part,
/// and then this macro is what's given as the impl block for each non-identity type pair.
#[doc(hidden)]
#[macro_export]
macro_rules! impl_concrete_ops {
    ($a:ty, $b:ty $(, $($bounds:tt)*)?) => {
        /// A / B -> A/B
        impl<$($($bounds)*)?> std::ops::Div<$b> for $a {
            type Output = Ratio<$a, $b>;
            fn div(self, rhs: $b) -> Self::Output {
                (self.value() / rhs.value()).into()
            }
        }

        /// A/BC * B -> A/C
        impl<A: BaseUnit, $($($bounds)*)?> std::ops::Mul<$a>
            for Ratio<A, Product<$a, $b>>
        {
            type Output = Ratio<A, $b>;
            fn mul(self, rhs: $a) -> Self::Output {
                (self.value() * rhs.value()).into()
            }
        }

        /// A/BC * C -> A/B
        impl<A: BaseUnit, $($($bounds)*)?> std::ops::Mul<$b>
            for Ratio<A, Product<$a, $b>>
        {
            type Output = Ratio<A, $a>;
            fn mul(self, rhs: $b) -> Self::Output {
                (self.value() * rhs.value()).into()
            }
        }
    };
}

/// Other unit-specific trait implementations.
#[doc(hidden)]
#[macro_export]
macro_rules! impl_ops {
    ($type:ty$(, $($bounds:tt)*)?) => {
        /// `Product` from two units,
        /// i.e. `A * B = AB`.
        ///
        /// See [`LocalUnit`] for why we use that as a trait bound here,
        /// rather than [`Unit`].
        impl<M: LocalUnit, $($($bounds)*)?> std::ops::Mul<M> for $type {
            type Output = Product<$type, M>;
            fn mul(self, rhs: M) -> Self::Output {
                (self.value() * rhs.value()).into()
            }
        }

        // NOTE: This doesn't actually work currently,
        // see note for [`impl_concrete_ops!`].
        // `Ratio` from two units,
        // i.e. `A / B = A/B`.
        //
        // See [`LocalUnit`] for why we use that as a trait bound here,
        // rather than [`Unit`].
        // impl<M: LocalUnit, $($($bounds)*)?> std::ops::Div<M> for $type {
        //     type Output = Ratio<$type, M>;
        //     fn div(self, rhs: M) -> Self::Output {
        //         (self.value() / rhs.value()).into()
        //     }
        // }

        impl<$($($bounds)*)?> std::fmt::Debug for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "{}{}",
                    self.value(),
                    Self::abbrev(),
                )
            }
        }

        impl<$($($bounds)*)?> std::fmt::Display for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "{}{}",
                    self.value(),
                    Self::abbrev(),
                )
            }
        }
    }
}

/// Use this trait to distinguish base units,
/// i.e. simple units, i.e. non-compound units
/// (i.e. not a `Product` or `Ratio`).
pub trait BaseUnit: Unit {}

/// Recover a base unit by dividing a product-unit by another base unit,
/// i.e. `AB/B -> A`.
impl<M: Unit, N: Unit> std::ops::Div<M> for Product<N, M> {
    type Output = N;
    fn div(self, rhs: M) -> Self::Output {
        (self.value() / rhs.value()).into()
    }
}

/// Canceling out the denominator of an `Ratio`,
/// i.e. `A/B * B = A`.
impl<N: BaseUnit, D: BaseUnit> std::ops::Mul<D> for Ratio<N, D> {
    type Output = N;
    fn mul(self, rhs: D) -> Self::Output {
        (self.value() * rhs.value()).into()
    }
}

// Handling some more specific cases.
// TODO Perhaps use a macro to implement a bunch of these?
/// A/BC * BC -> A
impl<A: BaseUnit, B: BaseUnit, C: BaseUnit> std::ops::Mul<Product<B, C>>
    for Ratio<A, Product<B, C>>
{
    type Output = A;
    fn mul(self, rhs: Product<B, C>) -> Self::Output {
        (self.value() * rhs.value()).into()
    }
}

// NOTE: These two implementations conflict,
// so this is actually implemented for concrete types
// in the [`impl_concrete_ops!`] macro.
//
// A/BC * B -> A/C
// impl<A: BaseUnit, B: BaseUnit, C: BaseUnit> std::ops::Mul<B>
//     for Ratio<A, Product<B, C>>
// {
//     type Output = Ratio<A, C>;
//     fn mul(self, rhs: B) -> Self::Output {
//         (self.value() * rhs.value()).into()
//     }
// }
// A/BC * C -> A/B
// impl<A: BaseUnit, B: BaseUnit, C: BaseUnit> std::ops::Mul<C>
//     for Ratio<A, Product<B, C>>
// {
//     type Output = Ratio<A, B>;
//     fn mul(self, rhs: B) -> Self::Output {
//         (self.value() * rhs.value()).into()
//     }
// }

/// A/BC * D/A -> D/BC
impl<A: BaseUnit, B: BaseUnit, C: BaseUnit, D: BaseUnit>
    std::ops::Mul<Ratio<D, A>> for Ratio<A, Product<B, C>>
{
    type Output = Ratio<D, Product<B, C>>;
    fn mul(self, rhs: Ratio<D, A>) -> Self::Output {
        (self.value() * rhs.value()).into()
    }
}

/// Multiplying two complementary `Ratio`s,
/// i.e. `A/B * B/C = A/C`.
impl<U: Unit, U0: Unit, U1: Unit> std::ops::Mul<Ratio<U, U1>>
    for Ratio<U0, U>
{
    type Output = Ratio<U0, U1>;
    fn mul(self, rhs: Ratio<U, U1>) -> Self::Output {
        (self.value() * rhs.value()).into()
    }
}

/// Represents a SI prefix (e.g. kilo, mega, giga, etc).
pub trait Prefix: Debug {
    /// The abbreviation for this prefix, e.g. `kilo -> k`.
    fn prefix() -> &'static str;

    /// The factor this prefix represents, e.g. `kilo -> 1e3`.
    fn factor() -> f32;
}

/// We treat the empty type `()` as no-prefix.
impl Prefix for () {
    fn prefix() -> &'static str {
        ""
    }
    fn factor() -> f32 {
        1e0
    }
}

/// Define common SI prefixes.
macro_rules! define_prefixes {
    ( $($key:ident:$prefix:ident:$factor:literal),* $(,)? ) => {
        $(
            #[doc = concat!("SI prefix representing a factor of ", stringify!($factor), ".")]
            #[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
            pub struct $key;
            impl Prefix for $key {
                fn prefix() -> &'static str {
                    stringify!($prefix)
                }
                fn factor() -> f32 {
                    $factor
                }
            }
        )*
    }
}

define_prefixes! {
    Exa:E:1e18,
    Peta:P:1e15,
    Tera:T:1e12,
    Giga:G:1e9,
    Mega:M:1e6,
    Kilo:k:1e3,
    Hecto:h:1e2,
    Deca:da:1e1,
    Deci:d:1e-1,
    Centi:c:1e-2,
    Milli:m:1e-3,
    Micro:u:1e-6,
    Nano:n:1e-9,
    Pico:p:1e-12,
    Femto:f:1e-15,
}

/// Define different units and implement a variety of useful traits
/// so they can be handled similarly to a regular `f32`.
///
/// Usage:
///
/// ```
/// define_units! {
///     NonSI {
///         Years:y,
///         Hours:h,
///         Dollars:$,
///     },
///     SI {
///         Watts:W,
///         Meters:m,
///     }
/// }
/// ```
#[macro_export]
macro_rules! define_units {
    (
        NonSI { $( $name_a:ident : $label_a:tt ),* $(, )? },
        SI { $( $name_b:ident : $label_b:tt ),* $(, )? }
    ) => {
        /// This trait exists as a hack for operator overloading.
        ///
        /// Ideally we could just overload e.g. `std::ops::Mul` but
        /// we basically run into this issue: <https://github.com/rust-lang/rfcs/issues/2758>,
        /// i.e. the compiler forbids this implementation as, in theory, `f32` could implement
        /// `Unit` at any point in the future (here filling in for the `M` generic),
        /// which conflicts with the other implementation of `std::ops::Mul<f32>`. In practice,
        /// as a commenter notes in that issue, I could make any change and break any number of
        /// things, so unclear why this one in particular is strictly disallowed.
        ///
        /// Features like negative trait bounds, negative trait implementations and negative trait
        /// coherence, and specialization should in theory allow this to work, e.g. we do
        /// `impl !Unit for f32`, but none of these features are mature enough to be used.
        ///
        /// Instead what seems to work is the (hacky) use of a trait local to the downstream crate.
        /// The conflict only occurs when a downstream crate uses the `Unit` trait bound,
        /// but not when the defining crate uses it. So we create this `LocalUnit` trait
        /// as part of the [`define_units!`] macro, so that the trait bound is using a local
        /// trait, and thus the compiler won't complain.
        pub trait LocalUnit: Unit {}

        // Non-SI units
        $(
            pub struct $name_a(f32);
            impl $name_a {
                pub fn new(value: f32) -> Self {
                    Self(value)
                }
            }

            impl Unit for $name_a {
                fn abbrev() -> String {
                    stringify!($label_a).into()
                }

                fn value(&self) -> f32 {
                    self.0
                }

                fn value_mut(&mut self) -> &mut f32 {
                    &mut self.0
                }
            }

            impl LocalUnit for $name_a {}
            impl BaseUnit for $name_a {}

            $crate::impl_numeric!($name_a);
            $crate::impl_ops!($name_a);
        )*

        // SI units
        $(
            pub struct $name_b<P: Prefix> {
                value: f32,
                marker: std::marker::PhantomData<P>,
            }
            impl<P: Prefix> Unit for $name_b<P> {
                fn abbrev() -> String {
                    format!("{}{}", P::prefix(), stringify!($label_b))
                }

                /// Get the raw value; if doing this you should
                /// be sure that the prefix is what you expect.
                fn value(&self) -> f32 {
                    self.value
                }

                fn value_mut(&mut self) -> &mut f32 {
                    &mut self.value
                }
            }

            impl<P: Prefix> LocalUnit for $name_b<P> {}
            impl<P: Prefix> BaseUnit for $name_b<P> {}

            impl<P: Prefix> $name_b<P> {
                fn new(value: f32) -> Self {
                    Self {
                        value,
                        marker: std::marker::PhantomData,
                    }
                }

                pub fn value(&self) -> f32 {
                    self.value
                }
            }

            impl<P: Prefix> $name_b<P> {
                /// Get this as a value, converted to
                /// its no-prefix equivalent.
                fn no_prefix(&self) -> f32 {
                    self.to::<()>().value
                }

                /// Convert this value to another SI prefix.
                pub fn to<O: Prefix>(&self) -> $name_b<O> {
                    let value = self.value * P::factor() / O::factor();
                    $name_b::<O>::new(value)
                }

                fn total_cmp(&self, other: &Self) -> std::cmp::Ordering {
                    self.no_prefix().total_cmp(&other.no_prefix())
                }

                pub fn units(&self) -> String {
                    Self::abbrev()
                }
            }

            impl<P: Prefix> SIUnit for $name_b<P> {
                type Prefix = P;
            }

            impl<P: Prefix, O: Prefix> ConvertPrefix<O> for $name_b<P> {
                type Output = $name_b<O>;
            }

            $crate::impl_numeric!($name_b<P>, P: Prefix);
            $crate::impl_ops!($name_b<P>, P: Prefix);
        )*

        $crate::non_identity_pairs!([$($name_a,)* $($name_b<P: Prefix>,)*] $crate::impl_concrete_ops);
    };
}

/// This helper trait is really only necessary so that
/// conversions of SI prefixes can be correctly
/// expressed in the type system.
///
/// In particular this allows us to express the prefix
/// as an associated type (e.g. `Watts<Mega>::Prefix`)
/// rather than as a generic (e.g. just `Watts<Mega>`).
///
/// Note that the expectation here is that the generic
/// prefix and the associated prefix are the same,
/// e.g. `Watts<Mega>::Prefix == Mega` but there is
/// actually nothing preventing one from implementing
/// `impl SIUnit for Watts<Mega>` where `SIUnit::Prefix`
/// is actually e.g. `Kilo`. But we assume that units
/// will be implemented through the provided macro which
/// only implements matching prefixes.
pub trait SIUnit: Unit {
    type Prefix: Prefix;
}

/// Trait indicating that an SI unit of one prefix
/// can be converted to another prefix.
///
/// The source prefix is known via the implementing type,
/// e.g. with `impl ConvertPrefix<Kilo> for Watts<Mega>`
/// we know the conversion is from `Mega -> Kilo`.
///
/// The associated type is for indicating the correct
/// return type after the conversion. In theory this
/// should be constrained to be the same base type,
/// e.g. implementing for `Watts<P>` will always
/// have an associated `Output` of `Watts<O>`,
/// but in practice there's no hard constraint.
pub trait ConvertPrefix<O: Prefix> {
    type Output: SIUnit<Prefix = O>;
}

impl<N: SIUnit, D: Unit> Ratio<N, D> {
    /// Change the prefix of a base-unit numerator.
    /// E.g. `MW/$ -> kW/$`
    pub fn num_to<O: Prefix>(&self) -> Ratio<N::Output, D>
    where
        N: ConvertPrefix<O>,
    {
        let factor = N::Prefix::factor() / O::factor();
        Ratio {
            value: self.value * factor,
            marker: std::marker::PhantomData,
        }
    }
}

impl<N: Unit, D: SIUnit> Ratio<N, D> {
    /// Change the prefix of a base-unit denominator.
    /// E.g. `$/MW -> $/kW`
    pub fn den_to<O: Prefix>(&self) -> Ratio<N, D::Output>
    where
        D: ConvertPrefix<O>,
    {
        let factor = O::factor() / D::Prefix::factor();
        Ratio {
            value: self.value * factor,
            marker: std::marker::PhantomData,
        }
    }
}

impl<U: SIUnit, N: Unit, M: Unit> Ratio<Product<U, M>, N> {
    /// Change the prefix of a product-unit numerator.
    /// E.g. `MWh/$ -> kWh/$`
    pub fn num_to<O: Prefix>(&self) -> Ratio<Product<U::Output, M>, N>
    where
        U: ConvertPrefix<O>,
    {
        let factor = U::Prefix::factor() / O::factor();
        Ratio {
            value: self.value * factor,
            marker: std::marker::PhantomData,
        }
    }
}

impl<N: Unit, M: Unit, U: SIUnit> Ratio<N, Product<U, M>> {
    /// Change the prefix of a product-unit denominator.
    /// E.g. `$/MWh -> $/kWh`
    pub fn den_to<O: Prefix>(&self) -> Ratio<N, Product<U::Output, M>>
    where
        U: ConvertPrefix<O>,
    {
        let factor = O::factor() / U::Prefix::factor();
        Ratio {
            value: self.value * factor,
            marker: std::marker::PhantomData,
        }
    }
}

impl<U: SIUnit, M: Unit> Product<U, M> {
    /// Convert the product to different a SI prefix.
    pub fn to<O: Prefix>(&self) -> Product<U::Output, M>
    where
        U: ConvertPrefix<O>,
    {
        let factor = U::Prefix::factor() / O::factor();
        Product {
            value: self.value * factor,
            marker: std::marker::PhantomData,
        }
    }
}

/// A type representing a percentage.
///
/// Because of its ubiquity and utility, percentages
/// are provided by the crate. This does *not*
/// implement `Unit` as technically it's not a unit (rather
/// it's a ratio), and fundamentally it is not much different
/// from a regular `f32`. The purpose is semantics:
/// It's for when you want to be explicit about
/// requiring or working with percentages.
#[derive(Debug)]
pub struct Percent(f32);
impl Percent {
    pub fn new(value: f32) -> Self {
        Self(value)
    }

    pub fn value(&self) -> f32 {
        self.0
    }

    pub fn value_mut(&mut self) -> &mut f32 {
        &mut self.0
    }
}
impl std::ops::Mul<Percent> for f32 {
    type Output = f32;
    fn mul(self, rhs: Percent) -> Self::Output {
        rhs.value() * self
    }
}
impl Display for Percent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}%", self.0 * 100.)
    }
}
impl_numeric!(Percent);

#[cfg(test)]
mod tests {
    use super::*;

    define_units! {
        NonSI {
            Years:y,
            Hours:h,
            Dollars:$,
        },
        SI {
            Watts:W,
            TCO2eq:TCO2eq,
        }
    }

    // Base units
    type kW = Watts<Kilo>;
    type MW = Watts<Mega>;
    type GW = Watts<Giga>;
    type TW = Watts<Tera>;

    // Simple ratios
    type DollarsPerWatt<P> = Ratio<Dollars, Watts<P>>;
    type d_kW = DollarsPerWatt<Kilo>;
    type d_MW = DollarsPerWatt<Mega>;
    type d_GW = DollarsPerWatt<Giga>;
    type d_TW = DollarsPerWatt<Tera>;

    type WattsPerDollar<P> = Ratio<Watts<P>, Dollars>;
    type kW_d = WattsPerDollar<Kilo>;
    type MW_d = WattsPerDollar<Mega>;
    type GW_d = WattsPerDollar<Giga>;
    type TW_d = WattsPerDollar<Tera>;

    // Simple products
    type WattHours<P> = Product<Watts<P>, Hours>;
    type kWh = WattHours<Kilo>;
    type MWh = WattHours<Mega>;
    type GWh = WattHours<Giga>;
    type TWh = WattHours<Tera>;

    // Compound ratios
    type DollarsPerWattHour<P> = Ratio<Dollars, WattHours<P>>;
    type d_kWh = DollarsPerWattHour<Kilo>;
    type d_MWh = DollarsPerWattHour<Mega>;
    type d_GWh = DollarsPerWattHour<Giga>;
    type d_TWh = DollarsPerWattHour<Tera>;

    type WattHoursPerDollar<P> = Ratio<WattHours<P>, Dollars>;
    type kWh_d = WattHoursPerDollar<Kilo>;
    type MWh_d = WattHoursPerDollar<Mega>;
    type GWh_d = WattHoursPerDollar<Giga>;
    type TWh_d = WattHoursPerDollar<Tera>;

    #[test]
    fn test_unit_display() {
        // Base
        let val = MW::from(123.);
        assert_eq!(val.to_string(), "123MW");

        // Products
        let val = MWh::from(123.);
        assert_eq!(val.to_string(), "123MWh");

        let val = kWh::from(123.);
        assert_eq!(val.to_string(), "123kWh");

        // Ratios
        let val = d_MWh::from(123.);
        assert_eq!(val.to_string(), "123$/MWh");

        let val = GWh_d::from(123.);
        assert_eq!(val.to_string(), "123GWh/$");
    }

    #[test]
    fn test_base_unit_conversions() {
        let mw = MW::from(1.);
        assert_eq!(mw.to::<Kilo>(), kW::from(1000.));
        assert_eq!(mw.to::<Giga>(), GW::from(0.001));
        assert_eq!(mw.to::<Tera>(), TW::from(0.000001));

        let kw = kW::from(1000.);
        assert_eq!(kw.to::<Mega>(), MW::from(1.));
        assert_eq!(kw.to::<Giga>(), GW::from(0.001));
        assert_eq!(kw.to::<Tera>(), TW::from(0.000001));
    }

    #[test]
    fn test_product_unit_conversions() {
        let mwh = MWh::from(1.);
        assert_eq!(mwh.to::<Kilo>(), kWh::from(1000.));
        assert_eq!(mwh.to::<Giga>(), GWh::from(0.001));
        assert_eq!(mwh.to::<Tera>(), TWh::from(0.000001));

        let kwh = kWh::from(1000.);
        assert_eq!(kwh.to::<Mega>(), MWh::from(1.));
        assert_eq!(kwh.to::<Giga>(), GWh::from(0.001));
        assert_eq!(kwh.to::<Tera>(), TWh::from(0.000001));
    }

    #[test]
    fn test_ratio_unit_conversions() {
        let d_mw = d_MW::from(1.);
        assert_eq!(d_mw.den_to::<Kilo>(), d_kW::from(0.001));
        assert_eq!(d_mw.den_to::<Giga>(), d_GW::from(1000.));
        assert_eq!(d_mw.den_to::<Tera>(), d_TW::from(1000000.));

        let mw_d = MW_d::from(1.);
        assert_eq!(mw_d.num_to::<Kilo>(), kW_d::from(1000.));
        assert_eq!(mw_d.num_to::<Giga>(), GW_d::from(0.001));
        assert_eq!(mw_d.num_to::<Tera>(), TW_d::from(0.000001));
    }

    #[test]
    fn test_compound_ratio_unit_conversions() {
        let d_mwh = d_MWh::from(1.);
        assert_eq!(d_mwh.den_to::<Kilo>(), d_kWh::from(0.001));
        assert_eq!(d_mwh.den_to::<Giga>(), d_GWh::from(1000.));
        assert_eq!(d_mwh.den_to::<Tera>(), d_TWh::from(1000000.));

        let mwh_d = MWh_d::from(1.);
        assert_eq!(mwh_d.num_to::<Kilo>(), kWh_d::from(1000.));
        assert_eq!(mwh_d.num_to::<Giga>(), GWh_d::from(0.001));
        assert_eq!(mwh_d.num_to::<Tera>(), TWh_d::from(0.000001));
    }

    #[test]
    fn test_unit_arithmetic() {
        // A * B -> AB
        let mw = MW::from(100.);
        let mwh = mw * Hours::from(2.);
        assert_eq!(mwh.to_string(), "200MWh");

        // A / B -> A/B
        let d = Dollars::from(100.);
        let d_mw = d / MW::from(2.);
        assert_eq!(d_mw.to_string(), "50$/MW");

        // AB / B -> A
        let mw = mwh / Hours::from(2.);
        assert_eq!(mw.to_string(), "100MW");

        // A/B * B -> A
        let d_kw = d_kW::from(10.);
        let d = d_kw * kW::from(5.);
        assert_eq!(d.to_string(), "50$");

        // A * f32 -> A
        let d_kw = d_kW::from(10.);
        let d_kw = d_kw * 2.;
        assert_eq!(d_kw.to_string(), "20$/kW");

        // A / A -> f32
        let mw = MW::from(10.);
        let f = mw / MW::from(2.);
        assert_eq!(f.to_string(), "5");

        // A/B * B/C -> A/C
        let d_kw = d_kW::from(10.);
        let kw_h = Ratio::<Watts<Kilo>, Hours>::from(5.);
        let r = d_kw * kw_h;
        assert_eq!(r.to_string(), "50$/h");
    }
}
