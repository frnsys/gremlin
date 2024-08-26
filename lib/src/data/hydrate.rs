//! A hydrator define refinement processes that
//! hydrate [`FromPartial`] types, potentially combining
//! data from multiple sources.
//!
//! For example when initializing `Generator`s we may have
//! one source for their nameplate capacities, another source
//! for their capacity factors, another source for their costs, etc,
//! and we want to merge them all.
//!
//! A hydrator that produces a collection of `T` (where `T: FromPartial`)
//! takes an initial [`Source`] which is the data source
//! that initializes the population of `T` as well as 0 or more [`Tributaries`](Tributary),
//! which represent additional data sources which fill in fields in each instance of `T`.

use std::fmt::Debug;

use super::partial::{FromPartial, HydrateError};

/// A hydrator defines a means of producing a fully-hydrated `T`
/// from potentially several different data sources.
/// See [`Source`] and [`Tributary`].
#[derive(Debug)]
pub struct Hydrator<T>
where
    T: Debug + FromPartial,
{
    /// The [`Source`] produces the initial partial versions of `T`.
    pub source: Box<dyn Source<T>>,

    /// These define additional data sources that can fill in or overwrite
    /// additional fields in the partial versions of `T`.
    /// Note that the sequence here is important, as it's the order each
    /// [`Tributary`] is applied in, so later ones can override earlier ones.
    pub tributaries: Vec<Box<dyn Tributary<T>>>,
}
impl<T> Hydrator<T>
where
    T: Debug + FromPartial,
{
    /// Run this hydrator using the provided year as `t=0`.
    pub fn run(&self, start_year: u16) -> Result<Vec<T>, HydrateError> {
        let mut items = self.source.generate(start_year)?;
        for trib in &self.tributaries {
            trib.fill(&mut items)?;
        }

        // TODO impute
        items
            .into_iter()
            .map(|partial| T::from(partial))
            .collect::<Result<Vec<T>, HydrateError>>()
    }
}

/// A `Source` produces the initial population of partial `T`s.
/// See [`Hydrator`].
pub trait Source<T>: Debug
where
    T: FromPartial,
{
    fn generate(
        &self,
        start_year: u16,
    ) -> Result<Vec<T::Partial>, HydrateError>;
}

/// A `Tributary` fills in fields in the population of partial `T`s.
/// See [`Hydrator`].
pub trait Tributary<T>: Debug
where
    T: FromPartial,
{
    fn fill(
        &self,
        items: &mut [T::Partial],
    ) -> Result<(), HydrateError>;
}
