//! Trait for incrementally filling ("hydrating") structs with data.

use anyhow::Error as AnyhowError;
use thiserror::Error;

/// This trait indicates an associated struct, `Partial`,
/// which is the same as this struct except that
/// all fields are wrapped in `Option`.
///
/// You should not implement
/// this trait yourself but instead use the [`Partial` derive macro](../../infra_macros/derive.Partial.html).
pub trait FromPartial: Sized + Default {
    type Partial;

    /// Create this struct from a fully-hydrated partial version.
    fn from(partial: Self::Partial) -> Result<Self, HydrateError>;

    /// Convert this into a partial version.
    fn into_partial(self) -> Self::Partial;

    /// Apply a partial to this struct.
    fn apply(&mut self, partial: Self::Partial);

    /// Create this struct from a default instance,
    /// and applying the partial.
    fn from_default(partial: Self::Partial) -> Self;
}

#[derive(Debug, Error)]
pub enum HydrateError {
    #[error("The following fields for {0} were empty: {1:?}.\n  You may need to add a tributary to fill the field(s).")]
    EmptyFields(&'static str, Vec<&'static str>),

    #[error("The following field for {0} is required to fill other fields: {1}")]
    MissingExpectedField(&'static str, &'static str),

    #[error(transparent)]
    Other(#[from] AnyhowError),
}
