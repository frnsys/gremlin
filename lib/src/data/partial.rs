//! Trait for incrementally filling ("hydrating") structs with data.

use anyhow::Error as AnyhowError;
use thiserror::Error;

/// This trait indicates this struct has an associated struct,
/// `Partial`, which is the same as this struct except that
/// all fields are wrapped in `Option`. You should not implement
/// this trait yourself but instead use the [`Partial` derive macro](../../infra_macros/derive.Partial.html).
pub trait FromPartial: Sized {
    type Partial;

    /// Create this struct from a fully-hydrated partial version.
    fn from(
        partial: Self::Partial,
    ) -> Result<Self, HydrateError>;
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
