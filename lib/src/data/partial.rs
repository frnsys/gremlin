//! Trait for incrementally filling ("hydrating") structs with data.

use super::river::HydrateError;

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

    /// Apply a partial to this struct.
    fn apply(&mut self, partial: Self::Partial);

    /// Create this struct from a default instance,
    /// and applying the partial.
    fn from_default(partial: Self::Partial) -> Self;
}
