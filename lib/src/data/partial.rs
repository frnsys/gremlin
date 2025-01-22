//! Trait for incrementally filling ("hydrating") structs with data.

use super::river::HydrateError;

/// This trait indicates an associated struct, `Partial`,
/// which is the same as this struct except that
/// all fields are wrapped in `Option`.
///
/// You should not implement
/// this trait yourself but instead use the [`Partial` derive macro](../../infra_macros/derive.Partial.html).
pub trait FromPartial: Sized {
    type Partial: Partial;

    /// Create this struct from a fully-hydrated partial version.
    fn from(partial: Self::Partial) -> Result<Self, HydrateError>;

    /// Apply a partial to this struct.
    fn apply(&mut self, partial: Self::Partial);
}

pub trait Partial {
    fn missing_fields(&self) -> Vec<&'static str>;
}

pub trait OptionExt<T> {
    fn fallback(&mut self, value: T);
}
impl<T> OptionExt<T> for Option<T> {
    fn fallback(&mut self, value: T) {
        if self.is_none() {
            *self = Some(value);
        }
    }
}
impl<T> OptionExt<Option<T>> for Option<T> {
    fn fallback(&mut self, value: Option<T>) {
        if self.is_none() && value.is_some() {
            *self = value;
        }
    }
}
